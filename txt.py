#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gera arquivo TXT no leiaute Domínio Sistemas (0000 / 6000 / 6100)
a partir de lançamentos da tabela silver_erp_staging, filtrando:

- tenant_id
- acc_tail (conta/banco específico)
- período (DATE_FROM a DATE_TO)
- apenas dias com conciliação completa, conforme gold_daily_status.

Suporta tipo de lançamento em lote X e V:

  X = um débito p/ um crédito
  V = vários débitos p/ vários créditos (gera duas 6100 para cada lançamento)
"""

import os
import argparse
from decimal import Decimal
from datetime import datetime, date

import psycopg2
import psycopg2.extras


# =========================
# Helpers de formato
# =========================

def limpar_cnpj(cnpj: str) -> str:
    return ''.join(ch for ch in cnpj if ch.isdigit())


def formata_data(dt_val) -> str:
    if isinstance(dt_val, datetime):
        dt_val = dt_val.date()
    if not isinstance(dt_val, date):
        raise ValueError(f"Valor de data inválido: {dt_val!r}")
    return dt_val.strftime("%d/%m/%Y")


def formata_valor(valor) -> str:
    """
    Valor do lançamento em formato brasileiro com vírgula
    (ex.: 1,14 / 5571,24), sempre positivo.
    """
    if valor is None:
        raise ValueError("Valor do lançamento não pode ser None")
    if not isinstance(valor, Decimal):
        valor = Decimal(str(valor))
    valor = valor.copy_abs().quantize(Decimal("0.01"))
    s = f"{valor:.2f}"
    return s.replace(".", ",")


def gerar_0000(cnpj: str) -> str:
    """
    Registro 0000 - Identificação da empresa.
    Campo 1 = 0000
    Campo 2 = CNPJ/CPF só com números
    """
    return f"|0000|{limpar_cnpj(cnpj)}|"


def gerar_6000(tipo: str) -> str:
    """
    Registro 6000 - Lançamentos em Lote

    Campo 1 = 6000
    Campo 2 = Tipo do lançamento
              D = Um débito p/ vários créditos
              C = Um crédito p/ vários débitos
              X = Um débito p/ um crédito
              V = Vários débitos p/ vários créditos
    """
    tipo = tipo.upper()
    if tipo not in ("X", "V", "D", "C"):
        raise ValueError(f"Tipo de lançamento inválido: {tipo}")
    return f"|6000|{tipo}||||"


def montar_historico(row) -> str:
    """
    Monta histórico contábil a partir das colunas do ERP.

    Ajuste como preferir. Aqui uso:
      descrição do cliente + ' DOC ' + nr_documento
    """
    desc = (row.get("description_client") or "").strip()
    nr_doc = (row.get("nr_documento") or "").strip()

    texto = desc
    if nr_doc:
        texto = f"{desc} DOC {nr_doc}" if desc else nr_doc

    # Domínio usa '|' como separador, então por segurança removo:
    texto = texto.replace("|", " ")
    return texto[:255]


def extrair_contas(row):
    """
    PONTO DE CUSTOMIZAÇÃO IMPORTANTE
    ...
    """
    try:
        conta_debito = str(row["conta_debito"]).strip()
        conta_credito = str(row["conta_credito"]).strip()
    except KeyError as e:
        raise KeyError(
            f"Coluna {e} não encontrada no row. "
            "Inclua conta_debito e conta_credito na silver_erp_staging (ou ajuste esta função)."
        )
    if not conta_debito or not conta_credito:
        raise ValueError(
            f"conta_debito / conta_credito vazias para cd_lancamento={row.get('cd_lancamento')}"
        )
    return conta_debito, conta_credito


# =========================
# Geração das 6100
# =========================

def gerar_6100_X(row, conta_debito: str, conta_credito: str,
                 usuario: str = "", cod_filial: str = "", cod_scp: str = "") -> str:
    """
    Gera uma linha 6100 no formato X (um débito x um crédito), usando
    conta_debito e conta_credito fixas (parâmetros).
    """
    data = formata_data(row["date_br"])
    valor = formata_valor(row["amount_client_abs"])
    hist = montar_historico(row)

    campos = [
        "6100",
        data,
        conta_debito,
        conta_credito,
        valor,
        "",               # código do histórico (se você usa 0220, preencha aqui)
        hist,
        usuario or "",
        cod_filial or "",
        cod_scp or "",
    ]
    return "|" + "|".join(campos) + "|"


def gerar_6100_V(row, conta_debito: str, conta_credito: str,
                 usuario: str = "", cod_filial: str = "", cod_scp: str = ""):
    """
    Gera duas linhas 6100 no formato V (vários débitos x vários créditos)
    para um único lançamento, duplicando o valor:
      1ª linha: só crédito
      2ª linha: só débito
    """
    data = formata_data(row["date_br"])
    valor = formata_valor(row["amount_client_abs"])
    hist = montar_historico(row)

    base = {
        "data": data,
        "valor": valor,
        "hist": hist,
        "usuario": usuario or "",
        "cod_filial": cod_filial or "",
        "cod_scp": cod_scp or "",
    }

    # 1ª linha: crédito (débito vazio)
    campos1 = [
        "6100",
        base["data"],
        "",                    # conta débito vazia
        conta_credito,
        base["valor"],
        "",
        base["hist"],
        base["usuario"],
        base["cod_filial"],
        base["cod_scp"],
    ]

    # 2ª linha: débito (crédito vazio)
    campos2 = [
        "6100",
        base["data"],
        conta_debito,
        "",
        base["valor"],
        "",
        base["hist"],
        base["usuario"],
        base["cod_filial"],
        base["cod_scp"],
    ]

    linha1 = "|" + "|".join(campos1) + "|"
    linha2 = "|" + "|".join(campos2) + "|"
    return [linha1, linha2]


# =========================
# Acesso ao Postgres
# =========================

def buscar_dias_conciliados(conn, tenant_id: str, acc_tail: str,
                            date_from: str, date_to: str,
                            bank_code: str | None,
                            daily_table: str = "gold_conciliation_daily"):
    sql = f"""
        SELECT date::date AS dia
        FROM {daily_table}
        WHERE tenant_id = %s
          AND acc_tail = %s
          AND (%s IS NULL OR bank_code = %s)
          AND date::date BETWEEN %s AND %s
          AND COALESCE(erp_unrec_abs, 0) = 0
          AND COALESCE(api_unrec_abs, 0) = 0
          AND COALESCE(unrec_total_abs, 0) = 0
          AND COALESCE(unrec_diff, 0) = 0
        ORDER BY date::date
    """
    with conn.cursor() as cur:
        cur.execute(sql, (tenant_id, acc_tail, bank_code, bank_code, date_from, date_to))
        return [r[0] for r in cur.fetchall()]


def buscar_lancamentos(conn, tenant_id: str, acc_tail: str, dia: date,
                       bank_code: str | None,
                       erp_table: str = "silver_erp_staging"):
    """
    Busca todos os lançamentos do dia na silver_erp_staging para o acc_tail.

    Usa account_norm, mas normaliza para pegar só o tail numérico
    (ex.: ' 00724-2 ' -> '7242').
    """
    sql = f"""
        SELECT
            tenant_id,
            cd_lancamento,
            nr_documento,
            erp_code,
            date_br,
            description_client,
            amount_client,
            amount_client_abs,
            bank,
            bank_code,
            agency_norm,
            account_norm,
            favorecido
        FROM {erp_table}
        WHERE tenant_id = %s
          AND RIGHT(regexp_replace(account_norm, '[^0-9]', '', 'g'), 4) = %s
          AND date_br::date = %s::date
          AND (%s IS NULL OR bank_code = %s)
        ORDER BY date_br, cd_lancamento
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, (tenant_id, acc_tail, dia, bank_code, bank_code))
        return cur.fetchall()


# =========================
# main
# =========================

def main():
    parser = argparse.ArgumentParser(
        description="Gera TXT (0000/6000/6100) para importação em lote no Domínio, "
                    "filtrando por período, tenant, acc_tail e apenas dias conciliados."
    )
    parser.add_argument("--tenant", required=True, help="tenant_id (ex.: anderle)")
    parser.add_argument("--cnpj", required=True, help="CNPJ da empresa no Domínio")
    parser.add_argument("--acc-tail", required=True, help="acc_tail da conta (ex.: 7242)")
    parser.add_argument("--bank-code", default=None, help="Código do banco (ex.: 237) – opcional")
    parser.add_argument("--date-from", required=True, help="Data inicial (AAAA-MM-DD)")
    parser.add_argument("--date-to", required=True, help="Data final (AAAA-MM-DD)")
    parser.add_argument(
        "--tipo",
        required=True,
        choices=["X", "V"],
        help="Tipo do lançamento: X (1x1) ou V (vários x vários, 2x6100 por lançamento)",
    )
    parser.add_argument("--saida", required=True, help="Caminho do arquivo TXT de saída")
    parser.add_argument("--usuario", default="", help="Campo usuário (6100-8), opcional")
    parser.add_argument("--filial", default="", help="Campo filial (6100-9), opcional")
    parser.add_argument("--scp", default="", help="Campo SCP (6100-10), opcional")
    parser.add_argument("--conta-debito", required=True, help="Conta contábil a débito no Domínio (ex.: 6, 1.1.01.01, 11101001)")
    parser.add_argument("--conta-credito", required=True, help="Conta contábil a crédito no Domínio (ex.: 194, 6.2.01.01, 62101001)")


    # nomes das tabelas caso queira parametrizar
    parser.add_argument("--erp-table", default="silver_erp_staging",
                        help="Nome da tabela de lançamentos (default: silver_erp_staging)")
    parser.add_argument("--daily-table", default="gold_conciliation_daily",
                        help="Nome da tabela de status diário (default: gold_conciliation_daily)")

    # parâmetros de conexão (env ou linha de comando)
    parser.add_argument("--pg-host", default=os.getenv("PGHOST", "localhost"))
    parser.add_argument("--pg-port", default=os.getenv("PGPORT", "5432"))
    parser.add_argument("--pg-db",   default=os.getenv("PGDATABASE", "databricks"))
    parser.add_argument("--pg-user", default=os.getenv("PGUSER", "postgres"))
    parser.add_argument("--pg-pass", default=os.getenv("PGPASSWORD", "joao12345"))

    args = parser.parse_args()

    # Conexão
    conn = psycopg2.connect(
        host=args.pg_host,
        port=args.pg_port,
        dbname=args.pg_db,
        user=args.pg_user,
        password=args.pg_pass,
    )

    try:
        # 1) Buscar dias conciliados no período
        dias_ok = buscar_dias_conciliados(
            conn=conn,
            tenant_id=args.tenant,
            acc_tail=args.acc_tail,
            date_from=args.date_from,
            date_to=args.date_to,
            bank_code=args.bank_code,
            daily_table=args.daily_table,
        )

        if not dias_ok:
            raise SystemExit(
                f"Nenhum dia conciliado encontrado para tenant={args.tenant}, "
                f"acc_tail={args.acc_tail}, período {args.date_from} a {args.date_to}."
            )

        linhas_txt = []
        # 0000 só uma vez
        linhas_txt.append(gerar_0000(args.cnpj))

        # 2) Para cada dia conciliado, buscar lançamentos e gerar 1 lote (6000) por dia
        for dia in dias_ok:
            rows = buscar_lancamentos(
                conn=conn,
                tenant_id=args.tenant,
                acc_tail=args.acc_tail,
                dia=dia,
                bank_code=args.bank_code,
                erp_table=args.erp_table,
            )

            if not rows:
                # Se por algum motivo não tiver lançamentos nesse dia, só pula
                continue

            # UM 6000 POR DIA (lote diário)
            linhas_txt.append(gerar_6000(args.tipo))

            # Todos os lançamentos desse dia viram 6100 dentro do mesmo lote
            for row in rows:
                if args.tipo == "X":
                    linhas_txt.append(
                        gerar_6100_X(
                            row,
                            conta_debito=args.conta_debito,
                            conta_credito=args.conta_credito,
                            usuario=args.usuario,
                            cod_filial=args.filial,
                            cod_scp=args.scp,
                        )
                    )
                elif args.tipo == "V":
                    for linha in gerar_6100_V(
                        row,
                        conta_debito=args.conta_debito,
                        conta_credito=args.conta_credito,
                        usuario=args.usuario,
                        cod_filial=args.filial,
                        cod_scp=args.scp,
                    ):
                        linhas_txt.append(linha)


        # 3) Gravar o arquivo
        with open(args.saida, "w", encoding="latin1", newline="\r\n") as f:
            for linha in linhas_txt:
                f.write(linha + "\r\n")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
