#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import os, io, time, datetime as dt, re, unicodedata
from collections import deque, defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from typing import List, Literal, Tuple, Optional, Dict

import psycopg2
import polars as pl
import pandas as pd

# ========================= PARAMS =========================
TENANT = "anderle"
DATE_FROM = "2025-08-01"
DATE_TO   = "2025-08-31"

# Janela de leitura com folga ±5 dias
READ_FROM = (dt.date.fromisoformat(DATE_FROM) - dt.timedelta(days=5)).isoformat()
READ_TO   = (dt.date.fromisoformat(DATE_TO)   + dt.timedelta(days=5)).isoformat()

# Quantidade de dígitos usados no tail da conta
ACC_TAIL_DIGITS = 8

# Filtro opcional de acc_tail: se None, roda para todas as contas;
# se, por exemplo, "7424", considera somente essa conta em API e ERP.
ACC_FILTER: Optional[str] = None

ENABLE_DESC_STAGES = True

KSUM_MAX_ITEMS = int(os.getenv("KSUM_MAX_ITEMS", "48"))  # antes 64
MAX_GROUP_GUARD = 2000
MITM_STATE_BUDGET = int(os.getenv("MITM_STATE_BUDGET", "200000"))  # antes 1_000_000
CAP_PER_VALUE     = int(os.getenv("CAP_PER_VALUE", "32"))

# Limites específicos para o fallback DP (programação dinâmica)
# acima disso, NÃO roda DP, só MITM
DP_MAX_TARGET_CENTS = int(os.getenv("DP_MAX_TARGET_CENTS", "200000"))  # R$ 2.000,00
DP_MAX_ITEMS_DP     = int(os.getenv("DP_MAX_ITEMS_DP", "24"))

# Nunca misturar contas em estágios: o código já usa acc_tail em praticamente tudo;
# agora também garantimos que o bank_code é sempre parte das chaves de agrupamento/join.

# Prioridade dos estágios de DESCRIÇÃO (entre M0_* e estágios genéricos)
DESC_STAGE_PRIO = {
    "01_DESC_MN_SIGNATURE": 9,
    "02_DESC_FULL_1N":      9,
    "03_DESC_KSUM_1N":      9,
    "03_DESC_KSUM_N1":      9,
}
DESC_STAGE_ORDER = [
    "01_DESC_MN_SIGNATURE",
    "02_DESC_FULL_1N",
    "03_DESC_KSUM_1N",
    "03_DESC_KSUM_N1",
]

# ========================= CONN PG =========================
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_DB   = os.getenv("PG_DB",   "databricks")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "joao12345")
PG_PORT = int(os.getenv("PG_PORT", "5432"))

def pg_conn():
    return psycopg2.connect(
        host=PG_HOST,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        port=PG_PORT
    )

def df_to_pg(conn, df: pl.DataFrame, table: str, create_sql: str | None = None, truncate: bool = True):
    with conn.cursor() as cur:
        if create_sql:
            cur.execute(create_sql)
        if truncate:
            cur.execute(f"TRUNCATE TABLE {table}")
        conn.commit()
    if df.is_empty():
        return
    csv_text = df.write_csv()
    with conn.cursor() as cur:
        cur.copy_expert(
            f"COPY {table} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)",
            io.StringIO(csv_text)
        )
    conn.commit()

def join_pairs(left: pl.DataFrame, right: pl.DataFrame,
               pairs: list[tuple[str, str]], how: str = "inner") -> pl.DataFrame:
    left_on  = [l for (l, r) in pairs]
    right_on = [r for (l, r) in pairs]
    return left.join(right, left_on=left_on, right_on=right_on, how=how)

def apply_per_group(df: pl.DataFrame, by_cols: list[str], func) -> pl.DataFrame:
    """Equivalente ao groupby.apply para versões do Polars sem .apply()."""
    if df.is_empty():
        return pl.DataFrame({"api_row_id": [], "erp_row_id": []})
    outs: list[pl.DataFrame] = []
    for g in df.partition_by(by_cols, maintain_order=True):
        out = func(g)
        if out is not None and not out.is_empty():
            outs.append(out)
    return pl.concat(outs) if outs else pl.DataFrame({"api_row_id": [], "erp_row_id": []})

# ========================= READ (silver_*_staging) =========================
def right_digits(col: str, n: int) -> pl.Expr:
    return (
        pl.col(col).cast(pl.Utf8)
        .str.replace_all(r"\D", "")
        .str.replace_all(r"^0+", "")
        .str.slice(-n)
    )

def load_api_from_pg() -> pl.DataFrame:
    with pg_conn() as c, c.cursor() as cur:
        cur.execute("""
            SELECT id, descriptionraw, currencycode, amount, date_api, "date",
                   category, categoryid, status, "type", operationtype,
                   accountid, tenant_id, account_number, bank_code, bank_name
            FROM silver_api_staging
            WHERE tenant_id = %s
              AND date("date") BETWEEN %s AND %s
        """, (TENANT, READ_FROM, READ_TO))
        rows = cur.fetchall()
        cols = [d.name for d in cur.description]

    if not rows:
        return pl.DataFrame(schema=[c.lower() for c in cols])

    df = (
        pl.DataFrame(rows, schema=cols, orient="row")
        .rename({c: c.lower() for c in cols})
    )

    # 1) IDs, datas, cents
    df = (
        df
        .with_columns([
            pl.col("id").cast(pl.Utf8).alias("api_uid"),
            pl.int_range(0, pl.len()).cast(pl.Int64).alias("api_row_id"),
            pl.col("date").dt.date().alias("api_date_raw"),
            (pl.col("amount") * 100).round(0).cast(pl.Int64).alias("api_cents"),
        ])
    )

    # 2) amount, sign, acc_tail, descrição normalizada
    df = (
        df
        .with_columns([
            (pl.col("api_cents") / 100).cast(pl.Float64).alias("api_amount"),
            pl.when(pl.col("api_cents") >= 0).then(1).otherwise(-1).alias("api_sign"),
            right_digits("account_number", ACC_TAIL_DIGITS).alias("api_acc_tail"),
            pl.coalesce([pl.col("descriptionraw"), pl.lit("")])
              .str.to_uppercase()
              .str.replace_all("Á|À|Â|Ã|Ä","A")
              .str.replace_all("É|È|Ê|Ë","E")
              .str.replace_all("Í|Ì|Î|Ï","I")
              .str.replace_all("Ó|Ò|Ô|Õ|Ö","O")
              .str.replace_all("Ú|Ù|Û|Ü","U")
              .str.replace_all("Ç","C")
              .str.replace_all(r"\s+"," ")
              .str.strip_chars()
              .alias("api_desc_norm"),
        ])
    )

    # 3a) operationType em UPPER numa with_columns sozinha
    df = (
        df
        .with_columns([
            pl.coalesce([pl.col("operationtype"), pl.lit("")])
              .str.to_uppercase()
              .alias("api_optype_upper"),
        ])
    )

    # 3b) flags básicas (TAX/BANKFEES/PIX + rent_d1 + rent_generic) usando api_optype_upper
    df = (
        df
        .with_columns([
            # Imposto
            (
                (pl.coalesce([pl.col("category"), pl.lit("")]).str.to_lowercase() == "tax on financial operations")
                | (pl.col("categoryid") == "15030000")
            ).alias("api_is_tax"),

            # Tarifas bancárias
            (
                (pl.col("categoryid") == "16000000")
                | (pl.coalesce([pl.col("category"), pl.lit("")]).str.to_lowercase() == "bank fees")
            ).alias("api_is_bankfees"),

            # Tarifa PIX
            (
                (pl.coalesce([pl.col("category"), pl.lit("")]).str.to_lowercase() == "transfer - pix")
                & (pl.col("api_optype_upper") == "TARIFA_SERVICOS_AVULSOS")
            ).alias("api_is_pix_tariff"),

            # RENT D+1 (Itaú) – RENDIMENTO_APLIC_FINANCEIRA
            (pl.col("api_optype_upper") == "RENDIMENTO_APLIC_FINANCEIRA").alias("api_is_rent_d1"),

            # RENT genérico (D+2): category / categoryid / RESGATE_APLIC_FINANCEIRA
            (
                (pl.coalesce([pl.col("category"), pl.lit("")]).str.to_lowercase() == "proceeds interests and dividends")
                | (pl.col("categoryid") == "03060000")
                | (pl.col("api_optype_upper") == "RESGATE_APLIC_FINANCEIRA")
            ).alias("api_is_rent_generic"),
        ])
    )

    # 4) Flag geral de rent (D+1 + D+2)
    df = (
        df
        .with_columns([
            (pl.col("api_is_rent_d1") | pl.col("api_is_rent_generic")).alias("api_is_rent"),
        ])
    )

    # 5) Datas de conciliação (ajustes especiais + fim de semana)
    bankfees_package_rule = (
        pl.col("api_is_bankfees")
        & (pl.col("api_optype_upper") == "PACOTE_TARIFA_SERVICOS")
    )

    txn_time_ref = pl.coalesce([pl.col("date"), pl.col("date_ts_utc"), pl.col("date_ts_br")])
    txn_seconds = (
        txn_time_ref.dt.hour().cast(pl.Int64) * 3600
        + txn_time_ref.dt.minute().cast(pl.Int64) * 60
        + txn_time_ref.dt.second().cast(pl.Int64)
    )

    early_transfer_rule = (
        (pl.col("categoryid") == "05030000")
        & (txn_seconds <= 5 * 3600)
    )

    bankfees_avulso_rule = (
        (
            (pl.col("categoryid") == "16000000")
            | (pl.coalesce([pl.col("category"), pl.lit("")]).str.to_lowercase() == "bank fees")
        )
        & (pl.col("api_optype_upper") == "TARIFA_SERVICOS_AVULSOS")
        & ~bankfees_package_rule
    )
    bankfees_carga_crt_rule = (
        bankfees_avulso_rule
        & pl.col("api_desc_norm").str.contains(
            "TARIFA BANCARIA - CARGA CRT TRANSP", literal=True
        )
    )

    # Ajustes de data passaram a ser feitos na criação da silver; mantemos a data bruta.
    # d_plus_2_rules = pl.lit(False)
    # d_minus_1_rules = (
    #     (pl.col("categoryid") == "15030000")
    #     | (bankfees_avulso_rule & ~bankfees_carga_crt_rule)
    #     | (pl.col("categoryid") == "05050000")
    #     | (pl.col("api_optype_upper") == "RENDIMENTO_APLIC_FINANCEIRA")
    #     | early_transfer_rule
    # )
    # d_minus_2_rules = (
    #     (pl.col("categoryid") == "03060000")
    #     | (
    #         (pl.col("categoryid") == "05070000")
    #         & (pl.col("api_optype_upper") == "TARIFA_SERVICOS_AVULSOS")
    #     )
    #     | bankfees_package_rule
    #     | bankfees_carga_crt_rule
    # )
    # conc_date_base = (
    #     pl.when(d_minus_1_rules)
    #       .then(shift_business_days(pl.col("api_date_raw"), -1))
    #       .when(d_minus_2_rules)
    #       .then(shift_business_days(pl.col("api_date_raw"), -2))
    #       .when(d_plus_2_rules)
    #       .then(shift_business_days(pl.col("api_date_raw"), 2))
    #       .otherwise(pl.col("api_date_raw"))
    # )
    # conc_date = (
    #     pl.when(d_minus_1_rules | d_minus_2_rules | d_plus_2_rules)
    #       # deslocamentos D-1/D-2/D+2 nunca podem ficar em fim de semana: volta para sexta
    #       .then(
    #           conc_date_base.map_elements(
    #               lambda d: prev_business_day(d) if is_weekend(d) else d,
    #               return_dtype=pl.Date,
    #           )
    #       )
    #       # datas originais em fim de semana vão para a segunda-feira seguinte
    #       .otherwise(
    #           conc_date_base.map_elements(
    #               lambda d: next_business_day(d) if is_weekend(d) else d,
    #               return_dtype=pl.Date,
    #           )
    #       )
    # )
    conc_date = pl.col("api_date_raw")

    # 6) Datas derivadas D-1 / D-2 + seleção final
    df = (
        df
        .with_columns([
            conc_date.alias("api_conciliation_date"),
            conc_date.alias("api_date"),
            conc_date.alias("api_date_d1"),
            conc_date.alias("api_date_d2"),
        ])
        .select([
            "api_row_id", "api_uid", "tenant_id",
            "api_date_raw", "api_conciliation_date", "api_date", "api_date_d1", "api_date_d2",
            "api_amount", "api_cents", "api_sign",
            "api_acc_tail", "api_desc_norm",
            "bank_code",
            "bank_name",
            "api_is_tax", "api_is_bankfees",
            "api_is_pix_tariff",
            "api_is_rent",        # geral (D+1 + D+2)
            "api_is_rent_d1",     # só RENDIMENTO_APLIC_FINANCEIRA (Itaú)
        ])
    )

    # Filtro opcional por acc_tail
    if ACC_FILTER is not None:
        df = df.filter(pl.col("api_acc_tail") == ACC_FILTER)

    return df

def load_erp_from_pg() -> pl.DataFrame:
    with pg_conn() as c, c.cursor() as cur:
        cur.execute("""
            SELECT tenant_id, cd_lancamento, nr_documento, erp_code,
                   date_br, description_client, amount_client, amount_client_abs,
                   bank, bank_code, agency_norm, account_norm, favorecido
            FROM silver_erp_staging
            WHERE tenant_id = %s
              AND date(date_br) BETWEEN %s AND %s
        """, (TENANT, READ_FROM, READ_TO))
        rows = cur.fetchall()
        cols = [d.name for d in cur.description]

    if not rows:
        return pl.DataFrame(schema=cols)

    df = pl.DataFrame(rows, schema=cols, orient="row")

    df = (
        df
        .with_columns([
            pl.col("cd_lancamento").cast(pl.Utf8).alias("erp_uid"),
            pl.int_range(0, pl.len()).cast(pl.Int64).alias("erp_row_id"),
            pl.col("date_br").dt.date().alias("erp_date"),
            (pl.col("amount_client") * 100).round(0).cast(pl.Int64).alias("erp_cents"),
            pl.col("bank").cast(pl.Utf8).alias("bank_name"),
        ])
        .with_columns([
            (pl.col("erp_cents") / 100).cast(pl.Float64).alias("erp_amount"),
            pl.when(pl.col("erp_cents") >= 0).then(1).otherwise(-1).alias("erp_sign"),
            right_digits("account_norm", ACC_TAIL_DIGITS).alias("erp_acc_tail"),
            pl.coalesce([pl.col("description_client"), pl.lit("")])
              .str.to_uppercase()
              .str.replace_all("Á|À|Â|Ã|Ä","A")
              .str.replace_all("É|È|Ê|Ë","E")
              .str.replace_all("Í|Ì|Î|Ï","I")
              .str.replace_all("Ó|Ò|Ô|Õ|Ö","O")
              .str.replace_all("Ú|Ù|Û|Ü","U")
              .str.replace_all("Ç","C")
              .str.replace_all(r"\s+"," ")
              .str.strip_chars()
              .alias("erp_desc_norm"),
        ])
        .select([
            "erp_row_id", "erp_uid", "tenant_id",
            "erp_date", "erp_amount", "erp_cents",
            "erp_sign", "erp_acc_tail", "erp_desc_norm",
            "bank_code", "bank_name"
        ])
    )

    if ACC_FILTER is not None:
        df = df.filter(pl.col("erp_acc_tail") == ACC_FILTER)

    return df

# ========================= CALENDÁRIO =========================
def build_calendar(dfrom: str, dto: str, pad: int = 15) -> pl.DataFrame:
    d0 = dt.date.fromisoformat(dfrom) - dt.timedelta(days=pad)
    d1 = dt.date.fromisoformat(dto)   + dt.timedelta(days=pad)
    dates = pl.date_range(d0, d1, "1d", eager=True).to_frame("cal_date")
    cal = (
        dates
        .with_columns(pl.col("cal_date").dt.weekday().alias("dow"))
        .with_columns((pl.col("dow").is_between(0, 4)).cast(pl.Int8).alias("is_biz"))
        .with_columns(pl.col("is_biz").cum_sum().alias("biz_ord"))
        .select(["cal_date", "biz_ord"])
    )
    return cal

# ========================= Subset-sum (meet-in-the-middle) =========================
def enforce_mitm_budget(items: list[tuple[int,int]]) -> list[tuple[int,int]]:
    """Garante que 2^(n/2) <= MITM_STATE_BUDGET."""
    n = len(items)
    if n <= 2:
        return items
    while (1 << (n // 2)) > MITM_STATE_BUDGET and n > 2:
        n -= 2
    return items[:n]

def subset_mitm(target_cents: int, items: list[tuple[int,int]]) -> list[int] | None:
    # items já devem estar capados/ordenados antes de chegar aqui
    if not items:
        return None
    items = enforce_mitm_budget(items)
    n = len(items)
    if n == 0:
        return None
    m = n // 2
    L = items[:m]
    R = items[m:]

    # LEFT
    left: list[tuple[int, list[int]]] = []
    for mask in range(1 << len(L)):
        s = 0
        ids: list[int] = []
        for i in range(len(L)):
            if (mask >> i) & 1:
                s += L[i][1]
                ids.append(L[i][0])
        left.append((s, ids))

    # RIGHT: melhor cardinalidade por soma
    best: dict[int, list[int]] = {}
    for mask in range(1 << len(R)):
        s = 0
        ids: list[int] = []
        for i in range(len(R)):
            if (mask >> i) & 1:
                s += R[i][1]
                ids.append(R[i][0])
        prev = best.get(s)
        if prev is None or len(ids) < len(prev):
            best[s] = ids

    for s, ids1 in left:
        need = target_cents - s
        ids2 = best.get(need)
        if ids2 is not None:
            return ids1 + ids2
    return None

def subset_dp(target: int, items: list[tuple[int, int]]) -> list[int] | None:
    """
    Fallback DP para casos pequeninos. Se o alvo for grande demais ou houver
    muitos itens, simplesmente NÃO roda DP (retorna None).
    Isso evita explosão de memória/tempo em targets grandes.
    """
    if target == 0:
        return []

    abs_target = abs(target)

    # Guarda forte: nada de DP para alvos gigantes ou muitos itens
    if abs_target > DP_MAX_TARGET_CENTS or len(items) > DP_MAX_ITEMS_DP:
        return None

    dp = [False] * (abs_target + 1)
    dp[0] = True
    used: list[list[int]] = [[] for _ in range(abs_target + 1)]

    for tid, c in items:
        c_abs = abs(c)
        if c_abs > abs_target:
            continue
        for s in range(abs_target, c_abs - 1, -1):
            if dp[s - c_abs]:
                dp[s] = True
                used[s] = used[s - c_abs] + [tid]

    if dp[abs_target]:
        return used[abs_target]
    return None

def _sum_ok(items_dict: dict[int, int], ids: list[int], target_cents: int) -> bool:
    s = 0
    ids_set = set(ids)
    for i, v in items_dict.items():
        if i in ids_set:
            s += v
    return s == target_cents

def cap_items_by_value(ids: list[int], cents: list[int], target_cents: int) -> list[tuple[int,int]]:
    """
    Agrupa por 'cents' e mantém no máx. o necessário p/ atingir target.
    """
    by_val: dict[int, list[int]] = defaultdict(list)
    for i, c in zip(ids, cents):
        by_val[int(c)].append(int(i))

    out: list[tuple[int,int]] = []
    t_abs = abs(int(target_cents))
    for c, idlist in by_val.items():
        c_abs = max(1, abs(int(c)))
        need = t_abs // c_abs
        k = min(len(idlist), max(1, need), CAP_PER_VALUE)
        for _id in idlist[:k]:
            out.append((int(_id), int(c)))

    out.sort(key=lambda x: abs(x[1]), reverse=True)
    if len(out) > KSUM_MAX_ITEMS:
        out = out[:KSUM_MAX_ITEMS]
    return out

def solve_n1_group(df_group: pl.DataFrame) -> pl.DataFrame:
    erp_id = int(df_group["erp_row_id"][0])
    target_cents = int(round(float(df_group["erp_amount"][0]) * 100))

    apis = (
        df_group
        .select(["api_row_id", "api_amount"])
        .unique()
        .with_columns((pl.col("api_amount") * 100).round(0).cast(pl.Int64).alias("cents"))
        .filter(pl.col("cents").abs() <= abs(target_cents))
        .select(["api_row_id", "cents"])
    )
    if apis.is_empty():
        return pl.DataFrame({"api_row_id": [], "erp_row_id": []})

    items = cap_items_by_value(apis["api_row_id"].to_list(),
                               apis["cents"].to_list(), target_cents)
    if not items:
        return pl.DataFrame({"api_row_id": [], "erp_row_id": []})

    sol = subset_mitm(target_cents, items)
    if sol is None:
        sol = subset_dp(target_cents, items)
    if not sol:
        return pl.DataFrame({"api_row_id": [], "erp_row_id": []})

    items_dict = {int(i): int(c) for i, c in items}
    if not _sum_ok(items_dict, [int(x) for x in sol], target_cents):
        return pl.DataFrame({"api_row_id": [], "erp_row_id": []})

    return pl.DataFrame(
        {"api_row_id": [int(x) for x in sol],
         "erp_row_id": [erp_id] * len(sol)}
    )

def solve_1n_group(df_group: pl.DataFrame) -> pl.DataFrame:
    api_id = int(df_group["api_row_id"][0])
    target_cents = int(round(float(df_group["api_amount"][0]) * 100))

    erps = (
        df_group
        .select(["erp_row_id", "erp_amount"])
        .unique()
        .with_columns((pl.col("erp_amount") * 100).round(0).cast(pl.Int64).alias("cents"))
        .filter(pl.col("cents").abs() <= abs(target_cents))
        .select(["erp_row_id", "cents"])
    )
    if erps.is_empty():
        return pl.DataFrame({"api_row_id": [], "erp_row_id": []})

    items = cap_items_by_value(erps["erp_row_id"].to_list(),
                               erps["cents"].to_list(), target_cents)
    if not items:
        return pl.DataFrame({"api_row_id": [], "erp_row_id": []})

    sol = subset_mitm(target_cents, items)
    if sol is None:
        sol = subset_dp(target_cents, items)
    if not sol:
        return pl.DataFrame({"api_row_id": [], "erp_row_id": []})

    items_dict = {int(i): int(c) for i, c in items}
    if not _sum_ok(items_dict, [int(x) for x in sol], target_cents):
        return pl.DataFrame({"api_row_id": [], "erp_row_id": []})

    return pl.DataFrame(
        {"api_row_id": [api_id] * len(sol),
         "erp_row_id": [int(x) for x in sol]}
    )

def solve_same_group(df_group: pl.DataFrame) -> pl.DataFrame:
    if df_group.is_empty():
        return pl.DataFrame({"api_row_id": [], "erp_row_id": []})

    apis = (
        df_group.filter(pl.col("side") == "API")
        .select(["api_row_id", "amount"])
        .unique()
    )
    erps = (
        df_group.filter(pl.col("side") == "ERP")
        .select(["erp_row_id", "amount"])
        .unique()
    )
    if apis.is_empty() or erps.is_empty():
        return pl.DataFrame({"api_row_id": [], "erp_row_id": []})

    apis = apis.with_columns((pl.col("amount") * 100).round(0).cast(pl.Int64).alias("cents"))
    erps = erps.with_columns((pl.col("amount") * 100).round(0).cast(pl.Int64).alias("cents"))

    group_date = df_group["date"][0]
    group_acc  = df_group["acc_tail"][0]
    group_sign = df_group["sign"][0]
    group_bank = df_group["bank_code"][0] if "bank_code" in df_group.columns else None

    if apis.height + erps.height > MAX_GROUP_GUARD:
        print(
            f"Large group detected: date={group_date}, bank={group_bank}, acc_tail={group_acc}, sign={group_sign}, "
            f"apis={apis.height}, erps={erps.height}. Trimming to top {KSUM_MAX_ITEMS} by abs cents."
        )
        apis = apis.sort(pl.col("cents").abs(), descending=True).head(KSUM_MAX_ITEMS)
        erps = erps.sort(pl.col("cents").abs(), descending=True).head(KSUM_MAX_ITEMS)

    used_api: set[int] = set()
    used_erp: set[int] = set()
    links_api: list[tuple[int,int]] = []
    links_erp: list[tuple[int,int]] = []

    api_uid_map = dict(zip(
        df_group.filter(pl.col("side") == "API")["api_row_id"].to_list(),
        df_group.filter(pl.col("side") == "API")["api_row_id"].to_list()
    ))
    erp_uid_map = dict(zip(
        df_group.filter(pl.col("side") == "ERP")["erp_row_id"].to_list(),
        df_group.filter(pl.col("side") == "ERP")["erp_row_id"].to_list()
    ))

    # N:1 (ERP alvo)
    for erp_id, cents in zip(erps["erp_row_id"].to_list(), erps["cents"].to_list()):
        if erp_id in used_erp or cents == 0:
            continue
        cand = apis.filter(~pl.col("api_row_id").is_in(list(used_api)))
        cand = cand.filter(pl.col("cents").abs() <= abs(int(cents)))
        if cand.is_empty():
            continue
        items = cap_items_by_value(cand["api_row_id"].to_list(),
                                   cand["cents"].to_list(), int(cents))
        if not items:
            continue
        sol = subset_mitm(int(cents), items)
        if sol is None:
            sol = subset_dp(int(cents), items)
        if sol:
            items_dict = {int(i): int(c) for i, c in items}
            if _sum_ok(items_dict, [int(x) for x in sol], int(cents)):
                print(
                    f"Found N:1 match in group (date={group_date}, bank={group_bank}, acc={group_acc}, sign={group_sign}) "
                    f"for ERP {erp_id} uid={erp_uid_map.get(erp_id, '??')} cents={cents}"
                )
                for aid in sol:
                    links_erp.append((int(aid), int(erp_id)))
                used_api.update([int(aid) for aid in sol])
                used_erp.add(int(erp_id))

    # 1:N (API alvo)
    for api_id, cents in zip(apis["api_row_id"].to_list(), apis["cents"].to_list()):
        if api_id in used_api or cents == 0:
            continue
        cand = erps.filter(~pl.col("erp_row_id").is_in(list(used_erp)))
        cand = cand.filter(pl.col("cents").abs() <= abs(int(cents)))
        if cand.is_empty():
            continue
        items = cap_items_by_value(cand["erp_row_id"].to_list(),
                                   cand["cents"].to_list(), int(cents))
        if not items:
            continue
        sol = subset_mitm(int(cents), items)
        if sol is None:
            sol = subset_dp(int(cents), items)
        if sol:
            items_dict = {int(i): int(c) for i, c in items}
            if _sum_ok(items_dict, [int(x) for x in sol], int(cents)):
                print(
                    f"Found 1:N match in group (date={group_date}, bank={group_bank}, acc={group_acc}, sign={group_sign}) "
                    f"for API {api_id} uid={api_uid_map.get(api_id, '??')} cents={cents}"
                )
                for eid in sol:
                    links_api.append((int(api_id), int(eid)))
                used_erp.update([int(eid) for eid in sol])
                used_api.add(int(api_id))

    out = links_erp + links_api
    if not out:
        return pl.DataFrame({"api_row_id": [], "erp_row_id": []})
    return pl.DataFrame(out, schema=[("api_row_id", pl.Int64), ("erp_row_id", pl.Int64)], orient="row")

# ========================= VALIDAÇÃO POR COMPONENTE =========================
def finalize_by_components(matches: pl.DataFrame,
                           A0: pl.DataFrame,
                           E0: pl.DataFrame) -> pl.DataFrame:
    """
    Mantém apenas arestas pertencentes a componentes conectados
    cuja soma(API_cents) == soma(ERP_cents).
    """
    if matches.is_empty():
        return matches

    api_map = dict(zip(A0["api_row_id"].to_list(), A0["api_cents"].to_list()))
    erp_map = dict(zip(E0["erp_row_id"].to_list(), E0["erp_cents"].to_list()))

    edges = [
        (int(a), int(e))
        for a, e in zip(matches["api_row_id"].to_list(),
                        matches["erp_row_id"].to_list())
    ]

    adjA: dict[int, set[int]] = defaultdict(set)
    adjE: dict[int, set[int]] = defaultdict(set)
    nodesA: set[int] = set()
    nodesE: set[int] = set()

    for a, e in edges:
        adjA[a].add(e)
        adjE[e].add(a)
        nodesA.add(a)
        nodesE.add(e)

    comp_id_of_A: dict[int, int] = {}
    comp_id_of_E: dict[int, int] = {}
    comp_idx = 0

    # componentes iniciando em A
    for a0 in list(nodesA):
        if a0 in comp_id_of_A:
            continue
        q = deque()
        q.append(('A', a0))
        comp_id = comp_idx
        comp_idx += 1
        while q:
            side, nid = q.popleft()
            if side == 'A':
                if nid in comp_id_of_A:
                    continue
                comp_id_of_A[nid] = comp_id
                for e in adjA.get(nid, []):
                    if e not in comp_id_of_E:
                        q.append(('E', e))
            else:
                if nid in comp_id_of_E:
                    continue
                comp_id_of_E[nid] = comp_id
                for a in adjE.get(nid, []):
                    if a not in comp_id_of_A:
                        q.append(('A', a))

    # componentes iniciando em E
    for e0 in list(nodesE):
        if e0 in comp_id_of_E:
            continue
        q = deque()
        q.append(('E', e0))
        comp_id = comp_idx
        comp_idx += 1
        while q:
            side, nid = q.popleft()
            if side == 'A':
                if nid in comp_id_of_A:
                    continue
                comp_id_of_A[nid] = comp_id
                for e in adjA.get(nid, []):
                    if e not in comp_id_of_E:
                        q.append(('E', e))
            else:
                if nid in comp_id_of_E:
                    continue
                comp_id_of_E[nid] = comp_id
                for a in adjE.get(nid, []):
                    if a not in comp_id_of_A:
                        q.append(('A', a))

    sum_api: dict[int, int] = defaultdict(int)
    sum_erp: dict[int, int] = defaultdict(int)

    for a, cid in comp_id_of_A.items():
        sum_api[cid] += int(api_map.get(a, 0))
    for e, cid in comp_id_of_E.items():
        sum_erp[cid] += int(erp_map.get(e, 0))

    valid_comp = {
        cid for cid in set(list(sum_api.keys()) + list(sum_erp.keys()))
        if sum_api.get(cid, 0) == sum_erp.get(cid, 0)
    }

    comp_for_edge: list[int] = []
    for a, e in edges:
        cid = comp_id_of_A.get(a, comp_id_of_E.get(e, -1))
        comp_for_edge.append(cid)

    matches_with_cc = matches.with_columns(
        pl.Series(name="cc_id", values=comp_for_edge)
    )
    return (
        matches_with_cc
        .filter(pl.col("cc_id").is_in(list(valid_comp)))
        .drop("cc_id")
    )

# ========================= HELPERS DE DATA ÚTIL (DESCRIÇÃO) =========================
def is_weekend(d: date) -> bool:
    return d.weekday() >= 5  # 5 = sábado, 6 = domingo

def prev_business_day(d: date) -> date:
    d -= timedelta(days=1)
    while is_weekend(d):
        d -= timedelta(days=1)
    return d

def next_business_day(d: date) -> date:
    d += timedelta(days=1)
    while is_weekend(d):
        d += timedelta(days=1)
    return d

def add_business_days(d: date, n: int) -> date:
    step = 1 if n >= 0 else -1
    for _ in range(abs(n)):
        if step > 0:
            d = next_business_day(d)
        else:
            d = prev_business_day(d)
    return d

def shift_business_days(expr: pl.Expr, n: int) -> pl.Expr:
    """Aplica um deslocamento em dias úteis a uma coluna de datas."""
    return expr.map_elements(
        lambda d: add_business_days(d, n),
        return_dtype=pl.Date,
    )

def candidate_dates(d: date) -> List[date]:
    """
    Ordem de busca:
    - dia exato
    - +1 dia útil
    - +2 dias úteis
    - -1 dia útil
    - -2 dias úteis
    """
    return [
        d,
        add_business_days(d, 1),
        add_business_days(d, 2),
        add_business_days(d, -1),
        add_business_days(d, -2),
    ]

# ========================= Helpers de texto (descrição) =========================
STOPWORDS = {
    "PAGAMENTO", "PAGTO", "PAGTO.", "PAGT", "PAG",
    "TRANSFERENCIA", "TRANSF", "ENTRE", "CONTAS",
    "SAIDA", "ENTRADA", "DEBITO", "CREDITO", "DOC",
    "TED", "PIX", "BOLETO", "COBRANCA", "REF", "PGTO",
    "NR", "NUM", "BANCO", "REGIONAL", "DESENVOLVIMENTO",
    "DOCTO", "CONTA", "VARIAVEL", "FIXO", "MENSAL",
    "FATURA", "PARCELA", "PARC", "TITULO",
    "RECEB", "RECEBIDO", "RECEBIMENTO", "VALOR",
    "COD", "TARIFAS", "BANCARIAS", "ANTECIPACAO", "ADM",
}

def normalize_text(text: str) -> str:
    if text is None:
        return ""
    text = str(text).upper()
    text = "".join(
        c for c in unicodedata.normalize("NFD", text)
        if unicodedata.category(c) != "Mn"
    )
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def extract_keywords(text: str, max_keywords: int = 8) -> List[str]:
    norm = normalize_text(text)
    tokens = norm.split()
    kws: List[str] = []
    seen: set[str] = set()
    for t in tokens:
        if len(t) <= 2:
            continue
        if t.isdigit():
            continue
        if t in STOPWORDS:
            continue
        if t in seen:
            continue
        seen.add(t)
        kws.append(t)
        if len(kws) >= max_keywords:
            break
    return kws

def normalize_account_number(acc: Optional[str]) -> str:
    if acc is None:
        return ""
    s = re.sub(r"\D+", "", str(acc))
    return s.lstrip("0")

def normalize_doc_key_str(s: Optional[str]) -> Optional[str]:
    """
    Converte um número de DOC/DOCTO em uma data canônica (YYYY-MM-DD).

    Formatos aceitos (apenas dígitos):
      - 8 dígitos: DDMMYYYY   -> 05082025
      - 6 dígitos: DDMMYY     -> 050825  (-> 05/08/2025)
      - 5 dígitos: DMMYY      -> 50825   (-> 05/08/2025)

    Qualquer coisa fora disso vira None.
    """
    if s is None:
        return None
    s = re.sub(r"\D+", "", str(s))
    if not s:
        return None

    try:
        if len(s) == 8:  # DDMMYYYY
            d = int(s[0:2])
            m = int(s[2:4])
            y = int(s[4:8])
        elif len(s) == 6:  # DDMMYY
            d = int(s[0:2])
            m = int(s[2:4])
            y = 2000 + int(s[4:6])
        elif len(s) == 5:  # DMMYY (ex.: 50825 -> 05/08/2025)
            d = int(s[0:1])
            m = int(s[1:3])
            y = 2000 + int(s[3:5])
        else:
            return None

        dt_obj = date(y, m, d)
    except Exception:
        return None

    return dt_obj.isoformat()

# ========================= Estrutura interna de TX (descrição) =========================
@dataclass
class Tx:
    side: Literal["api", "erp"]
    key: str
    idx: int
    amount: Decimal
    date: date
    work_date: date
    matched: bool = False
    tenant_id: str = ""
    bank_code: str = ""
    bank_name: str = ""
    acc_norm: str = ""
    acc_tail: str = ""

    @property
    def sign(self) -> int:
        if self.amount > 0:
            return 1
        if self.amount < 0:
            return -1
        return 0

# ========================= Carregar dados p/ descrição (pandas) =========================
def load_api_df(conn, tenant_id: str, date_from: str, date_to: str) -> pd.DataFrame:
    sql = """
        SELECT id, tenant_id, account_number, bank_code, bank_name,
               descriptionraw, currencycode,
               amount::numeric AS amount,
               "date"::date   AS date
        FROM silver_api_staging
        WHERE tenant_id = %s
          AND "date"::date BETWEEN %s AND %s;
    """
    df = pd.read_sql(sql, conn, params=(tenant_id, date_from, date_to))

    # Normaliza e filtra por acc_tail, se necessário
    df["acc_norm"] = df["account_number"].apply(normalize_account_number)
    df["acc_tail"] = df["acc_norm"].str[-ACC_TAIL_DIGITS:]
    if ACC_FILTER is not None:
        df = df[df["acc_tail"] == ACC_FILTER].copy()

    return df

def load_erp_df(conn, tenant_id: str, date_from: str, date_to: str) -> pd.DataFrame:
    sql = """
        SELECT tenant_id, cd_lancamento, nr_documento, erp_code,
               date_br::date       AS date_br,
               description_client,
               amount_client::numeric      AS amount_client,
               amount_client_abs::numeric  AS amount_client_abs,
               bank AS bank_name, bank_code, agency_norm, account_norm, favorecido
        FROM silver_erp_staging
        WHERE tenant_id = %s
          AND date_br::date BETWEEN %s AND %s;
    """
    df = pd.read_sql(sql, conn, params=(tenant_id, date_from, date_to))

    df["acc_norm"] = df["account_norm"].apply(normalize_account_number)
    df["acc_tail"] = df["acc_norm"].str[-ACC_TAIL_DIGITS:]
    if ACC_FILTER is not None:
        df = df[df["acc_tail"] == ACC_FILTER].copy()

    return df

# ========================= Funções de matching por descrição =========================
def _safe_decimal(val) -> Optional[Decimal]:
    """Converte para Decimal ignorando NaN/nulos/valores inválidos."""
    if pd.isna(val):
        return None
    try:
        return Decimal(str(val))
    except (InvalidOperation, TypeError, ValueError):
        return None

def _build_txs(
    api_df: pd.DataFrame,
    erp_df: pd.DataFrame
) -> Tuple[List[Tx], pd.DataFrame, pd.DataFrame]:
    txs: List[Tx] = []

    api_reset = api_df.reset_index(drop=True)
    for i, row in api_reset.iterrows():
        dt_api = pd.to_datetime(row["date"]).date()
        # Work date já ajustada na silver
        work_dt = dt_api

        acc_norm = normalize_account_number(row.get("account_number"))
        acc_tail = acc_norm[-ACC_TAIL_DIGITS:] if acc_norm else ""

        amount_dec = _safe_decimal(row["amount"])
        # se amount inválido/NaN, ignora essa transação na parte de descrição
        if amount_dec is None:
            # opcional: logar algo
            # print(f"[desc] ignorando API id={row['id']} amount inválido={row['amount']}")
            continue

        txs.append(
            Tx(
                side="api",
                key=str(row["id"]),
                idx=i,
                amount=amount_dec,
                date=dt_api,
                work_date=work_dt,
                tenant_id=str(row.get("tenant_id", "") or ""),
                bank_code=str(row.get("bank_code", "") or ""),
                bank_name=str(row.get("bank_name", "") or ""),
                acc_norm=acc_norm,
                acc_tail=acc_tail,
            )
        )

    erp_reset = erp_df.reset_index(drop=True)
    for i, row in erp_reset.iterrows():
        dt_erp = pd.to_datetime(row["date_br"]).date()
        acc_norm = normalize_account_number(row.get("account_norm"))
        acc_tail = acc_norm[-ACC_TAIL_DIGITS:] if acc_norm else ""

        amount_dec = _safe_decimal(row["amount_client"])
        if amount_dec is None:
            # opcional: print para rastrear
            # print(f"[desc] ignorando ERP cd_lanc={row['cd_lancamento']} amount inválido={row['amount_client']}")
            continue

        txs.append(
            Tx(
                side="erp",
                key=str(row["cd_lancamento"]),
                idx=i,
                amount=amount_dec,
                date=dt_erp,
                work_date=dt_erp,
                tenant_id=str(row.get("tenant_id", "") or ""),
                bank_code=str(row.get("bank_code", "") or ""),
                bank_name=str(row.get("bank_name", "") or ""),
                acc_norm=acc_norm,
                acc_tail=acc_tail,
            )
        )
    return txs, api_reset, erp_reset

def _subset_sum_for_anchor(
    target: Decimal,
    candidates: List[Tx],
    max_len: int = 8,
    eps: Decimal = Decimal("0.01"),
    max_nodes: int = 200_000,
) -> Optional[List[Tx]]:
    cands_sorted = sorted(candidates, key=lambda t: abs(t.amount), reverse=True)
    n = len(cands_sorted)
    abs_vals = [abs(t.amount) for t in cands_sorted]
    prefix_sum = [Decimal("0")]
    for v in abs_vals:
        prefix_sum.append(prefix_sum[-1] + v)

    def dfs(
        start: int,
        depth_limit: int,
        chosen: List[int],
        current_sum: Decimal,
        nodes_ref: List[int],
    ) -> Optional[List[int]]:
        nodes_ref[0] += 1
        if nodes_ref[0] > max_nodes:
            return None
        if len(chosen) == depth_limit:
            if abs(current_sum - target) <= eps:
                return list(chosen)
            return None
        if current_sum > target + eps:
            return None
        remaining_slots = depth_limit - len(chosen)
        if start + remaining_slots > n:
            return None
        max_possible_add = prefix_sum[start + remaining_slots] - prefix_sum[start]
        if current_sum + max_possible_add < target - eps:
            return None
        for i in range(start, n):
            tx = cands_sorted[i]
            if tx.matched:
                continue
            chosen.append(i)
            res = dfs(
                i + 1,
                depth_limit,
                chosen,
                current_sum + abs(tx.amount),
                nodes_ref,
            )
            if res is not None:
                return res
            chosen.pop()
        return None

    for depth in range(1, max_len + 1):
        nodes_ref = [0]
        res_idx = dfs(0, depth, [], Decimal("0"), nodes_ref)
        if res_idx is not None:
            return [cands_sorted[i] for i in res_idx]
    return None

def _match_many_to_many_by_signature(
    txs: List[Tx],
    api_reset: pd.DataFrame,
    erp_reset: pd.DataFrame,
    eps: Decimal = Decimal("0.01"),
) -> List[Tuple[List[int], List[int]]]:
    matches: List[Tuple[List[int], List[int]]] = []

    api_keywords: List[List[str]] = []
    for _, row in api_reset.iterrows():
        txt = (row.get("descriptionraw") or "")
        api_keywords.append(extract_keywords(txt, max_keywords=10))

    erp_keywords: List[List[str]] = []
    for _, row in erp_reset.iterrows():
        base_txt = (row.get("description_client") or "")
        fav_txt  = (row.get("favorecido") or "")
        txt = f"{base_txt} {fav_txt}".strip()
        erp_keywords.append(extract_keywords(txt, max_keywords=10))

    def get_signature(tx: Tx) -> str:
        if tx.side == "api":
            kws = api_keywords[tx.idx]
        else:
            kws = erp_keywords[tx.idx]
        if not kws:
            return ""
        return "|".join(kws[:3])

    clusters: dict[tuple, dict[str, List[int]]] = {}

    for i, tx in enumerate(txs):
        if tx.matched or tx.sign == 0:
            continue
        if not tx.bank_code or not tx.acc_norm:
            continue

        sig = get_signature(tx)
        if not sig:
            continue

        key = (tx.tenant_id, tx.bank_code, tx.acc_norm, tx.work_date, tx.sign, sig)
        if key not in clusters:
            clusters[key] = {"api": [], "erp": []}
        clusters[key][tx.side].append(i)

    for key, sides in clusters.items():
        api_idxs = [i for i in sides["api"] if not txs[i].matched]
        erp_idxs = [i for i in sides["erp"] if not txs[i].matched]
        if not api_idxs or not erp_idxs:
            continue
        total_api = sum(abs(txs[i].amount) for i in api_idxs)
        total_erp = sum(abs(txs[i].amount) for i in erp_idxs)
        if abs(total_api - total_erp) <= eps:
            for i in api_idxs + erp_idxs:
                txs[i].matched = True
            matches.append((erp_idxs, api_idxs))
    return matches

def _match_full_group_by_description(
    txs: List[Tx],
    api_reset: pd.DataFrame,
    erp_reset: pd.DataFrame,
    eps: Decimal = Decimal("0.01"),
    min_abs_amount: Decimal = Decimal("100000"),
    min_keyword_intersection: int = 2,
) -> List[Tuple[int, List[int]]]:
    matches: List[Tuple[int, List[int]]] = []

    api_keywords: List[List[str]] = []
    for _, row in api_reset.iterrows():
        txt = (row.get("descriptionraw") or "")
        api_keywords.append(extract_keywords(txt, max_keywords=10))

    erp_keywords: List[List[str]] = []
    for _, row in erp_reset.iterrows():
        base_txt = (row.get("description_client") or "")
        fav_txt  = (row.get("favorecido") or "")
        txt = f"{base_txt} {fav_txt}".strip()
        erp_keywords.append(extract_keywords(txt, max_keywords=10))

    anchor_indices = sorted(
        [
            i for i, tx in enumerate(txs)
            if (
                tx.side == "api" and not tx.matched and tx.sign != 0
                and abs(tx.amount) >= min_abs_amount
            )
        ],
        key=lambda i: abs(txs[i].amount),
        reverse=True,
    )

    for i in anchor_indices:
        anchor = txs[i]
        anchor_kws = set(api_keywords[anchor.idx])
        if not anchor_kws:
            continue

        allowed_dates = set(candidate_dates(anchor.work_date))

        erp_idxs: List[int] = []
        for j, tx in enumerate(txs):
            if tx.matched or tx.side != "erp" or tx.sign != anchor.sign:
                continue
            if tx.work_date not in allowed_dates:
                continue

            # Nunca misturar bancos nem contas diferentes
            if tx.tenant_id != anchor.tenant_id:
                continue
            if tx.bank_code != anchor.bank_code:
                continue
            if anchor.acc_norm and tx.acc_norm and tx.acc_norm != anchor.acc_norm:
                continue

            kws_erp = set(erp_keywords[tx.idx])
            if not kws_erp:
                continue
            inter = anchor_kws.intersection(kws_erp)
            if len(inter) < min_keyword_intersection:
                continue
            erp_idxs.append(j)

        if not erp_idxs:
            continue

        total_erp = sum(abs(txs[j].amount) for j in erp_idxs)
        if abs(total_erp - abs(anchor.amount)) <= eps:
            anchor.matched = True
            for j in erp_idxs:
                txs[j].matched = True
            matches.append((i, erp_idxs))

    return matches

def _match_one_to_many_by_description(
    txs: List[Tx],
    api_reset: pd.DataFrame,
    erp_reset: pd.DataFrame,
    eps: Decimal = Decimal("0.01"),
    max_len: int = 25,
    min_abs_amount: Decimal = Decimal("100000"),
    max_nodes: int = 200_000,
    min_keyword_intersection: int = 2,
) -> List[Tuple[int, List[int]]]:
    matches: List[Tuple[int, List[int]]] = []

    api_keywords: List[List[str]] = []
    for _, row in api_reset.iterrows():
        txt = (row.get("descriptionraw") or "")
        api_keywords.append(extract_keywords(txt, max_keywords=10))

    erp_keywords: List[List[str]] = []
    for _, row in erp_reset.iterrows():
        base_txt = (row.get("description_client") or "")
        fav_txt  = (row.get("favorecido") or "")
        txt = f"{base_txt} {fav_txt}".strip()
        erp_keywords.append(extract_keywords(txt, max_keywords=10))

    def get_keywords(tx: Tx) -> List[str]:
        if tx.side == "api":
            return api_keywords[tx.idx]
        else:
            return erp_keywords[tx.idx]

    order = sorted(
        range(len(txs)),
        key=lambda i: abs(txs[i].amount),
        reverse=True
    )

    for i in order:
        anchor = txs[i]
        if anchor.matched or anchor.sign == 0:
            continue
        if abs(anchor.amount) < min_abs_amount:
            break

        anchor_kws = set(get_keywords(anchor))
        if len(anchor_kws) < min_keyword_intersection:
            continue

        other_side = "api" if anchor.side == "erp" else "erp"
        allowed_dates = {anchor.work_date}
        cands: List[Tx] = []
        for tx in txs:
            if tx.matched or tx.side != other_side or tx.sign != anchor.sign:
                continue
            if tx.work_date not in allowed_dates:
                continue

            # Nunca misturar bancos nem contas diferentes
            if tx.tenant_id != anchor.tenant_id:
                continue
            if tx.bank_code != anchor.bank_code:
                continue
            if anchor.acc_norm and tx.acc_norm and tx.acc_norm != anchor.acc_norm:
                continue

            cand_kws = set(get_keywords(tx))
            if len(cand_kws) < min_keyword_intersection:
                continue
            inter = anchor_kws.intersection(cand_kws)
            if len(inter) < min_keyword_intersection:
                continue
            cands.append(tx)

        if not cands:
            continue

        target = abs(anchor.amount)
        group = _subset_sum_for_anchor(
            target=target,
            candidates=cands,
            max_len=max_len,
            eps=eps,
            max_nodes=max_nodes,
        )
        if group:
            anchor.matched = True
            group_idxs: List[int] = []
            for g in group:
                for j, original in enumerate(txs):
                    if original is g:
                        original.matched = True
                        group_idxs.append(j)
                        break
            matches.append((i, group_idxs))

    return matches

def reconcile_by_description(
    api_df: pd.DataFrame,
    erp_df: pd.DataFrame,
    eps: float = 0.01,
    desc_min_amount: float = 100000.0,
    desc_max_group_size: int = 25,
    desc_min_keywords: int = 2,
) -> pd.DataFrame:
    """
    Apenas estágios por descrição:
      - 01_DESC_MN_SIGNATURE
      - 02_DESC_FULL_1N
      - 03_DESC_KSUM_1N / 03_DESC_KSUM_N1
    Retorna matches_df semelhante ao código Sicredi original.
    """
    dec_eps = Decimal(str(eps))
    dec_desc_min_amount = Decimal(str(desc_min_amount))

    txs, api_reset, erp_reset = _build_txs(api_df, erp_df)

    many_to_many_sig = _match_many_to_many_by_signature(
        txs, api_reset=api_reset, erp_reset=erp_reset, eps=dec_eps
    )
    full_desc_matches = _match_full_group_by_description(
        txs, api_reset=api_reset, erp_reset=erp_reset,
        eps=dec_eps,
        min_abs_amount=dec_desc_min_amount,
        min_keyword_intersection=desc_min_keywords,
    )
    desc_matches = _match_one_to_many_by_description(
        txs, api_reset=api_reset, erp_reset=erp_reset,
        eps=dec_eps,
        max_len=desc_max_group_size,
        min_abs_amount=dec_desc_min_amount,
        max_nodes=200_000,
        min_keyword_intersection=desc_min_keywords,
    )

    records: List[dict] = []

    def add_group(
        match_type: str,
        erp_idxs: List[int],
        api_idxs: List[int],
        group_id: int,
    ):
        erp_keys: List[str] = []
        api_keys: List[str] = []

        for i in erp_idxs:
            tx = txs[i]
            if tx.side != "erp":
                continue
            row = erp_reset.iloc[tx.idx]
            erp_keys.append(str(row["cd_lancamento"]))

        for i in api_idxs:
            tx = txs[i]
            if tx.side != "api":
                continue
            row = api_reset.iloc[tx.idx]
            api_keys.append(str(row["id"]))

        erp_keys_str = ",".join(sorted(set(erp_keys)))
        api_keys_str = ",".join(sorted(set(api_keys)))

        # ERP side records
        for i in erp_idxs:
            tx = txs[i]
            if tx.side != "erp":
                continue
            row = erp_reset.iloc[tx.idx]
            records.append(
                {
                    "match_group_id": group_id,
                    "match_type": match_type,
                    "side": "erp",
                    "cd_lancamento": row["cd_lancamento"],
                    "nr_documento": row.get("nr_documento"),
                    "date_br": row["date_br"],
                    "amount": float(row["amount_client"]),
                    "matched_api_ids": api_keys_str,
                    "raw": row.to_dict(),
                }
            )

        # API side records
        for i in api_idxs:
            tx = txs[i]
            if tx.side != "api":
                continue
            row = api_reset.iloc[tx.idx]
            records.append(
                {
                    "match_group_id": group_id,
                    "match_type": match_type,
                    "side": "api",
                    "id": row["id"],
                    "descriptionraw": row.get("descriptionraw"),
                    "date": row["date"],
                    "amount": float(row["amount"]),
                    "matched_erp_cd_lanc": erp_keys_str,
                    "raw": row.to_dict(),
                }
            )

    group_id = 1
    for erp_idxs, api_idxs in many_to_many_sig:
        add_group("01_DESC_MN_SIGNATURE", erp_idxs, api_idxs, group_id)
        group_id += 1

    for api_idx, erp_idxs in full_desc_matches:
        add_group("02_DESC_FULL_1N", erp_idxs, [api_idx], group_id)
        group_id += 1

    for anchor_idx, other_indices in desc_matches:
        anchor = txs[anchor_idx]
        if anchor.side == "erp":
            erp_idxs = [anchor_idx]
            api_idxs = other_indices
            mtype = "03_DESC_KSUM_1N"
        else:
            erp_idxs = other_indices
            api_idxs = [anchor_idx]
            mtype = "03_DESC_KSUM_N1"
        add_group(mtype, erp_idxs, api_idxs, group_id)
        group_id += 1

    matches_df = pd.DataFrame(records)
    return matches_df

def build_desc_edges(
    A0: pl.DataFrame,
    E0: pl.DataFrame,
    matches_df: pd.DataFrame
) -> Dict[str, pl.DataFrame]:
    """
    Converte matches_df (pandas) em pares (api_row_id, erp_row_id) por tipo de estágio.
    Só considera os tipos 01/02/03.
    """
    if matches_df is None or matches_df.empty:
        return {}

    api_map: Dict[str, int] = {}
    for uid, rid, bank in zip(A0["api_uid"].to_list(), A0["api_row_id"].to_list(), A0["bank_code"].to_list()):
        api_map[str(uid)] = int(rid)

    erp_map: Dict[str, int] = {}
    for uid, rid, bank in zip(E0["erp_uid"].to_list(), E0["erp_row_id"].to_list(), E0["bank_code"].to_list()):
        erp_map[str(uid)] = int(rid)

    edges_by_type: Dict[str, List[tuple[int, int]]] = defaultdict(list)

    for gid, grp in matches_df.groupby("match_group_id"):
        if "match_type" not in grp.columns:
            continue
        match_type = str(grp["match_type"].iloc[0])
        if match_type not in {
            "01_DESC_MN_SIGNATURE",
            "02_DESC_FULL_1N",
            "03_DESC_KSUM_1N",
            "03_DESC_KSUM_N1",
        }:
            continue

        api_ids = (
            grp.loc[grp["side"] == "api", "id"]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
            if "id" in grp.columns
            else []
        )
        erp_ids = (
            grp.loc[grp["side"] == "erp", "cd_lancamento"]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
            if "cd_lancamento" in grp.columns
            else []
        )
        if not api_ids or not erp_ids:
            continue

        for aid in api_ids:
            a_row = api_map.get(str(aid))
            if a_row is None:
                continue
            for eid in erp_ids:
                e_row = erp_map.get(str(eid))
                if e_row is None:
                    continue
                edges_by_type[match_type].append((a_row, e_row))

    out: Dict[str, pl.DataFrame] = {}
    for mt, pairs in edges_by_type.items():
        out[mt] = pl.DataFrame(
            pairs,
            schema=[("api_row_id", pl.Int64), ("erp_row_id", pl.Int64)],
            orient="row",
        )
    return out

# ========================= TRANSFORM =========================
def transform(
    A0: pl.DataFrame,
    E0: pl.DataFrame,
    desc_edges_by_type: Optional[Dict[str, pl.DataFrame]] = None
) -> dict[str, pl.DataFrame]:
    if A0.is_empty() and E0.is_empty():
        return {k: pl.DataFrame() for k in ["matches", "unrec_api", "unrec_erp", "daily", "monthly"]}

    cal = build_calendar(DATE_FROM, DATE_TO, 15)

    bank_meta = (
        pl.concat([
            A0.select(["tenant_id", "bank_code", "bank_name"]),
            E0.select(["tenant_id", "bank_code", "bank_name"]),
        ], how="vertical")
        .unique(subset=["tenant_id", "bank_code"])
    )

    A = (
        A0.join(cal.rename({"cal_date": "api_date"}), on="api_date", how="left")
          .rename({"biz_ord": "api_biz_ord"})
          .with_columns([
              pl.coalesce([
                  pl.col("api_desc_norm").str.extract(r"DOCTO\s+(\d{5,8})", 1),
                  pl.col("api_desc_norm").str.extract(r"DOC\s+(\d{5,8})", 1),
              ])
              .map_elements(normalize_doc_key_str, return_dtype=pl.Utf8)
              .alias("api_doc_key")
          ])
    )

    E = (
        E0.join(cal.rename({"cal_date": "erp_date"}), on="erp_date", how="left")
          .rename({"biz_ord": "erp_biz_ord"})
          .with_columns([
              pl.coalesce([
                  pl.col("erp_desc_norm").str.extract(r"DOC\s+(\d{5,8})", 1),
                  pl.col("erp_desc_norm").str.extract(r"DOCTO\s+(\d{5,8})", 1),
              ])
              .map_elements(normalize_doc_key_str, return_dtype=pl.Utf8)
              .alias("erp_doc_key")
          ])
    )

    matches_parts: list[pl.DataFrame] = []

    # ================== FUNÇÃO consume COM DEBUG ==================
    def consume(tag_df: pl.DataFrame, stage: str, prio: int,
                ddiff_val: int | None = None):
        nonlocal A, E, matches_parts

        if tag_df.is_empty():
            return

        # --- lógica normal do consume (igual estava antes) ---
        cols = ["api_row_id", "erp_row_id"]
        has_dd = "ddiff" in tag_df.columns
        sel = cols + (["ddiff"] if has_dd else [])
        x = tag_df.select(sel).unique()

        if has_dd and ddiff_val is None:
            add = x.with_columns([
                pl.lit(stage).alias("stage"),
                pl.lit(int(prio)).alias("prio"),
            ])
        else:
            add = (
                x.select(cols)
                 .with_columns([
                     pl.lit(stage).alias("stage"),
                     pl.lit(int(prio)).alias("prio"),
                     pl.lit(int(ddiff_val if ddiff_val is not None else 0))
                       .cast(pl.Int64).alias("ddiff"),
                 ])
            )
        matches_parts.append(add.select(
            ["api_row_id", "erp_row_id", "stage", "prio", "ddiff"]
        ))

        # consome p/ próximos estágios
        A = A.join(x.select("api_row_id").unique(), on="api_row_id", how="anti")
        E = E.join(x.select("erp_row_id").unique(), on="erp_row_id", how="anti")

    # ---------- M0 TAX D-1 (RN 1x1 centavos) ----------
    A_tax = (
        A.filter(pl.col("api_is_tax"))
         .with_columns(pl.col("api_cents").alias("cents"))
         .sort(by=["tenant_id","bank_code","api_acc_tail","api_sign","api_date","cents","api_row_id"])
         .with_columns(
             pl.arange(1, pl.len() + 1)
               .over(["tenant_id","bank_code","api_acc_tail","api_sign","api_date","cents"])
               .alias("rn")
         )
         .select(["api_row_id","tenant_id","bank_code","api_acc_tail","api_sign",
                  "api_date","cents","rn"])
    )
    E_tax = (
        E.with_columns(pl.col("erp_cents").alias("cents"))
         .sort(by=["tenant_id","bank_code","erp_acc_tail","erp_sign","erp_date","cents","erp_row_id"])
         .with_columns(
             pl.arange(1, pl.len() + 1)
               .over(["tenant_id","bank_code","erp_acc_tail","erp_sign","erp_date","cents"])
               .alias("rn")
         )
         .select(["erp_row_id","tenant_id","bank_code","erp_acc_tail","erp_sign","erp_date","cents","rn"])
    )
    j_tax = join_pairs(
        A_tax, E_tax,
        [
            ("tenant_id","tenant_id"),
            ("bank_code","bank_code"),
            ("api_acc_tail","erp_acc_tail"),
            ("api_sign","erp_sign"),
            ("api_date","erp_date"),
            ("cents","cents"),
            ("rn","rn"),
        ],
    ).select(["api_row_id","erp_row_id"])

    consume(j_tax, "M0_TAX_DMINUS1_RN_1TO1", 5, ddiff_val=1)

    # ---------- M0 BANK FEES D-1 ----------
    A_bf = (
        A.filter(pl.col("api_is_bankfees"))
         .with_columns(pl.col("api_cents").alias("cents"))
         .sort(by=["tenant_id","bank_code","api_acc_tail","api_sign","api_date","cents","api_row_id"])
         .with_columns(
             pl.arange(1, pl.len() + 1)
               .over(["tenant_id","bank_code","api_acc_tail","api_sign","api_date","cents"])
               .alias("rn")
         )
         .select(["api_row_id","tenant_id","bank_code","api_acc_tail","api_sign","api_date","cents","rn"])
    )
    E_bf = (
        E.with_columns(pl.col("erp_cents").alias("cents"))
         .sort(by=["tenant_id","bank_code","erp_acc_tail","erp_sign","erp_date","cents","erp_row_id"])
         .with_columns(
             pl.arange(1, pl.len() + 1)
               .over(["tenant_id","bank_code","erp_acc_tail","erp_sign","erp_date","cents"])
               .alias("rn")
         )
         .select(["erp_row_id","tenant_id","bank_code","erp_acc_tail","erp_sign","erp_date","cents","rn"])
    )
    j_bf = join_pairs(
        A_bf, E_bf,
        [
            ("tenant_id","tenant_id"),
            ("bank_code","bank_code"),
            ("api_acc_tail","erp_acc_tail"),
            ("api_sign","erp_sign"),
            ("api_date","erp_date"),
            ("cents","cents"),
            ("rn","rn"),
        ],
    ).select(["api_row_id","erp_row_id"])
    consume(j_bf, "M0_BANKFEES_DMINUS1_RN_1TO1", 6, ddiff_val=1)

        # ---------- M0 RENT D-1 (RENDIMENTO_APLIC_FINANCEIRA, Itaú) ----------
    A_rent_d1 = (
        A.filter(pl.col("api_is_rent_d1"))
         .with_columns(pl.col("api_cents").alias("cents"))
         .sort(by=["tenant_id","bank_code","api_acc_tail","api_sign","api_date","cents","api_row_id"])
         .with_columns(
             pl.arange(1, pl.len() + 1)
               .over(["tenant_id","bank_code","api_acc_tail","api_sign","api_date","cents"])
               .alias("rn")
         )
         .select([
             "api_row_id","tenant_id","bank_code","api_acc_tail","api_sign",
             "api_date","cents","rn",
         ])
    )
    E_rent_d1 = (
        E.with_columns(pl.col("erp_cents").alias("cents"))
         .sort(by=["tenant_id","bank_code","erp_acc_tail","erp_sign","erp_date","cents","erp_row_id"])
         .with_columns(
             pl.arange(1, pl.len() + 1)
               .over(["tenant_id","bank_code","erp_acc_tail","erp_sign","erp_date","cents"])
               .alias("rn")
         )
         .select([
             "erp_row_id","tenant_id","bank_code","erp_acc_tail","erp_sign",
             "erp_date","cents","rn",
         ])
    )
    j_rent_d1 = join_pairs(
        A_rent_d1, E_rent_d1,
        [
            ("tenant_id","tenant_id"),
            ("bank_code","bank_code"),
            ("api_acc_tail","erp_acc_tail"),
            ("api_sign","erp_sign"),
            ("api_date","erp_date"),
            ("cents","cents"),
            ("rn","rn"),
        ],
    ).select(["api_row_id","erp_row_id"])

    consume(j_rent_d1, "M0_RENT_DMINUS1_RN_1TO1", 7, ddiff_val=1)

    # ---------- ESTÁGIOS POR DESCRIÇÃO (01/02/03) ----------
    if desc_edges_by_type:
        api_ids_alive = A["api_row_id"].unique() if not A.is_empty() else pl.Series([], dtype=pl.Int64)
        erp_ids_alive = E["erp_row_id"].unique() if not E.is_empty() else pl.Series([], dtype=pl.Int64)

        for mt in DESC_STAGE_ORDER:
            edges_df = desc_edges_by_type.get(mt)
            if edges_df is None or edges_df.is_empty():
                continue

            df_stage = edges_df
            if not A.is_empty() and not E.is_empty():
                df_stage = df_stage.filter(
                    pl.col("api_row_id").is_in(api_ids_alive)
                    & pl.col("erp_row_id").is_in(erp_ids_alive)
                )

            if df_stage.is_empty():
                continue

            consume(df_stage, mt, DESC_STAGE_PRIO.get(mt, 9), ddiff_val=0)

            api_ids_alive = A["api_row_id"].unique() if not A.is_empty() else pl.Series([], dtype=pl.Int64)
            erp_ids_alive = E["erp_row_id"].unique() if not E.is_empty() else pl.Series([], dtype=pl.Int64)

    # ---------- M1 mesmo dia (RN 1x1 centavos) ----------
    A_m1 = (
        A.with_columns(pl.col("api_cents").alias("cents"))
         .sort(by=["tenant_id","bank_code","api_acc_tail","api_sign","api_date","cents","api_row_id"])
         .with_columns(
             pl.arange(1, pl.len() + 1)
               .over(["tenant_id","bank_code","api_acc_tail","api_sign","api_date","cents"])
               .alias("rn")
         )
         .select(["api_row_id","tenant_id","bank_code","api_acc_tail","api_sign","api_date","cents","rn"])
    )
    E_m1 = (
        E.with_columns(pl.col("erp_cents").alias("cents"))
         .sort(by=["tenant_id","bank_code","erp_acc_tail","erp_sign","erp_date","cents","erp_row_id"])
         .with_columns(
             pl.arange(1, pl.len() + 1)
               .over(["tenant_id","bank_code","erp_acc_tail","erp_sign","erp_date","cents"])
               .alias("rn")
         )
         .select(["erp_row_id","tenant_id","bank_code","erp_acc_tail","erp_sign","erp_date","cents","rn"])
    )
    j_m1 = join_pairs(
        A_m1, E_m1,
        [
            ("tenant_id","tenant_id"),
            ("bank_code","bank_code"),
            ("api_acc_tail","erp_acc_tail"),
            ("api_sign","erp_sign"),
            ("api_date","erp_date"),
            ("cents","cents"),
            ("rn","rn"),
        ],
    ).select(["api_row_id","erp_row_id"])
    consume(j_m1, "M1_SAME_DAY_RN", 10, ddiff_val=0)

    # ---------- KSUM SAME-DAY (N:1 e 1:N) ----------
    if not A.is_empty() and not E.is_empty():
        A_k = A.filter(~pl.col("api_is_rent")).select([
            pl.col("api_row_id"),
            pl.lit(None, dtype=pl.Int64).alias("erp_row_id"),
            "tenant_id",
            "bank_code",
            pl.col("api_acc_tail").alias("acc_tail"),
            pl.col("api_date").alias("date"),
            pl.col("api_sign").alias("sign"),
            pl.col("api_amount").alias("amount"),
            pl.lit("API").alias("side"),
            pl.col("api_uid").alias("api_uid"),
            pl.lit(None, dtype=pl.Utf8).alias("erp_uid"),
        ])
        E_k = E.select([
            pl.lit(None, dtype=pl.Int64).alias("api_row_id"),
            pl.col("erp_row_id"),
            "tenant_id",
            "bank_code",
            pl.col("erp_acc_tail").alias("acc_tail"),
            pl.col("erp_date").alias("date"),
            pl.col("erp_sign").alias("sign"),
            pl.col("erp_amount").alias("amount"),
            pl.lit("ERP").alias("side"),
            pl.lit(None, dtype=pl.Utf8).alias("api_uid"),
            pl.col("erp_uid").alias("erp_uid"),
        ])
        kdf_same = pl.concat([A_k, E_k], how="vertical")
        same_keys = ["tenant_id","bank_code","acc_tail","sign","date"]
        m2_same = apply_per_group(kdf_same, same_keys, solve_same_group)
    else:
        m2_same = pl.DataFrame({"api_row_id": [], "erp_row_id": []})
    consume(m2_same, "M2_KSUM_SAME_DAY", 20, ddiff_val=0)

    # ---------- 07 FALLBACK BALANCE DAY (N:M por saldo diário) ----------
    if not A.is_empty() and not E.is_empty():
        A_fb = A.select([
            "api_row_id",
            "tenant_id",
            "bank_code",
            pl.col("api_acc_tail").alias("acc_tail"),
            pl.col("api_date").alias("date"),
            pl.col("api_sign").alias("sign"),
            pl.col("api_cents").alias("cents"),
        ])
        E_fb = E.select([
            "erp_row_id",
            "tenant_id",
            "bank_code",
            pl.col("erp_acc_tail").alias("acc_tail"),
            pl.col("erp_date").alias("date"),
            pl.col("erp_sign").alias("sign"),
            pl.col("erp_cents").alias("cents"),
        ])

        # soma por lado, por (tenant, banco, conta, data, sinal)
        A_sum = (
            A_fb
            .group_by(["tenant_id", "bank_code", "acc_tail", "date", "sign"])
            .agg(pl.col("cents").sum().alias("sum_api_cents"))
        )
        E_sum = (
            E_fb
            .group_by(["tenant_id", "bank_code", "acc_tail", "date", "sign"])
            .agg(pl.col("cents").sum().alias("sum_erp_cents"))
        )

        # grupos onde o saldo do dia bate exatamente
        fb_keys = (
            join_pairs(
                A_sum,
                E_sum,
                [
                    ("tenant_id", "tenant_id"),
                    ("bank_code", "bank_code"),
                    ("acc_tail", "acc_tail"),
                    ("date", "date"),
                    ("sign", "sign"),
                ]
            )
            .filter(pl.col("sum_api_cents") == pl.col("sum_erp_cents"))
            .select(["tenant_id", "bank_code", "acc_tail", "date", "sign"])
            .unique()
        )

        if not fb_keys.is_empty():
            # dá um id de grupo para cada chave (saldo diário)
            fb_keys = fb_keys.with_columns(
                pl.arange(0, pl.len()).cast(pl.Int64).alias("fb_group_id")
            )

            A_g = join_pairs(
                A_fb,
                fb_keys,
                [
                    ("tenant_id", "tenant_id"),
                    ("bank_code", "bank_code"),
                    ("acc_tail", "acc_tail"),
                    ("date", "date"),
                    ("sign", "sign"),
                ]
            )
            E_g = join_pairs(
                E_fb,
                fb_keys,
                [
                    ("tenant_id", "tenant_id"),
                    ("bank_code", "bank_code"),
                    ("acc_tail", "acc_tail"),
                    ("date", "date"),
                    ("sign", "sign"),
                ]
            )

            # produto cartesiano dentro de cada grupo (N:M)
            fb_edges = (
                A_g.select(["fb_group_id", "api_row_id"])
                   .join(
                       E_g.select(["fb_group_id", "erp_row_id"]),
                       on="fb_group_id",
                       how="inner",
                   )
                   .select(["api_row_id", "erp_row_id"])
                   .unique()
            )
        else:
            fb_edges = pl.DataFrame({"api_row_id": [], "erp_row_id": []})
    else:
        fb_edges = pl.DataFrame({"api_row_id": [], "erp_row_id": []})

    # prioridade alta, depois de todos os KSUMs
    consume(fb_edges, "07_FALLBACK_BALANCE_DAY", 30, ddiff_val=0)

    # ---------- UNION matches (pré-validação) ----------
    if matches_parts:
        matches = pl.concat(matches_parts)
    else:
        matches = pl.DataFrame(
            schema=[
                ("api_row_id",pl.Int64),
                ("erp_row_id",pl.Int64),
                ("stage",pl.Utf8),
                ("prio",pl.Int64),
                ("ddiff",pl.Int64),
            ]
        )
    matches = matches.unique(subset=["api_row_id","erp_row_id"])

    # ---------- VALIDAÇÃO POR COMPONENTE ----------
    matches = finalize_by_components(matches, A0, E0)

    # ---------- UNRECONCILED ----------
    unrec_api = (
        A0.join(matches.select(["api_row_id"]).unique(),
                on="api_row_id", how="anti")
          .select([
              "tenant_id",
              pl.col("api_acc_tail").alias("acc_tail"),
              pl.col("api_date").alias("date"),
              pl.col("api_amount").alias("amount"),
              pl.col("api_uid").alias("api_id"),
              pl.col("api_desc_norm").alias("desc_norm"),
              "bank_code",
          ])
    )
    unrec_erp = (
        E0.join(matches.select(["erp_row_id"]).unique(),
                on="erp_row_id", how="anti")
          .select([
              "tenant_id",
              pl.col("erp_acc_tail").alias("acc_tail"),
              pl.col("erp_date").alias("date"),
              pl.col("erp_amount").alias("amount"),
              pl.col("erp_uid").alias("cd_lancamento"),
              pl.col("erp_desc_norm").alias("desc_norm"),
              "bank_code",
          ])
    )

    # ---------- DAILY / MONTHLY ----------
    A1 = A0.select(["api_row_id","tenant_id","api_acc_tail","api_amount","bank_code"])
    E1 = E0.select(["erp_row_id","tenant_id","erp_acc_tail","erp_date","erp_amount","bank_code"])

    pair = (
        matches
        .join(
            A1.with_columns([
                pl.col("api_amount").abs().alias("api_amt_abs"),
                pl.col("api_acc_tail").alias("acc_tail"),
            ]),
            on="api_row_id",
            how="inner",
        )
        .join(
            E1.with_columns([
                pl.col("erp_amount").abs().alias("erp_amt_abs"),
                pl.col("erp_date").alias("date"),
            ]),
            on="erp_row_id",
            how="inner",
        )
    )

    if pair.is_empty():
        daily = pl.DataFrame(
            schema=[
                ("tenant_id", pl.Utf8),
                ("bank_code", pl.Utf8),
                ("bank_name", pl.Utf8),
                ("acc_tail", pl.Utf8),
                ("date", pl.Date),
                ("api_matched_abs", pl.Float64),
                ("erp_matched_abs", pl.Float64),
                ("api_unrec_abs", pl.Float64),
                ("erp_unrec_abs", pl.Float64),
                ("unrec_total_abs", pl.Float64),
                ("unrec_diff", pl.Float64),
            ]
        )
        monthly = pl.DataFrame(
            schema=[
                ("tenant_id", pl.Utf8),
                ("bank_code", pl.Utf8),
                ("bank_name", pl.Utf8),
                ("acc_tail", pl.Utf8),
                ("month", pl.Date),
                ("api_matched_abs", pl.Float64),
                ("erp_matched_abs", pl.Float64),
                ("api_unrec_abs", pl.Float64),
                ("erp_unrec_abs", pl.Float64),
                ("unrec_total_abs", pl.Float64),
            ]
        )
    else:
        weights = pair.group_by("api_row_id").agg(
            pl.col("erp_amt_abs").sum().alias("sum_erp_abs")
        )
        pair_w = (
            pair.join(weights, on="api_row_id", how="inner")
                .with_columns(
                    pl.when(pl.col("sum_erp_abs") > 0)
                      .then((pl.col("erp_amt_abs") / pl.col("sum_erp_abs")) * pl.col("api_amt_abs"))
                      .otherwise(0.0)
                      .round(2)
                      .alias("api_contrib_abs")
                )
        )

        m_api_daily = (
            pair_w.group_by(["tenant_id","bank_code","acc_tail","date"])
                  .agg(
                      pl.col("api_contrib_abs").sum().round(2).alias("api_matched_abs")
                  )
        )
        m_erp_daily = (
            matches.select("erp_row_id").unique()
                   .join(
                       E1.with_columns([
                           pl.col("erp_amount").abs().alias("erp_amt_abs"),
                           pl.col("erp_date").alias("date"),
                           pl.col("erp_acc_tail").alias("acc_tail"),
                       ]),
                       on="erp_row_id",
                       how="inner",
                   )
                   .group_by(["tenant_id","bank_code","acc_tail","date"])
                   .agg(
                       pl.col("erp_amt_abs").sum().round(2).alias("erp_matched_abs")
                   )
        )
        u_api_daily = (
            unrec_api.group_by(["tenant_id","bank_code","acc_tail","date"])
                     .agg(
                         pl.col("amount").abs().sum().round(2).alias("api_unrec_abs")
                     )
        )
        u_erp_daily = (
            unrec_erp.group_by(["tenant_id","bank_code","acc_tail","date"])
                     .agg(
                         pl.col("amount").abs().sum().round(2).alias("erp_unrec_abs")
                     )
        )

        spine_dim = (
            pl.concat([
                m_api_daily.select(["tenant_id","bank_code","acc_tail"]).unique(),
                m_erp_daily.select(["tenant_id","bank_code","acc_tail"]).unique(),
                unrec_api.select(["tenant_id","bank_code","acc_tail"]).unique(),
                unrec_erp.select(["tenant_id","bank_code","acc_tail"]).unique(),
            ]).unique()
        )
        dates = pl.date_range(
            dt.date.fromisoformat(DATE_FROM),
            dt.date.fromisoformat(DATE_TO),
            "1d",
            eager=True,
        ).to_frame("date")
        spine = spine_dim.join(dates, how="cross")

        daily = (
            spine
            .join(m_api_daily, on=["tenant_id","bank_code","acc_tail","date"], how="left")
            .join(m_erp_daily, on=["tenant_id","bank_code","acc_tail","date"], how="left")
            .join(u_api_daily, on=["tenant_id","bank_code","acc_tail","date"], how="left")
            .join(u_erp_daily, on=["tenant_id","bank_code","acc_tail","date"], how="left")
            .with_columns([
                pl.col("api_matched_abs").fill_null(0.0),
                pl.col("erp_matched_abs").fill_null(0.0),
                pl.col("api_unrec_abs").fill_null(0.0),
                pl.col("erp_unrec_abs").fill_null(0.0),
            ])
            .with_columns(
                (pl.col("api_unrec_abs") + pl.col("erp_unrec_abs"))
                .round(2)
                .alias("unrec_total_abs")
            )
            .with_columns(
                (pl.col("erp_unrec_abs") - pl.col("api_unrec_abs"))
                .round(2)
                .alias("unrec_diff")
            )
        )

        monthly = (
            daily
            .with_columns(pl.col("date").dt.truncate("1mo").alias("month"))
            .group_by(["tenant_id","bank_code","acc_tail","month"])
            .agg([
                pl.col("api_matched_abs").sum().round(2).alias("api_matched_abs"),
                pl.col("erp_matched_abs").sum().round(2).alias("erp_matched_abs"),
                pl.col("api_unrec_abs").sum().round(2).alias("api_unrec_abs"),
                pl.col("erp_unrec_abs").sum().round(2).alias("erp_unrec_abs"),
                pl.col("unrec_total_abs").sum().round(2).alias("unrec_total_abs"),
            ])
    )

    unrec_api = unrec_api.join(bank_meta, on=["tenant_id", "bank_code"], how="left")
    unrec_erp = unrec_erp.join(bank_meta, on=["tenant_id", "bank_code"], how="left")
    daily = daily.join(bank_meta, on=["tenant_id", "bank_code"], how="left")
    monthly = monthly.join(bank_meta, on=["tenant_id", "bank_code"], how="left")

    matches_audit = (
        matches
        .join(A0.select(["api_row_id","api_uid"]), on="api_row_id", how="left")
        .join(E0.select(["erp_row_id","erp_uid"]), on="erp_row_id", how="left")
        .select(["api_row_id","erp_row_id","api_uid","erp_uid","stage","prio","ddiff"])
    )

    return {
        "matches": matches_audit,
        "unrec_api": unrec_api,
        "unrec_erp": unrec_erp,
        "daily": daily,
        "monthly": monthly,
    }

# ========================= DDL GOLD =========================
CREATE_MATCHES = """
CREATE TABLE IF NOT EXISTS gold_conciliation_matches (
    api_row_id bigint,
    erp_row_id bigint,
    api_uid text,
    erp_uid text,
    stage text,
    prio int,
    ddiff int
)
"""

CREATE_UNREC_API = """
CREATE TABLE IF NOT EXISTS gold_unreconciled_api (
    tenant_id text,
    acc_tail text,
    date date,
    amount numeric(18,2),
    api_id text,
    desc_norm text,
    bank_code text,
    bank_name text
)
"""

CREATE_UNREC_ERP = """
CREATE TABLE IF NOT EXISTS gold_unreconciled_erp (
    tenant_id text,
    acc_tail text,
    date date,
    amount numeric(18,2),
    cd_lancamento text,
    desc_norm text,
    bank_code text,
    bank_name text
)
"""

CREATE_DAILY = """
CREATE TABLE IF NOT EXISTS gold_conciliation_daily (
    tenant_id text,
    bank_code text,
    bank_name text,
    acc_tail text,
    date date,
    api_matched_abs numeric(18,2),
    erp_matched_abs numeric(18,2),
    api_unrec_abs numeric(18,2),
    erp_unrec_abs numeric(18,2),
    unrec_total_abs numeric(18,2),
    unrec_diff numeric(18,2)
)
"""

CREATE_MONTHLY = """
CREATE TABLE IF NOT EXISTS gold_conciliation_monthly (
    tenant_id text,
    bank_code text,
    bank_name text,
    acc_tail text,
    month date,
    api_matched_abs numeric(18,2),
    erp_matched_abs numeric(18,2),
    api_unrec_abs numeric(18,2),
    erp_unrec_abs numeric(18,2),
    unrec_total_abs numeric(18,2)
)
"""

# ========================= MAIN =========================
def main():
    t0 = time.perf_counter()

    print("[*] Lendo silver (Polars) do Postgres…")
    A0 = load_api_from_pg()
    E0 = load_erp_from_pg()
    print(f" A0={A0.height} | E0={E0.height}")

    print("[DEBUG] ACC_FILTER bruto:", ACC_FILTER)
    print("[DEBUG] A0 (API) por banco/acc_tail:")
    print(
        A0.select(["tenant_id", "bank_code", "api_acc_tail"])
        .unique()
        .sort(["bank_code", "api_acc_tail"])
    )

    print("[DEBUG] E0 (ERP) por banco/acc_tail:")
    print(
        E0.select(["tenant_id", "bank_code", "erp_acc_tail"])
        .unique()
        .sort(["bank_code", "erp_acc_tail"])
    )

    if ENABLE_DESC_STAGES:
        print("[*] Rodando estágios por descrição (pandas)…")
        try:
            with pg_conn() as conn:
                api_df = load_api_df(conn, TENANT, READ_FROM, READ_TO)
                erp_df = load_erp_df(conn, TENANT, READ_FROM, READ_TO)
        except Exception as e:
            print(f"[WARN] Falha ao carregar dados para descrição: {e}")
            matches_desc_df = pd.DataFrame()
            desc_edges_by_type = {}
        else:
            matches_desc_df = reconcile_by_description(
                api_df,
                erp_df,
                eps=0.01,
                desc_min_amount=100000.0,
                desc_max_group_size=25,
                desc_min_keywords=2,
            )
            if not matches_desc_df.empty:
                print(f"  [desc] grupos conciliados (01–03): {matches_desc_df['match_group_id'].nunique()}")
                desc_edges_by_type = build_desc_edges(A0, E0, matches_desc_df)
            else:
                print("  [desc] nenhum grupo conciliado nos estágios 01–03")
                desc_edges_by_type = {}
    else:
        print("[*] Pulando estágios por descrição (ENABLE_DESC_STAGES=False)…")
        desc_edges_by_type = {}

    print("[*] Transformando (Polars + subset-sum MITM + descrição)…")
    gold = transform(A0, E0, desc_edges_by_type)

    print("[*] Gravando gold no Postgres…")
    with pg_conn() as conn:
        df_to_pg(conn, gold["matches"],    "gold_conciliation_matches",  CREATE_MATCHES)
        df_to_pg(conn, gold["unrec_api"],  "gold_unreconciled_api",      CREATE_UNREC_API)
        df_to_pg(conn, gold["unrec_erp"],  "gold_unreconciled_erp",      CREATE_UNREC_ERP)
        df_to_pg(conn, gold["daily"],      "gold_conciliation_daily",    CREATE_DAILY)
        df_to_pg(conn, gold["monthly"],    "gold_conciliation_monthly",  CREATE_MONTHLY)

    print(f"[OK] Concluído em {time.perf_counter() - t0:.2f}s")

if __name__ == "__main__":
    main()
