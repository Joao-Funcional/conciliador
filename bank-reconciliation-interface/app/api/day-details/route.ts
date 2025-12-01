import { NextResponse } from "next/server"
import { query } from "@/lib/db"

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url)
  const tenantId = searchParams.get("tenant")
  const bankCode = searchParams.get("bank")
  const accTail = searchParams.get("accTail")
  const date = searchParams.get("date")

  if (!tenantId || !bankCode || !accTail || !date) {
    return NextResponse.json({ error: "Parâmetros obrigatórios ausentes" }, { status: 400 })
  }

  try {
    const normalizedDate =
      "CASE WHEN date::text ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN date::date ELSE to_date(date::text, 'DD/MM/YYYY') END"

    const unreconciledApi = await query(
      `WITH normalized_api AS (
         SELECT tenant_id,
                bank_code,
                acc_tail,
                ${normalizedDate} AS date,
                COALESCE(amount, 0)::float AS amount,
                api_id,
                desc_norm
         FROM gold_unreconciled_api
         WHERE tenant_id = $1 AND bank_code = $2 AND acc_tail = $3
       )
       SELECT tenant_id, bank_code, acc_tail, to_char(date, 'YYYY-MM-DD') AS date,
              amount, api_id, desc_norm
       FROM normalized_api
       WHERE date = to_date($4, 'YYYY-MM-DD')
       ORDER BY amount DESC`,
      [tenantId, bankCode, accTail, date]
    )

    const unreconciledErp = await query(
      `WITH normalized_erp AS (
         SELECT tenant_id,
                bank_code,
                acc_tail,
                ${normalizedDate} AS date,
                COALESCE(amount, 0)::float AS amount,
                cd_lancamento,
                desc_norm
         FROM gold_unreconciled_erp
         WHERE tenant_id = $1 AND bank_code = $2 AND acc_tail = $3
       )
       SELECT tenant_id, bank_code, acc_tail, to_char(date, 'YYYY-MM-DD') AS date,
              amount, cd_lancamento, desc_norm
       FROM normalized_erp
       WHERE date = to_date($4, 'YYYY-MM-DD')
       ORDER BY amount DESC`,
      [tenantId, bankCode, accTail, date]
    )

    const matches = await query(
      `WITH normalized_daily AS (
         SELECT tenant_id,
                bank_code,
                acc_tail,
                ${normalizedDate} AS date,
                api_uid,
                erp_uid,
                unrec_diff
         FROM gold_conciliation_daily
         WHERE tenant_id = $1 AND bank_code = $2 AND acc_tail = $3
       ),
       matched_pairs AS (
         SELECT api_uid, erp_uid, stage, prio, ddiff
         FROM gold_conciliation_matches
         UNION ALL
         SELECT api_uid, erp_uid, NULL::text AS stage, NULL::int AS prio, NULL::float AS ddiff
         FROM normalized_daily
         WHERE date = to_date($4, 'YYYY-MM-DD')
           AND (api_uid IS NOT NULL OR erp_uid IS NOT NULL)
       )
       SELECT COALESCE(mp.api_uid, mp.erp_uid, '') AS api_uid,
              COALESCE(mp.erp_uid, '') AS erp_uid,
              COALESCE(mp.stage, 'Conciliado') AS stage,
              COALESCE(mp.prio, 0) AS prio,
              COALESCE(mp.ddiff, 0)::float AS ddiff,
              a.amount AS api_amount,
              a.descriptionraw AS api_desc,
              a.date_br::date AS api_date,
              e.amount_client AS erp_amount,
              e.description_client AS erp_desc,
              e.date_br::date AS erp_date
       FROM matched_pairs mp
       LEFT JOIN silver_api_staging a ON mp.api_uid = a.id
       LEFT JOIN silver_erp_staging e ON mp.erp_uid = e.cd_lancamento
       WHERE (a.tenant_id = $1 OR e.tenant_id = $1)
         AND (a.bank_code = $2 OR e.bank_code = $2)
         AND (
              (a.account_number ~ '\\d' AND right(regexp_replace(a.account_number, '\\D', '', 'g'), 8) = $3)
           OR (e.account_number ~ '\\d' AND right(regexp_replace(e.account_number, '\\D', '', 'g'), 8) = $3)
         )
         AND ((a.date_br::date = to_date($4, 'YYYY-MM-DD')) OR (e.date_br::date = to_date($4, 'YYYY-MM-DD')))`,
      [tenantId, bankCode, accTail, date]
    )

    return NextResponse.json({ unreconciledApi, unreconciledErp, matches })
  } catch (error) {
    console.error("Failed to load day details", error)
    return NextResponse.json({ error: "Erro ao carregar detalhes" }, { status: 500 })
  }
}
