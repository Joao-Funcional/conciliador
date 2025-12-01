import { NextResponse } from "next/server"
import { query } from "@/lib/db"

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url)
  const tenantId = searchParams.get("tenant")
  const bankCode = searchParams.get("bank")
  const accTail = searchParams.get("accTail")

  if (!tenantId || !bankCode || !accTail) {
    return NextResponse.json({ error: "Parâmetros obrigatórios ausentes" }, { status: 400 })
  }

  try {
    const monthly = await query(
      `SELECT tenant_id, bank_code, acc_tail, month::text AS month,
              COALESCE(api_matched_abs,0)::float AS api_matched_abs,
              COALESCE(erp_matched_abs,0)::float AS erp_matched_abs,
              COALESCE(api_unrec_abs,0)::float AS api_unrec_abs,
              COALESCE(erp_unrec_abs,0)::float AS erp_unrec_abs,
              COALESCE(unrec_total_abs,0)::float AS unrec_total_abs
       FROM gold_conciliation_monthly
       WHERE tenant_id = $1 AND bank_code = $2 AND acc_tail = $3
       ORDER BY month`,
      [tenantId, bankCode, accTail]
    )

    const daily = await query(
      `WITH all_days AS (
         SELECT tenant_id, bank_code, acc_tail, date::date AS date
         FROM gold_conciliation_daily
         WHERE tenant_id = $1 AND bank_code = $2 AND acc_tail = $3
         UNION
         SELECT tenant_id, bank_code, acc_tail, date::date AS date
         FROM gold_unreconciled_api
         WHERE tenant_id = $1 AND bank_code = $2 AND acc_tail = $3
         UNION
         SELECT tenant_id, bank_code, acc_tail, date::date AS date
         FROM gold_unreconciled_erp
         WHERE tenant_id = $1 AND bank_code = $2 AND acc_tail = $3
       ),
       daily_totals AS (
         SELECT tenant_id,
                bank_code,
                acc_tail,
                date::date AS date,
                SUM(COALESCE(api_matched_abs, 0)) AS api_matched_abs,
                SUM(COALESCE(erp_matched_abs, 0)) AS erp_matched_abs,
                SUM(COALESCE(api_unrec_abs, 0)) AS api_unrec_abs,
                SUM(COALESCE(erp_unrec_abs, 0)) AS erp_unrec_abs,
                SUM(COALESCE(unrec_diff, 0)) AS unrec_diff
         FROM gold_conciliation_daily
         WHERE tenant_id = $1 AND bank_code = $2 AND acc_tail = $3
         GROUP BY tenant_id, bank_code, acc_tail, date
       ),
       api_unrec AS (
         SELECT tenant_id, bank_code, acc_tail, date::date AS date, SUM(amount) AS api_unrec_abs
         FROM gold_unreconciled_api
         WHERE tenant_id = $1 AND bank_code = $2 AND acc_tail = $3
         GROUP BY tenant_id, bank_code, acc_tail, date
       ),
       erp_unrec AS (
         SELECT tenant_id, bank_code, acc_tail, date::date AS date, SUM(amount) AS erp_unrec_abs
         FROM gold_unreconciled_erp
         WHERE tenant_id = $1 AND bank_code = $2 AND acc_tail = $3
         GROUP BY tenant_id, bank_code, acc_tail, date
       )
       SELECT d.tenant_id,
              d.bank_code,
              d.acc_tail,
              d.date::text AS date,
              COALESCE(dt.api_matched_abs, 0)::float AS api_matched_abs,
              COALESCE(dt.erp_matched_abs, 0)::float AS erp_matched_abs,
              COALESCE(dt.api_unrec_abs, au.api_unrec_abs, 0)::float AS api_unrec_abs,
              COALESCE(dt.erp_unrec_abs, eu.erp_unrec_abs, 0)::float AS erp_unrec_abs,
              (
                COALESCE(dt.api_unrec_abs, au.api_unrec_abs, 0) +
                COALESCE(dt.erp_unrec_abs, eu.erp_unrec_abs, 0)
              )::float AS unrec_total_abs,
              COALESCE(dt.unrec_diff, 0)::float AS unrec_diff
       FROM all_days d
       LEFT JOIN daily_totals dt
         ON dt.tenant_id = d.tenant_id
        AND dt.bank_code = d.bank_code
        AND dt.acc_tail = d.acc_tail
        AND dt.date = d.date
       LEFT JOIN api_unrec au
         ON au.tenant_id = d.tenant_id
        AND au.bank_code = d.bank_code
        AND au.acc_tail = d.acc_tail
        AND au.date = d.date
       LEFT JOIN erp_unrec eu
         ON eu.tenant_id = d.tenant_id
        AND eu.bank_code = d.bank_code
        AND eu.acc_tail = d.acc_tail
        AND eu.date = d.date
       ORDER BY d.date`,
      [tenantId, bankCode, accTail]
    )

    return NextResponse.json({ monthly, daily })
  } catch (error) {
    console.error("Failed to load summary", error)
    return NextResponse.json({ error: "Erro ao carregar dados" }, { status: 500 })
  }
}
