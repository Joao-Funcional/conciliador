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
      `WITH daily AS (
        SELECT tenant_id,
               bank_code,
               acc_tail,
               date_trunc('month', date)::date AS month,
               COALESCE(api_matched_abs,0) AS api_matched_abs,
               COALESCE(erp_matched_abs,0) AS erp_matched_abs,
               COALESCE(api_unrec_abs,0) AS api_unrec_abs,
               COALESCE(erp_unrec_abs,0) AS erp_unrec_abs,
               COALESCE(unrec_total_abs,0) AS unrec_total_abs
        FROM gold_conciliation_daily
        WHERE tenant_id = $1 AND bank_code = $2 AND acc_tail = $3
      )
      SELECT tenant_id,
             bank_code,
             acc_tail,
             month::text AS month,
             SUM(api_matched_abs)::float AS api_matched_abs,
             SUM(erp_matched_abs)::float AS erp_matched_abs,
             SUM(api_unrec_abs)::float AS api_unrec_abs,
             SUM(erp_unrec_abs)::float AS erp_unrec_abs,
             SUM(unrec_total_abs)::float AS unrec_total_abs
      FROM daily
      GROUP BY tenant_id, bank_code, acc_tail, month
      ORDER BY month`,
      [tenantId, bankCode, accTail]
    )

    const daily = await query(
      `SELECT tenant_id, bank_code, acc_tail, date::text AS date,
              COALESCE(api_matched_abs,0)::float AS api_matched_abs,
              COALESCE(erp_matched_abs,0)::float AS erp_matched_abs,
              COALESCE(api_unrec_abs,0)::float AS api_unrec_abs,
              COALESCE(erp_unrec_abs,0)::float AS erp_unrec_abs,
              COALESCE(unrec_total_abs,0)::float AS unrec_total_abs,
              COALESCE(unrec_diff,0)::float AS unrec_diff
       FROM gold_conciliation_daily
       WHERE tenant_id = $1 AND bank_code = $2 AND acc_tail = $3
       ORDER BY date`,
      [tenantId, bankCode, accTail]
    )

    return NextResponse.json({ monthly, daily })
  } catch (error) {
    console.error("Failed to load summary", error)
    return NextResponse.json({ error: "Erro ao carregar dados" }, { status: 500 })
  }
}
