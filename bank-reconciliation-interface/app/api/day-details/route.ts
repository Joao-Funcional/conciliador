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
    const unreconciledApi = await query(
      `SELECT tenant_id, bank_code, acc_tail, date::text AS date,
              COALESCE(amount,0)::float AS amount, api_id, desc_norm
       FROM gold_unreconciled_api
       WHERE tenant_id = $1 AND bank_code = $2 AND acc_tail = $3 AND date = $4
       ORDER BY amount DESC`,
      [tenantId, bankCode, accTail, date]
    )

    const unreconciledErp = await query(
      `SELECT tenant_id, bank_code, acc_tail, date::text AS date,
              COALESCE(amount,0)::float AS amount, cd_lancamento, desc_norm
       FROM gold_unreconciled_erp
       WHERE tenant_id = $1 AND bank_code = $2 AND acc_tail = $3 AND date = $4
       ORDER BY amount DESC`,
      [tenantId, bankCode, accTail, date]
    )

    const matches = await query(
      `SELECT api_uid, erp_uid, stage, prio, ddiff,
              api_amount,
              api_desc,
              api_date,
              erp_amount,
              erp_desc,
              erp_date
       FROM gold_conciliation_matches_enriched
       WHERE tenant_id = $1
         AND bank_code = $2
         AND acc_tail = $3
         AND (api_date = $4 OR erp_date = $4)
       ORDER BY prio, api_uid, erp_uid`,
      [tenantId, bankCode, accTail, date]
    )

    return NextResponse.json({ unreconciledApi, unreconciledErp, matches })
  } catch (error) {
    console.error("Failed to load day details", error)
    return NextResponse.json({ error: "Erro ao carregar detalhes" }, { status: 500 })
  }
}
