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
      `SELECT m.api_uid, m.erp_uid, m.stage, m.prio, m.ddiff,
              a.amount AS api_amount,
              a.descriptionraw AS api_desc,
              a.date_br::date AS api_date,
              e.amount_client AS erp_amount,
              e.description_client AS erp_desc,
              e.date_br::date AS erp_date
       FROM gold_conciliation_matches m
       JOIN silver_api_staging a ON m.api_uid = a.id
       JOIN silver_erp_staging e ON m.erp_uid = e.cd_lancamento
       WHERE a.tenant_id = $1
         AND e.tenant_id = $1
         AND a.bank_code = $2
         AND (a.account_number ~ '\\d' AND right(regexp_replace(a.account_number, '\\D', '', 'g'), 8) = $3)
         AND (a.date_br::date = $4 OR e.date_br::date = $4)`,
      [tenantId, bankCode, accTail, date]
    )

    return NextResponse.json({ unreconciledApi, unreconciledErp, matches })
  } catch (error) {
    console.error("Failed to load day details", error)
    return NextResponse.json({ error: "Erro ao carregar detalhes" }, { status: 500 })
  }
}
