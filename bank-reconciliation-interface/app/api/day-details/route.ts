import { NextResponse } from "next/server"
import { query } from "@/lib/db"
import { normalizeAmount } from "@/lib/normalize-amount"

function isWeekend(date: Date) {
  const day = date.getUTCDay()
  return day === 0 || day === 6
}

function shiftBusinessDays(base: Date, step: -1 | 1, count: number) {
  const dates: Date[] = []
  let current = new Date(base)

  while (dates.length < count) {
    current = new Date(current)
    current.setUTCDate(current.getUTCDate() + step)
    if (isWeekend(current)) continue
    dates.push(new Date(current))
  }

  return dates
}

function getCandidateDates(baseDate: string) {
  const base = new Date(baseDate)
  if (Number.isNaN(base.getTime())) return []

  const prev = shiftBusinessDays(base, -1, 2)
  const next = shiftBusinessDays(base, 1, 2)

  const all = [base, ...prev, ...next]
  return all
    .map((d) => d.toISOString().slice(0, 10))
    .filter((value, index, self) => self.indexOf(value) === index)
}

type UnreconciledApiRow = {
  tenant_id: string
  bank_code: string
  acc_tail: string
  date: string
  amount: string | number | null
  api_id: string
  desc_norm: string | null
}

type UnreconciledErpRow = {
  tenant_id: string
  bank_code: string
  acc_tail: string
  date: string
  amount: string | number | null
  cd_lancamento: string
  desc_norm: string | null
}

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url)
  const tenantId = searchParams.get("tenant")
  const bankCode = searchParams.get("bank")
  const accTail = searchParams.get("accTail")
  const date = searchParams.get("date")
  const strictDate = searchParams.get("strictDate") === "true"

  if (!tenantId || !bankCode || !accTail || !date) {
    return NextResponse.json({ error: "Parâmetros obrigatórios ausentes" }, { status: 400 })
  }

  try {
    const candidateDates = strictDate ? [] : getCandidateDates(date)
    const dateFilter = strictDate ? [date] : candidateDates.length > 0 ? candidateDates : [date]

    const unreconciledApiRaw = await query<UnreconciledApiRow>(
      `SELECT tenant_id, bank_code, acc_tail, date::text AS date,
              COALESCE(amount::text,'0') AS amount, api_id, desc_norm
       FROM gold_unreconciled_api
       WHERE tenant_id = $1 AND bank_code = $2 AND acc_tail = $3 AND date = $4
       ORDER BY amount DESC`,
      [tenantId, bankCode, accTail, date]
    )

    const unreconciledApi = unreconciledApiRaw.map((row) => ({
      ...row,
      amount: normalizeAmount(row.amount),
    }))

    const unreconciledErpRaw = await query<UnreconciledErpRow>(
      `SELECT tenant_id, bank_code, acc_tail, date::text AS date,
              COALESCE(amount::text,'0') AS amount, cd_lancamento, desc_norm
       FROM gold_unreconciled_erp
       WHERE tenant_id = $1 AND bank_code = $2 AND acc_tail = $3 AND date = $4
       ORDER BY amount DESC`,
      [tenantId, bankCode, accTail, date]
    )

    const unreconciledErp = unreconciledErpRaw.map((row) => ({
      ...row,
      amount: normalizeAmount(row.amount),
    }))

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
         AND (acc_tail = $3 OR RIGHT(acc_tail, 4) = RIGHT($3, 4))
         AND (api_date = ANY($4::date[]) OR erp_date = ANY($4::date[]))
       ORDER BY prio, api_uid, erp_uid`,
      [tenantId, bankCode, accTail, dateFilter]
    )

    return NextResponse.json({ unreconciledApi, unreconciledErp, matches })
  } catch (error) {
    console.error("Failed to load day details", error)
    return NextResponse.json({ error: "Erro ao carregar detalhes" }, { status: 500 })
  }
}
