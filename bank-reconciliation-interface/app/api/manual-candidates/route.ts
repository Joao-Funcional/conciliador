import { NextResponse } from "next/server"
import { query } from "@/lib/db"

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

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url)
  const tenantId = searchParams.get("tenant")
  const bankCode = searchParams.get("bank")
  const accTail = searchParams.get("accTail")
  const baseDate = searchParams.get("baseDate")

  if (!tenantId || !bankCode || !accTail || !baseDate) {
    return NextResponse.json({ error: "Parâmetros obrigatórios ausentes" }, { status: 400 })
  }

  const candidateDates = getCandidateDates(baseDate)
  if (candidateDates.length === 0) {
    return NextResponse.json({ error: "Data inválida" }, { status: 400 })
  }

  try {
    const unreconciledApi = await query(
      `SELECT tenant_id, bank_code, acc_tail, date::text AS date,
              COALESCE(amount,0)::float AS amount, api_id, desc_norm
       FROM gold_unreconciled_api
       WHERE tenant_id = $1 AND bank_code = $2 AND acc_tail = $3 AND date = ANY($4::date[])
       ORDER BY date DESC, amount DESC`,
      [tenantId, bankCode, accTail, candidateDates]
    )

    const unreconciledErp = await query(
      `SELECT tenant_id, bank_code, acc_tail, date::text AS date,
              COALESCE(amount,0)::float AS amount, cd_lancamento, desc_norm
       FROM gold_unreconciled_erp
       WHERE tenant_id = $1 AND bank_code = $2 AND acc_tail = $3 AND date = ANY($4::date[])
       ORDER BY date DESC, amount DESC`,
      [tenantId, bankCode, accTail, candidateDates]
    )

    return NextResponse.json({
      dates: candidateDates,
      unreconciledApi,
      unreconciledErp,
    })
  } catch (error) {
    console.error("Failed to load manual candidates", error)
    return NextResponse.json({ error: "Erro ao carregar dados para conciliação manual" }, { status: 500 })
  }
}
