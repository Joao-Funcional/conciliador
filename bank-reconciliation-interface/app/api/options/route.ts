import { NextResponse } from "next/server"
import { query } from "@/lib/db"

type OptionRow = {
  tenant_id: string
  bank_code: string
  acc_tail: string
}

export async function GET() {
  try {
    const rows = await query<OptionRow>(
      `SELECT DISTINCT tenant_id, bank_code, acc_tail
       FROM gold_conciliation_daily
       ORDER BY tenant_id, bank_code, acc_tail`
    )
    return NextResponse.json({ options: rows })
  } catch (error) {
    console.error("Failed to load options", error)
    return NextResponse.json({ error: "Erro ao carregar filtros" }, { status: 500 })
  }
}
