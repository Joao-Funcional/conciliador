import { randomUUID } from "crypto"
import { NextResponse } from "next/server"
import { getClient } from "@/lib/db"
import { normalizeAmount } from "@/lib/normalize-amount"

type ApiRow = {
  api_id: string
  amount: number
  date: string
}

type ApiRowRaw = {
  api_id: string
  amount: string | number | null
  date: string
}

type ErpRow = {
  cd_lancamento: string
  amount: number
  date: string
}

type ErpRowRaw = {
  cd_lancamento: string
  amount: string | number | null
  date: string
}

export async function POST(request: Request) {
  const requestId = randomUUID()
  const body = await request.json().catch(() => null)
  if (!body) {
    return NextResponse.json({ error: "Payload inválido" }, { status: 400 })
  }

  const { tenantId, bankCode, accTail, apiIds, erpIds } = body as {
    tenantId?: string
    bankCode?: string
    accTail?: string
    apiIds?: string[]
    erpIds?: string[]
  }

  if (!tenantId || !bankCode || !accTail || !Array.isArray(apiIds) || !Array.isArray(erpIds)) {
    return NextResponse.json({ error: "Parâmetros obrigatórios ausentes" }, { status: 400 })
  }

  const sanitizeIds = (ids: unknown[], label: string) => {
    const valid: string[] = []
    const invalid: string[] = []

    ids.forEach((id) => {
      const raw = String(id ?? "").trim()
      const normalized = raw.replace(/,/g, ".")

      const dotCount = (normalized.match(/\./g) ?? []).length
      const numericish = /^[-+]?\d[\d.]*$/.test(normalized)

      if (
        !raw ||
        !/^[-\p{L}\p{N}_.:]+$/u.test(normalized) ||
        (numericish && dotCount > 1)
      ) {
        invalid.push(raw || "<vazio>")
        return
      }

      valid.push(normalized)
    })

    if (invalid.length > 0) {
      throw new Error(`IDs inválidos (${label}): ${invalid.join(", ")}`)
    }

    return valid
  }

  let normalizedApiIds: string[]
  let normalizedErpIds: string[]

  try {
    normalizedApiIds = sanitizeIds(apiIds, "API")
    normalizedErpIds = sanitizeIds(erpIds, "ERP")
  } catch (error: any) {
    return NextResponse.json({ error: error?.message ?? "IDs inválidos" }, { status: 400 })
  }

  if (normalizedApiIds.length === 0 || normalizedErpIds.length === 0) {
    return NextResponse.json({ error: "Selecione pelo menos um lançamento de cada lado" }, { status: 400 })
  }

  const logPrefix = `[manual-conciliation ${requestId}]`
  console.info(logPrefix, "payload accepted", {
    tenantId,
    bankCode,
    accTail,
    apiIdsCount: normalizedApiIds.length,
    erpIdsCount: normalizedErpIds.length,
    apiIdsSample: normalizedApiIds.slice(0, 5),
    erpIdsSample: normalizedErpIds.slice(0, 5),
  })

  const client = await getClient()

  try {
    await client.query("BEGIN")

    const apiRowsRaw = (
      await client.query<ApiRowRaw>(
        `SELECT api_id, COALESCE(amount::text,'0') AS amount, date::text AS date
         FROM gold_unreconciled_api
         WHERE tenant_id = $1 AND bank_code = $2 AND acc_tail = $3 AND api_id::text = ANY($4::text[])`,
        [tenantId, bankCode, accTail, normalizedApiIds]
      )
    ).rows

    const apiRows: ApiRow[] = apiRowsRaw.map((row) => ({
      api_id: row.api_id,
      amount: normalizeAmount(row.amount),
      date: row.date,
    }))

    console.info(logPrefix, "api rows fetched", {
      count: apiRows.length,
      sample: apiRows.slice(0, 3),
    })

    const erpRowsRaw = (
      await client.query<ErpRowRaw>(
        `SELECT cd_lancamento, COALESCE(amount::text,'0') AS amount, date::text AS date
         FROM gold_unreconciled_erp
         WHERE tenant_id = $1 AND bank_code = $2 AND acc_tail = $3 AND cd_lancamento::text = ANY($4::text[])`,
        [tenantId, bankCode, accTail, normalizedErpIds]
      )
    ).rows

    const erpRows: ErpRow[] = erpRowsRaw.map((row) => ({
      cd_lancamento: row.cd_lancamento,
      amount: normalizeAmount(row.amount),
      date: row.date,
    }))

    console.info(logPrefix, "erp rows fetched", {
      count: erpRows.length,
      sample: erpRows.slice(0, 3),
    })

    if (apiRows.length !== normalizedApiIds.length || erpRows.length !== normalizedErpIds.length) {
      throw new Error("Alguns lançamentos selecionados não estão mais disponíveis para conciliação")
    }

    const totalApi = apiRows.reduce((sum, row) => sum + row.amount, 0)
    const totalErp = erpRows.reduce((sum, row) => sum + row.amount, 0)

    const roundedApi = Math.round(totalApi * 100) / 100
    const roundedErp = Math.round(totalErp * 100) / 100

    if (roundedApi !== roundedErp) {
      throw new Error("Os valores selecionados precisam ter o mesmo total")
    }

    console.info(logPrefix, "amounts matched", {
      roundedApi,
      roundedErp,
    })

    const apiMap = new Map(apiRows.map((row) => [row.api_id, row]))
    const erpMap = new Map(erpRows.map((row) => [row.cd_lancamento, row]))

    const matchValues: Array<{ apiId: string; erpId: string; ddiff: number }> = []
    const matchImpact = new Map<string, { api: number; erp: number }>()

    for (const apiId of normalizedApiIds) {
      const api = apiMap.get(apiId)!
      for (const erpId of normalizedErpIds) {
        const erp = erpMap.get(erpId)!
        const dateKey = `${erp.date}`
        const current = matchImpact.get(dateKey) ?? { api: 0, erp: 0 }
        matchImpact.set(dateKey, {
          api: current.api + Math.abs(api.amount),
          erp: current.erp + Math.abs(erp.amount),
        })

        const apiDate = new Date(api.date)
        const erpDate = new Date(erp.date)
        const ddiff = Math.round((erpDate.getTime() - apiDate.getTime()) / (1000 * 60 * 60 * 24))
        matchValues.push({ apiId, erpId, ddiff })
      }
    }

    if (matchValues.length > 0) {
      const valuesClause = matchValues
        .map((_, idx) => `($${idx * 3 + 1}, $${idx * 3 + 2}, 'MANUAL', 9999, $${idx * 3 + 3})`)
        .join(",")
      const params: any[] = []
      matchValues.forEach((mv) => {
        params.push(mv.apiId, mv.erpId, mv.ddiff)
      })

      await client.query(
        `INSERT INTO gold_conciliation_matches (api_uid, erp_uid, stage, prio, ddiff)
         VALUES ${valuesClause}`,
        params
      )
    }

    await client.query(
      `DELETE FROM gold_unreconciled_api
       WHERE tenant_id = $1 AND bank_code = $2 AND acc_tail = $3 AND api_id::text = ANY($4::text[])`,
      [tenantId, bankCode, accTail, normalizedApiIds]
    )

    await client.query(
      `DELETE FROM gold_unreconciled_erp
       WHERE tenant_id = $1 AND bank_code = $2 AND acc_tail = $3 AND cd_lancamento::text = ANY($4::text[])`,
      [tenantId, bankCode, accTail, normalizedErpIds]
    )

    const apiUnrecAdjust = new Map<string, number>()
    apiRows.forEach((row) => {
      const key = row.date
      apiUnrecAdjust.set(key, (apiUnrecAdjust.get(key) ?? 0) + Math.abs(row.amount))
    })

    const erpUnrecAdjust = new Map<string, number>()
    erpRows.forEach((row) => {
      const key = row.date
      erpUnrecAdjust.set(key, (erpUnrecAdjust.get(key) ?? 0) + Math.abs(row.amount))
    })

    const datesToUpdate = new Set<string>([
      ...apiUnrecAdjust.keys(),
      ...erpUnrecAdjust.keys(),
      ...matchImpact.keys(),
    ])

    for (const date of datesToUpdate) {
      const current = (
        await client.query(
          `SELECT api_matched_abs, erp_matched_abs, api_unrec_abs, erp_unrec_abs
           FROM gold_conciliation_daily
           WHERE tenant_id = $1 AND bank_code = $2 AND acc_tail = $3 AND date = $4
           LIMIT 1`,
          [tenantId, bankCode, accTail, date]
        )
      ).rows[0]

      const apiMatched = (current?.api_matched_abs ?? 0) + (matchImpact.get(date)?.api ?? 0)
      const erpMatched = (current?.erp_matched_abs ?? 0) + (matchImpact.get(date)?.erp ?? 0)
      const apiUnrec = Math.max((current?.api_unrec_abs ?? 0) - (apiUnrecAdjust.get(date) ?? 0), 0)
      const erpUnrec = Math.max((current?.erp_unrec_abs ?? 0) - (erpUnrecAdjust.get(date) ?? 0), 0)
      const unrecTotal = apiUnrec + erpUnrec
      const unrecDiff = erpUnrec - apiUnrec

      const updated = await client.query(
        `UPDATE gold_conciliation_daily
         SET api_matched_abs = $1,
             erp_matched_abs = $2,
             api_unrec_abs = $3,
             erp_unrec_abs = $4,
             unrec_total_abs = $5,
             unrec_diff = $6
         WHERE tenant_id = $7 AND bank_code = $8 AND acc_tail = $9 AND date = $10`,
        [
          apiMatched,
          erpMatched,
          apiUnrec,
          erpUnrec,
          unrecTotal,
          unrecDiff,
          tenantId,
          bankCode,
          accTail,
          date,
        ]
      )

      if (updated.rowCount === 0) {
        await client.query(
          `INSERT INTO gold_conciliation_daily (
             tenant_id, bank_code, acc_tail, date,
             api_matched_abs, erp_matched_abs, api_unrec_abs, erp_unrec_abs, unrec_total_abs, unrec_diff
           ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
          [
            tenantId,
            bankCode,
            accTail,
            date,
            apiMatched,
            erpMatched,
            apiUnrec,
            erpUnrec,
            unrecTotal,
            unrecDiff,
          ]
        )
      }
    }

    await client.query("COMMIT")
    console.info(logPrefix, "manual conciliation committed")
    return NextResponse.json({ success: true })
  } catch (error: any) {
    await client.query("ROLLBACK")
    console.error(logPrefix, "manual conciliation failed", {
      error: error?.message,
      detail: error?.detail,
      code: error?.code,
      where: error?.where,
      position: error?.position,
      payload: {
        tenantId,
        bankCode,
        accTail,
        apiIds: normalizedApiIds,
        erpIds: normalizedErpIds,
      },
    })

    return NextResponse.json(
      { error: error?.message ?? "Erro ao conciliar manualmente" },
      { status: 500 }
    )
  } finally {
    client.release()
  }
}
