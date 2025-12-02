"use client"
import { useEffect, useMemo, useState } from "react"
import { Card } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { ChevronLeft } from "lucide-react"
import { parseDateOnly } from "@/lib/date"

export interface UnreconciledDetailsProps {
  tenantId: string
  bankCode: string
  accTail: string
  date: string
  showReconciled: boolean
  onShowReconciledChange: (show: boolean) => void
  onBack: () => void
}

type UnrecApi = {
  tenant_id: string
  bank_code: string
  acc_tail: string
  date: string
  amount: number
  api_id: string
  desc_norm: string
}

type UnrecErp = {
  tenant_id: string
  bank_code: string
  acc_tail: string
  date: string
  amount: number
  cd_lancamento: string
  desc_norm: string
}

type Match = {
  api_uid: string
  erp_uid: string
  stage: string
  prio: number
  ddiff: number
  api_amount: number
  api_desc: string
  api_date: string
  erp_amount: number
  erp_desc: string
  erp_date: string
}

export function UnreconciledDetails({
  tenantId,
  bankCode,
  accTail,
  date,
  showReconciled,
  onShowReconciledChange,
  onBack,
}: UnreconciledDetailsProps) {
  const [unrecApi, setUnrecApi] = useState<UnrecApi[]>([])
  const [unrecErp, setUnrecErp] = useState<UnrecErp[]>([])
  const [matches, setMatches] = useState<Match[]>([])
  const [loading, setLoading] = useState(false)
  const [loadingWindow, setLoadingWindow] = useState(false)
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [baseSelectionDate, setBaseSelectionDate] = useState<string | null>(null)
  const [windowDates, setWindowDates] = useState<string[]>([date])
  const [selectedApi, setSelectedApi] = useState<Set<string>>(new Set())
  const [selectedErp, setSelectedErp] = useState<Set<string>>(new Set())

  const fetchDayData = async (applyUnreconciled: boolean) => {
    setLoading(true)
    setError(null)
    try {
      const params = new URLSearchParams({
        tenant: tenantId,
        bank: bankCode,
        accTail,
        date,
      })
      const response = await fetch(`/api/day-details?${params.toString()}`)
      if (!response.ok) {
        const payload = await response.json().catch(() => ({ error: "Erro ao carregar dados" }))
        throw new Error(payload.error)
      }

      const payload = await response.json()
      if (applyUnreconciled) {
        setUnrecApi(payload.unreconciledApi ?? [])
        setUnrecErp(payload.unreconciledErp ?? [])
        setWindowDates([date])
      }
      setMatches(payload.matches ?? [])
    } catch (err: any) {
      console.error(err)
      setError(err?.message ?? "Erro ao buscar dados")
    } finally {
      setLoading(false)
    }
  }

  const fetchWindowData = async (baseDate: string) => {
    setLoadingWindow(true)
    setError(null)
    try {
      const params = new URLSearchParams({
        tenant: tenantId,
        bank: bankCode,
        accTail,
        baseDate,
      })

      const response = await fetch(`/api/manual-candidates?${params.toString()}`)
      if (!response.ok) {
        const payload = await response.json().catch(() => ({ error: "Erro ao carregar dados" }))
        throw new Error(payload.error)
      }

      const payload = await response.json()
      setUnrecApi(payload.unreconciledApi ?? [])
      setUnrecErp(payload.unreconciledErp ?? [])
      setWindowDates(payload.dates ?? [baseDate])
    } catch (err: any) {
      console.error(err)
      setError(err?.message ?? "Erro ao buscar dados")
    } finally {
      setLoadingWindow(false)
    }
  }

  useEffect(() => {
    setSelectedApi(new Set())
    setSelectedErp(new Set())
    setBaseSelectionDate(null)
    setWindowDates([date])
    fetchDayData(true)
  }, [tenantId, bankCode, accTail, date])

  useEffect(() => {
    if (showReconciled) {
      fetchDayData(false)
    }
  }, [showReconciled])

  const formatCurrency = (value: number) => {
    return new Intl.NumberFormat("pt-BR", {
      style: "currency",
      currency: "BRL",
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(value)
  }

  const apiById = useMemo(() => {
    const map = new Map<string, UnrecApi>()
    unrecApi.forEach((item) => map.set(item.api_id, item))
    return map
  }, [unrecApi])

  const erpById = useMemo(() => {
    const map = new Map<string, UnrecErp>()
    unrecErp.forEach((item) => map.set(item.cd_lancamento, item))
    return map
  }, [unrecErp])

  const selectedApiTotal = Array.from(selectedApi).reduce((sum, id) => sum + (apiById.get(id)?.amount ?? 0), 0)
  const selectedErpTotal = Array.from(selectedErp).reduce((sum, id) => sum + (erpById.get(id)?.amount ?? 0), 0)

  const totalsMatch = Math.round(selectedApiTotal * 100) === Math.round(selectedErpTotal * 100)

  const handleApiToggle = (item: UnrecApi) => {
    setSelectedApi((prev) => {
      const next = new Set(prev)
      if (next.has(item.api_id)) {
        next.delete(item.api_id)
      } else {
        next.add(item.api_id)
        if (!baseSelectionDate) {
          setBaseSelectionDate(item.date)
          fetchWindowData(item.date)
          fetchDayData(false)
        }
      }
      return next
    })
  }

  const handleErpToggle = (item: UnrecErp) => {
    setSelectedErp((prev) => {
      const next = new Set(prev)
      if (next.has(item.cd_lancamento)) {
        next.delete(item.cd_lancamento)
      } else {
        next.add(item.cd_lancamento)
        if (!baseSelectionDate) {
          setBaseSelectionDate(item.date)
          fetchWindowData(item.date)
          fetchDayData(false)
        }
      }
      return next
    })
  }

  const clearSelection = () => {
    setSelectedApi(new Set())
    setSelectedErp(new Set())
    setBaseSelectionDate(null)
    setWindowDates([date])
    fetchDayData(true)
  }

  const submitManualReconciliation = async () => {
    if (selectedApi.size === 0 || selectedErp.size === 0 || !totalsMatch) return
    setSubmitting(true)
    setError(null)
    try {
      const response = await fetch("/api/manual-conciliation", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          tenantId,
          bankCode,
          accTail,
          apiIds: Array.from(selectedApi),
          erpIds: Array.from(selectedErp),
        }),
      })

      if (!response.ok) {
        const payload = await response.json().catch(() => ({ error: "Erro ao conciliar" }))
        throw new Error(payload.error)
      }

      await fetchDayData(false)
      if (baseSelectionDate) {
        await fetchWindowData(baseSelectionDate)
      }
      setSelectedApi(new Set())
      setSelectedErp(new Set())
    } catch (err: any) {
      console.error(err)
      setError(err?.message ?? "Erro ao conciliar manualmente")
    } finally {
      setSubmitting(false)
    }
  }

  const dateObj = parseDateOnly(date)
  const formattedDate = dateObj.toLocaleDateString("pt-BR")

  const manualWindowLabel = windowDates
    .map((d) => parseDateOnly(d).toLocaleDateString("pt-BR"))
    .join(", ")

  return (
    <div>
      <div className="flex items-center gap-4 mb-6">
        <Button variant="outline" size="sm" onClick={onBack} className="gap-2 bg-transparent">
          <ChevronLeft className="h-4 w-4" />
          Voltar
        </Button>
        <h2 className="text-2xl font-bold text-foreground">Detalhes - {formattedDate}</h2>
      </div>

      <div className="mb-6">
        <label className="flex items-center gap-2 cursor-pointer">
          <input
            type="checkbox"
            checked={showReconciled}
            onChange={(e) => onShowReconciledChange(e.target.checked)}
            className="w-4 h-4"
          />
          <span className="text-foreground">Mostrar conciliados</span>
        </label>
      </div>

      {loading ? (
        <div className="text-muted-foreground">Carregando dados...</div>
      ) : error ? (
        <div className="text-destructive">{error}</div>
      ) : (
        <>
          <div className="flex flex-wrap gap-3 mb-4">
            <div className="rounded-md bg-slate-100 dark:bg-slate-900 px-4 py-3 text-sm text-foreground">
              <p className="font-semibold">Janela de busca</p>
              <p className="text-muted-foreground">{manualWindowLabel}</p>
              {baseSelectionDate ? (
                <p className="text-xs text-muted-foreground mt-1">Ancorada em {parseDateOnly(baseSelectionDate).toLocaleDateString("pt-BR")}</p>
              ) : (
                <p className="text-xs text-muted-foreground mt-1">Selecione um lançamento para abrir a janela de 2 dias úteis</p>
              )}
            </div>

            <div className="rounded-md bg-slate-100 dark:bg-slate-900 px-4 py-3 text-sm text-foreground flex items-center gap-3">
              <div>
                <p className="font-semibold">Selecionados</p>
                <p className="text-muted-foreground">API {formatCurrency(selectedApiTotal)} · ERP {formatCurrency(selectedErpTotal)}</p>
                {!totalsMatch && (selectedApi.size > 0 || selectedErp.size > 0) && (
                  <p className="text-xs text-destructive mt-1">Os totais precisam ser iguais</p>
                )}
                {totalsMatch && selectedApi.size > 0 && selectedErp.size > 0 && (
                  <p className="text-xs text-green-600 dark:text-green-400 mt-1">Totais alinhados para conciliação</p>
                )}
              </div>
              <div className="flex gap-2">
                <Button variant="outline" size="sm" onClick={clearSelection} disabled={loading || loadingWindow}>
                  Limpar seleção
                </Button>
                <Button
                  onClick={submitManualReconciliation}
                  disabled={
                    submitting ||
                    loadingWindow ||
                    selectedApi.size === 0 ||
                    selectedErp.size === 0 ||
                    !totalsMatch
                  }
                  className="bg-green-600 hover:bg-green-700"
                >
                  {submitting ? "Conciliando..." : "Conciliar manualmente"}
                </Button>
              </div>
            </div>
          </div>

          <div className="grid grid-cols-2 gap-6">
            <div>
              <h3 className="text-lg font-semibold text-foreground mb-4">API Pluggy</h3>
              <div className="space-y-2">
                {unrecApi.length > 0 ? (
                  unrecApi.map((item) => (
                    <Card
                      key={item.api_id}
                      className={`p-3 border transition-colors cursor-pointer ${
                        selectedApi.has(item.api_id)
                          ? "bg-green-50 dark:bg-green-900 border-green-300 dark:border-green-700"
                          : "bg-orange-50 dark:bg-orange-950 border-orange-200 dark:border-orange-800"
                      }`}
                      onClick={() => handleApiToggle(item)}
                    >
                      <div className="text-sm">
                        <p className="font-semibold text-foreground">{formatCurrency(item.amount)}</p>
                        <p className="text-xs text-muted-foreground mt-1">{parseDateOnly(item.date).toLocaleDateString("pt-BR")}</p>
                        <p className="text-xs text-muted-foreground mt-1">{item.desc_norm}</p>
                        <p className="text-xs text-muted-foreground mt-1">ID: {item.api_id.substring(0, 12)}...</p>
                      </div>
                    </Card>
                  ))
                ) : (
                  <div className="text-center py-8 text-muted-foreground">
                    <p>Nenhuma transação não conciliada</p>
                  </div>
                )}
              </div>
            </div>

            <div>
              <h3 className="text-lg font-semibold text-foreground mb-4">ERP</h3>
              <div className="space-y-2">
                {unrecErp.length > 0 ? (
                  unrecErp.map((item) => (
                    <Card
                      key={item.cd_lancamento}
                      className={`p-3 border transition-colors cursor-pointer ${
                        selectedErp.has(item.cd_lancamento)
                          ? "bg-green-50 dark:bg-green-900 border-green-300 dark:border-green-700"
                          : "bg-orange-50 dark:bg-orange-950 border-orange-200 dark:border-orange-800"
                      }`}
                      onClick={() => handleErpToggle(item)}
                    >
                      <div className="text-sm">
                        <p className="font-semibold text-foreground">{formatCurrency(item.amount)}</p>
                        <p className="text-xs text-muted-foreground mt-1">{parseDateOnly(item.date).toLocaleDateString("pt-BR")}</p>
                        <p className="text-xs text-muted-foreground mt-1">{item.desc_norm}</p>
                        <p className="text-xs text-muted-foreground mt-1">CD: {item.cd_lancamento}</p>
                      </div>
                    </Card>
                  ))
                ) : (
                  <div className="text-center py-8 text-muted-foreground">
                    <p>Nenhuma transação não conciliada</p>
                  </div>
                )}
              </div>
            </div>
          </div>

          {showReconciled && (
            <div className="mt-8">
              <h3 className="text-lg font-semibold text-foreground mb-4">Transações Conciliadas</h3>
              {matches.length > 0 ? (
                <div className="space-y-3">
                  {matches.map((match) => (
                    <Card
                      key={`${match.api_uid}-${match.erp_uid}`}
                      className="p-4 bg-green-50 dark:bg-green-950 border-green-200 dark:border-green-800"
                    >
                      <div className="grid grid-cols-3 gap-4">
                        <div>
                          <p className="text-xs text-muted-foreground mb-1">API</p>
                          <p className="font-semibold text-foreground text-sm">{formatCurrency(match.api_amount)}</p>
                          <p className="text-xs text-muted-foreground">{parseDateOnly(match.api_date).toLocaleDateString("pt-BR")}</p>
                          <p className="text-xs text-muted-foreground mt-1">{match.api_desc}</p>
                        </div>
                        <div className="flex items-center justify-center">
                          <span className="text-green-600 font-bold">↔</span>
                        </div>
                        <div>
                          <p className="text-xs text-muted-foreground mb-1">ERP</p>
                          <p className="font-semibold text-foreground text-sm">{formatCurrency(match.erp_amount)}</p>
                          <p className="text-xs text-muted-foreground">{parseDateOnly(match.erp_date).toLocaleDateString("pt-BR")}</p>
                          <p className="text-xs text-muted-foreground mt-1">{match.erp_desc}</p>
                        </div>
                      </div>
                      <div className="mt-2 text-xs text-muted-foreground">Estágio: {match.stage}</div>
                    </Card>
                  ))}
                </div>
              ) : (
                <div className="text-muted-foreground text-sm">Nenhuma transação conciliada para este período.</div>
              )}
            </div>
          )}
        </>
      )}
    </div>
  )
}
