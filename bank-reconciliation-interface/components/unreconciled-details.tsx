"use client"
import { useEffect, useMemo, useRef, useState } from "react"
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
  const [anchorSide, setAnchorSide] = useState<"api" | "erp" | null>(null)
  const [selectionWarning, setSelectionWarning] = useState<string | null>(null)
  const [isWindowFilterActive, setIsWindowFilterActive] = useState(false)
  const [reconciledFilter, setReconciledFilter] = useState<
    "all" | "oneToOne" | "erpAnchored" | "apiAnchored" | "fallback"
  >("all")
  const fetchIdRef = useRef(0)

  const fetchDayData = async (applyUnreconciled: boolean, strictDateForMatches = false) => {
    const fetchId = ++fetchIdRef.current
    setLoading(true)
    setError(null)
    try {
      const params = new URLSearchParams({
        tenant: tenantId,
        bank: bankCode,
        accTail,
        date,
      })
      params.append("strictDate", strictDateForMatches ? "true" : "false")
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
      if (fetchId === fetchIdRef.current) {
        setMatches(payload.matches ?? [])
      }
    } catch (err: any) {
      console.error(err)
      setError(err?.message ?? "Erro ao buscar dados")
    } finally {
      if (fetchId === fetchIdRef.current) {
        setLoading(false)
      }
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
    setAnchorSide(null)
    setSelectionWarning(null)
    setIsWindowFilterActive(false)
    setBaseSelectionDate(null)
    setWindowDates([date])
    fetchDayData(true)
  }, [tenantId, bankCode, accTail, date])

  useEffect(() => {
    if (showReconciled) {
      fetchDayData(false, true)
    }
    setReconciledFilter("all")
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

  const formattedWindowDates = useMemo(() => {
    return windowDates
      .map((d) => parseDateOnly(d))
      .sort((a, b) => a.getTime() - b.getTime())
      .map((d) => d.toLocaleDateString("pt-BR"))
  }, [windowDates])

  const matchCounts = useMemo(() => {
    const api = new Map<string, number>()
    const erp = new Map<string, number>()

    matches.forEach((match) => {
      api.set(match.api_uid, (api.get(match.api_uid) ?? 0) + 1)
      erp.set(match.erp_uid, (erp.get(match.erp_uid) ?? 0) + 1)
    })

    return { api, erp }
  }, [matches])

  const categorizeMatch = (match: Match) => {
    const apiCount = matchCounts.api.get(match.api_uid) ?? 0
    const erpCount = matchCounts.erp.get(match.erp_uid) ?? 0

    if (apiCount === 1 && erpCount === 1) return "oneToOne" as const
    if (apiCount > 1 && erpCount === 1) return "erpAnchored" as const
    if (apiCount === 1 && erpCount > 1) return "apiAnchored" as const
    return "fallback" as const
  }

  const sortedMatches = useMemo(() => {
    const groups = new Map<string, Match[]>()

    matches.forEach((match) => {
      const apiCount = matchCounts.api.get(match.api_uid) ?? 0
      const erpCount = matchCounts.erp.get(match.erp_uid) ?? 0

      let key: string
      if (apiCount > 1 && erpCount === 1) {
        key = `erp:${match.erp_uid}`
      } else if (apiCount === 1 && erpCount > 1) {
        key = `api:${match.api_uid}`
      } else {
        key = `pair:${match.api_uid}:${match.erp_uid}`
      }

      const existing = groups.get(key) ?? []
      existing.push(match)
      groups.set(key, existing)
    })

    const sortedKeys = Array.from(groups.keys()).sort((a, b) => {
      const aKey = a.split(":").slice(1).join(":")
      const bKey = b.split(":").slice(1).join(":")
      return aKey.localeCompare(bKey)
    })

    return sortedKeys.flatMap((key) => {
      const group = groups.get(key) ?? []
      return group.sort((a, b) => {
        if (a.api_uid !== b.api_uid) return a.api_uid.localeCompare(b.api_uid)
        if (a.erp_uid !== b.erp_uid) return a.erp_uid.localeCompare(b.erp_uid)
        return a.prio - b.prio
      })
    })
  }, [matches, matchCounts])

  const filteredMatches = useMemo(() => {
    if (reconciledFilter === "all") return sortedMatches

    return sortedMatches.filter((match) => {
      const category = categorizeMatch(match)
      if (reconciledFilter === "oneToOne") return category === "oneToOne"
      if (reconciledFilter === "erpAnchored") return category === "erpAnchored"
      if (reconciledFilter === "apiAnchored") return category === "apiAnchored"
      return category === "fallback"
    })
  }, [sortedMatches, reconciledFilter, matchCounts])

  const selectedApiTotal = Array.from(selectedApi).reduce((sum, id) => sum + (apiById.get(id)?.amount ?? 0), 0)
  const selectedErpTotal = Array.from(selectedErp).reduce((sum, id) => sum + (erpById.get(id)?.amount ?? 0), 0)

  const totalsMatch = Math.round(selectedApiTotal * 100) === Math.round(selectedErpTotal * 100)

  const handleApiToggle = (item: UnrecApi) => {
    setSelectionWarning(null)
    setSelectedApi((prev) => {
      const next = new Set(prev)
      if (next.has(item.api_id)) {
        next.delete(item.api_id)
        const wasAnchor = anchorSide === "api"
        if (wasAnchor) {
          setSelectedErp(new Set())
          setAnchorSide(null)
          if (!isWindowFilterActive) {
            setBaseSelectionDate(null)
          }
        } else {
          if (next.size === 0 && selectedErp.size === 0) {
            setAnchorSide(null)
            if (!isWindowFilterActive) {
              setBaseSelectionDate(null)
            }
          }
        }
      } else {
        if (anchorSide === "api" && next.size >= 1) {
          setSelectionWarning("Selecione apenas um lançamento do Extrato por vez.")
          return prev
        }
        next.add(item.api_id)
        if (!baseSelectionDate) {
          setBaseSelectionDate(item.date)
        }
        if (!anchorSide) {
          setAnchorSide("api")
        }
      }
      return next
    })
  }

  const handleErpToggle = (item: UnrecErp) => {
    setSelectionWarning(null)
    setSelectedErp((prev) => {
      const next = new Set(prev)
      if (next.has(item.cd_lancamento)) {
        next.delete(item.cd_lancamento)
        const wasAnchor = anchorSide === "erp"
        if (wasAnchor) {
          setSelectedApi(new Set())
          setAnchorSide(null)
          if (!isWindowFilterActive) {
            setBaseSelectionDate(null)
          }
        } else {
          if (next.size === 0 && selectedApi.size === 0) {
            setAnchorSide(null)
            if (!isWindowFilterActive) {
              setBaseSelectionDate(null)
            }
          }
        }
      } else {
        if (anchorSide === "erp" && next.size >= 1) {
          setSelectionWarning("Selecione apenas um lançamento do Cliente por vez.")
          return prev
        }
        next.add(item.cd_lancamento)
        if (!baseSelectionDate) {
          setBaseSelectionDate(item.date)
        }
        if (!anchorSide) {
          setAnchorSide("erp")
        }
      }
      return next
    })
  }

  const clearSelection = () => {
    setSelectedApi(new Set())
    setSelectedErp(new Set())
    setAnchorSide(null)
    setSelectionWarning(null)
    setIsWindowFilterActive(false)
    setBaseSelectionDate(null)
    setWindowDates([date])
    fetchDayData(true)
  }

  const handleWindowFilterToggle = async () => {
    if (isWindowFilterActive) {
      setIsWindowFilterActive(false)
      setWindowDates([date])
      setBaseSelectionDate(anchorSide ? baseSelectionDate : null)
      await fetchDayData(true)
      return
    }

    const anchorDate = baseSelectionDate ?? date
    setIsWindowFilterActive(true)
    setBaseSelectionDate(anchorDate)
    await fetchWindowData(anchorDate)
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

  return (
    <div>
      <div className="flex items-center gap-4 mb-6">
        <Button variant="outline" size="sm" onClick={onBack} className="gap-2 bg-transparent">
          <ChevronLeft className="h-4 w-4" />
          Voltar
        </Button>
        <h2 className="text-2xl font-bold text-foreground">Detalhes - {formattedDate}</h2>
      </div>

      <div className="mb-6 flex gap-2">
        <Button
          variant={showReconciled ? "outline" : "default"}
          size="sm"
          onClick={() => onShowReconciledChange(false)}
        >
          Não conciliados
        </Button>
        <Button
          variant={showReconciled ? "default" : "outline"}
          size="sm"
          onClick={() => onShowReconciledChange(true)}
        >
          Conciliados
        </Button>
      </div>

      {loading ? (
        <div className="text-muted-foreground">Carregando dados...</div>
      ) : error ? (
        <div className="text-destructive">{error}</div>
      ) : (
        <>
          {showReconciled ? (
            <div className="mt-4 space-y-4">
              <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
                <h3 className="text-xl font-semibold text-foreground">Transações Conciliadas</h3>
                <div className="flex flex-wrap gap-2 text-sm">
                  <Button
                    size="sm"
                    variant={reconciledFilter === "all" ? "default" : "outline"}
                    onClick={() => setReconciledFilter("all")}
                  >
                    Todas
                  </Button>
                  <Button
                    size="sm"
                    variant={reconciledFilter === "oneToOne" ? "default" : "outline"}
                    onClick={() => setReconciledFilter("oneToOne")}
                  >
                    1 × 1
                  </Button>
                  <Button
                    size="sm"
                    variant={reconciledFilter === "erpAnchored" ? "default" : "outline"}
                    onClick={() => setReconciledFilter("erpAnchored")}
                  >
                    1 (Extrato) × N (Cliente)
                  </Button>
                  <Button
                    size="sm"
                    variant={reconciledFilter === "apiAnchored" ? "default" : "outline"}
                    onClick={() => setReconciledFilter("apiAnchored")}
                  >
                    1 (Cliente) × N (Extrato)
                  </Button>
                  <Button
                    size="sm"
                    variant={reconciledFilter === "fallback" ? "default" : "outline"}
                    onClick={() => setReconciledFilter("fallback")}
                  >
                    Fallbacks
                  </Button>
                </div>
              </div>

              {filteredMatches.length > 0 ? (
                <div className="space-y-3">
                  {filteredMatches.map((match) => (
                    <Card
                      key={`${match.api_uid}-${match.erp_uid}-${match.stage}`}
                      className="p-4 bg-green-50 dark:bg-green-950 border-green-200 dark:border-green-800"
                    >
                      <div className="grid grid-cols-3 gap-4">
                        <div>
                          <p className="text-xs font-semibold text-emerald-900 dark:text-emerald-200 mb-1">Extrato</p>
                          <p className="font-semibold text-foreground text-base">{formatCurrency(match.api_amount)}</p>
                          <p className="text-sm text-slate-700 dark:text-slate-200">{parseDateOnly(match.api_date).toLocaleDateString("pt-BR")}</p>
                          <p className="text-sm text-slate-700 dark:text-slate-200 mt-1">{match.api_desc}</p>
                          <p className="text-xs text-emerald-800 dark:text-emerald-200 mt-1 font-semibold">ID API: {match.api_uid}</p>
                        </div>
                        <div className="flex items-center justify-center">
                          <span className="text-green-700 dark:text-green-300 font-bold text-lg">↔</span>
                        </div>
                        <div>
                          <p className="text-xs font-semibold text-emerald-900 dark:text-emerald-200 mb-1">Cliente</p>
                          <p className="font-semibold text-foreground text-base">{formatCurrency(match.erp_amount)}</p>
                          <p className="text-sm text-slate-700 dark:text-slate-200">{parseDateOnly(match.erp_date).toLocaleDateString("pt-BR")}</p>
                          <p className="text-sm text-slate-700 dark:text-slate-200 mt-1">{match.erp_desc}</p>
                          <p className="text-xs text-emerald-800 dark:text-emerald-200 mt-1 font-semibold">CD Cliente: {match.erp_uid}</p>
                        </div>
                      </div>
                    </Card>
                  ))}
                </div>
              ) : (
                <div className="text-muted-foreground text-sm">Nenhuma transação conciliada para este período.</div>
              )}
            </div>
          ) : (
            <>
              <div className="sticky top-4 z-20 mb-4">
                <div className="flex flex-wrap gap-3 bg-background/90 p-2 rounded-lg shadow-sm border border-border">
                  <div className="rounded-md bg-slate-100 dark:bg-slate-900 px-4 py-3 text-sm text-foreground w-full md:w-auto">
                    <p className="font-semibold">Janela de busca</p>
                    <div className="flex flex-wrap gap-2 mt-2">
                      {formattedWindowDates.map((label) => (
                        <span
                          key={label}
                          className="rounded-full bg-slate-200 dark:bg-slate-800 px-3 py-1 text-xs text-foreground"
                        >
                          {label}
                        </span>
                      ))}
                    </div>
                    <p className="text-xs text-muted-foreground mt-2">
                      {baseSelectionDate
                        ? `Ancorada em ${parseDateOnly(baseSelectionDate).toLocaleDateString("pt-BR")}`
                        : "Use o filtro para abrir a janela de 2 dias úteis"}
                    </p>
                  </div>

                  <div className="rounded-md bg-slate-100 dark:bg-slate-900 px-4 py-3 text-sm text-foreground flex flex-col gap-2">
                    <div className="flex items-center justify-between gap-3">
                      <div>
                        <p className="font-semibold">Filtro de datas</p>
                        <p className="text-xs text-muted-foreground">
                          Visualize somente a data atual ou expanda ±2 dias úteis.
                        </p>
                      </div>
                      <Button
                        variant={isWindowFilterActive ? "default" : "outline"}
                        size="sm"
                        onClick={handleWindowFilterToggle}
                        disabled={loading || loadingWindow}
                      >
                        {isWindowFilterActive ? "Remover filtro" : "Aplicar janela"}
                      </Button>
                    </div>
                    {loadingWindow && <p className="text-xs text-muted-foreground">Atualizando janela...</p>}
                  </div>

                  <div className="rounded-md bg-slate-100 dark:bg-slate-900 px-4 py-3 text-sm text-foreground flex items-center gap-3 w-full md:w-auto">
                    <div>
                      <p className="font-semibold text-slate-900 dark:text-slate-100">Selecionados</p>
                      <p className="text-slate-800 dark:text-slate-100 font-medium">
                        Extrato {formatCurrency(selectedApiTotal)} · Cliente {formatCurrency(selectedErpTotal)}
                      </p>
                      {!totalsMatch && (selectedApi.size > 0 || selectedErp.size > 0) && (
                        <p className="text-xs text-destructive mt-1">Os totais precisam ser iguais</p>
                      )}
                      {totalsMatch && selectedApi.size > 0 && selectedErp.size > 0 && (
                        <p className="text-xs text-green-600 dark:text-green-400 mt-1">Totais alinhados para conciliação</p>
                      )}
                      {selectionWarning && <p className="text-xs text-destructive mt-1">{selectionWarning}</p>}
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
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                <div>
                  <h3 className="text-lg font-semibold text-foreground mb-4">Extrato</h3>
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
                          <div className="text-sm text-slate-800 dark:text-slate-100">
                            <p className="font-semibold text-lg text-foreground">{formatCurrency(item.amount)}</p>
                            <p className="text-sm mt-1">{parseDateOnly(item.date).toLocaleDateString("pt-BR")}</p>
                            <p className="text-sm mt-1">{item.desc_norm}</p>
                            <p className="text-xs text-emerald-800 dark:text-emerald-200 mt-1 font-semibold">ID API: {item.api_id}</p>
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
                  <h3 className="text-lg font-semibold text-foreground mb-4">Cliente</h3>
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
                          <div className="text-sm text-slate-800 dark:text-slate-100">
                            <p className="font-semibold text-lg text-foreground">{formatCurrency(item.amount)}</p>
                            <p className="text-sm mt-1">{parseDateOnly(item.date).toLocaleDateString("pt-BR")}</p>
                            <p className="text-sm mt-1">{item.desc_norm}</p>
                            <p className="text-xs text-emerald-800 dark:text-emerald-200 mt-1 font-semibold">CD Cliente: {item.cd_lancamento}</p>
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
            </>
          )}
        </>
      )}
    </div>
  )
}
