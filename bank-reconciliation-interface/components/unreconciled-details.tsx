"use client"
import { useEffect, useMemo, useState } from "react"
import { Card } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { ChevronLeft } from "lucide-react"

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
  erp_amount: number
  erp_desc: string
}

type GroupedMatch = {
  api_uid: string
  api_amount: number
  api_desc: string
  stage: string
  erp: Array<Pick<Match, "erp_uid" | "erp_amount" | "erp_desc">>
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
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    const fetchData = async () => {
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
        setUnrecApi(payload.unreconciledApi ?? [])
        setUnrecErp(payload.unreconciledErp ?? [])
        setMatches(payload.matches ?? [])
      } catch (err: any) {
        console.error(err)
        setError(err?.message ?? "Erro ao buscar dados")
      } finally {
        setLoading(false)
      }
    }

    fetchData()
  }, [tenantId, bankCode, accTail, date])

  const formatCurrency = (value: number) => {
    return new Intl.NumberFormat("pt-BR", {
      style: "currency",
      currency: "BRL",
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(value)
  }

  const dateObj = new Date(date)
  const formattedDate = dateObj.toLocaleDateString("pt-BR")

  const groupedMatches = useMemo(() => {
    const groups: Record<string, GroupedMatch> = {}

    matches.forEach((match) => {
      if (!groups[match.api_uid]) {
        groups[match.api_uid] = {
          api_uid: match.api_uid,
          api_amount: match.api_amount,
          api_desc: match.api_desc,
          stage: match.stage,
          erp: [],
        }
      }

      groups[match.api_uid].erp.push({
        erp_uid: match.erp_uid,
        erp_amount: match.erp_amount,
        erp_desc: match.erp_desc,
      })
    })

    return Object.values(groups)
  }, [matches])

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
          <div className="grid grid-cols-2 gap-6">
            <div>
              <h3 className="text-lg font-semibold text-foreground mb-4">API Pluggy</h3>
              <div className="space-y-2">
                {unrecApi.length > 0 ? (
                  unrecApi.map((item) => (
                    <Card
                      key={item.api_id}
                      className="p-3 bg-orange-50 dark:bg-orange-950 border-orange-200 dark:border-orange-800"
                    >
                      <div className="text-sm">
                        <p className="font-semibold text-foreground">{formatCurrency(item.amount)}</p>
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
                      className="p-3 bg-orange-50 dark:bg-orange-950 border-orange-200 dark:border-orange-800"
                    >
                      <div className="text-sm">
                        <p className="font-semibold text-foreground">{formatCurrency(item.amount)}</p>
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
              {groupedMatches.length > 0 ? (
                <div className="space-y-3">
                  {groupedMatches.map((match) => (
                    <Card
                      key={match.api_uid}
                      className="p-4 bg-green-50 dark:bg-green-950 border-green-200 dark:border-green-800"
                    >
                      <div className="mb-3">
                        <p className="text-xs text-muted-foreground mb-1">API</p>
                        <p className="font-semibold text-foreground text-sm">{formatCurrency(match.api_amount)}</p>
                        <p className="text-xs text-muted-foreground mt-1">{match.api_desc}</p>
                        <p className="text-xs text-muted-foreground mt-1">Estágio: {match.stage}</p>
                      </div>

                      <div className="space-y-2">
                        {match.erp.map((erpMatch) => (
                          <div
                            key={`${match.api_uid}-${erpMatch.erp_uid}`}
                            className="grid grid-cols-3 gap-2 items-start"
                          >
                            <div className="text-xs text-muted-foreground">↳ ERP</div>
                            <div className="col-span-2">
                              <p className="font-semibold text-foreground text-sm">
                                {formatCurrency(erpMatch.erp_amount)}
                              </p>
                              <p className="text-xs text-muted-foreground mt-1">{erpMatch.erp_desc}</p>
                            </div>
                          </div>
                        ))}
                      </div>
                    </Card>
                  ))}
                </div>
              ) : (
                <div className="text-muted-foreground">Nenhuma transação conciliada para este dia.</div>
              )}
            </div>
          )}
        </>
      )}
    </div>
  )
}
