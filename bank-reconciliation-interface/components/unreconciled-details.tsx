"use client"
import { Card } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { ChevronLeft } from "lucide-react"
import { mockData } from "@/lib/mock-data"

export interface UnreconciledDetailsProps {
  tenantId: string
  bankCode: string
  accTail: string
  date: string
  showReconciled: boolean
  onShowReconciledChange: (show: boolean) => void
  onBack: () => void
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
  const formatCurrency = (value: number) => {
    return new Intl.NumberFormat("pt-BR", {
      style: "currency",
      currency: "BRL",
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(value)
  }

  const unreconciledApi = mockData.unreconciledApi.filter(
    (u) => u.tenant_id === tenantId && u.bank_code === bankCode && u.acc_tail === accTail && u.date === date,
  )

  const unreconciledErp = mockData.unreconciledErp.filter(
    (u) => u.tenant_id === tenantId && u.bank_code === bankCode && u.acc_tail === accTail && u.date === date,
  )

  const matches = mockData.matches.filter((m) => {
    const apiItem = mockData.unreconciledApi.find((u) => u.api_id === m.api_uid)
    const erpItem = mockData.unreconciledErp.find((u) => u.cd_lancamento === m.erp_uid)
    return (
      apiItem?.tenant_id === tenantId &&
      apiItem?.bank_code === bankCode &&
      apiItem?.acc_tail === accTail &&
      apiItem?.date === date &&
      erpItem?.tenant_id === tenantId
    )
  })

  const dateObj = new Date(date)
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

      <div className="grid grid-cols-2 gap-6">
        {/* API Side */}
        <div>
          <h3 className="text-lg font-semibold text-foreground mb-4">API Pluggy</h3>
          <div className="space-y-2">
            {unreconciledApi.length > 0 ? (
              unreconciledApi.map((item) => (
                <Card
                  key={item.api_id}
                  className="p-3 bg-orange-50 dark:bg-orange-950 border-orange-200 dark:border-orange-800"
                >
                  <div className="text-sm">
                    <p className="font-semibold text-foreground">{formatCurrency(item.amount)}</p>
                    <p className="text-xs text-muted-foreground mt-1">{item.desc}</p>
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

        {/* ERP Side */}
        <div>
          <h3 className="text-lg font-semibold text-foreground mb-4">ERP</h3>
          <div className="space-y-2">
            {unreconciledErp.length > 0 ? (
              unreconciledErp.map((item) => (
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

      {showReconciled && matches.length > 0 && (
        <div className="mt-8">
          <h3 className="text-lg font-semibold text-foreground mb-4">Transações Conciliadas</h3>
          <div className="space-y-3">
            {matches.map((match) => {
              const apiItem = mockData.unreconciledApi.find((u) => u.api_id === match.api_uid)
              const erpItem = mockData.unreconciledErp.find((u) => u.cd_lancamento === match.erp_uid)

              if (!apiItem || !erpItem) return null

              return (
                <Card
                  key={match.api_uid}
                  className="p-4 bg-green-50 dark:bg-green-950 border-green-200 dark:border-green-800"
                >
                  <div className="grid grid-cols-3 gap-4">
                    <div>
                      <p className="text-xs text-muted-foreground mb-1">API</p>
                      <p className="font-semibold text-foreground text-sm">{formatCurrency(apiItem.amount)}</p>
                      <p className="text-xs text-muted-foreground mt-1">{apiItem.desc}</p>
                    </div>
                    <div className="flex items-center justify-center">
                      <span className="text-green-600 font-bold">↔</span>
                    </div>
                    <div>
                      <p className="text-xs text-muted-foreground mb-1">ERP</p>
                      <p className="font-semibold text-foreground text-sm">{formatCurrency(erpItem.amount)}</p>
                      <p className="text-xs text-muted-foreground mt-1">{erpItem.desc_norm}</p>
                    </div>
                  </div>
                  <div className="mt-2 text-xs text-muted-foreground">Motivo: {match.priodiff}</div>
                </Card>
              )
            })}
          </div>
        </div>
      )}
    </div>
  )
}
