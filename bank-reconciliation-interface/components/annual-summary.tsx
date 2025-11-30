"use client"

import { Card } from "@/components/ui/card"

export interface MonthlyData {
  tenant_id: string
  bank_code: string
  acc_tail: string
  month: string
  api_matched_abs: number
  erp_matched_abs: number
  api_unrec_abs: number
  erp_unrec_abs: number
  unrec_total_abs: number
}

export interface AnnualSummaryProps {
  monthlyData: MonthlyData[]
  onMonthClick: (month: string) => void
}

export function AnnualSummary({ monthlyData, onMonthClick }: AnnualSummaryProps) {
  const formatCurrency = (value: number) => {
    return new Intl.NumberFormat("pt-BR", {
      style: "currency",
      currency: "BRL",
    }).format(value)
  }

  const sortedMonths = [...monthlyData].sort(
    (a, b) => new Date(a.month).getTime() - new Date(b.month).getTime(),
  )

  const yearLabel = sortedMonths[0]
    ? new Date(sortedMonths[0].month).getFullYear()
    : new Date().getFullYear()

  return (
    <div>
      <div className="mb-6">
        <h2 className="text-2xl font-bold text-foreground mb-2">Resumo Anual {yearLabel}</h2>
        <p className="text-muted-foreground">Clique em um mês para detalhar a conciliação</p>
      </div>

      {sortedMonths.length === 0 ? (
        <p className="text-muted-foreground">Nenhum dado encontrado para o filtro selecionado.</p>
      ) : (
        <div className="space-y-2">
          {sortedMonths.map((data) => {
            const totalApi = (data?.api_matched_abs ?? 0) + (data?.api_unrec_abs ?? 0)
            const totalErp = (data?.erp_matched_abs ?? 0) + (data?.erp_unrec_abs ?? 0)
            const unreconciled = data?.unrec_total_abs ?? 0
            const label = new Date(data.month).toLocaleString("pt-BR", { month: "short" })

            return (
              <Card
                key={data.month}
                className="p-4 cursor-pointer hover:bg-accent transition-colors"
                onClick={() => onMonthClick(data.month)}
              >
                <div className="flex items-center justify-between">
                  <div className="min-w-12">
                    <h3 className="text-lg font-semibold text-foreground capitalize">{label}</h3>
                    <p className="text-sm text-muted-foreground">{yearLabel}</p>
                  </div>

                  <div className="grid grid-cols-3 gap-8">
                    <div>
                      <p className="text-xs text-muted-foreground mb-1">API</p>
                      <p className="font-semibold text-foreground">{formatCurrency(totalApi)}</p>
                    </div>
                    <div>
                      <p className="text-xs text-muted-foreground mb-1">ERP</p>
                      <p className="font-semibold text-foreground">{formatCurrency(totalErp)}</p>
                    </div>
                    <div>
                      <p className="text-xs text-muted-foreground mb-1">Não Conciliado</p>
                      <p className={`font-semibold ${unreconciled > 0 ? "text-destructive" : "text-green-600"}`}>
                        {formatCurrency(unreconciled)}
                      </p>
                    </div>
                  </div>
                </div>
              </Card>
            )
          })}
        </div>
      )}
    </div>
  )
}
