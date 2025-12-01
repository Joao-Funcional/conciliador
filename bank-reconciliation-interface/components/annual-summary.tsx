"use client"

import { Card } from "@/components/ui/card"
import { parseDateOnly } from "@/lib/date"

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

  const availableYears = monthlyData.map((data) => parseDateOnly(data.month).getFullYear())
  const yearLabel = availableYears.length > 0 ? Math.max(...availableYears) : new Date().getFullYear()

  const monthlyMap = new Map(monthlyData.map((data) => [data.month, data]))

  const defaultValues: MonthlyData = {
    tenant_id: monthlyData[0]?.tenant_id ?? "",
    bank_code: monthlyData[0]?.bank_code ?? "",
    acc_tail: monthlyData[0]?.acc_tail ?? "",
    month: "",
    api_matched_abs: 0,
    erp_matched_abs: 0,
    api_unrec_abs: 0,
    erp_unrec_abs: 0,
    unrec_total_abs: 0,
  }

  const months = Array.from({ length: 12 }, (_, index) => {
    const monthKey = `${yearLabel}-${String(index + 1).padStart(2, "0")}-01`
    return monthlyMap.get(monthKey) ?? { ...defaultValues, month: monthKey }
  })

  return (
    <div>
      <div className="mb-6">
        <h2 className="text-2xl font-bold text-foreground mb-2">Resumo Anual {yearLabel}</h2>
        <p className="text-muted-foreground">Clique em um mês para detalhar a conciliação</p>
      </div>

      {months.length === 0 ? (
        <p className="text-muted-foreground">Nenhum dado encontrado para o filtro selecionado.</p>
      ) : (
        <div className="space-y-2">
          {months.map((data) => {
            const totalApi = (data?.api_matched_abs ?? 0) + (data?.api_unrec_abs ?? 0)
            const totalErp = (data?.erp_matched_abs ?? 0) + (data?.erp_unrec_abs ?? 0)
            const unreconciled = data?.unrec_total_abs ?? 0
            const label = parseDateOnly(data.month).toLocaleString("pt-BR", { month: "short" })

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
