"use client"
import { useState } from "react"
import { Card } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { ChevronLeft, Download } from "lucide-react"
import { parseDateOnly } from "@/lib/date"

export interface DailyData {
  tenant_id: string
  bank_code: string
  acc_tail: string
  date: string
  api_matched_abs: number
  erp_matched_abs: number
  api_unrec_abs: number
  erp_unrec_abs: number
  unrec_total_abs: number
  unrec_diff: number
}

export interface MonthlyCalendarProps {
  monthDate: string
  dailyData: DailyData[]
  onDayClick: (date: string) => void
  onBack: () => void
  tenantId: string
  bankCode: string
  accTail: string
}

export function MonthlyCalendar({
  monthDate,
  dailyData,
  onDayClick,
  onBack,
  tenantId,
  bankCode,
  accTail,
}: MonthlyCalendarProps) {
  const [generating, setGenerating] = useState(false)

  const baseDate = parseDateOnly(monthDate)
  const year = baseDate.getFullYear()
  const month = baseDate.getMonth()
  const daysInMonth = new Date(year, month + 1, 0).getDate()
  const firstDay = new Date(year, month, 1).getDay()
  const monthName = baseDate.toLocaleString("pt-BR", { month: "long", year: "numeric" })

  const formatCurrency = (value: number) => {
    return new Intl.NumberFormat("pt-BR", {
      style: "currency",
      currency: "BRL",
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(value)
  }

  const getDayData = (day: number) => {
    const dateStr = `${year}-${String(month + 1).padStart(2, "0")}-${String(day).padStart(2, "0")}`
    return dailyData.find((d) => d.date === dateStr)
  }

  const reconciliatedDays = dailyData.filter((d) => d.unrec_total_abs === 0)

  const generateTxt = async () => {
    if (reconciliatedDays.length === 0) return

    const cnpj = window.prompt("Informe o CNPJ (apenas números) para o TXT")
    const tipo = window.prompt("Tipo do lançamento (X ou V)", "X")
    const contaDebito = window.prompt("Conta contábil de débito")
    const contaCredito = window.prompt("Conta contábil de crédito")

    if (!cnpj || !tipo || !contaDebito || !contaCredito) {
      window.alert("Preencha todos os campos obrigatórios para gerar o arquivo.")
      return
    }

    try {
      setGenerating(true)
      const dateFrom = `${year}-${String(month + 1).padStart(2, "0")}-01`
      const dateTo = `${year}-${String(month + 1).padStart(2, "0")}-${String(daysInMonth).padStart(2, "0")}`

      const response = await fetch("/api/generate-txt", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          tenant: tenantId,
          cnpj,
          accTail,
          bankCode,
          dateFrom,
          dateTo,
          tipo: tipo.toUpperCase(),
          contaDebito,
          contaCredito,
        }),
      })

      if (!response.ok) {
        const error = await response.json().catch(() => ({ error: "Falha ao gerar TXT" }))
        throw new Error(error.error)
      }

      const blob = await response.blob()
      const url = window.URL.createObjectURL(blob)
      const link = document.createElement("a")
      link.href = url
      link.download = `conciliacao_${String(month + 1).padStart(2, "0")}_${year}.txt`
      document.body.appendChild(link)
      link.click()
      document.body.removeChild(link)
      window.URL.revokeObjectURL(url)
    } catch (error: any) {
      console.error(error)
      window.alert(error?.message ?? "Erro ao gerar arquivo")
    } finally {
      setGenerating(false)
    }
  }

  const dayLabels = ["Dom", "Seg", "Ter", "Qua", "Qui", "Sex", "Sab"]
  const days = []

  for (let i = 0; i < firstDay; i++) {
    days.push(null)
  }

  for (let i = 1; i <= daysInMonth; i++) {
    days.push(i)
  }

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-4">
          <Button variant="outline" size="sm" onClick={onBack} className="gap-2 bg-transparent">
            <ChevronLeft className="h-4 w-4" />
            Voltar
          </Button>
          <h2 className="text-2xl font-bold text-foreground capitalize">{monthName}</h2>
        </div>

        {reconciliatedDays.length > 0 && (
          <Button
            onClick={generateTxt}
            className="gap-2 bg-primary hover:bg-primary/90"
            disabled={generating}
          >
            <Download className="h-4 w-4" />
            {generating ? "Gerando..." : "Gerar TXT"}
          </Button>
        )}
      </div>

      <Card className="p-6">
        <div className="grid grid-cols-7 gap-2">
          {dayLabels.map((label) => (
            <div key={label} className="text-center font-semibold text-muted-foreground text-sm py-2">
              {label}
            </div>
          ))}

          {days.map((day, index) => {
            const dayData = day ? getDayData(day) : null
            const isReconciliated = dayData && dayData.unrec_total_abs === 0
            const isClickable = Boolean(day && dayData)

            return (
              <div
                key={index}
                className={`aspect-square p-2 rounded-lg border text-center flex flex-col justify-center ${
                  !day
                    ? "bg-muted"
                    : isReconciliated
                      ? "bg-green-50 dark:bg-green-950 border-green-200 dark:border-green-800"
                      : dayData?.unrec_total_abs
                        ? "bg-orange-50 dark:bg-orange-950 border-orange-200 dark:border-orange-800"
                        : "bg-card border-border text-muted-foreground"
                } ${isClickable ? "cursor-pointer hover:bg-accent" : "cursor-not-allowed"}`}
                onClick={() => {
                  if (!isClickable) return
                  const dateStr = `${year}-${String(month + 1).padStart(2, "0")}-${String(day).padStart(2, "0")}`
                  onDayClick(dateStr)
                }}
              >
                {day && (
                  <>
                    <div className="text-sm font-semibold text-foreground">{day}</div>
                    {dayData && (
                      <>
                        <div className="text-[13px] leading-tight mt-1 space-y-1">
                          <p
                            className={`font-semibold whitespace-nowrap ${
                              dayData.unrec_total_abs
                                ? "text-amber-900 dark:text-amber-100"
                                : "text-emerald-800 dark:text-emerald-100"
                            }`}
                          >
                            API: {formatCurrency(dayData.api_matched_abs)}
                          </p>
                          <p
                            className={`font-semibold whitespace-nowrap ${
                              dayData.unrec_total_abs
                                ? "text-amber-900 dark:text-amber-100"
                                : "text-emerald-800 dark:text-emerald-100"
                            }`}
                          >
                            ERP: {formatCurrency(dayData.erp_matched_abs)}
                          </p>
                          {dayData.unrec_total_abs > 0 && (
                            <p className="text-xs text-destructive font-semibold mt-1 whitespace-nowrap">
                              ⚠ {formatCurrency(dayData.unrec_total_abs)}
                            </p>
                          )}
                        </div>
                      </>
                    )}
                    {!dayData && (
                      <p className="text-xs text-muted-foreground mt-1">Sem registro</p>
                    )}
                  </>
                )}
              </div>
            )
          })}
        </div>
      </Card>

      {reconciliatedDays.length > 0 && (
        <div className="mt-4 p-4 bg-green-50 dark:bg-green-950 border border-green-200 dark:border-green-800 rounded-lg">
          <p className="text-sm text-green-700 dark:text-green-300">
            ✓ {reconciliatedDays.length} dia(s) totalmente conciliado(s)
          </p>
        </div>
      )}
    </div>
  )
}
