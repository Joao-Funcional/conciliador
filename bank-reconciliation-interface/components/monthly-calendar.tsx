"use client"
import { Card } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { ChevronLeft, Download } from "lucide-react"

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
  month: number
  dailyData: DailyData[]
  onDayClick: (day: number) => void
  onBack: () => void
}

export function MonthlyCalendar({ month, dailyData, onDayClick, onBack }: MonthlyCalendarProps) {
  const daysInMonth = new Date(2025, month, 0).getDate()
  const firstDay = new Date(2025, month - 1, 1).getDay()
  const monthName = new Date(2025, month - 1).toLocaleString("pt-BR", { month: "long", year: "numeric" })

  const formatCurrency = (value: number) => {
    return new Intl.NumberFormat("pt-BR", {
      style: "currency",
      currency: "BRL",
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(value)
  }

  const getDayData = (day: number) => {
    const dateStr = `2025-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`
    return dailyData.find((d) => d.date === dateStr)
  }

  const reconciliatedDays = dailyData.filter((d) => d.unrec_total_abs === 0)

  const generateTxt = () => {
    const txt = reconciliatedDays
      .map((d) => `${d.date};${d.api_matched_abs.toFixed(2)};${d.erp_matched_abs.toFixed(2)}`)
      .join("\n")

    const element = document.createElement("a")
    element.setAttribute("href", "data:text/plain;charset=utf-8," + encodeURIComponent(txt))
    element.setAttribute("download", `conciliacao_${month.toString().padStart(2, "0")}_2025.txt`)
    element.style.display = "none"
    document.body.appendChild(element)
    element.click()
    document.body.removeChild(element)
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
          <Button onClick={generateTxt} className="gap-2 bg-primary hover:bg-primary/90">
            <Download className="h-4 w-4" />
            Gerar TXT
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

            return (
              <div
                key={index}
                className={`aspect-square p-2 rounded-lg border text-center flex flex-col justify-center ${
                  !day
                    ? "bg-muted"
                    : isReconciliated
                      ? "bg-green-50 dark:bg-green-950 border-green-200 dark:border-green-800 cursor-pointer hover:bg-green-100 dark:hover:bg-green-900"
                      : dayData?.unrec_total_abs
                        ? "bg-orange-50 dark:bg-orange-950 border-orange-200 dark:border-orange-800 cursor-pointer hover:bg-orange-100 dark:hover:bg-orange-900"
                        : "bg-card border-border"
                }`}
                onClick={() => day && onDayClick(day)}
              >
                {day && (
                  <>
                    <div className="text-sm font-semibold text-foreground">{day}</div>
                    {dayData && (
                      <>
                        <div className="text-xs mt-1">
                          <p className="text-muted-foreground">API: {formatCurrency(dayData.api_matched_abs)}</p>
                          <p className="text-muted-foreground">ERP: {formatCurrency(dayData.erp_matched_abs)}</p>
                          {dayData.unrec_total_abs > 0 && (
                            <p className="text-destructive font-semibold mt-1">
                              ⚠ {formatCurrency(dayData.unrec_total_abs)}
                            </p>
                          )}
                        </div>
                      </>
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
