"use client"

import { useEffect, useMemo, useState } from "react"
import { BankSelector } from "@/components/bank-selector"
import { AnnualSummary, MonthlyData } from "@/components/annual-summary"
import { MonthlyCalendar, DailyData } from "@/components/monthly-calendar"
import { UnreconciledDetails } from "@/components/unreconciled-details"

interface OptionRow {
  tenant_id: string
  bank_code: string
  acc_tail: string
}

export default function Home() {
  const [options, setOptions] = useState<OptionRow[]>([])
  const [monthlyData, setMonthlyData] = useState<MonthlyData[]>([])
  const [dailyData, setDailyData] = useState<DailyData[]>([])
  const [selectedTenant, setSelectedTenant] = useState<string>("")
  const [selectedBank, setSelectedBank] = useState<string>("")
  const [selectedAccount, setSelectedAccount] = useState<string>("")
  const [selectedMonth, setSelectedMonth] = useState<string | null>(null)
  const [selectedDate, setSelectedDate] = useState<string | null>(null)
  const [showReconciled, setShowReconciled] = useState(false)
  const [loadingSummary, setLoadingSummary] = useState(false)

  useEffect(() => {
    const loadOptions = async () => {
      const response = await fetch("/api/options")
      const payload = await response.json()
      setOptions(payload.options ?? [])
    }

    loadOptions()
  }, [])

  useEffect(() => {
    // Ajusta tenant/banco/conta conforme opções disponíveis
    if (options.length === 0) return

    if (!selectedTenant) {
      setSelectedTenant(options[0].tenant_id)
      return
    }

    const banks = options.filter((o) => o.tenant_id === selectedTenant).map((o) => o.bank_code)
    if (!banks.includes(selectedBank)) {
      setSelectedBank(banks[0] ?? "")
      return
    }

    const accounts = options
      .filter((o) => o.tenant_id === selectedTenant && o.bank_code === selectedBank)
      .map((o) => o.acc_tail)
    if (!accounts.includes(selectedAccount)) {
      setSelectedAccount(accounts[0] ?? "")
      return
    }
  }, [options, selectedTenant, selectedBank, selectedAccount])

  useEffect(() => {
    const fetchSummary = async () => {
      if (!selectedTenant || !selectedBank || !selectedAccount) return
      setLoadingSummary(true)
      setSelectedMonth(null)
      setSelectedDate(null)
      setShowReconciled(false)

      const params = new URLSearchParams({
        tenant: selectedTenant,
        bank: selectedBank,
        accTail: selectedAccount,
      })

      const response = await fetch(`/api/summary?${params.toString()}`)
      if (response.ok) {
        const payload = await response.json()
        const monthlyMap = new Map<string, MonthlyData>()
        const dailyMap = new Map<string, DailyData>()

        ;(payload.monthly ?? []).forEach((item: MonthlyData) => {
          const existing = monthlyMap.get(item.month)
          monthlyMap.set(item.month, {
            ...item,
            api_matched_abs: (existing?.api_matched_abs ?? 0) + (item.api_matched_abs ?? 0),
            erp_matched_abs: (existing?.erp_matched_abs ?? 0) + (item.erp_matched_abs ?? 0),
            api_unrec_abs: (existing?.api_unrec_abs ?? 0) + (item.api_unrec_abs ?? 0),
            erp_unrec_abs: (existing?.erp_unrec_abs ?? 0) + (item.erp_unrec_abs ?? 0),
            unrec_total_abs: (existing?.unrec_total_abs ?? 0) + (item.unrec_total_abs ?? 0),
          })
        })

        ;(payload.daily ?? []).forEach((item: DailyData) => {
          const existing = dailyMap.get(item.date)
          dailyMap.set(item.date, {
            ...item,
            api_matched_abs: (existing?.api_matched_abs ?? 0) + (item.api_matched_abs ?? 0),
            erp_matched_abs: (existing?.erp_matched_abs ?? 0) + (item.erp_matched_abs ?? 0),
            api_unrec_abs: (existing?.api_unrec_abs ?? 0) + (item.api_unrec_abs ?? 0),
            erp_unrec_abs: (existing?.erp_unrec_abs ?? 0) + (item.erp_unrec_abs ?? 0),
            unrec_total_abs: (existing?.unrec_total_abs ?? 0) + (item.unrec_total_abs ?? 0),
            unrec_diff: (existing?.unrec_diff ?? 0) + (item.unrec_diff ?? 0),
          })
        })

        setMonthlyData(Array.from(monthlyMap.values()))
        setDailyData(Array.from(dailyMap.values()))
      } else {
        setMonthlyData([])
        setDailyData([])
      }
      setLoadingSummary(false)
    }

    fetchSummary()
  }, [selectedTenant, selectedBank, selectedAccount])

  const tenants = useMemo(() => {
    return Array.from(new Set(options.map((d) => d.tenant_id)))
  }, [options])

  const banks = useMemo(() => {
    return Array.from(new Set(options.filter((d) => d.tenant_id === selectedTenant).map((d) => d.bank_code)))
  }, [options, selectedTenant])

  const accounts = useMemo(() => {
    return Array.from(
      new Set(
        options
          .filter((d) => d.tenant_id === selectedTenant && d.bank_code === selectedBank)
          .map((d) => d.acc_tail),
      ),
    )
  }, [options, selectedTenant, selectedBank])

  const handleBackFromMonth = () => {
    setSelectedMonth(null)
    setSelectedDate(null)
  }

  const handleBackFromDay = () => {
    setSelectedDate(null)
  }

  const dailyForMonth = selectedMonth
    ? dailyData.filter((d) => d.date.startsWith(selectedMonth.slice(0, 7)))
    : []

  return (
    <main className="min-h-screen bg-background">
      <div className="sticky top-0 z-50 border-b border-border bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
          <h1 className="text-3xl font-bold text-foreground mb-4">Conciliador Bancário</h1>
          <BankSelector
            tenants={tenants}
            selectedTenant={selectedTenant}
            onTenantChange={setSelectedTenant}
            banks={banks}
            selectedBank={selectedBank}
            onBankChange={setSelectedBank}
            accounts={accounts}
            selectedAccount={selectedAccount}
            onAccountChange={setSelectedAccount}
          />
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {loadingSummary ? (
          <p className="text-muted-foreground">Carregando dados de conciliação...</p>
        ) : selectedDate !== null && selectedMonth ? (
          <UnreconciledDetails
            tenantId={selectedTenant}
            bankCode={selectedBank}
            accTail={selectedAccount}
            date={selectedDate}
            showReconciled={showReconciled}
            onShowReconciledChange={setShowReconciled}
            onBack={handleBackFromDay}
          />
        ) : selectedMonth !== null ? (
          <MonthlyCalendar
            monthDate={selectedMonth}
            dailyData={dailyForMonth}
            onDayClick={setSelectedDate}
            onBack={handleBackFromMonth}
            tenantId={selectedTenant}
            bankCode={selectedBank}
            accTail={selectedAccount}
          />
        ) : (
          <AnnualSummary monthlyData={monthlyData} onMonthClick={setSelectedMonth} />
        )}
      </div>
    </main>
  )
}
