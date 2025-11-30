"use client"

import { useState, useMemo } from "react"
import { BankSelector } from "@/components/bank-selector"
import { AnnualSummary } from "@/components/annual-summary"
import { MonthlyCalendar } from "@/components/monthly-calendar"
import { UnreconciledDetails } from "@/components/unreconciled-details"
import { mockData } from "@/lib/mock-data"

export default function Home() {
  const [selectedTenant, setSelectedTenant] = useState("anderle")
  const [selectedBank, setSelectedBank] = useState("237")
  const [selectedAccount, setSelectedAccount] = useState("7242")
  const [selectedMonth, setSelectedMonth] = useState<number | null>(null)
  const [selectedDay, setSelectedDay] = useState<number | null>(null)
  const [showReconciled, setShowReconciled] = useState(false)

  const tenants = useMemo(() => {
    return Array.from(new Set(mockData.daily.map((d) => d.tenant_id)))
  }, [])

  const banks = useMemo(() => {
    return Array.from(new Set(mockData.daily.filter((d) => d.tenant_id === selectedTenant).map((d) => d.bank_code)))
  }, [selectedTenant])

  const accounts = useMemo(() => {
    return Array.from(
      new Set(
        mockData.daily
          .filter((d) => d.tenant_id === selectedTenant && d.bank_code === selectedBank)
          .map((d) => d.acc_tail),
      ),
    )
  }, [selectedTenant, selectedBank])

  const monthlyData = useMemo(() => {
    return mockData.monthly.filter(
      (m) => m.tenant_id === selectedTenant && m.bank_code === selectedBank && m.acc_tail === selectedAccount,
    )
  }, [selectedTenant, selectedBank, selectedAccount])

  const dailyData = useMemo(() => {
    return mockData.daily.filter(
      (d) => d.tenant_id === selectedTenant && d.bank_code === selectedBank && d.acc_tail === selectedAccount,
    )
  }, [selectedTenant, selectedBank, selectedAccount])

  const handleBackFromMonth = () => {
    setSelectedMonth(null)
    setSelectedDay(null)
  }

  const handleDayClick = (day: number) => {
    setSelectedDay(day)
  }

  const handleBackFromDay = () => {
    setSelectedDay(null)
  }

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
        {selectedDay !== null ? (
          <UnreconciledDetails
            tenantId={selectedTenant}
            bankCode={selectedBank}
            accTail={selectedAccount}
            date={new Date(2025, selectedMonth! - 1, selectedDay).toISOString().split("T")[0]}
            showReconciled={showReconciled}
            onShowReconciledChange={setShowReconciled}
            onBack={handleBackFromDay}
          />
        ) : selectedMonth !== null ? (
          <MonthlyCalendar
            month={selectedMonth}
            dailyData={dailyData}
            onDayClick={handleDayClick}
            onBack={handleBackFromMonth}
          />
        ) : (
          <AnnualSummary monthlyData={monthlyData} onMonthClick={setSelectedMonth} />
        )}
      </div>
    </main>
  )
}
