"use client"

export interface BankSelectorProps {
  tenants: string[]
  selectedTenant: string
  onTenantChange: (tenant: string) => void
  banks: { code: string; name: string }[]
  selectedBank: string
  onBankChange: (bank: string) => void
  accounts: string[]
  selectedAccount: string
  onAccountChange: (account: string) => void
}

export function BankSelector({
  tenants,
  selectedTenant,
  onTenantChange,
  banks,
  selectedBank,
  onBankChange,
  accounts,
  selectedAccount,
  onAccountChange,
}: BankSelectorProps) {
  return (
    <div className="flex flex-col sm:flex-row gap-4">
      <div className="flex-1">
        <label className="block text-sm font-medium text-foreground mb-2">Empresa</label>
        <select
          value={selectedTenant}
          onChange={(e) => onTenantChange(e.target.value)}
          className="w-full px-3 py-2 border border-input rounded-md bg-card text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
        >
          {tenants.map((t) => (
            <option key={t} value={t}>
              {t}
            </option>
          ))}
        </select>
      </div>

      <div className="flex-1">
        <label className="block text-sm font-medium text-foreground mb-2">Banco</label>
        <select
          value={selectedBank}
          onChange={(e) => onBankChange(e.target.value)}
          className="w-full px-3 py-2 border border-input rounded-md bg-card text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
        >
          {banks.map((b) => (
            <option key={b.code} value={b.code}>
              {b.name}
            </option>
          ))}
        </select>
      </div>

      <div className="flex-1">
        <label className="block text-sm font-medium text-foreground mb-2">Conta</label>
        <select
          value={selectedAccount}
          onChange={(e) => onAccountChange(e.target.value)}
          className="w-full px-3 py-2 border border-input rounded-md bg-card text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
        >
          {accounts.map((a) => (
            <option key={a} value={a}>
              {a}
            </option>
          ))}
        </select>
      </div>
    </div>
  )
}
