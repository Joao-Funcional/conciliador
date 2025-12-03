export function normalizeAmount(value: unknown): number {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value
  }

  if (value === null || value === undefined) {
    return 0
  }

  const raw = String(value).trim()
  if (!raw) {
    return 0
  }

  const cleaned = raw.replace(/[^\d.,-]/g, "")
  const lastComma = cleaned.lastIndexOf(",")
  const lastDot = cleaned.lastIndexOf(".")

  let normalized = cleaned
  if (cleaned.includes(",") && lastComma > lastDot) {
    normalized = cleaned.replace(/\./g, "").replace(",", ".")
  } else {
    normalized = cleaned.replace(/,/g, "")
  }

  const parsed = Number.parseFloat(normalized)
  return Number.isFinite(parsed) ? parsed : 0
}
