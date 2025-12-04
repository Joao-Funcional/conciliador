export function parseDateOnly(dateString: string) {
  const cleanDate = dateString.slice(0, 10)
  const [year, month, day] = cleanDate.split("-").map(Number)
  return new Date(year, (month || 1) - 1, day || 1)
}
