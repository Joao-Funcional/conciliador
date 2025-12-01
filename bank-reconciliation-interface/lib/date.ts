export function parseDateOnly(dateString: string) {
  const [year, month, day] = dateString.split("-").map(Number)
  return new Date(year, (month || 1) - 1, day || 1)
}
