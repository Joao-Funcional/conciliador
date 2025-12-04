export function parseDateOnly(dateString: string) {
  const parsedDate = new Date(dateString)

  if (!Number.isNaN(parsedDate.getTime())) {
    return new Date(parsedDate.getFullYear(), parsedDate.getMonth(), parsedDate.getDate())
  }

  const [year, month, day] = dateString.split("-").map(Number)
  return new Date(year, (month || 1) - 1, day || 1)
}
