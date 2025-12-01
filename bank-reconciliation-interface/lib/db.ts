import { Pool } from "pg"

const pool = new Pool({
  host: process.env.PGHOST ?? process.env.PG_HOST ?? "localhost",
  port: Number(process.env.PGPORT ?? process.env.PG_PORT ?? 5432),
  database: process.env.PGDATABASE ?? process.env.PG_DB ?? "databricks",
  user: process.env.PGUSER ?? process.env.PG_USER ?? "postgres",
  password: process.env.PGPASSWORD ?? process.env.PG_PASSWORD ?? "joao12345",
  ssl: process.env.PGSSL === "true" ? { rejectUnauthorized: false } : undefined,
})

export async function query<T = any>(text: string, params: any[] = []) {
  const client = await pool.connect()
  try {
    const res = await client.query<T>(text, params)
    return res.rows
  } finally {
    client.release()
  }
}

export async function getClient() {
  return pool.connect()
}
