import { NextResponse } from "next/server"
import { promises as fs } from "fs"
import path from "path"
import os from "os"
import { execFile } from "child_process"
import { promisify } from "util"

const execFileAsync = promisify(execFile)

export async function POST(request: Request) {
  const body = await request.json().catch(() => null)
  if (!body) {
    return NextResponse.json({ error: "Payload inválido" }, { status: 400 })
  }

  const {
    tenant,
    cnpj,
    accTail,
    bankCode,
    dateFrom,
    dateTo,
    tipo,
    contaDebito,
    contaCredito,
    usuario = "",
    filial = "",
    scp = "",
  } = body

  if (!tenant || !cnpj || !accTail || !dateFrom || !dateTo || !tipo || !contaDebito || !contaCredito) {
    return NextResponse.json({ error: "Campos obrigatórios ausentes" }, { status: 400 })
  }

  const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "conciliador-"))
  const outputPath = path.join(tmpDir, `conciliacao_${tenant}_${accTail}.txt`)
  const scriptPath = path.resolve(process.cwd(), "..", "txt.py")

  try {
    const args = [
      scriptPath,
      "--tenant",
      tenant,
      "--cnpj",
      cnpj,
      "--acc-tail",
      accTail,
      "--bank-code",
      bankCode ?? "",
      "--date-from",
      dateFrom,
      "--date-to",
      dateTo,
      "--tipo",
      tipo,
      "--saida",
      outputPath,
      "--usuario",
      usuario,
      "--filial",
      filial,
      "--scp",
      scp,
      "--conta-debito",
      contaDebito,
      "--conta-credito",
      contaCredito,
    ]

    await execFileAsync("python3", args, {
      cwd: path.resolve(process.cwd(), ".."),
    })

    const content = await fs.readFile(outputPath)
    return new NextResponse(content, {
      status: 200,
      headers: {
        "Content-Type": "text/plain; charset=utf-8",
        "Content-Disposition": `attachment; filename="conciliacao_${tenant}_${accTail}.txt"`,
      },
    })
  } catch (error: any) {
    console.error("Erro ao gerar TXT", error)
    return NextResponse.json({ error: error?.message ?? "Falha ao gerar TXT" }, { status: 500 })
  } finally {
    // Best-effort cleanup
    try {
      await fs.unlink(outputPath)
      await fs.rmdir(tmpDir)
    } catch (_) {
      // ignore
    }
  }
}
