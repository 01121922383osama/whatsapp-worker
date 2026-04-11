/**
 * Baileys worker — reads `whatsapp_queue` from Supabase.
 *
 * Env: Railway/host vars, or dotenv from (in order, each overrides previous):
 *   ../../.env.local (monorepo root, local dev), ./.env.local, ./.env
 *
 * Logging: set WA_WORKER_LOG_LEVEL=info|debug|warn (default info).
 * Debug shows full message bodies; info truncates body preview.
 */
import { config as loadEnv } from 'dotenv'
import { existsSync } from 'fs'
import { dirname, resolve } from 'path'
import { fileURLToPath } from 'url'

const __dirname = dirname(fileURLToPath(import.meta.url))

const envPaths = [
  resolve(__dirname, '../../.env.local'),
  resolve(__dirname, '.env.local'),
  resolve(__dirname, '.env')
]
for (const p of envPaths) {
  if (existsSync(p)) loadEnv({ path: p, override: true })
}

import { createClient } from '@supabase/supabase-js'
import makeWASocket, {
  DisconnectReason,
  fetchLatestBaileysVersion,
  useMultiFileAuthState
} from '@whiskeysockets/baileys'
import pino from 'pino'
import qrcode from 'qrcode-terminal'

const logLevel = (process.env.WA_WORKER_LOG_LEVEL || 'info').trim().toLowerCase()
const logBodies = logLevel === 'debug'

const logger = pino({
  level: logLevel === 'debug' ? 'debug' : logLevel === 'warn' ? 'warn' : 'info'
})

const url = (process.env.SUPABASE_URL || process.env.NEXT_PUBLIC_SUPABASE_URL || '').trim()
const key = (process.env.SUPABASE_SERVICE_ROLE_KEY || '').trim()
const pollMs = Number(process.env.WORKER_POLL_MS) || 8000

if (!url || !key) {
  console.error('Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY')
  process.exit(1)
}

logger.info({ pollMs }, '[wa-worker] starting')

const supabase = createClient(url, key)

/** Resolves to the current socket; updated on every reconnect */
const socketRef = { promise: null }

function previewBody (text, max = 160) {
  const s = String(text ?? '')
  if (logBodies) return s
  if (s.length <= max) return s
  return `${s.slice(0, max)}…`
}

/**
 * Resolve queue recipient to a WhatsApp JID.
 * @returns {{ jid: string, resolution: string, meta?: Record<string, unknown> }}
 */
async function resolveRecipientJid (sock, recipientRaw) {
  const wa = String(recipientRaw ?? '').trim()
  if (!wa) {
    throw new Error('empty_phone')
  }

  if (wa.endsWith('@g.us') || wa.endsWith('@s.whatsapp.net')) {
    return { jid: wa, resolution: 'direct_jid' }
  }

  if (wa.startsWith('group:')) {
    const wanted = wa.slice('group:'.length).trim().toLowerCase()
    if (!wanted) {
      throw new Error('empty_group_name')
    }
    logger.info({ wanted }, '[wa-worker] resolving group by subject (exact match, case-insensitive)')
    const groups = await sock.groupFetchAllParticipating()
    const list = Object.values(groups)
    const hit = list.find((g) => {
      const subject = String(g?.subject ?? '').trim().toLowerCase()
      return subject === wanted
    })
    if (!hit?.id) {
      const sample = list
        .slice(0, 15)
        .map((g) => ({ id: g?.id, subject: g?.subject }))
      logger.warn(
        { wanted, participatingGroupCount: list.length, sampleSubjects: sample },
        '[wa-worker] group_not_found — check spelling vs WhatsApp group title'
      )
      throw new Error(`group_not_found:${wanted}`)
    }
    logger.info(
      { jid: hit.id, subject: hit.subject, wanted },
      '[wa-worker] group resolved'
    )
    return {
      jid: hit.id,
      resolution: 'group_by_name',
      meta: { subject: hit.subject }
    }
  }

  const digits = wa.replace(/\D/g, '')
  if (!digits) {
    throw new Error('invalid_phone')
  }
  const jid = `${digits}@s.whatsapp.net`
  return { jid, resolution: 'phone_digits', meta: { digits } }
}

async function buildSock () {
  const { version, isLatest } = await fetchLatestBaileysVersion()
  if (!isLatest) {
    logger.warn({ version }, '[wa-worker] Using fetched WA version (not marked latest)')
  }

  const authDir = resolve(__dirname, 'auth_info_baileys')
  const { state, saveCreds } = await useMultiFileAuthState(authDir)
  const sock = makeWASocket({
    logger: pino({ level: 'warn' }),
    version,
    auth: state,
    printQRInTerminal: false
  })

  sock.ev.on('creds.update', saveCreds)
  sock.ev.on('connection.update', (u) => {
    const { connection, lastDisconnect, qr } = u
    if (qr) qrcode.generate(qr, { small: true })
    if (connection === 'open') {
      const wid = sock.user?.id ?? null
      logger.info(
        { loggedInJid: wid },
        '[wa-worker] WhatsApp connected — this JID is the sending account (compare with your phone)'
      )
    }
    if (connection === 'close') {
      const err = lastDisconnect?.error
      const code = err?.output?.statusCode
      const msg = err?.message ?? String(err ?? '')
      logger.warn({ code, msg }, '[wa-worker] connection closed')

      const loggedOut = code === DisconnectReason.loggedOut
      if (loggedOut) {
        logger.error(
          '[wa-worker] Logged out. Delete folder auth_info_baileys and scan QR again.'
        )
        return
      }

      const delayMs =
        code === DisconnectReason.restartRequired ? 500 : 3000
      logger.warn({ delayMs }, '[wa-worker] reconnecting')
      setTimeout(() => {
        socketRef.promise = buildSock()
      }, delayMs)
    }
  })

  return sock
}

socketRef.promise = buildSock()

async function sendOnce (row) {
  const s = await socketRef.promise
  const queueId = row.id
  const messageType = row.message_type ?? '(unknown)'

  logger.info(
    {
      queueId,
      messageType,
      tenantId: row.tenant_id,
      recipientRaw: row.recipient_phone,
      recipientType: row.recipient_type,
      retryCount: row.retry_count ?? 0,
      bodyPreview: previewBody(row.message_body)
    },
    '[wa-worker] dequeue — about to send'
  )

  const { jid, resolution, meta } = await resolveRecipientJid(
    s,
    row.recipient_phone
  )

  logger.info(
    { queueId, jid, resolution, ...meta },
    '[wa-worker] resolved JID'
  )

  await s.sendMessage(jid, { text: String(row.message_body ?? '') })

  logger.info(
    { queueId, jid, messageType, resolution },
    '[wa-worker] sendMessage OK'
  )
}

async function pollLoop () {
  for (;;) {
    try {
      const { data: rows, error } = await supabase
        .from('whatsapp_queue')
        .select(
          'id, tenant_id, recipient_phone, message_type, recipient_type, message_body, status, retry_count'
        )
        .or('status.is.null,status.eq.pending')
        .lte('scheduled_at', new Date().toISOString())
        .limit(5)

      if (error) {
        logger.error({ err: error.message }, '[wa-worker] poll query failed')
      } else if (rows?.length) {
        logger.info({ count: rows.length }, '[wa-worker] fetched pending rows')
        for (const row of rows) {
          try {
            await sendOnce(row)
            await supabase
              .from('whatsapp_queue')
              .update({
                status: 'sent',
                sent_at: new Date().toISOString(),
                error: null
              })
              .eq('id', row.id)
            await supabase
              .from('whatsapp_messages_log')
              .update({ status: 'sent', error: null })
              .eq('queue_id', row.id)
            logger.info({ queueId: row.id }, '[wa-worker] DB updated: sent')
          } catch (e) {
            const msg = e instanceof Error ? e.message : String(e)
            const rc = (row.retry_count ?? 0) + 1
            const failed = rc >= 5
            logger.error(
              {
                queueId: row.id,
                messageType: row.message_type,
                recipientRaw: row.recipient_phone,
                err: msg,
                retryCount: rc,
                willFailPermanently: failed
              },
              '[wa-worker] send failed'
            )
            await supabase
              .from('whatsapp_queue')
              .update({
                status: failed ? 'failed' : 'pending',
                error: msg,
                retry_count: rc
              })
              .eq('id', row.id)
            await supabase
              .from('whatsapp_messages_log')
              .update({
                status: failed ? 'failed' : 'pending',
                error: msg
              })
              .eq('queue_id', row.id)
          }
        }
      }
    } catch (e) {
      logger.error({ err: e instanceof Error ? e.message : String(e) }, '[wa-worker] poll loop error')
    }
    await new Promise((r) => setTimeout(r, pollMs))
  }
}

pollLoop().catch((e) => {
  logger.fatal({ err: e instanceof Error ? e.message : String(e) }, '[wa-worker] fatal')
  process.exit(1)
})
