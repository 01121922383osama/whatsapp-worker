/**
 * Multi-tenant Baileys worker — reads `whatsapp_queue` and `whatsapp_sessions` from Postgres.
 *
 * Auth files live at `auth_info_baileys/<tenant_id>/` (one WhatsApp account per academy).
 * Legacy single-folder auth: move `auth_info_baileys/*` into `auth_info_baileys/<your-tenant-uuid>/`.
 *
 * Env: see README.md
 */
import { config as loadEnv } from 'dotenv'
import { existsSync } from 'fs'
import { rm } from 'fs/promises'
import { dirname, resolve } from 'path'
import { fileURLToPath } from 'url'
import pg from 'pg'

const __dirname = dirname(fileURLToPath(import.meta.url))

const envPaths = [
  resolve(__dirname, '../../.env.local'),
  resolve(__dirname, '.env.local'),
  resolve(__dirname, '.env')
]
for (const p of envPaths) {
  if (existsSync(p)) loadEnv({ path: p, override: true })
}

import makeWASocket, {
  DisconnectReason,
  fetchLatestBaileysVersion,
  useMultiFileAuthState
} from '@whiskeysockets/baileys'
import pino from 'pino'

const logLevel = (process.env.WA_WORKER_LOG_LEVEL || 'info').trim().toLowerCase()
const logBodies = logLevel === 'debug'
const logDebug = logLevel === 'debug'

const logger = pino({
  level: logLevel === 'debug' ? 'debug' : logLevel === 'warn' ? 'warn' : 'info'
})

/** Human-readable Baileys disconnect code (see DisconnectReason in @whiskeysockets/baileys) */
function disconnectReasonLabel (code) {
  if (code === undefined || code === null || Number.isNaN(code)) {
    return 'unknown'
  }
  for (const [name, value] of Object.entries(DisconnectReason)) {
    if (typeof value === 'number' && value === code) return name
  }
  return `code_${code}`
}

const databaseUrl = (process.env.DATABASE_URL || '').trim()

function dbHostHint () {
  try {
    const normalized = databaseUrl.replace(/^postgres(ql)?:\/\//i, 'http://')
    return new URL(normalized).host
  } catch {
    return '(invalid DATABASE_URL)'
  }
}

const pollMs = Number(process.env.WORKER_POLL_MS) || 8000
/** Default lowered DB pressure vs 2500ms; override with WORKER_SESSION_POLL_MS. */
const sessionPollMs = Number(process.env.WORKER_SESSION_POLL_MS) || 5000
const authRootRaw = (process.env.WA_AUTH_ROOT || '').trim()
const authRoot = authRootRaw
  ? resolve(authRootRaw)
  : resolve(__dirname, 'auth_info_baileys')

if (!databaseUrl) {
  console.error('Missing DATABASE_URL')
  process.exit(1)
}

const pool = new pg.Pool({
  connectionString: databaseUrl,
  max: 8,
  idleTimeoutMillis: 30_000,
  ssl: /localhost|127\.0\.0\.1/i.test(databaseUrl)
    ? false
    : { rejectUnauthorized: false }
})

logger.info(
  {
    pollMs,
    sessionPollMs,
    logLevel,
    dbHost: dbHostHint(),
    authRoot,
    authRootFromEnv: Boolean(authRootRaw)
  },
  '[wa-worker] starting'
)

const SESSION_LABEL = 'default'

/** @type {Map<string, string>} last pairing_requested_at (ISO string) we started handling */
const pairingHandled = new Map()

/** Skip redundant DB writes when Baileys repeats the same QR payload */
const lastPairingQrWritten = new Map()

/**
 * Stable key for pairing_requested_at. node-pg returns a fresh Date per query;
 * comparing with !== would always differ from a stored Date and restart pairing every poll.
 */
function normPairingRequestedAt (v) {
  if (v == null || v === '') return ''
  if (v instanceof Date) return v.toISOString()
  const s = String(v).trim()
  if (!s) return ''
  const d = new Date(s)
  if (!Number.isNaN(d.getTime())) return d.toISOString()
  return s
}

function tenantAuthDir (tenantId) {
  return resolve(authRoot, tenantId)
}

function hasPersistedCreds (tenantId) {
  return existsSync(resolve(tenantAuthDir(tenantId), 'creds.json'))
}

/** @type {Map<string, { sock: any }>} */
const tenantSockets = new Map()

/**
 * Rolling-window counter for transient session-level failures (Bad MAC, decrypt,
 * verifymac, init queries, code 500). Baileys usually self-heals these by
 * reopening the socket, so we no longer wipe auth_info_baileys/<tenant> on the
 * first error (which used to force a new QR). Auth is only wiped after too many
 * errors in a short window, which likely means the session is genuinely dead.
 * @type {Map<string, { count: number, firstAt: number, lastAt: number }>}
 */
const tenantSessionFailures = new Map()
const BAD_SESSION_WINDOW_MS = 10 * 60 * 1000
const BAD_SESSION_THRESHOLD = 7

function recordSessionFailure (tenantId) {
  const now = Date.now()
  const prev = tenantSessionFailures.get(tenantId)
  if (!prev || now - prev.firstAt > BAD_SESSION_WINDOW_MS) {
    const fresh = { count: 1, firstAt: now, lastAt: now }
    tenantSessionFailures.set(tenantId, fresh)
    return fresh
  }
  prev.count += 1
  prev.lastAt = now
  tenantSessionFailures.set(tenantId, prev)
  return prev
}

function resetSessionFailures (tenantId) {
  tenantSessionFailures.delete(tenantId)
}

function shouldWipeAuth (tenantId) {
  const entry = tenantSessionFailures.get(tenantId)
  if (!entry) return false
  if (Date.now() - entry.firstAt > BAD_SESSION_WINDOW_MS) {
    tenantSessionFailures.delete(tenantId)
    return false
  }
  return entry.count >= BAD_SESSION_THRESHOLD
}

/**
 * Cache participating-groups list per tenant for a short window.
 * Re-fetching it for every queued row triggered WhatsApp Web rate limits and
 * showed up as "stream errored out (ack)" / repeated reconnects in the logs.
 * @type {Map<string, { at: number, groups: { id: string, subject: string }[] }>}
 */
const tenantGroupsCache = new Map()
const GROUPS_CACHE_TTL_MS = 60_000

async function getCachedGroups (tenantId, sock) {
  const now = Date.now()
  const cached = tenantGroupsCache.get(tenantId)
  if (cached && now - cached.at < GROUPS_CACHE_TTL_MS) {
    return cached.groups
  }
  const map = await sock.groupFetchAllParticipating()
  const groups = Object.values(map).map((g) => ({
    id: String(g?.id ?? ''),
    subject: String(g?.subject ?? '').trim()
  }))
  tenantGroupsCache.set(tenantId, { at: now, groups })
  return groups
}

function invalidateGroupsCache (tenantId) {
  tenantGroupsCache.delete(tenantId)
}

/** Errors that will never resolve by retrying the same row — fail immediately. */
function isPermanentSendError (msg) {
  if (!msg) return false
  const m = String(msg).toLowerCase()
  if (m.startsWith('group_not_found')) return true
  if (m === 'empty_phone' || m === 'invalid_phone' || m === 'empty_group_name') return true
  if (m.includes('bad_session_repair_required')) return true
  return false
}

function previewBody (text, max = 160) {
  const s = String(text ?? '')
  if (logBodies) return s
  if (s.length <= max) return s
  return `${s.slice(0, max)}…`
}

async function updateSession (tenantId, patch) {
  const merged = { ...patch, updated_at: new Date().toISOString() }
  const fragments = []
  const params = [tenantId, SESSION_LABEL]
  let i = 3
  for (const [col, val] of Object.entries(merged)) {
    fragments.push(`${col} = $${i}`)
    params.push(val)
    i += 1
  }
  try {
    const res = await pool.query(
      `update public.whatsapp_sessions set ${fragments.join(', ')}
       where tenant_id = $1::uuid and label = $2
       returning id`,
      params
    )
    if (logDebug && res.rowCount) {
      logger.debug({ tenantId, keys: Object.keys(patch) }, '[wa-worker] session row updated')
    }
  } catch (err) {
    logger.error(
      {
        tenantId,
        err: err instanceof Error ? err.message : String(err)
      },
      '[wa-worker] session update failed'
    )
  }
}

async function destroyTenantSocket (tenantId) {
  const ent = tenantSockets.get(tenantId)
  if (!ent?.sock) {
    tenantSockets.delete(tenantId)
    return
  }
  try {
    ent.sock.end(undefined)
  } catch (e) {
    logger.warn(
      { tenantId, err: e instanceof Error ? e.message : String(e) },
      '[wa-worker] socket end'
    )
  }
  tenantSockets.delete(tenantId)
}

/** WA socket died mid-send — drop cached socket so next dequeue opens a fresh connection */
function shouldResetSocketAfterSendError (e) {
  if (!e) return false
  const msg = (e instanceof Error ? e.message : String(e)).toLowerCase()
  const outCode =
    typeof e?.output?.statusCode === 'number' ? e.output.statusCode : null
  if (outCode === 428) return true
  if (msg.includes('connection closed')) return true
  if (msg.includes('precondition required')) return true
  if (msg.includes('connection failure')) return true
  if (msg.includes('socket hang up')) return true
  if (msg.includes('econnreset')) return true
  if (msg.includes('etimedout') || msg.includes(' timeout')) return true
  if (msg.includes('stream errored')) return true
  if (msg.includes('restart required')) return true
  if (msg.includes('logged out')) return true
  if (msg.includes('bad mac')) return true
  if (msg.includes('failed to decrypt')) return true
  if (msg.includes('decrypt')) return true
  if (msg.includes('sessionerror')) return true
  if (msg.includes('no matching sessions')) return true
  if (msg.includes('verifymac')) return true
  return false
}

async function resetTenantSendSocket (tenantId, reason) {
  logger.warn({ tenantId, reason }, '[wa-worker] resetting send socket after error')
  await destroyTenantSocket(tenantId)
}

async function wipeTenantAuth (tenantId, reason) {
  logger.error(
    { tenantId, reason, failures: tenantSessionFailures.get(tenantId) },
    '[wa-worker] wiping tenant auth — repair required'
  )
  lastPairingQrWritten.delete(tenantId)
  pairingHandled.delete(tenantId)
  resetSessionFailures(tenantId)
  await destroyTenantSocket(tenantId)
  try {
    await rm(tenantAuthDir(tenantId), { recursive: true, force: true })
  } catch (e) {
    logger.warn(
      { tenantId, err: e instanceof Error ? e.message : String(e) },
      '[wa-worker] rm auth dir'
    )
  }
  await updateSession(tenantId, {
    status: 'error',
    linked_wa_jid: null,
    pairing_qr: null,
    pairing_requested_at: null,
    last_error: 'bad_session_repair_required',
    worker_checked_at: new Date().toISOString()
  })
}

async function processLogout (tenantId) {
  logger.info({ tenantId }, '[wa-worker] processing logout')
  const ent = tenantSockets.get(tenantId)
  if (ent?.sock) {
    try {
      await ent.sock.logout()
    } catch (e) {
      logger.warn(
        { tenantId, err: e instanceof Error ? e.message : String(e) },
        '[wa-worker] logout()'
      )
    }
  }
  tenantSockets.delete(tenantId)
  lastPairingQrWritten.delete(tenantId)
  pairingHandled.delete(tenantId)
  resetSessionFailures(tenantId)
  try {
    await rm(tenantAuthDir(tenantId), { recursive: true, force: true })
  } catch (e) {
    logger.warn(
      { tenantId, err: e instanceof Error ? e.message : String(e) },
      '[wa-worker] rm auth dir'
    )
  }
  await updateSession(tenantId, {
    status: 'disconnected',
    linked_wa_jid: null,
    pairing_qr: null,
    pairing_requested_at: null,
    logout_requested_at: null,
    last_error: null
  })
}

function scheduleReconnectSend (tenantId, delayMs, closeMeta) {
  logger.info(
    {
      tenantId,
      delayMs,
      lastCloseCode: closeMeta?.code,
      lastCloseReason: closeMeta?.reasonLabel
    },
    '[wa-worker] scheduling reconnect (send mode)'
  )
  setTimeout(() => {
    if (!hasPersistedCreds(tenantId)) {
      logger.warn({ tenantId }, '[wa-worker] reconnect skipped — no creds on disk')
      return
    }
    if (tenantSockets.has(tenantId)) {
      logger.debug({ tenantId }, '[wa-worker] reconnect skipped — socket already exists')
      return
    }
    startSocket(tenantId, { mode: 'send' }).catch((e) => {
      logger.error(
        { tenantId, err: e instanceof Error ? e.message : String(e) },
        '[wa-worker] reconnect send failed'
      )
    })
  }, delayMs)
}

/**
 * After QR / pair handshake, WA often closes with restartRequired (515). Pairing mode must
 * reopen the socket or pairing never completes (stream errored / restart required in logs).
 */
function scheduleReconnectPairing (tenantId, delayMs, closeMeta) {
  logger.info(
    {
      tenantId,
      delayMs,
      lastCloseCode: closeMeta?.code,
      lastCloseReason: closeMeta?.reasonLabel
    },
    '[wa-worker] scheduling reconnect (pairing mode, restartRequired)'
  )
  setTimeout(() => {
    void (async () => {
      if (tenantSockets.has(tenantId)) {
        logger.debug({ tenantId }, '[wa-worker] pairing reconnect skipped — socket already exists')
        return
      }
      let row = null
      try {
        const res = await pool.query(
          `select pairing_requested_at from public.whatsapp_sessions
           where tenant_id = $1::uuid and label = $2 limit 1`,
          [tenantId, SESSION_LABEL]
        )
        row = res.rows[0] ?? null
      } catch (err) {
        logger.warn(
          { tenantId, err: err instanceof Error ? err.message : String(err) },
          '[wa-worker] pairing reconnect skipped — session read failed'
        )
        return
      }
      if (!row?.pairing_requested_at) {
        logger.info(
          { tenantId },
          '[wa-worker] pairing reconnect skipped — pairing no longer requested'
        )
        return
      }
      try {
        await startSocket(tenantId, { mode: 'pairing' })
      } catch (e) {
        logger.error(
          { tenantId, err: e instanceof Error ? e.message : String(e) },
          '[wa-worker] reconnect pairing failed'
        )
      }
    })()
  }, delayMs)
}

/**
 * @param {string} tenantId
 * @param {{ mode: 'send' | 'pairing' }} ctx
 */
async function startSocket (tenantId, ctx) {
  await destroyTenantSocket(tenantId)
  lastPairingQrWritten.delete(tenantId)

  let version = null
  try {
    const latest = await fetchLatestBaileysVersion()
    version = latest.version
    if (!latest.isLatest) {
      logger.warn(
        { version, tenantId },
        '[wa-worker] Using fetched WA version (not marked latest)'
      )
    }
  } catch (e) {
    logger.warn(
      {
        tenantId,
        err: e instanceof Error ? e.message : String(e)
      },
      '[wa-worker] fetchLatestBaileysVersion failed; continuing with default bundled version'
    )
  }

  const authDir = tenantAuthDir(tenantId)
  if (logDebug) {
    logger.debug({ tenantId, authDir, mode: ctx.mode }, '[wa-worker] useMultiFileAuthState')
  }
  const { state, saveCreds } = await useMultiFileAuthState(authDir)
  const sockOptions = {
    logger: pino({
      level: logDebug ? 'debug' : 'error'
    }),
    auth: state
  }
  if (Array.isArray(version)) {
    sockOptions.version = version
  }
  const sock = makeWASocket(sockOptions)

  tenantSockets.set(tenantId, { sock })

  sock.ev.on('creds.update', saveCreds)
  sock.ev.on('connection.update', (u) => {
    const { connection, lastDisconnect, qr } = u

    if (logDebug) {
      logger.debug(
        {
          tenantId,
          mode: ctx.mode,
          connection: connection ?? null,
          hasQr: Boolean(qr),
          qrLen: qr ? String(qr).length : 0
        },
        '[wa-worker] connection.update'
      )
    }

    if (qr) {
      const prevQr = lastPairingQrWritten.get(tenantId)
      if (prevQr !== qr) {
        lastPairingQrWritten.set(tenantId, qr)
        void updateSession(tenantId, {
          pairing_qr: qr,
          status: 'pairing',
          last_error: null
        })
        logger.info(
          { tenantId, qrPayloadChars: String(qr).length },
          '[wa-worker] QR written to DB (scan in admin settings)'
        )
      }
    }

    if (connection === 'open') {
      const wid = sock.user?.id ?? null
      logger.info({ tenantId, loggedInJid: wid }, '[wa-worker] WhatsApp connected')
      lastPairingQrWritten.delete(tenantId)
      pairingHandled.delete(tenantId)
      resetSessionFailures(tenantId)
      void updateSession(tenantId, {
        status: 'connected',
        linked_wa_jid: wid,
        pairing_qr: null,
        pairing_requested_at: null,
        last_error: null,
        worker_checked_at: new Date().toISOString()
      })
    }

    if (connection === 'close') {
      const err = lastDisconnect?.error
      const code = err?.output?.statusCode
      const msg = err?.message ?? String(err ?? '')
      const msgLower = msg.toLowerCase()
      const reasonLabel = disconnectReasonLabel(code)
      const boomData = err?.data
      logger.warn(
        {
          tenantId,
          mode: ctx.mode,
          code,
          reasonLabel,
          msg,
          boomData: logDebug ? boomData : undefined,
          disconnectAt: lastDisconnect?.date
        },
        '[wa-worker] connection closed'
      )
      if (logDebug && err?.stack) {
        logger.debug({ tenantId, stack: err.stack }, '[wa-worker] close error stack')
      }

      const loggedOut = code === DisconnectReason.loggedOut
      tenantSockets.delete(tenantId)

      if (loggedOut) {
        lastPairingQrWritten.delete(tenantId)
        pairingHandled.delete(tenantId)
        resetSessionFailures(tenantId)
        void rm(tenantAuthDir(tenantId), { recursive: true, force: true }).catch(() => {})
        void updateSession(tenantId, {
          status: 'disconnected',
          linked_wa_jid: null,
          pairing_qr: null,
          pairing_requested_at: null,
          last_error: 'logged_out'
        })
        logger.warn({ tenantId }, '[wa-worker] logged out — creds cleared from disk')
        return
      }

      if (ctx.mode === 'pairing') {
        void updateSession(tenantId, {
          last_error: msg.slice(0, 500),
          worker_checked_at: new Date().toISOString()
        })
        if (code === DisconnectReason.restartRequired) {
          scheduleReconnectPairing(tenantId, 1600, { code, reasonLabel })
          return
        }
        logger.info(
          { tenantId, code, reasonLabel, msg: msg.slice(0, 200) },
          '[wa-worker] pairing socket closed (no auto-reconnect for this close code)'
        )
        return
      }

      /**
       * Transient signal/crypto errors look scary (code 500 / badSession / "init queries" /
       * Bad MAC / decrypt) but Baileys usually heals them by reopening the socket with the
       * same creds. Only wipe auth once we see many of these in a short window — otherwise
       * we force the admin to re-scan QR after every brief hiccup (common cause: the same
       * WhatsApp number signed into WhatsApp Web/Desktop elsewhere while the worker is running).
       */
      const sessionLevelError =
        code === 500 ||
        reasonLabel === 'badSession' ||
        msgLower.includes('badsession') ||
        msgLower.includes('init queries') ||
        msgLower.includes('bad mac') ||
        msgLower.includes('failed to decrypt') ||
        msgLower.includes('decrypt') ||
        msgLower.includes('no matching sessions') ||
        msgLower.includes('sessionerror') ||
        msgLower.includes('verifymac')

      if (sessionLevelError) {
        const tracker = recordSessionFailure(tenantId)
        if (shouldWipeAuth(tenantId)) {
          void wipeTenantAuth(tenantId, `close:${reasonLabel}:${msg.slice(0, 120)}`)
          return
        }
        logger.warn(
          {
            tenantId,
            code,
            reasonLabel,
            msg: msg.slice(0, 200),
            failureCount: tracker.count,
            threshold: BAD_SESSION_THRESHOLD,
            windowMs: BAD_SESSION_WINDOW_MS
          },
          '[wa-worker] transient session error — keeping auth, reconnecting'
        )
        void updateSession(tenantId, {
          status: 'disconnected',
          last_error: `transient:${msg.slice(0, 400)}`,
          worker_checked_at: new Date().toISOString()
        })
        scheduleReconnectSend(tenantId, 2000, { code, reasonLabel })
        return
      }

      void updateSession(tenantId, {
        status: 'disconnected',
        last_error: msg.slice(0, 500),
        worker_checked_at: new Date().toISOString()
      })

      const delayMs =
        code === DisconnectReason.restartRequired ? 500 : 3000
      scheduleReconnectSend(tenantId, delayMs, { code, reasonLabel })
    }
  })

  return sock
}

const SESSION_POLL_COLUMNS =
  'tenant_id,logout_requested_at,pairing_requested_at,metadata,linked_wa_jid,status'

async function pollSessions () {
  try {
    let rows = []
    try {
      const res = await pool.query(
        `select ${SESSION_POLL_COLUMNS} from public.whatsapp_sessions where label = $1`,
        [SESSION_LABEL]
      )
      rows = res.rows
    } catch (err) {
      logger.error(
        { err: err instanceof Error ? err.message : String(err) },
        '[wa-worker] session poll query failed'
      )
      return
    }

    if (logDebug) {
      logger.debug({ sessionRowCount: rows?.length ?? 0 }, '[wa-worker] session poll tick')
    }

    for (const row of rows ?? []) {
      const tid = row.tenant_id
      if (row.logout_requested_at) {
        await processLogout(tid)
        continue
      }

      if (row.pairing_requested_at) {
        const nextPr = normPairingRequestedAt(row.pairing_requested_at)
        if (!nextPr) {
          continue
        }
        const prev = pairingHandled.get(tid)
        if (prev !== nextPr) {
          pairingHandled.set(tid, nextPr)
          const meta =
            row.metadata && typeof row.metadata === 'object' && !Array.isArray(row.metadata)
              ? { ...row.metadata }
              : {}
          if (meta.wa_force_new_pair === true) {
            await destroyTenantSocket(tid)
            try {
              await rm(tenantAuthDir(tid), { recursive: true, force: true })
            } catch (e) {
              logger.warn(
                { tid, err: e instanceof Error ? e.message : String(e) },
                '[wa-worker] rm auth for force pair'
              )
            }
            delete meta.wa_force_new_pair
            await updateSession(tid, { metadata: meta })
          }
          logger.info({ tid }, '[wa-worker] starting pairing socket')
          try {
            await startSocket(tid, { mode: 'pairing' })
          } catch (e) {
            logger.error(
              { tid, err: e instanceof Error ? e.message : String(e) },
              '[wa-worker] pairing start failed'
            )
            await updateSession(tid, {
              last_error: e instanceof Error ? e.message.slice(0, 500) : String(e).slice(0, 500)
            })
          }
        }
      } else {
        if (!row.pairing_requested_at && tenantSockets.has(tid)) {
          const ent = tenantSockets.get(tid)
          if (!ent?.sock?.user && !hasPersistedCreds(tid)) {
            await destroyTenantSocket(tid)
            pairingHandled.delete(tid)
            await updateSession(tid, {
              pairing_qr: null,
              status: row.linked_wa_jid ? 'connected' : 'disconnected'
            })
            continue
          }
        }
        if (
          hasPersistedCreds(tid) &&
          !tenantSockets.has(tid) &&
          !row.pairing_requested_at
        ) {
          try {
            await startSocket(tid, { mode: 'send' })
          } catch (e) {
            logger.error(
              { tid, err: e instanceof Error ? e.message : String(e) },
              '[wa-worker] background reconnect failed'
            )
          }
        }
      }
    }

    const list = rows ?? []
    if (list.length > 0) {
      const tick = new Date().toISOString()
      const tenantIds = list.map((r) => r.tenant_id)
      try {
        await pool.query(
          `update public.whatsapp_sessions set worker_checked_at = $1::timestamptz
           where label = $2 and tenant_id = any($3::uuid[])`,
          [tick, SESSION_LABEL, tenantIds]
        )
      } catch (tickErr) {
        logger.error(
          {
            tenantCount: tenantIds.length,
            err: tickErr instanceof Error ? tickErr.message : String(tickErr)
          },
          '[wa-worker] worker_checked_at batch heartbeat failed'
        )
      }
    }
  } catch (e) {
    logger.error(
      { err: e instanceof Error ? e.message : String(e) },
      '[wa-worker] session poll error'
    )
  }
}

/** @type {Map<string, number>} */
const notLinkedLastLogged = new Map()
const NOT_LINKED_LOG_INTERVAL_MS = 5 * 60 * 1000

async function getSockForSend (tenantId) {
  if (!hasPersistedCreds(tenantId)) {
    const now = Date.now()
    const last = notLinkedLastLogged.get(tenantId) ?? 0
    if (now - last > NOT_LINKED_LOG_INTERVAL_MS) {
      notLinkedLastLogged.set(tenantId, now)
      logger.warn({ tenantId }, '[wa-worker] whatsapp_not_linked (no creds.json) — pair from admin settings')
    }
    throw new Error('whatsapp_not_linked')
  }
  notLinkedLastLogged.delete(tenantId)
  let ent = tenantSockets.get(tenantId)
  if (ent?.sock?.user) {
    return ent.sock
  }
  if (!ent?.sock) {
    logger.info({ tenantId }, '[wa-worker] getSockForSend — starting send-mode socket')
    await startSocket(tenantId, { mode: 'send' })
  }
  for (let i = 0; i < 150; i++) {
    await new Promise((r) => setTimeout(r, 100))
    const s = tenantSockets.get(tenantId)?.sock
    if (s?.user) {
      if (logDebug && i > 0) {
        logger.debug({ tenantId, waitIterations: i + 1 }, '[wa-worker] socket ready after wait')
      }
      return s
    }
    if (logDebug && i > 0 && i % 30 === 0) {
      logger.debug({ tenantId, waitIterations: i + 1 }, '[wa-worker] still waiting for socket user')
    }
  }
  logger.error(
    { tenantId, waitedMs: 150 * 100 },
    '[wa-worker] getSockForSend — whatsapp_not_ready (timeout)'
  )
  throw new Error('whatsapp_not_ready')
}

async function resolveRecipientJid (tenantId, sock, recipientRaw) {
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
    if (logDebug) {
      logger.debug({ tenantId, wanted }, '[wa-worker] resolving group by subject (cached)')
    }
    const groups = await getCachedGroups(tenantId, sock)
    const hit = groups.find((g) => g.subject.toLowerCase() === wanted)
    if (!hit?.id) {
      throw new Error(`group_not_found:${wanted}`)
    }
    if (logDebug) {
      logger.debug({ tenantId, jid: hit.id, subject: hit.subject }, '[wa-worker] group resolved')
    }
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

const LATE_START_MESSAGE_TYPES = new Set(['class_reminder_late', 'class_reminder_teacher_late'])
const ASSIGNMENT_MEDIA_MESSAGE_TYPE = 'assignment_created_parent_media'
const SESSION_HOMEWORK_MEDIA_MESSAGE_TYPE = 'session_attendance_homework_media'

function looksLikeLateStartReminder (row) {
  const messageType = row.message_type ?? ''
  if (LATE_START_MESSAGE_TYPES.has(messageType)) return true
  const body = String(row.message_body ?? '').toLowerCase()
  return body.includes('تنبيه تأخر') ||
    body.includes('تأخر بدء الحصة') ||
    body.includes('late class start') ||
    body.includes('late start')
}

function looksLikeQueuedMediaPayload (rawBody) {
  const s = String(rawBody ?? '').trim()
  if (!s.startsWith('{') || !s.endsWith('}')) return false
  try {
    const parsed = JSON.parse(s)
    const bucket = String(parsed?.bucket ?? '').trim()
    const path = String(parsed?.path ?? '').trim()
    const kind = parsed?.kind === 'image' ? 'image' : parsed?.kind === 'pdf' ? 'pdf' : ''
    const okBucket = bucket === 'assignments' || bucket === 'session_homework'
    return okBucket && Boolean(path) && Boolean(kind)
  } catch {
    return false
  }
}

function parseMediaPayloadFromRecipientType (recipientType) {
  const raw = String(recipientType ?? '').trim()
  if (!raw.startsWith('media:')) return null
  try {
    const json = Buffer.from(raw.slice('media:'.length), 'base64url').toString('utf8')
    return JSON.parse(json)
  } catch {
    throw new Error('invalid_media_payload')
  }
}

function parseQueuedMediaPayload (row) {
  const parsedFromRecipientType = parseMediaPayloadFromRecipientType(row.recipient_type)
  let parsed = parsedFromRecipientType
  if (!parsed) {
    try {
      parsed = JSON.parse(String(row.message_body ?? '{}'))
    } catch {
      throw new Error('invalid_media_payload')
    }
  }

  const bucket = String(parsed?.bucket ?? '').trim()
  const path = String(parsed?.path ?? '').trim()
  const fileName = String(parsed?.fileName ?? '').trim() || 'file'
  const kind = parsed?.kind === 'image' ? 'image' : parsed?.kind === 'pdf' ? 'pdf' : ''
  const mimeType = String(parsed?.mimeType ?? '').trim() ||
    (kind === 'pdf' ? 'application/pdf' : 'image/jpeg')
  const tenantIdJson = String(parsed?.tenantId ?? '').trim()

  if (bucket !== 'assignments' && bucket !== 'session_homework') {
    throw new Error('invalid_media_payload')
  }
  if (!path || !kind) {
    throw new Error('invalid_media_payload')
  }
  if (!path.startsWith(`${row.tenant_id}/`)) {
    throw new Error('media_path_tenant_mismatch')
  }
  if (tenantIdJson && tenantIdJson !== row.tenant_id) {
    throw new Error('media_tenant_mismatch')
  }

  if (bucket === 'assignments') {
    const assignmentId = String(parsed?.assignmentId ?? '').trim()
    return { bucket, path, fileName, kind, mimeType, assignmentId }
  }

  const sessionId = String(parsed?.sessionId ?? '').trim()
  if (!sessionId) {
    throw new Error('invalid_media_payload')
  }
  const sessionIdFromRow = row.session_id ? String(row.session_id).trim() : ''
  if (sessionIdFromRow && sessionIdFromRow !== sessionId) {
    throw new Error('media_session_mismatch')
  }
  return { bucket, path, fileName, kind, mimeType, sessionId }
}

function queuedMediaPublicBaseUrl (bucket) {
  if (bucket === 'assignments') {
    return (process.env.R2_ASSIGNMENTS_PUBLIC_BASE_URL || '').trim().replace(/\/$/, '')
  }
  if (bucket === 'session_homework') {
    return (process.env.R2_SESSION_HOMEWORK_PUBLIC_BASE_URL
      || process.env.R2_ASSIGNMENT_FEEDBACK_PUBLIC_BASE_URL || '').trim().replace(/\/$/, '')
  }
  return ''
}

function queuedMediaDeleteBaseUrl (bucket) {
  if (bucket === 'assignments') {
    return (process.env.R2_ASSIGNMENTS_DELETE_BASE_URL || process.env.R2_ASSIGNMENTS_PUT_BASE_URL || '')
      .trim()
      .replace(/\/$/, '')
  }
  if (bucket === 'session_homework') {
    return (process.env.R2_SESSION_HOMEWORK_DELETE_BASE_URL
      || process.env.R2_SESSION_HOMEWORK_PUT_BASE_URL
      || process.env.R2_ASSIGNMENT_FEEDBACK_DELETE_BASE_URL
      || '').trim().replace(/\/$/, '')
  }
  return ''
}

async function downloadQueuedStorageMedia (media) {
  const base = queuedMediaPublicBaseUrl(media.bucket)
  if (!base) {
    throw new Error(`media_download_failed:missing_public_base:${media.bucket}`)
  }
  const rel = String(media.path ?? '').replace(/^\//, '')
  const url = `${base}/${rel}`
  const res = await fetch(url)
  if (!res.ok) {
    throw new Error(`media_download_failed:${res.status}`)
  }
  return Buffer.from(await res.arrayBuffer())
}

function documentSendFileName (media) {
  const name = String(media.fileName ?? '').trim() || 'attachment'
  if (media.bucket === 'session_homework' || media.bucket === 'assignments') {
    if (/\.pdf$/i.test(name)) return name
    return `${name.replace(/\.[^/.]+$/, '')}.pdf`
  }
  return 'attachment.pdf'
}

async function sendQueuedMedia (sock, jid, row) {
  const media = parseQueuedMediaPayload(row)
  const buffer = await downloadQueuedStorageMedia(media)

  if (media.kind === 'image') {
    await sock.sendMessage(jid, {
      image: buffer,
      mimetype: media.mimeType
    })
    return
  }

  await sock.sendMessage(jid, {
    document: buffer,
    mimetype: media.mimeType || 'application/pdf',
    fileName: documentSendFileName(media)
  })
}

async function cleanupSessionHomeworkReportAfterSend (row, media) {
  const sessionId = media.sessionId
  if (!sessionId) return

  let sess = null
  try {
    const sRes = await pool.query(
      `select id, session_report from public.sessions
       where id = $1::uuid and tenant_id = $2::uuid limit 1`,
      [sessionId, row.tenant_id]
    )
    sess = sRes.rows[0] ?? null
  } catch (sErr) {
    logger.warn(
      {
        queueId: row.id,
        sessionId,
        err: sErr instanceof Error ? sErr.message : String(sErr)
      },
      '[wa-worker] session fetch failed for homework attachment cleanup'
    )
    return
  }

  if (!sess) {
    logger.warn(
      { queueId: row.id, sessionId },
      '[wa-worker] session not found for homework attachment cleanup'
    )
    return
  }

  const report =
    sess.session_report && typeof sess.session_report === 'object' && !Array.isArray(sess.session_report)
      ? { ...sess.session_report }
      : {}

  const rawAttachments = report.homework_attachments
  if (!Array.isArray(rawAttachments)) return

  const nextAttachments = rawAttachments.filter((item) => {
    if (!item || typeof item !== 'object') return true
    const p = String(item.path ?? '').trim()
    return p !== media.path
  })
  if (nextAttachments.length === rawAttachments.length) return

  if (nextAttachments.length > 0) {
    report.homework_attachments = nextAttachments
  } else {
    delete report.homework_attachments
  }

  try {
    await pool.query(
      `update public.sessions set session_report = $1::jsonb
       where id = $2::uuid and tenant_id = $3::uuid`,
      [JSON.stringify(report), sessionId, row.tenant_id]
    )
  } catch (uErr) {
    logger.warn(
      {
        queueId: row.id,
        sessionId,
        err: uErr instanceof Error ? uErr.message : String(uErr)
      },
      '[wa-worker] session_report update failed after homework media send'
    )
  }
}

async function tryDeleteObjectViaS3 (objectKey) {
  const endpoint = (process.env.S3_ENDPOINT || '').trim().replace(/\/$/, '')
  const bucket = (process.env.S3_BUCKET || '').trim()
  const accessKeyId = (process.env.S3_ACCESS_KEY_ID || '').trim()
  const secretAccessKey = (process.env.S3_SECRET_ACCESS_KEY || '').trim()
  if (!endpoint || !bucket || !accessKeyId || !secretAccessKey) return false
  const { S3Client, DeleteObjectCommand } = await import('@aws-sdk/client-s3')
  const region = (process.env.S3_REGION || 'auto').trim()
  const forcePathStyle = String(process.env.S3_FORCE_PATH_STYLE || '').toLowerCase() !== 'false'
  const client = new S3Client({
    region,
    endpoint,
    credentials: { accessKeyId, secretAccessKey },
    forcePathStyle,
    requestChecksumCalculation: 'WHEN_REQUIRED',
    responseChecksumValidation: 'WHEN_REQUIRED'
  })
  await client.send(new DeleteObjectCommand({
    Bucket: bucket,
    Key: String(objectKey ?? '').replace(/^\//, '')
  }))
  return true
}

async function cleanupAfterQueuedMediaSend (row, media) {
  const delBase = queuedMediaDeleteBaseUrl(media.bucket)
  const relPath = String(media.path ?? '').replace(/^\//, '')
  if (delBase) {
    try {
      const res = await fetch(`${delBase}/${relPath}`, { method: 'DELETE' })
      if (!res.ok && res.status !== 404) {
        logger.warn(
          { queueId: row.id, tenantId: row.tenant_id, path: media.path, bucket: media.bucket, status: res.status },
          '[wa-worker] storage delete failed (continuing)'
        )
      }
    } catch (err) {
      logger.warn(
        {
          queueId: row.id,
          tenantId: row.tenant_id,
          path: media.path,
          err: err instanceof Error ? err.message : String(err)
        },
        '[wa-worker] storage delete failed (continuing)'
      )
    }
  } else {
    try {
      const didS3 = await tryDeleteObjectViaS3(relPath)
      if (!didS3) {
        const hint =
          media.bucket === 'session_homework'
            ? '(set R2 delete/put base or S3_* on this worker)'
            : '(set R2_ASSIGNMENTS_DELETE_BASE_URL or R2_ASSIGNMENTS_PUT_BASE_URL, or S3_* on this worker)'
        logger.warn(
          { queueId: row.id, path: media.path, bucket: media.bucket },
          `[wa-worker] storage delete skipped ${hint}`
        )
      }
    } catch (s3Err) {
      logger.warn(
        {
          queueId: row.id,
          path: media.path,
          err: s3Err instanceof Error ? s3Err.message : String(s3Err)
        },
        '[wa-worker] S3 storage delete failed (continuing)'
      )
    }
  }

  if (media.bucket === 'assignments') {
    await cleanupAssignmentsAttachmentsDbAfterSend(row, media)
  } else if (media.bucket === 'session_homework') {
    await cleanupSessionHomeworkReportAfterSend(row, media)
  }
}

async function cleanupAssignmentsAttachmentsDbAfterSend (row, media) {
  if (!media.assignmentId) return

  let assignment = null
  try {
    const aRes = await pool.query(
      `select id, tenant_id, content, file_url from public.assignments
       where id = $1::uuid and tenant_id = $2::uuid limit 1`,
      [media.assignmentId, row.tenant_id]
    )
    assignment = aRes.rows[0] ?? null
  } catch (aErr) {
    logger.warn(
      {
        queueId: row.id,
        assignmentId: media.assignmentId,
        err: aErr instanceof Error ? aErr.message : String(aErr)
      },
      '[wa-worker] assignment fetch failed for attachment cleanup'
    )
    return
  }

  if (!assignment) {
    logger.warn(
      { queueId: row.id, assignmentId: media.assignmentId },
      '[wa-worker] assignment fetch failed for attachment cleanup'
    )
    return
  }

  const content = assignment.content && typeof assignment.content === 'object' && !Array.isArray(assignment.content)
    ? { ...assignment.content }
    : {}

  const rawAttachments = content.attachments
  if (Array.isArray(rawAttachments)) {
    const nextAttachments = rawAttachments.filter((item) => {
      if (!item || typeof item !== 'object') return true
      const p = String((item).path ?? '').trim()
      return p !== media.path
    })
    if (nextAttachments.length > 0) {
      content.attachments = nextAttachments
    } else {
      delete content.attachments
    }
  }

  const nextFileUrl = assignment.file_url === media.path ? null : assignment.file_url

  try {
    await pool.query(
      `update public.assignments set content = $1::jsonb, file_url = $2
       where id = $3::uuid and tenant_id = $4::uuid`,
      [content, nextFileUrl, media.assignmentId, row.tenant_id]
    )
  } catch (uErr) {
    logger.warn(
      {
        queueId: row.id,
        assignmentId: media.assignmentId,
        err: uErr instanceof Error ? uErr.message : String(uErr)
      },
      '[wa-worker] assignment content update failed after send'
    )
  }
}

/**
 * Late-start messages are not sent; drop queue + log rows so backlog does not
 * accumulate bodies or failed rows in the DB / admin UI.
 * @returns {Promise<boolean>} true if row was removed (caller must not send)
 */
async function abandonStaleLateReminderIfNeeded (row) {
  if (!looksLikeLateStartReminder(row)) return false
  logger.info(
    { queueId: row.id, messageType: row.message_type ?? '', sessionId: row.session_id },
    '[wa-worker] deleting disabled late-start reminder row'
  )
  await pool.query(
    'delete from public.whatsapp_messages_log where queue_id = $1::uuid',
    [row.id]
  )
  await pool.query('delete from public.whatsapp_queue where id = $1::uuid', [row.id])
  return true
}

async function sendOnce (row) {
  const tenantId = row.tenant_id
  const s = await getSockForSend(tenantId)
  const queueId = row.id
  const messageType = row.message_type ?? '(unknown)'

  logger.info(
    {
      queueId,
      messageType,
      tenantId,
      recipientRaw: row.recipient_phone,
      recipientType: row.recipient_type,
      retryCount: row.retry_count ?? 0,
      bodyPreview: previewBody(row.message_body)
    },
    '[wa-worker] dequeue — about to send'
  )

  const { jid, resolution, meta } = await resolveRecipientJid(tenantId, s, row.recipient_phone)

  if (logDebug) {
    logger.debug({ queueId, jid, resolution, ...meta }, '[wa-worker] resolved JID')
  }

  const bodyText = String(row.message_body ?? '')
  const hasMediaRecipientType = String(row.recipient_type ?? '').trim().startsWith('media:')
  const hasJsonMediaBody = looksLikeQueuedMediaPayload(bodyText)
  const shouldSendMedia =
    messageType === ASSIGNMENT_MEDIA_MESSAGE_TYPE ||
    messageType === SESSION_HOMEWORK_MEDIA_MESSAGE_TYPE ||
    hasMediaRecipientType ||
    hasJsonMediaBody

  if (shouldSendMedia) {
    if (
      messageType !== ASSIGNMENT_MEDIA_MESSAGE_TYPE &&
      messageType !== SESSION_HOMEWORK_MEDIA_MESSAGE_TYPE &&
      (hasMediaRecipientType || hasJsonMediaBody)
    ) {
      logger.warn(
        { queueId, messageType },
        '[wa-worker] media payload detected but message_type is unexpected — sending as media anyway'
      )
    }
    await sendQueuedMedia(s, jid, row)
    try {
      const media = parseQueuedMediaPayload(row)
      await cleanupAfterQueuedMediaSend(row, media)
    } catch (e) {
      logger.warn(
        { queueId, err: e instanceof Error ? e.message : String(e) },
        '[wa-worker] post-send cleanup skipped'
      )
    }
  } else {
    await s.sendMessage(jid, { text: bodyText })
  }

  logger.info({ queueId, jid, messageType, resolution }, '[wa-worker] sendMessage OK')
}

async function pollLoop () {
  for (;;) {
    try {
      let rows = []
      try {
        const res = await pool.query(
          `select id, tenant_id, recipient_phone, message_type, recipient_type, message_body, status, retry_count, session_id
           from public.whatsapp_queue
           where (status is null or status = 'pending')
             and scheduled_at <= $1::timestamptz
           order by scheduled_at asc, id asc
           limit 5`,
          [new Date().toISOString()]
        )
        rows = res.rows
      } catch (error) {
        logger.error(
          { err: error instanceof Error ? error.message : String(error) },
          '[wa-worker] poll query failed'
        )
      }

      if (rows?.length) {
        if (logDebug) {
          logger.debug({ count: rows.length }, '[wa-worker] fetched pending rows')
        }
        for (const row of rows) {
          try {
            const abandoned = await abandonStaleLateReminderIfNeeded(row)
            if (abandoned) continue
            /** Skip rows whose tenant is not linked (without burning a retry slot). */
            if (!hasPersistedCreds(row.tenant_id)) {
              /** Explain in DB/UI why pending rows never leave — without failing the row (sends after pairing). */
              const notLinkedErr = 'whatsapp_not_linked'
              const deferMs = 60_000 + (String(row.id).charCodeAt(0) % 45) * 1000
              const deferredUntil = new Date(Date.now() + deferMs).toISOString()
              /** Always bump schedule so one dead tenant cannot starve others in the same DB (FIFO poll). */
              await pool.query(
                `update public.whatsapp_queue set scheduled_at = $1::timestamptz where id = $2::uuid`,
                [deferredUntil, row.id]
              )
              await pool.query(
                `update public.whatsapp_queue set error = $1 where id = $2::uuid and error is null`,
                [notLinkedErr, row.id]
              )
              await pool.query(
                `update public.whatsapp_messages_log set error = $1 where queue_id = $2::uuid and error is null`,
                [notLinkedErr, row.id]
              )
              const now = Date.now()
              const last = notLinkedLastLogged.get(row.tenant_id) ?? 0
              if (now - last > NOT_LINKED_LOG_INTERVAL_MS) {
                notLinkedLastLogged.set(row.tenant_id, now)
                logger.warn(
                  { tenantId: row.tenant_id },
                  '[wa-worker] skipping queued rows — tenant not linked (pair from admin settings)'
                )
              }
              continue
            }
            await sendOnce(row)
            resetSessionFailures(row.tenant_id)
            const sentAt = new Date().toISOString()
            await pool.query(
              `update public.whatsapp_queue
               set status = 'sent', sent_at = $1::timestamptz, error = null
               where id = $2::uuid`,
              [sentAt, row.id]
            )
            await pool.query(
              `update public.whatsapp_messages_log set status = 'sent', error = null where queue_id = $1::uuid`,
              [row.id]
            )
            logger.info({ queueId: row.id }, '[wa-worker] DB updated: sent')
          } catch (e) {
            const msg = e instanceof Error ? e.message : String(e)
            const connDead = shouldResetSocketAfterSendError(e)
            const msgLower = msg.toLowerCase()
            const badSessionSendError =
              msgLower.includes('badsession') ||
              msgLower.includes('bad session') ||
              msgLower.includes('bad mac') ||
              msgLower.includes('failed to decrypt') ||
              msgLower.includes('no matching sessions') ||
              msgLower.includes('sessionerror') ||
              msgLower.includes('verifymac')
            if (msg.includes('whatsapp_not_linked')) {
              await updateSession(row.tenant_id, {
                status: 'disconnected',
                last_error: msg.slice(0, 500),
                worker_checked_at: new Date().toISOString()
              })
            }
            if (msgLower.startsWith('group_not_found')) {
              invalidateGroupsCache(row.tenant_id)
            }
            /**
             * Used to wipe auth immediately on the first decrypt/Bad MAC/verifymac — but
             * these are usually transient (often caused by the same WA number being used
             * on WhatsApp Web/Desktop elsewhere while the worker is running). Count them
             * in the rolling window; only wipe after the threshold so the admin doesn't
             * lose pairing on every hiccup.
             */
            let wiped = false
            if (badSessionSendError) {
              const tracker = recordSessionFailure(row.tenant_id)
              if (shouldWipeAuth(row.tenant_id)) {
                await wipeTenantAuth(row.tenant_id, `send:${msg.slice(0, 120)}`)
                wiped = true
              } else {
                logger.warn(
                  {
                    tenantId: row.tenant_id,
                    queueId: row.id,
                    err: msg.slice(0, 200),
                    failureCount: tracker.count,
                    threshold: BAD_SESSION_THRESHOLD
                  },
                  '[wa-worker] transient send-side session error — keeping auth, will retry'
                )
              }
            }
            if (connDead) {
              await resetTenantSendSocket(row.tenant_id, msg)
            }
            const rc = (row.retry_count ?? 0) + 1
            /**
             * `badSessionSendError` used to force a permanent failure on the queue row.
             * Now those are treated as transient (the message will go out once the socket
             * reopens), so we only permanently fail if isPermanentSendError says so OR we
             * actually wiped auth, OR retries are exhausted.
             */
            const permanent = isPermanentSendError(msg) || wiped
            /** Transient WA disconnects — allow more retries than hard failures */
            const failed = permanent || (connDead ? rc >= 12 : rc >= 5)
            logger.error(
              {
                queueId: row.id,
                messageType: row.message_type,
                recipientRaw: row.recipient_phone,
                err: msg,
                retryCount: rc,
                willFailPermanently: failed,
                permanent,
                wiped
              },
              '[wa-worker] send failed'
            )
            await pool.query(
              `update public.whatsapp_queue
               set status = $1, error = $2, retry_count = $3
               where id = $4::uuid`,
              [failed ? 'failed' : 'pending', msg, rc, row.id]
            )
            await pool.query(
              `update public.whatsapp_messages_log
               set status = $1, error = $2
               where queue_id = $3::uuid`,
              [failed ? 'failed' : 'pending', msg, row.id]
            )
          }
        }
      }
    } catch (e) {
      logger.error(
        { err: e instanceof Error ? e.message : String(e) },
        '[wa-worker] poll loop error'
      )
    }
    await new Promise((r) => setTimeout(r, pollMs))
  }
}

setInterval(() => {
  void pollSessions()
}, sessionPollMs)
void pollSessions()

pollLoop().catch((e) => {
  logger.fatal({ err: e instanceof Error ? e.message : String(e) }, '[wa-worker] fatal')
  process.exit(1)
})
