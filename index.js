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
import { randomUUID } from 'crypto'
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
  Browsers,
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
/** Worker uses app_runtime (DATABASE_URL) + SET LOCAL app.tenant_id per transaction. */
const poolConnectionString = databaseUrl

function dbHostHint () {
  try {
    const normalized = poolConnectionString.replace(/^postgres(ql)?:\/\//i, 'http://')
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

/** WhatsApp often rejects Ubuntu/Linux fingerprints from cloud servers; default macOS Chrome. */
const waWorkerBrowserEnv = (process.env.WA_WORKER_BROWSER || 'mac').trim().toLowerCase()

function resolveWaSocketBrowser () {
  if (waWorkerBrowserEnv === 'ubuntu' || waWorkerBrowserEnv === 'linux') {
    return Browsers.ubuntu('Chrome')
  }
  if (waWorkerBrowserEnv === 'win' || waWorkerBrowserEnv === 'windows') {
    return Browsers.windows('Chrome')
  }
  if (waWorkerBrowserEnv === 'appropriate') {
    return Browsers.appropriate('Chrome')
  }
  return Browsers.macOS('Chrome')
}

const WA_SOCKET_BROWSER = resolveWaSocketBrowser()

const _waConnectRaw = Number(process.env.WA_CONNECT_TIMEOUT_MS)
const waConnectTimeoutMs =
  Number.isFinite(_waConnectRaw) && _waConnectRaw >= 15_000
    ? Math.min(_waConnectRaw, 120_000)
    : 60_000

/** IQ replies (init queries / fetchProps) — Baileys default 60s often times out on cloud→WA paths */
const _waQtRaw = Number(process.env.WA_DEFAULT_QUERY_TIMEOUT_MS)
const waDefaultQueryTimeoutMs =
  Number.isFinite(_waQtRaw) && _waQtRaw >= 45_000
    ? Math.min(_waQtRaw, 300_000)
    : 120_000

const WORKER_INSTANCE_ID =
  (process.env.WA_WORKER_INSTANCE_ID || '').trim() || `wa:${randomUUID()}`

const _processingTimeoutRaw = Number(process.env.WA_QUEUE_PROCESSING_TIMEOUT_MS)
const QUEUE_PROCESSING_TIMEOUT_MS =
  Number.isFinite(_processingTimeoutRaw) && _processingTimeoutRaw >= 30_000
    ? Math.min(_processingTimeoutRaw, 3_600_000)
    : 120_000

const queueBatchSize = Math.min(
  75,
  Math.max(1, Number(process.env.WA_QUEUE_BATCH_SIZE) || 5)
)

const RETRY_BASE_MS = Math.max(5000, Number(process.env.WA_QUEUE_RETRY_BASE_MS) || 15_000)
const RETRY_MAX_MS = Math.max(RETRY_BASE_MS, Number(process.env.WA_QUEUE_RETRY_MAX_MS) || 1_800_000)
const RETRY_JITTER_MS = Math.max(0, Number(process.env.WA_QUEUE_RETRY_JITTER_MS) || 4000)

const STALE_RECOVERY_CAP = Math.max(
  1,
  Math.min(50, Number(process.env.WA_QUEUE_MAX_STALE_RECOVERIES_PER_ROW) || 8)
)

const CLEANUP_SENT_SKIPPED_DAYS = Math.max(7, Number(process.env.WA_QUEUE_CLEANUP_SENT_SKIPPED_DAYS) || 90)
const CLEANUP_FAILED_DAYS = Math.max(14, Number(process.env.WA_QUEUE_CLEANUP_FAILED_DAYS) || 180)
const CLEANUP_BATCH = Math.max(20, Math.min(2000, Number(process.env.WA_QUEUE_CLEANUP_BATCH) || 200))
const CLEANUP_EVERY_N_POLLS = Math.max(1, Number(process.env.WA_QUEUE_CLEANUP_EVERY_N_POLLS) || 25)

const DAY_MS = 86_400_000

if (!databaseUrl) {
  console.error('Missing DATABASE_URL')
  process.exit(1)
}

const pool = new pg.Pool({
  connectionString: poolConnectionString,
  max: 8,
  idleTimeoutMillis: 30_000,
  ssl: /localhost|127\.0\.0\.1/i.test(poolConnectionString)
    ? false
    : { rejectUnauthorized: false }
})

logger.info(
  {
    pollMs,
    sessionPollMs,
    logLevel,
    dbPool: 'app_runtime',
    dbHost: dbHostHint(),
    authRoot,
    authRootFromEnv: Boolean(authRootRaw),
    waBrowser: WA_SOCKET_BROWSER,
    waConnectTimeoutMs,
    waDefaultQueryTimeoutMs,
    workerInstanceId: WORKER_INSTANCE_ID,
    queueBatchSize,
    queueProcessingTimeoutMs: QUEUE_PROCESSING_TIMEOUT_MS
  },
  '[wa-worker] starting'
)

const SESSION_LABEL = 'default'

/** Run SQL with SET LOCAL app.tenant_id (RLS on whatsapp_sessions, etc.). */
async function withTenantTxn (tenantId, run) {
  const tid = String(tenantId ?? '').trim()
  if (!tid) throw new Error('withTenantTxn: empty tenantId')
  const client = await pool.connect()
  try {
    await client.query('begin')
    await client.query(`select set_config('app.tenant_id', $1, true)`, [tid])
    const out = await run(client)
    await client.query('commit')
    return out
  } catch (err) {
    await client.query('rollback').catch(() => {})
    if (err && typeof err === 'object' && err.code === '42501') {
      logger.error(
        {
          tenantId: tid,
          pgCode: err.code,
          message: err.message,
          sqlOrigin: 'withTenantTxn'
        },
        '[wa-worker] rls_query_denied'
      )
    }
    throw err
  } finally {
    client.release()
  }
}

async function listWhatsappWorkerTenantIds () {
  const ids = new Set()
  try {
    const listed = await pool.query(
      `select tenant_id from public.worker_list_whatsapp_tenant_ids($1)`,
      [SESSION_LABEL]
    )
    for (const r of listed.rows ?? []) {
      if (r.tenant_id) ids.add(String(r.tenant_id))
    }
  } catch (err) {
    logger.warn(
      { err: err instanceof Error ? err.message : String(err) },
      '[wa-worker] worker_list_whatsapp_tenant_ids unavailable — union queue tenants only'
    )
  }
  try {
    const q = await pool.query(
      `select distinct tenant_id from public.whatsapp_queue where tenant_id is not null`
    )
    for (const r of q.rows ?? []) {
      if (r.tenant_id) ids.add(String(r.tenant_id))
    }
  } catch (err) {
    logger.error(
      { err: err instanceof Error ? err.message : String(err) },
      '[wa-worker] distinct queue tenant_id failed'
    )
  }
  return [...ids]
}

/** @type {Map<string, number>} */
const notLinkedLastLogged = new Map()
const NOT_LINKED_LOG_INTERVAL_MS = 5 * 60 * 1000

let queuePollTicks = 0

/** Set on SIGTERM/SIGINT so the queue loop exits and the pool can close cleanly. */
let workerShuttingDown = false

/** @type {ReturnType<typeof setInterval> | null} */
let sessionPollTimer = null

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

function computeRetryDelayMs (retryCountAfterIncrement, idSeed) {
  const rc = Math.max(1, Math.floor(Number(retryCountAfterIncrement)))
  const exp = Math.min(RETRY_MAX_MS, RETRY_BASE_MS * (2 ** Math.min(rc - 1, 12)))
  let h = 0
  const seed = String(idSeed ?? '')
  for (let i = 0; i < seed.length; i++) {
    h = Math.imul(31, h) + seed.charCodeAt(i)
    h |= 0
  }
  const jitter = RETRY_JITTER_MS <= 0 ? 0 : Math.abs(h) % (RETRY_JITTER_MS + 1)
  return Math.min(RETRY_MAX_MS, exp + jitter)
}

async function finalizeQueueSkipped (queueRow, errCode, logFields = {}) {
  const tid = queueRow.tenant_id
  const qr = await withTenantTxn(tid, (client) =>
    client.query(
      `update public.whatsapp_queue
       set status = 'skipped',
           sent_at = coalesce(sent_at, now()),
           error = $1,
           processing_started_at = null,
           processing_owner = null
       where id = $2::uuid
         and processing_owner = $3
         and status = 'processing'
       returning id`,
      [errCode, queueRow.id, WORKER_INSTANCE_ID]
    )
  )
  if (!(qr.rowCount > 0)) {
    logger.warn(
      { queueId: queueRow.id, errCode },
      '[wa-worker] finalize skipped skipped — row not owned / already finalized'
    )
    return
  }
  await withTenantTxn(tid, (client) =>
    client.query(
      `update public.whatsapp_messages_log
       set status = 'skipped',
           error = $1
       where queue_id = $2::uuid`,
      [errCode, queueRow.id]
    )
  )
  logger.info({ queueId: queueRow.id, errCode, ...logFields }, '[wa-worker] queue row skipped')
}

async function releaseNotLinkedDefer (queueRow) {
  const deferMs =
    60_000 +
    ((String(queueRow.id).charCodeAt(0) || 0) % 45) * 1000
  const deferredUntil = new Date(Date.now() + deferMs).toISOString()
  const notLinkedErr = 'whatsapp_not_linked'
  const tid = queueRow.tenant_id
  const ur = await withTenantTxn(tid, (client) =>
    client.query(
      `update public.whatsapp_queue
       set scheduled_at = $1::timestamptz,
           status = 'pending',
           error = coalesce(error, $2),
           processing_started_at = null,
           processing_owner = null,
           last_attempt_at = now()
       where id = $3::uuid
         and processing_owner = $4
         and status = 'processing'`,
      [deferredUntil, notLinkedErr, queueRow.id, WORKER_INSTANCE_ID]
    )
  )
  if (!(ur.rowCount > 0)) return
  await withTenantTxn(tid, (client) =>
    client.query(
      `update public.whatsapp_messages_log
       set error = $1
       where queue_id = $2::uuid and error is null`,
      [notLinkedErr, queueRow.id]
    )
  )
  const now = Date.now()
  const last = notLinkedLastLogged.get(queueRow.tenant_id) ?? 0
  if (now - last > NOT_LINKED_LOG_INTERVAL_MS) {
    notLinkedLastLogged.set(queueRow.tenant_id, now)
    logger.warn(
      { tenantId: queueRow.tenant_id },
      '[wa-worker] deferring queued rows — tenant not linked'
    )
  }
}

async function recoverStaleProcessingRows () {
  const staleSec = Math.max(30, Math.floor(QUEUE_PROCESSING_TIMEOUT_MS / 1000))
  const client = await pool.connect()
  try {
    await client.query('begin')
    const stale = await client.query(
      `select id,
              tenant_id,
              processing_recovery_count,
              processing_owner
       from public.whatsapp_queue
       where status = 'processing'
         and processing_started_at < now () - $1::interval
       order by processing_started_at asc, id asc
       limit 100
       for update skip locked`,
      [`${staleSec} seconds`]
    )
    for (const row of stale.rows) {
      const recoveries =
        Math.min(999, (Number(row.processing_recovery_count) || 0) + 1)
      if (recoveries > STALE_RECOVERY_CAP) {
        await client.query(
          `update public.whatsapp_queue
           set status = 'failed',
               error = 'stale_processing_gave_up',
               processing_started_at = null,
               processing_owner = null,
               last_attempt_at = now (),
               processing_recovery_count = $2
           where id = $1::uuid`,
          [row.id, recoveries]
        )
        await client.query(
          `update public.whatsapp_messages_log
           set status = 'failed',
               error = 'stale_processing_gave_up'
           where queue_id = $1::uuid`,
          [row.id]
        )
        logger.error(
          {
            queueId: row.id,
            tenantId: row.tenant_id,
            staleRecoveries: recoveries,
            staleOwner: row.processing_owner
          },
          '[wa-worker] stale processing exceeded recovery cap — failed'
        )
      } else {
        const delay = computeRetryDelayMs(recoveries, String(row.id))
        const nextAt = new Date(Date.now() + delay).toISOString()
        const note =
          'stale_processing_recovered:' +
          `owner=${((row.processing_owner ?? '') + '').trim() || '?'}:${recoveries}`
        await client.query(
          `update public.whatsapp_queue
           set status = 'pending',
               scheduled_at = $2::timestamptz,
               processing_started_at = null,
               processing_owner = null,
               processing_recovery_count = $3,
               error = coalesce (error, $4),
               last_attempt_at = now ()
           where id = $1::uuid`,
          [row.id, nextAt, recoveries, note]
        )
        logger.warn(
          {
            queueId: row.id,
            tenantId: row.tenant_id,
            staleRecoveries: recoveries,
            staleOwner: row.processing_owner,
            nextScheduledAt: nextAt
          },
          '[wa-worker] stale processing recovered → pending'
        )
      }
    }
    await client.query('commit')
  } catch (err) {
    try {
      await client.query('rollback')
    } catch (_) {}
    logger.error(
      { err: err instanceof Error ? err.message : String(err) },
      '[wa-worker] stale recovery txn failed'
    )
  } finally {
    client.release()
  }
}

async function cleanupOldTerminalRowsBatch () {
  const sentCut =
    Date.now () - CLEANUP_SENT_SKIPPED_DAYS * DAY_MS
  const failCut =
    Date.now () - CLEANUP_FAILED_DAYS * DAY_MS
  try {
    const r1 = await pool.query(
      `with doomed as (
         select id
         from public.whatsapp_queue
         where status in ('sent', 'skipped')
           and coalesce(sent_at, created_at, now ()) < ($1::timestamptz)
         order by coalesce(sent_at, created_at), id
         limit $2
       ),
       dl as (
         delete from public.whatsapp_messages_log m
         using doomed d
         where m.queue_id = d.id
         returning m.queue_id
       )
       delete from public.whatsapp_queue q
       using doomed d
       where q.id = d.id`,
      [new Date(sentCut).toISOString(), CLEANUP_BATCH]
    )
    const n1 = r1.rowCount ?? 0
    if (n1 > 0) {
      logger.info(
        {
          deleted: n1,
          cutoff: new Date(sentCut).toISOString(),
          kind: 'sent_skipped_batch'
        },
        '[wa-worker] cleanup old terminal rows'
      )
    }
    const r2 = await pool.query(
      `with doomed as (
         select id
         from public.whatsapp_queue
         where status = 'failed'
           and created_at < ($1::timestamptz)
         order by created_at, id
         limit $2
       ),
       dl as (
         delete from public.whatsapp_messages_log m
         using doomed d
         where m.queue_id = d.id
       )
       delete from public.whatsapp_queue q
       using doomed d
       where q.id = d.id`,
      [new Date(failCut).toISOString(), CLEANUP_BATCH]
    )
    const n2 = r2.rowCount ?? 0
    if (n2 > 0) {
      logger.info(
        {
          deleted: n2,
          cutoff: new Date(failCut).toISOString(),
          kind: 'failed_batch'
        },
        '[wa-worker] cleanup old terminal rows'
      )
    }
  } catch (err) {
    logger.error(
      { err: err instanceof Error ? err.message : String(err) },
      '[wa-worker] cleanup batch failed'
    )
  }
}

async function claimNextQueueRows () {
  const res = await pool.query(
    `with cte as (
       select id
       from public.whatsapp_queue
       where status = 'pending'
         and scheduled_at <= now ()
       order by scheduled_at asc, id asc
       limit $1
       for update skip locked
     )
     update public.whatsapp_queue q
     set status = 'processing',
         processing_started_at = now (),
         processing_owner = $2,
         last_attempt_at = now ()
     from cte
     where q.id = cte.id
     returning q.id,
               q.tenant_id,
               q.recipient_phone,
               q.message_type,
               q.recipient_type,
               q.message_body,
               q.retry_count,
               q.session_id`,
    [queueBatchSize, WORKER_INSTANCE_ID]
  )
  return res.rows
}

/** Errors that will never resolve by retrying the same row — fail immediately. */
function isPermanentSendError (msg) {
  if (!msg) return false
  const m = String(msg).toLowerCase()
  if (m.startsWith('group_not_found')) return true
  if (m === 'empty_phone' || m === 'invalid_phone' || m === 'empty_group_name') return true
  if (m.includes('bad_session_repair_required')) return true
  if (m.includes('invalid_media_payload')) return true
  if (m.includes('media_path_tenant_mismatch')) return true
  if (m.includes('media_session_mismatch')) return true
  if (m.includes('media_tenant_mismatch')) return true
  if (m.startsWith('media_download_failed')) return true
  if (m.includes('stale_processing_gave_up')) return true
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
    const res = await withTenantTxn(tenantId, (client) =>
      client.query(
        `update public.whatsapp_sessions set ${fragments.join(', ')}
         where tenant_id = $1::uuid and label = $2
         returning id`,
        params
      )
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
        const res = await withTenantTxn(tenantId, (client) =>
          client.query(
            `select pairing_requested_at from public.whatsapp_sessions
             where tenant_id = $1::uuid and label = $2 limit 1`,
            [tenantId, SESSION_LABEL]
          )
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
    auth: state,
    browser: WA_SOCKET_BROWSER,
    connectTimeoutMs: waConnectTimeoutMs,
    defaultQueryTimeoutMs: waDefaultQueryTimeoutMs,
    markOnlineOnConnect: false,
    syncFullHistory: false
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
      const tenantIds = await listWhatsappWorkerTenantIds()
      for (const tid of tenantIds) {
        const res = await withTenantTxn(tid, (client) =>
          client.query(
            `select ${SESSION_POLL_COLUMNS} from public.whatsapp_sessions
             where label = $1 and tenant_id = $2::uuid
             limit 1`,
            [SESSION_LABEL, tid]
          )
        )
        if (res.rows[0]) rows.push(res.rows[0])
      }
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
          if (!ent?.sock?.user) {
            const shouldKill =
              !hasPersistedCreds(tid) || !row.linked_wa_jid
            if (shouldKill) {
              logger.info(
                { tid, hadCreds: hasPersistedCreds(tid), linked: Boolean(row.linked_wa_jid) },
                '[wa-worker] pairing stopped or abandoned — closing socket without WA user'
              )
              await destroyTenantSocket(tid)
              pairingHandled.delete(tid)
              await updateSession(tid, {
                pairing_qr: null,
                status: row.linked_wa_jid ? 'connected' : 'disconnected'
              })
              continue
            }
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
        for (const tid of tenantIds) {
          await withTenantTxn(tid, (client) =>
            client.query(
              `update public.whatsapp_sessions set worker_checked_at = $1::timestamptz
               where label = $2 and tenant_id = $3::uuid`,
              [tick, SESSION_LABEL, tid]
            )
          )
        }
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

const CLASS_REMINDER_MESSAGE_TYPES = new Set(['class_reminder', 'class_reminder_teacher'])
const ASSIGNMENT_MEDIA_MESSAGE_TYPE = 'assignment_created_parent_media'
const SESSION_HOMEWORK_MEDIA_MESSAGE_TYPE = 'session_attendance_homework_media'

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
  const tenantId = row.tenant_id

  let sess = null
  try {
    sess = await withTenantTxn(tenantId, async (client) => {
      const sRes = await client.query(
        `select id, session_report from public.sessions
         where id = $1::uuid and tenant_id = $2::uuid limit 1`,
        [sessionId, tenantId]
      )
      return sRes.rows[0] ?? null
    })
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
    await withTenantTxn(tenantId, (client) =>
      client.query(
        `update public.sessions set session_report = $1::jsonb
         where id = $2::uuid and tenant_id = $3::uuid`,
        [JSON.stringify(report), sessionId, tenantId]
      )
    )
  } catch (uErr) {
    if (uErr && typeof uErr === 'object' && uErr.code === '42501') {
      logger.error(
        { tenantId, sessionId, queueId: row.id, pgCode: uErr.code },
        '[wa-worker] rls_query_denied session_report update'
      )
    }
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
  /** R2/S3 often rejects unsigned HTTP DELETE on the PUT URL — fall back to S3 API when configured. */
  let storageRemoved = false

  if (delBase) {
    try {
      const res = await fetch(`${delBase}/${relPath}`, { method: 'DELETE' })
      if (res.ok || res.status === 404) {
        storageRemoved = true
        logger.info(
          {
            queueId: row.id,
            tenantId: row.tenant_id,
            path: media.path,
            bucket: media.bucket,
            status: res.status
          },
          '[wa-worker] storage delete OK (after WhatsApp send)'
        )
      } else {
        logger.warn(
          {
            queueId: row.id,
            tenantId: row.tenant_id,
            path: media.path,
            bucket: media.bucket,
            status: res.status
          },
          '[wa-worker] HTTP storage delete not OK — trying S3 DeleteObject if configured'
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
        '[wa-worker] HTTP storage delete failed — trying S3 DeleteObject if configured'
      )
    }
  }

  if (!storageRemoved) {
    try {
      const didS3 = await tryDeleteObjectViaS3(relPath)
      if (didS3) {
        storageRemoved = true
        logger.info(
          { queueId: row.id, path: media.path, bucket: media.bucket },
          '[wa-worker] S3 storage delete OK (after WhatsApp send)'
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

  if (!storageRemoved) {
    const hint =
      media.bucket === 'session_homework'
        ? '(set a working R2 DELETE URL or S3_* on this worker)'
        : '(set R2_ASSIGNMENTS_DELETE_BASE_URL or S3_* on this worker — unsigned DELETE on PUT URL often fails on R2)'
    logger.warn(
      { queueId: row.id, path: media.path, bucket: media.bucket },
      `[wa-worker] storage delete skipped ${hint}`
    )
  }

  if (media.bucket === 'assignments') {
    await cleanupAssignmentsAttachmentsDbAfterSend(row, media)
  } else if (media.bucket === 'session_homework') {
    await cleanupSessionHomeworkReportAfterSend(row, media)
  }
}

async function cleanupAssignmentsAttachmentsDbAfterSend (row, media) {
  if (!media.assignmentId) return

  const tenantId = row.tenant_id
  let assignment = null
  try {
    assignment = await withTenantTxn(tenantId, async (client) => {
      const aRes = await client.query(
        `select id, tenant_id, content, file_url from public.assignments
         where id = $1::uuid and tenant_id = $2::uuid limit 1`,
        [media.assignmentId, tenantId]
      )
      const row0 = aRes.rows[0] ?? null
      if (row0?.tenant_id && String(row0.tenant_id) !== String(tenantId)) {
        logger.error(
          {
            tenantId,
            actualTenantId: row0.tenant_id,
            assignmentId: media.assignmentId,
            queueId: row.id
          },
          '[wa-worker] unexpected_cross_tenant_row assignment cleanup'
        )
      }
      return row0
    })
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
    await withTenantTxn(tenantId, (client) =>
      client.query(
        `update public.assignments set content = $1::jsonb, file_url = $2
         where id = $3::uuid and tenant_id = $4::uuid`,
        [content, nextFileUrl, media.assignmentId, tenantId]
      )
    )
  } catch (uErr) {
    if (uErr && typeof uErr === 'object' && uErr.code === '42501') {
      logger.error(
        { tenantId, assignmentId: media.assignmentId, queueId: row.id, pgCode: uErr.code },
        '[wa-worker] rls_query_denied assignment cleanup update'
      )
    }
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
 * Session-scoped WhatsApp: do not deliver if student, teacher, or linked family is operationally inactive.
 * Terminates queue row without network send or retries.
 */
async function skipOperationalInactiveQueueRow (row) {
  const sessionId = row.session_id ? String(row.session_id).trim() : ''
  if (!sessionId) return false
  const tenantId = row.tenant_id
  try {
    const r = await withTenantTxn(tenantId, async (client) => {
      const q = await client.query(
        `select
           coalesce(sp.status, 'active') as student_status,
           coalesce(tp.status, 'active') as teacher_status,
           fp.status as family_status
         from public.sessions s
         inner join public.profiles sp on sp.id = s.student_id and sp.tenant_id = s.tenant_id
         inner join public.profiles tp on tp.id = s.teacher_id and tp.tenant_id = s.tenant_id
         left join public.student_details sd on sd.profile_id = s.student_id
         left join public.family_profiles fp
           on fp.tenant_id = s.tenant_id and fp.parent_profile_id = sd.parent_profile_id
         where s.tenant_id = $1::uuid and s.id = $2::uuid
         limit 1`,
        [tenantId, sessionId]
      )
      return q.rows[0]
    })
    if (!r) return false
    const inactiveStu = String(r.student_status ?? 'active').trim().toLowerCase() === 'inactive'
    const inactiveTea = String(r.teacher_status ?? 'active').trim().toLowerCase() === 'inactive'
    const famRaw = r.family_status
    const hasFam = famRaw != null && String(famRaw).trim() !== ''
    const inactiveFam = hasFam && String(famRaw).trim().toLowerCase() === 'inactive'
    if (!inactiveStu && !inactiveTea && !inactiveFam) return false
    await finalizeQueueSkipped(row, 'skipped_inactive_entity', {
      sessionId,
      inactiveStu,
      inactiveTea,
      inactiveFam
    })
    return true
  } catch (err) {
    logger.warn(
      { queueId: row.id, err: err instanceof Error ? err.message : String(err) },
      '[wa-worker] operational skip check failed — proceeding to send'
    )
    return false
  }
}

/**
 * Class reminders: do not send if the session row was deleted or is no longer active.
 * @returns {Promise<boolean>} true if row was skipped (caller must not send)
 */
async function skipClassReminderIfSessionNotSendable (row) {
  const messageType = row.message_type ?? ''
  if (!CLASS_REMINDER_MESSAGE_TYPES.has(messageType)) return false
  const sessionId = row.session_id ? String(row.session_id).trim() : ''
  if (!sessionId) return false
  const tenantId = row.tenant_id
  try {
    const r0 = await withTenantTxn(tenantId, async (client) => {
      const q = await client.query(
        `select status from public.sessions where tenant_id = $1::uuid and id = $2::uuid limit 1`,
        [tenantId, sessionId]
      )
      return q.rows[0]
    })
    if (!r0) {
      const errCode = 'skipped_missing_session'
      await finalizeQueueSkipped(row, errCode, {
        sessionId,
        messageType
      })
      return true
    }
    const low = String(r0.status ?? '').toLowerCase()
    if (
      low === 'completed' ||
      low === 'no_show' ||
      low === 'rescheduled' ||
      low.startsWith('cancel')
    ) {
      const errCode = 'skipped_session_not_active'
      await finalizeQueueSkipped(row, errCode, {
        sessionId,
        messageType,
        sessionStatus: r0.status
      })
      return true
    }
    return false
  } catch (err) {
    logger.warn(
      {
        queueId: row.id,
        err: err instanceof Error ? err.message : String(err)
      },
      '[wa-worker] class-reminder session guard failed — proceeding to send'
    )
    return false
  }
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

async function processClaimedQueueRow (row) {
  try {
    const skippedInactive = await skipOperationalInactiveQueueRow(row)
    if (skippedInactive) return
    const skippedClassReminder = await skipClassReminderIfSessionNotSendable(row)
    if (skippedClassReminder) return
    if (!hasPersistedCreds(row.tenant_id)) {
      await releaseNotLinkedDefer(row)
      return
    }
    await sendOnce(row)
    resetSessionFailures(row.tenant_id)
    const sentAt = new Date().toISOString()
    const fin = await pool.query(
      `update public.whatsapp_queue
       set status = 'sent',
           sent_at = $1::timestamptz,
           error = null,
           processing_started_at = null,
           processing_owner = null,
           processing_recovery_count = 0,
           last_attempt_at = now()
       where id = $2::uuid
         and processing_owner = $3
         and status = 'processing'`,
      [sentAt, row.id, WORKER_INSTANCE_ID]
    )
    if (!(fin.rowCount > 0)) {
      logger.warn(
        { queueId: row.id },
        '[wa-worker] sent OK but finalize lost race — another worker may have updated row'
      )
      return
    }
    await pool.query(
      `update public.whatsapp_messages_log
       set status = 'sent',
           error = null
       where queue_id = $1::uuid`,
      [row.id]
    )
    logger.info({ queueId: row.id }, '[wa-worker] DB updated: sent')
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e)
    if (msg.includes('whatsapp_not_linked')) {
      await updateSession(row.tenant_id, {
        status: 'disconnected',
        last_error: msg.slice(0, 500),
        worker_checked_at: new Date().toISOString()
      })
      await releaseNotLinkedDefer(row)
      return
    }
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
    if (msgLower.startsWith('group_not_found')) {
      invalidateGroupsCache(row.tenant_id)
    }
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
    const permanent = isPermanentSendError(msg) || wiped
    const failed = permanent || (connDead ? rc >= 12 : rc >= 5)
    const nextSched = failed
      ? null
      : new Date(Date.now() + computeRetryDelayMs(rc, String(row.id))).toISOString()
    logger.error(
      {
        queueId: row.id,
        messageType: row.message_type,
        recipientRaw: row.recipient_phone,
        err: msg,
        retryCount: rc,
        willFailPermanently: failed,
        permanent,
        wiped,
        nextScheduledAt: nextSched
      },
      '[wa-worker] send failed'
    )
    if (failed) {
      const uq = await pool.query(
        `update public.whatsapp_queue
         set status = 'failed',
             error = $1,
             retry_count = $2,
             processing_started_at = null,
             processing_owner = null,
             last_attempt_at = now()
         where id = $3::uuid
           and processing_owner = $4
           and status = 'processing'`,
        [msg, rc, row.id, WORKER_INSTANCE_ID]
      )
      if (uq.rowCount > 0) {
        await pool.query(
          `update public.whatsapp_messages_log
           set status = 'failed',
               error = $1
           where queue_id = $2::uuid`,
          [msg, row.id]
        )
      }
    } else {
      const uq = await pool.query(
        `update public.whatsapp_queue
         set status = 'pending',
             error = $1,
             retry_count = $2,
             scheduled_at = $3::timestamptz,
             processing_started_at = null,
             processing_owner = null,
             last_attempt_at = now()
         where id = $4::uuid
           and processing_owner = $5
           and status = 'processing'`,
        [msg, rc, nextSched, row.id, WORKER_INSTANCE_ID]
      )
      if (uq.rowCount > 0) {
        await pool.query(
          `update public.whatsapp_messages_log
           set status = 'pending',
               error = $1
           where queue_id = $2::uuid`,
          [msg, row.id]
        )
      }
    }
  }
}

async function pollLoop () {
  while (!workerShuttingDown) {
    try {
      queuePollTicks += 1
      await recoverStaleProcessingRows()
      if (queuePollTicks % CLEANUP_EVERY_N_POLLS === 0) {
        await cleanupOldTerminalRowsBatch()
      }
      let rows = []
      try {
        rows = await claimNextQueueRows()
      } catch (error) {
        logger.error(
          { err: error instanceof Error ? error.message : String(error) },
          '[wa-worker] claim batch failed'
        )
      }
      if (rows?.length) {
        if (logDebug) {
          logger.debug({ count: rows.length, workerId: WORKER_INSTANCE_ID }, '[wa-worker] claimed rows')
        }
        for (const row of rows) {
          if (workerShuttingDown) break
          await processClaimedQueueRow(row)
        }
      }
    } catch (e) {
      logger.error(
        { err: e instanceof Error ? e.message : String(e) },
        '[wa-worker] poll loop error'
      )
    }
    if (workerShuttingDown) break
    await new Promise((r) => setTimeout(r, pollMs))
  }
  logger.info('[wa-worker] queue poll loop stopped')
}

function onShutdownSignal (signal) {
  if (workerShuttingDown) return
  workerShuttingDown = true
  if (sessionPollTimer != null) {
    clearInterval(sessionPollTimer)
    sessionPollTimer = null
  }
  logger.warn(
    { signal },
    '[wa-worker] shutdown signal — platform stop, deploy, or manual kill (SIGTERM is normal); finishing current work then exiting'
  )
}

process.once('SIGTERM', () => onShutdownSignal('SIGTERM'))
process.once('SIGINT', () => onShutdownSignal('SIGINT'))

sessionPollTimer = setInterval(() => {
  void pollSessions()
}, sessionPollMs)
void pollSessions()

pollLoop()
  .then(async () => {
    logger.info('[wa-worker] closing pg pool')
    await pool.end().catch((err) => {
      logger.warn(
        { err: err instanceof Error ? err.message : String(err) },
        '[wa-worker] pool.end failed (ignored)'
      )
    })
    process.exit(0)
  })
  .catch((e) => {
    logger.fatal({ err: e instanceof Error ? e.message : String(e) }, '[wa-worker] fatal')
    process.exit(1)
  })
