# whatsapp-worker

Run this as a **separate Node process** (Docker, VPS, Railway). It is not part of the Next.js server.

## Environment

From the **repo root** `.env.local` (loaded automatically on `npm start`):

- `DATABASE_URL` — Postgres connection string (same as the Next.js app; Neon or self-hosted)

**Assignment media (optional):** when sending assignment files/images, the worker downloads from public R2 URLs and deletes via HTTP when configured:

- `R2_ASSIGNMENTS_PUBLIC_BASE_URL` — public read base (same as the web app)
- `R2_ASSIGNMENTS_DELETE_BASE_URL` or `R2_ASSIGNMENTS_PUT_BASE_URL` — base used for `DELETE` after a successful send (R2/S3-compatible)

**Session homework media (optional):** after **completing a session**, homework attachments queued for WhatsApp use the same pattern. The worker deletes the object from storage **only after** the media send succeeds, then removes the attachment from `sessions.session_report.homework_attachments`.

- `R2_SESSION_HOMEWORK_PUBLIC_BASE_URL` — public read base (same as the web app)
- `R2_SESSION_HOMEWORK_DELETE_BASE_URL` or `R2_SESSION_HOMEWORK_PUT_BASE_URL` — base for `DELETE` after send (or `R2_ASSIGNMENT_FEEDBACK_DELETE_BASE_URL` as a fallback, matching worker code)

If neither HTTP delete base nor S3 env vars are set on the worker, logs show `storage delete skipped` and files remain in the bucket (WhatsApp delivery still proceeds).

Optional:

- `WORKER_POLL_MS` — poll interval ms, default `8000`
- `WORKER_SESSION_POLL_MS` — how often to read `whatsapp_sessions`, default `5000`
- `WA_WORKER_LOG_LEVEL` — `info` (default), `debug`, or `warn`
- `WA_AUTH_ROOT` — absolute/relative path for Baileys auth files (recommended for persistent volume), default `services/whatsapp-worker/auth_info_baileys`
- `WA_WORKER_BROWSER` — Baileys client fingerprint for QR pairing: `mac` (default, **recommended on Railway/Docker**), `windows`, `ubuntu`, `linux`, or `appropriate` (maps to host OS — usually still Linux in containers)
- `WA_CONNECT_TIMEOUT_MS` — WebSocket connect timeout, default `60000` (min `15000`, max `120000`)
- `WA_DEFAULT_QUERY_TIMEOUT_MS` — timeout for WA **IQ / init queries** (`fetchProps`, etc.), default `120000` (min `45000`, max `300000`). Raise if logs show `init queries` / `Timed Out` (408) right after connect.

If the phone shows **“Couldn’t link device”** while the worker logs look healthy, try: default `WA_WORKER_BROWSER=mac`, ensure **only one** worker replica, unlink other **Linked devices** on the same WhatsApp account if you hit the device limit, then use **Cancel linking** in admin and start again with **Remove the old device link first** checked once to clear `WA_AUTH_ROOT/<tenant_id>/`.

### Debugging logs

Set **`WA_WORKER_LOG_LEVEL=debug`** on Railway (or in `.env.local`) to get:

- Baileys internal logs at `debug` (websocket / stream detail)
- Every `connection.update` (connection state, QR length — not the raw QR string)
- Session poll row counts and “still waiting for socket” progress
- Full disconnect **stack traces** and extra Boom payload when a stream errors

At **`info`** (default) you still get: startup config (host + paths), disconnect **code + reason name** (e.g. `restartRequired`), reconnect scheduling, pairing vs send mode, queue send failures with `queueId`, and database driver errors when queries/updates fail.

## Run

```bash
cd services/whatsapp-worker
npm install
npm start
```

## Deploys and `SIGTERM`

Containers and PaaS send **`SIGTERM`** when they stop the process: new deploy, scaling, restarts, or host maintenance. **`npm error signal SIGTERM`** is npm reporting that the child exited after a normal platform stop — it is **not** an application bug. The worker now logs **`[wa-worker] shutdown signal`** and closes the Postgres pool before exit when possible.

## Behaviour

1. Claims `whatsapp_queue` rows with **`FOR UPDATE SKIP LOCKED`**: eligible rows must have **`status = 'pending'`** and `scheduled_at <= now()`.
2. After claim sets **`processing`**, **`processing_started_at`**, and **`processing_owner`** (defaults to a UUID based id; override with **`WA_WORKER_INSTANCE_ID`**).
3. Sends via WhatsApp Web (Baileys) using the **academy’s** session (`tenant_id` on each row). Auth files live under **`auth_info_baileys/<tenant_id>/`** (one WhatsApp account per tenant).
4. Polls `whatsapp_sessions` (label `default`) for pairing / logout requests from the admin UI.
5. On success sets **`sent`**, `sent_at`, clears processing fields. Operational / class-reminder skips set **`skipped`** (no WhatsApp delivery). Retryable failures bump **`retry_count`**, reschedule with backoff + jitter, and reset to **`pending`**. Permanent failures (invalid phone/group, wiped auth, capped retries, etc.) set **`failed`**.
6. **Stale recovery:** rows stuck in **`processing`** older than **`WA_QUEUE_PROCESSING_TIMEOUT_MS`** (default `120000` ms, min 30s) are moved back to **`pending`** with a delay, counting **`processing_recovery_count`**; exceeding **`WA_QUEUE_MAX_STALE_RECOVERIES_PER_ROW`** (default `8`) marks **`failed`** with `stale_processing_gave_up`.
7. **Cleanup (batched):** every **`WA_QUEUE_CLEANUP_EVERY_N_POLLS`** poll ticks removes up to **`WA_QUEUE_CLEANUP_BATCH`** **`sent`** / **`skipped`** rows older than **`WA_QUEUE_CLEANUP_SENT_SKIPPED_DAYS`** days, and **`failed`** rows older than **`WA_QUEUE_CLEANUP_FAILED_DAYS`** days — plus their `whatsapp_messages_log` rows linked by `queue_id`.

**Queue tuning (optional)**

- `WA_WORKER_INSTANCE_ID` — stable id logged in Postgres on claimed rows (default random `wa:<uuid>`).
- `WA_QUEUE_BATCH_SIZE` — claimed rows per tick (default `5`, max `75`).
- `WA_QUEUE_RETRY_BASE_MS`, `WA_QUEUE_RETRY_MAX_MS`, `WA_QUEUE_RETRY_JITTER_MS` — reschedule curve after failed sends.

**Pairing:** use **Admin → Settings → WhatsApp** for that academy: “Show QR code to link WhatsApp”. The worker writes the QR into the database; scan it with WhatsApp → Linked devices. Persist the **`auth_info_baileys`** directory (or a Railway volume mounted there) across deploys.

**Database-level queue safety:** two workers polling the same `DATABASE_URL` will not dequeue the same row thanks to transactional claiming — but Baileys is still happiest with **one send worker per academy** session (paired device / QR contention). Prefer a single replica or separate DBs when possible.

## Keep sessions across deploys (important)

If WhatsApp disconnects after every deploy, your auth files are likely on ephemeral disk.

Use a persistent volume and set:

```bash
WA_AUTH_ROOT=/data/wa-auth
```

Then mount your platform volume to `/data/wa-auth` in the worker service.

Railway example:

- Add a Volume to the worker service
- Mount path: `/data/wa-auth`
- Environment variable: `WA_AUTH_ROOT=/data/wa-auth`

After this, pair once, and future deploys should keep the same WhatsApp session.

**Legacy single-folder auth:** if you previously had `auth_info_baileys/creds.json` at the root, move the whole contents into **`auth_info_baileys/<your-tenant-uuid>/`** (get `tenant_id` from the `tenants` table).

If you see **`connection closed` with code `405`**: update dependencies (`npm install`), ensure this worker uses **`fetchLatestBaileysVersion`** (already in `index.js`), then delete that tenant’s folder under **`auth_info_baileys/<tenant_id>/`** and pair again.
