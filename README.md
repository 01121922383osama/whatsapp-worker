# whatsapp-worker

Run this as a **separate Node process** (Docker, VPS, Railway). It is not part of the Next.js server.

## Environment

From the **repo root** `.env.local` (loaded automatically on `npm start`):

- `SUPABASE_URL` — same as your Supabase project URL (or use `NEXT_PUBLIC_SUPABASE_URL` only)
- `SUPABASE_SERVICE_ROLE_KEY` — service role (server only; never expose to browsers)

Optional:

- `WORKER_POLL_MS` — poll interval ms, default `8000`
- `WORKER_SESSION_POLL_MS` — how often to read `whatsapp_sessions`, default `2500`
- `WA_WORKER_LOG_LEVEL` — `info` (default), `debug`, or `warn`

### Debugging logs

Set **`WA_WORKER_LOG_LEVEL=debug`** on Railway (or in `.env.local`) to get:

- Baileys internal logs at `debug` (websocket / stream detail)
- Every `connection.update` (connection state, QR length — not the raw QR string)
- Session poll row counts and “still waiting for socket” progress
- Full disconnect **stack traces** and extra Boom payload when a stream errors

At **`info`** (default) you still get: startup config (host + paths), disconnect **code + reason name** (e.g. `restartRequired`), reconnect scheduling, pairing vs send mode, queue send failures with `queueId`, and Supabase errors (`message`, `code`, `details`) when queries/updates fail.

## Run

```bash
cd services/whatsapp-worker
npm install
npm start
```

## Behaviour

1. Polls `whatsapp_queue` for rows with `status = 'pending'` (or null).
2. Sends via WhatsApp Web (Baileys) using the **academy’s** session (`tenant_id` on each row). Auth files live under **`auth_info_baileys/<tenant_id>/`** (one WhatsApp account per tenant).
3. Polls `whatsapp_sessions` (label `default`) for pairing / logout requests from the admin UI.
4. On success, sets `status = 'sent'`, `sent_at = now()`. On failure, increments `retry_count` and stores `error`.

**Pairing:** use **Admin → Settings → WhatsApp** for that academy: “Show QR code to link WhatsApp”. The worker writes the QR into Supabase; scan it with WhatsApp → Linked devices. Persist the **`auth_info_baileys`** directory (or a Railway volume mounted there) across deploys.

**Legacy single-folder auth:** if you previously had `auth_info_baileys/creds.json` at the root, move the whole contents into **`auth_info_baileys/<your-tenant-uuid>/`** (get `tenant_id` from Supabase `tenants`).

If you see **`connection closed` with code `405`**: update dependencies (`npm install`), ensure this worker uses **`fetchLatestBaileysVersion`** (already in `index.js`), then delete that tenant’s folder under **`auth_info_baileys/<tenant_id>/`** and pair again.
