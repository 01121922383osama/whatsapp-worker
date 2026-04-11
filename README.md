# whatsapp-worker

Run this as a **separate Node process** (Docker, VPS, Railway). It is not part of the Next.js server.

## Environment

From the **repo root** `.env.local` (loaded automatically on `npm start`):

- `SUPABASE_URL` — same as your Supabase project URL (or use `NEXT_PUBLIC_SUPABASE_URL` only)
- `SUPABASE_SERVICE_ROLE_KEY` — service role (server only; never expose to browsers)

Optional:

- `WORKER_POLL_MS` — poll interval ms, default `8000`
- `WA_WORKER_LOG_LEVEL` — `info` (default), `debug`, or `warn`

## Run

```bash
cd services/whatsapp-worker
npm install
npm start
```

## Behaviour

1. Polls `whatsapp_queue` for rows with `status = 'pending'` (or null).
2. Sends via WhatsApp Web (Baileys). On success, sets `status = 'sent'`, `sent_at = now()`.
3. On failure, increments `retry_count` and stores `error`.

Pairing: scan QR printed to the console on first run. Persist the `auth_info_baileys` folder between restarts.

If you see **`connection closed` with code `405`**: update dependencies (`npm install`), ensure this worker uses **`fetchLatestBaileysVersion`** (already in `index.js`), then delete **`auth_info_baileys`** and pair again.
