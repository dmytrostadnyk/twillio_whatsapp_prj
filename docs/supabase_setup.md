# Supabase Setup — Step-by-Step

This guide walks you through creating a Supabase project from scratch and wiring it into this application. Estimated time: ~10 minutes.

---

## Step 1 — Create a Supabase account

1. Go to [supabase.com](https://supabase.com) and click **Start your project**.
2. Sign up with GitHub (easiest) or an email address.
3. Verify your email if prompted.

---

## Step 2 — Create a new project

1. Once logged in, click **New project**.
2. Choose your **Organisation** (your personal account is fine).
3. Fill in:
   - **Name**: `twilio-comm-intelligence` (or anything you like)
   - **Database Password**: choose a strong password and **save it somewhere safe** — you'll need it for the `DATABASE_URL`.
   - **Region**: pick the region closest to you for best latency.
4. Click **Create new project**.
5. Wait ~2 minutes for the project to initialise (you'll see a progress bar).

---

## Step 3 — Get your connection credentials

Once the project is ready, navigate to:

**Settings → API** (left sidebar)

Copy these values into your `.env` file:

| What | Where in the dashboard | `.env` variable |
|---|---|---|
| Project URL | "Project URL" field | `SUPABASE_URL` |
| Service role key | "Project API keys" → `service_role` | `SUPABASE_SERVICE_ROLE_KEY` |

> ⚠️ The `service_role` key **bypasses Row Level Security**. Keep it secret — it should only ever be in your `.env` file and never in frontend code.

Then navigate to **Settings → Database**:

Copy the **Connection string** in **URI** format. It looks like:
```
postgresql://postgres:[YOUR-PASSWORD]@db.xxxxxxxxxxxx.supabase.co:5432/postgres
```

Replace `[YOUR-PASSWORD]` with the password you set in Step 2. Put this in `.env` as `DATABASE_URL`.

---

## Step 4 — Enable the pgvector extension

This project uses `pgvector` for semantic search (storing AI-generated embeddings).

1. In the Supabase dashboard, go to **Database → Extensions** (left sidebar).
2. Search for **vector**.
3. Click the toggle to **enable** it.

You'll see it appear in the enabled list. This is required before running migrations.

---

## Step 5 — Run the database migrations

Make sure your `.env` is filled in, then run:

```bash
# Activate your virtual environment first
source .venv/bin/activate

# Apply all migrations in order
make db.migrate
```

This runs the numbered SQL files in `migrations/` in order, creating all the tables, indexes, and Row Level Security policies.

To verify it worked:
1. In the Supabase dashboard, go to **Table Editor**.
2. You should see tables: `comm_events`, `number_registry`, `transcripts`, `enrichments`, `embeddings`, `delivery_log`.

---

## Step 6 — Seed the number registry

The number registry tells the system which phone numbers belong to which source (campaign, affiliate, business unit). Without it, all events will be captured as `source = "unknown"` — which is fine, but less informative.

Edit `seeds/number_registry_seed.sql` to add your own numbers, then run:

```bash
psql "$DATABASE_URL" -f seeds/number_registry_seed.sql
```

---

## Step 7 — Verify Row Level Security (RLS)

RLS is a Supabase/Postgres feature that ensures each user can only access their own data. Even though this app uses the service-role key (which bypasses RLS), we still enable RLS on every table as a security baseline — if a misconfigured key ever leaks, RLS provides a second line of defence.

To verify RLS is on:
1. In the dashboard, go to **Authentication → Policies**.
2. You should see each table listed with RLS enabled.

Alternatively, run this SQL in the **SQL Editor**:
```sql
SELECT tablename, rowsecurity
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY tablename;
```

Every `rowsecurity` value should be `true`.

---

## Troubleshooting

**"connection refused" when running migrations**
→ Your `DATABASE_URL` is wrong. Double-check the password and project ref (the `xxxx` part of the Supabase URL).

**"extension vector does not exist"**
→ You skipped Step 4. Enable the `vector` extension in the dashboard first, then re-run migrations.

**"permission denied"**
→ You may be using the `anon` key instead of the `service_role` key. The `service_role` key is the one labelled `secret` in the dashboard.
