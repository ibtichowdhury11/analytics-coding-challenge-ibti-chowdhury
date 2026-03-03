# Lightdash Setup Guide

This guide assumes the Docker stack is running.

At `make up`, Lightdash is auto-configured and seeded with one **neutral starter** dashboard and one starter chart (not part of the challenge answer).

## 1) Open Lightdash

- URL: `http://localhost:8880`
- Email: `admin@lightdash.com`
- Password: `admin123!`

You should see a dashboard named **Starter Dashboard** with one chart: **Inventory Snapshot (Starter)**.

## 2) Build dbt artifacts for Lightdash model discovery

From repo root:

```bash
make dbt-deps
make dbt-run
make dbt-compile
```

Then in Lightdash, go to **Settings → Project → Syncing** and click **Refresh dbt**.

## 3) Candidate dashboard workflow (dashboard as code)

Candidates should build their own challenge dashboard(s) in Lightdash and commit dashboard code as part of the submission.

Recommended flow:
1. Create/modify charts and dashboards in Lightdash UI.
2. Open the dashboard menu and copy the **dashboard-as-code** YAML/JSON (or equivalent exported code).
3. Save exported artifacts under `lightdash/`.
4. Reference the committed dashboard artifact paths and screenshots in `DESIGN.md`.

This keeps dashboard changes reviewable in git, alongside dbt model changes.

## Troubleshooting

- If models are missing: run `make dbt-compile` and sync again.
- If login fails: run `make restart` and retry.
- If the starter dashboard is missing: check logs with `make logs` and inspect the `lightdash-setup` service output.
