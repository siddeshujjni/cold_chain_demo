# Evidence: Databricks App Deployment

> **Generated:** 2026-10-08 20:02:36 UTC

## App Details

| Property | Value |
|----------|-------|
| App Name | `mccain-cold-chain-monitor` |
| URL | [https://mccain-cold-chain-monitor-7474643830998004.aws.databricksapps.com](https://mccain-cold-chain-monitor-7474643830998004.aws.databricksapps.com) |
| Compute Status | **ACTIVE** |
| Deployment ID | `01f1c1dc8bd91d3192c9bef99f2b3612` |
| Deployment Status | **SUCCEEDED** |
| Framework | Gradio |

## App Architecture

- **Frontend:** Gradio web UI with 5 tabs
  1. Fleet Overview — trailer count, excursion rate KPI, last refresh timestamp
  2. Excursion Events — sortable table of all gold_excursion_events
  3. Carrier Scorecard — excursion rate per carrier with drill-down
  4. Complaint Analysis — AI classification results + sensor correlation
  5. ML Alerts — ML vs rule comparison with lead-time metrics

- **Backend:** Direct Spark SQL queries against Unity Catalog tables
- **Auth:** Databricks workspace SSO (same RBAC as notebooks)
- **Source files:**
  - `app/app.py` — Gradio application code
  - `app/app.yaml` — Databricks App manifest
  - `app/requirements.txt` — Python dependencies

## Verification

The app was deployed via `databricks apps deploy` and confirmed running via `w.apps.get("mccain-cold-chain-monitor")`.
The compute status is ACTIVE and the deployment status is SUCCEEDED.
