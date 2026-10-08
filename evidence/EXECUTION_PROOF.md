# McCain Cold Chain Demo — Execution Proof

> **Generated:** 2026-10-08 20:01:41 UTC
> **Workspace:** fevm-serverless-stable-qr9if1.cloud.databricks.com
> **Catalog:** `serverless_stable_qr9if1_catalog`
> **Schema:** `mccain_cold_chain_sju`
> **Executed by:** siddesh.ujjni@databricks.com on Databricks Serverless Compute

This folder contains machine-generated proof that every component of the McCain Cold Chain
demo was executed end-to-end on Databricks. Each evidence file is produced by querying live
Unity Catalog tables, MLflow model registry, the Databricks App API, and Delta table history.

## Components Verified

| # | Component | Evidence File | Status |
|---|-----------|--------------|--------|
| 1 | Lakebase (Postgres) | [01_lakebase_evidence.md](01_lakebase_evidence.md) | ✅ AVAILABLE |
| 2 | Bronze/Silver/Gold Pipeline | [02_pipeline_evidence.md](02_pipeline_evidence.md) | ✅ 18 tables, 492K rows |
| 3 | ML Model (Unity Catalog) | [03_model_evidence.md](03_model_evidence.md) | ✅ Version 1 READY |
| 4 | AI Inference + Governance | [04_ai_inference_evidence.md](04_ai_inference_evidence.md) | ✅ 28 complaints scored |
| 5 | Databricks App | [05_app_evidence.md](05_app_evidence.md) | ✅ RUNNING |
| 6 | Dashboard Queries | [06_dashboard_evidence.md](06_dashboard_evidence.md) | ✅ All 6 queries validated |
| 7 | Data Quality | [07_data_quality_evidence.md](07_data_quality_evidence.md) | ✅ All checks PASS |
| 8 | Delta Table History | [08_delta_history_evidence.md](08_delta_history_evidence.md) | ✅ Full audit trail |
| 9 | Genie Space (NL Interface) | [09_genie_space_evidence.md](09_genie_space_evidence.md) | ✅ 3 demo questions answered |

## Quick Summary

- **492,562 rows** across 18 Unity Catalog tables
- **ML model** registered in UC: `cold_chain_anomaly_detector` version 1 (READY)
- **154,080 telemetry readings** scored, 144 warm-trend detections (0.09%)
- **28 customer complaints** classified by AI (`ai_classify`, `ai_extract`)
- **22 excursion events** detected: 2 HIGH ($63K), 20 MED ($172K)
- **$235,503 total dollars at risk** across all excursions
- **Databricks App** deployed and RUNNING at production URL
- **Lakebase Postgres** instance AVAILABLE with 4 IoT tables
- **Genie Space** live with 9 governed tables — 3 demo questions answered end-to-end
- **All data quality checks PASS**: 0 nulls, 0 orphans, 0 absurd values
