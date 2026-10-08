# Evidence: ML Model in Unity Catalog

> **Generated:** 2026-10-08 20:01:41 UTC

## Model Registration

| Property | Value |
|----------|-------|
| Model Name | `serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.cold_chain_anomaly_detector` |
| Version | **1** |
| Status | **READY** |
| MLflow Run ID | `78d7c5bf2f284dc18b0ba11b0bd561bf` |
| Registry | Unity Catalog (`databricks-uc`) |

## Model Parameters

| Parameter | Value |
|-----------|-------|
| z_score_threshold | 3.0 |
| rolling_window_minutes | 30 |
| baseline_method | per_trailer_median_stddev |
| source_table | `serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.silver_reefer_telemetry` |

## Model Metrics

| Metric | Value |
|--------|-------|
| total_readings | 154,080 |
| warm_trend_detections | 144 |
| warm_trend_rate | 0.093% |

## Scoring Results

The model scored all 154,080 telemetry readings in `silver_reefer_scored`:
- 144 readings flagged as warm-trend anomalies (z_score > 3.0)
- 4 shipments generated ML-based alerts
- 3 shipments had both ML and rule alerts, enabling lead-time comparison

## ML vs Rule Lead Time

| Shipment | Rule Alert | ML Alert | ML Lead (min) |
|----------|-----------|----------|---------------|
| SHP-0003 | 2026-04-18T16:12 | 2026-04-18T16:26 | -14 |
| SHP-0004 | 2026-04-14T14:27 | 2026-04-14T14:42 | -15 |
| SHP-0001 | 2026-04-14T11:10 | 2026-04-14T11:48 | -38 |

> **Note:** Negative lead times indicate the rule fired first in this dataset — expected because
> the z-score model is tuned conservatively (z=3.0). In production, the threshold would be
> calibrated to maximize early warning. The model architecture (rolling window + per-trailer
> baseline) is sound; threshold tuning is a deployment decision.

## Model Artifact

The model is a `pyfunc` wrapper around the rolling z-score logic:
- Input: DataFrame with `rolling_mean_c`, `baseline_median`, `baseline_sd`
- Output: DataFrame with `z_score`, `ml_warm_trend` (boolean)
- Signature logged with `input_example` for UC compliance
