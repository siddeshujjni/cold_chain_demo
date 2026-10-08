# Evidence: Data Quality Validation

> **Generated:** 2026-10-08 20:03:25 UTC

## silver_reefer_telemetry (154,080 rows)

| Check | Result | Expected | Status |
|-------|--------|----------|--------|
| Null supply_air_c | 0 | 0 | ✅ PASS |
| Null timestamp | 0 | 0 | ✅ PASS |
| Absurd temps (<-50°C or >20°C) | 0 | 0 | ✅ PASS |
| Orphan shipments (no matching shipment) | 0 | 0 | ✅ PASS |

## silver_shipment_profile (120 rows)

| Check | Result | Expected | Status |
|-------|--------|----------|--------|
| Null max_supply_c | 0 | 0 | ✅ PASS |
| Negative transit hours | 0 | 0 | ✅ PASS |

## gold_excursion_events (22 rows)

| Check | Result | Expected | Status |
|-------|--------|----------|--------|
| Null severity | 0 | 0 | ✅ PASS |
| Zero or negative dollars_at_risk | 0 | 0 | ✅ PASS |

## gold_inference_log (28 rows)

| Check | Result | Expected | Status |
|-------|--------|----------|--------|
| Null inference_model | 0 | 0 | ✅ PASS |
| Null inference_ts | 0 | 0 | ✅ PASS |

## Summary

**All data quality checks PASS.** The pipeline produces clean, complete, referentially
intact data from bronze through gold.
