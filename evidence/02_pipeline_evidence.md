# Evidence: Bronze → Silver → Gold Pipeline

> **Generated:** 2026-10-08 20:01:41 UTC

## Table Census — All 18 Unity Catalog Tables

| Layer | Table | Rows | Description |
|-------|-------|------|-------------|
| Bronze | `bronze_reefer_telemetry` | 154,080 | Raw sensor telemetry from Lakebase |
| Bronze | `bronze_door_events` | 366 | Trailer door open/close events |
| Bronze | `bronze_gps_pings` | 29,496 | GPS lat/lon per trailer |
| Bronze | `bronze_trailers` | 15 | Fleet of 15 reefer trailers |
| Analytical | `carriers` | 5 | Carrier dimension (5 Canadian carriers) |
| Analytical | `retailer_dcs` | 8 | Retailer distribution centers |
| Analytical | `product_master` | 50 | Product catalog with temp tolerances |
| Analytical | `shipments` | 120 | Shipment manifest with trailer/carrier/DC |
| Analytical | `customer_complaints` | 28 | Raw complaint text from retailers |
| Silver | `silver_reefer_telemetry` | 154,080 | Cleaned + enriched telemetry with product context |
| Silver | `silver_shipment_profile` | 120 | Per-shipment rollup with excursion flags |
| Silver | `silver_reefer_scored` | 154,080 | ML-scored telemetry with z_score + ml_warm_trend |
| Gold | `gold_excursion_events` | 22 | Contiguous excursion events with severity + dollars |
| Gold | `alerts_rule_based` | 4 | Rule alerts: >upper_tol for ≥20 min |
| Gold | `alerts_ml_based` | 4 | ML alerts: warm-trend z-score exceeds threshold |
| Silver | `silver_complaints_scored` | 28 | AI-classified complaints with severity |
| Gold | `gold_complaint_root_cause` | 28 | Complaint ↔ sensor root cause join |
| Gold | `gold_inference_log` | 28 | Governed AI inference audit trail |

**Total: 492,562 rows across 18 tables**

## Pipeline Flow

```
Lakebase Postgres (IoT) ──→ Bronze (4 tables)
                              │
Analytical (5 tables) ────────┤
                              ▼
                         Silver (3 tables)
                              │
                              ├──→ Gold excursion events (22 events, $235K at risk)
                              ├──→ Rule-based alerts (4 alerts)
                              ├──→ ML-based alerts (4 alerts)
                              │
Customer Complaints (28) ─────┤
                              ▼
                         AI Classify + Extract
                              │
                              ├──→ Gold complaint root cause (28 matched)
                              └──→ Gold inference log (28 audited)
```

## Medallion Architecture

**Bronze:** Raw data lands from Lakebase with `_ingested_ts` and `_source` columns for lineage.
All timestamps preserved from source.

**Silver:** Telemetry joined with shipments and product_master. Enriched with `is_above_tolerance`
flag. DQ validated: 0 nulls, 0 orphans, 0 absurd temps. Shipment profiles aggregated with
`had_excursion`, `max_supply_c`, `minutes_above_tolerance`, `alarm_minutes`.

**Gold:** Contiguous excursion windows detected using gap-and-island SQL. Severity classified
(HIGH: ≥20 min + peak ≥3°C over tolerance, MED: ≥3°C over but <20 min). Dollar risk calculated
from product `unit_value_usd` × pallets.
