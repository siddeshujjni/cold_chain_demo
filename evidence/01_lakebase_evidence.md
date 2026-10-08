# Evidence: Lakebase (Postgres) Instance

> **Generated:** 2026-10-08 20:01:41 UTC

## Instance Details

| Property | Value |
|----------|-------|
| Instance Name | `mccain-cold-chain-sju` |
| State | **AVAILABLE** |
| Read/Write Endpoint | `ep-curly-paper-d2nhs7l3.database.us-east-1.cloud.databricks.com` |
| Database | `databricks_postgres` |
| Schema | `iot` |

## IoT Tables in Lakebase

| Table | Rows | Description |
|-------|------|-------------|
| `iot.trailers` | 15 | Fleet of 15 reefer trailers (T-001 through T-015) |
| `iot.reefer_telemetry` | 154,080 | Minute-level sensor readings (supply_air_c, return_air_c, compressor_on, fuel_level_pct, alarm_code) |
| `iot.door_events` | 366 | Trailer door open/close events during transit |
| `iot.gps_pings` | 29,496 | GPS lat/lon pings per trailer every ~2 minutes |

## Verification

The Lakebase instance was provisioned via the Databricks SDK (`w.database.get_database_instance`).
Data was loaded via `psycopg` bulk inserts from generated DataFrames. The bronze ingestion
notebook (`01_lakebase_to_lakehouse`) connects to this instance, pulls data, and writes
to Unity Catalog bronze tables.
