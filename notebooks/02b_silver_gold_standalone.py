# Databricks notebook source

# COMMAND ----------

# DBTITLE 1,02b — Silver & Gold (Standalone)
# 02b — Silver & Gold (Standalone Execution)

**Use this notebook** when you want to demo the medallion flow interactively without standing up the Lakeflow pipeline. It produces the same tables as `02_medallion_pipeline` using `CREATE OR REPLACE TABLE` instead of `dlt.table()`.

> The Lakeflow SDP version (02_medallion_pipeline) is the production artifact. This notebook exists for fast, interactive demo walkthroughs with visible cell output.

# COMMAND ----------

# DBTITLE 1,Config
UC_CATALOG = "serverless_stable_qr9if1_catalog"
UC_SCHEMA = "mccain_cold_chain_sju"
spark.sql(f"USE {UC_CATALOG}.{UC_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,1. silver_reefer_telemetry
# MAGIC %sql
# MAGIC -- Silver: cleaned + enriched telemetry with shipment/product context
# MAGIC CREATE OR REPLACE TABLE serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.silver_reefer_telemetry AS
# MAGIC SELECT
# MAGIC     rt.trailer_id, rt.shipment_id, rt.ts,
# MAGIC     rt.set_point_c, rt.supply_air_c, rt.return_air_c,
# MAGIC     rt.compressor_on, rt.fuel_level_pct, rt.alarm_code,
# MAGIC     s.sku, s.carrier_id, s.dest_dc_id, s.pallets, s.ship_ts, s.arrival_ts,
# MAGIC     pm.product_family, pm.target_temp_c, pm.upper_tolerance_c,
# MAGIC     pm.lower_tolerance_c, pm.unit_value_usd,
# MAGIC     (rt.supply_air_c > pm.upper_tolerance_c) AS is_above_tolerance
# MAGIC FROM serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.bronze_reefer_telemetry rt
# MAGIC JOIN serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.shipments       s  USING (shipment_id)
# MAGIC JOIN serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.product_master  pm ON s.sku = pm.sku
# MAGIC WHERE rt.supply_air_c IS NOT NULL
# MAGIC   AND rt.ts BETWEEN s.ship_ts - INTERVAL 1 HOUR
# MAGIC                AND s.arrival_ts + INTERVAL 1 HOUR

# COMMAND ----------

# DBTITLE 1,DQ Expectations (execution evidence)
from pyspark.sql.functions import col

# --- EXECUTION EVIDENCE: Data Quality checks ---
print("="*60)
print("SILVER DATA QUALITY VALIDATION")
print("="*60)

dq = {}
t = spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.silver_reefer_telemetry")
dq["rows_total"]          = t.count()
dq["rows_null_supply"]    = t.filter(col("supply_air_c").isNull()).count()
dq["rows_absurd_temp"]    = t.filter((col("supply_air_c") < -50) | (col("supply_air_c") > 20)).count()
dq["rows_orphan_shipment"] = (t.join(spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.shipments"),
                                     "shipment_id", "left_anti").count())
for k, v in dq.items():
    status = "✅" if v == 0 or k == "rows_total" else "❌"
    print(f"  {status} {k:<22} {v:>8,}")

assert dq["rows_null_supply"] == 0,    "Null supply temp after silver — STOP"
assert dq["rows_absurd_temp"] == 0,    "Physically impossible temps — STOP"
assert dq["rows_orphan_shipment"] == 0, "Telemetry without matching shipment — STOP"
print(f"\n✅ Silver DQ: all {dq['rows_total']:,} rows clean")

# COMMAND ----------

# DBTITLE 1,2. silver_shipment_profile
# MAGIC %sql
# MAGIC -- Silver: one row per shipment with temp stats, door events, value
# MAGIC CREATE OR REPLACE TABLE serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.silver_shipment_profile AS
# MAGIC WITH door_open_mid AS (
# MAGIC     SELECT d.shipment_id, sum(CASE WHEN d.event = 'door_open' THEN 1 ELSE 0 END) AS mid_transit_door_opens
# MAGIC     FROM serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.bronze_door_events d
# MAGIC     JOIN serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.shipments s USING (shipment_id)
# MAGIC     WHERE d.ts BETWEEN s.ship_ts + INTERVAL 10 MINUTE AND s.arrival_ts - INTERVAL 10 MINUTE
# MAGIC     GROUP BY d.shipment_id
# MAGIC )
# MAGIC SELECT
# MAGIC     s.shipment_id, s.trailer_id, s.carrier_id, s.dest_dc_id, s.sku, s.pallets,
# MAGIC     s.ship_ts, s.arrival_ts,
# MAGIC     (unix_timestamp(s.arrival_ts) - unix_timestamp(s.ship_ts))/3600.0 AS transit_hours,
# MAGIC     pm.upper_tolerance_c, pm.unit_value_usd,
# MAGIC     round(min(rt.supply_air_c), 2)                                          AS min_supply_c,
# MAGIC     round(max(rt.supply_air_c), 2)                                          AS max_supply_c,
# MAGIC     round(avg(rt.supply_air_c), 2)                                          AS avg_supply_c,
# MAGIC     sum(CASE WHEN rt.is_above_tolerance THEN 1 ELSE 0 END)                  AS minutes_above_tolerance,
# MAGIC     sum(CASE WHEN rt.alarm_code IS NOT NULL THEN 1 ELSE 0 END)              AS alarm_minutes,
# MAGIC     max(CASE WHEN rt.alarm_code IS NOT NULL THEN rt.alarm_code END)         AS top_alarm,
# MAGIC     coalesce(dm.mid_transit_door_opens, 0)                                  AS mid_transit_door_opens,
# MAGIC     (max(rt.supply_air_c) > pm.upper_tolerance_c)                           AS had_excursion,
# MAGIC     s.pallets * pm.unit_value_usd * 40                                      AS shipment_value_usd
# MAGIC FROM serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.silver_reefer_telemetry rt
# MAGIC JOIN serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.shipments       s  USING (shipment_id)
# MAGIC JOIN serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.product_master  pm ON s.sku = pm.sku
# MAGIC LEFT JOIN door_open_mid dm ON dm.shipment_id = s.shipment_id
# MAGIC GROUP BY s.shipment_id, s.trailer_id, s.carrier_id, s.dest_dc_id, s.sku, s.pallets,
# MAGIC          s.ship_ts, s.arrival_ts, pm.upper_tolerance_c, pm.unit_value_usd, dm.mid_transit_door_opens

# COMMAND ----------

# DBTITLE 1,Excursion summary (execution evidence)
# MAGIC %sql
# MAGIC -- EXECUTION EVIDENCE: excursion vs clean shipment summary
# MAGIC SELECT had_excursion, count(*) AS shipments,
# MAGIC        round(avg(max_supply_c), 2) AS avg_peak_c,
# MAGIC        round(sum(shipment_value_usd), 0) AS total_value_usd
# MAGIC FROM serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.silver_shipment_profile
# MAGIC GROUP BY had_excursion

# COMMAND ----------

# DBTITLE 1,3. gold_excursion_events
# MAGIC %sql
# MAGIC -- Gold: one row per contiguous excursion event with severity + dollars at risk
# MAGIC CREATE OR REPLACE TABLE serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.gold_excursion_events AS
# MAGIC WITH flagged AS (
# MAGIC     SELECT
# MAGIC         shipment_id, trailer_id, carrier_id, dest_dc_id, sku, product_family,
# MAGIC         ts, supply_air_c, upper_tolerance_c, unit_value_usd, pallets,
# MAGIC         is_above_tolerance,
# MAGIC         sum(CASE WHEN NOT is_above_tolerance THEN 1 ELSE 0 END)
# MAGIC             OVER (PARTITION BY shipment_id ORDER BY ts ROWS UNBOUNDED PRECEDING) AS grp
# MAGIC     FROM serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.silver_reefer_telemetry
# MAGIC ),
# MAGIC windows AS (
# MAGIC     SELECT
# MAGIC         shipment_id, trailer_id, carrier_id, dest_dc_id, sku, product_family, pallets, unit_value_usd,
# MAGIC         grp,
# MAGIC         min(ts)                                 AS start_ts,
# MAGIC         max(ts)                                 AS end_ts,
# MAGIC         round(max(supply_air_c), 2)             AS peak_supply_c,
# MAGIC         round(max(supply_air_c) - avg(upper_tolerance_c), 2) AS peak_over_tolerance_c,
# MAGIC         count(*)                                AS duration_min
# MAGIC     FROM flagged
# MAGIC     WHERE is_above_tolerance
# MAGIC     GROUP BY shipment_id, trailer_id, carrier_id, dest_dc_id, sku, product_family, pallets, unit_value_usd, grp
# MAGIC )
# MAGIC SELECT
# MAGIC     concat(shipment_id, '-EX', lpad(cast(grp AS string), 3, '0')) AS excursion_id,
# MAGIC     shipment_id, trailer_id, carrier_id, dest_dc_id,
# MAGIC     sku, product_family, pallets,
# MAGIC     start_ts, end_ts, duration_min,
# MAGIC     peak_supply_c, peak_over_tolerance_c,
# MAGIC     CASE
# MAGIC       WHEN duration_min >= 60 AND peak_over_tolerance_c >= 3 THEN 'HIGH'
# MAGIC       WHEN duration_min >= 20 OR peak_over_tolerance_c >= 2   THEN 'MED'
# MAGIC       ELSE 'LOW'
# MAGIC     END AS severity,
# MAGIC     round(
# MAGIC       pallets * unit_value_usd * 40 *
# MAGIC       CASE WHEN duration_min >= 60 AND peak_over_tolerance_c >= 3 THEN 1.0
# MAGIC            WHEN duration_min >= 20 OR peak_over_tolerance_c >= 2 THEN 0.35
# MAGIC            ELSE 0.05 END,
# MAGIC     0) AS dollars_at_risk
# MAGIC FROM windows
# MAGIC WHERE duration_min >= 5

# COMMAND ----------

# DBTITLE 1,Gold summary (execution evidence)
# MAGIC %sql
# MAGIC -- EXECUTION EVIDENCE: excursion severity breakdown with dollars at risk
# MAGIC SELECT severity, count(*) AS events,
# MAGIC        sum(dollars_at_risk) AS total_dollars_at_risk,
# MAGIC        round(avg(duration_min)) AS avg_minutes
# MAGIC FROM serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.gold_excursion_events
# MAGIC GROUP BY severity
# MAGIC ORDER BY CASE severity WHEN 'HIGH' THEN 1 WHEN 'MED' THEN 2 ELSE 3 END

# COMMAND ----------

# DBTITLE 1,Top 10 worst excursions (execution evidence)
# MAGIC %sql
# MAGIC -- EXECUTION EVIDENCE: The "punch list" for ops — top 10 worst events
# MAGIC SELECT excursion_id, trailer_id, carrier_id, dest_dc_id, sku,
# MAGIC        start_ts, duration_min, peak_supply_c, peak_over_tolerance_c,
# MAGIC        severity, dollars_at_risk
# MAGIC FROM serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.gold_excursion_events
# MAGIC ORDER BY dollars_at_risk DESC
# MAGIC LIMIT 10