# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver & Gold
# MAGIC
# MAGIC **What we build here:**
# MAGIC
# MAGIC - **silver_reefer_telemetry** — cleaned, joined with shipment context (SKU, required temp, tolerance)
# MAGIC - **silver_shipment_profile** — one row per shipment with peak/min temps, duration above tolerance, minutes the door was open mid-transit
# MAGIC - **gold_excursion_events** — discrete "excursion" events per shipment (contiguous runs above tolerance) — the thing operations actually cares about
# MAGIC
# MAGIC Data quality expectations live inline so you can talk to them during the demo.

# COMMAND ----------

UC_CATALOG = "main"
UC_SCHEMA = "mccain_cold_chain_sju"
spark.sql(f"USE {UC_CATALOG}.{UC_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. silver_reefer_telemetry  — cleaned + enriched

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE main.mccain_cold_chain_sju.silver_reefer_telemetry AS
# MAGIC SELECT
# MAGIC     rt.trailer_id,
# MAGIC     rt.shipment_id,
# MAGIC     rt.ts,
# MAGIC     rt.set_point_c,
# MAGIC     rt.supply_air_c,
# MAGIC     rt.return_air_c,
# MAGIC     rt.compressor_on,
# MAGIC     rt.fuel_level_pct,
# MAGIC     rt.alarm_code,
# MAGIC     s.sku,
# MAGIC     s.carrier_id,
# MAGIC     s.dest_dc_id,
# MAGIC     s.pallets,
# MAGIC     s.ship_ts,
# MAGIC     s.arrival_ts,
# MAGIC     pm.product_family,
# MAGIC     pm.target_temp_c,
# MAGIC     pm.upper_tolerance_c,
# MAGIC     pm.lower_tolerance_c,
# MAGIC     pm.unit_value_usd,
# MAGIC     (rt.supply_air_c > pm.upper_tolerance_c) AS is_above_tolerance
# MAGIC FROM main.mccain_cold_chain_sju.bronze_reefer_telemetry rt
# MAGIC JOIN main.mccain_cold_chain_sju.shipments       s  USING (shipment_id)
# MAGIC JOIN main.mccain_cold_chain_sju.product_master  pm ON s.sku = pm.sku
# MAGIC WHERE rt.supply_air_c IS NOT NULL
# MAGIC   AND rt.ts BETWEEN s.ship_ts - INTERVAL 1 HOUR
# MAGIC                AND s.arrival_ts + INTERVAL 1 HOUR;

# COMMAND ----------

# DBTITLE 1,Data Quality Expectations (fail-loud — talk to these on stage)
from pyspark.sql.functions import col

dq = {}
t = spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.silver_reefer_telemetry")
dq["rows_total"]          = t.count()
dq["rows_null_supply"]    = t.filter(col("supply_air_c").isNull()).count()
dq["rows_absurd_temp"]    = t.filter((col("supply_air_c") < -50) | (col("supply_air_c") > 20)).count()
dq["rows_orphan_shipment"] = (t.join(spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.shipments"),
                                     "shipment_id", "left_anti").count())
for k, v in dq.items():
    print(f"  {k:<22} {v:>8,}")
assert dq["rows_null_supply"] == 0,    "Null supply temp after silver — STOP"
assert dq["rows_absurd_temp"] == 0,    "Physically impossible temps — STOP"
assert dq["rows_orphan_shipment"] == 0, "Telemetry without matching shipment — STOP"
print("✅ silver DQ clean")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. silver_shipment_profile  — one row per shipment

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE main.mccain_cold_chain_sju.silver_shipment_profile AS
# MAGIC WITH door_open_mid AS (
# MAGIC     SELECT d.shipment_id, sum(CASE WHEN d.event = 'door_open' THEN 1 ELSE 0 END) AS mid_transit_door_opens
# MAGIC     FROM main.mccain_cold_chain_sju.bronze_door_events d
# MAGIC     JOIN main.mccain_cold_chain_sju.shipments s USING (shipment_id)
# MAGIC     WHERE d.ts BETWEEN s.ship_ts + INTERVAL 10 MINUTE AND s.arrival_ts - INTERVAL 10 MINUTE
# MAGIC     GROUP BY d.shipment_id
# MAGIC )
# MAGIC SELECT
# MAGIC     s.shipment_id,
# MAGIC     s.trailer_id,
# MAGIC     s.carrier_id,
# MAGIC     s.dest_dc_id,
# MAGIC     s.sku,
# MAGIC     s.pallets,
# MAGIC     s.ship_ts,
# MAGIC     s.arrival_ts,
# MAGIC     (unix_timestamp(s.arrival_ts) - unix_timestamp(s.ship_ts))/3600.0 AS transit_hours,
# MAGIC     pm.upper_tolerance_c,
# MAGIC     pm.unit_value_usd,
# MAGIC     round(min(rt.supply_air_c), 2)                                          AS min_supply_c,
# MAGIC     round(max(rt.supply_air_c), 2)                                          AS max_supply_c,
# MAGIC     round(avg(rt.supply_air_c), 2)                                          AS avg_supply_c,
# MAGIC     sum(CASE WHEN rt.is_above_tolerance THEN 1 ELSE 0 END)                  AS minutes_above_tolerance,
# MAGIC     sum(CASE WHEN rt.alarm_code IS NOT NULL THEN 1 ELSE 0 END)              AS alarm_minutes,
# MAGIC     max(CASE WHEN rt.alarm_code IS NOT NULL THEN rt.alarm_code END)         AS top_alarm,
# MAGIC     coalesce(dm.mid_transit_door_opens, 0)                                  AS mid_transit_door_opens,
# MAGIC     (max(rt.supply_air_c) > pm.upper_tolerance_c)                           AS had_excursion,
# MAGIC     s.pallets * pm.unit_value_usd * 40                                      AS shipment_value_usd
# MAGIC FROM main.mccain_cold_chain_sju.silver_reefer_telemetry rt
# MAGIC JOIN main.mccain_cold_chain_sju.shipments       s  USING (shipment_id)
# MAGIC JOIN main.mccain_cold_chain_sju.product_master  pm ON s.sku = pm.sku
# MAGIC LEFT JOIN door_open_mid dm ON dm.shipment_id = s.shipment_id
# MAGIC GROUP BY s.shipment_id, s.trailer_id, s.carrier_id, s.dest_dc_id, s.sku, s.pallets,
# MAGIC          s.ship_ts, s.arrival_ts, pm.upper_tolerance_c, pm.unit_value_usd, dm.mid_transit_door_opens;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT had_excursion, count(*) AS shipments,
# MAGIC        round(avg(max_supply_c), 2) AS avg_peak_c,
# MAGIC        round(sum(shipment_value_usd)::numeric, 0) AS total_value_usd
# MAGIC FROM main.mccain_cold_chain_sju.silver_shipment_profile
# MAGIC GROUP BY had_excursion;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. gold_excursion_events — one row per contiguous "warm window"
# MAGIC
# MAGIC This is the table operations acts on: each row is an actionable event with duration, peak temp, and dollars at risk.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE main.mccain_cold_chain_sju.gold_excursion_events AS
# MAGIC WITH flagged AS (
# MAGIC     SELECT
# MAGIC         shipment_id, trailer_id, carrier_id, dest_dc_id, sku, product_family,
# MAGIC         ts, supply_air_c, upper_tolerance_c, unit_value_usd, pallets,
# MAGIC         is_above_tolerance,
# MAGIC         -- group contiguous above-tolerance rows: count "transitions into tolerance" before each row
# MAGIC         sum(CASE WHEN NOT is_above_tolerance THEN 1 ELSE 0 END)
# MAGIC             OVER (PARTITION BY shipment_id ORDER BY ts ROWS UNBOUNDED PRECEDING) AS grp
# MAGIC     FROM main.mccain_cold_chain_sju.silver_reefer_telemetry
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
# MAGIC     -- simple risk score & $ at risk: short dips < 15 min are low risk;
# MAGIC     -- above-tolerance > 60 min with >3C overshoot → high risk, full pallet at-risk $
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
# MAGIC WHERE duration_min >= 5;  -- ignore <5-min flickers

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT severity, count(*) AS events,
# MAGIC        sum(dollars_at_risk) AS dollars_at_risk,
# MAGIC        round(avg(duration_min)) AS avg_minutes
# MAGIC FROM main.mccain_cold_chain_sju.gold_excursion_events
# MAGIC GROUP BY severity
# MAGIC ORDER BY CASE severity WHEN 'HIGH' THEN 1 WHEN 'MED' THEN 2 ELSE 3 END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The "punch list" for ops: top 10 worst events, ready to page a carrier.
# MAGIC SELECT excursion_id, trailer_id, carrier_id, dest_dc_id, sku,
# MAGIC        start_ts, duration_min, peak_supply_c, peak_over_tolerance_c,
# MAGIC        severity, dollars_at_risk
# MAGIC FROM main.mccain_cold_chain_sju.gold_excursion_events
# MAGIC ORDER BY dollars_at_risk DESC
# MAGIC LIMIT 10;
