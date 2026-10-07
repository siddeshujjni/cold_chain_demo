# Databricks notebook source

# COMMAND ----------

# DBTITLE 1,05 — Ops Dashboard Queries (validation)
# 05 — Ops Dashboard Queries

This notebook validates the queries that power the **McCain Cold Chain Monitor** AI/BI Dashboard. Run each query to confirm the gold tables produce correct data, then view the actual dashboard artifact.

**Dashboard contents:**
1. KPI row — active shipments, excursions, % excursion rate, total $ at risk
2. Carrier scorecard — excursions per 100 shipments, avg over-tolerance °C
3. Alerts feed — top 20 open excursion events
4. ML vs Rule lead time — bar chart
5. Complaint root-cause mix — stacked bar

> The dashboard is a real Lakeview artifact in this workspace, not just queries in a notebook.

# COMMAND ----------

# DBTITLE 1,Config
UC_CATALOG = "serverless_stable_qr9if1_catalog"
UC_SCHEMA = "mccain_cold_chain_sju"
spark.sql(f"USE {UC_CATALOG}.{UC_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Query 1 — KPI tiles
# MAGIC %sql
# MAGIC -- KPI tiles for the dashboard top row
# MAGIC SELECT
# MAGIC   (SELECT count(*) FROM serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.shipments)                                          AS active_shipments,
# MAGIC   (SELECT count(*) FROM serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.gold_excursion_events WHERE severity IN ('MED','HIGH')) AS recent_excursions,
# MAGIC   round(100.0 * (SELECT count(*) FROM serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.silver_shipment_profile WHERE had_excursion) /
# MAGIC                 (SELECT count(*) FROM serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.silver_shipment_profile), 1)          AS pct_shipments_excursed,
# MAGIC   coalesce((SELECT sum(dollars_at_risk) FROM serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.gold_excursion_events), 0)     AS total_dollars_at_risk

# COMMAND ----------

# DBTITLE 1,Query 2 — Trailer map positions
# MAGIC %sql
# MAGIC -- Latest known position per active trailer, color-coded by worst severity
# MAGIC WITH latest_pos AS (
# MAGIC   SELECT trailer_id, lat, lon, ts,
# MAGIC          row_number() OVER (PARTITION BY trailer_id ORDER BY ts DESC) AS rn
# MAGIC   FROM serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.bronze_gps_pings
# MAGIC ),
# MAGIC worst AS (
# MAGIC   SELECT trailer_id,
# MAGIC          max(CASE severity WHEN 'HIGH' THEN 3 WHEN 'MED' THEN 2 WHEN 'LOW' THEN 1 END) AS sev_rank
# MAGIC   FROM serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.gold_excursion_events
# MAGIC   GROUP BY trailer_id
# MAGIC )
# MAGIC SELECT p.trailer_id, p.lat, p.lon, p.ts,
# MAGIC        coalesce(CASE w.sev_rank WHEN 3 THEN 'HIGH' WHEN 2 THEN 'MED' WHEN 1 THEN 'LOW' END, 'OK') AS status
# MAGIC FROM latest_pos p
# MAGIC LEFT JOIN worst w USING (trailer_id)
# MAGIC WHERE rn = 1

# COMMAND ----------

# DBTITLE 1,Query 3 — Carrier scorecard
# MAGIC %sql
# MAGIC -- Carrier scorecard: excursion rate + dollars at risk per carrier
# MAGIC SELECT
# MAGIC     c.carrier_name,
# MAGIC     count(DISTINCT sp.shipment_id)                                                          AS shipments,
# MAGIC     sum(CASE WHEN sp.had_excursion THEN 1 ELSE 0 END)                                       AS excursions,
# MAGIC     round(100.0 * sum(CASE WHEN sp.had_excursion THEN 1 ELSE 0 END)
# MAGIC                 / nullif(count(DISTINCT sp.shipment_id), 0), 1)                             AS excursion_rate_pct,
# MAGIC     round(avg(CASE WHEN sp.had_excursion THEN sp.max_supply_c - sp.upper_tolerance_c END),2) AS avg_over_tolerance_c,
# MAGIC     sum(CASE WHEN e.dollars_at_risk IS NOT NULL THEN e.dollars_at_risk ELSE 0 END)          AS dollars_at_risk
# MAGIC FROM serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.silver_shipment_profile sp
# MAGIC JOIN serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.carriers c ON c.carrier_id = sp.carrier_id
# MAGIC LEFT JOIN serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.gold_excursion_events e ON e.shipment_id = sp.shipment_id
# MAGIC GROUP BY c.carrier_name
# MAGIC ORDER BY excursion_rate_pct DESC

# COMMAND ----------

# DBTITLE 1,Query 4 — Alerts feed
# MAGIC %sql
# MAGIC -- Ops punch-list: top 20 excursion events by severity and dollars at risk
# MAGIC SELECT excursion_id, shipment_id, trailer_id, carrier_id, dest_dc_id,
# MAGIC        sku, start_ts, duration_min, peak_supply_c, peak_over_tolerance_c,
# MAGIC        severity, dollars_at_risk
# MAGIC FROM serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.gold_excursion_events
# MAGIC ORDER BY CASE severity WHEN 'HIGH' THEN 1 WHEN 'MED' THEN 2 ELSE 3 END,
# MAGIC          dollars_at_risk DESC
# MAGIC LIMIT 20

# COMMAND ----------

# DBTITLE 1,Query 5 — ML lead time
# MAGIC %sql
# MAGIC -- ML vs Rule lead time comparison
# MAGIC WITH first_rule AS (
# MAGIC   SELECT shipment_id, min(detected_ts) AS rule_ts FROM serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.alerts_rule_based GROUP BY shipment_id
# MAGIC ),
# MAGIC first_ml AS (
# MAGIC   SELECT shipment_id, min(detected_ts) AS ml_ts FROM serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.alerts_ml_based GROUP BY shipment_id
# MAGIC )
# MAGIC SELECT f.shipment_id,
# MAGIC        cast((unix_timestamp(r.rule_ts) - unix_timestamp(f.ml_ts)) / 60 AS int) AS ml_lead_minutes
# MAGIC FROM first_ml f
# MAGIC JOIN first_rule r ON r.shipment_id = f.shipment_id
# MAGIC ORDER BY ml_lead_minutes DESC

# COMMAND ----------

# DBTITLE 1,Query 6 — Complaint root-cause mix
# MAGIC %sql
# MAGIC -- Complaint root-cause mix (stacked bar)
# MAGIC SELECT sensor_root_cause, alleged_cause, count(*) AS complaints
# MAGIC FROM serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.gold_complaint_root_cause
# MAGIC GROUP BY sensor_root_cause, alleged_cause
# MAGIC ORDER BY complaints DESC