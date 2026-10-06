# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Ops Dashboard (queries + import-ready spec)
# MAGIC
# MAGIC This notebook prepares the queries behind the AI/BI Dashboard. Run the queries once to validate them, then import the dashboard JSON emitted by the last cell into your workspace.
# MAGIC
# MAGIC **Dashboard contents:**
# MAGIC 1. **KPI row** — active shipments, excursions (24h), % excursion rate, total $ at risk
# MAGIC 2. **Live trailer map** (lat/lon from GPS, colored by severity)
# MAGIC 3. **Carrier scorecard** — excursions per 100 shipments, avg over-tolerance °C
# MAGIC 4. **Alerts feed** — top 20 open excursion events with drill links
# MAGIC 5. **ML vs Rule lead time** — bar chart
# MAGIC 6. **Complaint root-cause mix** — stacked bar from notebook 04

# COMMAND ----------

UC_CATALOG = "main"
UC_SCHEMA = "mccain_cold_chain_sju"
spark.sql(f"USE {UC_CATALOG}.{UC_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query 1 — KPI tiles

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT count(*) FROM main.mccain_cold_chain_sju.shipments)                                          AS active_shipments,
# MAGIC   (SELECT count(*) FROM main.mccain_cold_chain_sju.gold_excursion_events WHERE severity IN ('MED','HIGH')) AS recent_excursions,
# MAGIC   round(100.0 * (SELECT count(*) FROM main.mccain_cold_chain_sju.silver_shipment_profile WHERE had_excursion) /
# MAGIC                 (SELECT count(*) FROM main.mccain_cold_chain_sju.silver_shipment_profile), 1)          AS pct_shipments_excursed,
# MAGIC   coalesce((SELECT sum(dollars_at_risk) FROM main.mccain_cold_chain_sju.gold_excursion_events), 0)     AS total_dollars_at_risk;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query 2 — Live trailer map

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Latest known position per active trailer, color-coded by worst severity in window
# MAGIC WITH latest_pos AS (
# MAGIC   SELECT trailer_id, lat, lon, ts,
# MAGIC          row_number() OVER (PARTITION BY trailer_id ORDER BY ts DESC) AS rn
# MAGIC   FROM main.mccain_cold_chain_sju.bronze_gps_pings
# MAGIC ),
# MAGIC worst AS (
# MAGIC   SELECT trailer_id,
# MAGIC          max(CASE severity WHEN 'HIGH' THEN 3 WHEN 'MED' THEN 2 WHEN 'LOW' THEN 1 END) AS sev_rank
# MAGIC   FROM main.mccain_cold_chain_sju.gold_excursion_events
# MAGIC   GROUP BY trailer_id
# MAGIC )
# MAGIC SELECT p.trailer_id, p.lat, p.lon, p.ts,
# MAGIC        coalesce(CASE w.sev_rank WHEN 3 THEN 'HIGH' WHEN 2 THEN 'MED' WHEN 1 THEN 'LOW' END, 'OK') AS status
# MAGIC FROM latest_pos p
# MAGIC LEFT JOIN worst w USING (trailer_id)
# MAGIC WHERE rn = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query 3 — Carrier scorecard

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     c.carrier_name,
# MAGIC     count(DISTINCT sp.shipment_id)                                                          AS shipments,
# MAGIC     sum(CASE WHEN sp.had_excursion THEN 1 ELSE 0 END)                                       AS excursions,
# MAGIC     round(100.0 * sum(CASE WHEN sp.had_excursion THEN 1 ELSE 0 END)
# MAGIC                 / nullif(count(DISTINCT sp.shipment_id), 0), 1)                             AS excursion_rate_pct,
# MAGIC     round(avg(CASE WHEN sp.had_excursion THEN sp.max_supply_c - sp.upper_tolerance_c END),2) AS avg_over_tolerance_c,
# MAGIC     sum(CASE WHEN e.dollars_at_risk IS NOT NULL THEN e.dollars_at_risk ELSE 0 END)          AS dollars_at_risk
# MAGIC FROM main.mccain_cold_chain_sju.silver_shipment_profile sp
# MAGIC JOIN main.mccain_cold_chain_sju.carriers c ON c.carrier_id = sp.carrier_id
# MAGIC LEFT JOIN main.mccain_cold_chain_sju.gold_excursion_events e ON e.shipment_id = sp.shipment_id
# MAGIC GROUP BY c.carrier_name
# MAGIC ORDER BY excursion_rate_pct DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query 4 — Alerts feed (ops punch-list)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT excursion_id, shipment_id, trailer_id, carrier_id, dest_dc_id,
# MAGIC        sku, start_ts, duration_min, peak_supply_c, peak_over_tolerance_c,
# MAGIC        severity, dollars_at_risk
# MAGIC FROM main.mccain_cold_chain_sju.gold_excursion_events
# MAGIC ORDER BY CASE severity WHEN 'HIGH' THEN 1 WHEN 'MED' THEN 2 ELSE 3 END,
# MAGIC          dollars_at_risk DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query 5 — ML lead time

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH first_rule AS (
# MAGIC   SELECT shipment_id, min(detected_ts) AS rule_ts FROM main.mccain_cold_chain_sju.alerts_rule_based GROUP BY shipment_id
# MAGIC ),
# MAGIC first_ml AS (
# MAGIC   SELECT shipment_id, min(detected_ts) AS ml_ts FROM main.mccain_cold_chain_sju.alerts_ml_based GROUP BY shipment_id
# MAGIC )
# MAGIC SELECT f.shipment_id,
# MAGIC        cast((unix_timestamp(r.rule_ts) - unix_timestamp(f.ml_ts)) / 60 AS int) AS ml_lead_minutes
# MAGIC FROM first_ml f
# MAGIC JOIN first_rule r ON r.shipment_id = f.shipment_id
# MAGIC ORDER BY ml_lead_minutes DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query 6 — Complaint root-cause mix

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sensor_root_cause, alleged_cause, count(*) AS complaints
# MAGIC FROM main.mccain_cold_chain_sju.gold_complaint_root_cause
# MAGIC GROUP BY sensor_root_cause, alleged_cause
# MAGIC ORDER BY complaints DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to build the dashboard
# MAGIC
# MAGIC 1. **Workspace → Dashboards → New Dashboard** → name it *McCain Cold Chain Monitor*.
# MAGIC 2. For each query above, **Add dataset → SQL** and paste.
# MAGIC 3. Add widgets:
# MAGIC    - Q1 → four **Counter** tiles (top row)
# MAGIC    - Q2 → **Map** widget, lat/lon + color by `status`
# MAGIC    - Q3 → **Bar** chart, carrier_name × excursion_rate_pct, secondary axis dollars_at_risk
# MAGIC    - Q4 → **Table** with conditional formatting on severity
# MAGIC    - Q5 → **Histogram / Bar** of `ml_lead_minutes`
# MAGIC    - Q6 → **Stacked bar** — sensor_root_cause on x, segmented by alleged_cause
# MAGIC 4. Publish → schedule refresh → share with McCain-L&D audience.
# MAGIC
# MAGIC During the talk, open the dashboard **after** notebook 02 so the tiles come alive as you run upstream queries.
