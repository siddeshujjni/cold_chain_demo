# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Root Cause Analysis with AI Functions
# MAGIC
# MAGIC **The demo story:** the same lakehouse that processes sensor streams also reasons about **unstructured text** — customer complaint emails. We close the loop: sensor → excursion → complaint → root cause, all in one query surface.
# MAGIC
# MAGIC We'll use the built-in `ai_classify` / `ai_extract` SQL functions (Foundation Model APIs). No external model hosting, no Python — analysts can use these on day one.

# COMMAND ----------

UC_CATALOG = "main"
UC_SCHEMA = "mccain_cold_chain_sju"
spark.sql(f"USE {UC_CATALOG}.{UC_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. What the raw complaints look like

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT complaint_id, retailer, shipment_id_mentioned, subject, left(body, 200) AS body_preview
# MAGIC FROM main.mccain_cold_chain_sju.customer_complaints
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Classify & extract in SQL — one pass over the table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE main.mccain_cold_chain_sju.silver_complaints_scored AS
# MAGIC SELECT
# MAGIC     c.complaint_id,
# MAGIC     c.received_ts,
# MAGIC     c.retailer,
# MAGIC     c.shipment_id_mentioned,
# MAGIC     c.subject,
# MAGIC     c.body,
# MAGIC     -- Bucket the root cause the complaint is alleging
# MAGIC     ai_classify(
# MAGIC         c.body,
# MAGIC         ARRAY('cold_chain_break', 'freezer_burn', 'texture_quality', 'ice_formation', 'other')
# MAGIC     ) AS alleged_cause,
# MAGIC     -- Extract structured fields straight from the email body
# MAGIC     ai_extract(
# MAGIC         c.body,
# MAGIC         ARRAY('received_temp_c', 'sku_mentioned', 'pallets_affected')
# MAGIC     ) AS extracted,
# MAGIC     -- Severity via ai_classify as a lightweight sentiment proxy
# MAGIC     ai_classify(c.body, ARRAY('low', 'medium', 'high')) AS complaint_severity
# MAGIC FROM main.mccain_cold_chain_sju.customer_complaints c;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT alleged_cause, complaint_severity, count(*) AS n
# MAGIC FROM main.mccain_cold_chain_sju.silver_complaints_scored
# MAGIC GROUP BY alleged_cause, complaint_severity
# MAGIC ORDER BY n DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Close the loop — complaint ↔ excursion

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE main.mccain_cold_chain_sju.gold_complaint_root_cause AS
# MAGIC SELECT
# MAGIC     c.complaint_id,
# MAGIC     c.retailer,
# MAGIC     c.received_ts,
# MAGIC     c.alleged_cause,
# MAGIC     c.complaint_severity,
# MAGIC     c.shipment_id_mentioned AS shipment_id,
# MAGIC     sp.trailer_id,
# MAGIC     sp.carrier_id,
# MAGIC     sp.dest_dc_id,
# MAGIC     sp.sku,
# MAGIC     sp.had_excursion,
# MAGIC     sp.max_supply_c,
# MAGIC     sp.minutes_above_tolerance,
# MAGIC     sp.top_alarm,
# MAGIC     CASE
# MAGIC         WHEN sp.top_alarm = 'DOOR_OPEN'    THEN 'Door stuck open mid-transit'
# MAGIC         WHEN sp.top_alarm = 'COMP_FAULT'   THEN 'Compressor / reefer unit fault'
# MAGIC         WHEN sp.top_alarm = 'HIGH_AMBIENT' THEN 'Ambient heat spike during stop'
# MAGIC         WHEN sp.had_excursion             THEN 'Unclassified excursion — investigate'
# MAGIC         ELSE                                   'No sensor excursion recorded — quality audit plant-side'
# MAGIC     END AS sensor_root_cause
# MAGIC FROM main.mccain_cold_chain_sju.silver_complaints_scored c
# MAGIC LEFT JOIN main.mccain_cold_chain_sju.silver_shipment_profile sp
# MAGIC   ON sp.shipment_id = c.shipment_id_mentioned;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The killer slide: *here's every complaint this week, tied to the sensor story*
# MAGIC SELECT retailer, complaint_id, alleged_cause, had_excursion, top_alarm, sensor_root_cause
# MAGIC FROM main.mccain_cold_chain_sju.gold_complaint_root_cause
# MAGIC ORDER BY received_ts DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Correlate: do complaints match what sensors saw?
# MAGIC SELECT alleged_cause,
# MAGIC        sum(CASE WHEN had_excursion THEN 1 ELSE 0 END) AS excursion_confirmed,
# MAGIC        sum(CASE WHEN NOT had_excursion THEN 1 ELSE 0 END) AS no_sensor_signal,
# MAGIC        count(*) AS total
# MAGIC FROM main.mccain_cold_chain_sju.gold_complaint_root_cause
# MAGIC WHERE alleged_cause IS NOT NULL
# MAGIC GROUP BY alleged_cause;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Summarise the pattern with `ai_query` (LLM on structured data)
# MAGIC
# MAGIC The moment where a platform exec asks "so what's going on this week?" and we answer in one cell.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH weekly AS (
# MAGIC   SELECT sensor_root_cause, alleged_cause, count(*) AS n
# MAGIC   FROM main.mccain_cold_chain_sju.gold_complaint_root_cause
# MAGIC   GROUP BY sensor_root_cause, alleged_cause
# MAGIC )
# MAGIC SELECT ai_query(
# MAGIC     'databricks-meta-llama-3-3-70b-instruct',
# MAGIC     concat(
# MAGIC       'You are a cold-chain operations analyst. Given this weekly rollup of customer complaints and matched sensor root causes, write a 3-bullet exec summary (not more than 60 words total). Lead with the biggest pattern.\n\nDATA:\n',
# MAGIC       to_json(collect_list(named_struct(
# MAGIC         'sensor_root_cause', sensor_root_cause,
# MAGIC         'alleged_cause', alleged_cause,
# MAGIC         'complaints', n)))
# MAGIC     )
# MAGIC ) AS exec_summary
# MAGIC FROM weekly;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Talk track
# MAGIC > "Two weeks ago someone had to open a ticket, find the shipment in SAP, ask the carrier for reefer logs, and cross-check against the product master. Today it's one `JOIN` — and we're calling a SQL AI function to write the exec summary.
# MAGIC > The governance is the same UC ACLs. The data never moved. That's the demo."
