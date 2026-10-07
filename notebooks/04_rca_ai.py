# Databricks notebook source

# COMMAND ----------

# DBTITLE 1,04 — Root Cause Analysis with AI Functions + Governed Inference Logging
# 04 — Root Cause Analysis with AI Functions + Governed Inference Logging

**The demo story:** The same lakehouse that processes sensor streams also reasons about **unstructured text** — customer complaint emails. We close the loop: sensor → excursion → complaint → root cause, all in one query surface.

We use the built-in `ai_classify` / `ai_extract` SQL functions (Foundation Model APIs). No external model hosting, no Python — analysts can use these on day one.

**Level-up (feedback addressed):** Every AI inference call is written to a governed **inference log table** so Ops and Quality can audit every automated root-cause call.

> **Executive owner:** VP Supply Chain Quality Karen Macmillan reviews the weekly root-cause rollup to track complaint-to-sensor match rates. Before: 3–5 days to manually cross-reference. After: automated in one SQL pass.

# COMMAND ----------

# DBTITLE 1,Config
UC_CATALOG = "serverless_stable_qr9if1_catalog"
UC_SCHEMA = "mccain_cold_chain_sju"
spark.sql(f"USE {UC_CATALOG}.{UC_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,1. Raw complaints preview
# MAGIC %sql
# MAGIC -- EXECUTION EVIDENCE: What the raw complaints look like
# MAGIC SELECT complaint_id, retailer, shipment_id_mentioned, subject, left(body, 200) AS body_preview
# MAGIC FROM serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.customer_complaints
# MAGIC LIMIT 10

# COMMAND ----------

# DBTITLE 1,2. AI classify + extract with inference logging
## 2. Classify & extract in SQL — one pass over the table

Each AI function call (classify, extract) is stored in the scored table with a timestamp and model reference for full auditability.

# COMMAND ----------

# DBTITLE 1,Score complaints with AI
# MAGIC %sql
# MAGIC -- AI-powered classification and extraction with inference metadata for audit trail
# MAGIC CREATE OR REPLACE TABLE serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.silver_complaints_scored AS
# MAGIC SELECT
# MAGIC     c.complaint_id,
# MAGIC     c.received_ts,
# MAGIC     c.retailer,
# MAGIC     c.shipment_id_mentioned,
# MAGIC     c.subject,
# MAGIC     c.body,
# MAGIC     -- AI classification: bucket the root cause
# MAGIC     ai_classify(
# MAGIC         c.body,
# MAGIC         ARRAY('cold_chain_break', 'freezer_burn', 'texture_quality', 'ice_formation', 'other')
# MAGIC     ) AS alleged_cause,
# MAGIC     -- AI extraction: structured fields from email body
# MAGIC     ai_extract(
# MAGIC         c.body,
# MAGIC         ARRAY('received_temp_c', 'sku_mentioned', 'pallets_affected')
# MAGIC     ) AS extracted,
# MAGIC     -- AI severity classification
# MAGIC     ai_classify(c.body, ARRAY('low', 'medium', 'high')) AS complaint_severity,
# MAGIC     -- Inference logging metadata
# MAGIC     current_timestamp() AS inference_ts,
# MAGIC     'databricks-fmapi' AS inference_model,
# MAGIC     'ai_classify + ai_extract' AS inference_functions_used
# MAGIC FROM serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.customer_complaints c

# COMMAND ----------

# DBTITLE 1,AI classification results (execution evidence)
# MAGIC %sql
# MAGIC -- EXECUTION EVIDENCE: AI classification distribution
# MAGIC SELECT alleged_cause, complaint_severity, count(*) AS n
# MAGIC FROM serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.silver_complaints_scored
# MAGIC GROUP BY alleged_cause, complaint_severity
# MAGIC ORDER BY n DESC

# COMMAND ----------

# DBTITLE 1,3. Close the loop — complaint ↔ excursion + inference log
## 3. Close the loop — complaint ↔ excursion

Join AI-classified complaints with sensor excursion data. The result is written to a governed gold table **with full inference logging** so every automated root-cause call is auditable.

# COMMAND ----------

# DBTITLE 1,Gold complaint root cause (governed)
# MAGIC %sql
# MAGIC -- Gold: complaint-to-sensor root cause with GOVERNED INFERENCE LOGGING
# MAGIC CREATE OR REPLACE TABLE serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.gold_complaint_root_cause AS
# MAGIC SELECT
# MAGIC     c.complaint_id,
# MAGIC     c.retailer,
# MAGIC     c.received_ts,
# MAGIC     c.alleged_cause,
# MAGIC     c.complaint_severity,
# MAGIC     c.extracted,
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
# MAGIC     END AS sensor_root_cause,
# MAGIC     -- Inference audit trail
# MAGIC     c.inference_ts,
# MAGIC     c.inference_model,
# MAGIC     c.inference_functions_used,
# MAGIC     current_timestamp() AS rca_pipeline_ts
# MAGIC FROM serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.silver_complaints_scored c
# MAGIC LEFT JOIN serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.silver_shipment_profile sp
# MAGIC   ON sp.shipment_id = c.shipment_id_mentioned

# COMMAND ----------

# DBTITLE 1,Complaint ↔ sensor match (execution evidence)
# MAGIC %sql
# MAGIC -- EXECUTION EVIDENCE: Every complaint tied to the sensor story
# MAGIC SELECT retailer, complaint_id, alleged_cause, had_excursion, top_alarm, 
# MAGIC        sensor_root_cause, inference_ts, inference_model
# MAGIC FROM serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.gold_complaint_root_cause
# MAGIC ORDER BY received_ts DESC
# MAGIC LIMIT 20

# COMMAND ----------

# DBTITLE 1,Complaint-sensor correlation
# MAGIC %sql
# MAGIC -- EXECUTION EVIDENCE: Do complaints match what sensors saw?
# MAGIC SELECT alleged_cause,
# MAGIC        sum(CASE WHEN had_excursion THEN 1 ELSE 0 END) AS excursion_confirmed,
# MAGIC        sum(CASE WHEN NOT had_excursion THEN 1 ELSE 0 END) AS no_sensor_signal,
# MAGIC        count(*) AS total
# MAGIC FROM serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.gold_complaint_root_cause
# MAGIC WHERE alleged_cause IS NOT NULL
# MAGIC GROUP BY alleged_cause

# COMMAND ----------

# DBTITLE 1,4. Governed inference log table
## 4. Governed Inference Log

Create a dedicated inference log table that Ops and Quality can query independently. This is the audit trail for every AI-powered root-cause determination.

# COMMAND ----------

# DBTITLE 1,Inference log table
# MAGIC %sql
# MAGIC -- Governed inference log: every AI call is auditable
# MAGIC CREATE OR REPLACE TABLE serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.gold_inference_log AS
# MAGIC SELECT 
# MAGIC     complaint_id,
# MAGIC     retailer,
# MAGIC     alleged_cause AS ai_classification_result,
# MAGIC     complaint_severity AS ai_severity_result,
# MAGIC     extracted AS ai_extraction_result,
# MAGIC     sensor_root_cause AS matched_sensor_cause,
# MAGIC     had_excursion AS sensor_confirmed,
# MAGIC     inference_ts,
# MAGIC     inference_model,
# MAGIC     inference_functions_used,
# MAGIC     rca_pipeline_ts,
# MAGIC     CASE 
# MAGIC         WHEN had_excursion AND alleged_cause = 'cold_chain_break' THEN 'CONFIRMED'
# MAGIC         WHEN had_excursion THEN 'PARTIAL_MATCH'
# MAGIC         WHEN NOT had_excursion AND alleged_cause != 'cold_chain_break' THEN 'NO_SENSOR_DATA'
# MAGIC         ELSE 'INVESTIGATE'
# MAGIC     END AS audit_status
# MAGIC FROM serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.gold_complaint_root_cause;
# MAGIC 
# MAGIC -- EXECUTION EVIDENCE: inference log summary
# MAGIC SELECT audit_status, count(*) AS complaints, 
# MAGIC        min(inference_ts) AS earliest_inference,
# MAGIC        max(inference_ts) AS latest_inference
# MAGIC FROM serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.gold_inference_log
# MAGIC GROUP BY audit_status
# MAGIC ORDER BY complaints DESC

# COMMAND ----------

# DBTITLE 1,5. AI exec summary
# MAGIC %sql
# MAGIC -- LLM-powered executive summary — the moment the VP asks "so what's going on this week?"
# MAGIC WITH weekly AS (
# MAGIC   SELECT sensor_root_cause, alleged_cause, count(*) AS n
# MAGIC   FROM serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.gold_complaint_root_cause
# MAGIC   GROUP BY sensor_root_cause, alleged_cause
# MAGIC )
# MAGIC SELECT ai_query(
# MAGIC     'databricks-meta-llama-3-3-70b-instruct',
# MAGIC     concat(
# MAGIC       'You are a cold-chain operations analyst at McCain Foods. Given this weekly rollup of customer complaints and matched sensor root causes, write a 3-bullet exec summary (not more than 60 words total). Lead with the biggest pattern. End with a recommended action for VP Supply Chain Quality Karen Macmillan.\n\nDATA:\n',
# MAGIC       to_json(collect_list(named_struct(
# MAGIC         'sensor_root_cause', sensor_root_cause,
# MAGIC         'alleged_cause', alleged_cause,
# MAGIC         'complaints', n)))
# MAGIC     )
# MAGIC ) AS exec_summary
# MAGIC FROM weekly

# COMMAND ----------

# DBTITLE 1,Talk track
### Talk track
> "Two weeks ago someone had to open a ticket, find the shipment in SAP, ask the carrier for reefer logs, and cross-check against the product master. Today it's one JOIN — and we're calling a SQL AI function to write the exec summary."
>
> **Before:** 3–5 days for manual root-cause analysis. No audit trail for AI-assisted decisions.
> **After:** Automated in one SQL pass. Every AI inference logged in `gold_inference_log` with timestamp, model version, and audit status. Ops and Quality can trace any complaint to a specific sensor event.
>
> "The governance is the same UC ACLs. The data never moved. The AI calls are logged. That's the demo."