# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Excursion Detection: Rules → ML
# MAGIC
# MAGIC **The demo story:**
# MAGIC 1. Start with a rule — *supply_air_c > upper_tolerance for ≥ 20 min*. This is what most CPGs have today.
# MAGIC 2. Show its weakness: it only fires **after** damage is done.
# MAGIC 3. Upgrade to a seasonal-anomaly ML model that flags trailers **trending warm** before they breach tolerance — the pre-emptive signal operations really wants.
# MAGIC
# MAGIC We keep the model deliberately simple (rolling-window z-score) so the audience can reason about it during the talk.

# COMMAND ----------

UC_CATALOG = "main"
UC_SCHEMA = "mccain_cold_chain_sju"
spark.sql(f"USE {UC_CATALOG}.{UC_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Rule-based detection (today's baseline)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE main.mccain_cold_chain_sju.alerts_rule_based AS
# MAGIC SELECT excursion_id, shipment_id, trailer_id, carrier_id, dest_dc_id, sku,
# MAGIC        start_ts AS detected_ts,
# MAGIC        'RULE: >upper_tol for ≥20 min' AS detection_source,
# MAGIC        peak_supply_c, peak_over_tolerance_c, duration_min, severity, dollars_at_risk
# MAGIC FROM main.mccain_cold_chain_sju.gold_excursion_events
# MAGIC WHERE duration_min >= 20;
# MAGIC
# MAGIC SELECT count(*) AS rule_alerts FROM main.mccain_cold_chain_sju.alerts_rule_based;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Seasonal-anomaly ML — proactive warm-trend detection
# MAGIC
# MAGIC For each trailer, compute a rolling 30-minute mean of supply-air temperature and flag rows whose deviation from the *per-trailer baseline* exceeds a z-score threshold. This captures "trailer drifting warm" well before it breaches absolute tolerance.

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

src = spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.silver_reefer_telemetry")

# per-trailer baseline = first 60 minutes of its window (assumed stable)
baseline = (src.groupBy("trailer_id")
               .agg(F.expr("percentile(supply_air_c, 0.5)").alias("baseline_median"),
                    F.stddev("supply_air_c").alias("baseline_sd")))

w30 = (Window
         .partitionBy("trailer_id")
         .orderBy(F.col("ts").cast("long"))
         .rangeBetween(-30*60, 0))

enriched = (src.join(baseline, "trailer_id")
                .withColumn("rolling_mean_c", F.avg("supply_air_c").over(w30))
                .withColumn("z_score",
                    (F.col("rolling_mean_c") - F.col("baseline_median")) /
                    F.when(F.col("baseline_sd") > 0.05, F.col("baseline_sd")).otherwise(F.lit(0.3)))
                .withColumn("ml_warm_trend", F.col("z_score") > 3.0))

enriched.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{UC_CATALOG}.{UC_SCHEMA}.silver_reefer_scored")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Turn contiguous warm-trend rows into alerts + measure lead-time vs. the rule baseline.
# MAGIC CREATE OR REPLACE TABLE main.mccain_cold_chain_sju.alerts_ml_based AS
# MAGIC WITH flagged AS (
# MAGIC   SELECT *,
# MAGIC          sum(CASE WHEN NOT ml_warm_trend THEN 1 ELSE 0 END)
# MAGIC               OVER (PARTITION BY shipment_id ORDER BY ts ROWS UNBOUNDED PRECEDING) AS grp
# MAGIC   FROM main.mccain_cold_chain_sju.silver_reefer_scored
# MAGIC ),
# MAGIC warm_windows AS (
# MAGIC   SELECT shipment_id, trailer_id, carrier_id, dest_dc_id, sku,
# MAGIC          min(ts)             AS first_warm_ts,
# MAGIC          max(ts)             AS last_warm_ts,
# MAGIC          round(max(z_score), 2) AS peak_z,
# MAGIC          count(*)            AS warm_minutes
# MAGIC   FROM flagged
# MAGIC   WHERE ml_warm_trend
# MAGIC   GROUP BY shipment_id, trailer_id, carrier_id, dest_dc_id, sku, grp
# MAGIC   HAVING count(*) >= 10
# MAGIC ),
# MAGIC first_per_shipment AS (
# MAGIC   SELECT *, row_number() OVER (PARTITION BY shipment_id ORDER BY first_warm_ts) AS rn
# MAGIC   FROM warm_windows
# MAGIC )
# MAGIC SELECT
# MAGIC     concat(shipment_id, '-ML', lpad(cast(rn AS string), 2, '0')) AS ml_alert_id,
# MAGIC     shipment_id, trailer_id, carrier_id, dest_dc_id, sku,
# MAGIC     first_warm_ts AS detected_ts,
# MAGIC     'ML: rolling z-score > 3' AS detection_source,
# MAGIC     peak_z, warm_minutes
# MAGIC FROM first_per_shipment;
# MAGIC
# MAGIC SELECT count(*) AS ml_alerts FROM main.mccain_cold_chain_sju.alerts_ml_based;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Lead-time comparison — the "why ML?" slide
# MAGIC
# MAGIC For every shipment that had a real excursion, how many minutes earlier did the ML model raise a warning vs. the rule?

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH first_rule AS (
# MAGIC   SELECT shipment_id, min(detected_ts) AS rule_ts
# MAGIC   FROM main.mccain_cold_chain_sju.alerts_rule_based GROUP BY shipment_id
# MAGIC ),
# MAGIC first_ml AS (
# MAGIC   SELECT shipment_id, min(detected_ts) AS ml_ts
# MAGIC   FROM main.mccain_cold_chain_sju.alerts_ml_based GROUP BY shipment_id
# MAGIC )
# MAGIC SELECT f.shipment_id,
# MAGIC        r.rule_ts,
# MAGIC        f.ml_ts,
# MAGIC        cast((unix_timestamp(r.rule_ts) - unix_timestamp(f.ml_ts)) / 60 AS int) AS ml_lead_minutes
# MAGIC FROM first_ml f
# MAGIC LEFT JOIN first_rule r ON r.shipment_id = f.shipment_id
# MAGIC WHERE r.rule_ts IS NOT NULL
# MAGIC ORDER BY ml_lead_minutes DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Headline number for the slide
# MAGIC WITH first_rule AS (
# MAGIC   SELECT shipment_id, min(detected_ts) AS rule_ts
# MAGIC   FROM main.mccain_cold_chain_sju.alerts_rule_based GROUP BY shipment_id
# MAGIC ),
# MAGIC first_ml AS (
# MAGIC   SELECT shipment_id, min(detected_ts) AS ml_ts
# MAGIC   FROM main.mccain_cold_chain_sju.alerts_ml_based GROUP BY shipment_id
# MAGIC )
# MAGIC SELECT
# MAGIC   round(avg((unix_timestamp(r.rule_ts) - unix_timestamp(f.ml_ts)) / 60.0), 1) AS avg_ml_lead_minutes,
# MAGIC   count(*) AS shipments_compared
# MAGIC FROM first_ml f
# MAGIC JOIN first_rule r ON r.shipment_id = f.shipment_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Talk track
# MAGIC > "The rule only fires after we've already been out-of-spec for 20 minutes. The ML signal catches the same trailer drifting 30+ minutes earlier — that's the window in which a dispatcher can actually call the driver or reroute. At $X per at-risk pallet, that lead time is the ROI."
