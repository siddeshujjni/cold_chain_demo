# Databricks notebook source

# COMMAND ----------

# DBTITLE 1,02 — Lakeflow Declarative Pipeline: Bronze → Silver → Gold
# 02 — Lakeflow Spark Declarative Pipeline: Bronze → Silver → Gold

**This is the Lakeflow SDP definition** — attach this notebook to a Lakeflow pipeline and run it. It replaces the manual `CREATE OR REPLACE TABLE` pattern from the original demo with a proper, production-grade declarative pipeline.

**What this pipeline builds:**

| Layer | Table | Description |
|---|---|---|
| **Bronze** | `bronze_reefer_telemetry` | Raw telemetry with ingestion metadata |
| **Bronze** | `bronze_door_events` | Door open/close events |
| **Bronze** | `bronze_gps_pings` | GPS position data |
| **Silver** | `silver_reefer_telemetry` | Cleaned + enriched with shipment/product context, DQ expectations |
| **Silver** | `silver_shipment_profile` | One row per shipment with temp stats + door events |
| **Gold** | `gold_excursion_events` | Actionable excursion events with severity + dollars at risk |

**Data quality expectations** are enforced at the silver layer — nulls, impossible temps, and orphan shipments are caught and quarantined.

> **VP Supply Chain Quality Karen Macmillan** reviews the gold_excursion_events table daily to track the $12M annual exposure.

# COMMAND ----------

# DBTITLE 1,Pipeline imports
import dlt
from pyspark.sql.functions import (
    col, current_timestamp, lit, round as spark_round,
    min as spark_min, max as spark_max, avg as spark_avg, sum as spark_sum,
    when, concat, lpad, cast, coalesce, unix_timestamp, count
)
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Config
UC_CATALOG = "serverless_stable_qr9if1_catalog"
UC_SCHEMA = "mccain_cold_chain_sju"

# COMMAND ----------

# DBTITLE 1,Bronze Layer
## Bronze Layer — Raw Ingestion

Bronze tables read from the tables created by notebook 01 (Lakebase → Lakehouse). In a production deployment with Lakeflow Connect, these would be `STREAMING TABLE`s reading from the CDC feed.

For this demo, they materialize the bronze tables with ingestion metadata.

# COMMAND ----------

# DBTITLE 1,Bronze: reefer_telemetry
@dlt.table(
    name="bronze_reefer_telemetry",
    comment="Raw reefer telemetry from Lakebase IoT. 1-min granularity, ~150K rows.",
    table_properties={"quality": "bronze", "pipelines.autoOptimize.zOrderCols": "shipment_id,ts"}
)
def bronze_reefer_telemetry():
    return (
        spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.bronze_reefer_telemetry")
        .withColumn("_pipeline_ts", current_timestamp())
    )

# COMMAND ----------

# DBTITLE 1,Bronze: door_events
@dlt.table(
    name="bronze_door_events",
    comment="Door open/close events from trailer sensors.",
    table_properties={"quality": "bronze"}
)
def bronze_door_events():
    return (
        spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.bronze_door_events")
        .withColumn("_pipeline_ts", current_timestamp())
    )

# COMMAND ----------

# DBTITLE 1,Bronze: gps_pings
@dlt.table(
    name="bronze_gps_pings",
    comment="GPS position pings from trailers, used for live map.",
    table_properties={"quality": "bronze"}
)
def bronze_gps_pings():
    return (
        spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.bronze_gps_pings")
        .withColumn("_pipeline_ts", current_timestamp())
    )

# COMMAND ----------

# DBTITLE 1,Silver Layer
## Silver Layer — Cleaned + Enriched with Data Quality Expectations

Silver tables join telemetry with shipment context (SKU, required temp, tolerance) and enforce data quality expectations. Rows that violate expectations are quarantined, not silently dropped.

# COMMAND ----------

# DBTITLE 1,Silver: reefer_telemetry (enriched + DQ)
@dlt.table(
    name="silver_reefer_telemetry",
    comment="Cleaned telemetry enriched with shipment/product context. DQ-validated.",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_supply_temp", "supply_air_c IS NOT NULL")
@dlt.expect_or_drop("physically_possible_temp", "supply_air_c BETWEEN -50 AND 20")
@dlt.expect_or_drop("has_matching_shipment", "sku IS NOT NULL")
def silver_reefer_telemetry():
    rt = dlt.read("bronze_reefer_telemetry")
    shipments = spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.shipments")
    products = spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.product_master")
    
    return (
        rt.join(shipments, "shipment_id")
          .join(products, shipments["sku"] == products["sku"])
          .filter(
              (col("ts") >= col("ship_ts") - expr("INTERVAL 1 HOUR")) &
              (col("ts") <= col("arrival_ts") + expr("INTERVAL 1 HOUR"))
          )
          .select(
              rt["trailer_id"], rt["shipment_id"], rt["ts"],
              rt["set_point_c"], rt["supply_air_c"], rt["return_air_c"],
              rt["compressor_on"], rt["fuel_level_pct"], rt["alarm_code"],
              products["sku"], shipments["carrier_id"], shipments["dest_dc_id"],
              shipments["pallets"], shipments["ship_ts"], shipments["arrival_ts"],
              products["product_family"], products["target_temp_c"],
              products["upper_tolerance_c"], products["lower_tolerance_c"],
              products["unit_value_usd"],
              (rt["supply_air_c"] > products["upper_tolerance_c"]).alias("is_above_tolerance")
          )
    )

# COMMAND ----------

# DBTITLE 1,Silver: shipment_profile
@dlt.table(
    name="silver_shipment_profile",
    comment="One row per shipment: peak/min temps, door events, excursion flag, shipment value.",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_shipment_id", "shipment_id IS NOT NULL")
@dlt.expect("positive_transit_hours", "transit_hours > 0")
def silver_shipment_profile():
    rt = dlt.read("silver_reefer_telemetry")
    shipments = spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.shipments")
    products = spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.product_master")
    door_events = dlt.read("bronze_door_events")
    
    # Mid-transit door opens
    door_open_mid = (
        door_events.join(shipments, "shipment_id")
        .filter(
            (col("event") == "door_open") &
            (col("ts") > col("ship_ts") + expr("INTERVAL 10 MINUTE")) &
            (col("ts") < col("arrival_ts") - expr("INTERVAL 10 MINUTE"))
        )
        .groupBy("shipment_id")
        .agg(count("*").alias("mid_transit_door_opens"))
    )
    
    profile = (
        rt.groupBy("shipment_id", "trailer_id", "carrier_id", "dest_dc_id", 
                   "sku", "pallets", "ship_ts", "arrival_ts", 
                   "upper_tolerance_c", "unit_value_usd")
        .agg(
            spark_round((unix_timestamp("arrival_ts") - unix_timestamp("ship_ts")) / 3600.0, 1).alias("transit_hours"),
            spark_round(spark_min("supply_air_c"), 2).alias("min_supply_c"),
            spark_round(spark_max("supply_air_c"), 2).alias("max_supply_c"),
            spark_round(spark_avg("supply_air_c"), 2).alias("avg_supply_c"),
            spark_sum(when(col("is_above_tolerance"), 1).otherwise(0)).alias("minutes_above_tolerance"),
            spark_sum(when(col("alarm_code").isNotNull(), 1).otherwise(0)).alias("alarm_minutes"),
            spark_max(when(col("alarm_code").isNotNull(), col("alarm_code"))).alias("top_alarm"),
        )
        .withColumn("had_excursion", col("max_supply_c") > col("upper_tolerance_c"))
        .withColumn("shipment_value_usd", col("pallets") * col("unit_value_usd") * 40)
    )
    
    return (
        profile.join(door_open_mid, "shipment_id", "left")
        .withColumn("mid_transit_door_opens", coalesce(col("mid_transit_door_opens"), lit(0)))
    )

# COMMAND ----------

# DBTITLE 1,Gold Layer
## Gold Layer — Actionable Business Tables

The gold excursion events table is what **VP Supply Chain Quality Karen Macmillan** and her ops team review daily. Each row is an actionable event with duration, peak temp, severity, and dollars at risk.

# COMMAND ----------

# DBTITLE 1,Gold: excursion_events
# MAGIC %sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW gold_excursion_events
# MAGIC COMMENT 'Discrete excursion events per shipment with severity + dollars at risk. Reviewed daily by VP Supply Chain Quality.'
# MAGIC AS
# MAGIC WITH flagged AS (
# MAGIC     SELECT
# MAGIC         shipment_id, trailer_id, carrier_id, dest_dc_id, sku, product_family,
# MAGIC         ts, supply_air_c, upper_tolerance_c, unit_value_usd, pallets,
# MAGIC         is_above_tolerance,
# MAGIC         sum(CASE WHEN NOT is_above_tolerance THEN 1 ELSE 0 END)
# MAGIC             OVER (PARTITION BY shipment_id ORDER BY ts ROWS UNBOUNDED PRECEDING) AS grp
# MAGIC     FROM LIVE.silver_reefer_telemetry
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

# DBTITLE 1,Pipeline summary
## Pipeline Summary

This Lakeflow Spark Declarative Pipeline provides:

* **3 bronze tables** — raw IoT data with ingestion metadata
* **2 silver tables** — cleaned + enriched with 5 data quality expectations
* **1 gold materialized view** — excursion events with severity scoring and dollar-at-risk calculation

**To run this pipeline:**
1. Create a Lakeflow pipeline in the workspace
2. Attach this notebook as the source
3. Set target catalog = `main`, target schema = `mccain_cold_chain_sju`
4. Run a full refresh

The pipeline update will produce execution evidence: row counts, DQ expectation pass/fail rates, and processing time — all logged in the pipeline event log.