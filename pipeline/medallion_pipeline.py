# McCain Cold Chain — Lakeflow Spark Declarative Pipeline
# Bronze → Silver → Gold medallion with data quality expectations

import dlt
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, current_timestamp, lit, round as spark_round,
    min as spark_min, max as spark_max, avg as spark_avg, sum as spark_sum,
    when, concat, lpad, coalesce, unix_timestamp, count, expr
)
from pyspark.sql.window import Window

UC_CATALOG = "serverless_stable_qr9if1_catalog"
UC_SCHEMA = "mccain_cold_chain_sju"

# ---------------------------------------------------------------------------
# BRONZE LAYER — Raw Ingestion
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# SILVER LAYER — Cleaned + Enriched with Data Quality Expectations
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# GOLD LAYER — Actionable Business Tables
# ---------------------------------------------------------------------------

@dlt.table(
    name="gold_excursion_events",
    comment="Discrete excursion events per shipment with severity + dollars at risk. Reviewed daily by VP Supply Chain Quality.",
    table_properties={"quality": "gold"}
)
def gold_excursion_events():
    silver = dlt.read("silver_reefer_telemetry")

    # Mark contiguous above-tolerance windows
    w = Window.partitionBy("shipment_id").orderBy("ts").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    flagged = silver.withColumn(
        "grp",
        F.sum(when(~col("is_above_tolerance"), 1).otherwise(0)).over(w)
    ).filter(col("is_above_tolerance"))

    windows = (
        flagged.groupBy(
            "shipment_id", "trailer_id", "carrier_id", "dest_dc_id",
            "sku", "product_family", "pallets", "unit_value_usd", "grp"
        ).agg(
            spark_min("ts").alias("start_ts"),
            spark_max("ts").alias("end_ts"),
            spark_round(spark_max("supply_air_c"), 2).alias("peak_supply_c"),
            spark_round(spark_max("supply_air_c") - spark_avg("upper_tolerance_c"), 2).alias("peak_over_tolerance_c"),
            count("*").alias("duration_min"),
        )
        .filter(col("duration_min") >= 5)
    )

    return (
        windows
        .withColumn("excursion_id", concat(col("shipment_id"), lit("-EX"), lpad(col("grp").cast("string"), 3, "0")))
        .withColumn("severity",
            when((col("duration_min") >= 60) & (col("peak_over_tolerance_c") >= 3), "HIGH")
            .when((col("duration_min") >= 20) | (col("peak_over_tolerance_c") >= 2), "MED")
            .otherwise("LOW")
        )
        .withColumn("dollars_at_risk",
            spark_round(
                col("pallets") * col("unit_value_usd") * 40 *
                when((col("duration_min") >= 60) & (col("peak_over_tolerance_c") >= 3), 1.0)
                .when((col("duration_min") >= 20) | (col("peak_over_tolerance_c") >= 2), 0.35)
                .otherwise(0.05),
                0
            )
        )
        .select(
            "excursion_id", "shipment_id", "trailer_id", "carrier_id", "dest_dc_id",
            "sku", "product_family", "pallets",
            "start_ts", "end_ts", "duration_min",
            "peak_supply_c", "peak_over_tolerance_c",
            "severity", "dollars_at_risk"
        )
    )
