# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Lakebase → Lakehouse
# MAGIC
# MAGIC **Story for the audience:** the trucks and reefer units phone home into an operational Postgres database (**Lakebase**). The analytics team needs that data joined with shipments, products, and retailer DCs to reason about cold-chain quality.
# MAGIC
# MAGIC We land the IoT data in bronze Delta so downstream medallion and ML workloads run fast and cheap on the same governance layer as everything else.
# MAGIC
# MAGIC > **Production pattern:** use Lakeflow Connect (managed Postgres CDC) for continuous, incremental sync. We use a one-shot read here so the demo is reproducible.

# COMMAND ----------

# MAGIC %pip install -q "psycopg[binary]" databricks-sdk --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

LAKEBASE_INSTANCE_NAME = "mccain-cold-chain-sju"
PG_DB = "databricks_postgres"
PG_SCHEMA = "iot"
UC_CATALOG = "main"
UC_SCHEMA = "mccain_cold_chain_sju"

spark.sql(f"USE {UC_CATALOG}.{UC_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Open a read connection to Lakebase
# MAGIC
# MAGIC The token is issued for the current user — same RBAC model as every other Databricks surface.

# COMMAND ----------

import uuid
import psycopg
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
me = w.current_user.me().user_name
inst = w.database.get_database_instance(name=LAKEBASE_INSTANCE_NAME)
cred = w.database.generate_database_credential(
    request_id=str(uuid.uuid4()),
    instance_names=[LAKEBASE_INSTANCE_NAME],
)

PG_CONN_KW = dict(
    host=inst.read_write_dns, dbname=PG_DB,
    user=me, password=cred.token, sslmode="require",
)

with psycopg.connect(**PG_CONN_KW) as c, c.cursor() as cur:
    cur.execute(f"""
        SELECT trailer_id, count(*) AS readings,
               round(avg(supply_air_c)::numeric, 2) AS avg_c,
               round(max(supply_air_c)::numeric, 2) AS max_c
        FROM {PG_SCHEMA}.reefer_telemetry
        GROUP BY trailer_id ORDER BY max_c DESC LIMIT 20
    """)
    rows = cur.fetchall()
    print(f"{'trailer':<10} {'readings':>9} {'avg °C':>8} {'max °C':>8}")
    for r in rows: print(f"{r[0]:<10} {r[1]:>9,} {str(r[2]):>8} {str(r[3]):>8}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Bulk-pull each Lakebase table → bronze Delta
# MAGIC
# MAGIC psycopg server-side cursor streams the rows → pandas → Spark DataFrame → Delta. One cell, one read, one write per table.

# COMMAND ----------

import pandas as pd

def pull(table: str) -> pd.DataFrame:
    with psycopg.connect(**PG_CONN_KW) as c:
        return pd.read_sql(f"SELECT * FROM {PG_SCHEMA}.{table}", c)

from pyspark.sql.functions import current_timestamp, lit

for t in ["reefer_telemetry", "trailers", "door_events", "gps_pings"]:
    pdf = pull(t)
    sdf = (spark.createDataFrame(pdf)
              .withColumn("_ingested_ts", current_timestamp())
              .withColumn("_source", lit(f"lakebase.{PG_SCHEMA}.{t}")))
    (sdf.write.mode("overwrite").option("overwriteSchema", "true")
         .saveAsTable(f"{UC_CATALOG}.{UC_SCHEMA}.bronze_{t}"))
    print(f"  bronze_{t:<18} = {sdf.count():>8,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Same governance surface as everything else
# MAGIC
# MAGIC Bronze tables live under Unity Catalog. Joins with analytical tables (`shipments`, `product_master`) are standard SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Rank trailer-shipments by how far they drifted above the product's upper tolerance.
# MAGIC SELECT rt.trailer_id,
# MAGIC        s.shipment_id,
# MAGIC        pm.sku,
# MAGIC        pm.product_family,
# MAGIC        round(max(rt.supply_air_c)::numeric, 2)                 AS peak_supply_c,
# MAGIC        round((max(rt.supply_air_c) - pm.upper_tolerance_c)::numeric, 2) AS over_tolerance_c
# MAGIC FROM main.mccain_cold_chain_sju.bronze_reefer_telemetry rt
# MAGIC JOIN main.mccain_cold_chain_sju.shipments       s  USING (shipment_id)
# MAGIC JOIN main.mccain_cold_chain_sju.product_master  pm ON s.sku = pm.sku
# MAGIC GROUP BY rt.trailer_id, s.shipment_id, pm.sku, pm.product_family, pm.upper_tolerance_c
# MAGIC HAVING max(rt.supply_air_c) > pm.upper_tolerance_c
# MAGIC ORDER BY over_tolerance_c DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- All alarmed readings across the fleet — useful to show live during the talk.
# MAGIC SELECT ts, trailer_id, shipment_id, supply_air_c, compressor_on, alarm_code
# MAGIC FROM main.mccain_cold_chain_sju.bronze_reefer_telemetry
# MAGIC WHERE alarm_code IS NOT NULL
# MAGIC ORDER BY ts
# MAGIC LIMIT 50;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Talk track
# MAGIC
# MAGIC > "The trucks write to Postgres the same way they write to any CPG's operational DB — so nothing changes for the edge. What changes is that that Postgres **is** Databricks: one token, one ACL, one audit trail. And the moment the data is in bronze, I can join it to SAP shipments and the product master in the same SQL cell."
