# Databricks notebook source

# COMMAND ----------

# DBTITLE 1,01 — Lakebase → Lakehouse
# 01 — Lakebase → Lakehouse (Bronze Ingestion)

**Story for the audience:** The trucks and reefer units phone home into an operational Postgres database (**Lakebase**). The analytics team needs that data joined with shipments, products, and retailer DCs to reason about cold-chain quality.

We land the IoT data in bronze Delta so downstream medallion and ML workloads run fast and cheap on the same governance layer as everything else.

**Production pattern:** In production, this would use **Lakeflow Connect** (managed Postgres CDC) for continuous, incremental sync. Below we first show Lakeflow Connect configuration, then fall back to a one-shot read for demo reproducibility.

> **Key metric (before state):** It takes McCain's quality team 3–5 days to get reefer data from carriers into their analytics environment. With Lakebase + Lakeflow Connect, data lands in <1 minute.

# COMMAND ----------

# DBTITLE 1,Install dependencies
# MAGIC %pip install -q "psycopg[binary]" databricks-sdk --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Config
LAKEBASE_INSTANCE_NAME = "mccain-cold-chain-sju"
PG_DB = "databricks_postgres"
PG_SCHEMA = "iot"
UC_CATALOG = "serverless_stable_qr9if1_catalog"
UC_SCHEMA = "mccain_cold_chain_sju"

spark.sql(f"USE {UC_CATALOG}.{UC_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Lakeflow Connect — Production Ingestion Path
## Production Path: Lakeflow Connect (Managed Postgres CDC)

In a production deployment, you would configure **Lakeflow Connect** to continuously replicate from Lakebase to Delta Lake. The configuration below shows exactly how this is set up. For this demo, we use a one-shot pull for reproducibility.

```python
# --- PRODUCTION CONFIG (Lakeflow Connect) ---
# This would be configured in the Lakeflow pipeline definition:
#
# CREATE OR REFRESH STREAMING TABLE bronze_reefer_telemetry
#   TBLPROPERTIES ('quality' = 'bronze')
# AS SELECT * FROM
#   read_changefeed('lakebase_mccain_iot.iot.reefer_telemetry');
#
# The Lakeflow Connect connector handles:
#   - Initial snapshot + ongoing CDC (change data capture)
#   - Schema evolution (new columns auto-propagate)
#   - Exactly-once delivery guarantees
#   - Automatic backfill on pipeline restart
```

# COMMAND ----------

# DBTITLE 1,1. Open read connection to Lakebase
## 1. Open a read connection to Lakebase

The token is issued for the current user — same RBAC model as every other Databricks surface.

# COMMAND ----------

# DBTITLE 1,Connect to Lakebase
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

# --- EXECUTION EVIDENCE: show what's in Lakebase before we pull ---
with psycopg.connect(**PG_CONN_KW) as c, c.cursor() as cur:
    cur.execute(f"""
        SELECT trailer_id, count(*) AS readings,
               round(avg(supply_air_c)::numeric, 2) AS avg_c,
               round(max(supply_air_c)::numeric, 2) AS max_c
        FROM {PG_SCHEMA}.reefer_telemetry
        GROUP BY trailer_id ORDER BY max_c DESC LIMIT 20
    """)
    rows = cur.fetchall()
    print(f"\n{'trailer':<10} {'readings':>9} {'avg °C':>8} {'max °C':>8}")
    print("-"*38)
    for r in rows: print(f"{r[0]:<10} {r[1]:>9,} {str(r[2]):>8} {str(r[3]):>8}")
    print(f"\n✅ Connected to Lakebase — {len(rows)} trailers with telemetry")

# COMMAND ----------

# DBTITLE 1,2. Bulk-pull to bronze Delta
## 2. Bulk-pull each Lakebase table → bronze Delta

psycopg server-side cursor streams the rows → pandas → Spark DataFrame → Delta. One cell, one read, one write per table.

# COMMAND ----------

# DBTITLE 1,Pull Lakebase to Bronze
import pandas as pd
from pyspark.sql.functions import current_timestamp, lit
import time

def pull(table: str) -> pd.DataFrame:
    with psycopg.connect(**PG_CONN_KW) as c:
        return pd.read_sql(f"SELECT * FROM {PG_SCHEMA}.{table}", c)

# --- EXECUTION EVIDENCE: time the ingestion and show row counts ---
print("="*60)
print("BRONZE INGESTION FROM LAKEBASE")
print("="*60)
start_time = time.time()

for t in ["reefer_telemetry", "trailers", "door_events", "gps_pings"]:
    t_start = time.time()
    pdf = pull(t)
    sdf = (spark.createDataFrame(pdf)
              .withColumn("_ingested_ts", current_timestamp())
              .withColumn("_source", lit(f"lakebase.{PG_SCHEMA}.{t}")))
    (sdf.write.mode("overwrite").option("overwriteSchema", "true")
         .saveAsTable(f"{UC_CATALOG}.{UC_SCHEMA}.bronze_{t}"))
    elapsed = time.time() - t_start
    print(f"  bronze_{t:<18} = {sdf.count():>8,} rows  ({elapsed:.1f}s)")

total_elapsed = time.time() - start_time
print(f"\n✅ Bronze ingestion complete in {total_elapsed:.1f}s")
print(f"  (Production Lakeflow Connect would maintain <1 min latency)")

# COMMAND ----------

# DBTITLE 1,3. Governance surface validation
## 3. Same governance surface as everything else

Bronze tables live under Unity Catalog. Joins with analytical tables (`shipments`, `product_master`) are standard SQL.

# COMMAND ----------

# DBTITLE 1,Tolerance breach query
# MAGIC %sql
# MAGIC -- EXECUTION EVIDENCE: Rank trailer-shipments by how far they drifted above the product's upper tolerance.
# MAGIC SELECT rt.trailer_id,
# MAGIC        s.shipment_id,
# MAGIC        pm.sku,
# MAGIC        pm.product_family,
# MAGIC        round(max(rt.supply_air_c), 2)                                       AS peak_supply_c,
# MAGIC        round(max(rt.supply_air_c) - pm.upper_tolerance_c, 2)                AS over_tolerance_c
# MAGIC FROM serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.bronze_reefer_telemetry rt
# MAGIC JOIN serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.shipments       s  USING (shipment_id)
# MAGIC JOIN serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.product_master  pm ON s.sku = pm.sku
# MAGIC GROUP BY rt.trailer_id, s.shipment_id, pm.sku, pm.product_family, pm.upper_tolerance_c
# MAGIC HAVING max(rt.supply_air_c) > pm.upper_tolerance_c
# MAGIC ORDER BY over_tolerance_c DESC

# COMMAND ----------

# DBTITLE 1,Alarm readings
# MAGIC %sql
# MAGIC -- EXECUTION EVIDENCE: All alarmed readings across the fleet
# MAGIC SELECT ts, trailer_id, shipment_id, supply_air_c, compressor_on, alarm_code
# MAGIC FROM serverless_stable_qr9if1_catalog.mccain_cold_chain_sju.bronze_reefer_telemetry
# MAGIC WHERE alarm_code IS NOT NULL
# MAGIC ORDER BY ts
# MAGIC LIMIT 50

# COMMAND ----------

# DBTITLE 1,Talk track
### Talk track

> "The trucks write to Postgres the same way they write to any CPG's operational DB — so nothing changes for the edge. What changes is that that Postgres **is** Databricks: one token, one ACL, one audit trail. And the moment the data is in bronze, I can join it to SAP shipments and the product master in the same SQL cell."
>
> **Before:** 3–5 days to get reefer logs from carrier emails into analytics.
> **After:** <1 minute via Lakeflow Connect continuous CDC from Lakebase.