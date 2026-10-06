# Databricks notebook source
# MAGIC %md
# MAGIC # McCain Cold Chain — 00 Setup
# MAGIC
# MAGIC Provisions the demo environment on this workspace:
# MAGIC 1. **Lakebase (Postgres) instance** for raw IoT telemetry — the OLTP system that trucks phone home to.
# MAGIC 2. **Unity Catalog schema** for analytical tables (shipments, products, complaints).
# MAGIC 3. **Seed data**: ~150K reefer telemetry rows + 120 shipments + 50 SKUs + 28 complaint emails, with 3 pre-seeded excursion scenarios (door stuck open, reefer unit failure, heat spike).
# MAGIC
# MAGIC Run this once, then notebooks 01 → 05 in order.

# COMMAND ----------

# MAGIC %pip install -q "psycopg[binary]" databricks-sdk --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Config — edit if you want different names
LAKEBASE_INSTANCE_NAME = "mccain-cold-chain-sju"
LAKEBASE_CAPACITY = "CU_1"          # smallest tier; ~$0.40/hr
UC_CATALOG = "main"
UC_SCHEMA = "mccain_cold_chain_sju"
PG_DB = "databricks_postgres"
PG_SCHEMA = "iot"
TELEMETRY_STEP_MIN = 1              # 1-minute granularity → ~150K rows

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create (or reuse) the Lakebase instance

# COMMAND ----------

import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import DatabaseInstance

w = WorkspaceClient()
me = w.current_user.me().user_name
print(f"Current user: {me}")

try:
    inst = w.database.get_database_instance(name=LAKEBASE_INSTANCE_NAME)
    print(f"Reusing existing instance: {inst.name} (state={inst.state})")
except Exception:
    print(f"Creating new Lakebase instance '{LAKEBASE_INSTANCE_NAME}' ({LAKEBASE_CAPACITY})…")
    inst = w.database.create_database_instance(
        database_instance=DatabaseInstance(
            name=LAKEBASE_INSTANCE_NAME,
            capacity=LAKEBASE_CAPACITY,
        )
    )

# Wait for AVAILABLE (takes ~3–6 min on first create)
def _state_name(s) -> str:
    return s.name if hasattr(s, "name") else str(s).split(".")[-1]

deadline = time.time() + 900
while _state_name(inst.state) != "AVAILABLE":
    if time.time() > deadline:
        raise TimeoutError(f"Instance did not become AVAILABLE in 15 min — current state: {inst.state}")
    print(f"  state={_state_name(inst.state)} — waiting…")
    time.sleep(20)
    inst = w.database.get_database_instance(name=LAKEBASE_INSTANCE_NAME)

print(f"Ready. Endpoint: {inst.read_write_dns}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Open a Postgres connection using the current user's OAuth token

# COMMAND ----------

import uuid
import psycopg

cred = w.database.generate_database_credential(
    request_id=str(uuid.uuid4()),
    instance_names=[LAKEBASE_INSTANCE_NAME],
)

def pg_connect(dbname: str = PG_DB, autocommit: bool = True) -> psycopg.Connection:
    return psycopg.connect(
        host=inst.read_write_dns,
        dbname=dbname,
        user=me,
        password=cred.token,
        sslmode="require",
        autocommit=autocommit,
    )

with pg_connect() as c:
    with c.cursor() as cur:
        cur.execute("select version();")
        print(cur.fetchone()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create the IoT schema + tables in Lakebase

# COMMAND ----------

DDL = f"""
CREATE SCHEMA IF NOT EXISTS {PG_SCHEMA};

DROP TABLE IF EXISTS {PG_SCHEMA}.reefer_telemetry;
DROP TABLE IF EXISTS {PG_SCHEMA}.door_events;
DROP TABLE IF EXISTS {PG_SCHEMA}.gps_pings;
DROP TABLE IF EXISTS {PG_SCHEMA}.trailers;

CREATE TABLE {PG_SCHEMA}.trailers (
    trailer_id       text PRIMARY KEY,
    carrier_id       text NOT NULL,
    reefer_model     text NOT NULL,
    capacity_pallets int  NOT NULL,
    install_date     date NOT NULL
);

CREATE TABLE {PG_SCHEMA}.reefer_telemetry (
    trailer_id      text        NOT NULL,
    shipment_id     text        NOT NULL,
    ts              timestamptz NOT NULL,
    set_point_c     numeric(5,2),
    supply_air_c    numeric(5,2),
    return_air_c    numeric(5,2),
    compressor_on   boolean,
    fuel_level_pct  numeric(5,2),
    alarm_code      text
);
CREATE INDEX ON {PG_SCHEMA}.reefer_telemetry (trailer_id, ts);
CREATE INDEX ON {PG_SCHEMA}.reefer_telemetry (shipment_id);

CREATE TABLE {PG_SCHEMA}.door_events (
    event_id     text PRIMARY KEY,
    trailer_id   text NOT NULL,
    shipment_id  text NOT NULL,
    ts           timestamptz NOT NULL,
    event        text NOT NULL,
    location     text
);

CREATE TABLE {PG_SCHEMA}.gps_pings (
    trailer_id   text NOT NULL,
    shipment_id  text NOT NULL,
    ts           timestamptz NOT NULL,
    lat          numeric(9,5),
    lon          numeric(9,5),
    speed_kmh    numeric(5,1)
);
CREATE INDEX ON {PG_SCHEMA}.gps_pings (shipment_id, ts);
"""

with pg_connect() as c:
    with c.cursor() as cur:
        cur.execute(DDL)
print("IoT schema + tables created in Lakebase.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Generate synthetic data
# MAGIC
# MAGIC Uses the `generate` module sitting next to this notebook. Deterministic (seed=42) so the same excursion scenarios reproduce every run.

# COMMAND ----------

import os, sys
notebook_dir = os.path.dirname(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
sys.path.insert(0, f"/Workspace{notebook_dir}/../data")
import importlib, generate  # noqa: E402
importlib.reload(generate)

tables = generate.build_all(out_dir=None, telemetry_step_min=TELEMETRY_STEP_MIN)
for name, df in tables.items():
    print(f"{name:<22} {len(df):>8,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Bulk-load IoT data into Lakebase via COPY

# COMMAND ----------

import io

def copy_df(conn, df, qualified_table: str, cols: list[str]):
    buf = io.StringIO()
    df[cols].to_csv(buf, index=False, header=False, na_rep="\\N")
    buf.seek(0)
    with conn.cursor() as cur:
        with cur.copy(
            f"COPY {qualified_table} ({', '.join(cols)}) FROM STDIN WITH (FORMAT csv, NULL '\\N')"
        ) as cpy:
            cpy.write(buf.read())

with pg_connect(autocommit=False) as conn:
    copy_df(conn, tables["trailers"], f"{PG_SCHEMA}.trailers",
            ["trailer_id", "carrier_id", "reefer_model", "capacity_pallets", "install_date"])
    copy_df(conn, tables["reefer_telemetry"], f"{PG_SCHEMA}.reefer_telemetry",
            ["trailer_id", "shipment_id", "ts", "set_point_c", "supply_air_c", "return_air_c",
             "compressor_on", "fuel_level_pct", "alarm_code"])
    copy_df(conn, tables["door_events"], f"{PG_SCHEMA}.door_events",
            ["event_id", "trailer_id", "shipment_id", "ts", "event", "location"])
    copy_df(conn, tables["gps_pings"], f"{PG_SCHEMA}.gps_pings",
            ["trailer_id", "shipment_id", "ts", "lat", "lon", "speed_kmh"])
    conn.commit()

with pg_connect() as c:
    with c.cursor() as cur:
        for t in ["trailers", "reefer_telemetry", "door_events", "gps_pings"]:
            cur.execute(f"SELECT count(*) FROM {PG_SCHEMA}.{t}")
            print(f"  {PG_SCHEMA}.{t:<18} = {cur.fetchone()[0]:>8,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Create the UC schema + land analytical tables

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {UC_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}")
spark.sql(f"USE {UC_CATALOG}.{UC_SCHEMA}")

for name in ["carriers", "retailer_dcs", "product_master", "shipments", "customer_complaints"]:
    sdf = spark.createDataFrame(tables[name])
    (sdf.write.mode("overwrite").option("overwriteSchema", "true")
         .saveAsTable(f"{UC_CATALOG}.{UC_SCHEMA}.{name}"))
    print(f"  {UC_CATALOG}.{UC_SCHEMA}.{name:<20} = {sdf.count():>6} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done ✅
# MAGIC
# MAGIC | What | Where |
# MAGIC |---|---|
# MAGIC | Raw IoT (OLTP) | Lakebase `mccain-cold-chain-sju` → schema `iot` |
# MAGIC | Analytical     | `main.mccain_cold_chain_sju` |
# MAGIC
# MAGIC Next: **01_lakebase_to_lakehouse** — read IoT from Lakebase and land bronze Delta.
