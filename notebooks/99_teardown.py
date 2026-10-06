# Databricks notebook source
# MAGIC %md
# MAGIC # 99 — Teardown
# MAGIC
# MAGIC Deletes every resource created by this demo so it stops costing money.
# MAGIC
# MAGIC **Leaves in place:** the notebooks themselves, the generator, the architecture diagram. Everything can be re-spun by running `00_setup` again.

# COMMAND ----------

# MAGIC %pip install -q databricks-sdk --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

LAKEBASE_INSTANCE_NAME = "mccain-cold-chain-sju"
UC_CATALOG = "main"
UC_SCHEMA = "mccain_cold_chain_sju"
FED_CATALOG = "lakebase_mccain_iot"
CONNECTION_NAME = f"lb_conn_{LAKEBASE_INSTANCE_NAME.replace('-', '_')}"

# Safety toggle — set to True before running to actually tear things down.
CONFIRM_DESTROY = False

# COMMAND ----------

assert CONFIRM_DESTROY, "Set CONFIRM_DESTROY = True in the cell above, then re-run."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Drop the federated catalog and connection

# COMMAND ----------

for stmt in [
    f"DROP CATALOG IF EXISTS {FED_CATALOG} CASCADE",
    f"DROP CONNECTION IF EXISTS {CONNECTION_NAME}",
]:
    try:
        spark.sql(stmt)
        print(f"  ✅ {stmt}")
    except Exception as e:
        print(f"  ⚠️  {stmt} — {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Drop the demo schema (all silver/gold/bronze tables go with it)

# COMMAND ----------

spark.sql(f"DROP SCHEMA IF EXISTS {UC_CATALOG}.{UC_SCHEMA} CASCADE")
print(f"  ✅ dropped {UC_CATALOG}.{UC_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Delete the Lakebase instance
# MAGIC
# MAGIC This is the cost-bearing resource. Deletion is permanent.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
try:
    inst = w.database.get_database_instance(name=LAKEBASE_INSTANCE_NAME)
    w.database.delete_database_instance(name=LAKEBASE_INSTANCE_NAME, purge=True)
    print(f"  ✅ deleted Lakebase instance {LAKEBASE_INSTANCE_NAME}")
except Exception as e:
    print(f"  ⚠️  {LAKEBASE_INSTANCE_NAME} — {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done. Run `00_setup` any time to rebuild the whole demo.
