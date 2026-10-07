# Databricks notebook source

# COMMAND ----------

# DBTITLE 1,99 — Teardown
# 99 — Teardown

Deletes every resource created by this demo so it stops costing money.

**Leaves in place:** the notebooks themselves, the generator, the architecture diagram. Everything can be re-spun by running `00_setup` again.

# COMMAND ----------

# DBTITLE 1,Install dependencies
# MAGIC %pip install -q databricks-sdk --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Config + safety toggle
LAKEBASE_INSTANCE_NAME = "mccain-cold-chain-sju"
UC_CATALOG = "main"
UC_SCHEMA = "mccain_cold_chain_sju"
MODEL_NAME = f"{UC_CATALOG}.{UC_SCHEMA}.cold_chain_anomaly_detector"

# Safety toggle — set to True before running to actually tear things down.
CONFIRM_DESTROY = False

# COMMAND ----------

# DBTITLE 1,Safety check
assert CONFIRM_DESTROY, "Set CONFIRM_DESTROY = True in the cell above, then re-run."

# COMMAND ----------

# DBTITLE 1,1. Drop the UC schema
# Drop the demo schema (all silver/gold/bronze tables go with it)
spark.sql(f"DROP SCHEMA IF EXISTS {UC_CATALOG}.{UC_SCHEMA} CASCADE")
print(f"  ✅ dropped {UC_CATALOG}.{UC_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,2. Delete MLflow model
import mlflow
mlflow.set_registry_uri("databricks-uc")
try:
    client = mlflow.MlflowClient()
    client.delete_registered_model(MODEL_NAME)
    print(f"  ✅ deleted model {MODEL_NAME}")
except Exception as e:
    print(f"  ⚠️  {MODEL_NAME} — {e}")

# COMMAND ----------

# DBTITLE 1,3. Delete Lakebase instance
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
try:
    inst = w.database.get_database_instance(name=LAKEBASE_INSTANCE_NAME)
    w.database.delete_database_instance(name=LAKEBASE_INSTANCE_NAME, purge=True)
    print(f"  ✅ deleted Lakebase instance {LAKEBASE_INSTANCE_NAME}")
except Exception as e:
    print(f"  ⚠️  {LAKEBASE_INSTANCE_NAME} — {e}")

# COMMAND ----------

# DBTITLE 1,Done
## Done. Run `00_setup` any time to rebuild the whole demo.