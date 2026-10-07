# Databricks notebook source

# COMMAND ----------

# DBTITLE 1,06 — Genie Space (Live RCA Q&A)
# 06 — Genie Space Setup (Live RCA Q&A)

The "wow moment" of the demo: a stakeholder asks a question in English, Genie answers with a correct SQL query against the gold tables.

This notebook:
1. Confirms the required tables exist
2. **Programmatically creates the Genie space** via the SDK
3. Lists the demo questions to ask on stage

> "Same data, same governance, but now the line-of-business person asks in English. This is where the McCain ops analyst lives, not in a notebook."

# COMMAND ----------

# DBTITLE 1,Config
UC_CATALOG = "serverless_stable_qr9if1_catalog"
UC_SCHEMA = "mccain_cold_chain_sju"
GENIE_SPACE_NAME = "McCain Cold Chain Operations"

# COMMAND ----------

# DBTITLE 1,1. Confirm tables exist
# --- EXECUTION EVIDENCE: verify all required tables exist ---
required_tables = [
    "silver_shipment_profile", "gold_excursion_events", "gold_complaint_root_cause",
    "gold_inference_log", "carriers", "retailer_dcs", "product_master",
    "alerts_rule_based", "alerts_ml_based"
]

print("="*60)
print("GENIE SPACE TABLE VERIFICATION")
print("="*60)
for t in required_tables:
    try:
        count = spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.{t}").count()
        print(f"  ✅ {t:<35} {count:>8,} rows")
    except Exception as e:
        print(f"  ❌ {t:<35} MISSING - run upstream notebooks first")

# COMMAND ----------

# DBTITLE 1,2. Genie space instructions
GENIE_INSTRUCTIONS = """\
You answer cold-chain quality questions for McCain Foods operations.

DOMAIN PRIMER
- McCain produces frozen food (fries, hash browns, pizza, appetizers) and ships from plants (primary: Florenceville-Bristol, NB) to retailer distribution centers (DCs).
- Product target temperatures are stored in `product_master.target_temp_c` with `upper_tolerance_c` above which a shipment is out-of-spec.
- An 'excursion' is a contiguous period where supply-air temperature exceeded upper_tolerance. Excursions of ≥20 min and peaks ≥3°C over tolerance are HIGH severity.

PREFERRED TABLES
- Use `gold_excursion_events` to answer anything about out-of-spec events (counts, dollars_at_risk, severity, trailer/carrier/DC/SKU attribution).
- Use `silver_shipment_profile` for per-shipment rollups (had_excursion, max_supply_c, minutes_above_tolerance, alarm summary).
- Use `gold_complaint_root_cause` to answer questions that involve customer complaints and to tie complaints to sensor signals.
- Use `gold_inference_log` to audit AI-powered root-cause determinations.
- Join to `carriers`, `retailer_dcs`, `product_master` for friendly names.

NAMING
- 'Excursion rate' = (shipments with had_excursion = true) / total shipments.
- 'Dollars at risk' = sum(gold_excursion_events.dollars_at_risk).
- 'Ahead-of-rule lead time' = minutes between first ML alert and first rule alert per shipment.
- VP Supply Chain Quality Karen Macmillan owns the excursion exposure metric.
"""
print(GENIE_INSTRUCTIONS)

# COMMAND ----------

# DBTITLE 1,3. Create Genie Space programmatically
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Tables to include in the Genie space
genie_tables = [
    f"{UC_CATALOG}.{UC_SCHEMA}.silver_shipment_profile",
    f"{UC_CATALOG}.{UC_SCHEMA}.gold_excursion_events",
    f"{UC_CATALOG}.{UC_SCHEMA}.gold_complaint_root_cause",
    f"{UC_CATALOG}.{UC_SCHEMA}.gold_inference_log",
    f"{UC_CATALOG}.{UC_SCHEMA}.carriers",
    f"{UC_CATALOG}.{UC_SCHEMA}.retailer_dcs",
    f"{UC_CATALOG}.{UC_SCHEMA}.product_master",
    f"{UC_CATALOG}.{UC_SCHEMA}.alerts_rule_based",
    f"{UC_CATALOG}.{UC_SCHEMA}.alerts_ml_based",
]

try:
    spaces = list(w.genie.list_spaces())
    existing = [s for s in spaces if s.title == GENIE_SPACE_NAME]
    if existing:
        space = existing[0]
        print(f"✅ Reusing existing Genie space: {space.space_id}")
    else:
        raise Exception("not found")
except Exception:
    try:
        space = w.genie.create_space(
            title=GENIE_SPACE_NAME,
            description="Cold-chain operations Q&A for McCain Foods. Covers excursions, complaints, carriers, and ML early-warning alerts.",
        )
        print(f"✅ Created Genie space: {space.space_id}")
    except Exception as e:
        print(f"⚠️ Could not create Genie space via SDK: {e}")
        print(f"Manual setup: Create a Genie space named '{GENIE_SPACE_NAME}' with these tables:")
        for t in genie_tables:
            print(f"  - {t}")

# COMMAND ----------

# DBTITLE 1,4. Demo questions
# --- Demo questions to ask on stage (in order) ---
DEMO_QUESTIONS = [
    "How many shipments had cold-chain excursions this week and what is the total dollars at risk?",
    "Which carriers have the highest excursion rate? Show excursion rate and dollars at risk.",
    "Which trailer has the most HIGH severity excursions, and what alarm codes fired?",
    "Which retailer DCs are receiving the most shipments with sensor excursions?",
    "Which product families are most affected by out-of-tolerance events?",
    "Show me customer complaints where the sensor root cause was a door-open event.",
    "On shipments where ML warned before the rule, what was the average lead time in minutes?",
    "Show me the AI inference audit log for complaints classified as cold_chain_break.",
]

print("\nDEMO QUESTIONS (ask these on stage):")
print("="*60)
for i, q in enumerate(DEMO_QUESTIONS, 1):
    print(f"  {i}. {q}")

# COMMAND ----------

# DBTITLE 1,Talk track
### Talk track

> "Same data, same governance, but now the line-of-business person asks in English. This is where the McCain ops analyst lives, not in a notebook."
>
> "And notice question 8 — we can even audit the AI's own decisions through Genie. The inference log is a governed table, so the quality team can trace any root-cause call back to the model, the input, and the timestamp."