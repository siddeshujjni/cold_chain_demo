# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Genie Space setup (live RCA Q&A)
# MAGIC
# MAGIC The "wow moment" of the demo: a stakeholder asks a question in English, Genie answers with a correct SQL query against the gold tables.
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Tags the right set of tables (silver_shipment_profile, gold_excursion_events, gold_complaint_root_cause + dimensions)
# MAGIC 2. Emits recommended Genie instructions
# MAGIC 3. Lists the demo questions to ask on stage

# COMMAND ----------

UC_CATALOG = "main"
UC_SCHEMA = "mccain_cold_chain_sju"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Confirm tables exist

# COMMAND ----------

display(spark.sql(f"""
  SHOW TABLES IN {UC_CATALOG}.{UC_SCHEMA}
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Tables to include in the Genie space
# MAGIC
# MAGIC From **Catalog Explorer**, create a new Genie space scoped to this schema. Add:
# MAGIC - `silver_shipment_profile` (one row per shipment — main fact)
# MAGIC - `gold_excursion_events` (one row per excursion — primary answer surface)
# MAGIC - `gold_complaint_root_cause` (closes complaint ↔ sensor loop)
# MAGIC - `carriers`, `retailer_dcs`, `product_master`, `trailers` (dimensions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Suggested Genie space instructions
# MAGIC
# MAGIC Paste this into the Genie space's **Instructions** tab — it teaches Genie the business vocabulary and preferred joins.

# COMMAND ----------

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
- Join to `carriers`, `retailer_dcs`, `product_master`, `trailers` for friendly names.

NAMING
- 'Excursion rate' = (shipments with had_excursion = true) / total shipments.
- 'Dollars at risk' = sum(gold_excursion_events.dollars_at_risk).
- 'Ahead-of-rule lead time' = minutes between first ML alert and first rule alert per shipment.
"""
print(GENIE_INSTRUCTIONS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Demo questions — ask these on stage (in order)
# MAGIC
# MAGIC These are crafted so Genie answers correctly against the tables above. Test each before the talk.

# COMMAND ----------

DEMO_QUESTIONS = [
    # Warm-up — a tile answer
    "How many shipments had cold-chain excursions this week and what is the total dollars at risk?",
    # Attribution
    "Which carriers have the highest excursion rate? Show excursion rate and dollars at risk.",
    # Drill-down
    "Which trailer has the most HIGH severity excursions, and what alarm codes fired?",
    # Destination view
    "Which retailer DCs are receiving the most shipments with sensor excursions?",
    # Product-level
    "Which product families are most affected by out-of-tolerance events?",
    # Complaint closure
    "Show me customer complaints where the sensor root cause was a door-open event.",
    # The money slide
    "On shipments where ML warned before the rule, what was the average lead time in minutes?",
]

for i, q in enumerate(DEMO_QUESTIONS, 1):
    print(f"{i}.  {q}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Closer
# MAGIC
# MAGIC > "Same data, same governance, but now the line-of-business person asks in English. This is where the McCain ops analyst lives, not in a notebook."
