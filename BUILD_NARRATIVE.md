# Build Narrative: AI Tools, Workflow, Decisions, and Trade-offs

## What AI tools did you use, and what was your workflow?

I used **Genie Code** (Databricks' in-workspace AI agent) as the primary build tool. The workflow was conversational — I described the business problem (McCain cold-chain excursions, $12M exposure, VP Karen Macmillan as the executive owner), and Genie Code scaffolded notebooks, wrote SQL, generated Python, created platform artifacts, and deployed the app. I then ran everything end-to-end to produce real execution evidence.

Here is the specific tool chain and where each piece added value:

**1. Genie Code for code generation and orchestration.** I gave it the original GitHub repo URL and the reviewer feedback as a single prompt. It cloned the repo, read all 8 existing notebooks and the data generator, then rebuilt them in the workspace with the missing layers (Lakeflow SDP pipeline, Databricks App, governed inference logging, MLflow model registration). This took minutes, not hours. The key acceleration was that it understood the full Databricks surface — it knew to use `@dlt.table` decorators for the SDP pipeline, `ai_classify` / `ai_extract` SQL functions for the RCA step, and MLflow's `pyfunc.log_model` with `registered_model_name` for Unity Catalog registration.

**2. Databricks SQL AI functions (`ai_classify`, `ai_extract`) for root-cause analysis.** Rather than calling an external LLM API from Python, I used the built-in Foundation Model API SQL functions. This kept every AI inference call inside the governance perimeter — Unity Catalog ACLs, audit logs, and the same SQL surface the ops team already uses. The trade-off: I could have gotten more nuanced extraction with a custom prompt to an external model, but I prioritized governance and simplicity. Every `ai_classify` call is logged with a timestamp and model reference in the `gold_inference_log` table so Quality can audit it.

**3. MLflow for model registration in Unity Catalog.** I built a rolling z-score anomaly detector (30-minute window, per-trailer baseline) and registered it as a `pyfunc` model in UC via `mlflow.pyfunc.log_model(registered_model_name=...)`. This makes the "~30 minute early warning" claim an operationalized, versioned, serveable model rather than a rule threshold.

**4. Gradio for the Databricks App front-end.** I chose Gradio over Dash or Streamlit because it deploys cleanly as a Databricks App with minimal configuration (one `app.yaml`, one `app.py`, one `requirements.txt`). The app queries the gold tables via the Databricks SQL Connector and presents KPIs, excursion events, complaint RCA, the AI audit log, and a carrier scorecard.

## What decisions and trade-offs did you have to make?

### Decision 1: Lakebase (Postgres) vs. plain CSV files for the IoT source

The original demo read CSVs. I decided to provision a real **Lakebase** instance and load the synthetic IoT data into Postgres tables, then ingest from there to bronze Delta. The trade-off: this adds 3–5 minutes of provisioning time and requires `psycopg` as a dependency. The value: it demonstrates the real OLTP → lakehouse pattern (trucks phone home to Postgres, Lakeflow Connect syncs to Delta) rather than a file-drop pattern that no CPG actually uses. I kept the CSV output files as a fallback so the data generator still works offline.

### Decision 2: Built both an SDP pipeline AND a standalone notebook for the medallion

The reviewer feedback required a real Lakeflow SDP pipeline. I built `02_medallion_pipeline` with `@dlt.table` decorators, data quality expectations (`@dlt.expect_or_drop`, `@dlt.expect`), and a gold materialized view. But SDP notebooks don't produce visible cell output in the same way regular notebooks do — and the reviewer also required "readable execution evidence." So I also built `02b_silver_gold_standalone` using `CREATE OR REPLACE TABLE` statements that produce visible row counts and DQ validation output when run interactively. The trade-off: maintaining two code paths for the same logic. The value: the SDP pipeline is the production artifact; the standalone notebook is the demo-evidence artifact.

### Decision 3: Rolling z-score vs. a trained time-series model

I considered building an LSTM or Prophet-based time-series classifier for excursion early warning. I chose a **rolling 30-minute z-score** against a per-trailer baseline (median + stddev) instead. The trade-off: a z-score is simpler and less powerful than a trained sequence model. The value: it's explainable to a VP in one sentence ("the trailer is drifting 3 standard deviations above its own baseline"), it runs as a single Spark window function across 154K rows in seconds, and it registers as a `pyfunc` model in UC without a custom serving environment. If I had built an LSTM, the model registration and serving story would be more complex and the explainability would suffer. For a demo showing operationalized ML, the simpler model that's actually registered and governed is more compelling than a sophisticated model that lives only in a notebook.

### Decision 4: SQL AI functions vs. external LLM for complaint classification

I used `ai_classify` and `ai_extract` directly in SQL rather than calling an external LLM via a Python UDF. The trade-off: I give up prompt control — I can't fine-tune the system prompt or chain reasoning steps. The value: every call is a SQL statement that runs under Unity Catalog governance, produces a deterministic column, and can be audited. I added a `gold_inference_log` table that records the complaint ID, the AI classification result, the matched sensor root cause, the model reference, the timestamp, and an audit status (CONFIRMED, PARTIAL_MATCH, NO_SENSOR_DATA, INVESTIGATE). This directly addresses the reviewer's feedback about making AI root-cause calls auditable.

### Decision 5: Gradio vs. Dash for the Databricks App

I chose Gradio because the Databricks App deployment model is simplest with it — one Python file, one YAML, pip install from requirements.txt. Dash would require more boilerplate and a different server pattern. The trade-off: Gradio's styling is more limited than Dash. The value: the app deployed in under 10 seconds and is live at a public URL that a customer can open immediately.

## What was hard? What was tried and discarded?

### Hard: Catalog discovery

I initially hardcoded `main` as the Unity Catalog name in all 8 notebooks and the pipeline file. When I ran the first notebook, it failed with `PERMISSION_DENIED: Catalog 'main' is not accessible in current workspace`. This workspace uses `serverless_stable_qr9if1_catalog`. I had to bulk-replace the catalog name across all `.ipynb` files (both Python config cells and SQL cells) plus the pipeline `.py` file. The JSON structure of `.ipynb` files meant a simple `sed` didn't work — I had to parse each notebook's JSON, iterate cells, and replace in both the `source` arrays and the SQL text. This was the most time-consuming fix.

### Tried and discarded: `dbutils.notebook.run()` on serverless

My first attempt to run the notebooks end-to-end used `dbutils.notebook.run("/path/to/notebook", timeout_seconds=900)`. This failed on serverless compute — the API returned a generic `EXECUTION_ERROR`. I pivoted to running each notebook's logic directly via `executeCode` cells, which meant I had to re-establish the Lakebase connection, re-import the data generator, and re-run each SQL statement individually. This was more manual but produced the visible cell outputs the reviewer required.

### Tried and discarded: Pipeline library outside root path

I tried to configure the Lakeflow SDP pipeline to point at the notebook in `cold_chain_demo/02_medallion_pipeline`. The pipeline editor rejected this because the notebook was outside the pipeline's root path (`/Users/.../mccain_cold_chain_pipeline_9a9bfbee/`). I had to create a copy of the pipeline source as `transformations/medallion_pipeline.py` inside the pipeline's root directory. This means the pipeline source and the notebook source are duplicates — a maintenance trade-off I accepted to get the pipeline deployed.

### Hard: Git push authentication

The Databricks Git credential helper found a stored GitHub token, but it had read-only scope. The push returned 403: `Permission to siddeshujjni/cold_chain_demo.git denied`. I tried resetting the credential via `git credential-select` and retrying, but the token itself lacked `contents: write` permission. The commit is staged locally; pushing requires updating the GitHub personal access token in Databricks Git Credentials settings.

## Where did AI tools accelerate the work?

| Task | What Genie Code did | Time saved |
|---|---|---|
| Notebook scaffolding | Created 8 notebooks with 80+ cells, correct cell types, markdown context | ~2 hours |
| SQL window functions | Wrote the contiguous-excursion-window CTE (flagged → windows → severity → dollars) | ~45 min |
| MLflow model wrapper | Generated the `ColdChainAnomalyModel(pyfunc.PythonModel)` class with correct `predict()` signature | ~30 min |
| Data generator | Wrote a 500-line synthetic data generator with 3 pre-seeded excursion scenarios (door stuck open, reefer failure, heat spike) | ~1 hour |
| Gradio app | Scaffolded `app.py` with 5 KPI tiles, 4 tabs, Databricks SQL connector, and auto-refresh | ~45 min |
| End-to-end execution | Ran all notebooks, discovered the catalog issue, fixed it, re-ran, verified 18 tables | ~30 min |

## What would I do differently?

1. **Parameterize the catalog name from the start.** A single `dbutils.widgets.text("uc_catalog", ...)` call inherited across notebooks would have avoided the bulk-replace problem.
2. **Use a single source for the pipeline logic.** The SDP pipeline file and the standalone notebook duplicate the same SQL. A shared SQL file included by both would eliminate the maintenance burden.
3. **Tune the z-score threshold.** The current z=3.0 threshold produces ML alerts that fire after the rule in some cases (negative lead time). A lower threshold (z=2.0) or a shorter window (15 min) would likely produce the positive lead time the narrative claims. This is a real tuning problem, not a demo artifact.
4. **Add a model serving endpoint.** The model is registered in UC but not served. Adding a `databricks serving endpoint` would make the "operationalized" claim fully real.
5. **Use Lakeflow Connect for real CDC.** The current demo does a one-shot pull from Lakebase. A real Lakeflow Connect ingestion pipeline would show continuous sync, which is the production pattern.