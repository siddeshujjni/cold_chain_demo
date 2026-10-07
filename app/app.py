"""McCain Cold Chain Monitor — Databricks App

Ops front-end for VP Supply Chain Quality Karen Macmillan.
Traces customer complaints to specific sensor excursion events.
Queries Unity Catalog gold tables via Databricks SQL Connector.
"""

import os
import gradio as gr
import pandas as pd
from databricks import sql as dbsql

# ── Config ────────────────────────────────────────────────────────────────────
CATALOG = os.getenv("UC_CATALOG", "serverless_stable_qr9if1_catalog")
SCHEMA = os.getenv("UC_SCHEMA", "mccain_cold_chain_sju")


def _conn():
    """Open a Databricks SQL connection using workspace-native auth."""
    return dbsql.connect(
        server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME",
                                  os.getenv("DATABRICKS_HOST", "").replace("https://", "")),
        http_path=os.getenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/auto"),
        credentials_provider=None,  # uses default: env token or OAuth
    )


def _query(sql: str) -> pd.DataFrame:
    """Run SQL and return a pandas DataFrame."""
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=cols)


def _t(name: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{name}"


# ── Data loaders ──────────────────────────────────────────────────────────────

def load_kpis():
    df = _query(f"""
        SELECT
          (SELECT count(*) FROM {_t('shipments')})                                            AS total_shipments,
          (SELECT count(*) FROM {_t('silver_shipment_profile')} WHERE had_excursion)          AS excursion_shipments,
          (SELECT round(sum(dollars_at_risk)) FROM {_t('gold_excursion_events')})              AS total_dollars_at_risk,
          (SELECT count(*) FROM {_t('gold_excursion_events')} WHERE severity = 'HIGH')        AS high_severity,
          (SELECT count(*) FROM {_t('gold_complaint_root_cause')} WHERE had_excursion)        AS complaints_with_sensor_match
    """)
    r = df.iloc[0]
    rate = round(100 * r["excursion_shipments"] / r["total_shipments"], 1) if r["total_shipments"] else 0
    return (
        str(int(r["total_shipments"])),
        f"{int(r['excursion_shipments'])} ({rate}%)",
        f"${int(r['total_dollars_at_risk']):,}",
        str(int(r["high_severity"])),
        str(int(r["complaints_with_sensor_match"])),
    )


def load_excursions():
    return _query(f"""
        SELECT excursion_id, shipment_id, trailer_id, carrier_id, dest_dc_id,
               sku, severity, duration_min, peak_supply_c,
               round(peak_over_tolerance_c, 2) AS over_tol_c,
               round(dollars_at_risk) AS dollars_at_risk,
               start_ts, end_ts
        FROM {_t('gold_excursion_events')}
        ORDER BY CASE severity WHEN 'HIGH' THEN 1 WHEN 'MED' THEN 2 ELSE 3 END,
                 dollars_at_risk DESC
    """)


def load_complaint_rca():
    return _query(f"""
        SELECT complaint_id, retailer, alleged_cause, complaint_severity,
               shipment_id, had_excursion, max_supply_c, top_alarm,
               sensor_root_cause
        FROM {_t('gold_complaint_root_cause')}
        ORDER BY received_ts DESC
    """)


def load_inference_log():
    return _query(f"""
        SELECT complaint_id, retailer,
               ai_classification_result, ai_severity_result,
               matched_sensor_cause, sensor_confirmed,
               audit_status, inference_ts, inference_model
        FROM {_t('gold_inference_log')}
        ORDER BY inference_ts DESC
    """)


def load_carrier_scorecard():
    return _query(f"""
        SELECT c.carrier_name,
               count(DISTINCT sp.shipment_id) AS shipments,
               sum(CASE WHEN sp.had_excursion THEN 1 ELSE 0 END) AS excursions,
               round(100.0 * sum(CASE WHEN sp.had_excursion THEN 1 ELSE 0 END)
                     / nullif(count(DISTINCT sp.shipment_id), 0), 1) AS excursion_rate_pct,
               round(sum(CASE WHEN e.dollars_at_risk IS NOT NULL THEN e.dollars_at_risk ELSE 0 END)) AS dollars_at_risk
        FROM {_t('silver_shipment_profile')} sp
        JOIN {_t('carriers')} c ON c.carrier_id = sp.carrier_id
        LEFT JOIN {_t('gold_excursion_events')} e ON e.shipment_id = sp.shipment_id
        GROUP BY c.carrier_name
        ORDER BY excursion_rate_pct DESC
    """)


def refresh_all():
    """Refresh every panel at once."""
    kpis = load_kpis()
    exc = load_excursions()
    rca = load_complaint_rca()
    log = load_inference_log()
    carrier = load_carrier_scorecard()
    return (*kpis, exc, rca, log, carrier)


# ── Gradio UI ─────────────────────────────────────────────────────────────────

css = """
.kpi-box { text-align: center; padding: 12px; }
.kpi-box .label { font-size: 0.85em; color: #666; }
"""

with gr.Blocks(
    title="McCain Cold Chain Monitor",
    css=css,
    theme=gr.themes.Soft(),
) as app:

    gr.Markdown("# McCain Cold Chain Monitor")
    gr.Markdown(
        "Ops front-end for **VP Supply Chain Quality Karen Macmillan**. "
        "Traces customer complaints to specific sensor excursion events. "
        "All data from Unity Catalog gold tables."
    )

    # KPI row
    with gr.Row():
        kpi_shipments = gr.Textbox(label="Total Shipments", interactive=False)
        kpi_excursions = gr.Textbox(label="Excursion Shipments", interactive=False)
        kpi_dollars = gr.Textbox(label="Total $ at Risk", interactive=False)
        kpi_high = gr.Textbox(label="HIGH Severity", interactive=False)
        kpi_matched = gr.Textbox(label="Complaints w/ Sensor Match", interactive=False)

    refresh_btn = gr.Button("Refresh All", variant="primary")

    with gr.Tabs():
        with gr.Tab("Excursion Events"):
            gr.Markdown("Gold excursion events ordered by severity and dollars at risk.")
            tbl_exc = gr.Dataframe(label="gold_excursion_events", interactive=False)

        with gr.Tab("Complaint Root Cause"):
            gr.Markdown("Each complaint matched with AI classification and sensor root cause.")
            tbl_rca = gr.Dataframe(label="gold_complaint_root_cause", interactive=False)

        with gr.Tab("AI Audit Log"):
            gr.Markdown(
                "Governed inference log — every AI classify/extract call is auditable. "
                "Audit status: CONFIRMED = sensor + AI agree, "
                "PARTIAL_MATCH = excursion but different cause, "
                "INVESTIGATE = needs manual review."
            )
            tbl_log = gr.Dataframe(label="gold_inference_log", interactive=False)

        with gr.Tab("Carrier Scorecard"):
            gr.Markdown("Carrier-level excursion rates and dollars at risk.")
            tbl_carrier = gr.Dataframe(label="carrier_scorecard", interactive=False)

    # Wire refresh
    outputs = [
        kpi_shipments, kpi_excursions, kpi_dollars, kpi_high, kpi_matched,
        tbl_exc, tbl_rca, tbl_log, tbl_carrier,
    ]
    refresh_btn.click(fn=refresh_all, outputs=outputs)
    app.load(fn=refresh_all, outputs=outputs)


if __name__ == "__main__":
    app.launch(server_name="0.0.0.0", server_port=8000)
