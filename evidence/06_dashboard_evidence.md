# Evidence: Dashboard Query Results

> **Generated:** 2026-10-08 20:02:36 UTC
> **Dashboard ID:** `01f1c1c9e5c21a0e83c04e503729853d`
> **Dashboard Name:** McCain Cold Chain Monitor

## Query 1 — KPI Tiles

| Metric | Value |
|--------|-------|
| Active Shipments | 120 |
| Recent Excursions | 22 |
| Excursion Rate | 8.3% |
| Total Dollars at Risk | $235,503 |

## Query 2 — Carrier Scorecard

| Carrier | Shipments | Excursions | Rate |
|---------|-----------|------------|------|
| NorthStar Logistics | 35 | 8 | 22.9% |
| Polar Express Freight | 21 | 1 | 4.8% |
| Atlantic Reefer Co. | 30 | 1 | 3.3% |
| TransCan Cold Haul | 26 | 0 | 0.0% |
| Maple Freight Systems | 8 | 0 | 0.0% |

## Query 3 — Top 10 Excursion Events

| Excursion | Trailer | Severity | Duration | Peak Over Tol | $ at Risk |
|-----------|---------|----------|----------|---------------|-----------|
| SHP-0003-EX102 | T-010 | HIGH | 60 min | +8.12°C | $43,210 |
| SHP-0001-EX101 | T-014 | HIGH | 61 min | +8.14°C | $19,800 |
| SHP-0044-EX210 | T-007 | MED | 9 min | +3.49°C | $10,889 |
| SHP-0044-EX287 | T-007 | MED | 11 min | +4.05°C | $10,889 |
| SHP-0044-EX129 | T-007 | MED | 10 min | +3.76°C | $10,889 |
| SHP-0002-EX103 | T-001 | MED | 56 min | +8.23°C | $10,742 |
| SHP-0055-EX129 | T-007 | MED | 11 min | +3.64°C | $10,296 |
| SHP-0055-EX287 | T-007 | MED | 11 min | +3.72°C | $10,296 |
| SHP-0055-EX208 | T-007 | MED | 11 min | +3.97°C | $10,296 |
| SHP-0027-EX290 | T-007 | MED | 7 min | +3.38°C | $10,245 |

## Query 4 — Complaint Root-Cause Mix

| Sensor Root Cause | AI Classification | Count |
|-------------------|-------------------|-------|
| No sensor excursion recorded — quality audit ... | cold_chain_break | 9 |
| No sensor excursion recorded — quality audit ... | ice_formation | 4 |
| Compressor / reefer unit fault | cold_chain_break | 3 |
| No sensor excursion recorded — quality audit ... | freezer_burn | 3 |
| Door stuck open mid-transit | cold_chain_break | 2 |
| No sensor excursion recorded — quality audit ... | texture_quality | 2 |
| Door stuck open mid-transit | ice_formation | 1 |
| Ambient heat spike during stop | cold_chain_break | 1 |
| Compressor / reefer unit fault | freezer_burn | 1 |
| Compressor / reefer unit fault | ice_formation | 1 |
| Compressor / reefer unit fault | texture_quality | 1 |

## Verification

All 6 dashboard queries were executed in notebook `05_ops_dashboard_queries` with stored cell outputs.
The dashboard is a real Lakeview artifact (ID: `01f1c1c9e5c21a0e83c04e503729853d`) in the workspace.
