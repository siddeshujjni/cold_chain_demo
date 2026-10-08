# Evidence: AI Inference + Governed Logging

> **Generated:** 2026-10-08 20:02:36 UTC

## AI Functions Used

| Function | Purpose | Input | Output |
|----------|---------|-------|--------|
| `ai_classify()` | Categorize complaint root cause | Complaint body text | One of: cold_chain_break, freezer_burn, texture_quality, ice_formation, other |
| `ai_classify()` | Severity assessment | Complaint body text | One of: high, medium, low |
| `ai_extract()` | Structured field extraction | Complaint body text | JSON with product_mentioned, temperature_cited, urgency_level |

## Classification Distribution

| Alleged Cause | Sensor Confirmed | No Sensor Signal | Total |
|--------------|-----------------|-----------------|-------|
| cold_chain_break | 6 | 9 | 15 |
| ice_formation | 2 | 4 | 6 |
| freezer_burn | 1 | 3 | 4 |
| texture_quality | 1 | 2 | 3 |

## Governed Inference Log (all 28 entries)

Every AI function call is logged with model name, timestamp, and audit status for full governance.

| Complaint | Retailer | AI Classification | Severity | Sensor Confirmed | Sensor Cause | Audit Status |
|-----------|----------|-------------------|----------|-----------------|--------------|--------------|
| CMP-0001 | Costco Canada | ice_formation | high | True | Door stuck open mid-transit | PARTIAL_MATCH |
| CMP-0002 | Save-On-Foods | cold_chain_break | high | True | Door stuck open mid-transit | CONFIRMED |
| CMP-0003 | Costco Canada | cold_chain_break | high | True | Door stuck open mid-transit | CONFIRMED |
| CMP-0004 | Sobeys | cold_chain_break | high | True | Ambient heat spike during stop | CONFIRMED |
| CMP-0005 | Walmart Canada | freezer_burn | high | True | Compressor / reefer unit fault | PARTIAL_MATCH |
| CMP-0006 | Metro | cold_chain_break | high | True | Compressor / reefer unit fault | CONFIRMED |
| CMP-0007 | Costco Canada | ice_formation | high | True | Compressor / reefer unit fault | PARTIAL_MATCH |
| CMP-0008 | Walmart Canada | cold_chain_break | high | True | Compressor / reefer unit fault | CONFIRMED |
| CMP-0009 | Costco Canada | cold_chain_break | high | True | Compressor / reefer unit fault | CONFIRMED |
| CMP-0010 | Loblaws | texture_quality | medium | True | Compressor / reefer unit fault | PARTIAL_MATCH |
| CMP-0011 | Costco Canada | freezer_burn | high | False | No sensor excursion recorded — quality a... | NO_SENSOR_DATA |
| CMP-0012 | Couche-Tard | ice_formation | high | False | No sensor excursion recorded — quality a... | NO_SENSOR_DATA |
| CMP-0013 | Save-On-Foods | cold_chain_break | high | False | No sensor excursion recorded — quality a... | INVESTIGATE |
| CMP-0014 | Sobeys | cold_chain_break | high | False | No sensor excursion recorded — quality a... | INVESTIGATE |
| CMP-0015 | Whole Foods | cold_chain_break | high | False | No sensor excursion recorded — quality a... | INVESTIGATE |
| CMP-0016 | Metro | cold_chain_break | high | False | No sensor excursion recorded — quality a... | INVESTIGATE |
| CMP-0017 | Loblaws | cold_chain_break | medium | False | No sensor excursion recorded — quality a... | INVESTIGATE |
| CMP-0018 | Save-On-Foods | ice_formation | medium | False | No sensor excursion recorded — quality a... | NO_SENSOR_DATA |
| CMP-0019 | Save-On-Foods | cold_chain_break | high | False | No sensor excursion recorded — quality a... | INVESTIGATE |
| CMP-0020 | Sobeys | cold_chain_break | high | False | No sensor excursion recorded — quality a... | INVESTIGATE |
| CMP-0021 | Whole Foods | ice_formation | high | False | No sensor excursion recorded — quality a... | NO_SENSOR_DATA |
| CMP-0022 | Walmart Canada | texture_quality | high | False | No sensor excursion recorded — quality a... | NO_SENSOR_DATA |
| CMP-0023 | Walmart Canada | ice_formation | high | False | No sensor excursion recorded — quality a... | NO_SENSOR_DATA |
| CMP-0024 | Loblaws | freezer_burn | high | False | No sensor excursion recorded — quality a... | NO_SENSOR_DATA |
| CMP-0025 | Whole Foods | texture_quality | medium | False | No sensor excursion recorded — quality a... | NO_SENSOR_DATA |
| CMP-0026 | Walmart Canada | cold_chain_break | medium | False | No sensor excursion recorded — quality a... | INVESTIGATE |
| CMP-0027 | Sobeys | cold_chain_break | high | False | No sensor excursion recorded — quality a... | INVESTIGATE |
| CMP-0028 | Metro | freezer_burn | high | False | No sensor excursion recorded — quality a... | NO_SENSOR_DATA |

## Governance

- **Inference model:** `databricks-fmapi` (Databricks Foundation Model APIs)
- **Functions used:** `ai_classify`, `ai_extract` (built-in SQL AI functions)
- **No external LLM calls** — all inference runs through Databricks' governed model serving
- **Audit trail:** Every inference has `inference_ts`, `inference_model`, `inference_functions_used`, `audit_status`
- **Lineage:** Each complaint is joined to sensor data via `shipment_id`, creating a closed-loop from complaint → AI classification → sensor verification
