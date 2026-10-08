# Evidence: Genie Space — Natural-Language Interface

> **Generated:** 2026-10-08 UTC
> **Genie Space ID:** `01f1c3585b1118a2917715c29d89bf1c`
> **Genie Space Name:** McCain Cold Chain Operations
> **URL:** `/genie/rooms/01f1c3585b1118a2917715c29d89bf1c`

## What This Is

The Genie Space is the **conversational natural-language interface** over the governed
cold-chain tables. It enables the "ask a question in plain English and get an answer"
journey promised to Karen Macmillan and the McCain ops team — no SQL knowledge required.

> "Same data, same governance, but now the line-of-business person asks in English.
> This is where the McCain ops analyst lives, not in a notebook."

## Genie Space Configuration

| Property | Value |
|----------|-------|
| Space Name | McCain Cold Chain Operations |
| Space ID | `01f1c3585b1118a2917715c29d89bf1c` |
| Tables Included | 9 governed Unity Catalog tables |
| Auth | Databricks workspace SSO (same RBAC as all other layers) |

### Tables Backing the Genie Space

| Table | Purpose |
|-------|---------|
| `silver_shipment_profile` | Per-shipment rollup with excursion flags, transit hours, dollar values |
| `gold_excursion_events` | Detected excursion windows with severity, duration, dollars at risk |
| `gold_complaint_root_cause` | Complaint ↔ sensor root cause join |
| `gold_inference_log` | Governed AI inference audit trail |
| `carriers` | Carrier dimension (5 Canadian carriers) |
| `retailer_dcs` | Retailer distribution centers |
| `product_master` | Product catalog with temp tolerances |
| `alerts_rule_based` | Rule-based excursion alerts |
| `alerts_ml_based` | ML-based warm-trend alerts |

## Live Demo Evidence — 3 Natural-Language Questions Answered

### Question 1: Excursion Summary

**User asked:** _"How many shipments had cold-chain excursions and what is the total dollars at risk?"_

**Genie SQL:**
```sql
SELECT
  COUNT(DISTINCT shipment_id) as shipments_with_excursions,
  SUM(shipment_value_usd) as total_dollars_at_risk
FROM silver_shipment_profile
WHERE had_excursion = true
```

**Result:**

| Shipments with Excursions | Total Dollars at Risk |
|--------------------------|----------------------|
| 10 | $256,430 |

**Genie Analysis:** _"10 shipments experienced cold-chain excursions, with a total of $256,430 at risk."_

---

### Question 2: Carrier Scorecard

**User asked:** _"Which carriers have the highest excursion rate? Show excursion rate and dollars at risk."_

**Genie SQL:**
```sql
WITH carrier_shipments AS (
  SELECT sp.carrier_id, c.carrier_name,
    COUNT(*) as total_shipments,
    SUM(CASE WHEN sp.had_excursion = true THEN 1 ELSE 0 END) as excursion_shipments,
    ROUND(100.0 * SUM(CASE WHEN sp.had_excursion = true THEN 1 ELSE 0 END) / COUNT(*), 2) as excursion_rate_pct
  FROM silver_shipment_profile sp
  LEFT JOIN carriers c ON sp.carrier_id = c.carrier_id
  GROUP BY sp.carrier_id, c.carrier_name
),
carrier_risk AS (
  SELECT carrier_id, SUM(dollars_at_risk) as total_dollars_at_risk
  FROM gold_excursion_events GROUP BY carrier_id
)
SELECT cs.carrier_name, cs.total_shipments, cs.excursion_shipments,
       cs.excursion_rate_pct, COALESCE(cr.total_dollars_at_risk, 0) as total_dollars_at_risk
FROM carrier_shipments cs
LEFT JOIN carrier_risk cr ON cs.carrier_id = cr.carrier_id
ORDER BY cs.excursion_rate_pct DESC
```

**Result:**

| Carrier | Shipments | Excursions | Rate | $ at Risk |
|---------|-----------|------------|------|-----------|
| NorthStar Logistics | 35 | 8 | 22.86% | $220,204 |
| Polar Express Freight | 21 | 1 | 4.76% | $10,742 |
| Atlantic Reefer Co. | 30 | 1 | 3.33% | $4,557 |
| TransCan Cold Haul | 26 | 0 | 0.00% | $0 |
| Maple Freight Systems | 8 | 0 | 0.00% | $0 |

**Genie Analysis:** _"NorthStar Logistics has by far the highest excursion rate at 22.86%, with 8 out of 35 shipments experiencing temperature excursions and $220,204 in total dollars at risk. NorthStar accounts for 93.5% of all dollars at risk across carriers."_

---

### Question 3: AI + Sensor Correlation (Closed-Loop)

**User asked:** _"Show me customer complaints where the AI classified the root cause as cold_chain_break and the sensor data confirmed it."_

**Genie SQL:**
```sql
SELECT c.complaint_id, c.retailer, c.received_ts, c.alleged_cause,
       c.shipment_id, c.trailer_id, c.had_excursion, c.max_supply_c,
       c.minutes_above_tolerance, c.top_alarm, c.sensor_root_cause,
       i.ai_classification_result, i.sensor_confirmed
FROM gold_complaint_root_cause c
INNER JOIN gold_inference_log i ON c.complaint_id = i.complaint_id
WHERE i.ai_classification_result = 'cold_chain_break'
  AND i.sensor_confirmed = true
ORDER BY c.received_ts DESC
```

**Result:** 6 complaints returned

| Complaint | Retailer | Trailer | Alarm | Sensor Root Cause | Minutes Over |
|-----------|----------|---------|-------|-------------------|--------------|
| CMP-0003 | Costco Canada | T-010 | DOOR_OPEN | Door stuck open mid-transit | 61 |
| CMP-0004 | Sobeys | T-011 | HIGH_AMBIENT | Ambient heat spike during stop | 41 |
| CMP-0009 | Costco Canada | T-007 | COMP_FAULT | Compressor / reefer unit fault | 32 |
| CMP-0002 | Save-On-Foods | T-001 | DOOR_OPEN | Door stuck open mid-transit | 60 |
| CMP-0008 | Walmart Canada | T-007 | COMP_FAULT | Compressor / reefer unit fault | 33 |
| CMP-0006 | Metro | T-007 | COMP_FAULT | Compressor / reefer unit fault | 30 |

**Genie Analysis:** _"Found 6 customer complaints where the AI classified the root cause as cold_chain_break and sensor data confirmed the diagnosis. 100% had temperature excursions during transit, all rated as high severity. Trailer T-007 appeared in 3 complaints, all with compressor faults. CAR-01 had 4 of the 6 confirmed incidents (67%)."_

---

## What This Proves

1. **A real Genie Space exists** as a workspace artifact (ID: `01f1c3585b1118a2917715c29d89bf1c`)
2. **Natural-language questions produce correct SQL** against the governed gold tables
3. **The full ask-in-English → get-an-answer journey works end-to-end**
4. **Complex multi-table joins** (complaint ↔ inference log ↔ sensor data) are handled automatically
5. **Same governance** — Genie uses Unity Catalog RBAC, same as notebooks and dashboards
6. **Three question types demonstrated:**
   - Summary KPIs (excursion count + dollar risk)
   - Analytical ranking (carrier scorecard with calculated rates)
   - Cross-domain correlation (AI classification + sensor confirmation)

## Demo Questions for Stage

1. How many shipments had cold-chain excursions this week and what is the total dollars at risk?
2. Which carriers have the highest excursion rate? Show excursion rate and dollars at risk.
3. Which trailer has the most HIGH severity excursions, and what alarm codes fired?
4. Which retailer DCs are receiving the most shipments with sensor excursions?
5. Which product families are most affected by out-of-tolerance events?
6. Show me customer complaints where the sensor root cause was a door-open event.
7. On shipments where ML warned before the rule fired, how much earlier was the ML alert?
8. Show the AI inference audit log — which complaints did the model classify as high severity and what was the sensor confirmation rate?
