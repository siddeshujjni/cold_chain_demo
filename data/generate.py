"""
McCain Cold Chain Demo — synthetic data generator.

Produces two families of data:
  - IoT / OLTP  (destined for Lakebase / Postgres):
      trailers, reefer_telemetry, door_events, gps_pings
  - Analytical  (destined for Unity Catalog / Delta):
      carriers, retailer_dcs, product_master, shipments, customer_complaints

Seeded excursion scenarios (so the demo narrative always works):
  1. Door stuck open at Montreal consolidation stop (shipments SHP-0031..0033)
  2. Reefer unit failure on trailer T-007 (compressor intermittent)
  3. Ambient heat spike during afternoon stop (shipment SHP-0078)

Run standalone to emit CSVs under ./out/  — the same logic is inlined
into notebook 00_setup so the demo is self-contained.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

SEED = 42
random.seed(SEED)

# ---------------------------------------------------------------------------
# Reference / dimension data
# ---------------------------------------------------------------------------

CARRIERS = [
    ("CAR-01", "NorthStar Logistics",   "Canada", 0.92),
    ("CAR-02", "Maple Freight Systems", "Canada", 0.88),
    ("CAR-03", "TransCan Cold Haul",    "Canada", 0.81),
    ("CAR-04", "Polar Express Freight", "Canada", 0.95),
    ("CAR-05", "Atlantic Reefer Co.",   "Canada", 0.79),
]

RETAILER_DCS = [
    ("DC-LOB-01", "Loblaws",       "Mississauga, ON",   43.5890, -79.6441),
    ("DC-SOB-01", "Sobeys",        "Mississauga, ON",   43.6010, -79.6700),
    ("DC-MET-01", "Metro",         "Toronto, ON",       43.7182, -79.5430),
    ("DC-WMT-01", "Walmart Canada","Cornwall, ON",      45.0280, -74.7290),
    ("DC-CST-01", "Costco Canada", "Ottawa, ON",        45.3140, -75.7680),
    ("DC-SVN-01", "Save-On-Foods", "Calgary, AB",       51.1500, -114.0500),
    ("DC-CCH-01", "Couche-Tard",   "Laval, QC",         45.6000, -73.7120),
    ("DC-WHL-01", "Whole Foods",   "Vaughan, ON",       43.8400, -79.5300),
]

ORIGIN_PLANT = ("Florenceville-Bristol, NB", 46.4420, -67.6140)

# 50 SKU product master — weight toward frozen core (fries / hash browns / appetizers)
PRODUCT_FAMILIES = [
    ("Fries",        "FRY", -18.0),
    ("Hash Browns",  "HBR", -18.0),
    ("Appetizers",   "APP", -18.0),
    ("Pizza",        "PZA", -18.0),
    ("Desserts",     "DES", -20.0),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts_range(start: datetime, end: datetime, step: timedelta):
    t = start
    while t < end:
        yield t
        t += step


def _noisy(center: float, sd: float) -> float:
    return center + random.gauss(0, sd)


def _reefer_model(i: int) -> str:
    return random.choice(["TRU-2034", "TRU-2100X", "ColdGuard-450", "ArcticPro-9"])


# ---------------------------------------------------------------------------
# Dimension generators
# ---------------------------------------------------------------------------

def gen_carriers() -> pd.DataFrame:
    return pd.DataFrame(CARRIERS, columns=["carrier_id", "carrier_name", "country", "on_time_rate_baseline"])


def gen_retailer_dcs() -> pd.DataFrame:
    return pd.DataFrame(
        RETAILER_DCS,
        columns=["dc_id", "retailer_name", "location", "lat", "lon"],
    )


def gen_product_master(n: int = 50) -> pd.DataFrame:
    rows = []
    for i in range(n):
        fam, code, target_c = random.choice(PRODUCT_FAMILIES)
        sku = f"MC-{code}-{1000 + i:04d}"
        rows.append({
            "sku": sku,
            "product_family": fam,
            "description": f"{fam} — grade {random.choice(['A','B','premium'])} / pack {random.choice(['2x2.27kg','6x1kg','24x340g'])}",
            "target_temp_c": target_c,
            "upper_tolerance_c": target_c + 3.0,
            "lower_tolerance_c": target_c - 5.0,
            "unit_value_usd": round(random.uniform(18.0, 65.0), 2),
            "shelf_life_days": random.choice([180, 270, 365, 540]),
        })
    return pd.DataFrame(rows)


def gen_trailers(n: int = 15) -> pd.DataFrame:
    rows = []
    for i in range(n):
        trailer_id = f"T-{i+1:03d}"
        rows.append({
            "trailer_id": trailer_id,
            "carrier_id": random.choice([c[0] for c in CARRIERS]),
            "reefer_model": _reefer_model(i),
            "capacity_pallets": random.choice([24, 26, 30]),
            "install_date": (datetime(2022, 1, 1) + timedelta(days=random.randint(0, 900))).date().isoformat(),
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Shipments (analytical, but drives IoT scenario windows)
# ---------------------------------------------------------------------------

@dataclass
class Shipment:
    shipment_id: str
    trailer_id: str
    carrier_id: str
    sku: str
    dest_dc_id: str
    dest_lat: float
    dest_lon: float
    pallets: int
    ship_ts: datetime
    arrival_ts: datetime
    required_temp_c: float
    # Scenario hint for telemetry generator
    scenario: str  # 'normal' | 'door_open' | 'reefer_fail' | 'heat_spike'


def gen_shipments(
    trailers: pd.DataFrame,
    products: pd.DataFrame,
    n: int = 120,
    window_start: datetime | None = None,
) -> tuple[pd.DataFrame, list[Shipment]]:
    if window_start is None:
        window_start = datetime(2026, 4, 13, 0, 0, tzinfo=timezone.utc)

    shipments: list[Shipment] = []
    dc_lookup = {r.dc_id: r for r in gen_retailer_dcs().itertuples()}

    for i in range(n):
        sku = random.choice(products["sku"].tolist())
        prow = products.loc[products.sku == sku].iloc[0]
        trailer = trailers.sample(1).iloc[0]
        dc_id = random.choice([d[0] for d in RETAILER_DCS])
        dc = dc_lookup[dc_id]

        ship_ts = window_start + timedelta(
            hours=random.randint(0, 6 * 24 - 8),
            minutes=random.randint(0, 59),
        )
        transit_hours = random.randint(6, 36)
        arrival_ts = ship_ts + timedelta(hours=transit_hours)

        shipments.append(Shipment(
            shipment_id=f"SHP-{i+1:04d}",
            trailer_id=trailer.trailer_id,
            carrier_id=trailer.carrier_id,
            sku=sku,
            dest_dc_id=dc_id,
            dest_lat=dc.lat,
            dest_lon=dc.lon,
            pallets=random.randint(8, int(trailer.capacity_pallets)),
            ship_ts=ship_ts,
            arrival_ts=arrival_ts,
            required_temp_c=float(prow.target_temp_c),
            scenario="normal",
        ))

    # ---- seed 3 excursion scenarios against known shipments ----
    # Reefer unit failure: all shipments on trailer T-007 in the window
    for s in shipments:
        if s.trailer_id == "T-007":
            s.scenario = "reefer_fail"

    # Door stuck open at Montreal consolidation stop (3 non-T007 shipments)
    door_candidates = [s for s in shipments if s.trailer_id != "T-007"][:3]
    for s in door_candidates:
        s.scenario = "door_open"

    # Ambient heat spike (one distinct shipment, not already scenario-tagged)
    for s in shipments:
        if s.scenario == "normal":
            s.scenario = "heat_spike"
            break

    df = pd.DataFrame([{
        "shipment_id": s.shipment_id,
        "trailer_id": s.trailer_id,
        "carrier_id": s.carrier_id,
        "sku": s.sku,
        "origin_plant": ORIGIN_PLANT[0],
        "dest_dc_id": s.dest_dc_id,
        "pallets": s.pallets,
        "ship_ts": s.ship_ts,
        "arrival_ts": s.arrival_ts,
        "required_temp_c": s.required_temp_c,
        "scenario_seed": s.scenario,
    } for s in shipments])
    return df, shipments


# ---------------------------------------------------------------------------
# IoT telemetry (the main event)
# ---------------------------------------------------------------------------

def _telemetry_for_shipment(s: Shipment, step_min: int = 1) -> list[dict]:
    """Emit per-minute reefer telemetry covering the shipment window + 30-min pre/post tail."""
    start = s.ship_ts - timedelta(minutes=30)
    end = s.arrival_ts + timedelta(minutes=30)
    required = s.required_temp_c
    rows: list[dict] = []

    # Scenario window anchor: ~midway through transit
    mid = s.ship_ts + (s.arrival_ts - s.ship_ts) / 2

    for ts in _ts_range(start, end, timedelta(minutes=step_min)):
        # baseline
        supply = _noisy(required, 0.4)
        return_air = supply + _noisy(1.0, 0.3)
        set_point = required
        compressor_on = True
        fuel_level = max(15.0, 95.0 - (ts - start).total_seconds() / 3600 * 0.6)
        alarm = None

        # ---- scenario overlays ----
        if s.scenario == "door_open":
            # 45-min door open window starting ~1h into transit
            door_start = s.ship_ts + timedelta(hours=1)
            door_end = door_start + timedelta(minutes=45)
            if door_start <= ts <= door_end:
                elapsed = (ts - door_start).total_seconds() / 60
                supply = required + elapsed * 0.25 + _noisy(0, 0.4)
                return_air = supply + _noisy(2.5, 0.5)
                alarm = "DOOR_OPEN" if elapsed > 5 else None
            elif ts > door_end and ts < door_end + timedelta(minutes=60):
                # slow recovery
                recovery = (ts - door_end).total_seconds() / 60
                peak = required + 45 * 0.25
                supply = peak - recovery * 0.3 + _noisy(0, 0.3)
                return_air = supply + _noisy(1.5, 0.4)

        elif s.scenario == "reefer_fail":
            # Intermittent compressor — 3 cycles of 20-min off / 25-min on
            cyc = int((ts - s.ship_ts).total_seconds() // (45 * 60))
            phase = (ts - s.ship_ts).total_seconds() % (45 * 60)
            if cyc in (2, 4, 6) and phase < 20 * 60:
                compressor_on = False
                drift = phase / 60 * 0.35
                supply = required + drift + _noisy(0, 0.5)
                return_air = supply + _noisy(1.2, 0.4)
                alarm = "COMP_FAULT" if drift > 2 else None

        elif s.scenario == "heat_spike":
            # 90-min afternoon stop, compressor struggles
            if mid - timedelta(minutes=45) <= ts <= mid + timedelta(minutes=45):
                elapsed = abs((ts - mid).total_seconds()) / 60
                bump = max(0, (45 - elapsed)) * 0.12
                supply = required + bump + _noisy(0, 0.3)
                return_air = supply + _noisy(1.5, 0.3)
                alarm = "HIGH_AMBIENT" if bump > 3 else None

        rows.append({
            "trailer_id": s.trailer_id,
            "shipment_id": s.shipment_id,
            "ts": ts,
            "set_point_c": round(set_point, 2),
            "supply_air_c": round(supply, 2),
            "return_air_c": round(return_air, 2),
            "compressor_on": compressor_on,
            "fuel_level_pct": round(fuel_level, 1),
            "alarm_code": alarm,
        })
    return rows


def gen_reefer_telemetry(shipments: list[Shipment], step_min: int = 1) -> pd.DataFrame:
    all_rows: list[dict] = []
    for s in shipments:
        all_rows.extend(_telemetry_for_shipment(s, step_min=step_min))
    return pd.DataFrame(all_rows)


def gen_door_events(shipments: list[Shipment]) -> pd.DataFrame:
    events = []
    for s in shipments:
        # Always a load-door-open at origin and a receive-door-open at dest
        events.append({
            "event_id": f"DR-{s.shipment_id}-LD",
            "trailer_id": s.trailer_id,
            "shipment_id": s.shipment_id,
            "ts": s.ship_ts - timedelta(minutes=20),
            "event": "door_open",
            "location": "Florenceville Plant",
        })
        events.append({
            "event_id": f"DR-{s.shipment_id}-LC",
            "trailer_id": s.trailer_id,
            "shipment_id": s.shipment_id,
            "ts": s.ship_ts - timedelta(minutes=5),
            "event": "door_close",
            "location": "Florenceville Plant",
        })
        events.append({
            "event_id": f"DR-{s.shipment_id}-UD",
            "trailer_id": s.trailer_id,
            "shipment_id": s.shipment_id,
            "ts": s.arrival_ts + timedelta(minutes=3),
            "event": "door_open",
            "location": f"{s.dest_dc_id} receiving",
        })
        if s.scenario == "door_open":
            door_start = s.ship_ts + timedelta(hours=1)
            events.append({
                "event_id": f"DR-{s.shipment_id}-C1",
                "trailer_id": s.trailer_id,
                "shipment_id": s.shipment_id,
                "ts": door_start,
                "event": "door_open",
                "location": "Montreal consolidation stop",
            })
            events.append({
                "event_id": f"DR-{s.shipment_id}-C2",
                "trailer_id": s.trailer_id,
                "shipment_id": s.shipment_id,
                "ts": door_start + timedelta(minutes=45),
                "event": "door_close",
                "location": "Montreal consolidation stop",
            })
    return pd.DataFrame(events)


def gen_gps_pings(shipments: list[Shipment], step_min: int = 5) -> pd.DataFrame:
    rows = []
    for s in shipments:
        total_min = int((s.arrival_ts - s.ship_ts).total_seconds() // 60)
        steps = max(1, total_min // step_min)
        for i in range(steps + 1):
            f = i / max(steps, 1)
            lat = ORIGIN_PLANT[1] + (s.dest_lat - ORIGIN_PLANT[1]) * f + _noisy(0, 0.02)
            lon = ORIGIN_PLANT[2] + (s.dest_lon - ORIGIN_PLANT[2]) * f + _noisy(0, 0.02)
            rows.append({
                "trailer_id": s.trailer_id,
                "shipment_id": s.shipment_id,
                "ts": s.ship_ts + timedelta(minutes=i * step_min),
                "lat": round(lat, 5),
                "lon": round(lon, 5),
                "speed_kmh": round(max(0, random.gauss(85, 15)) if 0 < f < 1 else 0, 1),
            })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Customer complaints (unstructured text)
# ---------------------------------------------------------------------------

COMPLAINT_TEMPLATES = [
    ("Freezer burn on fries batch", "We received shipment {shipment_id} at {dc} yesterday. Opened a case of {family} and found obvious ice crystals and freezer burn. This is a quality issue — pallet smelled off-gas as well. Please investigate."),
    ("Product thawed on arrival",   "Hi team — shipment {shipment_id} arrived at our {dc} DC with three pallets that were clearly partially thawed. Received temp gun reading was around -8C on the outside cases. We'll be submitting a chargeback."),
    ("Texture issue, multiple SKUs","Customer complaints at retail on {family} from shipment {shipment_id}. Soggy texture after frying, suggests temperature abuse in transit. Batch code traces to load delivered to {dc}."),
    ("Suspected cold chain break",  "Our receivers at {dc} flagged shipment {shipment_id} — reefer temp readout on arrival showed -11C, well above the -18C spec. Requesting full cold chain trace."),
    ("Ice formation inside packs",  "Shipment {shipment_id} delivered to {dc} — opened retail packs showing heavy ice formation. This typically indicates partial thaw and refreeze. Need root cause before next PO."),
]


def gen_complaints(shipments: list[Shipment], products: pd.DataFrame) -> pd.DataFrame:
    # Target complaints: all seeded excursion shipments + some random noise
    excursion_ids = [s.shipment_id for s in shipments if s.scenario != "normal"]
    noise_ids = [s.shipment_id for s in shipments if s.scenario == "normal"]
    target_ids = excursion_ids + random.sample(noise_ids, k=min(18, len(noise_ids)))

    sku_family = dict(zip(products.sku, products.product_family))
    dc_name = {r[0]: r[1] for r in RETAILER_DCS}
    ship_lookup = {s.shipment_id: s for s in shipments}

    rows = []
    for sid in target_ids:
        s = ship_lookup[sid]
        subject, body = random.choice(COMPLAINT_TEMPLATES)
        rows.append({
            "complaint_id": f"CMP-{len(rows)+1:04d}",
            "received_ts": s.arrival_ts + timedelta(days=random.randint(1, 9), hours=random.randint(0, 23)),
            "retailer": dc_name.get(s.dest_dc_id, "Unknown"),
            "shipment_id_mentioned": sid,
            "subject": subject,
            "body": body.format(shipment_id=sid, dc=dc_name.get(s.dest_dc_id, "our DC"), family=sku_family.get(s.sku, "product")),
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def build_all(out_dir: Path | None = None, telemetry_step_min: int = 1) -> dict[str, pd.DataFrame]:
    carriers_df = gen_carriers()
    dcs_df = gen_retailer_dcs()
    products_df = gen_product_master()
    trailers_df = gen_trailers()
    shipments_df, shipments = gen_shipments(trailers_df, products_df)
    telemetry_df = gen_reefer_telemetry(shipments, step_min=telemetry_step_min)
    doors_df = gen_door_events(shipments)
    gps_df = gen_gps_pings(shipments)
    complaints_df = gen_complaints(shipments, products_df)

    out = {
        "carriers": carriers_df,
        "retailer_dcs": dcs_df,
        "product_master": products_df,
        "trailers": trailers_df,
        "shipments": shipments_df,
        "reefer_telemetry": telemetry_df,
        "door_events": doors_df,
        "gps_pings": gps_df,
        "customer_complaints": complaints_df,
    }

    if out_dir is not None:
        out_dir = Path(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        for name, df in out.items():
            df.to_csv(out_dir / f"{name}.csv", index=False)

    return out


if __name__ == "__main__":
    result = build_all(out_dir=Path(__file__).parent / "out")
    for name, df in result.items():
        print(f"{name:<22} {len(df):>8,} rows")
