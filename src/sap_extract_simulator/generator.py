"""Simulates SAP ODP-style delta extracts, the kind of files an extractor such as
AecorSoft or SAP Datasphere replication flows lands in cloud storage.

Tables (real SAP names, simplified column sets):
    KNA1  customer master        -> SCD Type 2 downstream
    MARA  material master (+MAKT) -> SCD Type 1 downstream
    VBAK  sales document header   -> transactional, CDC
    VBAP  sales document item     -> transactional, CDC

Every row carries ODP delta metadata:
    ODQ_CHANGEMODE  C = create, U = update (after image), D = delete
    ODQ_ENTITYCNTR  +1 / -1 (record counter, as in ODP)
    EXTRACT_TS      when the delta was extracted (used as the CDC sequence)

SAP quirks reproduced on purpose, because real SAP data has them:
    * zero-padded keys (KUNNR 10 chars, MATNR 18 chars) that must stay strings
    * '00000000' meaning "no date"
    * dates as YYYYMMDD strings, times as HHMMSS strings
    * bad rows: missing keys, negative quantities, items pointing at a material
      that only arrives in the next extract (late-arriving dimension)

The simulator is deterministic: batch N is always rebuilt by replaying batches
0..N from the same seed, so re-running a batch reproduces the same files.
"""

from __future__ import annotations

import csv
import os
import random
from dataclasses import dataclass
from datetime import datetime, timedelta

ODP_COLUMNS = ["ODQ_CHANGEMODE", "ODQ_ENTITYCNTR", "EXTRACT_TS"]

COLUMNS: dict[str, list[str]] = {
    "KNA1": ["KUNNR", "NAME1", "ORT01", "REGIO", "LAND1", "KTOKD", "ERDAT", *ODP_COLUMNS],
    "MARA": ["MATNR", "MAKTX", "MTART", "MATKL", "MEINS", "BRGEW", "ERSDA", *ODP_COLUMNS],
    "VBAK": ["VBELN", "AUART", "VKORG", "KUNNR", "ERDAT", "ERZET", "WAERK", *ODP_COLUMNS],
    "VBAP": ["VBELN", "POSNR", "MATNR", "WERKS", "KWMENG", "VRKME", "NETWR", "WAERK", *ODP_COLUMNS],
}

BASE_DATE = datetime(2026, 1, 1)

# Completion markers live outside the table folders, so Auto Loader never reads them.
MARKER_DIR = "_batches"

# (city, region code) pairs, Indian states, LAND1 = IN
CITIES = [
    ("Mumbai", "MH"), ("Pune", "MH"), ("Nagpur", "MH"), ("Bengaluru", "KA"),
    ("Mysuru", "KA"), ("Chennai", "TN"), ("Coimbatore", "TN"), ("Hyderabad", "TS"),
    ("Ahmedabad", "GJ"), ("Surat", "GJ"), ("Delhi", "DL"), ("Kolkata", "WB"),
    ("Jaipur", "RJ"), ("Lucknow", "UP"), ("Kochi", "KL"),
]
NAME_PREFIX = ["Shree", "Sai", "Om", "Laxmi", "Ganesh", "Royal", "City", "Metro", "Prime", "Sun"]
NAME_CORE = ["Traders", "Mart", "Distributors", "Retail", "Foods", "Stores", "Agencies", "Wholesale"]
ACCOUNT_GROUPS = ["Z001", "Z002", "Z003"]  # e.g. key account, retail, distributor
MATERIAL_TYPES = ["FERT", "HAWA"]  # finished good, trading good
MATERIAL_GROUPS = {
    "CEREAL": ["Oats", "Corn Flakes", "Muesli", "Granola"],
    "SNACKS": ["Nachos", "Trail Mix", "Protein Bar", "Popcorn"],
    "BAKING": ["Cake Mix", "Pancake Mix", "Flour 1kg", "Brownie Mix"],
    "DAIRY": ["Yogurt Cup", "Cheese Slice", "Butter 500g", "Paneer 200g"],
}
PLANTS = ["IN01", "IN02", "IN03"]
SALES_ORGS = ["1000", "2000"]
ORDER_TYPES = ["ZOR", "ZRE"]  # standard order, returns


@dataclass(frozen=True)
class SimConfig:
    initial_customers: int = 200
    initial_materials: int = 48
    orders_per_batch: int = 80
    new_customers_per_batch: int = 5
    new_materials_per_batch: int = 2
    customer_update_rate: float = 0.08
    customer_delete_rate: float = 0.01
    material_update_rate: float = 0.05
    item_update_rate: float = 0.05
    item_cancel_rate: float = 0.02
    bad_row_rate: float = 0.01


def master_extract_ts(batch_id: int) -> datetime:
    """Master data deltas (KNA1, MARA) are extracted at 00:00 on day N."""
    return BASE_DATE + timedelta(days=batch_id)


def transaction_extract_ts(batch_id: int) -> datetime:
    """Sales deltas (VBAK, VBAP) are extracted at 23:00 on day N."""
    return BASE_DATE + timedelta(days=batch_id, hours=23)


def _fmt_ts(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def _kunnr(n: int) -> str:
    return f"{n:010d}"


def _matnr(n: int) -> str:
    return f"{n:018d}"


class _State:
    """Current source-system state, mutated batch by batch."""

    def __init__(self, cfg: SimConfig, seed: int):
        self.cfg = cfg
        self.rng = random.Random(seed)
        self.customers: dict[str, dict] = {}
        self.materials: dict[str, dict] = {}
        self.items: dict[tuple[str, str], dict] = {}  # open order items
        self.next_customer = 100001
        self.next_material = 500001
        self.next_order = 1
        self.pending_late_material: str | None = None  # created in the next batch

    # ---------- row builders ----------
    def new_customer(self, day: datetime) -> dict:
        r = self.rng
        city, region = r.choice(CITIES)
        erdat = day.strftime("%Y%m%d")
        if r.random() < 0.03:
            erdat = "00000000"  # SAP's "no date"
        row = {
            "KUNNR": _kunnr(self.next_customer),
            "NAME1": f"{r.choice(NAME_PREFIX)} {r.choice(NAME_CORE)}, {city}",
            "ORT01": city,
            "REGIO": region,
            "LAND1": "IN",
            "KTOKD": r.choice(ACCOUNT_GROUPS),
            "ERDAT": erdat,
        }
        self.next_customer += 1
        return row

    def new_material(self, day: datetime, matnr: str | None = None) -> dict:
        r = self.rng
        group = r.choice(list(MATERIAL_GROUPS))
        if matnr is None:
            matnr = _matnr(self.next_material)
            self.next_material += 1
        return {
            "MATNR": matnr,
            "MAKTX": f"{r.choice(MATERIAL_GROUPS[group])} {r.choice(['Classic', 'Lite', 'Family Pack', 'Mini'])}",
            "MTART": r.choice(MATERIAL_TYPES),
            "MATKL": group,
            "MEINS": "EA",
            "BRGEW": f"{r.uniform(0.1, 2.5):.3f}",
            "ERSDA": day.strftime("%Y%m%d"),
        }


def _tag(row: dict, mode: str, ts: datetime) -> dict:
    out = dict(row)
    out["ODQ_CHANGEMODE"] = mode
    out["ODQ_ENTITYCNTR"] = "-1" if mode == "D" else "1"
    out["EXTRACT_TS"] = _fmt_ts(ts)
    return out


def _step(state: _State, batch_id: int) -> dict[str, list[dict]]:
    cfg, r = state.cfg, state.rng
    m_ts, t_ts = master_extract_ts(batch_id), transaction_extract_ts(batch_id)
    day = m_ts
    out: dict[str, list[dict]] = {t: [] for t in COLUMNS}

    # ---------------- KNA1 ----------------
    if batch_id == 0:
        for _ in range(cfg.initial_customers):
            c = state.new_customer(day)
            state.customers[c["KUNNR"]] = c
            out["KNA1"].append(_tag(c, "C", m_ts))
    else:
        keys = sorted(state.customers)
        touched = set()
        for k in keys:
            if r.random() < cfg.customer_delete_rate:
                deleted = state.customers.pop(k)
                out["KNA1"].append(_tag(deleted, "D", m_ts))
                touched.add(k)
        for k in sorted(state.customers):
            if k in touched or r.random() >= cfg.customer_update_rate:
                continue
            c = dict(state.customers[k])
            if r.random() < 0.6:  # relocation -> history worth keeping (SCD2)
                c["ORT01"], c["REGIO"] = r.choice([x for x in CITIES if x[0] != c["ORT01"]])
            else:  # reclassification
                c["KTOKD"] = r.choice([g for g in ACCOUNT_GROUPS if g != c["KTOKD"]])
            state.customers[k] = c
            out["KNA1"].append(_tag(c, "U", m_ts))
        for _ in range(cfg.new_customers_per_batch):
            c = state.new_customer(day)
            state.customers[c["KUNNR"]] = c
            out["KNA1"].append(_tag(c, "C", m_ts))
        # one broken row per delta: key missing (should be dropped by expectations)
        bad = state.new_customer(day)
        state.next_customer -= 1  # do not burn a key on garbage
        bad["KUNNR"] = ""
        out["KNA1"].append(_tag(bad, "C", m_ts))

    # ---------------- MARA ----------------
    if batch_id == 0:
        for _ in range(cfg.initial_materials):
            m = state.new_material(day)
            state.materials[m["MATNR"]] = m
            out["MARA"].append(_tag(m, "C", m_ts))
    else:
        if state.pending_late_material:  # late-arriving dimension finally shows up
            m = state.new_material(day, matnr=state.pending_late_material)
            state.materials[m["MATNR"]] = m
            out["MARA"].append(_tag(m, "C", m_ts))
            state.pending_late_material = None
        for k in sorted(state.materials):
            if r.random() < cfg.material_update_rate and k not in {
                row["MATNR"] for row in out["MARA"]
            }:
                m = dict(state.materials[k])
                m["BRGEW"] = f"{float(m['BRGEW']) * r.uniform(0.9, 1.1):.3f}"
                state.materials[k] = m
                out["MARA"].append(_tag(m, "U", m_ts))
        for _ in range(cfg.new_materials_per_batch):
            m = state.new_material(day)
            state.materials[m["MATNR"]] = m
            out["MARA"].append(_tag(m, "C", m_ts))

    # ---------------- VBAP updates / cancellations of earlier items ----------------
    if batch_id > 0:
        for key in sorted(state.items):
            roll = r.random()
            item = state.items[key]
            if roll < cfg.item_cancel_rate:
                state.items.pop(key)
                out["VBAP"].append(_tag(item, "D", t_ts))
            elif roll < cfg.item_cancel_rate + cfg.item_update_rate:
                item = dict(item)
                qty = int(item["KWMENG"]) + r.randint(1, 5)
                price = float(item["NETWR"]) / max(int(item["KWMENG"]), 1)
                item["KWMENG"], item["NETWR"] = str(qty), f"{qty * price:.2f}"
                state.items[key] = item
                out["VBAP"].append(_tag(item, "U", t_ts))

    # ---------------- new orders (VBAK + VBAP) ----------------
    late_used = False
    customer_keys = sorted(state.customers)
    material_keys = sorted(state.materials)
    for _ in range(cfg.orders_per_batch):
        vbeln = f"{state.next_order:010d}"
        state.next_order += 1
        created = day + timedelta(seconds=r.randint(8 * 3600, 20 * 3600))
        waerk = "INR"
        header = {
            "VBELN": vbeln,
            "AUART": "ZRE" if r.random() < 0.03 else "ZOR",
            "VKORG": r.choice(SALES_ORGS),
            "KUNNR": r.choice(customer_keys),
            "ERDAT": created.strftime("%Y%m%d"),
            "ERZET": created.strftime("%H%M%S"),
            "WAERK": waerk,
        }
        out["VBAK"].append(_tag(header, "C", t_ts))
        for pos in range(1, r.randint(1, 5) + 1):
            qty = r.randint(1, 50)
            price = r.uniform(40, 900)
            item = {
                "VBELN": vbeln,
                "POSNR": f"{pos * 10:06d}",
                "MATNR": r.choice(material_keys),
                "WERKS": r.choice(PLANTS),
                "KWMENG": str(qty),
                "VRKME": "EA",
                "NETWR": f"{qty * price:.2f}",
                "WAERK": waerk,
            }
            roll = r.random()
            if roll < cfg.bad_row_rate:
                item["KWMENG"] = str(-qty)  # negative quantity -> quarantine
            elif roll < 2 * cfg.bad_row_rate:
                item["MATNR"] = ""  # missing material -> quarantine
            elif not late_used and batch_id > 0 and pos == 1:
                # material that MARA will only deliver in the next batch
                state.pending_late_material = _matnr(state.next_material)
                state.next_material += 1
                item["MATNR"] = state.pending_late_material
                late_used = True
            state.items[(vbeln, item["POSNR"])] = item
            out["VBAP"].append(_tag(item, "C", t_ts))

    return out


def generate_batch(batch_id: int, seed: int = 42, cfg: SimConfig | None = None) -> dict[str, list[dict]]:
    """Return the delta rows for one batch, keyed by SAP table name."""
    if batch_id < 0:
        raise ValueError("batch_id must be >= 0")
    state = _State(cfg or SimConfig(), seed)
    result: dict[str, list[dict]] = {}
    for b in range(batch_id + 1):
        result = _step(state, b)
    return result


def write_batch(batch_id: int, landing_root: str, seed: int = 42, cfg: SimConfig | None = None) -> dict[str, str]:
    """Write one batch as CSV files: <landing_root>/<TABLE>/extract_<NNNN>.csv.

    Writing the same batch twice produces identical files, so a retried job
    task is harmless.
    """
    rows = generate_batch(batch_id, seed, cfg)
    paths: dict[str, str] = {}
    for table, records in rows.items():
        folder = os.path.join(landing_root, table)
        os.makedirs(folder, exist_ok=True)
        path = os.path.join(folder, f"extract_{batch_id:04d}.csv")
        with open(path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=COLUMNS[table], quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()
            writer.writerows(records)
        paths[table] = path

    # Written LAST: its presence means all four tables of this batch are complete.
    marker = marker_path(landing_root, batch_id)
    os.makedirs(os.path.dirname(marker), exist_ok=True)
    open(marker, "w").close()
    return paths


def marker_path(landing_root: str, batch_id: int) -> str:
    """<landing_root>/_batches/batch_NNNN.done"""
    return os.path.join(landing_root, MARKER_DIR, f"batch_{batch_id:04d}.done")


def next_batch_id(landing_root: str) -> int:
    """Next batch to generate = highest COMPLETED batch + 1.

    Counts completion markers, not data files. A batch that crashed halfway has
    some data files but no marker, so it is generated again. That is safe because
    batches are deterministic and the files are simply overwritten.
    """
    folder = os.path.join(landing_root, MARKER_DIR)
    if not os.path.isdir(folder):
        return 0
    ids = [
        int(name[len("batch_"):-len(".done")])
        for name in os.listdir(folder)
        if name.startswith("batch_") and name.endswith(".done")
    ]
    return max(ids) + 1 if ids else 0