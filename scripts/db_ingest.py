"""
PowerSim v4.0 — SQL scenario store  (#15)
==========================================

Ingests any PowerSim results JSON into a single SQLite database
(`powersim_store.sqlite` by default) so multiple runs can be queried,
ranked, and compared with plain SQL.

Schema (auto-created on first run):

    CREATE TABLE runs (
        run_id                  TEXT PRIMARY KEY,
        scenario                TEXT,
        schema_version          TEXT,
        solver_version          TEXT,
        solved_at               TEXT,
        horizon_hours           INTEGER,
        resolution_min          INTEGER,
        total_cost_usd          REAL,
        total_energy_mwh        REAL,
        avg_lambda_usd_mwh      REAL,
        peak_load_mw            REAL,
        total_gas_mm3           REAL,
        total_unserved_mwh      REAL,
        total_curtailed_mwh     REAL,
        closure_ok              INTEGER,
        closure_gap             REAL,
        solve_time_s            REAL,
        tag                     TEXT,          -- user-supplied label
        source_path             TEXT
    );

    CREATE TABLE run_by_unit (
        run_id     TEXT,
        asset_id   TEXT,
        asset_type TEXT,
        energy_mwh REAL,
        cf_pct     REAL,
        gross_cost REAL,
        gas_mm3    REAL,
        starts     INTEGER,
        oper_hours REAL,
        PRIMARY KEY (run_id, asset_id),
        FOREIGN KEY (run_id) REFERENCES runs(run_id)
    );

    CREATE TABLE run_monthly (
        run_id   TEXT,
        month    INTEGER,
        label    TEXT,
        hours    REAL,
        total_energy_mwh REAL,
        total_cost_usd   REAL,
        avg_lambda       REAL,
        gas_mm3          REAL,
        unserved_mwh     REAL,
        PRIMARY KEY (run_id, month),
        FOREIGN KEY (run_id) REFERENCES runs(run_id)
    );

Usage:

    python scripts/db_ingest.py --results out/gse_720h/powersim_results.json --tag "gse-720h"
    python scripts/db_ingest.py --scan out/              # ingest every results file under out/
    python scripts/db_ingest.py --list                   # list all runs
    python scripts/db_ingest.py --compare <run_id1> <run_id2>
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from pathlib import Path

DB_DEFAULT = Path(__file__).resolve().parent.parent / "powersim_store.sqlite"


_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id                  TEXT PRIMARY KEY,
    scenario                TEXT,
    schema_version          TEXT,
    solver_version          TEXT,
    solved_at               TEXT,
    horizon_hours           INTEGER,
    resolution_min          INTEGER,
    total_cost_usd          REAL,
    total_energy_mwh        REAL,
    avg_lambda_usd_mwh      REAL,
    peak_load_mw            REAL,
    total_gas_mm3           REAL,
    total_unserved_mwh      REAL,
    total_curtailed_mwh     REAL,
    closure_ok              INTEGER,
    closure_gap             REAL,
    solve_time_s            REAL,
    tag                     TEXT,
    source_path             TEXT
);
CREATE TABLE IF NOT EXISTS run_by_unit (
    run_id     TEXT,
    asset_id   TEXT,
    asset_type TEXT,
    energy_mwh REAL,
    cf_pct     REAL,
    gross_cost REAL,
    gas_mm3    REAL,
    starts     INTEGER,
    oper_hours REAL,
    PRIMARY KEY (run_id, asset_id)
);
CREATE TABLE IF NOT EXISTS run_monthly (
    run_id   TEXT,
    month    INTEGER,
    label    TEXT,
    hours    REAL,
    total_energy_mwh REAL,
    total_cost_usd   REAL,
    avg_lambda       REAL,
    gas_mm3          REAL,
    unserved_mwh     REAL,
    PRIMARY KEY (run_id, month)
);
CREATE INDEX IF NOT EXISTS runs_scenario_idx ON runs(scenario);
CREATE INDEX IF NOT EXISTS runs_tag_idx      ON runs(tag);
"""


def _open(db_path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(str(db_path))
    con.executescript(_SCHEMA)
    return con


def _run_id(results: dict, source_path: Path | None = None) -> str:
    meta = results.get("metadata") or {}
    basis = f"{meta.get('scenario','?')}|{meta.get('horizon_hours','?')}|{meta.get('solved_at','?')}"
    if source_path: basis += f"|{source_path}"
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()[:12]


def ingest_one(db: sqlite3.Connection, results: dict, *,
               tag: str = "", source_path: Path | None = None) -> str:
    meta = results.get("metadata") or {}
    sm   = results.get("system_summary") or {}
    diag = results.get("diagnostics") or {}
    rid  = _run_id(results, source_path)

    db.execute("DELETE FROM runs        WHERE run_id = ?", (rid,))
    db.execute("DELETE FROM run_by_unit WHERE run_id = ?", (rid,))
    db.execute("DELETE FROM run_monthly WHERE run_id = ?", (rid,))

    db.execute("""
        INSERT INTO runs VALUES
            (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        rid,
        meta.get("scenario"),
        meta.get("schema_version"),
        diag.get("solver_version") or meta.get("solver_version"),
        meta.get("solved_at"),
        meta.get("horizon_hours"),
        meta.get("resolution_min", 60),
        sm.get("total_cost_usd"),
        sm.get("total_energy_mwh"),
        sm.get("avg_lambda_usd_mwh"),
        sm.get("peak_load_mw"),
        sm.get("total_gas_mm3"),
        sm.get("total_unserved_mwh"),
        sm.get("total_curtailed_mwh"),
        1 if meta.get("closure_ok") else 0,
        meta.get("closure_gap"),
        diag.get("solve_time_s"),
        tag,
        str(source_path) if source_path else None,
    ))
    for aid, bu in (results.get("by_unit_summary") or {}).items():
        db.execute("INSERT INTO run_by_unit VALUES (?,?,?,?,?,?,?,?,?)", (
            rid, aid, bu.get("type"),
            bu.get("energy_mwh"), bu.get("capacity_factor"),
            bu.get("gross_cost"), bu.get("gas_mm3"),
            bu.get("starts"), bu.get("oper_hours"),
        ))
    for m in results.get("monthly_summary") or []:
        db.execute("INSERT INTO run_monthly VALUES (?,?,?,?,?,?,?,?,?)", (
            rid, m.get("month"), m.get("label"), m.get("hours"),
            m.get("total_energy_mwh"), m.get("total_cost_usd"),
            m.get("avg_lambda"), m.get("gas_mm3"), m.get("unserved_mwh"),
        ))
    db.commit()
    return rid


def scan_and_ingest(db: sqlite3.Connection, root: Path, tag: str = "") -> list:
    ids = []
    for p in root.rglob("powersim_results.json"):
        try:
            res = json.loads(p.read_text(encoding="utf-8"))
            ids.append((str(p), ingest_one(db, res, tag=tag, source_path=p)))
        except Exception as e:
            print(f"   ! skip {p}: {e}")
    return ids


def list_runs(db: sqlite3.Connection) -> None:
    rows = db.execute("""
        SELECT run_id, scenario, horizon_hours, resolution_min,
               total_cost_usd, avg_lambda_usd_mwh, total_unserved_mwh,
               closure_ok, tag, solved_at
        FROM runs ORDER BY solved_at DESC
    """).fetchall()
    if not rows:
        print("(no runs in store)"); return
    print(f"{'run_id':<12} {'scenario':<10} {'hours':>6} {'res':>4} "
          f"{'cost_$':>14} {'λ̄_$/MWh':>9} {'unserved':>10} "
          f"{'closure':>7}  tag")
    for r in rows:
        rid, sc, h, rm, cost, lam, un, clo, tag, _ = r
        print(f"{rid:<12} {sc or '?':<10} {h or 0:>6} {rm or 60:>4} "
              f"{cost or 0:>14,.0f} {lam or 0:>9.2f} "
              f"{un or 0:>10.1f} {'ok' if clo else 'gap':>7}  {tag or ''}")


def compare(db: sqlite3.Connection, ids: list) -> None:
    q = f"""
        SELECT run_id, scenario, horizon_hours,
               total_cost_usd, avg_lambda_usd_mwh,
               total_unserved_mwh, total_gas_mm3, peak_load_mw
        FROM runs WHERE run_id IN ({','.join('?' * len(ids))})
    """
    rows = db.execute(q, ids).fetchall()
    for r in rows:
        print(r)


# ══════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════
def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=str(DB_DEFAULT))
    ap.add_argument("--results", help="Ingest a single results JSON.")
    ap.add_argument("--scan", help="Recursively ingest every powersim_results.json under PATH.")
    ap.add_argument("--tag", default="")
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--compare", nargs="+", help="List run_ids to dump side-by-side.")
    ap.add_argument("--query", help="Raw SQL — result printed as TSV.")
    args = ap.parse_args(argv)

    db = _open(Path(args.db))

    if args.results:
        res = json.loads(Path(args.results).read_text(encoding="utf-8"))
        rid = ingest_one(db, res, tag=args.tag, source_path=Path(args.results))
        print(f"✓ ingested → run_id={rid}")
    if args.scan:
        hits = scan_and_ingest(db, Path(args.scan), tag=args.tag)
        for src, rid in hits: print(f"   {rid}  {src}")
    if args.list:     list_runs(db)
    if args.compare:  compare(db, args.compare)
    if args.query:
        cur = db.execute(args.query)
        cols = [c[0] for c in cur.description or []]
        print("\t".join(cols))
        for r in cur.fetchall():
            print("\t".join(str(x) for x in r))
    return 0


if __name__ == "__main__":
    sys.exit(main())
