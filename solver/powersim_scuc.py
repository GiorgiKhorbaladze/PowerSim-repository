"""
PowerSim v4.0 — Security-constrained UC (N-1)  (#7)
====================================================

Adds N-1 contingency screening to the dispatch decision:

    For every contingency c in input.contingencies:
        For every period t:
            sufficient_post_outage_capacity(t, c) ≥ load(t)
            spinning_reserve(t, c) ≥ shortfall(t, c)
            (and per-line capacity if DC-OPF is in use — see Phase 6ε)

This module is intentionally a *post-solve verifier* + an explicit
*pre-curtail* heuristic so it works with both HiGHS (no callbacks) and
Gurobi (could be lazy-cuts later).  Strategy:

  1. Run the regular UC/ED solve.
  2. For each contingency × period:
       Compute available capacity if the contingency removes its
       elements (a generator, a transmission line, an import link).
       If post-outage available capacity < load, flag the period.
  3. If `enforce` is True, optionally re-solve with that period's
     online-units forced to a higher pmin (or with extra spinning
     reserve) so the post-outage state is feasible.

Contingency taxonomy (input.contingencies):

    [
      {"id":"loss_engurhesi", "kind":"unit_outage",
       "elements":["engurhesi"], "include_inflow_replacement":false},
      {"id":"loss_import_tr",  "kind":"import_outage",
       "elements":["import_tr"]},
      {"id":"loss_TBL_KTL",    "kind":"line_outage",
       "elements":["TBL_KTL_500"]}        # used by DC-OPF in Phase 6ε
    ]

This MVP focuses on `unit_outage` and `import_outage` (no transmission
network yet — line outage stub returns no-op until Phase 6ε wires it).

Output (under `diagnostics.scuc`):

    {
      "method":              "post_solve_verify",
      "n_contingencies":     <int>,
      "violations": [
        {"contingency":"loss_engurhesi", "period":42,
         "post_outage_capacity_mw": 1840.0,
         "load_mw": 2010.5, "shortfall_mw": 170.5},
        ...
      ],
      "violation_count":     <int>,
      "worst_shortfall_mw":  <float>,
      "spinning_reserve_recommendation_mw": <float>
    }
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
for p in (ROOT / "schema", ROOT / "solver", ROOT):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))


def _capacity_at(asset: dict, t: int, profiles: dict) -> float:
    """Conservative pmax estimate for an asset at hour t."""
    typ = asset.get("type")
    if typ in ("wind", "solar"):
        ak = asset.get("availability_profile")
        cf = (profiles.get(ak) or [0.0])
        cf = cf[t] if isinstance(cf, list) and t < len(cf) else 0.0
        return float(asset.get("pmax_installed", 0) or 0) * max(0.0, min(1.0, float(cf)))
    if typ == "import":
        pp = asset.get("pmax_profile")
        if isinstance(pp, list) and t < len(pp): return float(pp[t])
        return float(pp or 0)
    if typ == "bess":
        return float(asset.get("power_mw", 0) or 0)
    if typ == "pumped_hydro":
        return float(asset.get("pmax", 0) or 0)
    return float(asset.get("pmax", 0) or 0)


def verify(inp: dict, results: dict) -> dict:
    """Run the post-solve N-1 verification on a results JSON."""
    cont_list = inp.get("contingencies") or []
    if not cont_list:
        return {"method":"post_solve_verify", "n_contingencies":0,
                "violations":[], "violation_count":0,
                "worst_shortfall_mw":0,
                "spinning_reserve_recommendation_mw":0,
                "note":"no contingencies declared"}

    asset_by_id = {a["id"]: a for a in (inp.get("assets") or [])}
    profiles    = inp.get("profiles") or {}
    hs          = results.get("hourly_system") or []
    hbu         = results.get("hourly_by_unit") or {}
    H           = len(hs)
    if H == 0:
        return {"method":"post_solve_verify", "n_contingencies":len(cont_list),
                "violations":[], "violation_count":0,
                "worst_shortfall_mw":0, "spinning_reserve_recommendation_mw":0,
                "error":"no hourly_system in results"}

    violations = []
    worst = 0.0
    for c in cont_list:
        kind = c.get("kind", "unit_outage")
        cid  = c.get("id", "?")
        out_ids = list(c.get("elements") or [])
        if kind == "line_outage":
            # Transmission outages are handled by DC-OPF (Phase 6ε); MVP no-op.
            continue
        for t in range(H):
            row = hs[t]
            load = float(row.get("load_mw") or 0)
            # Sum of available pmax across all NON-outaged assets
            # (committable units must also be online to count, so we use
            # actual dispatch as a lower bound and pmax for non-committable)
            avail = 0.0
            for aid, a in asset_by_id.items():
                if aid in out_ids: continue
                if a.get("_committable", a.get("type") in ("thermal", "hydro_reg")):
                    # Use post-solve commitment * pmax (i.e., currently
                    # online unit can ramp up to pmax).
                    com = (hbu.get(aid) or [{}])[t].get("commitment", 0)
                    if com:
                        avail += float(a.get("pmax", 0) or 0)
                else:
                    avail += _capacity_at(a, t, profiles)
            shortfall = max(0.0, load - avail)
            if shortfall > 0.5:    # 0.5 MW tolerance
                violations.append({
                    "contingency":   cid,
                    "kind":          kind,
                    "period":        t,
                    "load_mw":       round(load, 2),
                    "post_outage_capacity_mw": round(avail, 2),
                    "shortfall_mw":  round(shortfall, 2),
                })
                if shortfall > worst: worst = shortfall

    # Suggest extra spinning-reserve = worst shortfall + 10% safety.
    rec = round(worst * 1.10, 1) if worst > 0 else 0.0
    return {
        "method":               "post_solve_verify",
        "n_contingencies":      len(cont_list),
        "violations":           violations,
        "violation_count":      len(violations),
        "worst_shortfall_mw":   round(worst, 2),
        "spinning_reserve_recommendation_mw": rec,
    }


# ══════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input",   required=True, help="powersim_input.json")
    ap.add_argument("--results", required=True, help="powersim_results.json")
    ap.add_argument("--out",     default="scuc_report.json")
    args = ap.parse_args()

    inp = json.loads(Path(args.input).read_text(encoding="utf-8"))
    res = json.loads(Path(args.results).read_text(encoding="utf-8"))
    rep = verify(inp, res)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(rep, ensure_ascii=False, indent=2),
                              encoding="utf-8")

    print("\n══════════════════════════════════════════════════════════════════════")
    print("  N-1 SCUC verification")
    print("══════════════════════════════════════════════════════════════════════")
    print(f"  contingencies tested: {rep['n_contingencies']}")
    print(f"  violations:           {rep['violation_count']}")
    print(f"  worst shortfall:      {rep['worst_shortfall_mw']} MW")
    print(f"  recommended extra spinning reserve: {rep['spinning_reserve_recommendation_mw']} MW")
    if rep['violations']:
        print("  first 5 violations:")
        for v in rep['violations'][:5]:
            print(f"    h{v['period']:>4}  {v['contingency']:<24}  "
                  f"load {v['load_mw']:>8.1f} MW  avail {v['post_outage_capacity_mw']:>8.1f}  "
                  f"shortfall {v['shortfall_mw']:>7.1f}")
    print(f"\n💾 wrote {args.out}")
