"""
PowerSim v4.0 — User-defined KPI Template Engine  (#19)
========================================================

Lets analysts declare reusable, named measures that apply to any results
JSON without touching Python code. Each template looks like:

    {
      "id":          "peak_hours_avg_lambda",
      "label":       "Avg λ during peak hours (17-22)",
      "formula":     "avg(lambda_usd_mwh | hour_of_day in [17,18,19,20,21,22])",
      "unit":        "$/MWh"
    }

Supported functions (case-insensitive, whitespace-tolerant):

    sum(<field> [ | <filter> ])
    avg(<field> [ | <filter> ])
    min(<field> [ | <filter> ])
    max(<field> [ | <filter> ])
    count(<field> | <filter>)
    p10(<field>), p50(…), p90(…)
    ratio(<num> / <den>)                  — computes num/den of the two fields
    by_unit_sum(<field>, <asset_id>)     — pulls from by_unit_summary
    hours_where(<filter>)                 — counts rows matching the filter

Filter DSL (intentionally tiny):

    <field> <op> <number>                 # op ∈ == != < <= > >=
    <field> in [<num>, <num>, ...]        # set-membership
    <field> between <num> and <num>

Fields available in each `hourly_system` row:

    t, hour_of_year, period_minutes, load_mw, generation_mw,
    lambda_usd_mwh, unserved_mwh, curtailed_mwh, gas_mm3h

Virtual fields derived lazily:

    hour_of_day  = (hour_of_year) mod 24
    is_weekend   = ((floor(hour_of_year/24)) mod 7) >= 5

This module is intentionally syntax-lite: no arbitrary Python eval. Only
the named functions & operators above are recognised, so it is safe to
run on untrusted KPI definitions.
"""

from __future__ import annotations

import json
import re
import statistics
import sys
from pathlib import Path


_VIRT_FIELDS = {
    "hour_of_day":  lambda row: int(row.get("hour_of_year") or row.get("t", 0)) % 24,
    "is_weekend":   lambda row: (int(row.get("hour_of_year") or row.get("t", 0)) // 24) % 7 >= 5,
}


def _get_field(row: dict, field: str):
    if field in _VIRT_FIELDS:
        return _VIRT_FIELDS[field](row)
    return row.get(field)


# ── Filter mini-DSL ──────────────────────────────────────────────────
_FILTER_BETWEEN = re.compile(r"^(?P<f>\w+)\s+between\s+(?P<lo>[-\d\.]+)\s+and\s+(?P<hi>[-\d\.]+)$", re.I)
_FILTER_IN      = re.compile(r"^(?P<f>\w+)\s+in\s+\[(?P<items>[^\]]+)\]$", re.I)
_FILTER_OP      = re.compile(r"^(?P<f>\w+)\s*(?P<op>==|!=|<=|>=|<|>)\s*(?P<v>[-\d\.]+)$")


def _parse_filter(s: str):
    s = s.strip()
    m = _FILTER_BETWEEN.match(s)
    if m:
        lo, hi = float(m["lo"]), float(m["hi"])
        f = m["f"]
        return lambda row: (x := _get_field(row, f)) is not None and lo <= float(x) <= hi
    m = _FILTER_IN.match(s)
    if m:
        items = [float(x.strip()) for x in m["items"].split(",")]
        f = m["f"]
        return lambda row: _get_field(row, f) in items
    m = _FILTER_OP.match(s)
    if m:
        f, op, v = m["f"], m["op"], float(m["v"])
        ops = {
            "==": (lambda a, b: a == b), "!=": (lambda a, b: a != b),
            "<=": (lambda a, b: a <= b), ">=": (lambda a, b: a >= b),
            "<":  (lambda a, b: a < b),  ">":  (lambda a, b: a > b),
        }[op]
        return lambda row: (x := _get_field(row, f)) is not None and ops(float(x), v)
    raise ValueError(f"bad filter expression: {s!r}")


# ── Top-level parsing ────────────────────────────────────────────────
_FN_RE = re.compile(
    r"^\s*(?P<fn>sum|avg|mean|min|max|count|p10|p50|p90|hours_where|ratio|by_unit_sum)\s*"
    r"\((?P<args>.*)\)\s*$", re.I,
)


def _percentile(vals, q):
    if not vals: return 0.0
    s = sorted(vals)
    i = max(0, min(len(s) - 1, int(round(q * (len(s) - 1)))))
    return s[i]


def evaluate(formula: str, results: dict) -> float:
    """Evaluate a single KPI expression against a PowerSim results dict."""
    m = _FN_RE.match(formula or "")
    if not m:
        raise ValueError(f"unrecognised KPI formula: {formula!r}")
    fn = m["fn"].lower(); args = m["args"].strip()
    hs = results.get("hourly_system") or []
    bu = results.get("by_unit_summary") or {}

    if fn == "ratio":
        num_s, den_s = args.split("/", 1)
        num = sum(float(_get_field(r, num_s.strip()) or 0) for r in hs)
        den = sum(float(_get_field(r, den_s.strip()) or 0) for r in hs)
        return num / den if den else 0.0

    if fn == "hours_where":
        pred = _parse_filter(args)
        return sum(1 for r in hs if pred(r))

    if fn == "by_unit_sum":
        field, aid = [x.strip() for x in args.split(",", 1)]
        entry = bu.get(aid) or {}
        return float(entry.get(field, 0) or 0)

    # sum/avg/min/max/count/p10/p50/p90
    if "|" in args:
        field, filt = [x.strip() for x in args.split("|", 1)]
        pred = _parse_filter(filt)
        vals = [float(_get_field(r, field) or 0) for r in hs if pred(r)]
    else:
        field = args.strip()
        vals = [float(_get_field(r, field) or 0) for r in hs]

    if fn == "sum":   return sum(vals)
    if fn in ("avg", "mean"): return statistics.fmean(vals) if vals else 0.0
    if fn == "min":   return min(vals) if vals else 0.0
    if fn == "max":   return max(vals) if vals else 0.0
    if fn == "count": return float(len(vals))
    if fn == "p10":   return _percentile(vals, 0.10)
    if fn == "p50":   return _percentile(vals, 0.50)
    if fn == "p90":   return _percentile(vals, 0.90)
    raise ValueError(f"unhandled KPI function: {fn}")


def evaluate_many(templates: list, results: dict) -> list:
    out = []
    for kp in templates or []:
        try:
            v = evaluate(kp["formula"], results)
            out.append({"id": kp["id"], "label": kp.get("label", kp["id"]),
                        "unit": kp.get("unit", ""), "value": round(v, 4)})
        except Exception as e:
            out.append({"id": kp["id"], "label": kp.get("label", kp["id"]),
                        "unit": kp.get("unit", ""), "error": str(e)})
    return out


# ══════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--results", required=True,
                    help="Path to powersim_results.json")
    ap.add_argument("--templates", default=None,
                    help="Path to a JSON list of KPI templates. "
                         "If omitted, reads templates from results.metadata or embedded inputs.")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    res = json.loads(Path(args.results).read_text(encoding="utf-8"))
    if args.templates:
        templates = json.loads(Path(args.templates).read_text(encoding="utf-8"))
    else:
        templates = (res.get("metadata", {}).get("kpi_templates")
                     or res.get("kpi_templates")
                     or [])
    out = evaluate_many(templates, res)
    text = json.dumps(out, ensure_ascii=False, indent=2)
    if args.out:
        Path(args.out).write_text(text, encoding="utf-8")
        print(f"💾 wrote {args.out}")
    print(text)
