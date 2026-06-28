"""PowerSim adequacy screening (Stage 1).

Transparent PASA-style screening for LOLE/LOLP/EENS, simplified forced outages,
duration-limited storage firm capacity, and greedy expansion handoff. This is not
a full chronological storage adequacy model or PLEXOS PASA replacement.
"""
from __future__ import annotations

import copy, json, math, random
from pathlib import Path
from typing import Any

DISPATCHABLE = {"thermal", "hydro", "hydro_reg", "hydro_ror", "import", "pumped_hydro"}
VRE = {"wind", "solar"}


def _num(x: Any, default: float = 0.0) -> float:
    try:
        if x is None: return default
        return float(x)
    except (TypeError, ValueError):
        return default


def _series(v: Any, n: int, default: float = 0.0) -> list[float]:
    if isinstance(v, list):
        out = [_num(x, default) for x in v[:n]]
        return out + [default] * max(0, n - len(out))
    return [_num(v, default)] * n


def _profile(inp: dict, key: str, n: int) -> list[float] | None:
    p = inp.get("profiles") or {}
    if key in p: return _series(p[key], n, 0.0)
    return None


def _asset_capacity(a: dict) -> float:
    t = str(a.get("type", "")).lower()
    if t == "bess": return _num(a.get("power_mw") or a.get("pmax"))
    if t in VRE: return _num(a.get("pmax_installed") or a.get("pmax"))
    if t == "import": return _num(a.get("pmax") or a.get("pmax_profile") or a.get("import_capacity_mw"))
    return _num(a.get("pmax") or a.get("capacity_mw") or a.get("power_mw"))


def _maint_available(a: dict, h: int) -> bool:
    for w in a.get("maintenance") or a.get("maintenance_windows") or []:
        try:
            if int(w.get("start_h", w.get("start", -1))) <= h < int(w.get("end_h", w.get("end", -1))):
                return False
        except Exception:
            continue
    return True


def bess_firm_capacity(a: dict, required_duration_h: float = 4.0) -> float:
    if "firm_capacity_mw" in a: return _num(a.get("firm_capacity_mw"))
    power = _num(a.get("power_mw") or a.get("pmax"))
    energy = _num(a.get("energy_mwh") or a.get("energy_capacity_mwh"), power * required_duration_h)
    return min(power, energy / max(required_duration_h, 1e-9))


def _candidate_firm_mw(c: dict, required_duration_h: float) -> float:
    block = _num(c.get("block_mw"), _num(c.get("max_build_mw")))
    typ = str(c.get("type", "")).lower()
    if typ == "bess":
        dur = _num(c.get("duration_h"), required_duration_h)
        return min(block, block * dur / max(required_duration_h, 1e-9)) * _num(c.get("capacity_credit"), 1.0)
    if typ in VRE:
        return block * _num(c.get("capacity_credit"), 0.15 if typ == "solar" else 0.20)
    return block * _num(c.get("capacity_credit"), 1.0 - _num(c.get("for_rate"), 0.0))


def _asset_hourly_available(a: dict, inp: dict, n: int, required_duration_h: float, derated: bool, rng: random.Random | None = None) -> list[float]:
    if a.get("adequacy_eligible", True) is False: return [0.0] * n
    t = str(a.get("type", "")).lower(); cap = _asset_capacity(a); for_rate = max(0.0, min(1.0, _num(a.get("for_rate"), 0.0)))
    if t == "bess": return [bess_firm_capacity(a, required_duration_h)] * n
    if t == "dr": return [_num(a.get("pmax_curtail") or a.get("pmax"))] * n
    if "firm_capacity_mw" in a: base = [_num(a.get("firm_capacity_mw"))] * n
    elif t in VRE:
        prof = _profile(inp, str(a.get("profile_key") or a.get("id") or ""), n) or _profile(inp, f"{t}_availability", n) or _profile(inp, t, n)
        if prof is not None: base = [cap * x for x in prof]
        else: base = [cap * _num(a.get("capacity_credit"), 0.15 if t == "solar" else 0.20)] * n
    else:
        base = _series(a.get("pmax_profile"), n, cap) if isinstance(a.get("pmax_profile"), list) else [cap] * n
    out = []
    model = str(a.get("outage_model", "derated" if derated else "bernoulli")).lower()
    for h, b in enumerate(base):
        val = b if _maint_available(a, h) else 0.0
        if model == "none": pass
        elif derated or model == "derated": val *= (1.0 - for_rate)
        elif rng is not None and model == "bernoulli": val = 0.0 if rng.random() < for_rate else val
        out.append(val)
    return out


def run_adequacy(inp: dict, mode: str | None = None, samples: int | None = None, seed: int | None = None) -> dict:
    aq = inp.get("adequacy") or {}; demand = [_num(x) for x in ((inp.get("profiles") or {}).get("demand") or [])]
    if not demand: raise ValueError("profiles.demand is required for adequacy screening")
    n = len(demand); mode = mode or aq.get("mode", "deterministic_derated"); samples = int(samples or aq.get("samples", 200)); seed = int(seed if seed is not None else aq.get("seed", 42))
    req_dur = _num(aq.get("required_storage_duration_h"), 4.0)
    lole_target = _num(aq.get("lole_target_h"), 3.0); eens_target = _num(aq.get("eens_target_mwh"), 0.0); rm_target = _num(aq.get("reserve_margin_target_pct"), 0.0)
    def one(rng=None, derated=True):
        avail = [0.0] * n
        for a in inp.get("assets") or []:
            aa = _asset_hourly_available(a, inp, n, req_dur, derated, rng)
            avail = [x + y for x, y in zip(avail, aa)]
        sf = [max(0.0, d - a) for d, a in zip(demand, avail)]
        return sf, avail
    if mode == "monte_carlo":
        sum_sf = [0.0] * n; lole = 0.0; eens = 0.0; peak_sf = 0.0
        rng = random.Random(seed)
        for _ in range(samples):
            sf, _ = one(rng, derated=False); lole += sum(1 for x in sf if x > 1e-9); eens += sum(sf); peak_sf = max(peak_sf, max(sf or [0.0])); sum_sf = [a+b for a,b in zip(sum_sf, sf)]
        lole /= samples; eens /= samples; peak_sf = peak_sf
    else:
        sf, _ = one(None, derated=True); lole = float(sum(1 for x in sf if x > 1e-9)); eens = float(sum(sf)); peak_sf = max(sf or [0.0])
    firm = sum(_asset_hourly_available(a, inp, n, req_dur, True, None)[demand.index(max(demand))] for a in inp.get("assets") or [])
    peak = max(demand); rm = ((firm - peak) / peak * 100.0) if peak else 0.0
    passed = lole <= lole_target and eens <= eens_target and rm >= rm_target
    return {"mode": mode, "samples": samples if mode == "monte_carlo" else None, "seed": seed if mode == "monte_carlo" else None, "LOLE_h_per_year": lole, "LOLP_pct": lole / n * 100.0, "EENS_mwh": eens, "peak_shortfall_mw": peak_sf, "expected_shortfall_hours": lole, "reserve_margin_pct": rm, "firm_capacity_mw": firm, "peak_load_mw": peak, "adequacy_pass": bool(passed), "targets": {"lole_target_h": lole_target, "eens_target_mwh": eens_target, "reserve_margin_target_pct": rm_target}, "notes": ["Stage 1 screening approximation; BESS firm capacity is duration-limited, not chronological storage adequacy."]}


def annualized_candidate_cost(c: dict) -> float:
    return _num(c.get("capex_per_mw")) + _num(c.get("fixed_om_per_mw_yr") or c.get("opex_per_mw_yr"))


def build_assets_from_expansion_plan(base_assets: list[dict], plan: dict, candidates: list[dict]) -> tuple[list[dict], list[str]]:
    assets = copy.deepcopy(base_assets); warnings=[]; cmap={c.get("id"): c for c in candidates}
    for cid, b in (plan.get("recommended_builds") or {}).items():
        mw = _num(b.get("mw")); c = cmap.get(cid, {}); typ=str(c.get("type", "thermal")).lower();
        if mw <= 0: continue
        a={"id": f"adequacy_{cid}", "type": typ, "adequacy_eligible": True}
        if typ == "bess":
            dur=_num(c.get("duration_h"),4.0); a.update({"power_mw": mw, "energy_mwh": _num(b.get("energy_mwh"), mw*dur), "soc_init": _num(c.get("soc_init"), 0.5), "soc_min": _num(c.get("soc_min"), 0.0), "soc_max": _num(c.get("soc_max"), 1.0), "eta_charge": _num(c.get("eta_charge"), 0.95), "eta_discharge": _num(c.get("eta_discharge"), 0.95)})
        elif typ in VRE: a.update({"pmax_installed": mw, "capacity_credit": _num(c.get("capacity_credit"),0.15)})
        else: a.update({"pmax": mw, "pmin": _num(c.get("pmin"),0.0), "heat_rate": _num(c.get("heat_rate"), 0.0), "ramp_up": _num(c.get("ramp_up"), mw), "ramp_down": _num(c.get("ramp_down"), mw), "min_up": int(_num(c.get("min_up"), 0)), "min_down": int(_num(c.get("min_down"), 0)), "variable_cost": _num(c.get("variable_cost"),0.0), "for_rate": _num(c.get("for_rate"),0.0)})
        assets.append(a)
    return assets, warnings


def plan_adequacy_expansion(inp: dict, mode: str | None = None, samples: int | None = None, seed: int | None = None) -> dict:
    cands=(inp.get("expansion") or {}).get("candidates") or []; aq=inp.get("adequacy") or {}; req=_num(aq.get("required_storage_duration_h"),4.0)
    base=run_adequacy(inp, mode, samples, seed); work=copy.deepcopy(inp); builds={}; cost=0.0; notes=[]
    def ok(m): return m["LOLE_h_per_year"] <= m["targets"]["lole_target_h"] and m["EENS_mwh"] <= m["targets"]["eens_target_mwh"] and m["reserve_margin_pct"] >= m["targets"]["reserve_margin_target_pct"]
    remaining={c.get("id"): _num(c.get("max_build_mw")) for c in cands}
    while not ok(run_adequacy(work, mode, samples, seed)):
        viable=[]
        for c in cands:
            cid=c.get("id"); block=min(_num(c.get("block_mw"), remaining[cid]), remaining[cid])
            firm=_candidate_firm_mw({**c,"block_mw":block}, req)
            if block>0 and firm>0: viable.append((annualized_candidate_cost(c)/firm, c, block))
        if not viable: break
        _, c, block = min(viable, key=lambda x: (x[0], str(x[1].get("id"))))
        cid=c.get("id"); remaining[cid]-=block; builds.setdefault(cid,{"mw":0.0}); builds[cid]["mw"] += block
        if str(c.get("type")).lower()=="bess": builds[cid]["energy_mwh"] = builds[cid]["mw"] * _num(c.get("duration_h"), req)
        cost += block * annualized_candidate_cost(c)
        assets,_=build_assets_from_expansion_plan(inp.get("assets") or [], {"recommended_builds": builds}, cands); work["assets"]=assets
    post=run_adequacy(work, mode, samples, seed); infeasible=not ok(post)
    if infeasible: notes.append("Candidate max_build_mw limits cannot close all adequacy targets.")
    return {"base_adequacy": base, "targets": base["targets"], "recommended_builds": builds, "post_expansion_adequacy": post, "annualized_cost_usd": cost, "infeasible": infeasible, "binding_targets": [k for k,hit in {"LOLE": post["LOLE_h_per_year"]>base["targets"]["lole_target_h"], "EENS": post["EENS_mwh"]>base["targets"]["eens_target_mwh"], "reserve_margin": post["reserve_margin_pct"]<base["targets"]["reserve_margin_target_pct"]}.items() if hit], "notes": notes + ["Greedy least annualized cost per firm MW screening; test recommended builds in UC/ED before decisions."]}


def write_json(path: str | Path, obj: dict) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True); Path(path).write_text(json.dumps(obj, indent=2), encoding="utf-8")
