"""Chronological probabilistic resource-adequacy simulation.

This is deliberately separate from :mod:`powersim_adequacy`, whose historical
``run_adequacy`` API is a static firm-capacity screening calculation.  This
module follows demand, renewable availability, independent forced outages and
energy-limited BESS state through every resolved period.  It is a Monte-Carlo
adequacy simulation, not a UC/ED, network, or correlated-outage model.
"""
from __future__ import annotations

import math
import random
from collections.abc import Mapping
from typing import Any

DISPATCHABLE = {"thermal", "hydro", "hydro_reg", "hydro_ror", "import"}
VRE = {"wind", "solar"}


class AdequacyValidationError(ValueError):
    """Fail-closed chronological-adequacy input error with structured issues."""
    def __init__(self, issues: list[dict[str, str]]):
        self.issues = issues
        super().__init__("chronological adequacy input is invalid: " + "; ".join(
            f"{item['path']}: {item['message']}" for item in issues))


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _profile(raw: Any, *, path: str, periods: int, issues: list[dict[str, str]],
             nonnegative: bool = True, factor: bool = False) -> list[float]:
    if not isinstance(raw, list) or len(raw) < periods:
        issues.append({"path": path, "message": f"must be a list covering {periods} periods"})
        return []
    out: list[float] = []
    for index, value in enumerate(raw[:periods]):
        number = _finite(value)
        if number is None or (nonnegative and number < 0) or (factor and number > 1):
            suffix = " in [0, 1]" if factor else " finite and non-negative"
            issues.append({"path": f"{path}[{index}]", "message": f"must be{suffix}"})
        else:
            out.append(number)
    return out


def _asset_capacity(asset: Mapping[str, Any]) -> float | None:
    if str(asset.get("type", "")).lower() == "bess":
        return _finite(asset.get("power_mw", asset.get("pmax")))
    return _finite(asset.get("pmax_installed", asset.get("pmax", asset.get("capacity_mw"))))


def validate_chronological_adequacy_input(inp: Mapping[str, Any]) -> list[dict[str, str]]:
    """Return all contract issues.  Callers must not coerce invalid data to zero."""
    issues: list[dict[str, str]] = []
    if not isinstance(inp, Mapping):
        return [{"path": "input", "message": "must be an object"}]
    profiles = inp.get("profiles")
    demand_raw = profiles.get("demand") if isinstance(profiles, Mapping) else None
    if not isinstance(demand_raw, list) or not demand_raw:
        return [{"path": "profiles.demand", "message": "must be a non-empty list"}]
    periods = len(demand_raw)
    _profile(demand_raw, path="profiles.demand", periods=periods, issues=issues)
    resolution = _finite(inp.get("resolution_min", 60))
    if resolution is None or resolution <= 0 or 60 % resolution != 0:
        issues.append({"path": "resolution_min", "message": "must divide 60 and be positive"})
    config = inp.get("chronological_adequacy")
    if not isinstance(config, Mapping):
        issues.append({"path": "chronological_adequacy", "message": "is required"})
        config = {}
    samples = _finite(config.get("samples"))
    if samples is None or samples <= 0 or samples != int(samples):
        issues.append({"path": "chronological_adequacy.samples", "message": "must be a positive integer"})
    seed = _finite(config.get("seed"))
    if seed is None or seed != int(seed):
        issues.append({"path": "chronological_adequacy.seed", "message": "must be a finite integer"})
    assets = inp.get("assets")
    if not isinstance(assets, list):
        issues.append({"path": "assets", "message": "must be a list"})
        return issues
    identifiers: set[str] = set()
    for index, asset in enumerate(assets):
        path = f"assets[{index}]"
        if not isinstance(asset, Mapping):
            issues.append({"path": path, "message": "must be an object"}); continue
        identifier = asset.get("id")
        if not isinstance(identifier, str) or not identifier or identifier in identifiers:
            issues.append({"path": f"{path}.id", "message": "must be a unique non-empty string"})
        else:
            identifiers.add(identifier)
        kind = str(asset.get("type", "")).lower()
        if kind not in DISPATCHABLE | VRE | {"bess"}:
            issues.append({"path": f"{path}.type", "message": "is not supported by chronological adequacy"}); continue
        cap = _asset_capacity(asset)
        if cap is None or cap < 0:
            issues.append({"path": f"{path}.pmax", "message": "must be finite and non-negative"})
        for_rate = _finite(asset.get("for_rate", 0.0))
        if for_rate is None or not 0 <= for_rate <= 1:
            issues.append({"path": f"{path}.for_rate", "message": "must be finite in [0, 1]"})
        if kind in VRE:
            key = asset.get("availability_profile", asset.get("profile_key", asset.get("id")))
            raw = profiles.get(key) if isinstance(key, str) and isinstance(profiles, Mapping) else None
            _profile(raw, path=f"profiles.{key}", periods=periods, issues=issues, factor=True)
        if kind == "bess":
            if for_rate not in (None, 0.0):
                issues.append({"path": f"{path}.for_rate", "message": "is unsupported for chronological BESS adequacy"})
            energy = _finite(asset.get("energy_mwh"))
            if energy is None or energy < 0:
                issues.append({"path": f"{path}.energy_mwh", "message": "must be finite and non-negative"})
            for key, default in (("soc_init", 0.5), ("soc_min", 0.0), ("soc_max", 1.0),
                                 ("eta_charge", 1.0), ("eta_discharge", 1.0)):
                value = _finite(asset.get(key, default))
                if value is None or not 0 <= value <= 1:
                    issues.append({"path": f"{path}.{key}", "message": "must be finite in [0, 1]"})
            low = _finite(asset.get("soc_min", 0.0)); high = _finite(asset.get("soc_max", 1.0)); initial = _finite(asset.get("soc_init", 0.5))
            if low is not None and high is not None and initial is not None and not (low <= initial <= high):
                issues.append({"path": path, "message": "requires soc_min <= soc_init <= soc_max"})
    return issues


def _asset_availability(asset: Mapping[str, Any], profiles: Mapping[str, Any], period: int,
                        rng: random.Random) -> float:
    if asset.get("adequacy_eligible", True) is False:
        return 0.0
    kind = str(asset.get("type", "")).lower(); cap = float(_asset_capacity(asset) or 0.0)
    if kind in VRE:
        key = asset.get("availability_profile", asset.get("profile_key", asset.get("id")))
        base = cap * float(profiles[str(key)][period])
    else:
        base = cap
    return 0.0 if rng.random() < float(asset.get("for_rate", 0.0)) else base


def _qa(inp: Mapping[str, Any], result: Mapping[str, Any]) -> dict[str, Any]:
    """Reconstruct state and metrics from resolved input plus canonical rows."""
    issues: list[str] = []
    dt = float(inp.get("resolution_min", 60)) / 60.0
    samples = result.get("samples") if isinstance(result.get("samples"), list) else []
    expected_eens = expected_lole = 0.0
    bess_assets = {str(a.get("id")): a for a in inp.get("assets", []) if isinstance(a, Mapping) and a.get("type") == "bess"}
    for sample_index, sample in enumerate(samples):
        rows = sample.get("periods") if isinstance(sample, Mapping) else None
        if not isinstance(rows, list): issues.append(f"sample[{sample_index}] missing periods"); continue
        eens = lole = 0.0
        previous: dict[str, float] = {gid: float(a.get("energy_mwh", 0)) * float(a.get("soc_init", .5)) for gid, a in bess_assets.items()}
        for t, row in enumerate(rows):
            if not isinstance(row, Mapping): issues.append(f"sample[{sample_index}].period[{t}] invalid"); continue
            unserved = _finite(row.get("unserved_mw"))
            if unserved is None or unserved < 0: issues.append(f"sample[{sample_index}].period[{t}] invalid unserved_mw"); continue
            available = row.get("available_supply_mw")
            if not isinstance(available, Mapping) or any((_finite(v) is None or _finite(v) < 0) for v in available.values()):
                issues.append(f"sample[{sample_index}].period[{t}] invalid available supply")
            else:
                stated_supply = _finite(row.get("nonstorage_supply_mw"))
                if stated_supply is None or abs(sum(float(v) for v in available.values()) - stated_supply) > 1e-7:
                    issues.append(f"sample[{sample_index}].period[{t}] nonstorage supply does not reconcile")
            eens += unserved * dt; lole += dt if unserved > 1e-9 else 0.0
            storage = row.get("storage") if isinstance(row.get("storage"), Mapping) else {}
            charge_total = discharge_total = 0.0
            for gid, asset in bess_assets.items():
                record = storage.get(gid)
                if not isinstance(record, Mapping): issues.append(f"sample[{sample_index}].period[{t}] missing BESS {gid}"); continue
                start, charge, discharge, end = (_finite(record.get(k)) for k in ("soc_start_mwh", "charge_mw", "discharge_mw", "soc_end_mwh"))
                if None in (start, charge, discharge, end): issues.append(f"sample[{sample_index}].period[{t}] non-finite BESS {gid}"); continue
                energy=float(asset["energy_mwh"]); low=energy*float(asset.get("soc_min",0)); high=energy*float(asset.get("soc_max",1))
                calculated = float(start) + float(charge)*float(asset.get("eta_charge",1))*dt - float(discharge)/float(asset.get("eta_discharge",1))*dt
                power=float(_asset_capacity(asset) or 0)
                if (float(charge) > power+1e-7 or float(discharge) > power+1e-7 or
                    (float(charge) > 1e-7 and float(discharge) > 1e-7) or
                    abs(float(start)-previous[gid]) > 1e-7 or abs(calculated-float(end)) > 1e-7 or not low-1e-7 <= float(end) <= high+1e-7):
                    issues.append(f"sample[{sample_index}].period[{t}] BESS SOC recurrence/bound {gid}")
                previous[gid] = float(end)
                charge_total += float(charge); discharge_total += float(discharge)
            demand = _finite(row.get("demand_mw")); curtailed = _finite(row.get("curtailed_surplus_mw")); supply = _finite(row.get("nonstorage_supply_mw"))
            if None in (demand, curtailed, supply) or abs(float(supply) + discharge_total + float(unserved) - float(demand) - charge_total - float(curtailed)) > 1e-7:
                issues.append(f"sample[{sample_index}].period[{t}] power balance does not reconstruct")
        expected_eens += eens; expected_lole += lole
    count = len(samples)
    if not count: issues.append("no canonical samples")
    else:
        metrics = result.get("metrics") if isinstance(result.get("metrics"), Mapping) else {}
        for key, expected in (("EENS_mwh", expected_eens/count), ("LOLE_h", expected_lole/count)):
            got = _finite(metrics.get(key))
            if got is None or abs(got - expected) > 1e-7:
                issues.append(f"metric {key} does not reconstruct")
    return {"status": "pass" if not issues else "fail", "issues": issues}


def run_chronological_adequacy(inp: Mapping[str, Any]) -> dict[str, Any]:
    """Run reproducible sequential Monte-Carlo adequacy with chronological BESS."""
    issues = validate_chronological_adequacy_input(inp)
    if issues:
        raise AdequacyValidationError(issues)
    config = inp["chronological_adequacy"]; profiles = inp["profiles"]; demand = [float(x) for x in profiles["demand"]]
    dt = float(inp.get("resolution_min", 60)) / 60.0; samples = int(config["samples"]); rng = random.Random(int(float(config["seed"])))
    nonstorage = [a for a in inp["assets"] if str(a.get("type", "")).lower() != "bess"]
    stores = [a for a in inp["assets"] if str(a.get("type", "")).lower() == "bess" and a.get("adequacy_eligible", True) is not False]
    canonical_samples: list[dict[str, Any]] = []
    for sample_id in range(samples):
        soc = {str(a.get("id")): float(a["energy_mwh"]) * float(a.get("soc_init", .5)) for a in stores}
        rows: list[dict[str, Any]] = []
        for period, load in enumerate(demand):
            available_supply = {str(a.get("id")): _asset_availability(a, profiles, period, rng) for a in nonstorage}
            supplied = sum(available_supply.values())
            surplus = max(0.0, supplied - load); deficit = max(0.0, load - supplied); storage: dict[str, Any] = {}
            # Fixed transparent policy: surplus charges in input order; deficit discharges in input order.
            for asset in stores:
                gid=str(asset.get("id")); start=soc[gid]; cap=float(asset["energy_mwh"]); low=cap*float(asset.get("soc_min",0)); high=cap*float(asset.get("soc_max",1)); power=float(_asset_capacity(asset) or 0); eta_c=float(asset.get("eta_charge",1)); eta_d=float(asset.get("eta_discharge",1))
                charge=min(surplus, power, max(0.0, (high-start)/(eta_c*dt))) if eta_c and dt else 0.0
                surplus -= charge; after_charge=start+charge*eta_c*dt
                discharge=min(deficit, power, max(0.0, (after_charge-low)*eta_d/dt)) if eta_d and dt else 0.0
                deficit -= discharge; end=after_charge-discharge/eta_d*dt; soc[gid]=end
                storage[gid]={"soc_start_mwh":start,"charge_mw":charge,"discharge_mw":discharge,"soc_end_mwh":end}
            rows.append({"period":period,"demand_mw":load,"available_supply_mw":available_supply,"nonstorage_supply_mw":supplied,"curtailed_surplus_mw":surplus,"unserved_mw":deficit,"storage":storage})
        canonical_samples.append({"sample_id":sample_id,"periods":rows})
    count=float(samples); eens=sum(sum(float(row["unserved_mw"])*dt for row in sample["periods"]) for sample in canonical_samples)/count
    lole=sum(sum(dt for row in sample["periods"] if float(row["unserved_mw"]) > 1e-9) for sample in canonical_samples)/count
    result: dict[str, Any] = {"workflow":"chronological_probabilistic_adequacy","resolution_min":int(inp.get("resolution_min",60)),"samples":canonical_samples,"metrics":{"LOLE_h":lole,"LOLP":lole/(len(demand)*dt),"EENS_mwh":eens},"assumptions":["Independent per-period forced outages.","BESS is chronological and energy-limited under the documented greedy charge/discharge policy.","No UC, transmission network, outage correlation, hydro chronology or N-1 security is modelled."],"screening_api":"run_adequacy remains a separate legacy compatibility/screening workflow."}
    result.update(evaluate_chronological_adequacy_publication(inp, result))
    return result


def run_chronological_adequacy_qa(inp: Mapping[str, Any], result: Mapping[str, Any]) -> dict[str, Any]:
    """Public independent-QA entrypoint for extraction/publication tests."""
    return _qa(inp, result)


def evaluate_chronological_adequacy_publication(inp: Mapping[str, Any], result: Mapping[str, Any]) -> dict[str, Any]:
    """Apply the adequacy QA gate to extracted/canonical results before publication."""
    qa = _qa(inp, result)
    valid = qa["status"] == "pass"
    return {"qa": qa, "result_validity": "valid" if valid else "invalid",
            "publication": {"publishable": valid,
                            "reasons": [] if valid else ["chronological_adequacy_qa_failed"]}}
