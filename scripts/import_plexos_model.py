"""Build a typed PowerSim project from the Georgia PLEXOS model archive.

Usage::

    python scripts/import_plexos_model.py "plexos model.zip" \
        --out samples/projects/georgia_2030_plexos.json

The archive holds ``BESS Research 800-1400.xml`` (PLEXOS 11) and its CSV
data files.  The mapping is explicit and documented in
``docs/GEORGIA_PLEXOS_MODEL.md``; every asset and profile records the
archive SHA-256, the PLEXOS object and the source file.

Mapping (PLEXOS -> PowerSim):

* 16 run-of-river zone generators -> ``hydro_ror`` with the zone's hourly
  rating-factor profile (percent / 100).
* 5 wind and 9 PV regional generators -> ``wind`` / ``solar`` with their
  hourly rating-factor profile.
* PLEXOS forces these renewables to their rating (``Generation =
  Rated Capacity`` constraints) and dumps any surplus as dump energy.
  PowerSim keeps them curtailable instead, so the same surplus appears as
  curtailment / spill.  The energy balance is identical; see the docs.
* "Regulating HPP" -> ``hydro_ror`` with a flat availability profile, the
  monthly Max Capacity and Min Stable Level series and the monthly energy
  constraint (GWh -> MWh) as an ``exact`` monthly energy budget.
* "TPP/Import" -> ``import`` fixed to its hourly file (pmin = pmax profile),
  VO&M 3.
* Battery "Ksani" -> ``bess`` 800 MW / 1,400 MWh, efficiency 100 %, initial
  SoC 50 %.  PLEXOS VO&M Charge 1.5 is mapped to ``vom_discharge`` 1.5
  (equal energy for a lossless, cyclic battery).
* Region load + the export region's hourly load (served over the 800 MW
  Geo-Exp line, never above 800 MW in the data) -> one ``demand`` profile.

Scenarios swap profile references: hydrology Dry / Flood, wind Calm / Storm,
solar Low / High, and a combined stress case.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import zipfile
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path
from typing import Any

MODEL_DIR = "plexos model/BESS Research 800-1400/"
MODEL_XML = MODEL_DIR + "BESS Research 800-1400.xml"
HOURS = 8760


def _rows(root, ns: str, table: str) -> list[dict[str, str]]:
    return [{child.tag.replace(ns, ""): child.text for child in row} for row in root.findall(ns + table)]


def read_model(archive: zipfile.ZipFile) -> dict[str, Any]:
    """Return generator/battery properties, data-file tags and monthly series."""
    root = ET.fromstring(archive.read(MODEL_XML))
    ns = root.tag.split("}")[0] + "}" if root.tag.startswith("{") else ""
    classes = {r["class_id"]: r["name"] for r in _rows(root, ns, "t_class")}
    objects = {o["object_id"]: o for o in _rows(root, ns, "t_object")}
    properties = {p["property_id"]: p["name"] for p in _rows(root, ns, "t_property")}
    memberships = {m["membership_id"]: m for m in _rows(root, ns, "t_membership")}
    texts = {t["data_id"]: t["value"] for t in _rows(root, ns, "t_text")}
    tags = {t["data_id"]: objects[t["object_id"]]["name"] for t in _rows(root, ns, "t_tag")}
    files = {}
    scalar: dict[str, dict[str, float]] = defaultdict(dict)
    monthly: dict[str, dict[str, dict[str, float]]] = defaultdict(lambda: defaultdict(dict))
    profile_of: dict[str, dict[str, str]] = defaultdict(dict)
    kinds: dict[str, str] = {}
    for row in _rows(root, ns, "t_data"):
        membership = memberships[row["membership_id"]]
        child = objects[membership["child_object_id"]]
        parent = objects[membership["parent_object_id"]]
        prop = properties[row["property_id"]]
        child_class = classes[child["class_id"]]
        if child_class == "Data File" and prop == "Filename":
            files[child["name"]] = texts[row["data_id"]]
            continue
        # PLEXOS constraints often share a generator's name ("01"), so
        # objects are keyed by class as well as name.
        target = parent if classes[parent["class_id"]] == "Constraint" else child
        name = f"{classes[target['class_id']]}:{target['name']}"
        kinds[name] = classes[target["class_id"]]
        value = float(row["value"])
        if row["data_id"] in tags:
            profile_of[name][prop] = tags[row["data_id"]]
        elif row["data_id"] in texts and texts[row["data_id"]].startswith("M"):
            monthly[name][prop][texts[row["data_id"]]] = value
        else:
            scalar[name][prop] = value
    return {"files": files, "scalar": scalar, "monthly": monthly, "profile_of": profile_of, "kinds": kinds}


def read_series(archive: zipfile.ZipFile, file_name: str) -> list[float]:
    lines = archive.read(MODEL_DIR + file_name).decode("utf-8-sig").splitlines()[1:]
    values = [float(line.split(";")[1].replace(",", ".")) for line in lines if line.strip()]
    if len(values) != HOURS:
        raise ValueError(f"{file_name}: expected {HOURS} hourly values, got {len(values)}")
    return values


def _months(series: dict[str, float], scale: float = 1.0) -> list[float]:
    return [round(series[f"M{index:02d}"] * scale, 6) for index in range(1, 13)]


def _slug(text: str) -> str:
    return "".join(ch if ch.isalnum() else "-" for ch in text.lower()).strip("-").replace("--", "-")


VARIANTS = {"RoR": ("Medium", "Dry", "Flood"), "Wind": ("Average", "Calm", "Storm"), "PV": ("Average", "Low", "High")}


def build_project(archive_path: Path) -> dict[str, Any]:
    raw = archive_path.read_bytes()
    sha = "sha256:" + hashlib.sha256(raw).hexdigest()
    archive = zipfile.ZipFile(archive_path)
    model = read_model(archive)
    source = f"PLEXOS model {Path(MODEL_XML).name} in {archive_path.name}"

    def provenance(obj: str, **details: Any) -> dict[str, Any]:
        return {"source": source, "source_version": sha, "transformation": "scripts/import_plexos_model.py",
                "details": {"plexos_object": obj, **details}}

    profiles: dict[str, dict[str, Any]] = {}

    def add_profile(profile_id: str, values: list[float], unit: str, file_name: str) -> str:
        profiles[profile_id] = {"id": profile_id, "unit": unit, "values": [round(v, 6) for v in values],
                                "provenance": provenance(file_name, file=file_name)}
        return profile_id

    load = read_series(archive, model["files"]["Load"])
    export = read_series(archive, model["files"]["Export"])
    line_limit = model["scalar"]["Line:Geo-Exp"]["Max Flow"]
    if max(export) > line_limit + 1e-6:
        raise ValueError(f"export {max(export)} MW exceeds the Geo-Exp line limit {line_limit} MW")
    add_profile("demand", [a + b for a, b in zip(load, export)], "MW", "Load.csv + Export.csv")
    profiles["demand"]["provenance"]["details"]["note"] = (
        "Georgia region load plus the export region load served over the Geo-Exp line")
    add_profile("flat", [1.0] * HOURS, "per-unit", "constant")
    tpp = read_series(archive, model["files"]["TPP/IMP"])
    add_profile("tpp-import-mw", tpp, "MW", model["files"]["TPP/IMP"])

    assets: list[dict[str, Any]] = []
    renewable_refs: dict[str, dict[str, str]] = {}  # asset id -> variant -> profile id
    for key in sorted(n for n, k in model["kinds"].items() if k == "Generator"):
        name = key.split(":", 1)[1]
        refs = model["profile_of"].get(key, {})
        data_file = refs.get("Rating Factor")
        if not data_file:
            continue
        family = data_file.split(" ")[0]  # RoR / Wind / PV
        stem = data_file.rsplit(" ", 1)[0]  # e.g. "RoR Kodori"
        kind = {"RoR": "hydro_ror", "Wind": "wind", "PV": "solar"}[family]
        capacity = model["scalar"][key]["Max Capacity"]
        asset_id = f"{_slug(family)}-{_slug(stem.split(' ', 1)[1])}"
        renewable_refs[asset_id] = {}
        for variant in VARIANTS[family]:
            file_name = model["files"][f"{stem} {variant}"]
            values = read_series(archive, file_name)
            scale = 100.0 if max(values) > 1.5 else 1.0  # rating factors are given in percent
            renewable_refs[asset_id][variant] = add_profile(
                f"cf-{_slug(stem)}-{variant.lower()}", [v / scale for v in values], "per-unit", file_name)
        default = renewable_refs[asset_id][VARIANTS[family][0]]
        assets.append({"id": asset_id, "kind": kind, "capacity_min_mw": 0, "capacity_max_mw": capacity,
                       "profile_references": [default],
                       "legacy_extensions": {"name": f"{stem} ({name})", "committable": False, "vom": 0},
                       "provenance": provenance(name, max_capacity_mw=capacity, rating_factor_file=data_file)})

    reg = model["monthly"]["Generator:Regulating HPP"]
    # the equality constraint's monthly RHS is authoritative; Max Energy Month carries the same values
    reg_energy_gwh = model["monthly"]["Constraint:Regulating HPP"].get("RHS Month") or reg["Max Energy Month"]
    assets.append({
        "id": "regulating-hpp", "kind": "hydro_ror", "capacity_min_mw": 0,
        "capacity_max_mw": max(_months(reg["Max Capacity"])), "profile_references": ["flat"],
        "legacy_extensions": {
            "name": "Regulating HPP (monthly energy-limited aggregate)", "committable": False, "vom": 0,
            "monthly_pmax_mw": _months(reg["Max Capacity"]), "monthly_pmin_mw": _months(reg["Min Stable Level"]),
            "monthly_energy_mwh": _months(reg_energy_gwh, 1000.0), "monthly_energy_mode": "exact"},
        "provenance": provenance("Regulating HPP", note="monthly Max Capacity, Min Stable Level (must-run) and "
                                 "monthly energy constraint (GWh) mapped to monthly_pmax_mw, monthly_pmin_mw and "
                                 "an exact monthly_energy_mwh budget")})
    tpp_scalar = model["scalar"]["Generator:TPP/Import"]
    assets.append({
        "id": "tpp-import", "kind": "import", "capacity_min_mw": 0, "capacity_max_mw": tpp_scalar["Max Capacity"],
        "legacy_extensions": {"name": "TPP / Import (fixed hourly)", "vom": tpp_scalar["VO&M Charge"],
                              "pmax_profile": "tpp-import-mw", "pmin_profile": "tpp-import-mw"},
        "provenance": provenance("TPP/Import", note="generation fixed to the TPP/IMP file by the TPP/Imp constraint")})
    battery = model["scalar"]["Battery:Ksani"]
    assets.append({
        "id": "bess-ksani", "kind": "bess", "capacity_min_mw": 0, "capacity_max_mw": battery["Max Power"],
        "legacy_extensions": {"name": "Ksani BESS", "power_mw": battery["Max Power"], "energy_mwh": battery["Capacity"],
                              "eta_charge": battery["Charge Efficiency"] / 100, "eta_discharge": battery["Discharge Efficiency"] / 100,
                              "soc_init": battery["Initial SoC"] / 100, "soc_min": battery["Min SoC"] / 100,
                              "soc_max": battery["Max SoC"] / 100, "vom_discharge": battery["VO&M Charge"]},
        "provenance": provenance("Ksani", note="PLEXOS VO&M Charge mapped to vom_discharge (lossless, cyclic battery)")})

    def variant_assets(choice: dict[str, str]) -> list[dict[str, Any]]:
        out = []
        for asset in assets:
            refs = renewable_refs.get(asset["id"])
            if refs:
                family = {"hydro_ror": "RoR", "wind": "Wind", "solar": "PV"}[asset["kind"]]
                asset = {**asset, "profile_references": [refs[choice.get(family, VARIANTS[family][0])]]}
            out.append(asset)
        return out

    scenario_specs = [
        ("hydro-dry", "Dry hydrology (RoR Dry)", {"RoR": "Dry"}),
        ("hydro-flood", "Flood hydrology (RoR Flood)", {"RoR": "Flood"}),
        ("wind-calm", "Calm wind year", {"Wind": "Calm"}),
        ("wind-storm", "Windy year", {"Wind": "Storm"}),
        ("solar-low", "Low solar year", {"PV": "Low"}),
        ("solar-high", "High solar year", {"PV": "High"}),
        ("stress-dry-calm-low", "Stress: dry, calm, low solar", {"RoR": "Dry", "Wind": "Calm", "PV": "Low"}),
    ]
    scenarios = [{"id": sid, "name": name, "overlay": {"assets": variant_assets(choice)}}
                 for sid, name, choice in scenario_specs]
    return {
        "id": "georgia-2030-plexos", "version": {"revision": 1}, "workflow": "deterministic_uc",
        "metadata": {
            "name": "Georgia 2030 (PLEXOS model reconstruction)",
            "classification": "study model reconstructed from the user-approved PLEXOS model 'BESS Research 800-1400'",
            "source_archive": archive_path.name, "source_sha256": sha,
            "mapping_document": "docs/GEORGIA_PLEXOS_MODEL.md",
            "limitations": [
                "Aggregated zonal/regional representation as in the PLEXOS model; not a unit-level model.",
                "PLEXOS must-take renewables with dump energy are represented as curtailable renewables.",
                "The regulating HPP is an energy-limited aggregate without a reservoir model.",
                "TPP/Import is fixed to its hourly file; thermal costs are not modelled beyond VO&M 3.",
            ],
        },
        "units": {"currency": "USD"},
        "time": {"timezone": "Asia/Tbilisi", "study_year": 2030, "resolution_minutes": 60,
                 "interval_duration_hours": 1, "start": "2030-01-01T00:00:00+04:00", "periods": HOURS,
                 "calendar_policy": "non_leap"},
        "assets": assets,
        "profiles": list(profiles.values()),
        "solver_settings": {"solver": "highs", "rolling_window_h": HOURS, "rolling_step_h": HOURS,
                            "time_limit_s": 900, "mip_gap": 0.001},
        "scenarios": scenarios,
        "legacy_payload": {"resolution_min": 60, "study_horizon": {"start_hour": 0, "horizon_hours": HOURS}},
    }


MONTH_END_HOURS = (744, 1416, 2160, 2880, 3624, 4344, 5088, 5832, 6552, 7296, 8016, 8760)


def benchmark_reference(archive_path: Path, project: dict[str, Any]) -> dict[str, Any]:
    """Reference figures from the PLEXOS 'BEST' solution (hourly Interval files).

    Sums use math.fsum (correctly rounded), so the file is identical on every
    Python version (3.12 changed the float algorithm of the builtin sum).

    PLEXOS does not export unserved or dump energy here, so both are derived
    hour by hour from its own results: supply = generation + battery
    discharge - battery charge, compared with the same demand profile.
    """
    archive = zipfile.ZipFile(archive_path)
    base = MODEL_DIR + "Model BEST Solution/"
    ids = {}
    for line in archive.read(base + "id2name.csv").decode("utf-8-sig").splitlines()[1:]:
        cls, oid, name = line.split(";")
        ids[(cls, oid)] = name

    def interval(file_name: str) -> list[float]:
        lines = archive.read(base + "Interval/" + file_name).decode("utf-8-sig").splitlines()[1:]
        return [float(line.split(";")[1].replace(",", ".")) for line in lines if line.strip()]

    names = [n.split("/")[-1] for n in archive.namelist() if n.startswith(base + "Interval/")]
    generation: dict[str, list[float]] = {}
    for file_name in names:
        if file_name.startswith("ST Generator(") and file_name.endswith(".Generation.csv"):
            oid = file_name.split("(")[1].split(")")[0]
            generation[ids[("Generator", oid)]] = interval(file_name)
    charge, discharge = interval("ST Battery(1).Charging.csv"), interval("ST Battery(1).Discharging.csv")
    demand = next(p["values"] for p in project["profiles"] if p["id"] == "demand")
    supply = [math.fsum(series[h] for series in generation.values()) + discharge[h] - charge[h] for h in range(HOURS)]
    month = [next(i for i, end in enumerate(MONTH_END_HOURS) if h < end) for h in range(HOURS)]

    def monthly(values: list[float]) -> list[float]:
        return [round(math.fsum(values[h] for h in range(HOURS) if month[h] == m), 3) for m in range(12)]

    shortfall = [max(0.0, demand[h] - supply[h]) for h in range(HOURS)]
    surplus = [max(0.0, supply[h] - demand[h]) for h in range(HOURS)]
    return {
        "source_archive": archive_path.name,
        "source_sha256": "sha256:" + hashlib.sha256(archive_path.read_bytes()).hexdigest(),
        "plexos_model": "BEST (ST Schedule, Regional)",
        "derivation": "hourly Interval results; unserved/dump derived as demand minus (generation + discharge - charge)",
        "demand_mwh": round(math.fsum(demand), 3),
        "generation_mwh": {name: round(math.fsum(values), 3) for name, values in sorted(generation.items())},
        "regulating_hpp_monthly_mwh": monthly(generation["Regulating HPP"]),
        "battery_charge_mwh": round(math.fsum(charge), 3), "battery_discharge_mwh": round(math.fsum(discharge), 3),
        "implied_unserved_mwh": round(math.fsum(shortfall), 3),
        "implied_unserved_monthly_mwh": monthly(shortfall),
        "implied_unserved_hours": sum(1 for value in shortfall if value > 0.01),
        "implied_dump_mwh": round(math.fsum(surplus), 3),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("archive", type=Path)
    parser.add_argument("--out", type=Path, default=Path("samples/projects/georgia_2030_plexos.json"))
    parser.add_argument("--benchmark-out", type=Path, default=Path("data/benchmarks/plexos_bess_research_800_1400.json"))
    args = parser.parse_args(argv)
    project = build_project(args.archive)
    args.benchmark_out.parent.mkdir(parents=True, exist_ok=True)
    args.benchmark_out.write_text(json.dumps(benchmark_reference(args.archive, project), indent=2) + "\n",
                                  encoding="utf-8")
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(project, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
    print(json.dumps({"assets": len(project["assets"]), "profiles": len(project["profiles"]),
                      "scenarios": [s["id"] for s in project["scenarios"]],
                      "bytes": args.out.stat().st_size}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
