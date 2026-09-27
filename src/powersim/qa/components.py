"""Independent QA over canonical Stage-3A results (never live constraints)."""
from __future__ import annotations

import math
from collections import defaultdict

from powersim.components.results import ComponentResult
from powersim.contracts import QACheckResult, QAStatus

from .tolerance import DEFAULT_TOLERANCES, TolerancePolicy


def _record(
    buckets: dict,
    check_id: str,
    *,
    passed: bool,
    violation: float,
    witness: dict,
    tolerance: float | None = None,
) -> None:
    bucket = buckets.setdefault(
        check_id,
        {
            "passed": True,
            "max_violation": 0.0,
            "checked": 0,
            "witnesses": [],
            "tolerance": tolerance,
        },
    )
    bucket["passed"] = bucket["passed"] and passed
    bucket["checked"] += 1
    bucket["max_violation"] = max(bucket["max_violation"], max(0.0, violation))
    if tolerance is not None:
        bucket["tolerance"] = max(bucket["tolerance"] or 0.0, tolerance)
    if not passed and len(bucket["witnesses"]) < 10:
        bucket["witnesses"].append(witness)


def check_component_results(
    results: list[ComponentResult],
    duration_hours: float,
    tolerance: TolerancePolicy | None = None,
    resolved_input=None,
) -> list[QACheckResult]:
    """Return canonical QA checks for shared component results.

    The checks intentionally reconstruct physics from the extracted component
    results and do not inspect live Pyomo constraints.
    """
    tolerance = tolerance or DEFAULT_TOLERANCES
    buckets: dict[str, dict] = {}

    for result in results:
        values = {
            "available_mw": result.available_mw,
            "injection_mw": result.injection_mw,
            "withdrawal_mw": result.withdrawal_mw,
            "curtailed_mw": result.curtailed_mw,
            "curtailed_mwh": result.curtailed_mwh,
            "state_of_charge_mwh": result.state_of_charge_mwh,
            "previous_state_of_charge_mwh": result.previous_state_of_charge_mwh,
            "generation_high_mw": result.generation_high_mw,
            "generation_deep_mw": result.generation_deep_mw,
            "pumping_high_mw": result.pumping_high_mw,
            "pumping_deep_mw": result.pumping_deep_mw,
            "state_of_water_mm3": result.state_of_water_mm3,
            "previous_state_of_water_mm3": result.previous_state_of_water_mm3,
            "water_inflow_mm3h": result.water_inflow_mm3h,
            "water_release_mm3h": result.water_release_mm3h,
            "water_spill_mm3h": result.water_spill_mm3h,
            "water_cascade_inflow_mm3h": result.water_cascade_inflow_mm3h,
            "water_efficiency_mwh_per_mm3": result.water_efficiency_mwh_per_mm3,
            "gas_consumption_mm3": result.gas_consumption_mm3,
        }
        finite = all(value is None or math.isfinite(float(value)) for value in values.values())
        negatives = {
            key: value
            for key, value in values.items()
            if value is not None and math.isfinite(float(value)) and float(value) < 0
        }
        finite_nonnegative = finite and not negatives
        _record(
            buckets,
            "component.finite_nonnegative",
            passed=finite_nonnegative,
            violation=0.0 if finite_nonnegative else 1.0,
            witness={
                "asset_id": result.asset_id,
                "period": result.period,
                "component_kind": result.component_kind,
                "values": values,
            },
        )

        available = result.available_mw
        injection = result.injection_mw
        if available is None or not math.isfinite(float(available)) or not math.isfinite(float(injection)):
            continue

        scale = max(1.0, abs(float(available)), abs(float(injection)))
        threshold = tolerance.limit(scale)
        bound_violation = max(0.0, float(injection) - float(available))
        check_id = (
            "component.import_bound"
            if result.component_kind == "import"
            else "component.availability_bound"
        )
        _record(
            buckets,
            check_id,
            passed=bound_violation <= threshold,
            violation=bound_violation,
            tolerance=threshold,
            witness={
                "asset_id": result.asset_id,
                "period": result.period,
                "available_mw": available,
                "injection_mw": injection,
            },
        )

        # VRE curtailment is available generation that was not injected.
        # Demand response uses ``curtailed_*`` to mean deliberately reduced
        # demand, which is represented as a positive supply injection.  Its
        # accounting is checked separately below.
        if result.component_kind in {"import", "dr"} or result.curtailed_mw is None:
            continue

        curtailed = float(result.curtailed_mw)
        accounting = abs(float(available) - float(injection) - curtailed)
        _record(
            buckets,
            "component.resource_accounting",
            passed=accounting <= threshold,
            violation=accounting,
            tolerance=threshold,
            witness={
                "asset_id": result.asset_id,
                "period": result.period,
                "available_mw": available,
                "injection_mw": injection,
                "curtailed_mw": curtailed,
            },
        )

        expected_mwh = curtailed * duration_hours
        actual_mwh = float(result.curtailed_mwh or 0.0)
        energy_threshold = tolerance.limit(max(1.0, abs(expected_mwh), abs(actual_mwh)))
        energy_violation = abs(actual_mwh - expected_mwh)
        _record(
            buckets,
            "component.curtailment_energy",
            passed=energy_violation <= energy_threshold,
            violation=energy_violation,
            tolerance=energy_threshold,
            witness={
                "asset_id": result.asset_id,
                "period": result.period,
                "curtailed_mw": curtailed,
                "duration_hours": duration_hours,
                "curtailed_mwh": actual_mwh,
                "expected_mwh": expected_mwh,
            },
        )

    # Thermal UC QA uses only resolved input and canonical extraction.  It
    # intentionally does not inspect the Pyomo model or its live constraints.
    raw_assets = (resolved_input or {}).get("assets", []) if isinstance(resolved_input, dict) else []
    asset_map = {str(a.get("id")): a for a in raw_assets if isinstance(a, dict) and a.get("id") is not None}
    thermal_by_asset: dict[str, list[ComponentResult]] = defaultdict(list)
    for item in results:
        if item.component_kind == "thermal":
            thermal_by_asset[item.asset_id].append(item)
    for asset_id, observations in thermal_by_asset.items():
        asset = asset_map.get(asset_id, {})
        observations.sort(key=lambda item: item.period)
        pmin, pmax = float(asset.get("pmin", 0) or 0), float(asset.get("pmax", 0) or 0)
        ramp_up, ramp_down = float(asset.get("ramp_up", 9999) or 9999), float(asset.get("ramp_down", 9999) or 9999)
        for item in observations:
            u, dispatch = float(item.commitment if item.commitment is not None else 1.0), float(item.injection_mw)
            lo, hi = pmin * u, pmax * u
            limit = tolerance.limit(max(1.0, abs(lo), abs(hi), abs(dispatch)))
            violation = max(0.0, lo - dispatch, dispatch - hi)
            _record(buckets, "component.thermal_bounds", passed=violation <= limit, violation=violation, tolerance=limit,
                    witness={"asset_id": asset_id, "period": item.period, "dispatch_mw": dispatch, "commitment": u, "pmin_mw": lo, "pmax_mw": hi})
        for previous, current in zip(observations, observations[1:]):
            delta = float(current.injection_mw) - float(previous.injection_mw)
            for check_id, rate, violation in (
                ("component.thermal_ramp_up", ramp_up, max(0.0, delta - ramp_up * duration_hours)),
                ("component.thermal_ramp_down", ramp_down, max(0.0, -delta - ramp_down * duration_hours)),
            ):
                if rate < 9999:
                    limit = tolerance.limit(max(1.0, rate * duration_hours, abs(delta)))
                    _record(buckets, check_id, passed=violation <= limit, violation=violation, tolerance=limit,
                            witness={"asset_id": asset_id, "period": current.period, "delta_mw": delta, "limit_mw": rate * duration_hours})
            expected = float(current.commitment or 0) - float(previous.commitment or 0)
            actual = float(current.startup or 0) - float(current.shutdown or 0)
            violation, limit = abs(expected - actual), tolerance.limit(1.0)
            _record(buckets, "component.thermal_uc_transition", passed=violation <= limit, violation=violation, tolerance=limit,
                    witness={"asset_id": asset_id, "period": current.period, "commitment_delta": expected, "startup_minus_shutdown": actual})

        gas_rate = 0.0
        if str(asset.get("fuel_type", "gas")).lower() == "gas":
            gas_rate = float(asset.get("heat_rate", 0) or 0) / 35000.0
        for item in observations:
            actual = float(item.gas_consumption_mm3 or 0)
            expected = float(item.injection_mw) * duration_hours * gas_rate
            violation = abs(actual - expected)
            limit = tolerance.limit(max(1.0, abs(actual), abs(expected)))
            _record(buckets, "component.thermal_gas_conversion", passed=violation <= limit, violation=violation, tolerance=limit,
                    witness={"asset_id":asset_id,"period":item.period,"gas_consumption_mm3":actual,"expected_gas_consumption_mm3":expected,"duration_hours":duration_hours,"gas_rate_mm3_per_mwh":gas_rate})

    # Study-level gas budgets are reconstructed from resolved eligibility and
    # canonical thermal volumes.  No live gas constraint is consulted.
    gas_config = (resolved_input or {}).get("gas_constraints", {}) if isinstance(resolved_input, dict) else {}
    gas_mode = str(gas_config.get("mode", "none"))
    eligible = {str(asset_id) for asset_id in (gas_config.get("applies_to") or [])}
    eligible_observations = [item for asset_id, rows in thermal_by_asset.items() if asset_id in eligible for item in rows]
    if gas_mode != "none" and eligible:
        total = sum(float(item.gas_consumption_mm3 or 0) for item in eligible_observations)
        annual = ((gas_config.get("annual") or {}).get("cap"))
        if gas_mode in {"annual", "annual+monthly"} and annual is not None:
            cap = float(annual)
            violation = max(0.0, total - cap)
            limit = tolerance.limit(max(1.0, cap, total))
            _record(buckets, "component.gas_annual_cap", passed=violation <= limit, violation=violation, tolerance=limit,
                    witness={"eligible_assets":sorted(eligible),"gas_consumption_mm3":total,"annual_cap_mm3":cap})
        monthly_raw = gas_config.get("monthly") or {}
        if gas_mode in {"monthly", "annual+monthly"} and monthly_raw:
            month_names = ("jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec")
            month_hours = (744, 1416, 2160, 2880, 3624, 4344, 5088, 5832, 6552, 7296, 8016, 8760)
            def month_from_key(key):
                text = str(key).strip().lower()
                if text.isdigit():
                    month = int(text)
                    return month if 1 <= month <= 12 else None
                shorthand = text[:3]
                return month_names.index(shorthand) + 1 if shorthand in month_names else None
            caps = {month_from_key(key): float(value) for key, value in monthly_raw.items() if month_from_key(key) is not None}
            start = int(((resolved_input or {}).get("study_horizon", {}) or {}).get("start_hour", 0) or 0)
            consumption = {month: 0.0 for month in caps}
            covered_hours = {month: 0.0 for month in caps}
            covered_periods: set[tuple[int, int]] = set()
            for item in eligible_observations:
                hour = (start + int(item.period * duration_hours)) % 8760
                month = next(index + 1 for index, end in enumerate(month_hours) if hour < end)
                if month in consumption:
                    consumption[month] += float(item.gas_consumption_mm3 or 0)
                    period_key = (month, int(item.period))
                    if period_key not in covered_periods:
                        covered_periods.add(period_key)
                        covered_hours[month] += duration_hours
            solver_settings = (resolved_input or {}).get("solver_settings", {}) or {}
            horizon_hours = float(((resolved_input or {}).get("study_horizon", {}) or {}).get("horizon_hours", 0) or 0)
            rolling_window_hours = float(solver_settings.get("rolling_window_h", horizon_hours) or horizon_hours)
            rolling = horizon_hours > rolling_window_hours
            for month, cap in caps.items():
                # Legacy single-window solves pro-rate a calendar-month cap
                # to the hours represented in that window.  Rolling solves
                # instead carry the full monthly remainder across committed
                # slices.  This is intentionally reconstructed rather than
                # normalized here: changing it would alter study semantics.
                effective_cap = cap if rolling else cap * covered_hours[month] / month_hours[month - 1]
                actual = consumption[month]; violation = max(0.0, actual - effective_cap); limit = tolerance.limit(max(1.0, effective_cap, actual))
                _record(buckets, "component.gas_monthly_cap", passed=violation <= limit, violation=violation, tolerance=limit,
                        witness={"month":month,"eligible_assets":sorted(eligible),"gas_consumption_mm3":actual,"monthly_cap_mm3":cap,"effective_cap_mm3":effective_cap,"rolling":rolling})

    bess_by_asset: dict[str, list[ComponentResult]] = defaultdict(list)
    for item in results:
        if item.component_kind == "bess":
            bess_by_asset[item.asset_id].append(item)
    for asset_id, observations in bess_by_asset.items():
        asset = asset_map.get(asset_id, {})
        observations.sort(key=lambda item: item.period)
        energy = float(asset.get("energy_mwh", 0) or 0)
        lower = float(asset.get("soc_min", 0) or 0) * energy
        upper = float(asset.get("soc_max", 1) or 1) * energy
        charge_cap = float(asset.get("charge_power_mw", asset.get("power_mw", 0)) or 0)
        discharge_cap = float(asset.get("discharge_power_mw", asset.get("power_mw", 0)) or 0)
        if asset.get("c_rate_max") is not None:
            c_rate_power = float(asset.get("c_rate_max") or 0) * energy
            charge_cap, discharge_cap = min(charge_cap, c_rate_power), min(discharge_cap, c_rate_power)
        for item in observations:
            soc = float(item.state_of_charge_mwh or 0)
            violation = max(0.0, lower - soc, soc - upper, -float(item.injection_mw), -float(item.withdrawal_mw), float(item.injection_mw) - discharge_cap, float(item.withdrawal_mw) - charge_cap)
            limit = tolerance.limit(max(1.0, energy, charge_cap, discharge_cap))
            _record(buckets, "component.bess_bounds", passed=violation <= limit, violation=violation, tolerance=limit,
                    witness={"asset_id": asset_id, "period": item.period, "soc_mwh": soc, "charge_mw": item.withdrawal_mw, "discharge_mw": item.injection_mw})
            mode_violation = min(float(item.injection_mw), float(item.withdrawal_mw))
            _record(buckets, "component.bess_mode", passed=mode_violation <= limit, violation=mode_violation, tolerance=limit,
                    witness={"asset_id": asset_id, "period": item.period, "charge_mw": item.withdrawal_mw, "discharge_mw": item.injection_mw})
            prior_soc = item.previous_state_of_charge_mwh
            if prior_soc is None:
                prior_soc = float(asset.get("soc_init", 0) or 0) * energy if item is observations[0] else float(observations[observations.index(item)-1].state_of_charge_mwh or 0)
            eta_c = float(asset.get("eta_charge", 1) or 1)
            eta_d = max(float(asset.get("eta_discharge", 1) or 1), .001)
            self_discharge = float(asset.get("self_discharge_pct_per_h", 0) or 0) / 100.0
            aux = float(asset.get("aux_mw", 0) or 0)
            expected_soc = float(prior_soc) * (1 - self_discharge * duration_hours) + (eta_c * float(item.withdrawal_mw) - float(item.injection_mw) / eta_d - aux) * duration_hours
            recurrence_violation = abs(soc - expected_soc)
            recurrence_limit = tolerance.limit(max(1.0, energy, abs(soc), abs(expected_soc)))
            _record(buckets, "component.bess_soc_recurrence", passed=recurrence_violation <= recurrence_limit, violation=recurrence_violation, tolerance=recurrence_limit,
                    witness={"asset_id":asset_id,"period":item.period,"soc_mwh":soc,"prior_soc_mwh":prior_soc,"expected_soc_mwh":expected_soc})
            prior_charge = item.previous_withdrawal_mw
            prior_discharge = item.previous_injection_mw
            if prior_charge is None: prior_charge = 0.0 if item is observations[0] else float(observations[observations.index(item)-1].withdrawal_mw)
            if prior_discharge is None: prior_discharge = 0.0 if item is observations[0] else float(observations[observations.index(item)-1].injection_mw)
            for check_id, delta, rate in (
                ("component.bess_charge_ramp_up", float(item.withdrawal_mw)-float(prior_charge), float(asset.get("ramp_up_mw_min",0) or 0)),
                ("component.bess_charge_ramp_down", float(prior_charge)-float(item.withdrawal_mw), float(asset.get("ramp_down_mw_min",0) or 0)),
                ("component.bess_discharge_ramp_up", float(item.injection_mw)-float(prior_discharge), float(asset.get("ramp_up_mw_min",0) or 0)),
                ("component.bess_discharge_ramp_down", float(prior_discharge)-float(item.injection_mw), float(asset.get("ramp_down_mw_min",0) or 0)),
            ):
                if rate > 0:
                    allowed=rate * 60 * duration_hours; violation=max(0.0,delta-allowed); limit=tolerance.limit(max(1.0,allowed,abs(delta)))
                    _record(buckets,check_id,passed=violation <= limit,violation=violation,tolerance=limit,witness={"asset_id":asset_id,"period":item.period,"delta_mw":delta,"limit_mw":allowed})

    ph_by_asset: dict[str, list[ComponentResult]] = defaultdict(list)
    for item in results:
        if item.component_kind == "pumped_hydro":
            ph_by_asset[item.asset_id].append(item)
    for asset_id, observations in ph_by_asset.items():
        asset = asset_map.get(asset_id, {})
        cap = float(asset.get("energy_mwh", 0) or 0)
        lower, upper = float(asset.get("soc_min", 0) or 0) * cap, float(asset.get("soc_max", 1) or 1) * cap
        gen_cap, pump_cap = float(asset.get("pmax", 0) or 0), float(asset.get("pump_mw", 0) or 0)
        observations.sort(key=lambda item: item.period)
        for index, item in enumerate(observations):
            soc = float(item.state_of_charge_mwh or 0)
            violation = max(0.0, lower-soc, soc-upper, float(item.injection_mw)-gen_cap, float(item.withdrawal_mw)-pump_cap, -float(item.injection_mw), -float(item.withdrawal_mw))
            limit = tolerance.limit(max(1.0, cap, gen_cap, pump_cap))
            _record(buckets, "component.pumped_hydro_bounds", passed=violation <= limit, violation=violation, tolerance=limit, witness={"asset_id":asset_id,"period":item.period,"soc_mwh":soc})
            violation = min(float(item.injection_mw), float(item.withdrawal_mw))
            _record(buckets, "component.pumped_hydro_mode", passed=violation <= limit, violation=violation, tolerance=limit, witness={"asset_id":asset_id,"period":item.period})
            gen_hi, gen_lo = float(item.generation_high_mw or 0), float(item.generation_deep_mw or 0)
            pmp_hi, pmp_lo = float(item.pumping_high_mw or 0), float(item.pumping_deep_mw or 0)
            segment_violation=max(0.0,abs((gen_hi+gen_lo)-float(item.injection_mw)),abs((pmp_hi+pmp_lo)-float(item.withdrawal_mw)))
            _record(buckets,"component.pumped_hydro_segments",passed=segment_violation <= limit,violation=segment_violation,tolerance=limit,witness={"asset_id":asset_id,"period":item.period,"injection_mw":item.injection_mw,"generation_high_mw":gen_hi,"generation_deep_mw":gen_lo})
            prior=item.previous_state_of_charge_mwh
            if prior is None: prior=float(asset.get("soc_init",0) or 0)*cap if index == 0 else float(observations[index-1].state_of_charge_mwh or 0)
            ep_hi=float(asset.get("efficiency_pump",1) or 1); ep_lo=float(asset.get("efficiency_pump_deep",ep_hi*.85) or 0)
            eg_hi=max(float(asset.get("efficiency_gen",1) or 1),.001); eg_lo=max(float(asset.get("efficiency_gen_deep",eg_hi*.85) or 0),.001)
            expected=float(prior)+(ep_hi*pmp_hi+ep_lo*pmp_lo-gen_hi/eg_hi-gen_lo/eg_lo)*duration_hours
            recurrence_violation=abs(soc-expected); recurrence_limit=tolerance.limit(max(1.0,cap,abs(soc),abs(expected)))
            _record(buckets,"component.pumped_hydro_soc_recurrence",passed=recurrence_violation <= recurrence_limit,violation=recurrence_violation,tolerance=recurrence_limit,witness={"asset_id":asset_id,"period":item.period,"soc_mwh":soc,"prior_soc_mwh":prior,"expected_soc_mwh":expected})
            expected_cost=float(asset.get("vom",.1) or 0)*float(item.injection_mw)*duration_hours
            cost_violation=abs(float(item.cost_usd or 0)-expected_cost)
            _record(buckets,"component.pumped_hydro_cost",passed=cost_violation <= tolerance.limit(max(1.0,abs(expected_cost))),violation=cost_violation,tolerance=tolerance.limit(max(1.0,abs(expected_cost))),witness={"asset_id":asset_id,"period":item.period,"cost_usd":item.cost_usd,"expected_cost_usd":expected_cost})

    reservoir_by_asset: dict[str, list[ComponentResult]] = defaultdict(list)
    for item in results:
        if item.component_kind == "hydro_reg":
            reservoir_by_asset[item.asset_id].append(item)
    for asset_id, observations in reservoir_by_asset.items():
        asset, hydro = asset_map.get(asset_id, {}), (asset_map.get(asset_id, {}).get("hydro") or {})
        observations.sort(key=lambda item: item.period)
        lower, upper = float(hydro.get("reservoir_min", 0) or 0), float(hydro.get("reservoir_max", 0) or 0)
        pmax = float(asset.get("pmax", 0) or 0)
        minimum_release = float(hydro.get("min_release_mm3h", 0) or 0)
        spill_price = float(hydro.get("spill_cost_usd_per_mm3", hydro.get("spill_cost", 0)) or 0)
        for index, item in enumerate(observations):
            storage = float(item.state_of_water_mm3 or 0)
            injection = float(item.injection_mw)
            inflow = float(item.water_inflow_mm3h or 0)
            release = float(item.water_release_mm3h or 0)
            spill = float(item.water_spill_mm3h or 0)
            cascade = float(item.water_cascade_inflow_mm3h or 0)
            efficiency = float(item.water_efficiency_mwh_per_mm3 or 0)
            limit = tolerance.limit(max(1.0, upper, pmax, abs(storage)))
            bounds_violation = max(0.0, lower - storage, storage - upper, -injection, injection - pmax, -spill)
            _record(buckets, "component.reservoir_bounds", passed=bounds_violation <= limit, violation=bounds_violation, tolerance=limit,
                    witness={"asset_id":asset_id,"period":item.period,"storage_mm3":storage,"injection_mw":injection})
            release_violation = abs(release - injection / max(efficiency, .001))
            _record(buckets, "component.reservoir_release_conversion", passed=release_violation <= limit, violation=release_violation, tolerance=limit,
                    witness={"asset_id":asset_id,"period":item.period,"release_mm3h":release,"injection_mw":injection,"efficiency_mwh_per_mm3":efficiency})
            prior = item.previous_state_of_water_mm3
            if prior is None:
                prior = float(hydro.get("reservoir_init", 0) or 0) if index == 0 else float(observations[index - 1].state_of_water_mm3 or 0)
            expected = float(prior) + (inflow + cascade - release - spill) * duration_hours
            balance_violation = abs(storage - expected)
            balance_limit = tolerance.limit(max(1.0, upper, abs(storage), abs(expected)))
            _record(buckets, "component.reservoir_water_balance", passed=balance_violation <= balance_limit, violation=balance_violation, tolerance=balance_limit,
                    witness={"asset_id":asset_id,"period":item.period,"storage_mm3":storage,"prior_storage_mm3":prior,"expected_storage_mm3":expected,"inflow_mm3h":inflow,"release_mm3h":release,"spill_mm3h":spill,"cascade_mm3h":cascade,"duration_hours":duration_hours})
            if minimum_release > 0:
                release_min_violation = max(0.0, minimum_release - release - spill)
                _record(buckets, "component.reservoir_min_release", passed=release_min_violation <= limit, violation=release_min_violation, tolerance=limit,
                        witness={"asset_id":asset_id,"period":item.period,"minimum_release_mm3h":minimum_release,"release_mm3h":release,"spill_mm3h":spill})
            expected_cost = spill_price * spill * duration_hours
            cost_violation = abs(float(item.spill_cost_usd or 0) - expected_cost)
            _record(buckets, "component.reservoir_spill_cost", passed=cost_violation <= tolerance.limit(max(1.0, abs(expected_cost))), violation=cost_violation, tolerance=tolerance.limit(max(1.0, abs(expected_cost))),
                    witness={"asset_id":asset_id,"period":item.period,"spill_cost_usd":item.spill_cost_usd,"expected_spill_cost_usd":expected_cost})

    # Cascade transport is checked from resolved topology and canonical water
    # records only.  It intentionally does not read the live Pyomo balance.
    hydro_index = {(item.asset_id, item.period): item for rows in reservoir_by_asset.values() for item in rows}
    periods_per_hour = max(1, int(round(1 / duration_hours)))
    for downstream, observations in reservoir_by_asset.items():
        hydro = (asset_map.get(downstream, {}).get("hydro") or {})
        upstream = hydro.get("cascade_upstream")
        if not upstream:
            continue
        delay = int(hydro.get("cascade_travel_delay_h", 0) or 0) * periods_per_hour
        gain = float(hydro.get("cascade_gain", 1) or 0)
        mode = hydro.get("cascade_flow_mode", "turbined_only")
        for item in observations:
            source = hydro_index.get((str(upstream), item.period - delay))
            expected = 0.0
            if source is not None:
                expected = float(source.water_release_mm3h or 0)
                if mode == "release_plus_spill":
                    expected += float(source.water_spill_mm3h or 0)
                expected *= gain
            actual = float(item.water_cascade_inflow_mm3h or 0)
            limit = tolerance.limit(max(1.0, abs(expected), abs(actual)))
            violation = abs(actual - expected)
            _record(buckets, "component.reservoir_cascade_transport", passed=violation <= limit, violation=violation, tolerance=limit,
                    witness={"downstream_asset_id":downstream,"upstream_asset_id":upstream,"period":item.period,"delay_periods":delay,"flow_mode":mode,"expected_cascade_mm3h":expected,"actual_cascade_mm3h":actual})

    dr_by_asset: dict[str, list[ComponentResult]] = defaultdict(list)
    for item in results:
        if item.component_kind == "dr":
            dr_by_asset[item.asset_id].append(item)
    for asset_id, observations in dr_by_asset.items():
        asset = asset_map.get(asset_id, {})
        observations.sort(key=lambda item: item.period)
        cap = float(asset.get("pmax_curtail", 0) or 0)
        total_energy = 0.0
        for item in observations:
            curtailed = float(item.curtailed_mw or 0)
            limit = tolerance.limit(max(1.0, cap, curtailed, float(item.injection_mw)))
            violation = max(0.0, curtailed - cap, abs(float(item.injection_mw) - curtailed))
            _record(buckets, "component.demand_response_bounds", passed=violation <= limit, violation=violation, tolerance=limit,
                    witness={"asset_id": asset_id, "period": item.period, "curtailed_mw": curtailed, "injection_mw": item.injection_mw, "available_mw": item.available_mw})
            total_energy += float(item.curtailed_mwh or 0)
            expected_cost = float(asset.get("price_per_mwh", 0) or 0) * float(item.curtailed_mwh or 0)
            cost_violation = abs(float(item.cost_usd or 0) - expected_cost)
            _record(buckets, "component.demand_response_cost", passed=cost_violation <= tolerance.limit(max(1.0, abs(expected_cost))), violation=cost_violation,
                    tolerance=tolerance.limit(max(1.0, abs(expected_cost))), witness={"asset_id": asset_id, "period": item.period, "cost_usd": item.cost_usd, "expected_cost_usd": expected_cost})
        hours = float(asset.get("hours_per_year_max", 8760) or 0)
        horizon_hours = len(observations) * duration_hours
        # solve_all uses a pro-rated cap only for a single window.  Once the
        # study exceeds rolling_window_h it seeds a full annual call-out
        # budget and carries the remaining hours across committed slices.
        settings=(resolved_input or {}).get("solver_settings", {}) if isinstance(resolved_input,dict) else {}
        study=(resolved_input or {}).get("study_horizon", {}) if isinstance(resolved_input,dict) else {}
        configured_horizon=float(study.get("horizon_hours", horizon_hours) or horizon_hours)
        window_hours=float(settings.get("rolling_window_h",168) or 168)
        rolling=configured_horizon > window_hours
        cap_mwh = cap * hours if rolling else cap * hours * min(1.0, horizon_hours / 8760.0)
        annual_violation = max(0.0, total_energy - cap_mwh)
        annual_limit = tolerance.limit(max(1.0, cap_mwh, total_energy))
        _record(buckets, "component.demand_response_energy_cap", passed=annual_violation <= annual_limit, violation=annual_violation, tolerance=annual_limit,
                witness={"asset_id": asset_id, "curtailed_mwh": total_energy, "cap_mwh": cap_mwh, "horizon_hours": horizon_hours, "rolling_budget_semantics":rolling})

    checks: list[QACheckResult] = []
    for check_id in sorted(buckets):
        bucket = buckets[check_id]
        status = QAStatus.PASS if bucket["passed"] else QAStatus.FAIL
        checks.append(
            QACheckResult(
                check_id=check_id,
                status=status,
                message=(
                    f"{bucket['checked']} shared-component observation(s) checked"
                    if bucket["passed"]
                    else f"{len(bucket['witnesses'])} witness(es) show a component invariant violation"
                ),
                witness={"worst": bucket["witnesses"]},
                tolerance=bucket["tolerance"],
                max_violation=bucket["max_violation"],
                checked_count=bucket["checked"],
            )
        )
    return checks
