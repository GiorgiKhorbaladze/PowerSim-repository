"""Independent DC-network QA using extracted angles, injections and flows."""
from __future__ import annotations

from powersim.contracts import QACheckResult, QAStatus
from .tolerance import TolerancePolicy


def check_network_results(resolved_input: dict, result: dict, tolerance: TolerancePolicy) -> list[QACheckResult]:
    buses = list(resolved_input.get("buses") or [])
    lines = list(resolved_input.get("lines") or [])
    if not buses and not lines:
        return []
    # The solver's canonical normalized branches are exposed in diagnostics.
    normalized = ((result.get("diagnostics") or {}).get("dc_network") or {})
    lines = normalized.get("lines") or lines
    if not buses or not lines:
        return [QACheckResult(check_id="network.canonical_extraction", status=QAStatus.FAIL,
            message="network-mode result lacks canonical buses or branches", witness={})]
    rows = result.get("hourly_system") or []
    if not rows:
        return [QACheckResult(check_id="network.canonical_extraction", status=QAStatus.FAIL,
            message="network-mode result lacks interval extraction", witness={})]
    bus_ids = [b["id"] for b in buses]; slack = next((b["id"] for b in buses if b.get("is_slack")), None)
    flow_bad=[]; balance_bad=[]; angle_bad=[]; count=0
    unserved_mwh = 0.0
    for row in rows:
        try:
            unserved_mwh += float(row.get("unserved_mwh", 0.0) or 0.0)
        except (TypeError, ValueError):
            return [QACheckResult(check_id="network.canonical_extraction", status=QAStatus.FAIL,
                message="network-mode result has non-numeric unserved energy", witness={"t": row.get("t")})]
        angle=row.get("bus_angle_rad") or {}; injection=row.get("bus_injection_mw") or {}; flows=row.get("line_flow") or {}
        if set(angle) != set(bus_ids) or set(injection) != set(bus_ids):
            return [QACheckResult(check_id="network.canonical_extraction", status=QAStatus.FAIL,
                message="network-mode result lacks per-bus angle or injection", witness={"t":row.get("t")})]
        net={b:0.0 for b in bus_ids}
        for line in lines:
            lid=line["id"]; f=float(flows.get(lid, float("nan"))); b=float(line["susceptance_mw_per_rad"])
            expected=b*(float(angle[line["from_bus"]])-float(angle[line["to_bus"]]))
            limit=float(line.get("capacity_mw", line.get("normal_limit_mw")))
            tol=tolerance.limit(max(1.0,abs(f),abs(expected),limit))
            if abs(f-expected)>tol or abs(f)>limit+tol: flow_bad.append({"t":row.get("t"),"line":lid,"flow":f,"expected":expected,"limit":limit})
            net[line["from_bus"]]+=f; net[line["to_bus"]]-=f
        demand_by_bus=(resolved_input.get("demand_by_bus") or {})
        shares=(resolved_input.get("load_share_by_bus") or {})
        for bus in bus_ids:
            demand=float(demand_by_bus[bus][count] if isinstance(demand_by_bus.get(bus),list) else demand_by_bus.get(bus, float(row["load_mw"])*float(shares.get(bus,0))))
            # unserved follows declared load shares in the current model.
            share=float(shares.get(bus,0)); unserved=float(row.get("unserved_mwh",0))/ (float(row.get("period_minutes",60))/60) * share
            residual=float(injection[bus])+unserved-demand-net[bus]
            if abs(residual)>tolerance.balance_limit(max(1.0,abs(demand),abs(injection[bus])),float(row.get("period_minutes",60))/60,persisted=True): balance_bad.append({"t":row.get("t"),"bus":bus,"residual_mw":residual})
        if slack is not None and abs(float(angle[slack]))>tolerance.limit(1.0): angle_bad.append({"t":row.get("t"),"slack":slack,"angle":angle[slack]})
        count+=1
    def output(check_id,bad,msg):
        return QACheckResult(check_id=check_id,status=QAStatus.FAIL if bad else QAStatus.PASS,message=msg,witness={"bad":bad[:10]},checked_count=count,max_violation=1.0 if bad else 0.0)
    # The current solver has one system-level unserved-energy variable.  It
    # cannot identify the constrained load bus without a locational slack.
    # Preserve the legacy equation, but never validate/publish a network run
    # that uses it as though the shortage were locationally resolved.
    scarcity_limit = tolerance.persisted_energy_rounding * max(1, len(rows))
    scarcity = QACheckResult(
        check_id="network.locational_unserved_scope",
        status=QAStatus.FAIL if unserved_mwh > scarcity_limit else QAStatus.PASS,
        message=("network scarcity is unsupported without locational unserved-energy variables"
                 if unserved_mwh > scarcity_limit else "no network scarcity requiring locational unserved energy"),
        witness={"unserved_mwh": unserved_mwh, "validated_limit_mwh": scarcity_limit},
        checked_count=count, max_violation=max(0.0, unserved_mwh - scarcity_limit),
    )
    return [output("network.flow_and_limit",flow_bad,"DC branch flow equation and limits"),output("network.nodal_balance",balance_bad,"DC nodal balances"),output("network.reference_angle",angle_bad,"DC reference angle"),scarcity]
