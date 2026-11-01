"""Turn the Georgia 2027-2032 outlook results into a Georgian HTML report and an Excel workbook.

Usage::

    python scripts/georgia_outlook_report.py data/outlook/georgia_outlook_2027_2032.json \
        --html report.html --xlsx report.xlsx

Every number in the report is read from the outlook JSON written by
``scripts/georgia_outlook.py``; nothing is typed in by hand.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

GROUPS = [  # (key, Georgian label) in stacking order
    ("reg_hydro", "მარეგულირებელი ჰესები"),
    ("thermal_existing", "არსებული თბოსადგურები"),
    ("ror_hydro", "სეზონური და მცირე ჰესები"),
    ("solar", "მზე"),
    ("import", "იმპორტი (სალდო)"),
    ("wind", "ქარი"),
    ("thermal_new", "ახალი: RICE / CCGT-3"),
]
SENS_LABELS = {"dry_2025": "ჰიდროლოგია 2025", "wet_2024": "ჰიდროლოგია 2024",
               "demand_+5": "მოთხოვნა +5%", "demand_-5": "მოთხოვნა −5%"}
SCEN_LABELS = {"A": "A — მხოლოდ ცნობილი პროექტები", "B": "B — PLEXOS-ის განვითარება"}


def group_of(asset_id: str) -> str | None:
    if asset_id.startswith("reg-"):
        return "reg_hydro"
    if asset_id.startswith("ror-") or asset_id == "new-ror-plexos":
        return "ror_hydro"
    if asset_id.startswith(("wind-", "new-wind-")):
        return "wind"
    if asset_id.startswith(("solar-", "new-pv-")):
        return "solar"
    if asset_id == "net-import":
        return "import"
    if asset_id.startswith("tpp-"):
        return "thermal_existing"
    if asset_id in ("new-rice", "new-ccgt-3"):
        return "thermal_new"
    return None  # battery: net energy is a loss, reported separately


def energy_by_group(monthly: dict[str, list[float]]) -> tuple[dict[str, float], dict[str, list[float]]]:
    year = {k: 0.0 for k, _ in GROUPS}
    months = {k: [0.0] * 12 for k, _ in GROUPS}
    for asset_id, values in monthly.items():
        group = group_of(asset_id)
        if group:
            year[group] += sum(values)
            months[group] = [a + b for a, b in zip(months[group], values)]
    return {k: round(v, 1) for k, v in year.items()}, {k: [round(x, 1) for x in v] for k, v in months.items()}


def thermal_gwh(monthly: dict[str, list[float]]) -> float:
    return round(sum(sum(v) for k, v in monthly.items() if group_of(k) in ("thermal_existing", "thermal_new")), 1)


def summarise(data: dict) -> dict:
    rows = []
    for scenario, plan in data["plans"].items():
        for step in plan:
            key = f"{scenario}-{step['year']}"
            d = step["dispatch"]
            year_energy, month_energy = energy_by_group(d["monthly_gwh"])
            sens = data["sensitivities"].get(key, {})
            best = min(step["options"], key=lambda o: o["annual_cost_usd"])
            rows.append({
                "scenario": scenario, "year": step["year"], "rice_mw": step["rice_mw"], "ccgt3": step["ccgt3"],
                "meets_target": step["meets_target"],
                "energy": year_energy, "energy_monthly": month_energy,
                "thermal_gwh": thermal_gwh(d["monthly_gwh"]),
                "gas_mcm": round(d["gas_mm3"] or 0.0, 1),
                "served_gwh": d["served_gwh"], "unserved_gwh": d["unserved_gwh"], "curtailed_gwh": d["curtailed_gwh"],
                "peak_load_mw": d["peak_load_mw"], "hours_thermal_full": d["hours_thermal_full"],
                "objective_musd": round(d["objective_usd"] / 1e6, 1),
                "capital_fom_musd": round(best["capital_fom_usd"] / 1e6, 1),
                "total_cost_musd": round(best["annual_cost_usd"] / 1e6, 1),
                "lole_plan": round(step["adequacy"]["LOLE_h"], 2), "eens_plan_mwh": round(step["adequacy"]["EENS_mwh"], 0),
                "lole_no_build": round(data["no_expansion"][key]["LOLE_h"], 1),
                "eens_no_build_mwh": round(data["no_expansion"][key]["EENS_mwh"], 0),
                "lole_keep_tbilsresi": round(data["no_retirement"][key]["LOLE_h"], 1),
                "reserve": d["reserve"], "qa": d["qa"], "publishable": d["publishable"],
                "options": [{"ccgt3": o["ccgt3"], "rice_mw": o["rice_mw"], "total_musd": round(o["annual_cost_usd"] / 1e6, 1),
                             "lole": round(o["adequacy"]["LOLE_h"], 2), "meets": o["meets_target"]} for o in step["options"]],
                "sensitivities": {label: {"thermal_gwh": thermal_gwh(s["monthly_gwh"]), "unserved_gwh": s["unserved_gwh"],
                                          "lole": round(s["adequacy"]["LOLE_h"], 2) if "adequacy" in s else None,
                                          "qa": s["qa"], "publishable": s["publishable"]}
                                  for label, s in sens.items()},
            })
    pathways = [{"scenario": p["scenario"], "ccgt_from": p["ccgt_from"], "npv_musd": round(p["npv_usd"] / 1e6, 1),
                 "years": [{"year": y["year"], "rice_mw": y["rice_mw"], "ccgt3": y["ccgt3"],
                            "cost_musd": round(y["annual_cost_usd"] / 1e6, 1), "lole": round(y["LOLE_h"], 2)}
                           for y in p["years"]]} for p in data.get("pathways", [])]
    return {"rows": rows, "assumptions": data["assumptions"], "target": data["lole_target_h"],
            "candidates": data["candidates"], "bess": data["bess"], "pathways": pathways,
            "stress": [{"scenario": k.split("-")[0], "year": int(k.split("-")[1]), **v}
                       for k, v in data.get("stress", {}).items()]}


def first_year(rows, scenario, predicate):
    return next((r["year"] for r in rows if r["scenario"] == scenario and predicate(r)), None)


def findings(summary: dict) -> list[str]:
    rows = summary["rows"]
    out = []
    for scen in ("A", "B"):
        srows = [r for r in rows if r["scenario"] == scen]
        if not srows:
            continue
        rice_year = first_year(rows, scen, lambda r: r["rice_mw"] > 0)
        ccgt_year = first_year(rows, scen, lambda r: r["ccgt3"])
        last = srows[-1]
        parts = [f"სცენარი {scen}:"]
        parts.append(f"RICE საჭიროა {rice_year} წლიდან" if rice_year else "RICE 2032 წლამდე არ არის საჭირო")
        if rice_year:
            parts.append(f"2032 წლისთვის სულ {last['rice_mw']:.0f} MW")
        parts.append(f"CCGT-3 — {ccgt_year} წლიდან" if ccgt_year else "CCGT-3 2032 წლამდე ეკონომიკურად არ ჩანს")
        out.append(f"{parts[0]} " + "; ".join(parts[1:]) + ".")
    for scen in ("A", "B"):
        paths = sorted((p for p in summary.get("pathways", []) if p["scenario"] == scen), key=lambda p: p["npv_musd"])
        if len(paths) > 1:
            best, rice_only = paths[0], next((p for p in paths if p["ccgt_from"] is None), None)
            name = lambda p: "მხოლოდ RICE" if p["ccgt_from"] is None else f"CCGT-3 {p['ccgt_from']}-დან + RICE"  # noqa: E731
            text = (f"სცენარი {scen}, 2027–2032 წლების დისკონტირებული ხარჯით (NPV) ყველაზე იაფია: {name(best)} "
                    f"({best['npv_musd']:,.0f} მლნ $)")
            if rice_only and best is not rice_only:
                text += f"; მხოლოდ RICE-თან შედარებით {rice_only['npv_musd'] - best['npv_musd']:,.0f} მლნ $-ით ნაკლები"
            elif rice_only:
                second = paths[1]
                text += f"; {name(second)} {second['npv_musd'] - best['npv_musd']:,.0f} მლნ $-ით ძვირია"
            out.append(text + ".")
    b2030 = next((r for r in rows if r["scenario"] == "B" and r["year"] == 2030), None)
    if b2030 and b2030["curtailed_gwh"] > 100:
        out.append(f"სცენარი B-ში PLEXOS-ის 2030 წლის ქარი, მზე და ჰესები GSE-ის მოთხოვნაზე ბევრად მეტს გამოიმუშავებს: "
                   f"2030 წელს {b2030['curtailed_gwh']:,.0f} GWh ზედმეტი ენერგია იჭრება (ექსპორტი ფიქსირებულია). "
                   "ეს პოტენციური ექსპორტია და ქსელისა და ექსპორტის შესაძლებლობას მოითხოვს.")
    stress = {(x["scenario"], x["year"]): x for x in summary.get("stress", [])}
    for scen, label in (("B", "2024 წლის ზამთრის ჰიდროლოგიით, მშვიდი ქარითა და დაბალი მზით (PLEXOS-ის პროფილები)"),
                        ("A", "2024 წლის ზამთრის ჰიდროლოგიით")):
        worst = [x for (sc, _), x in stress.items() if sc == scen and x.get("rice_needed_mw") is not None]
        if worst:
            top = max(worst, key=lambda x: x["rice_needed_mw"])
            out.append(f"სტრეს-ტესტი, სცენარი {scen}, {label}: LOLE-ს ნორმისთვის {top['year']} წელს "
                       f"{top['rice_needed_mw']:,.0f} MW RICE (ან ექვივალენტური მყარი სიმძლავრე/იმპორტი) იქნებოდა საჭირო.")
    a2027 = next((r for r in rows if r["scenario"] == "A" and r["year"] == 2027), None)
    if a2027:
        out.append(f"თბილსრესის III და IV ბლოკების გასვლა 2027 წელს LOLE-ს {a2027['lole_keep_tbilsresi']:.1f} სთ-დან "
                   f"{a2027['lole_no_build']:.1f} სთ-მდე ზრდის (ნორმა ≤ {summary['target']:.0f} სთ/წელი). "
                   "2027 წელს ახალი სიმძლავრე ჯერ ვერ აშენდება, ამიტომ ეს წელი იმპორტზე ან ბლოკების "
                   "გასვლის გადავადებაზეა დამოკიდებული.")
    for scen in ("A", "B"):
        r = next((x for x in rows if x["scenario"] == scen and x["year"] == 2030), None)
        if r:
            out.append(f"2030 წელს ({scen}) თბოსადგურები {r['thermal_gwh']:,.0f} GWh-ს გამოიმუშავებენ, "
                       f"გაზი ≈ {r['gas_mcm']:,.0f} მლნ მ³; საჭირო რეზერვი {r['reserve']['requirement_mw']:,.0f} MW.")
    return out


# --------------------------------------------------------------------------- Excel
def write_xlsx(summary: dict, path: Path) -> None:
    import xlsxwriter
    book = xlsxwriter.Workbook(str(path))
    head = book.add_format({"bold": True, "bg_color": "#DCE6E3", "border": 1, "text_wrap": True, "valign": "top"})
    num = book.add_format({"num_format": "#,##0.0", "border": 1})
    txt = book.add_format({"border": 1, "text_wrap": True, "valign": "top"})

    def sheet(name, header, rows, widths=None):
        ws = book.add_worksheet(name)
        ws.write_row(0, 0, header, head)
        for i, row in enumerate(rows, start=1):
            for j, value in enumerate(row):
                ws.write(i, j, value, num if isinstance(value, (int, float)) and not isinstance(value, bool) else txt)
        for j, w in enumerate(widths or [14] * len(header)):
            ws.set_column(j, j, w)
        ws.freeze_panes(1, 0)

    rows = summary["rows"]
    sheet("შეჯამება", ["სცენარი", "წელი", "RICE, MW (ჯამი)", "CCGT-3", "ნორმა დაკმაყოფილებულია", "LOLE გეგმით, სთ",
                       "EENS გეგმით, MWh", "LOLE მშენებლობის გარეშე, სთ", "LOLE თბილსრესით, სთ", "თბოსადგურები, GWh",
                       "გაზი, მლნ მ³", "დაუკმაყოფილებელი, GWh", "ჩაჭრილი, GWh", "პიკი, MW", "სრული დატვირთვის საათები",
                       "დისპეტჩირების ხარჯი, მლნ $", "კაპიტალი+FOM, მლნ $", "სულ, მლნ $", "QA", "გამოქვეყნებადი"],
          [[r["scenario"], r["year"], r["rice_mw"], "კი" if r["ccgt3"] else "არა", "კი" if r["meets_target"] else "არა",
            r["lole_plan"], r["eens_plan_mwh"], r["lole_no_build"], r["lole_keep_tbilsresi"], r["thermal_gwh"],
            r["gas_mcm"], r["unserved_gwh"], r["curtailed_gwh"], r["peak_load_mw"], r["hours_thermal_full"],
            r["objective_musd"], r["capital_fom_musd"], r["total_cost_musd"], r["qa"], "კი" if r["publishable"] else "არა"]
           for r in rows])
    sheet("ენერგობალანსი", ["სცენარი", "წელი"] + [label for _, label in GROUPS] + ["მოხმარება+ექსპორტი"],
          [[r["scenario"], r["year"]] + [r["energy"][k] for k, _ in GROUPS] + [r["served_gwh"]] for r in rows])
    monthly = []
    for r in rows:
        for k, label in GROUPS:
            monthly.append([r["scenario"], r["year"], label] + r["energy_monthly"][k])
    sheet("თვიური", ["სცენარი", "წელი", "წყარო"] + ["იან", "თებ", "მარ", "აპრ", "მაი", "ივნ", "ივლ", "აგვ", "სექ",
                                                     "ოქტ", "ნოე", "დეკ"], monthly, [10, 8, 26] + [9] * 12)
    sheet("რეზერვი", ["სცენარი", "წელი", "უდიდესი ბლოკი, MW", "ქარი/მზის ბალანსირება, MW", "მოთხოვნა, MW",
                      "მინ. მარაგი, MW", "საათები დეფიციტით", "ქარი, MW", "მზე, MW"],
          [[r["scenario"], r["year"], r["reserve"]["largest_unit_mw"], r["reserve"]["vre_balancing_mw"],
            r["reserve"]["requirement_mw"], r["reserve"]["min_headroom_mw"], r["reserve"]["hours_short"],
            r["reserve"]["wind_mw"], r["reserve"]["solar_mw"]] for r in rows])
    sens = []
    for r in rows:
        for label, s in r["sensitivities"].items():
            sens.append([r["scenario"], r["year"], SENS_LABELS.get(label, label), s["thermal_gwh"], s["unserved_gwh"],
                         s["lole"] if s["lole"] is not None else "—", s["qa"]])
    sheet("მგრძნობელობა", ["სცენარი", "წელი", "შემთხვევა", "თბოსადგურები, GWh", "დაუკმაყოფილებელი, GWh",
                           "LOLE, სთ", "QA"], sens)
    opts = []
    for r in rows:
        for o in r["options"]:
            opts.append([r["scenario"], r["year"], "CCGT-3 + RICE" if o["ccgt3"] else "მხოლოდ RICE", o["rice_mw"],
                         o["total_musd"], o["lole"], "კი" if o["meets"] else "არა"])
    sheet("ვარიანტები", ["სცენარი", "წელი", "ვარიანტი", "RICE, MW", "წლიური ხარჯი, მლნ $", "LOLE, სთ",
                         "ნორმა"], opts)
    paths = []
    for p in summary.get("pathways", []):
        for y in p["years"]:
            paths.append([p["scenario"], "მხოლოდ RICE" if p["ccgt_from"] is None else f"CCGT-3 {p['ccgt_from']}-დან",
                          y["year"], y["rice_mw"], "კი" if y["ccgt3"] else "არა", y["cost_musd"], y["lole"], p["npv_musd"]])
    stress_rows = [[x["scenario"], x["year"], x["weather"], round(x["lole_no_build"], 1), round(x["eens_no_build_mwh"]),
                    x["rice_needed_mw"] if x["rice_needed_mw"] is not None else "—", round(x["lole_with_rice"], 2)]
                   for x in summary.get("stress", [])]
    if stress_rows:
        sheet("სტრეს-ტესტი", ["სცენარი", "წელი", "ამინდი/ჰიდროლოგია", "LOLE გარეშე, სთ", "EENS გარეშე, MWh",
                              "საჭირო RICE, MW", "LOLE RICE-ით, სთ"], stress_rows)
    if paths:
        sheet("გზები (NPV)", ["სცენარი", "გზა", "წელი", "RICE, MW", "CCGT-3", "წლიური ხარჯი, მლნ $", "LOLE, სთ",
                              "NPV 2027–2032, მლნ $"], paths)
    sheet("დაშვებები", ["#", "დაშვება"], [[i + 1, a] for i, a in enumerate(summary["assumptions"])], [5, 140])
    book.close()


# --------------------------------------------------------------------------- HTML
def write_html(summary: dict, path: Path, template: Path) -> None:
    payload = json.dumps({"summary": summary, "findings": findings(summary), "groups": GROUPS,
                          "sensLabels": SENS_LABELS, "scenLabels": SCEN_LABELS}, ensure_ascii=False)
    html = template.read_text(encoding="utf-8").replace("/*__DATA__*/null", payload)
    path.write_text(html, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("results", type=Path)
    parser.add_argument("--html", type=Path)
    parser.add_argument("--xlsx", type=Path)
    parser.add_argument("--template", type=Path,
                        default=Path(__file__).resolve().parent / "templates/georgia_outlook_report.html")
    args = parser.parse_args()
    summary = summarise(json.loads(args.results.read_text(encoding="utf-8")))
    if args.xlsx:
        write_xlsx(summary, args.xlsx)
    if args.html:
        write_html(summary, args.html, args.template)
    for line in findings(summary):
        print("-", line)


if __name__ == "__main__":
    main()
