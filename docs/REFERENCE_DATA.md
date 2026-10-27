# Plant-parameter reference catalogue

`data/reference/plant_parameters/` holds a normalized transcription of the user-provided workbook `Power Plant Parameters English.xlsx` (SHA-256 `f8c8b90f61ccbfcda12ea7b4d353c96fa27aac3bc9e58a8bfb7ee56a845ff4ca`, 138 sheets).

**It is reference data, not approved study input.** Values must be reviewed against approved sources before they are used in a calculation. PowerSim never adds catalogue values to a study automatically.

## Files

| File | Source sheet | Records |
|---|---|---|
| `manifest.json` | workbook inventory, SHA-256, record counts | - |
| `stations_by_location.json` | Stations by Location | 139 rows: 134 plants, 2 section headings, 3 aggregate totals |
| `reservoirs.json` | Reservoirs | 83 |
| `hydropower_assets.json` | Hydropower plants | 122 rows in two sections: run-of-river (90) and storage (32) |
| `hpp_summary.json` | HPP Summary | 118 |
| `plant_detail_sheets.json` | the 134 per-plant technical sheets | 134 sheets, all non-empty rows kept |

The workbook has 138 sheets: four summary sheets and 134 plant technical sheets.

Each record keeps `source_sheet`, the 1-based `source_row`, a `record_type` and the raw cell `values`; unexpected extra cells are kept in `extra_cells`. Nothing is converted, filled in or reconciled during transcription.

`scripts/build_reference_catalogue.py <workbook> --out data/reference/plant_parameters` regenerates the files. The script exists because simple one-header exports lose data:

- `Hydropower plants` has two sections with different two-row headers. A single-header export merges them under the wrong column names and reads the storage section's header row as "a plant row without a name". In the section-aware export there is no nameless plant row.
- `HPP Summary` has a two-row header. A single-header export uses the first plant (Khrami HPP-1) as the header and drops it as a record.
- `Stations by Location` mixes plants with section headings and totals. The two "records without installed and available capacity" are the *Solar and Wind Power Plants* and *Thermal Power Plants* headings; they are reported and cannot be imported.

## Quality checks (`powersim.reference.catalogue`)

| Code | Severity | Meaning |
|---|---|---|
| `missing_installed_capacity` | error | installed and/or available capacity missing |
| `available_exceeds_installed` | error | available > installed (for example Shuakhevi HPP, Gardabani TPP 1/2) |
| `unit_capacity_mismatch` | error | unit string does not add up to installed capacity (for example Aragvi 2 HPP `1x95` vs 1.95 MW) |
| `capacity_conflict` | error | installed capacity differs from the HPP summary or the plant technical sheet |
| `reservoir_volume_order`, `reservoir_level_order`, `negative_value` | error | inconsistent reservoir volumes/levels |
| `missing_plant_name` | error | record without a name |
| `duplicate_name_in_table` | warning | same normalized name more than once in a table (20 hydropower names, including Dariali, Ortachala, Shuakhevi, Zahesi, Gumat HPPi) |
| `naming_inconsistency` | warning | station and hydropower names match only as a spelling variant or possible match (for example `Gumati HPP-1` / `Gumat HPPi`, `Rioni HPP` / `Rion HPP`) |
| `unit_count_conflict` | warning | unit counts differ between sources |
| `unparsed_unit_string` | warning | unit string kept verbatim |
| `comma_decimal`, `compound_value` | info | `3x37,6` read as 37.6; `292/195` not parsed |
| `not_a_plant_record` | info | section heading or total |

Tolerances: capacities conflict when they differ by more than 2 % **and** more than 0.05 MW. Names are matched after removing generic words (HPP, TPP, PP, ...) and comparing word stems; `exact` is the only identity, while `variant` and `possible` (stem similarity at least 0.85) are reported for user confirmation.

## Import (`powersim.reference.importer`)

Only station plant records can be imported, as `thermal`, `hydro_ror`, `hydro_reg`, `solar` or `wind` assets. The review maps:

| PowerSim field | Source / rule | Status |
|---|---|---|
| `id` | `ref-` + plant-name slug | derived |
| `kind` | from Regulation Type (Run-of-river -> hydro_ror; Annual/Daily -> hydro_reg; Thermal -> thermal; Renewable + name -> solar/wind) | derived, importer assumption warning |
| `legacy_extensions.name` | Power Plant Name | mapped |
| `capacity_max_mw` | Installed Capacity (MW) | mapped; `conflict` when a capacity error exists |
| `capacity_min_mw` | 0 | assumed, warning |
| `legacy_extensions.fuel_type` | not in the workbook | thermal: blocking `missing_fuel` until overridden |
| `enabled` | false | assumed |
| solver inputs absent from the workbook | cost basis / availability profile / inflow and reservoir model | `unmapped_required_field` warning |

All other columns, parsed unit configurations, capacity evidence from other tables and matched records are attached as reference metadata in the asset's provenance.

Gates:

- Any error blocks the import. Errors are resolved only with a manual override that has a reason (for example a `capacity_max_mw` override resolves capacity conflicts). The decision is kept in provenance.
- Warnings must be explicitly acknowledged.
- Imported assets are disabled with `review_status = requires_user_review`. The project validator blocks runs when an enabled reference asset is unreviewed, or lacks a cost basis (thermal), availability profile (run-of-river/solar/wind) or inflow/reservoir model (reservoir hydro).
- Provenance records the workbook, SHA-256, sheet, row, record id, import id, timestamp, importer version, the importing user (optional), field origins, overrides, acknowledged warnings and resolved issues. The project metadata keeps an import history, and the project revision increases with the previous fingerprint as parent.
