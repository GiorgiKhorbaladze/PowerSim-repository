# PowerSim — Troubleshooting

## Installation

| Symptom | Fix |
|---------|-----|
| `ModuleNotFoundError: pyomo` | `pip install -r requirements.txt` (in your venv) |
| `ImportError: highspy` | `pip install --upgrade highspy` (Pyomo finds it via the appsi solver) |
| `LookupError: 'utf-8' codec` on Georgian filenames | Make sure your shell locale supports UTF-8: `export LC_ALL=en_US.UTF-8` (Linux/macOS) |

## Project data discovery

The dataio loader searches a fixed set of subdirectories
(`renewables/`, `re/`, `data/`, `profiles/`, `inputs/`, …) and finally
does a recursive `rglob` for the requested filename. Common errors:

| Symptom | Fix |
|---------|-----|
| `DataIOError: hydro file for scenario 'A_mean' not found under …` | Make sure `2026_A_historical_mean.csv` is somewhere under `--project-dir` (root or any standard subfolder) |
| `DataIOError: renewables file (source=wind, site=Mta_Sabueti, …) not found` | Filename must follow `Wind_<Site>_2026_<Scenario>.csv`; underscores or spaces in `<Site>` are both accepted |
| `DataIOError: workbook 'GSE_CharYear_Normalized_1.xlsx' not found` | Required for `demand_mode = "shape_times_annual"`. Switch to `"absolute"` and supply `GSE_PLEXOS_sc2_2026_1.xlsx` if you don't have the shape file |

## Schema validation

| Symptom | Fix |
|---------|-----|
| `errors: ["schema_version 'X' unsupported"]` | Re-export from the latest HTML — it emits v1.2. v1.0 / v1.1 still load with a warning |
| `errors: ["profiles.demand length N ≠ 8760"]` | Demand must always cover a full non-leap year. Pad with the same daily profile or upload an 8760-row file |
| `errors: ["hydro 'X': cascade_upstream 'Y' not in assets"]` | Asset id mismatch. The id is the lowercase ASCII transliteration of the Georgian name (see asset mapper preview) |

## Solver

| Symptom | Fix |
|---------|-----|
| Solve time > 5 min on 720h | Loosen `mip_gap` (try 0.02 → 0.03) or shrink `rolling_window_h` (168 → 120) |
| `closure_gap > 0.5%` | Tighten `mip_gap` to 0.005 — closure tracks LP optimality |
| Large `total_unserved_mwh` | Either real adequacy gap (intended) OR the import asset's `pmax_profile` is too low. Check `by_unit_summary.import_*.energy_mwh` vs `pmax_profile`-implied capacity. |
| All-zero hydro dispatch | Check `water_value`: if it's > the marginal cost of the cheapest fossil unit, the solver will prefer fossil. Lower it via the reservoir override CSV |
| Excel export fails with `MergeError` | An asset id is colliding with another after column-name truncation; recent versions key columns by asset id which is unique by construction. Update to ≥ solver 1.1.0 |

## HTML UI

| Symptom | Fix |
|---------|-----|
| Results tab shows "შედეგები ჯერ არ ჩაიტვირთა" after import | Open the browser DevTools console; the import handler logs validation errors there |
| Charts don't render | The CDN-hosted ApexCharts script may be blocked. Save `apexcharts.min.js` locally and update the `<script src=…>` tag in `PowerSim_v4.html` |
| Compare tab shows zeros | At least one of the loaded files isn't a results JSON — confirm both files have a `system_summary` key at top level |
| Georgian text renders as boxes | Ensure your browser has `Noto Sans Georgian` available (the HTML pulls it from Google Fonts; if blocked, install locally) |

## Performance budget reference

| Horizon | Fleet | mip_gap | Wallclock (single core) |
|---------|-------|---------|-------------------------|
| 168 h   | 8 assets   | 0.01 | ~5 s |
| 720 h   | 8 assets   | 0.02 | ~10 s (rolling 168 h × 5 windows) |
| 8760 h  | 8 assets   | 0.03 | ~60 s (rolling 168 h × 53 windows) |
| 168 h   | 131 plants | 0.01 | ~50–80 s |
| 720 h   | 131 plants | 0.02 | ~3–5 min |
| 8760 h  | 131 plants | 0.02 | ~30–60 min |

If your numbers are far off, profile with `--time-limit 1200` and watch
which window takes the longest — usually a single dry-MC scenario with a
binding gas cap.
