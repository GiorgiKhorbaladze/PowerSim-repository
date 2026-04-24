# PowerSim — Google Colab quick start

Use Colab when you don't want to install Python locally. The HTML stays on
your laptop — only the solver runs in Colab.

## 1. Open a new Colab notebook

```python
# Cell 1 — install
!pip install -q pyomo highspy pandas openpyxl xlsxwriter numpy
```

## 2. Upload the PowerSim solver bundle

Easiest: clone this repo from a public mirror.

```python
# Cell 2 — fetch repo
!git clone https://github.com/<your-org>/<this-repo>.git powersim
%cd powersim
```

Or upload the four solver files via the Colab Files pane:
`solver/powersim_solver.py`, `solver/powersim_dataio.py`,
`solver/powersim_asset_mapper.py`, `schema/powersim_schema.py`.

## 3. Provide project data

Either (a) the synthetic demo:

```python
# Cell 3a — synthesize demo project_data
!python scripts/build_demo_project.py --out project_data
```

…or (b) upload your real GSE files (same filenames as in
[`docs/HAPPY_PATH.md`](HAPPY_PATH.md)) under `project_data/`.

## 4. Upload your input JSON from the HTML UI

Use the Colab Files pane to upload `powersim_input.json` exported from
the HTML. Place it at the repo root (or anywhere — pass `--input` to the
solver).

## 5. Run the solver

```python
# Cell 4 — solve
!python solver/powersim_solver.py \
    --input  powersim_input.json \
    --output powersim_results.json \
    --excel  powersim_results.xlsx
```

For a horizon-driven build-and-solve in one shot:

```python
!python scripts/run_horizon.py \
    --project-dir project_data \
    --config      tests/stage1_smoke_fleet.json \
    --hours       720 \
    --out-dir     out/run_720h
```

## 6. Download the results

```python
# Cell 5 — download
from google.colab import files
files.download("powersim_results.json")
files.download("powersim_results.xlsx")
```

## 7. Import into the HTML

Open `html/PowerSim_v4.html` locally and click **📥 Import Results JSON**.
That's it.
