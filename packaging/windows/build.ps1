$ErrorActionPreference = 'Stop'
python -m PyInstaller --noconfirm --clean --onedir --name PowerSim `
  --collect-all powersim --collect-all solver --collect-all highspy --collect-all pyomo `
  --add-data "data/reference/plant_parameters;data/reference/plant_parameters" `
  --add-data "samples/projects;samples/projects" --add-data "samples/application_compact_demo.json;samples" --windowed src/powersim/desktop.py
iscc packaging/windows/PowerSim.iss
