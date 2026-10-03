$ErrorActionPreference = 'Stop'
python -m PyInstaller --noconfirm --clean --onedir --name PowerSim `
  --collect-all powersim --collect-all solver --collect-all highspy --collect-all pyomo `
  --add-data "html;html" --windowed src/powersim/desktop.py
iscc packaging/windows/PowerSim.iss
