@echo off
rem PowerSim launcher for a source checkout (no administrator rights needed).
rem First run: creates .venv with Python 3.10-3.12 and installs PowerSim.
rem Later runs: starts the server and opens the browser.
cd /d "%~dp0"
if not exist ".venv\Scripts\powersim.exe" (
  echo Setting up PowerSim for the first time. This takes a few minutes...
  py -3.11 -m venv .venv 2>nul || py -3.12 -m venv .venv 2>nul || py -3.10 -m venv .venv
  if not exist ".venv\Scripts\python.exe" (
    echo Python 3.10, 3.11 or 3.12 was not found. Install it for your user from python.org and run this file again.
    pause
    exit /b 1
  )
  ".venv\Scripts\python.exe" -m pip install .
  if errorlevel 1 (
    echo Installation failed. See the messages above.
    pause
    exit /b 1
  )
)
if "%POWERSIM_WORKSPACE%"=="" set "POWERSIM_WORKSPACE=%~dp0powersim_workspace"
echo Starting PowerSim. Keep this window open while you use it; close it to stop PowerSim.
start "" /b cmd /c "timeout /t 4 >nul & start "" http://127.0.0.1:8000/"
".venv\Scripts\powersim.exe" serve --workspace "%POWERSIM_WORKSPACE%"
pause
