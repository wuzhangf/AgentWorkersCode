@echo off
rem office-demo one-click launcher (Windows). Double-click or run from a shell.
rem A new run kills leftovers from the previous run, clears old characters,
rem and reuses the pi worker if it is still alive.
cd /d "%~dp0"

if not defined MODEL_API_KEY (
  if exist ".local\env.cmd" call ".local\env.cmd"
)
if not defined MODEL_API_KEY (
  echo [demo] MODEL_API_KEY is not set.
  echo [demo] Export it, or create .local\env.cmd that sets it, then run again.
  pause
  exit /b 1
)

node runner.mjs
if errorlevel 1 (
  echo.
  echo [demo] exited with an error - see messages above.
  pause
)
