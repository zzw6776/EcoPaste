@echo off
setlocal

cd /d "%~dp0"

if exist "node_modules\.bin\tsx.cmd" if exist "node_modules\.bin\tauri.cmd" goto start

echo Installing Windows dependencies...
call pnpm.cmd install --frozen-lockfile
if errorlevel 1 (
  echo.
  echo Failed to install Windows dependencies.
  pause
  exit /b 1
)

:start
call pnpm.cmd tauri dev
set "ECOPASTE_EXIT_CODE=%errorlevel%"

if not "%ECOPASTE_EXIT_CODE%"=="0" (
  echo.
  echo EcoPaste development process exited with code %ECOPASTE_EXIT_CODE%.
  pause
)

exit /b %ECOPASTE_EXIT_CODE%
