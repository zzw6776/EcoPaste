@echo off
setlocal

cd /d "%~dp0"

set "ECOPASTE_DEVICE=192.168.50.187:35555"

where pnpm.cmd >nul 2>nul
if errorlevel 1 (
  echo pnpm.cmd was not found in PATH.
  pause
  exit /b 1
)

where adb.exe >nul 2>nul
if errorlevel 1 (
  echo adb.exe was not found in PATH.
  pause
  exit /b 1
)

echo Building EcoPaste Android arm64 release APK...
call pnpm.cmd android:build:release
if errorlevel 1 (
  echo.
  echo Android build failed.
  pause
  exit /b 1
)

for /f "delims=" %%V in ('powershell.exe -NoProfile -Command "(Get-Content package.json -Raw | ConvertFrom-Json).version"') do set "ECOPASTE_VERSION=%%V"
if not defined ECOPASTE_VERSION (
  echo.
  echo Failed to read the EcoPaste version from package.json.
  pause
  exit /b 1
)

set "ECOPASTE_APK=artifacts\android\EcoPaste-%ECOPASTE_VERSION%-android-arm64-release.apk"
if not exist "%ECOPASTE_APK%" (
  echo.
  echo APK was not found: %ECOPASTE_APK%
  pause
  exit /b 1
)

echo.
echo Connecting to %ECOPASTE_DEVICE%...
adb.exe connect "%ECOPASTE_DEVICE%"
adb.exe -s "%ECOPASTE_DEVICE%" get-state >nul 2>nul
if errorlevel 1 (
  echo.
  echo Unable to connect to %ECOPASTE_DEVICE%.
  pause
  exit /b 1
)

echo Installing %ECOPASTE_APK%...
adb.exe -s "%ECOPASTE_DEVICE%" install -r "%ECOPASTE_APK%"
if errorlevel 1 (
  echo.
  echo APK installation failed.
  pause
  exit /b 1
)

echo.
echo EcoPaste was installed successfully on %ECOPASTE_DEVICE%.
pause
exit /b 0
