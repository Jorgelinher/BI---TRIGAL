@echo off
setlocal enabledelayedexpansion
set CLASP=%APPDATA%\npm\clasp.cmd
if "%TRIGAL_CLASP_USER%"=="" (
  echo Define TRIGAL_CLASP_USER con la cuenta Google autorizada para clasp.
  exit /b 1
)
if "%TRIGAL_DASHBOARD_DEPLOYMENT_ID%"=="" (
  echo Define TRIGAL_DASHBOARD_DEPLOYMENT_ID con el deployment canonico de la Web App.
  exit /b 1
)
set DESCRIPTION=TRIGAL SUR DASHBOARD WEB CANONICO

pushd "%~dp0analytics_etl"
call "%CLASP%" -u %TRIGAL_CLASP_USER% -P . push --force
for /f "tokens=3" %%V in ('"%CLASP%" -u %TRIGAL_CLASP_USER% -P . version "Dashboard web canonico"') do set VERSION=%%V
if "%VERSION%"=="" (
  echo No se pudo crear version.
  popd
  exit /b 1
)
call "%CLASP%" -u %TRIGAL_CLASP_USER% -P . update-deployment %TRIGAL_DASHBOARD_DEPLOYMENT_ID% -V %VERSION% -d "%DESCRIPTION%"
call "%CLASP%" -u %TRIGAL_CLASP_USER% -P . open-web-app %TRIGAL_DASHBOARD_DEPLOYMENT_ID%
popd
