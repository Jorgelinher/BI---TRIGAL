@echo off
set CLASP=%APPDATA%\npm\clasp.cmd
if "%TRIGAL_CLASP_USER%"=="" (
  echo Define TRIGAL_CLASP_USER con la cuenta Google autorizada para clasp.
  exit /b 1
)

echo Pulling CRM...
pushd "%~dp0crm"
call "%CLASP%" -u %TRIGAL_CLASP_USER% -P . pull
popd

echo.
echo Pulling Analytics ETL...
pushd "%~dp0analytics_etl"
call "%CLASP%" -u %TRIGAL_CLASP_USER% -P . pull
popd
