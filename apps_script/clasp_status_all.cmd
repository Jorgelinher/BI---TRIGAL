@echo off
set CLASP=%APPDATA%\npm\clasp.cmd
if "%TRIGAL_CLASP_USER%"=="" (
  echo Define TRIGAL_CLASP_USER con la cuenta Google autorizada para clasp.
  exit /b 1
)

echo === Authorized user ===
call "%CLASP%" -u %TRIGAL_CLASP_USER% show-authorized-user

echo.
echo === CRM status ===
pushd "%~dp0crm"
call "%CLASP%" -u %TRIGAL_CLASP_USER% -P . show-file-status
popd

echo.
echo === Analytics ETL status ===
pushd "%~dp0analytics_etl"
call "%CLASP%" -u %TRIGAL_CLASP_USER% -P . show-file-status
popd
