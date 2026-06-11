# Clasp setup - Trigal Sur

Este workspace separa los Apps Script en dos proyectos:

- `crm`: script CRM operativo.
- `analytics_etl`: script Analytics ETL + Dashboard Web.

## Configuracion local privada

Este repositorio no guarda correos, IDs reales de Apps Script ni IDs de deployment.

Antes de usar estos scripts, define variables de entorno locales:

```bat
set TRIGAL_CLASP_USER=your-google-account@example.com
set TRIGAL_DASHBOARD_DEPLOYMENT_ID=your-web-app-deployment-id
```

Ademas, copia los archivos `.clasp.example.json` a `.clasp.json` y coloca alli los Script IDs reales:

```bat
copy apps_script\crm\.clasp.example.json apps_script\crm\.clasp.json
copy apps_script\analytics_etl\.clasp.example.json apps_script\analytics_etl\.clasp.json
```

Los `.clasp.json` reales estan ignorados por Git.

## Login recomendado

Abrir **CMD**, no PowerShell, y ejecutar:

```bat
cd C:\Users\jorge\Desktop\BI-TRIGAL
%APPDATA%\npm\clasp.cmd -u %TRIGAL_CLASP_USER% login
```

Cuando se abra el navegador, elegir explicitamente la cuenta configurada en `TRIGAL_CLASP_USER`.

Verificar:

```bat
apps_script\clasp_status_all.cmd
```

Debe mostrar la cuenta configurada:

```text
You are logged in as ...
```

## Primer pull seguro

Antes de hacer push:

```bat
apps_script\clasp_pull_all.cmd
```

Esto trae los archivos remotos y permite revisar si hay diferencias antes de subir.

## Push

Solo Analytics ETL + Dashboard:

```bat
apps_script\clasp_push_analytics.cmd
```

Publicar una nueva version del dashboard sobre el deployment canonico:

```bat
apps_script\clasp_deploy_dashboard.cmd
```

Solo CRM:

```bat
apps_script\clasp_push_crm.cmd
```
