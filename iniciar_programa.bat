@echo off
REM ======================================================================
REM  Hospital W 2.0 - Levanta el programa con doble clic.
REM  Abre el navegador en http://127.0.0.1:5000 a los pocos segundos.
REM  Para apagarlo: cerrar esta ventana (o Ctrl+C).
REM ======================================================================
title Hospital W 2.0 - servidor
REM %~dp0 = la carpeta donde esta este archivo (funciona aunque la ruta tenga espacios).
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
    echo No se encontro el entorno virtual .venv en esta carpeta.
    echo Instalalo primero con:  python -m venv .venv  y  .venv\Scripts\pip install -r requirements.txt
    pause
    exit /b 1
)

REM Abre el navegador dentro de 6 segundos, cuando el servidor ya este escuchando.
start "" /min cmd /c "timeout /t 6 /nobreak >nul & start http://127.0.0.1:5000"

echo Iniciando Hospital W 2.0... (no cierres esta ventana mientras lo uses)
".venv\Scripts\python.exe" app.py
pause
