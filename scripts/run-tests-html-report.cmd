@echo off
setlocal
cd /d "%~dp0.."
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0run-tests-html-report.ps1" %*
exit /b %ERRORLEVEL%
