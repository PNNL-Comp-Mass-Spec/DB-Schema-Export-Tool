@echo off
echo Export table data from SQL Server, creating files at F:\Cached_DBSchema_SQLServer_to_PgSql\DBSchema__DMS_Capture

If Not [%1] == [] Set ExePath=%1

If [%1] == [] Set ExePath=..\..\..\DB_Schema_Export_Tool\bin\DB_Schema_Export_Tool.exe

@echo on
%ExePath% /conf:DataExportOptions_DMS_Capture.conf

@echo off
If [%1] == [] pause
