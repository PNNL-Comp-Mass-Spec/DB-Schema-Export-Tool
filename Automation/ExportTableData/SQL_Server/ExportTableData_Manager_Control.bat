@echo off
echo Export table data from SQL Server, creating files at F:\Cached_DBSchema_SQLServer_to_PgSql\DBSchema__Manager_Control

If Not [%1] == [] Set ExePath=%1

If [%1] == [] Set ExePath=..\..\..\DB_Schema_Export_Tool\bin\DB_Schema_Export_Tool.exe

@echo on
%ExePath% /conf:DataExportOptions_ManagerControl.conf

@echo off
If [%1] == [] pause
