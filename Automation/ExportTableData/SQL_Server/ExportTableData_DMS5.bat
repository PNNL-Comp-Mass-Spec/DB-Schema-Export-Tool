@echo off
echo Export table data from SQL Server, creating files at F:\Cached_DBSchema_SQLServer_to_PgSql\DBSchema__DMS5
echo.
echo Note that the one or more tables may need to be truncated prior to importing new data to avoid Unique Constraint conflicts
echo In particular:
echo   t_cached_instrument_dataset_type_usage

If Not [%1] == [] Set ExePath=%1

If [%1] == [] Set ExePath=..\..\..\DB_Schema_Export_Tool\bin\DB_Schema_Export_Tool.exe

@echo on
%ExePath% /conf:DataExportOptions_DMS5.conf

rem %ExePath% /conf:DataExportOptions_DMS5_Specific_Tables.conf

@echo off
If [%1] == [] pause
