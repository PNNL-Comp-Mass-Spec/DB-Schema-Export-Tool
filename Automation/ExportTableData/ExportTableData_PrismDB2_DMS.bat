@echo off
echo Export table data from PostgreSQL (for every table), creating files at F:\Cached_DBData_PgSql\DBSchema__DMS
echo.
echo Note that one or more tables may need to be truncated prior to importing new data to avoid Unique Constraint conflicts
echo In particular:
echo   t_cached_instrument_dataset_type_usage

If Not [%1] == [] Set ExePath=%1

rem If [%1] == [] Set ExePath=..\..\..\DB_Schema_Export_Tool\bin\DB_Schema_Export_Tool.exe
If [%1] == [] Set ExePath=F:\Documents\Projects\DataMining\Database_Tools\DB_Schema_Export_Tool\bin\DB_Schema_Export_Tool.exe


@echo on
%ExePath% /conf:PrismDB2_DMS_DataExportOptions.conf

@echo off
If [%1] == [] pause
