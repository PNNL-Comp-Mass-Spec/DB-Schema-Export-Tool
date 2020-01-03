rem Can specify data tables for export using /data:F:\Cached_DBSchema\GigasaxTableData.txt 
rem Can auto-commit with /Commit

F:
cd "F:\Documents\Projects\DataMining\Database_Tools\DB_Schema_Export_Tool\Automation"

..\bin\DB_Schema_Export_Tool.exe /conf:PgSQL_ExportOptions_ManagerControl.conf
