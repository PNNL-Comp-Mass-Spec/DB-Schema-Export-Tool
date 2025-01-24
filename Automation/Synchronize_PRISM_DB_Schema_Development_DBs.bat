@echo off
rem Can specify data tables for export using /data:F:\Cached_DBSchema\GigasaxTableData.txt 
rem Can auto-commit with /Commit

F:
pushd "F:\Documents\Projects\DataMining\Database_Tools\DB_Schema_Export_Tool\bin"

@echo on
DB_Schema_Export_Tool.exe F:\Cached_DBSchema /server:Gigasax      /DBList:DMS_Capture_T3,DMS_Data_Package_T3,DMS_Pipeline_T3,DMS5_T3  /sync:"F:\Documents\Projects\DataMining\Database_Schema\DMS_Development"
DB_Schema_Export_Tool.exe F:\Cached_DBSchema /server:Proteinseqs  /DBList:Protein_Sequences_T3,Manager_Control_T3 /sync:"F:\Documents\Projects\DataMining\Database_Schema\DMS_Development"

popd
