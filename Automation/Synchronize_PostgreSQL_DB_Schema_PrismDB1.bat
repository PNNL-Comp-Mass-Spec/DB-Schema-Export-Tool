@echo off
rem Batch file for exporting DDL for tables, views, functions, procedures, etc.
rem
rem DB_Schema_Export_Tool.exe can be found at https://ci.appveyor.com/project/PNNLCompMassSpec/db-schema-export-tool/build/artifacts
rem PgSQL_ExportOptions_Prismweb3.conf can be found at https://github.com/PNNL-Comp-Mass-Spec/DB-Schema-Export-Tool/blob/master/Automation/PgSQL_ExportOptions_Prismweb3.conf

F:
pushd "F:\Documents\Projects\DataMining\Database_Tools\DB_Schema_Export_Tool\Automation"

@echo on
..\bin\DB_Schema_Export_Tool.exe /conf:PgSQL_ExportOptions_Prismweb3.conf
..\bin\DB_Schema_Export_Tool.exe /conf:PgSQL_ExportOptions_PrismDB1.conf

popd