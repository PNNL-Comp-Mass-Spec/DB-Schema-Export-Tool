@echo off
echo Export table data from SQL Server, creating files at F:\Cached_DBSchema_SQLServer_to_PgSql\DBSchema__Protein_Sequences

If Not [%1] == [] Set ExePath=%1

If [%1] == [] Set ExePath=..\..\..\DB_Schema_Export_Tool\bin\DB_Schema_Export_Tool.exe

@echo on
%ExePath% /conf:DataExportOptions_Protein_Sequences.conf

@echo off
echo.
echo.
echo Updating target table names in the t_migrate_protein data files to use the correct name
echo.

pushd F:\Cached_DBSchema_SQLServer_to_PgSql\DBSchema__Protein_Sequences\pc

echo Replacing "pc"."t_migrate_protein
echo with      "pc"."t_protein

@echo on
cat pc.t_migrate_protein_collection_members_cached_Data.sql | sed -r "s/""pc""\.""t_migrate_protein/""pc""\.""t_protein/g" > pc.t_protein_collection_members_cached_Data.sql
cat pc.t_migrate_protein_collection_members_Data.sql        | sed -r "s/""pc""\.""t_migrate_protein/""pc""\.""t_protein/g" > pc.t_protein_collection_members_Data.sql
cat pc.t_migrate_protein_headers_Data.sql                   | sed -r "s/""pc""\.""t_migrate_protein/""pc""\.""t_protein/g" > pc.t_protein_headers_Data.sql
cat pc.t_migrate_protein_names_Data.sql                     | sed -r "s/""pc""\.""t_migrate_protein/""pc""\.""t_protein/g" > pc.t_protein_names_Data.sql
cat pc.t_migrate_proteins_Data.sql                          | sed -r "s/""pc""\.""t_migrate_protein/""pc""\.""t_protein/g" > pc.t_proteins_Data.sql

@echo off
if not exist _Trash mkdir _Trash
move pc.t_migrate* _Trash\
cd ..

echo Updating filenames in LoadData.sh

@echo on
cat LoadData.sh | sed -r "s/pc.t_migrate_protein/pc.t_protein/g" > LoadData_FixedNames.sh

@echo off
if exist LoadData.sh.old del LoadData.sh.old
move LoadData.sh LoadData.sh.old
move LoadData_FixedNames.sh LoadData.sh

echo Done
echo.
echo.

echo After importing the data, use this query to mark old protein collections as deleted
echo.
echo UPDATE pc.t_protein_collections
echo SET collection_state_id = 5
echo WHERE collection_state_id ^< 5 AND
echo       NOT EXISTS 
echo           ( SELECT DISTINCT PCM.protein_collection_id
echo             FROM pc.t_protein_collection_members PCM
echo             WHERE PCM.protein_collection_id = pc.t_protein_collections.protein_collection_id );
echo.

popd

@echo off
If [%1] == [] pause
