# DB Schema Export Tool

The DB Schema Export Tool exports SQL Server or PostgreSQL database objects as schema files.
Exported objects include tables, views, stored procedures, functions, and synonyms,
plus also database properties including database roles and logins.

Uses integrated authentication or a named user to connect to a Microsoft SQL Server database.

Uses a named user, password, and port to connect to a PostgreSQL database.

### Continuous Integration

The latest version of the application is available on the [AppVeyor CI server](https://ci.appveyor.com/project/PNNLCompMassSpec/db-schema-export-tool/build/artifacts)

[![Build status](https://ci.appveyor.com/api/projects/status/sdk00tc0l0seupic?svg=true)](https://ci.appveyor.com/project/PNNLCompMassSpec/db-schema-export-tool)

## Features

In addition to creating schema files for tables, views, stored procedures, functions, and synonyms,
the program can also export table data to create SQL files with an Insert statement for each row.

The DB Schema Export Tool has both a command line interface and a GUI mode.
Starting the program without any parameters will show the GUI.

The command line interface is useful for automatically exporting schema files on a regular basis.
Example command (wrapped here for readability):

```
DB_Schema_Export_Tool.exe 
  C:\Cached_DBSchema 
  /Server:Proteinseqs 
  /DBList:Manager_Control,Protein_Sequences 
  /Sync:"F:\Projects\Database_Schema\DMS" 
  /Git /Commit 
  /L /LogDir:Logs 
  /Data:ProteinSeqs_Data_Tables.txt
```

## Console switches

```
DB_Schema_Export_Tool.exe
 SchemaFileDirectory /Server:ServerName
 /DB:Database /DBList:CommaSeparatedDatabaseName
 [/DBUser:username] [/DBPass:username]
 [/PgUser:username] [/PgPass:password] [/PgPort:5432]
 [/DirectoryPrefix:PrefixText] [/NoSubdirectory] [/CreateDBDirectories]
 [/DataTables:TableDataToExport.txt] [/Map:ColumnMapping.txt]
 [/DateFilter:MinimumDate] [/TableFilterList]
 [/Schema:SchemaName] [/ExistingSchema:SchemaFileName]
 [/NoAutoData] [/ExportAllData] [/MaxRows:1000] [/NoData]
 [/SnakeCase] [/PgDump] [/PgInsert] [/PgInsertChunkSize:50000] 
 [/ServerInfo] [/NoSchema] [/ScriptLoad]
 [/Sync:TargetDirectoryPath] [/Git] [/Svn] [/Hg] [/Commit]
 [/L] [/LogFile:BaseName] [/LogDir:LogDirectoryPath] 
 [/Preview] [/Stats] [/Trace]
 [/ParamFile:ParamFileName.conf] [/CreateParamFile]
```

`SchemaFileDirectory` is the path to the directory where the schema files will be saved (aka the output directory)
* Optionally use named parameter `/OutputDir:DirectoryPath`

To process a single database, use `/Server` and `/DB`
* The server is assumed to be Microsoft SQL Server
* However, if `/PgUser` is provided (or `PgUser` is defined in the parameter file), will treat as a Postgres server
                      
Use `/DBList` to process several databases (separate names with commas)

Use `/DBUser` and `/DBPass` to specify a username and password for connecting to a SQL Server instance
* If not defined, will use Windows Integrated Authentication

Use `/PgUser` to define the username to use to connect to a PostgreSQL database

Use `/PgPass` to define the password for the PostgreSQL user
* As described below, you can create a file named `pgpass.conf` at `%APPDATA%\postgresql` to track passwords for PostgreSQL users
* The format is `server:port:database:username:password`
* pg_dump.exe will obtain the password from this file if it exists
* On Linux, the file is stored at `~/.pgpass`

Use `/PgPort` to specify the port to use for PostgreSQL
* Defaults to 5432

By default, a subdirectory named DBSchema__DatabaseName will be created below `SchemaFileDirectory/`
* Customize this prefix text using `/DirectoryPrefix`

Use `/NoSubdirectory` to disable auto creating a subdirectory for the database being exported
* Note: subdirectories will always be created if you use `/DBList` and specify more than one database

Use `/CreateDBDirectories:False` to disable creating a subdirectory for the schema files for each database

Use `/DataTables` or `/Data` to define a text file with table names (one name per line) for which the data 
should be exported. In addition to table names defined in `/Data`, there are default tables 
which will have their data exported; disable the defaults using `/NoAutoData`
* Also supports a multi-column, tab-delimited format
  * Put `<skip>` in the TargetTableName column to indicate that the table should not be included when using `/ExportAllData`
* File format

| SourceTableName  | TargetSchemaName | TargetTableName  | PgInsert  | KeyColumn(s)  |
|------------------|------------------|------------------|-----------|---------------|
| T_Log_Entries    | public           | t_log_entries    | false     |               |
| T_Job_Events     | cap              | t_job_Events     | false     |               |
| T_Job_State_Name | cap              | t_job_state_name | true      | job           |
| T_Users          | public           | t_users          | true      | user_id       |
| x_T_MgrState     | public           | `<skip>`         |           |               |

Tables with `PgInsert` set to true will have data insert commands formatted as PostgreSQL compatible 
`INSERT INTO` statements using the `ON CONFLICT (key_column) DO UPDATE SET` syntax
* This is only applicable when exporting data from SQL Server
* Use the `KeyColumn(s)` column to specify the primary key (or pair of unique keys) used for determining a conflict
  * Any columns that are not key columns will be updated when a conflict is found
  * If `PgInsert` is true, but the `KeyColumn(s)` column is empty, the program will look for an identity column on the source table
* Example generated SQL:

```PLpgSQL
INSERT INTO mc.t_mgr_types (mt_type_id, mt_type_name, mt_active)
OVERRIDING SYSTEM VALUE
VALUES 
  (1, 'Analysis', 1),
  (2, 'Archive', 0),
  (3, 'Archive Verify', 0),
  (4, 'Capture', 1),
  (5, 'Space', 1),
  (6, 'Data Import', 1)
ON CONFLICT (mt_type_id) 
DO UPDATE SET 
  mt_type_name = EXCLUDED.mt_type_name,
  mt_active = EXCLUDED.mt_active;
    
-- Set the sequence's current value to the maximum current ID
SELECT setval('mc.t_mgr_types_mt_type_id_seq', (SELECT MAX(mt_type_id) FROM mc.t_mgr_types));

-- View the current ID for the sequence
SELECT currval('mc.t_mgr_types_mt_type_id_seq');
```

Use `/Map` or `/ColumnMap` to define a tab-delimited text file mapping source column names to target column names, by table
* Use keyword `<skip>` in the `TargetColumnName` column to indicate that a source column should not be included in the output file
* If `/SnakeCase` is used to auto-change column names, mappings in this file will override the snake case auto-conversion
* File format:

| SourceTableName  | SourceColumnName | TargetColumnName |
|------------------|------------------|------------------|
| T_Analysis_Job   | AJ_jobID         | job              |
| T_Analysis_Job   | AJ_start         | start            |
| T_Analysis_Job   | AJ_finish        | finish           |
| t_users	       | name_with_prn	  | `<skip>`         |

Use `/TableFilterList` or `/TableNameFilter` to specify a table name (or comma separated list of names) to restrict table export operations. 
* This is useful for exporting the data from just a single table
* This parameter does not support reading names from a file; it only supports actual table names

Use `/DateFilter` or `/TableDataDateFilter` to define a tab-delimited text file that defines date filters to use when exporting data from tables.
* The data file will include the start date in the name, for example: `mc.t_log_entries_Data_Since_2020-01-01.sql`
* File format:

| SourceTableName          | DateColumnName   | MinimumDate  |
|--------------------------|------------------|--------------|
| T_Event_Log              | Entered          | 2020-01-01   |
| T_Log_Entries            | posting_time     | 2020-01-01   |
| T_ParamValue             | last_affected    | 2020-01-01   |
| T_ParamValue_OldManagers | last_affected    | 2016-01-07   |

Use `/Schema` or `/DefaultSchema` to define the default schema name to use when exporting data from tables
* Entries in the `/DataTables` file will override this default schema

Use `/ExistingSchema` or `/ExistingDDL` to define a text file that should be parsed to rename columns, based on data loaded from the `/ColumnMap` file
* Will create a new text file with CREATE TABLE blocks that include the new column names
  * Will skip columns with `<skip>`
* Will skip tables (and views, procedures, etc.) that are defined in the `/DataTables` file but have `<skip>` in the TargetTableName column

Use `/ExportAllData` or `/ExportAllTables` to export data from every table in the database
* Will skip tables (and views) that are defined in the `/DataTables` file but have `<skip>` in the TargetTableName column
* Ignored if `/NoTableData` is true

Use `/MaxRows` to define the maximum number of data rows to export
* Defaults to 1000
* Use 0 to export all rows
* If you use `/ExportAllData` but do not specify `/MaxRows`, the program will auto set `/MaxRows` to 0

Use `/NoTableData` or `/NoData` to prevent any table data from being exported
* This parameter is useful when processing an existing DDL file with `/ExistingDDL`

Use `/SnakeCase` to auto change column names from Upper_Case and UpperCase to lower_case when exporting data from tables
* Also used for table names when exporting data from tables
* Entries in the `/DataTables` and `/ColumnMap` files will override auto-generated snake_case names

Use `/PgDump` or `/PgDumpData` to specify that exported data should use `COPY` commands instead of `INSERT INTO` statements
* With SQL Server databases, table data will be exported using pg_dump compatible COPY commands when `/PgDump` is used
* With PostgreSQL data, table data will be exported using the pg_dump application
* With SQL Server data and PostgreSQL data, if `/PgDump` is not provided, data is exported with `INSERT INTO` statements
* With SQL Server data, if `/PgDump` is provided, but the `/DataTables` file has `true` in the `PgInsert` column, `INSERT INTO` statements will be used for that table

The `/PgInsert` applies when exporting data from SQL Server database tables
* While reading the TableDataToExport.txt file, default the PgInsert column to true when `/PgInsert` is provided, meaning table data will be exported via INSERT INTO statements, as mentioned above
* Ignored for PostgreSQL data

Use `PgInsertChunkSize` to specify the number of values to insert at a time when PgInsert is true for a table
* Only applicable when exporting data from SQL Server

Use `/ServerInfo` to export server settings, logins, and SQL Server Agent jobs

Use `/NoSchema` to skip exporting schema (tables, views, functions, etc.)

Use `/ScriptLoad` or `/Script` to create a bash script file for loading exported table data into a PostgreSQL database

Use `/Sync` to copy new/changed files from the output directory to an alternative directory
* This is advantageous to prevent file timestamps from getting updated every time the schema is exported

Use `/Git` to auto-update any new or changed files using Git

Use `/Svn` to auto-update any new or changed files using Subversion

Use `/Hg`  to auto-update any new or changed files using Mercurial

Use `/Commit` to commit any updates to the repository

Use `/L` to log messages to a file

Use `/LogFile`' to specify the base name for the log file
* Log files will be named using the base name, followed by the current date

Use `/LogDir` or `/LogDirectory` to specify the directory to save the log file in
* By default, the log file is created in the current working directory

Use `/Preview` to count the number of database objects that would be exported

Use `/Stats` to show (but not log) export stats

Use `/Trace` to show additional debug messages at the console

The processing options can be specified in a parameter file using `/ParamFile:Options.conf` or `/Conf:Options.conf`
* Define options using the format `ArgumentName=Value`
* Lines starting with `#` or `;` will be treated as comments
* Additional arguments on the command line can supplement or override the arguments in the parameter file

Use `/CreateParamFile` to create an example parameter file
* By default, the example parameter file content is shown at the console
* To create a file named Options.conf, use `/CreateParamFile:Options.conf`

## Software Dependencies

This program leverages the SQL Server Management Objects (SMO) Framework
* All of the required DLLs should be included in the DBSchemaExportTool .zip file

In order to use the `/Git,` `/Svn,` or `/Hg` switches, you need the following software installed
and the executables present at a specific location.

Git
* `C:\Program Files\Git\bin\git.exe`
* Installed with 64-bit Git for Windows, available at https://git-scm.com/download/win

Subversion: 
* `C:\Program Files\TortoiseSVN\bin\svn.exe`
* Installed with 64-bit Tortoise SVN, available at https://tortoisesvn.net/downloads.html

Mercurial
* `C:\Program Files\TortoiseHg\hg.exe`
* Installed with 64-bit Tortoise Hg,  available at https://tortoisehg.bitbucket.io/download/index.html

### PostgreSQL

Schema export from PostgreSQL databases utilizes the pg_dump utility
* pg_dump.exe is available as part of the PostgreSQL for Windows installer from the EDB website
  * https://www.enterprisedb.com/downloads/postgres-postgresql-downloads
* Run the installer
  * De-select installing PostgreSQL Server
* Copy `pg_dump.exe` plus several DLLs from `C:\Program Files\PostgreSQL\12\bin` to the directory with the DB_Schema_Export_Tool executable
  * See batch file Lib\Copy_pg_dump_files.bat

File `pgpass.conf` at `%APPDATA%\postgresql` can be used to store the password for the PostgreSQL user
* On Linux, the file is stored at `~/.pgpass`

```
mkdir %APPDATA%\postgresql
echo prismweb3:5432:dms:dmsreader:dmspasswordhere > %APPDATA%\postgresql\pgpass.conf
```

Example call to pg_dump:
```
pg_dump.exe -h prismweb3 -p 5432 -U dmsreader -d dms --schema-only --format=p --file=_AllObjects_.sql
```

Note that on Windows the .sql file created by pg_dump.exe includes extra carriage returns, resulting in `CR CR LF` instead of `CR LF`
* DB_Schema_Export_Tool.exe removes the extra carriage returns while parsing the SQL dump file

## Contacts

Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA) \
E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov\
Website: https://omics.pnl.gov/ or https://www.pnnl.gov/sysbio/ or https://panomics.pnnl.gov/

## License

The DB Schema Export Tool is licensed under the 2-Clause BSD License; 
you may not use this file except in compliance with the License.  You may obtain 
a copy of the License at https://opensource.org/licenses/BSD-2-Clause

Copyright 2019 Battelle Memorial Institute
