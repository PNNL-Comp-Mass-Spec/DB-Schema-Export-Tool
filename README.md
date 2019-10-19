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
  /server:Proteinseqs 
  /DBList:Manager_Control,Protein_Sequences 
  /sync:"F:\Projects\Database_Schema\DMS" 
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
 [/DirectoryPrefix:PrefixText] [/NoSubdirectory]
 [/Data:TableDataToExport.txt] [/NoAutoData] [/PgDD]
 [/ServerInfo] [/NoSchema]
 [/Sync:TargetDirectoryPath] [/Git] [/Svn] [/Hg] [/Commit]
 [/L[:LogFilePath]] [/LogDir:LogDirectoryPath] [/Preview] [/Stats]
```

SchemaFileDirectory is the path to the directory where the schema files will be saved

To process a single database, use `/Server` and `/DB`

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

Use `/NoSubdirectory` to disable auto creating a subdirectory for the database being exported. 
* Note: subsirectories will always be created if you use `/DBList` and specify more than one database

Use `/Data` to define a text file with table names (one name per line) for which the data 
should be exported. In addition to table names defined in `/Data`, there are default tables 
which will have their data exported; disable the defaults using `/NoAutoData`

Use `/PgDD` or `/PgDumpData` to specify that pg_dump should be used to export table data from PostgreSQL databases
* By default, uses Npgsql.dll

Use `/ServerInfo` to export server settings, logins, and SQL Server Agent jobs

Use `/NoSchema` to skip exporting schema (tables, views, functions, etc.)

Use `/Sync` to copy new/changed files from the output directory to an alternative directory. 
* This is advantageous to prevent file timestamps from getting updated every time the schema is exported

Use `/Git` to auto-update any new or changed files using Git

Use `/Svn` to auto-update any new or changed files using Subversion

Use `/Hg`  to auto-update any new or changed files using Mercurial

Use `/Commit` to commit any updates to the repository

Use `/L` to log messages to a file; you can optionally specify a log file name using `/L:LogFilePath`

Use `/LogDirectory` to specify the directory to save the log file in. 
* By default, the log file is created in the current working directory.

Use `/Preview` to count the number of database objects that would be exported

Use `/Stats` to show (but not log) export stats

## Software Dependencies

This program leverages SMO from SQL Server 2012.

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

Schema export from PostgreSQL databases utilizes the pg_dump utility. 
* pg_dump.exe is available as part of the PostgreSQL for Windows installer from the EDB website
  * https://www.enterprisedb.com/downloads/postgres-postgresql-downloads
* Run the installer, but de-select installing PostgreSQL Server
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
pg_dump.exe -Fp -h prismweb3 -p 5432 -d dms -U dmsreader --schema-only > DMS_Schema_Dump.sql
```

Note that the .sql file created by pg_dump.exe includes extra carriage returns, resulting in `CR CR LF` instead of `CR LF`
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
