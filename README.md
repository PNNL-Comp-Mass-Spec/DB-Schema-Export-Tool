# DB Schema Export Tool

The DB Schema Export Tool exports SQL Server database objects as schema files. 
Exported objects include tables, views, stored procedures, functions, and synonyms,
plus also database properties including database roles and logins.

### Continuous Integration

The latest version of the application is available on the [AppVeyor CI server](https://ci.appveyor.com/project/PNNLCompMassSpec/db-schema-export-tool/build/artifacts)

[![Build status](https://ci.appveyor.com/api/projects/status/sdk00tc0l0seupic?svg=true)](https://ci.appveyor.com/project/PNNLCompMassSpec/db-schema-export-tool)

## Features

In addition to creating schema files for tables, views, stored procedures, functions, and synonyms,
The program can also export table data to create SQL files with 
an Insert statement for each row.

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
  /L /LogFolder:Logs 
  /Data:ProteinSeqs_Data_Tables.txt
```


## Console switches

```
DB_Schema_Export_Tool.exe
 SchemaFileFolder /Server:ServerName
 /DB:Database /DBList:CommaSeparatedDatabaseName
 [/FolderPrefix:PrefixText] [/NoSubfolder]
 [/Data:TableDataToExport.txt] [/NoAutoData]
 [/Sync:TargetFolderPath] [/Git] [/Svn] [/Hg] [/Commit]
 [/L[:LogFilePath]] [/LogFolder:LogFolderPath] [/Preview] [/Stats]
```

SchemaFileFolder is the path to the folder where the schema files will be saved

To process a single database, use /Server and /DB

Use /DBList to process several databases (separate names with commas)

By default, a subfolder named DBSchema__DatabaseName will be created below SchemaFileFolder/ 
Customize this the prefix text using /FolderPrefix

Use /NoSubfolder to disable auto creating a subfolder for the database being exported. 
* Note: subfolders will always be created if you use /DBList and specify more than one database

Use /Data to define a text file with table names (one name per line) for which the data 
should be exported. In addition to table names defined in /Data, there are default tables 
which will have their data exported; disable the defaults using /NoAutoData

Use /Sync to copy new/changed files from the output folder to an alternative folder. 
* This is advantageous to prevent file timestamps from getting updated every time the schema is exported

Use /Git to auto-update any new or changed files using Git

Use /Svn to auto-update any new or changed files using Subversion

Use /Hg  to auto-update any new or changed files using Mercurial

Use /Commit to commit any updates to the repository

Use /L to log messages to a file; you can optionally specify a log file name using /L:LogFilePath.

Use /LogFolder to specify the folder to save the log file in. 
* By default, the log file is created in the current working directory.

Use /Preview to count the number of database objects that would be exported

Use /Stats to show (but not log) export stats

## Software Dependencies

This program leverages SMO from Sql Server 2012.  The required DLLs are in the Lib folder

In order to use the /Git, /Svn, or /Hg switches, you need the following software installed
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

## Contacts

Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA) \
E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov\
Website: https://omics.pnl.gov/ or https://www.pnnl.gov/sysbio/ or https://panomics.pnnl.gov/

## License

The DB Schema Export Tool is licensed under the 2-Clause BSD License; 
you may not use this file except in compliance with the License.  You may obtain 
a copy of the License at https://opensource.org/licenses/BSD-2-Clause

Copyright 2018 Battelle Memorial Institute
