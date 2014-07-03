The DB Schema Export Tool exports Sql Server database objects as schema files. 
Exported objects include tables, views, stored procedures, and functions,
plus also database properties including database roles and logins.  The 
program can also export table data to create SQL files with 
an Insert Into statement for each row.

The DB Schema Export Tool has both a command line interface and a GUI mode.
Starting the program without any parameters will show the GUI.

== Command line syntax ==

DB_Schema_Export_Tool.exe
 SchemaFileFolder /Server:ServerName
 /DB:Database /DBList:CommaSeparatedDatabaseName
 [/FolderPrefix:PrefixText] [/NoSubfolder]
 [/Data:TableDataToExport.txt] [/NoAutoData] 
 [/Sync:TargetFolderPath] [/Svn] [/Git] [/Hg] [/Commit]
 [/L[:LogFilePath]] [/LogFolder:LogFolderPath]

SchemaFileFolder is the path to the folder where the schema files will be saved
To process a single database, use /Server and /DB
Use /DBList to process several databases (separate names with commas)

By default, a subfolder named DBSchema__DatabaseName will be created below SchemaFileFolder
Customize this the prefix text using /FolderPrefix
Use /NoSubfolder to disable auto creating a subfolder for the database being exported
Note: subfolders will always be created if you use /DBList and specify more than one database

Use /Data to define a text file with table names (one name per line) for which the data 
should be exported. In addition to table names defined in /Data, there are default tables 
which will have their data exported; disable the defaults using /NoAutoData

Use /Sync to copy new/changed files from the output folder to an alternative folder
This is advantageous to prevent file timestamps from getting updated every time the schema is exported

Use /Svn to auto-update any new or changed files using Subversion
Use /Git to auto-update any new or changed files using Git
Use /Hg  to auto-update any new or changed files using Mercurial
Use /Commit to commit any updates to the repository

Use /L to log messages to a file; you can optionally specify a log file name using /L:LogFilePath.
Use /LogFolder to specify the folder to save the log file in. 
By default, the log file is created in the current working directory.

== Software Dependencies ==

This program leverages SMO from Sql Server 2012.  The required DLLs are in the Lib folder

In order to use the /Svn, /Git, and/or /Hg switches, you need the following software installed
and the executables present at a specific location.

Svn
	C:\Program Files\TortoiseSVN\bin\svn.exe
	Installed with 64-bit Tortoise SVN, available at http://tortoisesvn.net/downloads.html

Mercurial
	C:\Program Files\TortoiseHg\hg.exe
	Installed with 64-bit Tortoise Hg,  available at http://tortoisehg.bitbucket.org/download/

Git
	C:\Program Files (x86)\Git\bin\git.exe
	Installed with 32-bit Git for Windows, available at http://git-scm.com/download/win

------------------------------------------------------------------------------------
Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA) in 2006
Command line interface added in 2014

E-mail: matthew.monroe@pnnl.gov or matt@alchemistmatt.com
Website: http://panomics.pnnl.gov/ or http://omics.pnl.gov
------------------------------------------------------------------------------------

Licensed under the Apache License, Version 2.0; you may not use this file except 
in compliance with the License.  You may obtain a copy of the License at 
http://www.apache.org/licenses/LICENSE-2.0
