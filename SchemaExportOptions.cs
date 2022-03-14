using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using PRISM;
using PRISM.Logging;

namespace DB_Schema_Export_Tool
{
    /// <summary>
    /// Schema export options
    /// </summary>
    public class SchemaExportOptions
    {
        // Ignore Spelling: Npgsql, PostgreSQL, psql, schemas, Svn, username

        #region "Constants and Enums"

        /// <summary>
        /// Program date
        /// </summary>
        public const string PROGRAM_DATE = "August 26, 2021";

        /// <summary>
        /// Default output directory name prefix
        /// </summary>
        public const string DEFAULT_DB_OUTPUT_DIRECTORY_NAME_PREFIX = "DBSchema__";

        /// <summary>
        /// Default server schema output directory name prefix
        /// </summary>
        public const string DEFAULT_SERVER_OUTPUT_DIRECTORY_NAME_PREFIX = "ServerSchema__";

        #endregion

        #region "Properties"

        /// <summary>
        /// Column name mapping
        /// Keys are source table name, values are a class tracking the source and target column names for the table
        /// </summary>
        /// <remarks>
        /// Keys are not case sensitive
        /// </remarks>
        public Dictionary<string, ColumnMapInfo> ColumnMapForDataExport { get; }

        /// <summary>
        /// List of databases to process (all on the same server)
        /// </summary>
        public SortedSet<string> DatabasesToProcess { get; }

        /// <summary>
        /// List of tables to limit the processing to
        /// </summary>
        /// <remarks>
        /// If this is empty, processes all tables specified via other settings
        /// If this is not empty and if NoSchema is False, will only export the schema for tables in this list
        /// (will not export schema for views, procedures, etc.)
        /// </remarks>
        public SortedSet<string> TableNameFilterSet { get; }

        /// <summary>
        /// List of schema to ignore
        /// </summary>
        public SortedSet<string> SchemaNameSkipList { get; }

        /// <summary>
        /// Maximum rows of data to export
        /// </summary>
        /// <remarks>
        /// When null and ExportAllData is false, default to 1000 rows
        /// When null and ExportAllData is true, default to 0
        /// </remarks>
        public int? MaxRowsToExport { get; private set; }

        /// <summary>
        /// Options defining what to script
        /// </summary>
        public DatabaseScriptingOptions ScriptingOptions { get; }

        #endregion

        #region "Command Line Argument Properties "

        /// <summary>
        /// Output directory
        /// </summary>
        [Option("OutputDir", "O", ArgPosition = 1, Required = true, HelpShowsDefault = false,
            HelpText = "Directory to save the schema files")]
        public string OutputDirectoryPath { get; set; }

        /// <summary>
        /// Prefix to use for the ServerSchema directory
        /// </summary>
        public string ServerOutputDirectoryNamePrefix { get; set; }

        /// <summary>
        /// Server name
        /// </summary>
        [Option("Server", Required = true, HelpShowsDefault = false,
            HelpText = "Database server name; assumed to be Microsoft SQL Server unless /PgUser is provided (or PgUser is defined in the .conf file)")]
        public string ServerName { get; set; }

        /// <summary>
        /// Database username
        /// </summary>
        [Option("DBUser", HelpShowsDefault = false,
            HelpText = "Database username; ignored if connecting to SQL Server with integrated authentication (default). Required if connecting to PostgreSQL")]
        public string DBUser { get; set; }

        /// <summary>
        /// Password for the database user
        /// </summary>
        [Option("DBPass", HelpShowsDefault = false,
            HelpText = "Password for the database user; ignored if connecting to SQL Server with integrated authentication\n" +
                       // ReSharper disable StringLiteralTypo
                       @"For PostgreSQL, you can create a file named pgpass.conf at %APPDATA%\postgresql (or ~/.pgpass on Linux) to track passwords for PostgreSQL users\n" +
                       // ReSharper restore StringLiteralTypo
                       "List one entry per line, using the format server:port:database:username:password (see README.md for more info)")]
        public string DBUserPassword { get; set; }

        /// <summary>
        /// Existing schema (DDL) file to parse to rename columns based on information in the ColumnMap file
        /// </summary>
        [Option("ExistingDDL", "ExistingSchema", HelpShowsDefault = false, IsInputFilePath = true,
            HelpText = "Existing schema (DDL) file to parse to rename columns based on information in the ColumnMap file\n" +
                       "Will also skip any tables or views with <skip> in the DataTables file\n" +
                       "The updated DDL file will end with _UpdatedColumnNames.sql")]
        public string ExistingSchemaFileToParse { get; set; }

        /// <summary>
        /// When true, connecting to a PostgreSQL server
        /// </summary>
        /// <remarks>
        /// Auto set to true if /PgUser is defined
        /// </remarks>
        public bool PostgreSQL { get; set; }

        /// <summary>
        /// <para>
        /// With SQL Server databases, dump table data using COPY commands instead of INSERT INTO statements
        /// </para>
        /// <para>
        /// With PostgreSQL data, dump table data using pg_dump.exe and COPY commands
        /// </para>
        /// </summary>
        [Option("PgDump", "PgDumpData", HelpShowsDefault = false,
            HelpText = "With SQL Server databases, dump table data using COPY commands instead of INSERT INTO statements\n" +
                       "With PostgreSQL data, dump table data using pg_dump.exe and COPY commands")]
        public bool PgDumpTableData { get; set; }

        /// <summary>
        /// When true, do not delete the PgDump output file (_AllObjects_.sql)
        /// </summary>
        [Option("KeepPgDumpFile", "KeepPgDump", HelpShowsDefault = false, SecondaryArg = true,
            HelpText = "By default, the PgDump output file (_AllObjects_.sql) is deleted after it has been processed\n" +
                       "Set this to True to skip the deletion\n" +
                       "If any unhandled scripting commands are encountered, the _AllObjects_.sql file will not be deleted, even if KeepPgDumpFile is false")]
        public bool KeepPgDumpFile { get; set; }

        /// <summary>
        /// Number of values to insert at a time when PgInsert is true for a table
        /// </summary>
        [Option("PgInsertChunkSize", HelpShowsDefault = true,
            HelpText = "Number of values to insert at a time when PgInsert is true for a table; only applicable when exporting data from SQL Server")]
        public int PgInsertChunkSize { get; set; }

        /// <summary>
        /// With SQL Server databases, while reading the TableDataToExport.txt file, default the PgInsert column to true
        /// meaning table data will be exported via INSERT INTO statements using the syntax "ON CONFLICT (key_column) DO UPDATE SET"
        /// </summary>
        [Option("PgInsert", HelpShowsDefault = false,
            HelpText = "With SQL Server databases, while reading the TableDataToExport.txt file, default the PgInsert column to true, " +
                       "meaning table data will be exported via INSERT INTO statements using the syntax \"ON CONFLICT (key_column) DO UPDATE SET\"\n" +
                       "Ignored for PostgreSQL data")]
        public bool PgInsertTableData { get; set; }

        /// <summary>
        /// Database username when connecting to PostgreSQL
        /// </summary>
        [Option("PgUser", HelpShowsDefault = false,
            HelpText = "Database username when connecting to a PostgreSQL server")]
        public string PgUser
        {
            get => DBUser;
            set
            {
                DBUser = value;
                if (!string.IsNullOrWhiteSpace(value))
                {
                    PostgreSQL = true;
                }
            }
        }

        /// <summary>
        /// PostgreSQL user password
        /// </summary>
        [Option("PgPass", HelpShowsDefault = false,
            HelpText = "PostgreSQL user password")]
        public string PgPass
        {
            get => DBUserPassword;
            set => DBUserPassword = value;
        }

        /// <summary>
        /// PostgreSQL port
        /// </summary>
        [Option("PgPort", HelpShowsDefault = false, SecondaryArg = true,
            HelpText = "Port to use for a PostgreSQL database")]
        public int PgPort { get; set; }

        /// <summary>
        /// Database name to process
        /// </summary>
        /// <remarks>
        /// If processing multiple databases, returns a comma separated list of database names
        /// </remarks>
        [Option("DB", HelpShowsDefault = false,
            HelpText = "Database name (alternatively, use DBList for a list of databases)")]
        public string Database
        {
            get
            {
                return DatabasesToProcess.Count switch
                {
                    0 => string.Empty,
                    1 => DatabasesToProcess.First(),
                    _ => "DB List: " + string.Join(", ", DatabasesToProcess)
                };
            }
            set
            {
                if (string.IsNullOrWhiteSpace(value))
                    return;

                if (!DatabasesToProcess.Contains(value))
                {
                    DatabasesToProcess.Add(value);
                }
            }
        }

        /// <summary>
        /// Comma separated list of database names
        /// </summary>
        [Option("DBList", "DBs", HelpShowsDefault = false, HelpText = "Comma separated list of database names")]
        public string DatabaseList
        {
            get => DatabasesToProcess.Count == 0 ? string.Empty : string.Join(", ", DatabasesToProcess);
            set
            {
                foreach (var dbName in value.Split(','))
                {
                    if (!DatabasesToProcess.Contains(dbName))
                    {
                        DatabasesToProcess.Add(dbName);
                    }
                }
            }
        }

        /// <summary>
        /// Prefix for each database directory
        /// </summary>
        [Option("DirectoryPrefix", HelpShowsDefault = false,
            HelpText = "Prefix name for output directories; default name is " + DEFAULT_DB_OUTPUT_DIRECTORY_NAME_PREFIX + "DatabaseName")]
        public string DatabaseSubdirectoryPrefix { get; set; }

        /// <summary>
        /// When true, skip exporting schema
        /// </summary>
        [Option("NoSchema", HelpShowsDefault = false, HelpText = "Skip exporting schema")]
        public bool NoSchema { get; set; }

        /// <summary>
        /// When true, do not create a subdirectory for each database when copying data to the Sync directory
        /// </summary>
        [Option("NoSubdirectory", HelpShowsDefault = false,
            HelpText = "Disable creating a subdirectory for each database when copying data to the Sync directory")]
        public bool NoSubdirectoryOnSync { get; set; }

        /// <summary>
        /// When true, create a subdirectory for the schema files for each database
        /// </summary>
        [Option("CreateDBDirectories", HelpShowsDefault = false, HelpText = "Create a subdirectory for the schema files for each database")]
        public bool CreateDirectoryForEachDB { get; set; }

        /// <summary>
        /// When true, export server settings, logins, and SQL Server Agent jobs
        /// </summary>
        [Option("ServerInfo", HelpShowsDefault = false, HelpText = "Export server settings, logins, and jobs")]
        public bool ExportServerInfo
        {
            get => ScriptingOptions.ExportServerSettingsLoginsAndJobs;
            set => ScriptingOptions.ExportServerSettingsLoginsAndJobs = value;
        }

        /// <summary>
        /// Text file with table names (one name per line) for which table data should be exported
        /// </summary>
        [Option("DataTables", "Data", HelpShowsDefault = false, IsInputFilePath = true,
            HelpText = "Text file with table names (one name per line) for which table data should be exported\n" +
                       "Also supports a multi-column, tab-delimited format:\n" +
                       "SourceTableName  TargetSchemaName  TargetTableName  PgInsert  KeyColumn(s)")]
        public string TableDataToExportFile { get; set; }

        /// <summary>
        /// Text file mapping source column names to target column names
        /// </summary>
        [Option("ColumnMap", "Map", HelpShowsDefault = false, IsInputFilePath = true,
            HelpText = "Text file mapping source column names to target column names. " +
                       "Tab-delimited columns are:\n" +
                       "SourceTableName  SourceColumnName  TargetColumnName\n" +
                       "TargetColumnName supports <skip> for not including the given column in the output file")]
        public string TableDataColumnMapFile { get; set; }

        /// <summary>
        /// Table name (or comma separated list of names) to restrict table export operations
        /// </summary>
        [Option("TableFilterList", "TableNameFilter", HelpShowsDefault = false,
            HelpText = "Table name (or comma separated list of names) to restrict table export operations\n" +
                       "This is useful for exporting the data from just a single table")]
        public string TableNameFilterList
        {
            get => TableNameFilterSet.Count == 0 ? string.Empty : string.Join(", ", TableNameFilterSet);
            set
            {
                if (string.IsNullOrWhiteSpace(value))
                {
                    TableNameFilterSet.Clear();
                    return;
                }

                foreach (var tableName in value.Split(','))
                {
                    var trimmedName = tableName.Trim();
                    if (!TableNameFilterSet.Contains(trimmedName))
                    {
                        TableNameFilterSet.Add(trimmedName);
                    }
                }
            }
        }

        /// <summary>
        /// Schema name (or comma separated list of names) of schema to skip
        /// </summary>
        [Option("SchemaSkipList", HelpShowsDefault = false, SecondaryArg = true,
            HelpText = "Schema name (or comma separated list of names) of schema to skip. " +
                       "Useful if using partitioned tables")]
        public string SchemaSkipList
        {
            get => SchemaNameSkipList.Count == 0 ? string.Empty : string.Join(", ", SchemaNameSkipList);
            set
            {
                if (string.IsNullOrWhiteSpace(value))
                {
                    SchemaNameSkipList.Clear();
                    return;
                }

                foreach (var schemaName in value.Split(','))
                {
                    var trimmedName = schemaName.Trim();
                    if (!SchemaNameSkipList.Contains(trimmedName))
                    {
                        SchemaNameSkipList.Add(trimmedName);
                    }
                }
            }
        }

        /// <summary>
        /// Text file used to filter data by date when exporting data from tables
        /// </summary>
        [Option("TableDataDateFilter", "DateFilter", HelpShowsDefault = false, IsInputFilePath = true,
            HelpText = "Text file used to filter data by date when exporting data from tables. " +
                       "Tab-delimited columns are:\n" +
                       "SourceTableName  DateColumnName  MinimumDate")]
        public string TableDataDateFilterFile { get; set; }

        /// <summary>
        /// Only export objects that contain this text
        /// </summary>
        /// <remarks>
        /// Supports RegEx symbols like ^ and $
        /// </remarks>
        [Option("ObjectNameFilter", "NameFilter", HelpShowsDefault = false,
            HelpText = "Only export objects that contain this text; supports RegEx symbols like ^ and $")]
        public string ObjectNameFilter { get; set; }

        /// <summary>
        /// "Default schema for exported tables and data
        /// </summary>
        [Option("DefaultSchema", "Schema", HelpShowsDefault = false,
            HelpText = "Default schema for exported tables and data. If undefined, use the original table's schema")]
        public string DefaultSchemaName { get; set; }

        /// <summary>
        /// When true, disable auto-selecting tables for exporting data
        /// </summary>
        [Option("NoAutoData", HelpShowsDefault = false,
            HelpText = "In addition to table names defined by DataTables (or /Data), there are default tables which will have their data exported\n" +
                       "Disable the defaults using NoAutoData=True (or /NoAutoData)")]
        public bool DisableAutoDataExport { get; set; }

        /// <summary>
        /// Export data from every table in the database
        /// </summary>
        [Option("ExportAllData", "ExportAllTables", "AllData", HelpShowsDefault = false,
            HelpText = "Export data from every table in the database\n" +
                       "Will skip tables (and views) that have <skip> in the DataTables file\n" +
                       "Ignored if NoTableData is true")]
        public bool ExportAllData { get; set; }

        /// <summary>
        /// Maximum number of rows of data to export
        /// </summary>
        [Option("MaxRows", HelpShowsDefault = false, SecondaryArg = true,
            HelpText = "Maximum number of rows of data to export; defaults to 1000\n" +
                       "Use 0 to export all rows")]
        public int MaxRows
        {
            get
            {
                if (ExportAllData)
                {
                    return MaxRowsToExport ?? 0;
                }

                return MaxRowsToExport ?? DBSchemaExporterBase.MAX_ROWS_DATA_TO_EXPORT;
            }
            set => MaxRowsToExport = value;
        }

        /// <summary>
        /// When true, prevent any table data from being exported
        /// </summary>
        [Option("NoTableData", "NoData", HelpShowsDefault = false,
                HelpText = "Use NoTableData=True (or /NoTableData) to prevent any table data from being exported\n" +
                           "This parameter is useful when processing an existing DDL file with ExistingDDL")]
        public bool DisableDataExport { get; set; }

        /// <summary>
        /// Generate a bash script for loading table data
        /// </summary>
        [Option("ScriptLoad", "Script", HelpShowsDefault = false,
            HelpText = "Generate a bash script for loading table data")]
        public bool ScriptPgLoadCommands { get; set; }

        /// <summary>
        /// Auto-change column names from Upper_Case and UpperCase to lower_case when exporting table data
        /// </summary>
        [Option("SnakeCase", HelpShowsDefault = false,
            HelpText = "Auto changes column names from Upper_Case and UpperCase to lower_case when exporting table data\n" +
                       "Also used for table names when exporting data\n" +
                       "Entries in the DataTables and ColumnMap files will override auto-generated snake_case names")]
        public bool TableDataSnakeCase { get; set; }

        /// <summary>
        /// When true, export server settings, logins, and SQL Server Agent jobs
        /// </summary>
        public bool ExportServerSettingsLoginsAndJobs
        {
            get => ScriptingOptions.ExportServerSettingsLoginsAndJobs;
            set => ScriptingOptions.ExportServerSettingsLoginsAndJobs = value;
        }

        /// <summary>
        /// True when SyncDirectoryPath is not empty
        /// </summary>
        public bool Sync => !string.IsNullOrWhiteSpace(SyncDirectoryPath);

        /// <summary>
        /// Copy new/changed files from the output directory to an alternative directory
        /// </summary>
        [Option("Sync", HelpShowsDefault = false, SecondaryArg = true,
            HelpText = "Copy new/changed files from the output directory to an alternative directory\n" +
                       "This is advantageous to prevent file timestamps from getting updated every time the schema is exported")]
        public string SyncDirectoryPath { get; set; }

        /// <summary>
        /// Auto-update any new or changed files using Git
        /// </summary>
        [Option("Git", HelpShowsDefault = false, SecondaryArg = true,
            HelpText = "Auto-update any new or changed files using Git")]
        public bool GitUpdate { get; set; }

        /// <summary>
        /// Auto-update any new or changed files using Subversion
        /// </summary>
        [Option("Svn", HelpShowsDefault = false, SecondaryArg = true,
            HelpText = "Auto-update any new or changed files using Subversion")]
        public bool SvnUpdate { get; set; }

        /// <summary>
        /// Auto-update any new or changed files using Mercurial
        /// </summary>
        [Option("Hg", HelpShowsDefault = false, SecondaryArg = true,
            HelpText = "Auto-update any new or changed files using Mercurial")]
        public bool HgUpdate { get; set; }

        /// <summary>
        /// Commit any updates to the repository
        /// </summary>
        [Option("Commit", HelpShowsDefault = false, SecondaryArg = true,
            HelpText = "Commit any updates to the repository")]
        public bool CommitUpdates { get; set; }

        /// <summary>
        /// When true, log messages to a file
        /// </summary>
        [Option("CreateLogFile", "L", HelpShowsDefault = false, SecondaryArg = true,
            HelpText = "Log messages to a file; specify a base log file name using /LogFile:LogFileName")]
        public bool LogMessagesToFile { get; set; }

        /// <summary>
        /// Base log file name
        /// </summary>
        /// <remarks>
        /// The actual name will include today's date
        /// </remarks>
        [Option("BaseLogFileName", "LogFile", HelpShowsDefault = false, SecondaryArg = true,
            HelpText = "Base log file name (the actual name will include today's date); defaults to DB_Schema_Export_Tool")]
        public string LogFileBaseName { get; set; }

        /// <summary>
        /// Directory path for the log file
        /// </summary>
        [Option("LogDir", "LogDirectory",
            HelpShowsDefault = false, SecondaryArg = true,
            HelpText = "Specify the directory to save the log file in\n" +
                       "By default, the log file is created in the current working directory")]
        public string LogDirectoryPath { get; set; }

        /// <summary>
        /// When true, display a count of the number of database objects that would be exported
        /// </summary>
        [Option("Preview", HelpShowsDefault = false,
            HelpText = "Count the number of database objects that would be exported")]
        public bool PreviewExport { get; set; }

        /// <summary>
        /// Show (but do not log) export stats
        /// </summary>
        [Option("Stats", HelpShowsDefault = false, SecondaryArg = true,
            HelpText = "Show (but do not log) export stats")]
        public bool ShowStats { get; set; }

        /// <summary>
        /// Show additional debug messages
        /// </summary>
        [Option("Trace", HelpShowsDefault = false, SecondaryArg = true,
            HelpText = "Show additional debug messages, both at the console and in the log file")]
        public bool Trace { get; set; }

        /// <summary>
        /// This returns true if DBUser is empty; or false if a username is defined
        /// </summary>
        public bool UseIntegratedAuthentication => !string.IsNullOrWhiteSpace(DBUser);

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        public SchemaExportOptions()
        {
            OutputDirectoryPath = ".";

            DatabasesToProcess = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);
            TableNameFilterSet = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);
            SchemaNameSkipList = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);

            DatabaseSubdirectoryPrefix = DEFAULT_DB_OUTPUT_DIRECTORY_NAME_PREFIX;

            ColumnMapForDataExport = new Dictionary<string, ColumnMapInfo>(StringComparer.OrdinalIgnoreCase);

            NoSubdirectoryOnSync = false;
            SyncDirectoryPath = string.Empty;

            DBUser = string.Empty;
            DBUserPassword = string.Empty;

            PostgreSQL = false;
            PgDumpTableData = false;
            KeepPgDumpFile = false;

            PgInsertTableData = false;
            PgInsertChunkSize = 50000;

            PgPort = DBSchemaExporterPostgreSQL.DEFAULT_PORT;

            ExistingSchemaFileToParse = string.Empty;

            OutputDirectoryPath = string.Empty;
            DatabaseSubdirectoryPrefix = DEFAULT_DB_OUTPUT_DIRECTORY_NAME_PREFIX;
            CreateDirectoryForEachDB = true;

            ServerOutputDirectoryNamePrefix = DEFAULT_SERVER_OUTPUT_DIRECTORY_NAME_PREFIX;

            ScriptingOptions = new DatabaseScriptingOptions();

            LogMessagesToFile = false;
            LogFileBaseName = string.Empty;
            LogDirectoryPath = string.Empty;
        }

        /// <summary>
        /// Return Enabled if value is true
        /// Return Disabled if value is false
        /// </summary>
        /// <param name="value"></param>
        private static string BoolToEnabledDisabled(bool value)
        {
            return value ? "Enabled" : "Disabled";
        }

        /// <summary>
        /// Get the program version
        /// </summary>
        public static string GetAppVersion()
        {
            return Assembly.GetExecutingAssembly().GetName().Version + " (" + PROGRAM_DATE + ")";
        }

        /// <summary>
        /// Get the expected base log file path
        /// </summary>
        /// <param name="options"></param>
        /// <param name="defaultBaseLogFileName">Explicit base log file name to use</param>
        public static string GetLogFilePath(SchemaExportOptions options, string defaultBaseLogFileName)
        {
            var logDirectoryPath = string.IsNullOrWhiteSpace(options.LogDirectoryPath) ? "." : options.LogDirectoryPath;
            var baseLogFileName = string.IsNullOrWhiteSpace(options.LogFileBaseName) ? defaultBaseLogFileName : options.LogFileBaseName;
            var baseLogFilePath = Path.Combine(logDirectoryPath, baseLogFileName);

            return baseLogFilePath;
        }

        /// <summary>
        /// Show the options at the console
        /// </summary>
        public void OutputSetOptions()
        {
            Console.WriteLine("Options:");
            Console.WriteLine();

            Console.WriteLine(" {0,-48} {1}", "Output directory path:", OutputDirectoryPath);

            Console.WriteLine(" {0,-48} {1}", "Create directory for each DB:", BoolToEnabledDisabled(CreateDirectoryForEachDB));

            Console.WriteLine(" {0,-48} {1}", "Source Server:", ServerName);

            if (!PostgreSQL)
            {
                Console.WriteLine(" {0,-48} {1}", "Server type:", "Microsoft SQL Server");
                if (string.IsNullOrWhiteSpace(DBUser))
                {
                    Console.WriteLine(" {0,-48} {1}", "Database user:", "<integrated authentication>");
                }
                else
                {
                    Console.WriteLine(" {0,-48} {1}", "Database user:", DBUser);
                    if (string.IsNullOrWhiteSpace(DBUserPassword))
                        Console.WriteLine(" {0,-48} {1}", "Password:", "Error: undefined");
                    else
                        Console.WriteLine(" {0,-48} {1}", "Password:", "Provided");
                }
            }
            else
            {
                Console.WriteLine(" {0,-48} {1}", "Server type:", "PostgreSQL");

                if (string.IsNullOrWhiteSpace(PgUser))
                    Console.WriteLine(" {0,-48} {1}", "Username:", "Error: undefined");
                else
                    Console.WriteLine(" {0,-48} {1}", "Username:", PgUser);

                if (string.IsNullOrWhiteSpace(PgPass))
                {
                    if (SystemInfo.IsLinux)
                        Console.WriteLine(" {0,-48} {1}", "Password:", "Will be read from ~/.pgpass");
                    else
                        Console.WriteLine(" {0,-48} {1}", "Password:", @"Will be read from %APPDATA%\postgresql\pgpass.conf");
                }
                else
                {
                    Console.WriteLine(" {0,-48} {1}", "Password:", "Provided");
                }

                Console.WriteLine(" {0,-48} {1}", "Port:", PgPort);
            }

            if (DatabasesToProcess.Count == 0)
            {
                Console.WriteLine(" {0,-48} {1}", "Database to export:", "Error: Undefined");
            }
            else if (DatabasesToProcess.Count == 1)
            {
                Console.WriteLine(" {0,-48} {1}", "Database to export:", DatabasesToProcess.First());
            }
            else
            {
                Console.WriteLine(" {0,-48} {1}", "Databases to export:", DatabaseList);
            }

            Console.WriteLine(" {0,-48} {1}", "Database Subdirectory Prefix:", DatabaseSubdirectoryPrefix);

            if (!CreateDirectoryForEachDB)
            {
                Console.WriteLine(" Will not create a subdirectory for each database");
            }

            if (PgDumpTableData)
            {
                if (PostgreSQL)
                {
                    Console.WriteLine(" {0,-48} {1}", "Table data export tool:", "pg_dump");
                    Console.WriteLine(" {0,-48} {1}", "Keep the pg_dump output file, _AllObjects_.sql:", BoolToEnabledDisabled(KeepPgDumpFile));
                }
                Console.WriteLine(" {0,-48} {1}", "Dump table data as:", "PostgreSQL COPY commands");
            }
            else
            {
                if (PostgreSQL)
                {
                    Console.WriteLine(" {0,-48} {1}", "Table data export tool:", "Npgsql");
                }
                Console.WriteLine(" {0,-48} {1}", "Dump table data as:", "INSERT INTO statements");
            }

            if (!string.IsNullOrWhiteSpace(TableDataToExportFile))
            {
                Console.WriteLine(" {0,-48} {1}", "File with table names for exporting data:", PathUtils.CompactPathString(TableDataToExportFile, 80));
                if (!PostgreSQL)
                {
                    Console.WriteLine(" {0,-48} {1}", "Default value for the PgInsert column:", PgInsertTableData);
                    Console.WriteLine(" {0,-48} {1}", "PgInsert chunk size:", PgInsertChunkSize);
                }
            }

            if (TableNameFilterSet.Count > 0)
            {
                Console.WriteLine(" {0,-48} {1}",
                                  TableNameFilterSet.Count > 1 ? "List of tables to process:" : "Single table to process:",
                                  TableNameFilterList);
            }

            if (SchemaNameSkipList.Count > 0)
            {
                Console.WriteLine(" {0,-48} {1}",
                    SchemaNameSkipList.Count > 1 ? "List of schemas to skip:" : "Schema name to skip:",
                    SchemaSkipList);
            }

            if (!string.IsNullOrWhiteSpace(TableDataColumnMapFile))
            {
                Console.WriteLine(" {0,-48} {1}", "File with source/target column names:", PathUtils.CompactPathString(TableDataColumnMapFile, 80));
            }

            if (!string.IsNullOrWhiteSpace(TableDataDateFilterFile))
            {
                Console.WriteLine(" {0,-48} {1}", "File with date filter column info:", PathUtils.CompactPathString(TableDataDateFilterFile, 80));
            }

            if (!string.IsNullOrEmpty(ObjectNameFilter))
            {
                Console.WriteLine(" {0,-48} {1}", "Object Name Filter:", ObjectNameFilter);
            }

            if (!string.IsNullOrWhiteSpace(ExistingSchemaFileToParse))
            {
                Console.WriteLine(" {0,-48} {1}", "Existing schema file to update column names:", PathUtils.CompactPathString(ExistingSchemaFileToParse, 80));
            }

            if (!string.IsNullOrWhiteSpace(DefaultSchemaName))
            {
                Console.WriteLine(" {0,-48} {1}", "Default schema for exported data from tables:", DefaultSchemaName);
            }

            if (DisableDataExport)
            {
                Console.WriteLine(" {0,-48} {1}", "Data export from tables:", "Disabled");
            }
            else
            {
                if (ExportAllData)
                {
                    Console.WriteLine(" {0,-48} {1}", "Data export from all tables:", BoolToEnabledDisabled(ExportAllData));
                }
                else
                {
                    Console.WriteLine(" {0,-48} {1}", "Data export from standard tables:", BoolToEnabledDisabled(!DisableAutoDataExport));
                }

                if (PostgreSQL && PgDumpTableData && TableDataSnakeCase)
                {
                    // Assure that this is false
                    ConsoleMsgUtils.ShowWarning("Ignoring /SnakeCase since exporting data from a PostgreSQL server using pg_dump");
                    TableDataSnakeCase = false;
                }

                if (!DisableAutoDataExport || !string.IsNullOrWhiteSpace(TableDataToExportFile) || ExportAllData)
                {
                    var enabledNote = TableDataSnakeCase ? " (when exporting data)" : string.Empty;
                    Console.WriteLine(" {0,-48} {1}", "Use snake_case for table and column names:", BoolToEnabledDisabled(TableDataSnakeCase) + enabledNote);
                }

                if (MaxRowsToExport == null)
                {
                    if (ExportAllData)
                    {
                        Console.WriteLine(" {0,-48} {1}", "Maximum rows to export, per table:", "Export all rows");
                    }
                    else
                    {
                        Console.WriteLine(" {0,-48} {1}", "Maximum rows to export, per table:", MaxRows);
                    }
                }
                else
                {
                    Console.WriteLine(" {0,-48} {1}", "Maximum rows to export, per table:", MaxRows);
                }
            }

            if (NoSchema)
            {
                if (DisableDataExport)
                {
                    if (string.IsNullOrWhiteSpace(ExistingSchemaFileToParse))
                    {
                        ConsoleMsgUtils.ShowWarning("Warning: NoTableData=True, NoSchema=True, and ExistingSchemaFileToParse is empty; there is nothing to do");
                        Console.WriteLine();
                    }
                }
                else
                {
                    Console.WriteLine(" {0,-48} {1}", "Export table data only; no schema:", "Enabled");
                }
            }
            else
            {
                Console.WriteLine(" {0,-48} {1}", "Export server settings, logins, and jobs:", BoolToEnabledDisabled(ScriptingOptions.ExportServerSettingsLoginsAndJobs));

                if (Sync)
                {
                    Console.WriteLine(" {0,-48} {1}", "Synchronizing schema files to:", SyncDirectoryPath);

                    if (NoSubdirectoryOnSync)
                    {
                        Console.WriteLine(" When syncing, will not create a subdirectory for each database");
                    }
                }

                if (GitUpdate || SvnUpdate || HgUpdate)
                {
                    if (GitUpdate)
                    {
                        Console.WriteLine(" {0,-48} {1}", "Auto-updating any new or changed files using:", "Git");
                    }

                    if (SvnUpdate)
                    {
                        Console.WriteLine(" {0,-48} {1}", "Auto-updating any new or changed files using:", "Subversion");
                    }

                    if (HgUpdate)
                    {
                        Console.WriteLine(" {0,-48} {1}", "Auto-updating any new or changed files using:", "Mercurial");
                    }

                    Console.WriteLine(" {0,-48} {1}", "Commit updates:", BoolToEnabledDisabled(CommitUpdates));
                }
            }

            Console.WriteLine(" {0,-48} {1}", "Create a bash script for loading data with psql:", BoolToEnabledDisabled(ScriptPgLoadCommands));

            if (LogMessagesToFile)
            {
                if (string.IsNullOrWhiteSpace(LogFileBaseName))
                {
                    Console.WriteLine(" {0,-48} {1}", "Base log file name: ", "Default");
                }
                else
                {
                    Console.WriteLine(" {0,-48} {1}", "Base log file name:", LogFileBaseName);
                }

                if (!string.IsNullOrWhiteSpace(LogDirectoryPath))
                {
                    Console.WriteLine(" {0,-48} {1}", "Log directory path:", LogDirectoryPath);
                }
            }

            if (PreviewExport)
            {
                Console.WriteLine(" Preview mode: counting the number of database objects that would be exported");
            }

            if (ShowStats)
            {
                Console.WriteLine(" Showing export stats");
            }

            Console.WriteLine();
        }

        /// <summary>
        /// Validate the options
        /// </summary>
        public bool ValidateArgs(out string errorMessage)
        {
            if (string.IsNullOrWhiteSpace(OutputDirectoryPath))
            {
                errorMessage = "Use /O to specify the output directory";
                return false;
            }

            if (string.IsNullOrWhiteSpace(ServerName))
            {
                errorMessage = "Use /Server to specify the database server";
                return false;
            }

            if (PostgreSQL && string.IsNullOrWhiteSpace(PgUser))
            {
                errorMessage = "Use /PgUser to specify the database username for connecting to the server";
                return false;
            }

            if (DatabasesToProcess.Count == 0)
            {
                errorMessage = "Use /DB or /DBList to specify the databases to process";
                return false;
            }

            if (TableNameFilterSet.Count == 1)
            {
                try
                {
                    // Make sure the user didn't specify a text file
                    var candidateFile = new FileInfo(TableNameFilterSet.First());
                    if (candidateFile.Exists)
                    {
                        errorMessage = "The -TableFilterList parameter must be the name of a table; not a file on disk";
                        return false;
                    }
                }
                catch
                {
                    // Ignore errors here
                }

                if (TableNameFilterSet.First().EndsWith(".txt", StringComparison.OrdinalIgnoreCase))
                {
                    Console.WriteLine("The -TableFilterList parameter should be the name of a table; not a text file");
                }
            }

            errorMessage = string.Empty;
            return true;
        }

        /// <summary>
        /// Assure that output parameters have a value
        /// </summary>
        public void ValidateOutputOptions()
        {
            OutputDirectoryPath ??= string.Empty;

            DatabaseSubdirectoryPrefix ??= DEFAULT_DB_OUTPUT_DIRECTORY_NAME_PREFIX;

            ServerOutputDirectoryNamePrefix ??= DEFAULT_SERVER_OUTPUT_DIRECTORY_NAME_PREFIX;
        }
    }
}
