using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using PRISM;

namespace DB_Schema_Export_Tool
{
    [SuppressMessage("ReSharper", "UnusedMember.Global")]
    public class SchemaExportOptions
    {
        #region "Constants and Enums"

        /// <summary>
        /// Program date
        /// </summary>
        public const string PROGRAM_DATE = "October 21, 2019";

        public const string DEFAULT_DB_OUTPUT_DIRECTORY_NAME_PREFIX = "DBSchema__";

        public const string DEFAULT_SERVER_OUTPUT_DIRECTORY_NAME_PREFIX = "ServerSchema__";

        #endregion

        #region "Properties"

        /// <summary>
        /// Column name mapping
        /// </summary>
        /// <remarks>Keys are source table name, values are a class tracking the source and target column names for the table</remarks>
        public Dictionary<string, ColumnMapInfo> ColumnMapForDataExport { get; }

        /// <summary>
        /// List of databases to process (all on the same server)
        /// </summary>
        public SortedSet<string> DatabasesToProcess { get; }

        /// <summary>
        /// Maximum rows of data to export
        /// </summary>
        /// <remarks>When null, default to 1000 rows, unless ExportAllData is true, then default to 0</remarks>
        public int? MaxRowsToExport { get; private set; }

        /// <summary>
        /// Options defining what to script
        /// </summary>
        public DatabaseScriptingOptions ScriptingOptions { get; }

        #endregion

        #region "Command Line Argument Properties "

        [Option("O", ArgPosition = 1, Required = true, HelpShowsDefault = false, HelpText = "Directory to save the schema files")]
        public string OutputDirectoryPath { get; set; }

        /// <summary>
        /// Prefix to use for the ServerSchema directory
        /// </summary>
        public string ServerOutputDirectoryNamePrefix { get; set; }

        [Option("Server", Required = true, HelpShowsDefault = false,
            HelpText = "Database server name; assumed to be Microsoft SQL Server unless /PgUser is provided")]
        public string ServerName { get; set; }

        [Option("DBUser", HelpShowsDefault = false,
            HelpText = "Database username; ignored if connecting to SQL Server with integrated authentication (default). Required if connecting to PostgreSQL")]
        public string DBUser { get; set; }

        [Option("DBPass", HelpShowsDefault = false,
            HelpText = @"Password for the database user; ignored if connecting to SQL Server with integrated authentication. " +
                       // ReSharper disable StringLiteralTypo
                       @"For PostgreSQL, you can create a file named pgpass.conf at %APPDATA%\postgresql (or ~/.pgpass on Linux) to track passwords for PostgreSQL users. " +
                       // ReSharper restore StringLiteralTypo
                       "List one entry per line, using the format server:port:database:username:password (see README.md for more info)")]
        public string DBUserPassword { get; set; }

        /// <summary>
        /// When true, connecting to a PostgreSQL server
        /// </summary>
        /// <remarks>Auto set to true if /PgUser is defined</remarks>
        public bool PostgreSQL { get; set; }

        [Option("PgDump", "PgDumpData", HelpShowsDefault = false,
            HelpText = "With SQL Server databases, dump table data using COPY commands instead of INSERT INTO statements.  " +
                       "With PostgreSQL data, dump table data using pg_dump and COPY commands.")]
        public bool PgDumpTableData { get; set; }

        [Option("PgUser", HelpShowsDefault = false, HelpText = "Database username when connecting to a PostgreSQL server")]
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

        [Option("PgPass", HelpShowsDefault = false, HelpText = "PostgreSQL user password")]
        public string PgPass
        {
            get => DBUserPassword;
            set => DBUserPassword = value;
        }

        [Option("PgPort", HelpShowsDefault = false, HelpText = "Port to use for a PostgreSQL database")]
        public int PgPort { get; set; }

        [Option("DB", HelpShowsDefault = false, HelpText = "Database name")]
        public string Database
        {
            get
            {
                if (DatabasesToProcess.Count == 0)
                    return string.Empty;

                if (DatabasesToProcess.Count == 1)
                    return DatabasesToProcess.First();

                return "DB List: " + string.Join(", ", DatabasesToProcess);
            }
            set
            {
                if (!DatabasesToProcess.Contains(value))
                {
                    DatabasesToProcess.Add(value);
                }
            }
        }

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

        [Option("NoSchema", HelpShowsDefault = false, HelpText = "Skip exporting schema")]
        public bool NoSchema { get; set; }

        [Option("NoSubdirectory", HelpShowsDefault = false,
            HelpText = "Disable creating a subdirectory for each database when copying data to the Sync directory")]
        public bool NoSubdirectoryOnSync { get; set; }

        /// <summary>
        /// When true (default), create a directory for each database
        /// </summary>
        public bool CreateDirectoryForEachDB { get; set; }

        [Option("Data", "DataTables", HelpShowsDefault = false,
            HelpText = "Text file with table names (one name per line) for which table data should be exported. " +
                       @"Also supports a multi-column, tab-delimited format:\nSourceTableName TargetSchemaName TargetTableName UseMergeStatement")]
        public string TableDataToExportFile { get; set; }

        [Option("Map", "ColumnMap", HelpShowsDefault = false,
            HelpText = "Text file mapping source column names to target column names. " +
                       @"Tab-delimited columns are:\nSourceTableName  SourceColumnName  TargetColumnName\n" +
                       "The TargetColumn supports <Skip> for not including the given column in the output file")]
        public string TableDataColumnMapFile { get; set; }

        [Option("DefaultSchema", "Schema", HelpShowsDefault = false,
            HelpText = "Default schema for exported tables and data. If undefined, use the original table's schema")]
        public string DefaultSchemaName { get; set; }

        [Option("NoAutoData", HelpShowsDefault = false,
            HelpText = "In addition to table names defined in /Data, there are default tables which will have their data exported. " +
                       "Disable the defaults using /NoAutoData")]
        public bool DisableAutoDataExport { get; set; }

        [Option("ExportAllData", "ExportAllTables", "AllData", HelpShowsDefault = false,
            HelpText = "Export data from every table in the database")]
        public bool ExportAllData { get; set; }

        [Option("MaxRows", HelpShowsDefault = false,
            HelpText = "Maximum number of rows of data to export; defaults to 1000")]
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

        [Option("SnakeCase", HelpShowsDefault = false,
            HelpText = "Auto changes column names from Upper_Case and UpperCase to lower_case")]
        public bool TableDataSnakeCase { get; set; }

        [Option("ServerInfo", HelpShowsDefault = false, HelpText = "Export server settings, logins, and jobs")]
        public bool ExportServerInfo
        {
            get => ScriptingOptions.ExportServerSettingsLoginsAndJobs;
            set => ScriptingOptions.ExportServerSettingsLoginsAndJobs = value;
        }

        public bool IncludeSystemObjects
        {
            get => ScriptingOptions.IncludeSystemObjects;
            set => ScriptingOptions.IncludeSystemObjects = value;
        }

        public bool IncludeTimestampInScriptFileHeader
        {
            get => ScriptingOptions.IncludeTimestampInScriptFileHeader;
            set => ScriptingOptions.IncludeTimestampInScriptFileHeader = value;
        }

        public bool ExportServerSettingsLoginsAndJobs
        {
            get => ScriptingOptions.ExportServerSettingsLoginsAndJobs;
            set => ScriptingOptions.ExportServerSettingsLoginsAndJobs = value;
        }

        /// <summary>
        /// True when SyncDirectoryPath is not empty
        /// </summary>
        public bool Sync => !string.IsNullOrWhiteSpace(SyncDirectoryPath);

        [Option("Sync", HelpShowsDefault = false,
            HelpText = "Copy new/changed files from the output directory to an alternative directory. " +
                       "This is advantageous to prevent file timestamps from getting updated every time the schema is exported.")]
        public string SyncDirectoryPath { get; set; }

        [Option("Git", HelpShowsDefault = false, HelpText = "Auto-update any new or changed files using Git")]
        public bool GitUpdate { get; set; }

        [Option("Svn", HelpShowsDefault = false, HelpText = "Auto-update any new or changed files using Subversion")]
        public bool SvnUpdate { get; set; }

        [Option("Hg", HelpShowsDefault = false, HelpText = "Auto-update any new or changed files using Mercurial")]
        public bool HgUpdate { get; set; }

        [Option("Commit", HelpShowsDefault = false, HelpText = "Commit any updates to the repository")]
        public bool CommitUpdates { get; set; }

        [Option("L", HelpShowsDefault = false, HelpText = "Log messages to a file; specify a log file name using /LogFile:LogFilePath")]
        public bool LogMessagesToFile { get; set; }

        [Option("LogFile", HelpShowsDefault = false, HelpText = "Log file name")]
        public string LogFilePath { get; set; }

        [Option("LogDir", HelpShowsDefault = false, HelpText = "Specify the directory to save the log file in; by default, the log file is created in the current working directory")]
        public string LogDirectoryPath { get; set; }

        [Option("Preview", HelpShowsDefault = false, HelpText = "Count the number of database objects that would be exported")]
        public bool PreviewExport { get; set; }

        [Option("Stats", HelpShowsDefault = false, HelpText = "Show (but do not log) export stats")]
        public bool ShowStats { get; set; }

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
            DatabaseSubdirectoryPrefix = DEFAULT_DB_OUTPUT_DIRECTORY_NAME_PREFIX;

            ColumnMapForDataExport = new Dictionary<string, ColumnMapInfo>(StringComparer.OrdinalIgnoreCase);

            NoSubdirectoryOnSync = false;
            SyncDirectoryPath = string.Empty;

            DBUser = string.Empty;
            DBUserPassword = string.Empty;

            PostgreSQL = false;
            PgDumpTableData = false;
            PgPort = DBSchemaExporterPostgreSQL.DEFAULT_PORT;

            OutputDirectoryPath = string.Empty;
            DatabaseSubdirectoryPrefix = DEFAULT_DB_OUTPUT_DIRECTORY_NAME_PREFIX;
            CreateDirectoryForEachDB = true;

            ServerOutputDirectoryNamePrefix = DEFAULT_SERVER_OUTPUT_DIRECTORY_NAME_PREFIX;

            ScriptingOptions = new DatabaseScriptingOptions();
        }

        /// <summary>
        /// Return Enabled if value is true
        /// Return Disabled if value is false
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        private static string BoolToEnabledDisabled(bool value)
        {
            return value ? "Enabled" : "Disabled";
        }

        /// <summary>
        /// Get the program version
        /// </summary>
        /// <returns></returns>
        public static string GetAppVersion()
        {
            var version = Assembly.GetExecutingAssembly().GetName().Version + " (" + PROGRAM_DATE + ")";

            return version;
        }

        /// <summary>
        /// Show the options at the console
        /// </summary>
        public void OutputSetOptions()
        {
            Console.WriteLine("Options:");
            Console.WriteLine();

            Console.WriteLine(" {0,-48} {1}", "Output directory path:", OutputDirectoryPath);

            Console.WriteLine(" {0,-48} {1}", "Server:", ServerName);

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
                Console.WriteLine(" {0,-48} {1}", "File with table names for exporting data:", TableDataToExportFile);
            }

            if (!string.IsNullOrWhiteSpace(TableDataColumnMapFile))
            {
                Console.WriteLine(" {0,-48} {1}", "File with source/target column names:", TableDataColumnMapFile);
            }

            if (!string.IsNullOrWhiteSpace(DefaultSchemaName))
            {
                Console.WriteLine(" {0,-48} {1}", "Default schema for exported tables and data:", DefaultSchemaName);
            }

            if (!DisableAutoDataExport || !string.IsNullOrWhiteSpace(TableDataToExportFile) || ExportAllData)
            {
                Console.WriteLine(" {0,-48} {1}", "Convert table and column names to snake_case:", BoolToEnabledDisabled(TableDataSnakeCase));
            }

            if (ExportAllData)
            {
                Console.WriteLine(" {0,-48} {1}", "Data export from all tables:", BoolToEnabledDisabled(ExportAllData));
            }
            else
            {
                Console.WriteLine(" {0,-48} {1}", "Data export from standard tables:", BoolToEnabledDisabled(!DisableAutoDataExport));
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

            if (NoSchema)
            {
                Console.WriteLine(" {0,-48} {1}", "Export table data only; no schema:", "Enabled");
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


            if (LogMessagesToFile)
            {
                if (string.IsNullOrWhiteSpace(LogFilePath))
                {
                    Console.WriteLine(" {0,-48} {1}", "Logging messages to:", "Default log file");
                }
                else
                {
                    Console.WriteLine(" {0,-48} {1}", "Logging messages to:", LogFilePath);
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
        /// <returns></returns>
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

            errorMessage = string.Empty;

            return true;
        }

        public void ValidateOutputOptions()
        {
            if (OutputDirectoryPath == null)
                OutputDirectoryPath = string.Empty;

            if (DatabaseSubdirectoryPrefix == null)
                DatabaseSubdirectoryPrefix = DEFAULT_DB_OUTPUT_DIRECTORY_NAME_PREFIX;

            if (ServerOutputDirectoryNamePrefix == null)
                ServerOutputDirectoryNamePrefix = DEFAULT_SERVER_OUTPUT_DIRECTORY_NAME_PREFIX;
        }
    }
}
