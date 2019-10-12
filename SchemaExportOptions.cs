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
        public const string PROGRAM_DATE = "October 12, 2019";

        public const string DEFAULT_DB_OUTPUT_DIRECTORY_NAME_PREFIX = "DBSchema__";

        public const string DEFAULT_SERVER_OUTPUT_DIRECTORY_NAME_PREFIX = "ServerSchema__";

        #endregion

        #region "Classwide Variables"

        /// <summary>
        /// List of databases to process (all on the same server)
        /// </summary>
        public SortedSet<string> DatabasesToProcess { get; }

        /// <summary>
        /// Options defining what to script
        /// </summary>
        public DatabaseScriptingOptions ScriptingOptions { get; }

        #endregion

        #region "Properties"

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

        [Option("PgPass", HelpShowsDefault = false,
            HelpText = "PostgreSQL user password")]
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

        [Option("NoSubdirectory", HelpShowsDefault = false, HelpText = "Disable creating a subdirectory for each database")]
        public bool NoSubdirectory
        {
            get => !CreateDirectoryForEachDB;
            set => CreateDirectoryForEachDB = !value;
        }

        /// <summary>
        /// When true (default), create a directory for each database
        /// </summary>
        public bool CreateDirectoryForEachDB { get; set; }

        [Option("Data", HelpShowsDefault = false, HelpText = "Text file with table names (one name per line) for which the table data should be exported")]
        public string TableDataToExportFile { get; set; }

        [Option("NoAutoData", HelpShowsDefault = false,
            HelpText = "In addition to table names defined in /Data, there are default tables which will have their data exported. " +
                       "Disable the defaults using /NoAutoData")]
        public bool DisableAutoDataExport { get; set; }

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
            CreateDirectoryForEachDB = true;

            DBUser = string.Empty;
            DBUserPassword = string.Empty;

            PostgreSQL = false;
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

            Console.WriteLine(" Output directory path: {0}", OutputDirectoryPath);

            Console.WriteLine(" Server: {0}", ServerName);

            if (!PostgreSQL)
            {
                Console.WriteLine(" Server type:   Microsoft SQL Server");
                if (string.IsNullOrWhiteSpace(DBUser))
                {
                    Console.WriteLine(" Database user: <integrated authentication>");
                }
                else
                {
                    Console.WriteLine(" Database user: {0}", DBUser);
                    if (string.IsNullOrWhiteSpace(DBUserPassword))
                        Console.WriteLine(" Password:      {0}", "Error: undefined");
                    else
                        Console.WriteLine(" Password:      {0}", "Provided");
                }
            }
            else
            {
                Console.WriteLine(" Server type: PostgreSQL");

                if (string.IsNullOrWhiteSpace(PgUser))
                    Console.WriteLine(" Username:    {0}", "Error: undefined");
                else
                    Console.WriteLine(" Username:    {0}", PgUser);

                if (string.IsNullOrWhiteSpace(PgPass))
                {
                    if (SystemInfo.IsLinux)
                        Console.WriteLine(" Password:    {0}", "Will be read from ~/.pgpass");
                    else
                        Console.WriteLine(" Password:    {0}", @"Will be read from %APPDATA%\postgresql\pgpass.conf");
                }
                else
                {
                    Console.WriteLine(" Password:      {0}", "Provided");
                }

                Console.WriteLine(" Port:        {0}", PgPort);
            }

            if (DatabasesToProcess.Count == 0)
            {
                Console.WriteLine(" Database to export: {0}", "Error: Undefined");
            }
            else if (DatabasesToProcess.Count == 1)
            {
                Console.WriteLine(" Database to export: {0}", DatabasesToProcess.First());
            }
            else
            {
                Console.WriteLine(" Databases to export: {0}", DatabaseList);
            }

            Console.WriteLine(" Database Subdirectory Prefix: {0}", DatabaseSubdirectoryPrefix);

            if (NoSubdirectory)
            {
                Console.WriteLine(" Will not create a subdirectory for each database");
            }

            if (!string.IsNullOrWhiteSpace(TableDataToExportFile))
            {
                Console.WriteLine(" Table name text file: {0}", DatabaseList);
            }

            Console.WriteLine(" Data export from standard tables: {0}", BoolToEnabledDisabled(!DisableAutoDataExport));

            Console.WriteLine(" Export server settings, logins, and jobs: {0}", BoolToEnabledDisabled(ScriptingOptions.ExportServerSettingsLoginsAndJobs));

            if (Sync)
            {
                Console.WriteLine(" Synchronizing schema files to: {0}", SyncDirectoryPath);
            }

            if (GitUpdate || SvnUpdate || HgUpdate)
            {
                if (GitUpdate)
                {
                    Console.WriteLine(" Auto-updating any new or changed files using Git");
                }

                if (SvnUpdate)
                {
                    Console.WriteLine(" Auto-updating any new or changed files using Subversion");
                }

                if (HgUpdate)
                {
                    Console.WriteLine(" Auto-updating any new or changed files using Mercurial");
                }

                Console.WriteLine(" Commit updates: {0}", BoolToEnabledDisabled(CommitUpdates));
            }


            if (LogMessagesToFile)
            {
                if (string.IsNullOrWhiteSpace(LogFilePath))
                {
                    Console.WriteLine(" Logging messages to the default log file");
                }
                else
                {
                    Console.WriteLine(" Logging messages to {0}", LogFilePath);
                }

                if (!string.IsNullOrWhiteSpace(LogDirectoryPath))
                {
                    Console.WriteLine(" Log directory path: {0}", LogDirectoryPath);
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
