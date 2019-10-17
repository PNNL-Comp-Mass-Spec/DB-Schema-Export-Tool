using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Npgsql;
using PRISM;

namespace DB_Schema_Export_Tool
{
    public class DBSchemaExporterPostgreSQL : DBSchemaExporterBase
    {
        #region "Constants and Enums"

        public const int DEFAULT_PORT = 5432;

        public const string POSTGRES_DATABASE = "postgres";

        #endregion

        #region "Classwide Variables"

        /// <summary>
        /// Dictionary tracking tables by database
        /// Keys are database name, values are a dictionary where keys are table names and values are row counts
        /// </summary>
        /// <remarks>Row counts will only be loaded if conditional compilation constant ENABLE_GUI is defined</remarks>
        private readonly Dictionary<string, Dictionary<string, long>> mCachedDatabaseTableNames;

        /// <summary>
        /// Dictionary of executables that have been found by FindPgDumpExecutable
        /// </summary>
        /// <remarks>Keys are the executable name; values are the file info object</remarks>
        private readonly Dictionary<string, FileInfo> mCachedExecutables;

        private readonly Regex mAclMatcherFunction;
        private readonly Regex mAclMatcherSchema;
        private readonly Regex mAclMatcherTable;

        private readonly Regex mFunctionNameMatcher;

        private readonly Regex mNameTypeSchemaMatcher;

        private readonly Regex mNameTypeTargetMatcher;

        private readonly Regex mTriggerTargetTableMatcher;

        private readonly ProgramRunner mProgramRunner;

        private NpgsqlConnection mPgConnection;

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="options"></param>
        public DBSchemaExporterPostgreSQL(SchemaExportOptions options) : base(options)
        {
            mProgramRunner = new ProgramRunner();
            RegisterEvents(mProgramRunner);

            mCachedDatabaseTableNames = new Dictionary<string, Dictionary<string, long>>();

            mCachedExecutables = new Dictionary<string, FileInfo>();

            // Match text like:
            // FUNCTION get_stat_activity(
            mAclMatcherFunction = new Regex("FUNCTION (?<FunctionName>[^(]+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            // Match text like:
            // SCHEMA mc
            mAclMatcherSchema = new Regex("SCHEMA (?<SchemaName>.+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            // Match text like:
            // TABLE t_event_log
            mAclMatcherTable = new Regex("TABLE (?<TableName>.+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            // Match text like:
            // get_stat_activity()
            mFunctionNameMatcher = new Regex("^[^(]+", RegexOptions.Compiled);

            // Match lines like:
            // -- Name: t_param_value; Type: TABLE; Schema: mc; Owner: d3l243
            // -- Name: v_manager_type_report; Type: VIEW; Schema: mc; Owner: d3l243
            mNameTypeSchemaMatcher = new Regex("^-- Name: (?<Name>.+); Type: (?<Type>.+); Schema: (?<Schema>.+); Owner: ?(?<Owner>.*)", RegexOptions.Compiled);

            // Match text like:
            // FUNCTION get_stat_activity()
            mNameTypeTargetMatcher = new Regex("(?<ObjectType>[a-z]+) (?<ObjectName>[^(]+)", RegexOptions.IgnoreCase | RegexOptions.Compiled);

            // Match text like:
            // t_users t_users_trigger_update_persisted
            mTriggerTargetTableMatcher = new Regex("[^ ]+", RegexOptions.Compiled);

        }

        /// <summary>
        /// Determines the table names for which data will be exported
        /// </summary>
        /// <param name="databaseName">Database name</param>
        /// <param name="tableNamesForDataExport">Table names that should be auto-selected</param>
        /// <param name="databaseNotFound">Output: true if the database does not exist on the server (or is inaccessible)</param>
        /// <returns>Dictionary where keys are table names and values are the maximum number of rows to export</returns>
        private Dictionary<string, long> AutoSelectTableNamesForDataExport(
            string databaseName,
            IEnumerable<string> tableNamesForDataExport,
            out bool databaseNotFound)
        {
            if (!mCachedDatabaseTableNames.ContainsKey(databaseName))
            {
                CacheDatabaseTableNames(databaseName, out databaseNotFound);
                if (databaseNotFound)
                {
                    SetLocalError(DBSchemaExportErrorCodes.GeneralError,
                                  string.Format("Error in AutoSelectTableNamesForDataExport; database not found: " + databaseName));
                    return new Dictionary<string, long>();
                }
            }

            var tablesInDatabase = mCachedDatabaseTableNames[databaseName];
            databaseNotFound = false;

            var tablesToExport = AutoSelectTableNamesForDataExport(tablesInDatabase.Keys.ToList(), tableNamesForDataExport);
            return tablesToExport;
        }

        private void CacheDatabaseTableNames(string databaseName, out bool databaseNotFound)
        {
#if ENABLE_GUI
            const bool INCLUDE_TABLE_ROW_COUNTS = true;
#else
            const bool INCLUDE_TABLE_ROW_COUNTS = false;
#endif
            const bool INCLUDE_SYSTEM_OBJECTS = false;
            const bool CLEAR_SCHEMA_OUTPUT_DIRS = false;

            var tablesInDatabase = GetPgServerDatabaseTableNames(databaseName, INCLUDE_TABLE_ROW_COUNTS,
                                                                 INCLUDE_SYSTEM_OBJECTS,
                                                                 CLEAR_SCHEMA_OUTPUT_DIRS,
                                                                 out databaseNotFound);

            if (mCachedDatabaseTableNames.ContainsKey(databaseName))
            {
                mCachedDatabaseTableNames[databaseName] = tablesInDatabase;
            }
            else
            {
                mCachedDatabaseTableNames.Add(databaseName, tablesInDatabase);
            }

        }

        /// <summary>
        /// Connect to the server specified in mOptions
        /// </summary>
        /// <param name="databaseName">PostgreSQL database to connect to</param>
        /// <returns>True if successfully connected, false if a problem</returns>
        public override bool ConnectToServer(string databaseName = "")
        {
            var success = ConnectToPgServer(databaseName);
            return success;
        }

        /// <summary>
        /// Connect to the server specified in mOptions
        /// </summary>
        /// <param name="databaseName">PostgreSQL database to connect to</param>
        /// <returns>True if successfully connected, false if a problem</returns>
        private bool ConnectToPgServer(string databaseName)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(databaseName))
                {
                    databaseName = POSTGRES_DATABASE;
                }

                // Initialize the current connection options
                if (mPgConnection == null)
                {
                    ResetServerConnection();
                }
                else if (ValidServerConnection())
                {
                    var expectedNameAndPort = string.Format("tcp://{0}:{1}", mOptions.ServerName, mOptions.PgPort);
                    if (string.Equals(mPgConnection.DataSource, expectedNameAndPort, StringComparison.OrdinalIgnoreCase))
                    {
                        if (mCurrentServerInfo.DatabaseName.Equals(databaseName) &&
                            mCurrentServerInfo.UserName.Equals(mOptions.DBUser))
                        {
                            // Already connected; no need to re-connect
                            return true;
                        }
                    }

                    try
                    {
                        mPgConnection.Close();
                    }
                    catch (Exception)
                    {
                        // Ignore errors here
                    }

                }

                // Connect to server mOptions.ServerName
                var connected = LoginToServerWork(databaseName, out mPgConnection);
                if (!connected)
                {
                    if (ErrorCode == DBSchemaExportErrorCodes.NoError)
                    {
                        SetLocalError(DBSchemaExportErrorCodes.DatabaseConnectionError, "Error logging into server " + GetServerConnectionInfo());
                    }
                }

                return connected;
            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.DatabaseConnectionError, "Error logging into server " + GetServerConnectionInfo(), ex);
                mConnectedToServer = false;
                mPgConnection = null;
                return false;
            }

        }

        /// <summary>
        /// Export the tables, views, procedures, etc. in the given database
        /// Also export data from tables in tableNamesForDataExport
        /// </summary>
        /// <param name="databaseName"></param>
        /// <param name="tableNamesForDataExport"></param>
        /// <param name="databaseNotFound"></param>
        /// <returns>True if successful, false if an error</returns>
        protected override bool ExportDBObjects(
            string databaseName,
            IReadOnlyCollection<string> tableNamesForDataExport,
            out bool databaseNotFound)
        {
            return ExportDBObjectsAndTableData(databaseName, tableNamesForDataExport, out databaseNotFound);
        }

        /// <summary>
        /// Script the tables, views, function, etc. in the specified database
        /// </summary>
        /// <param name="databaseName">Database name</param>
        /// <param name="tableNamesForDataExport">Table names that should be auto-selected</param>
        /// <param name="databaseNotFound">Output: true if the database does not exist on the server (or is inaccessible)</param>
        /// <returns>True if successful, false if an error</returns>
        private bool ExportDBObjectsAndTableData(
            string databaseName,
            IEnumerable<string> tableNamesForDataExport,
            out bool databaseNotFound)
        {
            var workingParams = new WorkingParams();

            // Keys are table names to export
            // Values are the maximum number of rows to export
            Dictionary<string, long> tablesToExport;
            databaseNotFound = false;

            OnDBExportStarting(databaseName);

            var isValid = ValidateOutputDirectoryForDatabaseExport(databaseName, workingParams);
            if (!isValid)
            {
                return false;
            }

            try
            {
                if (mOptions.ScriptingOptions.AutoSelectTableNamesForDataExport)
                {
                    tablesToExport = AutoSelectTableNamesForDataExport(databaseName, tableNamesForDataExport, out databaseNotFound);
                }
                else
                {
                    tablesToExport = new Dictionary<string, long>();
                    foreach (var tableName in tableNamesForDataExport)
                    {
                        tablesToExport.Add(tableName, 0);
                    }

                }
            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.DatabaseConnectionError,
                              "Error auto selecting table names for data export from database" + databaseName, ex);
                databaseNotFound = false;
                return false;
            }

            try
            {
                // Export the database schema
                var success = ExportDBObjectsWork(databaseName, workingParams, out databaseNotFound);

                // Export data from tables specified by tablesToExport
                var dataSuccess = ExportDBTableData(databaseName, tablesToExport, workingParams);

                return success && dataSuccess;
            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.GeneralError, "Error scripting objects in database " + databaseName, ex);
                return false;
            }
        }

        /// <summary>
        /// Script the tables, views, function, etc. in the specified database
        /// </summary>
        /// <param name="databaseName">Database name</param>
        /// <param name="workingParams">Working parameters</param>
        /// <param name="databaseNotFound">Output: true if the database does not exist on the server (or is inaccessible)</param>
        /// <remarks>
        /// Do not include a Try block in this Function; let the calling function handle errors
        /// </remarks>
        private bool ExportDBObjectsWork(string databaseName, WorkingParams workingParams, out bool databaseNotFound)
        {
            databaseNotFound = false;

            var pgDumpOutputFile = new FileInfo(Path.Combine(workingParams.OutputDirectory.FullName, "_AllObjects_.sql"));

            var existingData = pgDumpOutputFile.Exists ? pgDumpOutputFile.LastWriteTime : DateTime.MinValue;

            var serverInfoArgs = GetPgDumpServerInfoArgs(string.Empty);

            // pg_dump -h host -p port -U user -W PasswordIfDefined -d database --schema-only --format=p --file=OutFilePath
            var cmdArgs = string.Format("{0} -d {1} --schema-only --format=p --file={2}", serverInfoArgs, databaseName, pgDumpOutputFile.FullName);
            var maxRuntimeSeconds = 60;

            var pgDump = FindPgDumpExecutable();
            if (pgDump == null)
            {
                return false;
            }

            if (mOptions.PreviewExport)
            {
                OnStatusEvent(string.Format("Preview running {0} {1}", pgDump.FullName, cmdArgs));
                return true;
            }

            var success = mProgramRunner.RunCommand(pgDump.FullName, cmdArgs, workingParams.OutputDirectory.FullName,
                                                    out var consoleOutput, out var errorOutput, maxRuntimeSeconds);

            if (!success)
            {
                OnWarningEvent(string.Format("Error reported for {0}: {1}", pgDump.Name, consoleOutput));

                var matcher = new Regex("database [^ ]+ does not exist", RegexOptions.Compiled | RegexOptions.IgnoreCase);

                databaseNotFound = matcher.IsMatch(consoleOutput) || matcher.IsMatch(errorOutput);
                return false;
            }

            // Assure that the file was created
            pgDumpOutputFile.Refresh();

            if (pgDumpOutputFile.LastWriteTime > existingData)
            {

                // Parse the pgDump output file and create separate files for each object
                ProcessPgDumpSchemaFile(databaseName, pgDumpOutputFile, out var unhandledScriptingCommands);

                if (!unhandledScriptingCommands)
                {
                    // Delete the _AllObjects_.sql file (since we no longer need it)

                    // ToDo: Uncomment the delete command
                    Console.WriteLine("ToDo: Delete file " + pgDumpOutputFile.FullName);
                    // pgDumpOutputFile.Delete();
                }

                return true;
            }

            if (pgDumpOutputFile.Exists)
                OnWarningEvent(string.Format("{0} did not replace {1}", pgDump.Name, pgDumpOutputFile.FullName));
            else
                OnWarningEvent(string.Format("{0} did not create {1}", pgDump.Name, pgDumpOutputFile.FullName));

            return false;
        }
        /// <summary>
        /// Export data from the specified table (if it exists)
        /// </summary>
        /// <param name="databaseName">Database name</param>
        /// <param name="tableName">Table name</param>
        /// <param name="maxRowsToExport">Maximum rows to export</param>
        /// <param name="workingParams">Working parameters</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>If the table does not exist, will still return true</remarks>
        protected override bool ExportDBTableData(string databaseName, string tableName, long maxRowsToExport, WorkingParams workingParams)
        {

            if (!mCachedDatabaseTableNames.ContainsKey(databaseName))
            {
                CacheDatabaseTableNames(databaseName, out var databaseNotFound);
                if (databaseNotFound)
                {
                    SetLocalError(DBSchemaExportErrorCodes.GeneralError,
                                  string.Format("Error in ExportDBTableData; database not found: " + databaseName));
                    return false;
                }
            }

            var tablesInDatabase = mCachedDatabaseTableNames[databaseName];
            var tableNameWithSchema = string.Empty;

            var quotedTableName = PossiblyQuoteName(tableName);

            var tableNamesToFind = new List<string> {
                tableName,
                quotedTableName,
                "public." + tableName,
                "public." + PossiblyQuoteName(tableName)};

            foreach (var nameToFind in tableNamesToFind)
            {
                if (tablesInDatabase.ContainsKey(nameToFind))
                {
                    tableNameWithSchema = nameToFind;
                    break;
                }
            }

            if (string.IsNullOrWhiteSpace(tableNameWithSchema))
            {
                OnDebugEvent(string.Format("Database {0} does not have table {1}; skipping data export", databaseName, tableName));
                return true;
            }

            bool success;
            if (mOptions.PgDumpTableData)
            {
                success = ExportDBTableDataUsingPgDump(databaseName, tableName, workingParams);
            }
            else
            {
                success = ExportDBTableDataUsingNpgsql(databaseName, tableName, maxRowsToExport, workingParams);
            }

            return success;
        }

        private bool ExportDBTableDataUsingNpgsql(string databaseName, string tableName, long maxRowsToExport, WorkingParams workingParams)
        {
            ConnectToServer(databaseName);
            throw new NotImplementedException();
        }

        private bool ExportDBTableDataUsingPgDump(string databaseName, string tableName, WorkingParams workingParams)
        {
            var pgDumpOutputFile = new FileInfo(Path.Combine(workingParams.OutputDirectory.FullName, tableName + "_data.sql"));

            var existingData = pgDumpOutputFile.Exists ? pgDumpOutputFile.LastWriteTime : DateTime.MinValue;

            var serverInfoArgs = GetPgDumpServerInfoArgs(string.Empty);

            // pg_dump -h host -p port -U user -W PasswordIfDefined -d database --data-only --table=TableName --format=p --file=OutFilePath
            var cmdArgs = string.Format("{0} -d {1} --data-only --table={2} --format=p --file={3}", serverInfoArgs, databaseName, tableName, pgDumpOutputFile.FullName);
            var maxRuntimeSeconds = 60;

            var pgDump = FindPgDumpExecutable();
            if (pgDump == null)
            {
                return false;
            }

            if (mOptions.PreviewExport)
            {
                OnStatusEvent(string.Format("Preview running {0} {1}", pgDump.FullName, cmdArgs));
                return true;
            }

            var success = mProgramRunner.RunCommand(pgDump.FullName, cmdArgs, workingParams.OutputDirectory.FullName,
                                                    out var consoleOutput, out var errorOutput, maxRuntimeSeconds);

            if (!success)
            {
                OnWarningEvent(string.Format("Error reported for {0}: {1}", pgDump.Name, consoleOutput));
                return false;
            }

            // Assure that the file was created
            pgDumpOutputFile.Refresh();

            if (pgDumpOutputFile.LastWriteTime > existingData)
            {

                // Parse the pgDump output file to clean it up
                ProcessPgDumpDataFile(pgDumpOutputFile);
                return true;
            }

            if (pgDumpOutputFile.Exists)
                OnWarningEvent(string.Format("{0} did not replace {1}", pgDump.Name, pgDumpOutputFile.FullName));
            else
                OnWarningEvent(string.Format("{0} did not create {1}", pgDump.Name, pgDumpOutputFile.FullName));

            return false;
        }

        private FileInfo FindNewestExecutable(DirectoryInfo baseDirectory, string exeName)
        {
            var foundFiles = baseDirectory.GetFileSystemInfos(exeName, SearchOption.AllDirectories);
            if (foundFiles.Length == 0)
                return null;

            FileInfo newestItem = null;

            foreach (var item in foundFiles)
            {
                if (item is FileInfo matchingExe && (newestItem == null || item.LastWriteTime > newestItem.LastWriteTime))
                {
                    newestItem = matchingExe;
                }
            }

            return newestItem;
        }

        private FileInfo FindPgDumpExecutable()
        {
            return SystemInfo.IsLinux ? FindPgExecutableLinux("pg_dump") : FindPgExecutableWindows("pg_dump.exe");
        }

        private FileInfo FindPgDumpAllExecutable()
        {
            return SystemInfo.IsLinux ? FindPgExecutableLinux("pg_dumpall") : FindPgExecutableWindows("pg_dumpall.exe");
        }

        private FileInfo FindPgExecutableLinux(string exeName)
        {
            try
            {
                if (mCachedExecutables.TryGetValue(exeName, out var cachedFileInfo))
                    return cachedFileInfo;

                var alternativesDir = new DirectoryInfo("/etc/alternatives");
                if (alternativesDir.Exists)
                {
                    var symLinkFile = new FileInfo("/etc/alternatives/pgsql-" + exeName);
                    if (symLinkFile.Exists)
                    {
                        mCachedExecutables.Add(exeName, symLinkFile);
                        return symLinkFile;
                    }
                }

                var userDirectory = new DirectoryInfo("/usr");
                if (userDirectory.Exists)
                {
                    // Find the newest file named exeName
                    var foundFile = FindNewestExecutable(userDirectory, exeName);
                    if (foundFile != null)
                    {
                        mCachedExecutables.Add(exeName, foundFile);
                        return foundFile;
                    }
                }

                var workingDirectory = new DirectoryInfo(".");
                var foundWorkDirFile = FindNewestExecutable(workingDirectory, exeName);
                if (foundWorkDirFile != null)
                {
                    mCachedExecutables.Add(exeName, foundWorkDirFile);
                    return foundWorkDirFile;
                }

                OnWarningEvent(string.Format("Could not find {0} in {1}, below {2}, or below the working directory",
                                             exeName, alternativesDir.FullName, userDirectory.FullName));
                return null;


            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.GeneralError,
                              string.Format("Error in FindPgExecutableLinux finding {0}", exeName), ex);

                return null;
            }

        }

        private FileInfo FindPgExecutableWindows(string exeName)
        {
            try
            {
                if (mCachedExecutables.TryGetValue(exeName, out var cachedFileInfo))
                    return cachedFileInfo;

                var postgresDirectory = new DirectoryInfo(@"C:\Program Files\PostgreSQL");
                if (postgresDirectory.Exists)
                {
                    // Find the newest file named exeName
                    var foundFile = FindNewestExecutable(postgresDirectory, exeName);
                    if (foundFile != null)
                    {
                        mCachedExecutables.Add(exeName, foundFile);
                        return foundFile;
                    }
                }

                var workingDirectory = new DirectoryInfo(".");
                var foundWorkDirFile = FindNewestExecutable(workingDirectory, exeName);
                if (foundWorkDirFile != null)
                {
                    mCachedExecutables.Add(exeName, foundWorkDirFile);
                    return foundWorkDirFile;
                }

                OnWarningEvent(string.Format("Could not find {0} in either a subdirectory of {1} or below the working directory",
                                             exeName, postgresDirectory.FullName));
                return null;
            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.GeneralError,
                              string.Format("Error in FindPgExecutableWindows finding {0}", exeName), ex);

                return null;
            }

        }

        /// <summary>
        /// Retrieve a list of tables in the given database
        /// </summary>
        /// <param name="databaseName">Database to query</param>
        /// <param name="includeTableRowCounts">When true, then determines the row count in each table</param>
        /// <param name="includeSystemObjects">When true, then also returns system var tables</param>
        /// <returns>Dictionary where keys are table names and values are row counts (if includeTableRowCounts = true)</returns>
        public override Dictionary<string, long> GetDatabaseTableNames(string databaseName, bool includeTableRowCounts, bool includeSystemObjects)
        {
            return GetPgServerDatabaseTableNames(databaseName, includeTableRowCounts, includeSystemObjects, out _);
        }

        /// <summary>
        /// Obtain the arguments to pass to pg_dump or pg_dumpall, specifying host, port, user, etc.
        /// </summary>
        /// <param name="databaseName">Database name, or an empty string if no database</param>
        /// <returns>List of arguments in the form "-h hostname -p 5432 -U Username"</returns>
        private string GetPgDumpServerInfoArgs(string databaseName)
        {
            string passwordArgument;
            if (string.IsNullOrWhiteSpace(mOptions.DBUserPassword))
                passwordArgument = string.Empty;
            else
                passwordArgument = "-W " + mOptions.DBUserPassword;

            string databaseArgument;
            if (string.IsNullOrWhiteSpace(databaseName))
                databaseArgument = string.Empty;
            else
                databaseArgument = "-d " + databaseName;

            var serverInfoArgs = string.Format("-h {0} -p {1} -U {2} {3}{4}",
                                               mOptions.ServerName, mOptions.PgPort, mOptions.DBUser,
                                               passwordArgument, databaseArgument);

            return serverInfoArgs;
        }

        /// <summary>
        /// Lookup the table names in the specified database, optionally also determining table row counts
        /// </summary>
        /// <param name="databaseName">Database to query</param>
        /// <param name="includeTableRowCounts">When true, then determines the row count in each table</param>
        /// <param name="includeSystemObjects">When true, then also returns system var tables</param>
        /// <param name="databaseNotFound">Output: true if the database does not exist on the server (or is inaccessible)</param>
        /// <returns>Dictionary where keys are table names and values are row counts (if includeTableRowCounts = true)</returns>
        public Dictionary<string, long> GetPgServerDatabaseTableNames(
            string databaseName,
            bool includeTableRowCounts,
            bool includeSystemObjects,
            out bool databaseNotFound)
        {
            // Keys are table names; values are row counts
            var databaseTableInfo = new Dictionary<string, long>();
            databaseNotFound = false;

            try
            {
                InitializeLocalVariables(clearSchemaOutputDirectories);

                if (!ConnectToServer(databaseName))
                {
                    var databaseList = GetServerDatabases();
                    if (!databaseList.Contains(databaseName))
                    {
                        OnWarningEvent(string.Format("Database {0} not found on sever {1}", databaseName, mCurrentServerInfo.ServerName));
                        databaseNotFound = true;
                    }

                    return databaseTableInfo;
                }

                if (!ValidServerConnection())
                {
                    OnWarningEvent("Not connected to a server; cannot retrieve the list of the tables in database " + databaseName);
                    return databaseTableInfo;
                }
            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.DatabaseConnectionError,
                              string.Format("Error connecting to server {0}", mCurrentServerInfo.ServerName), ex);
                return new Dictionary<string, long>();
            }

            try
            {
                if (string.IsNullOrWhiteSpace(databaseName))
                {
                    OnWarningEvent("Empty database name sent to GetSqlServerDatabaseTableNames");
                    return databaseTableInfo;
                }

                // Get the list of tables in this database
                OnDebugEvent(string.Format("Obtaining list of tables in database {0} on server {1}",
                                           databaseName, mCurrentServerInfo.ServerName));

                const string sql = "SELECT schemaname, tablename, tableowner FROM pg_tables";

                var tableListCommand = new NpgsqlCommand(sql, mPgConnection);
                using (var reader = tableListCommand.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            var schemaName = reader.GetString(0);
                            var tableName = reader.GetString(1);
                            // var tableOwner = reader.GetString(2);

                            var isSystemObject = schemaName.Equals("pg_catalog") || schemaName.Equals("information_schema");

                            if (!includeSystemObjects && isSystemObject)
                                continue;

                            var tableNameWithSchema = PossiblyQuoteColumnName(schemaName, false) + "." +
                                                      PossiblyQuoteColumnName(tableName, false);

                            databaseTableInfo.Add(tableNameWithSchema, 0);

                        }
                    }
                }

                if (includeTableRowCounts)
                {
                    var tableNames = databaseTableInfo.Keys.ToList();
                    var index = 0;

                    foreach (var tableNameWithSchema in tableNames)
                    {

                        // ReSharper disable StringLiteralTypo
                        var rowCountSql = string.Format("SELECT relname, reltuples::bigint as ApproximateRowCount " +
                                                        "FROM pg_class " +
                                                        "WHERE oid = '{0}'::regclass", tableNameWithSchema);
                        // ReSharper restore StringLiteralTypo

                        var rowCountCmd = new NpgsqlCommand(rowCountSql, mPgConnection);
                        using (var reader = rowCountCmd.ExecuteReader())
                        {
                            if (reader.HasRows && reader.Read())
                            {
                                // var tableName = reader.GetString(0);
                                var approximateRowCount = reader.GetInt64(1);

                                databaseTableInfo[tableNameWithSchema] = approximateRowCount;
                            }
                        }

                        index++;
                        var subTaskProgress = ComputeSubtaskProgress(index, tableNames.Count);
                        var percentComplete = ComputeIncrementalProgress(0, 50, subTaskProgress);
                        OnProgressUpdate("Reading database tables", percentComplete);

                        if (mAbortProcessing)
                        {
                            OnWarningEvent("Aborted processing");
                            break;
                        }
                    }

                    // Re-query tables with a row count of 0, since they likely were not properly listed in pg_class
                    index = 0;
                    foreach (var tableNameWithSchema in tableNames)
                    {
                        if (databaseTableInfo[tableNameWithSchema] > 0)
                            continue;

                        var rowCountSql = string.Format("SELECT count(*) FROM {0} ", tableNameWithSchema);

                        var rowCountCmd = new NpgsqlCommand(rowCountSql, mPgConnection);
                        using (var reader = rowCountCmd.ExecuteReader())
                        {
                            if (reader.HasRows && reader.Read())
                            {
                                var rowCount = reader.GetInt64(0);
                                databaseTableInfo[tableNameWithSchema] = rowCount;
                            }
                        }

                        index++;
                        var subTaskProgress = ComputeSubtaskProgress(index, tableNames.Count);
                        var percentComplete = ComputeIncrementalProgress(50, 100, subTaskProgress);
                        OnProgressUpdate("Reading database tables", percentComplete);

                        if (mAbortProcessing)
                        {
                            OnWarningEvent("Aborted processing");
                            break;
                        }
                    }

                }

                OnProgressComplete();
                OnStatusEvent(string.Format("Found {0} tables in database {1}", databaseTableInfo.Count, databaseName));

                return databaseTableInfo;
            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.GeneralError,
                              string.Format("Error obtaining list of tables in database {0} on server {1}",
                                            databaseName, mCurrentServerInfo.ServerName), ex);

                return new Dictionary<string, long>();
            }
        }

        private string GetServerConnectionInfo()
        {
            return string.Format("{0}, port {1}, user {2}", mOptions.ServerName, mOptions.PgPort, mOptions.DBUser);
        }

        public override IEnumerable<string> GetServerDatabases()
        {
            try
            {
                InitializeLocalVariables(true);

                if (!ConnectToServer())
                {
                    return new List<string>();
                }

                if (!ValidServerConnection())
                {
                    OnWarningEvent("Not connected to a server; cannot retrieve the list of the server's databases");
                    return new List<string>();
                }

                // Obtain a list of all databases actually residing on the server (according to the Master database)
                OnStatusEvent("Obtaining list of databases on " + mCurrentServerInfo.ServerName);

                var databaseNames = GetServerDatabasesWork();
                if (!mAbortProcessing)
                {
                    OnProgressUpdate("Done", 100);
                }

                return databaseNames;

            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.DatabaseConnectionError, "Error obtaining list of databases on current server", ex);
                return new List<string>();
            }
        }

        protected override IEnumerable<string> GetServerDatabasesCurrentConnection()
        {
            var databaseNames = GetServerDatabasesWork();
            return databaseNames;
        }

        private IEnumerable<string> GetServerDatabasesWork()
        {
            var databaseNames = new List<string>();

            // ReSharper disable StringLiteralTypo
            const string sql = "SELECT datname FROM pg_database WHERE datistemplate=false and datallowconn=true ORDER BY datname";
            // ReSharper restore StringLiteralTypo

            var cmd = new NpgsqlCommand(sql, mPgConnection);
            using (var reader = cmd.ExecuteReader())
            {
                if (reader.HasRows)
                {
                    while (reader.Read())
                    {
                        databaseNames.Add(reader.GetString(0));
                    }
                }
            }

            return databaseNames;
        }

        /// <summary>
        /// Login to the server
        /// </summary>
        /// <param name="databaseName">PostgreSQL database to connect to</param>
        /// <param name="pgConnection"></param>
        /// <returns>True if success, otherwise false</returns>
        private bool LoginToServerWork(string databaseName, out NpgsqlConnection pgConnection)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(mOptions.DBUser))
                {
                    SetLocalError(DBSchemaExportErrorCodes.DatabaseConnectionError, "Cannot use an empty username to login to server " + mOptions.ServerName);
                    pgConnection = null;
                    return false;
                }

                string userPassword;
                if (string.IsNullOrWhiteSpace(mOptions.DBUserPassword))
                {
                    userPassword = LookupUserPasswordFromDisk(mOptions.DBUser, databaseName, out var definedInPgPassFile);
                    if (string.IsNullOrWhiteSpace(userPassword) && !definedInPgPassFile)
                    {
                        // A warning or error should have already been logged
                        pgConnection = null;
                        return false;
                    }
                }
                else
                {
                    userPassword = mOptions.DBUserPassword;
                }

                // If userPassword is blank, Npgsql (and pg_dump and pd_dumpall) will look for the user's password in the user's pgpass file
                var connectionString = string.Format("Host={0};Username={1};Password={2};Database={3}",
                                                     mOptions.ServerName, mOptions.DBUser, userPassword, databaseName);

                pgConnection = new NpgsqlConnection(connectionString);
                pgConnection.Open();

                // If no error occurred, set .Connected = True and duplicate the connection info
                mConnectedToServer = true;
                mCurrentServerInfo.UpdateInfo(mOptions, databaseName);

                return true;
            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.DatabaseConnectionError, "Error logging into server " + GetServerConnectionInfo(), ex);
                pgConnection = null;
                return false;
            }
        }

        private string LookupUserPasswordFromDisk(string pgUser, string currentDatabase, out bool definedInPgPassFile)
        {
            try
            {
                // Keys in this dictionary are file paths, values are true if this is the standard location for a pgpass file
                // This method will return an empty string if a match is found to an entry in a pgpass file in the standard location for this OS
                var candidateFilePaths = new Dictionary<string, bool>();
                string passwordFileName;

                if (SystemInfo.IsLinux)
                {
                    passwordFileName = ".pgpass";
                    // Standard location: ~/.pgpass
                    candidateFilePaths.Add(Path.Combine("~", passwordFileName), true);

                    // .pgpass in the current directory
                    candidateFilePaths.Add(passwordFileName, false);
                }
                else
                {
                    passwordFileName = "pgpass.conf";
                    var appdataDirectory = Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData);

                    // Standard location: %APPDATA%\postgresql\pgpass.conf
                    candidateFilePaths.Add(Path.Combine(appdataDirectory, "postgresql", passwordFileName), true);

                    // pgpass.conf in the current directory
                    candidateFilePaths.Add(passwordFileName, false);
                }

                var passwordFileExists = false;

                for (var iteration = 0; iteration < 2; iteration++)
                {
                    var caseSensitive = iteration == 0;

                    foreach (var candidateFileInfo in candidateFilePaths)
                    {
                        var candidateFile = new FileInfo(candidateFileInfo.Key);
                        var isStandardLocation = candidateFileInfo.Value;

                        if (candidateFile.Exists)
                        {
                            passwordFileExists = true;
                            var passwordToReturn = LookupUserPasswordFromDisk(candidateFile, isStandardLocation,
                                                                              pgUser, currentDatabase,
                                                                              caseSensitive, out definedInPgPassFile);

                            if (!string.IsNullOrWhiteSpace(passwordToReturn) || definedInPgPassFile)
                            {
                                return passwordToReturn;
                            }
                        }
                    }
                }

                if (!passwordFileExists)
                {
                    OnWarningEvent(string.Format("Could not find the {0} file; unable to determine the password for user {1}",
                                                 passwordFileName, pgUser));
                    definedInPgPassFile = false;
                    return string.Empty;
                }

                OnWarningEvent(string.Format("Could not find a valid password for user {0} in file {1}", pgUser, passwordFileName));
                definedInPgPassFile = false;
                return string.Empty;
            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.DatabaseConnectionError,
                              string.Format("Error looking up the password for user {0} on server {1}",
                                            pgUser, GetServerConnectionInfo()), ex);
                definedInPgPassFile = false;
                return string.Empty;
            }
        }

        private string LookupUserPasswordFromDisk(
            FileSystemInfo passwordFile,
            bool isStandardLocation,
            string pgUser,
            string currentDatabase,
            bool caseSensitive,
            out bool definedInPgPassFile)
        {

            using (var reader = new StreamReader(new FileStream(passwordFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
            {
                var comparisonType = caseSensitive ? StringComparison.Ordinal : StringComparison.OrdinalIgnoreCase;

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();
                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    // dataLine is of the form
                    // hostname:port:database:username:password
                    // The first four fields can contain a *
                    var lineParts = dataLine.Split(new[] { ':' }, 5);

                    if (lineParts.Length < 5)
                        continue;

                    var hostname = lineParts[0];
                    var port = lineParts[1];
                    var database = lineParts[2];
                    var username = lineParts[3];

                    // Passwords are allowed to contain a colon; it should be preceded by a backslash in the pgpass file
                    var password = lineParts[4].Replace(@"\:", ":");

                    if (!string.Equals(hostname, mOptions.ServerName, comparisonType) && !hostname.Equals("*"))
                        continue;

                    if (!port.Equals("*"))
                    {
                        if (!int.TryParse(port, out var portValue))
                            continue;

                        if (portValue != mOptions.PgPort)
                            continue;
                    }

                    if (!string.IsNullOrWhiteSpace(currentDatabase))
                    {
                        if (!string.Equals(database, currentDatabase, comparisonType) && !database.Equals("*"))
                            continue;
                    }

                    if (!string.Equals(username, pgUser, comparisonType) && !username.Equals("*"))
                        continue;

                    if (string.IsNullOrWhiteSpace(password))
                    {
                        OnWarningEvent(string.Format("The {0} file has a blank password for user {1}, server {2}, database {3}; ignoring this entry",
                                                     passwordFile.Name, username, hostname, database));
                        continue;
                    }

                    // If we get here, this password is valid for the current connection

                    if (comparisonType == StringComparison.OrdinalIgnoreCase)
                    {
                        // Auto-update server and/or user if a case mismatch
                        if (string.Equals(hostname, mOptions.ServerName, StringComparison.OrdinalIgnoreCase) &&
                            !string.Equals(hostname, mOptions.ServerName, StringComparison.Ordinal))
                        {
                            mOptions.ServerName = hostname;
                        }

                        if (string.Equals(username, mOptions.DBUser, StringComparison.OrdinalIgnoreCase) &&
                            !string.Equals(username, mOptions.DBUser, StringComparison.Ordinal))
                        {
                            mOptions.DBUser = username;
                        }

                    }

                    OnDebugEvent(string.Format("Determined password for user {0} using {1}", pgUser, passwordFile.FullName));

                    if (isStandardLocation)
                    {
                        definedInPgPassFile = true;
                        return string.Empty;
                    }

                    definedInPgPassFile = false;
                    return password;
                }
            }

            definedInPgPassFile = false;
            return string.Empty;
        }

        /// <summary>
        /// If objectName contains characters other than A-Z, a-z, 0-9, or an underscore, surround the name with double quotes
        /// </summary>
        /// <param name="objectName"></param>
        /// <returns></returns>
        protected override string PossiblyQuoteName(string objectName)
        {
            return PossiblyQuoteName(objectName, false);
        }

        private void ProcessCachedLines(
            string databaseName,
            IEnumerable<string> cachedLines,
            string currentObjectName,
            string currentObjectType,
            string currentObjectSchema,
            string previousTargetScriptFile,
            out string targetScriptFile,
            out bool unhandledScriptingCommands)
        {

            unhandledScriptingCommands = false;

            if (string.IsNullOrEmpty(currentObjectName))
            {
                targetScriptFile = string.Format("DatabaseInfo_{0}.sql", databaseName);
                return;
            }

            var schemaToUse = currentObjectSchema;
            var nameToUse = currentObjectName;

            switch (currentObjectType)
            {
                case "TABLE":
                case "VIEW":
                    break;

                case "ACL":
                    var aclFunctionMatch = mAclMatcherFunction.Match(currentObjectName);
                    var aclSchemaMatch = mAclMatcherSchema.Match(currentObjectName);
                    var aclTableMatch = mAclMatcherTable.Match(currentObjectName);

                    if (aclTableMatch.Success)
                    {
                        nameToUse = aclTableMatch.Groups["TableName"].Value;
                    }
                    else if (aclFunctionMatch.Success) {
                        nameToUse = aclFunctionMatch.Groups["FunctionName"].Value;
                    }
                    else if (aclSchemaMatch.Success) {
                        nameToUse = "_Schema_" + aclSchemaMatch.Groups["SchemaName"].Value;
                        schemaToUse = string.Empty;
                    }
                    else
                    {
                        // Unmatched ACL
                        schemaToUse = string.Empty;
                        nameToUse = "_Permissions";
                    }

                    break;

                case "DEFAULT ACL":
                    schemaToUse = string.Empty;
                    nameToUse = "_Permissions";
                    break;

                case "COMMENT":
                    var typeMatch = mNameTypeTargetMatcher.Match(currentObjectName);
                    if (typeMatch.Success)
                    {
                        var targetObjectType = typeMatch.Groups["ObjectType"].Value;
                        var targetObjectName = typeMatch.Groups["ObjectName"].Value;

                        switch (targetObjectType)
                        {
                            case "EXTENSION":
                                nameToUse = "_Extension_" + targetObjectName;
                                break;
                            case "FUNCTION":
                                nameToUse = targetObjectName;
                                break;
                            default:
                                OnWarningEvent("Possibly add a custom object type handler for target object " + targetObjectType);
                                nameToUse = targetObjectName;
                                unhandledScriptingCommands = true;
                                break;
                        }


                    }
                    else
                    {
                        OnWarningEvent("Comment object type match failure for " + currentObjectName);
                        unhandledScriptingCommands = true;
                    }

                    break;

                case "CONSTRAINT":
                case "FK CONSTRAINT":
                    // Parse out the target table name from the Alter Table DDL

                    // The regex will match lines like this:
                    // ALTER TABLE mc.t_event_log
                    // ALTER TABLE ONLY mc.t_event_log

                    var alterTableMatcher = new Regex(string.Format(@"ALTER TABLE.+ {0}\.(?<TargetTable>.+)", schemaToUse),
                                                      RegexOptions.Compiled | RegexOptions.IgnoreCase);

                    var alterTableMatched = false;
                    foreach (var cachedLine in cachedLines)
                    {
                        var match = alterTableMatcher.Match(cachedLine);
                        if (!match.Success)
                            continue;

                        nameToUse = match.Groups["TargetTable"].Value;
                        alterTableMatched = true;
                        break;
                    }

                    if (!alterTableMatched)
                    {
                        OnWarningEvent("Did not find a valid ALTER TABLE line in the cached lines for a constraint against: " + currentObjectName);
                        unhandledScriptingCommands = true;
                    }
                    break;

                case "EVENT TRIGGER":
                    nameToUse = "_EventTrigger_" + currentObjectName;
                    break;

                case "EXTENSION":
                    nameToUse = "_Extension_" + currentObjectName;
                    break;

                case "FUNCTION":
                    var functionNameMatch = mFunctionNameMatcher.Match(currentObjectName);
                    if (functionNameMatch.Success)
                    {
                        nameToUse = functionNameMatch.Value;
                    }
                    else
                    {
                        OnWarningEvent("Did not find a function name in : " + currentObjectName);
                        unhandledScriptingCommands = true;
                    }

                    break;

                case "INDEX":
                    var indexName = currentObjectName;
                    var createIndexMatcher = new Regex(string.Format(@"CREATE.+INDEX {0} ON (?<TargetTable>.+) USING", indexName),
                                                       RegexOptions.Compiled | RegexOptions.IgnoreCase);

                    var createIndexMatched = false;
                    foreach (var cachedLine in cachedLines)
                    {
                        var match = createIndexMatcher.Match(cachedLine);
                        if (!match.Success)
                            continue;

                        nameToUse = match.Groups["TargetTable"].Value;
                        createIndexMatched = true;
                        break;
                    }

                    if (!createIndexMatched)
                    {
                        OnWarningEvent("Did not find a valid CREATE INDEX line in the cached lines for index: " + currentObjectName);
                        unhandledScriptingCommands = true;
                    }

                    break;

                case "SCHEMA":
                    nameToUse = "_Schema_" + currentObjectName;
                    schemaToUse = string.Empty;
                    break;

                case "SEQUENCE":
                    targetScriptFile = previousTargetScriptFile;
                    return;

                case "TRIGGER":
                    var triggerTableMatch = mTriggerTargetTableMatcher.Match(currentObjectName);

                    if (triggerTableMatch.Success)
                    {
                        nameToUse = triggerTableMatch.Value;
                    }
                    else
                    {
                        OnWarningEvent("Did not find a valid table name for trigger: " + currentObjectName);
                        unhandledScriptingCommands = true;
                    }
                    break;

                default:
                    OnWarningEvent("Unrecognized object type: " + currentObjectType);
                    unhandledScriptingCommands = true;
                    break;
            }

            string namePrefix;
            if (string.IsNullOrWhiteSpace(schemaToUse) || schemaToUse.Equals("-") || schemaToUse.Equals("public"))
                namePrefix = string.Empty;
            else if (nameToUse.StartsWith(schemaToUse + "."))
                namePrefix = string.Empty;
            else
                namePrefix = schemaToUse + ".";

            targetScriptFile = namePrefix + nameToUse + ".sql";
        }

        private void ProcessPgDumpDataFile(FileSystemInfo pgDumpOutputFile)
        {
            var linesProcessed = 0;

            using (var reader = new StreamReader(new FileStream(pgDumpOutputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
            {
                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();
                    if (dataLine == null)
                        continue;

                    if (linesProcessed < 10)
                        Console.WriteLine(dataLine);

                    // ToDo: is anything required?

                    linesProcessed++;
                }
            }
        }

        private void ProcessPgDumpSchemaFile(string databaseName, FileInfo pgDumpOutputFile, out bool unhandledScriptingCommands)
        {

            unhandledScriptingCommands = false;

            if (pgDumpOutputFile.Directory == null)
            {
                OnErrorEvent("Could not determine the parent directory of " + pgDumpOutputFile.FullName);
                return;
            }

            var scriptInfoByObject = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);

            using (var reader = new StreamReader(new FileStream(pgDumpOutputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
            {
                var cachedLines = new List<string>();
                var currentObjectName = string.Empty;
                var currentObjectType = string.Empty;
                var currentObjectSchema = string.Empty;
                var currentObjectOwner = string.Empty;
                var previousTargetScriptFile = string.Empty;

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();
                    if (dataLine == null)
                        continue;

                    var match = mNameTypeSchemaMatcher.Match(dataLine);
                    try
                    {

                        if (!match.Success)
                        {
                            cachedLines.Add(dataLine);
                            continue;
                        }

                        ProcessCachedLines(databaseName, cachedLines,
                                           currentObjectName, currentObjectType, currentObjectSchema,
                                           previousTargetScriptFile,
                                           out var targetScriptFile,
                                           out unhandledScriptingCommands);

                        StoreCachedLinesForObject(scriptInfoByObject, cachedLines, targetScriptFile);
                        previousTargetScriptFile = string.Copy(targetScriptFile);

                        UpdateCachedObjectInfo(match, out currentObjectName, out currentObjectType, out currentObjectSchema, out currentObjectOwner);
                        cachedLines = new List<string> {
                            "--",
                            dataLine
                        };

                    }
                    catch (Exception ex)
                    {
                        OnWarningEvent("Error in ProcessPgDumpFile: " + ex.Message);
                    }

                }

                var outputDirectory = pgDumpOutputFile.Directory.FullName;
                WriteCachedLines(outputDirectory, scriptInfoByObject);

                if (mOptions.ScriptingOptions.ExportDBSchemasAndRoles)
                {
                    // Create files for schemas and roles
                    if (mAbortProcessing)
                    {
                        return;
                    }
                }

                if (mOptions.ScriptingOptions.ExportTables)
                {
                    // Create files for tables
                    if (mAbortProcessing)
                    {
                        return;
                    }
                }

                if (mOptions.ScriptingOptions.ExportViews ||
                    mOptions.ScriptingOptions.ExportUserDefinedFunctions ||
                    mOptions.ScriptingOptions.ExportStoredProcedures ||
                    mOptions.ScriptingOptions.ExportSynonyms ||
                    mOptions.ScriptingOptions.ExportUserDefinedDataTypes ||
                    mOptions.ScriptingOptions.ExportUserDefinedTypes)
                {
                    // Create files for views, functions, etc.
                    if (mAbortProcessing)
                    {
                        return;
                    }
                }
            }

        }

        /// <summary>
        /// Scripts out the objects on the current server
        /// </summary>
        /// <param name="databaseList">Database names to export></param>
        /// <param name="tableNamesForDataExport">Table names for which data should be exported</param>
        /// <returns>True if success, false if a problem</returns>
        public override bool ScriptServerAndDBObjects(List<string> databaseList, List<string> tableNamesForDataExport)
        {
            var validated = ValidateOptionsToScriptServerAndDBObjects(databaseList);
            if (!validated)
                return false;

            OnStatusEvent("Exporting schema to: " + PathUtils.CompactPathString(mOptions.OutputDirectoryPath));
            OnDebugEvent("Connecting to " + mOptions.ServerName);

            // Assure that we're connected to the postgres database
            // ScriptDBObjects calls GetServerDatabasesWork to get a list of databases on the server
            if (!ConnectToServer(POSTGRES_DATABASE))
            {
                return false;
            }

            if (!ValidServerConnection())
            {
                return false;
            }

            if (mOptions.ExportServerInfo)
            {
                var success = ScriptServerObjects();
                if (!success || mAbortProcessing)
                {
                    return false;
                }
            }

            if (databaseList != null && databaseList.Count > 0)
            {
                var success = ScriptDBObjects(databaseList, tableNamesForDataExport);
                if (!success || mAbortProcessing)
                {
                    return false;
                }
            }

            OnProgressComplete();

            return true;
        }

        private bool ScriptServerObjects()
        {
            var outputDirectoryPath = "??";

            DirectoryInfo outputDirectoryPathCurrentServer;

            try
            {
                // Construct the path to the output directory
                outputDirectoryPath = Path.Combine(mOptions.OutputDirectoryPath, mOptions.ServerOutputDirectoryNamePrefix + mOptions.ServerName);
                outputDirectoryPathCurrentServer = new DirectoryInfo(outputDirectoryPath);

                // Create the directory if it doesn't exist
                if (!outputDirectoryPathCurrentServer.Exists && !mOptions.PreviewExport)
                {
                    outputDirectoryPathCurrentServer.Create();
                }

            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.DatabaseConnectionError, "Error validating or creating directory " + outputDirectoryPath, ex);
                return false;
            }

            try
            {
                OnStatusEvent("Exporting Server objects to: " + PathUtils.CompactPathString(mOptions.OutputDirectoryPath));

                var outputFile = new FileInfo(Path.Combine(outputDirectoryPathCurrentServer.FullName, "ServerInfo.sql"));

                var existingData = outputFile.Exists ? outputFile.LastWriteTime : DateTime.MinValue;

                var serverInfoArgs = GetPgDumpServerInfoArgs(string.Empty);

                // pg_dumpall --schema-only --globals-only
                var cmdArgs = string.Format("{0} --schema-only --globals-ony --format=p --file={1}", serverInfoArgs, outputFile.FullName);
                var maxRuntimeSeconds = 60;

                var pgDumpAll = FindPgDumpAllExecutable();
                if (pgDumpAll == null)
                    return false;

                if (mOptions.PreviewExport)
                {
                    OnStatusEvent(string.Format("Preview running {0} {1}", pgDumpAll.FullName, cmdArgs));
                    return true;
                }

                var success = mProgramRunner.RunCommand(pgDumpAll.FullName, cmdArgs, outputDirectoryPathCurrentServer.FullName,
                                                       out var consoleOutput, out var errorOutput, maxRuntimeSeconds);

                if (!success)
                {
                    OnWarningEvent(string.Format("Error reported for {0}: {1}", pgDumpAll.Name, consoleOutput));
                    return false;
                }

                // Assure that the file was created
                outputFile.Refresh();

                if (outputFile.LastWriteTime > existingData)
                    return true;

                if (outputFile.Exists)
                    OnWarningEvent(string.Format("{0} did not replace {1}", pgDumpAll.Name, outputFile.FullName));
                else
                    OnWarningEvent(string.Format("{0} did not create {1}", pgDumpAll.Name, outputFile.FullName));

                return false;

            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.DatabaseConnectionError, "Error scripting objects for server " + mOptions.ServerName, ex);
                return false;
            }

        }

        private void StoreCachedLinesForObject(
            IDictionary<string, List<string>> scriptInfoByObject,
            List<string> cachedLines,
            string targetScriptFile)
        {
            if (cachedLines.Count == 0)
                return;

            var outputFileName = CleanNameForOS(targetScriptFile);

            // Remove the final "--" from cachedLines, plus any blank lines
            while (cachedLines.Count > 0 && string.IsNullOrWhiteSpace(cachedLines.Last()) || cachedLines.Last().Equals("--"))
            {
                cachedLines.RemoveAt(cachedLines.Count - 1);
            }

            if (scriptInfoByObject.TryGetValue(outputFileName, out var scriptInfo))
            {
                scriptInfo.Add(string.Empty);
                scriptInfo.AddRange(cachedLines);
            }
            else
            {
                scriptInfoByObject.Add(outputFileName, cachedLines);
            }

        }

        private void UpdateCachedObjectInfo(
            Match match,
            out string currentObjectName,
            out string currentObjectType,
            out string currentObjectSchema,
            out string currentObjectOwner)
        {
            currentObjectName = match.Groups["Name"].Value;
            currentObjectType = match.Groups["Type"].Value;
            currentObjectSchema = match.Groups["Schema"].Value;
            currentObjectOwner = match.Groups["Owner"].Value;
        }

        private bool ValidServerConnection()
        {
            return mConnectedToServer && mPgConnection != null &&
                   mPgConnection.State != ConnectionState.Broken &&
                   mPgConnection.State != ConnectionState.Closed;
        }

        private void WriteCachedLines(string outputDirectory, Dictionary<string, List<string>> scriptInfoByObject)
        {
            foreach (var item in scriptInfoByObject)
            {
                var outputFileName = item.Key;
                var cachedLines = item.Value;

                var outputFilePath = Path.Combine(outputDirectory, outputFileName);

                OnDebugEvent("Writing " + outputFilePath);

                using (var writer = new StreamWriter(new FileStream(outputFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
                {
                    foreach (var cachedLine in cachedLines)
                    {
                        writer.WriteLine(cachedLine);
                    }
                }

            }

        }


    }
}
