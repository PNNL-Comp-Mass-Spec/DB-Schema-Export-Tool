using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Npgsql;
using PRISM;

namespace DB_Schema_Export_Tool
{
    public sealed class DBSchemaExporterPostgreSQL : DBSchemaExporterBase
    {
        #region "Constants and Enums"

        public const int DEFAULT_PORT = 5432;

        public const string POSTGRES_DATABASE = "postgres";

        #endregion

        #region "Classwide Variables"

        /// <summary>
        /// Dictionary tracking tables by database
        /// Keys are database name, values are a dictionary where keys are table name info and values are row counts
        /// </summary>
        /// <remarks>Row counts will only be loaded if conditional compilation constant ENABLE_GUI is defined</remarks>
        private readonly Dictionary<string, Dictionary<TableDataExportInfo, long>> mCachedDatabaseTableInfo;

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

            mCachedDatabaseTableInfo = new Dictionary<string, Dictionary<TableDataExportInfo, long>>();

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
        /// <param name="tablesForDataExport">Table names that should be auto-selected</param>
        /// <param name="databaseNotFound">Output: true if the database does not exist on the server (or is inaccessible)</param>
        /// <returns>Dictionary where keys are table names and values are the maximum number of rows to export</returns>
        private Dictionary<TableDataExportInfo, long> AutoSelectTablesForDataExport(
            string databaseName,
            IReadOnlyList<TableDataExportInfo> tablesForDataExport,
            out bool databaseNotFound)
        {
            if (!mCachedDatabaseTableInfo.ContainsKey(databaseName))
            {
                CacheDatabaseTables(databaseName, out databaseNotFound);
                if (databaseNotFound)
                {
                    SetLocalError(DBSchemaExportErrorCodes.GeneralError,
                                  string.Format("Error in AutoSelectTablesForDataExport; database not found: " + databaseName));
                    return new Dictionary<TableDataExportInfo, long>();
                }
            }

            var tablesInDatabase = mCachedDatabaseTableInfo[databaseName];
            databaseNotFound = false;

            var tablesToExportData = AutoSelectTablesForDataExport(tablesInDatabase.Keys.ToList(), tablesForDataExport);
            return tablesToExportData;
        }

        private void CacheDatabaseTables(string databaseName, out bool databaseNotFound)
        {
#if ENABLE_GUI
            const bool INCLUDE_TABLE_ROW_COUNTS = true;
#else
            const bool INCLUDE_TABLE_ROW_COUNTS = false;
#endif
            const bool INCLUDE_SYSTEM_OBJECTS = false;
            const bool CLEAR_SCHEMA_OUTPUT_DIRS = false;

            var tablesInDatabase = GetPgServerDatabaseTables(databaseName, INCLUDE_TABLE_ROW_COUNTS,
                                                                 INCLUDE_SYSTEM_OBJECTS,
                                                                 CLEAR_SCHEMA_OUTPUT_DIRS,
                                                                 out databaseNotFound);

            if (mCachedDatabaseTableInfo.ContainsKey(databaseName))
            {
                mCachedDatabaseTableInfo[databaseName] = tablesInDatabase;
            }
            else
            {
                mCachedDatabaseTableInfo.Add(databaseName, tablesInDatabase);
            }

        }

        /// <summary>
        /// Connect to the server specified in mOptions
        /// </summary>
        /// <returns>True if successfully connected, false if a problem</returns>
        public override bool ConnectToServer()
        {
            var success = ConnectToServer(POSTGRES_DATABASE);
            return success;
        }

        /// <summary>
        /// Connect to the server specified in mOptions
        /// </summary>
        /// <param name="databaseName">PostgreSQL database to connect to</param>
        /// <returns>True if successfully connected, false if a problem</returns>
        public bool ConnectToServer(string databaseName)
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
        /// Also export data from tables in tablesForDataExport
        /// </summary>
        /// <param name="databaseName">Database name</param>
        /// <param name="tablesForDataExport">Table names that should be auto-selected</param>
        /// <param name="databaseNotFound">Output: true if the database does not exist on the server (or is inaccessible)</param>
        /// <returns>True if successful, false if an error</returns>
        protected override bool ExportDBObjectsAndTableData(
            string databaseName,
            IReadOnlyList<TableDataExportInfo> tablesForDataExport,
            out bool databaseNotFound)
        {
            var workingParams = new WorkingParams();

            // Keys are table names to export data
            // Values are the maximum number of rows to export
            Dictionary<TableDataExportInfo, long> tablesToExportData;
            databaseNotFound = false;

            OnDBExportStarting(databaseName);

            var isValid = ValidateOutputDirectoryForDatabaseExport(databaseName, workingParams);
            if (!isValid)
            {
                return false;
            }

            try
            {
                if (mOptions.ScriptingOptions.AutoSelectTablesForDataExport || mOptions.ExportAllData)
                {
                    tablesToExportData = AutoSelectTablesForDataExport(databaseName, tablesForDataExport, out databaseNotFound);
                }
                else
                {
                    tablesToExportData = new Dictionary<TableDataExportInfo, long>();
                    foreach (var item in tablesForDataExport)
                    {
                        tablesToExportData.Add(item, 0);
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

                // Export data from tables specified by tablesToExportData
                var dataSuccess = ExportDBTableData(databaseName, tablesToExportData, workingParams);

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

            if (mOptions.NoSchema)
                return true;

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

                if (unhandledScriptingCommands)
                {
                    OnWarningEvent("One or more unsupported scripting commands were encountered.  Inspect file " + pgDumpOutputFile.FullName);
                }
                else
                {
                    // Delete the _AllObjects_.sql file (since we no longer need it)
                    try
                    {
                        pgDumpOutputFile.Delete();
                    }
                    catch (Exception ex)
                    {
                        OnWarningEvent(string.Format("Unable to delete {0}: {1}", pgDumpOutputFile.FullName, ex.Message));
                    }
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
        /// <param name="tableInfo">Table info</param>
        /// <param name="maxRowsToExport">Maximum rows to export</param>
        /// <param name="workingParams">Working parameters</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>If the table does not exist, will still return true</remarks>
        protected override bool ExportDBTableData(
            string databaseName,
            TableDataExportInfo tableInfo,
            long maxRowsToExport,
            WorkingParams workingParams)
        {
            try
            {
                if (!mCachedDatabaseTableInfo.ContainsKey(databaseName))
                {
                    CacheDatabaseTables(databaseName, out var databaseNotFound);
                    if (databaseNotFound)
                    {
                        SetLocalError(DBSchemaExportErrorCodes.GeneralError,
                                      string.Format("Error in ExportDBTableData; database not found: " + databaseName));
                        return false;
                    }
                }

                var tablesInDatabase = mCachedDatabaseTableInfo[databaseName];
                var sourceTableNameWithSchema = string.Empty;

                var quotedTableName = PossiblyQuoteName(tableInfo.SourceTableName);

                var tableNamesToFind = new List<string>
                {
                    tableInfo.SourceTableName,
                    quotedTableName,
                    "public." + tableInfo.SourceTableName,
                    "public." + PossiblyQuoteName(tableInfo.SourceTableName)
                };

                foreach (var nameToFind in tableNamesToFind)
                {
                    foreach (var candidateTable in tablesInDatabase)
                    {
                        if (candidateTable.Key.SourceTableName.Equals(nameToFind))
                        {
                            sourceTableNameWithSchema = nameToFind;
                            break;
                        }
                    }
                }

                if (string.IsNullOrWhiteSpace(sourceTableNameWithSchema))
                {
                    // Table not found in this database
                    // Do not treat this as an error, so return true
                    return true;
                }

                var subTaskProgress = ComputeSubtaskProgress(workingParams.ProcessCount, workingParams.ProcessCountExpected);
                var percentComplete = ComputeIncrementalProgress(mPercentCompleteStart, mPercentCompleteEnd, subTaskProgress);

                OnProgressUpdate("Exporting data from " + tableInfo.SourceTableName, percentComplete);

                bool success;
                if (mOptions.PgDumpTableData)
                {
                    success = ExportDBTableDataUsingPgDump(databaseName, workingParams, tableInfo, sourceTableNameWithSchema);
                }
                else
                {
                    success = ExportDBTableDataUsingNpgsql(databaseName, workingParams, tableInfo, sourceTableNameWithSchema, maxRowsToExport);
                }

                return success;
            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.GeneralError, "Error in ExportDBTableData", ex);
                return false;
            }
        }

        private bool ExportDBTableDataUsingNpgsql(
            string databaseName,
            WorkingParams workingParams,
            TableDataExportInfo tableInfo,
            string sourceTableNameWithSchema,
            long maxRowsToExport)
        {
            if (!ConnectToServer(databaseName))
            {
                return false;
            }

            // Export the data from tableNameWithSchema, possibly limiting the number of rows to export
            var sql = "SELECT * FROM " + sourceTableNameWithSchema;
            if (maxRowsToExport > 0)
            {
                sql += " limit " + maxRowsToExport;
            }

            if (mOptions.PreviewExport)
            {
                OnStatusEvent(string.Format("Preview querying database {0} with {1}", databaseName, sql));
                return true;
            }

            var tableListCommand = new NpgsqlCommand(sql, mPgConnection);
            using (var reader = tableListCommand.ExecuteReader())
            {

                if (!reader.HasRows)
                    return true;

                var dataExportParams = new DataExportWorkingParams(false, "null")
                {
                    TargetTableNameWithSchema = GetTargetTableName(sourceTableNameWithSchema, tableInfo, out var targetTableName)
                };

                var headerRows = new List<string>();

                var header = COMMENT_START_TEXT + "Object:  Table " + dataExportParams.TargetTableNameWithSchema;

                if (mOptions.ScriptingOptions.IncludeTimestampInScriptFileHeader)
                {
                    header += "    " + COMMENT_SCRIPT_DATE_TEXT + GetTimeStamp();
                }

                header += COMMENT_END_TEXT;
                headerRows.Add(header);

                // See if any of the columns in the table is an identity column

                var columnSchema = reader.GetColumnSchema();
                foreach (var dbColumn in columnSchema)
                {
                    if (dbColumn.IsIdentity.HasValue && dbColumn.IsIdentity.Value)
                    {
                        dataExportParams.IdentityColumnFound = true;
                    }
                    else if (dbColumn.IsAutoIncrement.HasValue && dbColumn.IsAutoIncrement.Value)
                    {
                        dataExportParams.IdentityColumnFound = true;
                    }
                }

                var columnCount = reader.FieldCount;

                for (var columnIndex = 0; columnIndex < columnCount; columnIndex++)
                {
                    var currentColumnName = reader.GetName(columnIndex);
                    var currentColumnType = reader.GetFieldType(columnIndex);
                    dataExportParams.ColumnInfoByType.Add(new KeyValuePair<string, Type>(currentColumnName, currentColumnType));
                }

                const bool quoteWithSquareBrackets = false;

                ConvertDataTableColumnInfo(sourceTableNameWithSchema, quoteWithSquareBrackets, dataExportParams);

                var insertIntoLine = string.Empty;

                if (mOptions.ScriptingOptions.SaveDataAsInsertIntoStatements)
                {
                    // For tables with an identity column that uses "GENERATED BY DEFAULT", we can explicitly set the value of the identity field

                    // However, for tables with "GENERATED ALWAYS", we need to use "OVERRIDING SYSTEM VALUE"
                    //
                    // INSERT INTO UserDetail (UserDetailId, UserName, Password)
                    // OVERRIDING SYSTEM VALUE
                    //   VALUES(1,'admin', 'password');

                    insertIntoLine = string.Format("INSERT INTO {0} VALUES (", dataExportParams.TargetTableNameWithSchema);

                    headerRows.Add(COMMENT_START_TEXT + "Columns: " + dataExportParams.HeaderRowValues + COMMENT_END_TEXT);

                    dataExportParams.ColSepChar = ',';
                }
                else
                {
                    // Export data as a tab-delimited table
                    headerRows.Add(dataExportParams.HeaderRowValues.ToString());
                    dataExportParams.ColSepChar = '\t';
                }

                var outFilePath = GetFileNameForTableDataExport(targetTableName, dataExportParams.TargetTableNameWithSchema, workingParams);
                OnDebugEvent("Writing table data to " + outFilePath);

                if (mOptions.ScriptPgLoadCommands)
                {
                    workingParams.AddDataLoadScriptFile(Path.GetFileName(outFilePath));
                }

                using (var writer = new StreamWriter(new FileStream(outFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
                {
                    foreach (var headerRow in headerRows)
                    {
                        writer.WriteLine(headerRow);
                    }

                    ExportDBTableDataUsingNpgsql(writer, reader, dataExportParams, insertIntoLine);

                    if (dataExportParams.IdentityColumnFound && mOptions.ScriptingOptions.SaveDataAsInsertIntoStatements)
                    {
                        writer.WriteLine("-- If a table has an identity value, after inserting data with explicit identities,");
                        writer.WriteLine("-- the sequence will need to be sync'd up with the table");
                        writer.WriteLine();
                        writer.WriteLine("-- Option 1, for columns that use a serial for the identity field");
                        writer.WriteLine("-- select setval(pg_get_serial_sequence('{0}', 'my_serial_column'),", dataExportParams.TargetTableNameWithSchema);
                        writer.WriteLine("--               (select max(my_serial_column) from {0}) );", dataExportParams.TargetTableNameWithSchema);
                        writer.WriteLine();
                        writer.WriteLine("-- Option 2, for columns that get their default value from a sequence");
                        writer.WriteLine("-- select setval('my_sequence_name', (select max(my_serial_column) from {0});", dataExportParams.TargetTableNameWithSchema);
                    }
                }

            }

            return true;
        }

        private void ExportDBTableDataUsingNpgsql(
            TextWriter writer,
            IDataReader reader,
            DataExportWorkingParams dataExportParams,
            string insertIntoLine)
        {
            var columnCount = reader.FieldCount;

            var delimitedRowValues = new StringBuilder();

            var columnValues = new object[columnCount];

            while (reader.Read())
            {
                delimitedRowValues.Clear();
                if (mOptions.ScriptingOptions.SaveDataAsInsertIntoStatements)
                {
                    delimitedRowValues.Append(insertIntoLine);
                }

                for (var columnIndex = 0; columnIndex < columnCount; columnIndex++)
                {
                    columnValues[columnIndex] = reader.GetValue(columnIndex);
                }

                ExportDBTableDataRow(writer, dataExportParams, delimitedRowValues, columnCount, columnValues);
            }

        }

        private bool ExportDBTableDataUsingPgDump(
            string databaseName,
            WorkingParams workingParams,
            TableDataExportInfo tableInfo,
            string sourceTableNameWithSchema)
        {
            var targetTableNameWithSchema = GetTargetTableName(sourceTableNameWithSchema, tableInfo, out var targetTableName);

            var outFilePath = GetFileNameForTableDataExport(targetTableName, targetTableNameWithSchema, workingParams);
            var tableDataOutputFile = new FileInfo(outFilePath);

            var existingData = tableDataOutputFile.Exists ? tableDataOutputFile.LastWriteTime : DateTime.MinValue;

            var serverInfoArgs = GetPgDumpServerInfoArgs(string.Empty);

            // pg_dump -h host -p port -U user -W PasswordIfDefined -d database --data-only --table=TableName --format=p --file=OutFilePath
            var cmdArgs = string.Format("{0} -d {1} --data-only --table={2} --format=p --file={3}",
                                        serverInfoArgs, databaseName, sourceTableNameWithSchema, tableDataOutputFile.FullName);
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
                                                    out var consoleOutput, out _, maxRuntimeSeconds);

            if (!success)
            {
                OnWarningEvent(string.Format("Error reported for {0}: {1}", pgDump.Name, consoleOutput));
                return false;
            }

            if (consoleOutput.Contains("no matching tables were found"))
            {
                // Table not found; this method should not have been called
                return false;
            }

            // Assure that the file was created
            tableDataOutputFile.Refresh();

            if (tableDataOutputFile.LastWriteTime > existingData)
            {

                // Skip: Parse the pgDump output file to clean it up
                // ProcessPgDumpDataFile(tableDataOutputFile);
                return true;
            }

            if (tableDataOutputFile.Exists)
                OnWarningEvent(string.Format("{0} did not replace {1}", pgDump.Name, tableDataOutputFile.FullName));
            else
                OnWarningEvent(string.Format("{0} did not create {1}", pgDump.Name, tableDataOutputFile.FullName));

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
        public override Dictionary<TableDataExportInfo, long> GetDatabaseTables(string databaseName, bool includeTableRowCounts, bool includeSystemObjects)
        {
            return GetPgServerDatabaseTables(databaseName, includeTableRowCounts, includeSystemObjects, true, out _);
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
        /// Lookup the tables in the specified database, optionally also determining table row counts
        /// </summary>
        /// <param name="databaseName">Database to query</param>
        /// <param name="includeTableRowCounts">When true, then determines the row count in each table</param>
        /// <param name="includeSystemObjects">When true, then also returns system var tables</param>
        /// <param name="clearSchemaOutputDirectories">When true, remove all items in dictionary SchemaOutputDirectories</param>
        /// <param name="databaseNotFound">Output: true if the database does not exist on the server (or is inaccessible)</param>
        /// <returns>
        /// Dictionary where keys are instances of TableDataExportInfo and values are row counts (if includeTableRowCounts = true)
        /// Schema and table names will be surrounded by double quotes if they contain a space or any other non-alphanumeric character
        /// </returns>
        public Dictionary<TableDataExportInfo, long> GetPgServerDatabaseTables(
            string databaseName,
            bool includeTableRowCounts,
            bool includeSystemObjects,
            bool clearSchemaOutputDirectories,
            out bool databaseNotFound)
        {
            // Keys are table names (with schema); values are row counts
            var databaseTableInfo = new Dictionary<TableDataExportInfo, long>();
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
                return new Dictionary<TableDataExportInfo, long>();
            }

            try
            {
                if (string.IsNullOrWhiteSpace(databaseName))
                {
                    OnWarningEvent("Empty database name sent to GetPgServerDatabaseTables");
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

                            var tableNameWithSchema = PossiblyQuoteName(schemaName) + "." +
                                                      PossiblyQuoteName(tableName);

                            databaseTableInfo.Add(new TableDataExportInfo(tableNameWithSchema), 0);

                        }
                    }
                }

                if (includeTableRowCounts)
                {
                    var databaseTables = databaseTableInfo.Keys.ToList();
                    var index = 0;

                    foreach (var item in databaseTables)
                    {

                        // ReSharper disable StringLiteralTypo
                        var rowCountSql = string.Format("SELECT relname, reltuples::bigint as ApproximateRowCount " +
                                                        "FROM pg_class " +
                                                        "WHERE oid = '{0}'::regclass", item.SourceTableName);
                        // ReSharper restore StringLiteralTypo

                        var rowCountCmd = new NpgsqlCommand(rowCountSql, mPgConnection);
                        using (var reader = rowCountCmd.ExecuteReader())
                        {
                            if (reader.HasRows && reader.Read())
                            {
                                // var tableName = reader.GetString(0);
                                var approximateRowCount = reader.GetInt64(1);

                                databaseTableInfo[item] = approximateRowCount;
                            }
                        }

                        index++;
                        var subTaskProgress = ComputeSubtaskProgress(index, databaseTables.Count);
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
                    foreach (var item in databaseTables)
                    {
                        if (databaseTableInfo[item] > 0)
                            continue;

                        var rowCountSql = string.Format("SELECT count(*) FROM {0} ", item.SourceTableName);

                        var rowCountCmd = new NpgsqlCommand(rowCountSql, mPgConnection);
                        using (var reader = rowCountCmd.ExecuteReader())
                        {
                            if (reader.HasRows && reader.Read())
                            {
                                var rowCount = reader.GetInt64(0);
                                databaseTableInfo[item] = rowCount;
                            }
                        }

                        index++;
                        var subTaskProgress = ComputeSubtaskProgress(index, databaseTables.Count);
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

                return new Dictionary<TableDataExportInfo, long>();
            }
        }

        private string GetServerConnectionInfo()
        {
            return string.Format("{0}, port {1}, user {2}", mOptions.ServerName, mOptions.PgPort, mOptions.DBUser);
        }

        /// <summary>
        /// Retrieve a list of database names on the server defined in mOptions
        /// </summary>
        /// <returns></returns>
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

        /// <summary>
        /// Get the list of databases from the current server
        /// </summary>
        /// <returns></returns>
        /// <remarks>Assumes we already have an active server connection</remarks>
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

        private string GetTargetTableName(string sourceTableNameWithSchema, TableDataExportInfo tableInfo, out string targetTableName)
        {
            const bool quoteWithSquareBrackets = false;
            const bool alwaysQuoteNames = false;

            var targetTableNameWithSchema = GetTargetTableName(sourceTableNameWithSchema, tableInfo,
                                                               quoteWithSquareBrackets, alwaysQuoteNames,
                                                               out targetTableName);
            return targetTableNameWithSchema;
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
        private string PossiblyQuoteName(string objectName)
        {
            return PossiblyQuoteName(objectName, false);
        }

        private void ProcessCachedLines(
            string databaseName,
            IList<string> cachedLines,
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
                    else if (aclFunctionMatch.Success)
                    {
                        nameToUse = aclFunctionMatch.Groups["FunctionName"].Value;
                    }
                    else if (aclSchemaMatch.Success)
                    {
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
                case "DEFAULT":
                    // Parse out the target table name from the Alter Table DDL

                    // The regex will match lines like this:
                    // ALTER TABLE mc.t_event_log
                    // ALTER TABLE ONLY mc.t_event_log

                    var alterTableMatcher = new Regex(string.Format(@"ALTER TABLE.+ {0}\.(?<TargetTable>.+)", schemaToUse),
                                                      RegexOptions.Compiled | RegexOptions.IgnoreCase);

                    var alterTableAlterColumnMatcher = new Regex(string.Format(@"ALTER TABLE.+ {0}\.(?<TargetTable>.+) ALTER COLUMN", schemaToUse),
                                                                 RegexOptions.Compiled | RegexOptions.IgnoreCase);

                    var alterTableMatched = false;
                    foreach (var cachedLine in cachedLines)
                    {
                        var match = alterTableAlterColumnMatcher.Match(cachedLine);
                        if (!match.Success)
                            continue;

                        nameToUse = match.Groups["TargetTable"].Value;
                        alterTableMatched = true;
                        break;
                    }

                    if (!alterTableMatched)
                    {
                        foreach (var cachedLine in cachedLines)
                        {
                            var match = alterTableMatcher.Match(cachedLine);
                            if (!match.Success)
                                continue;

                            nameToUse = match.Groups["TargetTable"].Value;
                            alterTableMatched = true;
                            break;
                        }
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

                case "SEQUENCE OWNED BY":
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

        [Obsolete("Unused")]
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

                    linesProcessed++;
                }
            }
        }

        /// <summary>
        /// Process the database dump file created by pgDump
        /// </summary>
        /// <param name="databaseName"></param>
        /// <param name="pgDumpOutputFile"></param>
        /// <param name="unhandledScriptingCommands"></param>
        /// <remarks>
        /// This file will have DDL for schemas, roles, permissions, extensions, functions, tables, and views
        /// </remarks>
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
                            if (dataLine.Equals("SET default_tablespace = '';") ||
                                dataLine.Equals("SET default_table_access_method = heap;"))
                            {
                                // Store these lines in the DatabaseInfo file
                                var databaseInfoScriptFile = string.Format("DatabaseInfo_{0}.sql", databaseName);

                                var setDefaultLine = new List<string> {
                                    dataLine
                                };

                                StoreCachedLinesForObject(scriptInfoByObject, setDefaultLine, databaseInfoScriptFile);
                                continue;
                            }

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

                        UpdateCachedObjectInfo(match, out currentObjectName, out currentObjectType, out currentObjectSchema, out _);
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
            }

        }

        /// <summary>
        /// Export PostgreSQL Server settings
        /// </summary>
        /// <returns></returns>
        protected override bool ScriptServerObjects()
        {

            try
            {
                var serverInfoOutputDirectory = GetServerInfoOutputDirectory(mOptions.ServerName);
                if (serverInfoOutputDirectory == null)
                {
                    return false;
                }

                OnStatusEvent("Exporting Server objects to: " + PathUtils.CompactPathString(serverInfoOutputDirectory.FullName));

                var outputFile = new FileInfo(Path.Combine(serverInfoOutputDirectory.FullName, "ServerInfo.sql"));

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

                var success = mProgramRunner.RunCommand(pgDumpAll.FullName, cmdArgs, serverInfoOutputDirectory.FullName,
                                                       out var consoleOutput, out _, maxRuntimeSeconds);

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
                SetLocalError(DBSchemaExportErrorCodes.GeneralError, "Error scripting objects for server " + mOptions.ServerName, ex);
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

        /// <summary>
        /// Return true if we have a valid server connection
        /// </summary>
        /// <returns></returns>
        protected override bool ValidServerConnection()
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
