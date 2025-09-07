using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Npgsql;
using PRISM;
using PRISMDatabaseUtils;
using TableNameMapContainer;

namespace DB_Schema_Export_Tool
{
    /// <summary>
    /// PostgreSQL schema and data exporter
    /// </summary>
    public sealed class DBSchemaExporterPostgreSQL : DBSchemaExporterBase
    {
        // ReSharper disable CommentTypo

        // Ignore Spelling: bigint, datallowconn, datistemplate, datname, dumpall, hostname,
        // Ignore Spelling: mc, Npgsql, oid, pgpass, pgsql, Postgre, postgres, PostgreSQL
        // Ignore Spelling: regclass, relname, reltuples, schemaname, schemas, setval
        // Ignore Spelling: tablename, tableowner, tablespace, tcp, udf, username, usr

        // ReSharper restore CommentTypo

        /// <summary>
        /// Default server port
        /// </summary>
        public const int DEFAULT_PORT = 5432;

        /// <summary>
        /// Maximum length of object names in PostgreSQL
        /// </summary>
        public const int MAX_OBJECT_NAME_LENGTH = 63;

        /// <summary>
        /// postgres database
        /// </summary>
        public const string POSTGRES_DATABASE = "postgres";

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

        /// <summary>
        /// Use this to find text
        /// COLUMN t_log_entries.entered_by
        /// </summary>
        private readonly Regex mAclMatcherColumn = new(
            @"COLUMN (?<TableOrViewName>.+)\.(?<ColumnName>.+)",
            RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Use this to find text
        /// FUNCTION get_stat_activity(
        /// PROCEDURE post_log_entry(
        /// </summary>
        private readonly Regex mAclMatcherFunctionOrProcedure = new(
            "(FUNCTION|PROCEDURE) (?<ObjectName>[^(]+)",
            RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Use this to find text
        /// SCHEMA mc
        /// </summary>
        private readonly Regex mAclMatcherSchema = new(
            "SCHEMA (?<SchemaName>.+)",
            RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Use this to find text
        /// TABLE t_event_log
        /// </summary>
        private readonly Regex mAclMatcherTable = new(
            "TABLE (?<TableName>.+)",
            RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Use this to find text
        /// get_stat_activity()
        /// </summary>
        private readonly Regex mFunctionOrProcedureNameMatcher = new(
            "^(?<Name>[^(]+)",
            RegexOptions.Compiled);

        /// <summary>
        /// Use this to find lines that match
        /// -- Name: t_param_value; Type: TABLE; Schema: mc; Owner: d3l243
        /// -- Name: v_manager_type_report; Type: VIEW; Schema: mc; Owner: d3l243
        /// </summary>
        private readonly Regex mNameTypeSchemaMatcher = new(
            "^-- Name: (?<Name>.+); Type: (?<Type>.+); Schema: (?<Schema>.+); Owner: ?(?<Owner>.*)",
            RegexOptions.Compiled);

        /// <summary>
        /// Use this to find text
        /// FUNCTION get_stat_activity()
        /// </summary>
        private readonly Regex mNameTypeTargetMatcher = new(
            "(?<ObjectType>[a-z]+) (?<ObjectName>[^(]+)",
            RegexOptions.IgnoreCase | RegexOptions.Compiled);

        /// <summary>
        /// Use this to parse out the table and column name from column comment lines, e.g. t_analysis_job.param_file_name
        /// This RegEx assumes the schema name has already been removed
        /// </summary>
        private readonly Regex mTableColumnMatcher = new(
            @"(?<TableName>[^.]+)\.(?<ColumnName>[^ ]+)",
            RegexOptions.IgnoreCase | RegexOptions.Compiled);

        /// <summary>
        /// Use this to parse out the schema, table, and column name from column comment lines, e.g.
        /// timetable.chain.client_name
        /// </summary>
        private readonly Regex mTableColumnMatcherWithSchema = new(
            @"(?<SchemaName>[^.]+)\.(?<TableName>[^.]+)\.(?<ColumnName>[^ ]+)",
            RegexOptions.IgnoreCase | RegexOptions.Compiled);

        // ReSharper disable once GrammarMistakeInComment

        /// <summary>
        /// Use this to parse out the schema name and table name, or just the table name if there is no schema
        /// </summary>
        /// <remarks>
        /// Example matches:
        ///   T_Separation_Group
        ///   timetable.parameter
        /// </remarks>
        private readonly Regex mTableNameAndSchemaMatcher = new(
            @"^((?<SchemaName>[^.]+)\.)?(?<TableName>.+)$",
            RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Use this to find text
        /// t_users t_users_trigger_update_persisted
        /// </summary>
        private readonly Regex mTriggerTargetTableMatcher = new(
            "[^ ]+",
            RegexOptions.Compiled);

        /// <summary>
        /// This is optionally used to sort table data exported using PgDump
        /// </summary>
        private PgDumpTableDataSorter PgDumpDataSorter { get; }

        private readonly ProgramRunner mProgramRunner;

        private NpgsqlConnection mPgConnection;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="options">Options</param>
        public DBSchemaExporterPostgreSQL(SchemaExportOptions options) : base(options)
        {
            mProgramRunner = new ProgramRunner();
            RegisterEvents(mProgramRunner);

            mCachedDatabaseTableInfo = new Dictionary<string, Dictionary<TableDataExportInfo, long>>();

            mCachedExecutables = new Dictionary<string, FileInfo>();

            PgDumpDataSorter = new PgDumpTableDataSorter(mOptions.KeepPgDumpFile);
            RegisterEvents(PgDumpDataSorter);
        }

        /// <summary>
        /// Look for Name/Type lines in cachedLines, e.g. Name: function_name(argument int); Type: FUNCTION; Schema: public; Owner: username
        /// If there is more than one Name/Type line, add a comment for each overload
        /// </summary>
        /// <param name="cachedLines">Cached text lines</param>
        private void AddCommentsIfOverloaded(ICollection<string> cachedLines)
        {
            // Keys in this dictionary are of the form type_name
            // Values are the occurrence count of each key
            var nameTypeCounts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

            foreach (var cachedLine in cachedLines)
            {
                var match = mNameTypeSchemaMatcher.Match(cachedLine);

                if (!match.Success)
                    continue;

                var objectType = match.Groups["Type"].Value;

                if (!objectType.Equals("FUNCTION") &&
                    !objectType.Equals("PROCEDURE"))
                {
                    continue;
                }

                var typeAndName = GetObjectTypeNameCode(match);

                if (nameTypeCounts.TryGetValue(typeAndName, out var matchCount))
                {
                    nameTypeCounts[typeAndName] = matchCount + 1;
                }
                else
                {
                    nameTypeCounts.Add(typeAndName, 1);
                }
            }

            // This tracks the list of overloaded items in cachedLines
            // Keys are of the form type_name
            // Values will be increment below as each overload is encountered
            var overloadList = new Dictionary<string, int>();

            foreach (var item in nameTypeCounts)
            {
                if (item.Value > 1)
                {
                    overloadList.Add(item.Key, 0);
                }
            }

            if (overloadList.Count == 0)
            {
                return;
            }

            var updatedLines = new List<string>();
            var addCommentAfterNextLine = false;
            var overloadCountToUse = 1;

            foreach (var cachedLine in cachedLines)
            {
                updatedLines.Add(cachedLine);

                var match = mNameTypeSchemaMatcher.Match(cachedLine);

                if (!match.Success)
                {
                    if (addCommentAfterNextLine)
                    {
                        updatedLines.Add(string.Format("-- Overload {0}", overloadCountToUse));
                        addCommentAfterNextLine = false;
                    }

                    continue;
                }

                var typeAndName = GetObjectTypeNameCode(match);

                if (!overloadList.TryGetValue(typeAndName, out var matchCount))
                {
                    continue;
                }

                // The next line should be "--"
                // Add a comment after the next line, with the overload number
                addCommentAfterNextLine = true;

                overloadCountToUse = matchCount + 1;
                overloadList[typeAndName] = overloadCountToUse;
            }

            cachedLines.Clear();

            foreach (var updatedLine in updatedLines)
            {
                cachedLines.Add(updatedLine);
            }
        }

        /// <summary>
        /// Determines the table names for which data will be exported
        /// </summary>
        /// <param name="databaseName">Database name</param>
        /// <param name="tablesForDataExport">Table names that should be auto-selected</param>
        /// <param name="databaseNotFound">Output: true if the database does not exist on the server (or is inaccessible)</param>
        /// <returns>Dictionary where keys are information on tables to export and values are the maximum number of rows to export</returns>
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
                    SetLocalError(
                        DBSchemaExportErrorCodes.GeneralError,
                        string.Format("Error in AutoSelectTablesForDataExport; database not found: " + databaseName));

                    return new Dictionary<TableDataExportInfo, long>();
                }
            }

            var tablesInDatabase = mCachedDatabaseTableInfo[databaseName];
            databaseNotFound = false;

            var tableText = tablesInDatabase.Count == 1 ? "table" : "tables";
            ShowTrace(string.Format(
                "Found {0} {1} in database {2}",
                tablesInDatabase.Count, tableText, databaseName));

            return AutoSelectTablesForDataExport(databaseName, tablesInDatabase.Keys.ToList(), tablesForDataExport);
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

            // Add/update the dictionary
            mCachedDatabaseTableInfo[databaseName] = GetPgServerDatabaseTables(
                databaseName,
                INCLUDE_TABLE_ROW_COUNTS,
                INCLUDE_SYSTEM_OBJECTS,
                CLEAR_SCHEMA_OUTPUT_DIRS,
                out databaseNotFound);
        }

        /// <summary>
        /// Connect to the server specified in mOptions
        /// </summary>
        /// <returns>True if successfully connected, false if a problem</returns>
        public override bool ConnectToServer()
        {
            return ConnectToServer(POSTGRES_DATABASE);
        }

        /// <summary>
        /// Connect to the server specified in mOptions
        /// </summary>
        /// <param name="databaseName">PostgreSQL database to connect to</param>
        /// <returns>True if successfully connected, false if a problem</returns>
        public bool ConnectToServer(string databaseName)
        {
            return ConnectToPgServer(databaseName);
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

                    if (string.Equals(mPgConnection.DataSource, expectedNameAndPort, StringComparison.OrdinalIgnoreCase) &&
                        mCurrentServerInfo.DatabaseName.Equals(databaseName) &&
                        mCurrentServerInfo.UserName.Equals(mOptions.DBUser))
                    {
                        // Already connected; no need to re-connect
                        return true;
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

                if (!connected && ErrorCode == DBSchemaExportErrorCodes.NoError)
                {
                    SetLocalError(DBSchemaExportErrorCodes.DatabaseConnectionError, "Error logging into server " + GetServerConnectionInfo());
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
        /// <param name="tableDataExportOrder">List of table names that defines the order that table data should be exported</param>
        /// <param name="databaseNotFound">Output: true if the database does not exist on the server (or is inaccessible)</param>
        /// <param name="workingParams">Working parameters</param>
        /// <returns>True if successful, false if an error</returns>
        protected override bool ExportDBObjectsAndTableData(
            string databaseName,
            IReadOnlyList<TableDataExportInfo> tablesForDataExport,
            IReadOnlyList<string> tableDataExportOrder,
            out bool databaseNotFound,
            out WorkingParams workingParams)
        {
            workingParams = new WorkingParams();

            // Keys are information on tables to export
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
                if (mOptions.DisableDataExport)
                {
                    tablesToExportData = new Dictionary<TableDataExportInfo, long>();
                }
                else if (mOptions.ScriptingOptions.AutoSelectTablesForDataExport || mOptions.ExportAllData)
                {
                    tablesToExportData = AutoSelectTablesForDataExport(databaseName, tablesForDataExport, out databaseNotFound);
                }
                else
                {
                    tablesToExportData = new Dictionary<TableDataExportInfo, long>();

                    foreach (var item in tablesForDataExport)
                    {
                        if (SkipTableForDataExport(item))
                        {
                            ShowTrace("Skipping data export from table " + item.SourceTableName);
                            continue;
                        }

                        tablesToExportData.Add(item, 0);
                    }
                }
            }
            catch (Exception ex)
            {
                SetLocalError(
                    DBSchemaExportErrorCodes.GeneralError,
                    "Error auto selecting table names for data export from database " + databaseName, ex);

                databaseNotFound = false;
                return false;
            }

            try
            {
                if (!string.IsNullOrWhiteSpace(mOptions.PgDumpTableDataSortOrderFile))
                {
                    var fileLoaded = PgDumpDataSorter.LoadPgDumpTableDataSortOrderFile(mOptions.PgDumpTableDataSortOrderFile);

                    if (!fileLoaded)
                    {
                        SetLocalError(
                            DBSchemaExportErrorCodes.GeneralError,
                            "Error loading the pgDump table data sort order file: LoadPgDumpTableDataSortOrderFile returned false");

                        return false;
                    }
                }
            }
            catch (Exception ex)
            {
                SetLocalError(
                    DBSchemaExportErrorCodes.GeneralError,
                    "Error loading the pgDump table data sort order file", ex);

                databaseNotFound = false;
                return false;
            }

            try
            {
                // Export the database schema
                var success = ExportDBObjectsWork(databaseName, workingParams, out databaseNotFound);

                // Export data from tables specified by tablesToExportData
                var dataSuccess = ExportDBTableData(databaseName, tablesToExportData, tableDataExportOrder, workingParams);

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
        /// <remarks>
        /// Do not include a Try block in this Function; let the calling function handle errors
        /// </remarks>
        /// <param name="databaseName">Database name</param>
        /// <param name="workingParams">Working parameters</param>
        /// <param name="databaseNotFound">Output: true if the database does not exist on the server (or is inaccessible)</param>
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

            const int maxRuntimeSeconds = 180;

            var pgDump = FindPgDumpExecutable();

            if (pgDump == null)
            {
                return false;
            }

            if (mOptions.PreviewExport)
            {
                OnStatusEvent("Preview running {0} {1}", pgDump.FullName, cmdArgs);
                return true;
            }

            var success = mProgramRunner.RunCommand(
                pgDump.FullName, cmdArgs, workingParams.OutputDirectory.FullName,
                      out var consoleOutput, out var errorOutput, maxRuntimeSeconds);

            if (!success)
            {
                OnWarningEvent("Error reported for {0}: {1}", pgDump.Name, consoleOutput);

                var matcher = new Regex("database [^ ]+ does not exist", RegexOptions.Compiled | RegexOptions.IgnoreCase);

                databaseNotFound = matcher.IsMatch(consoleOutput) || matcher.IsMatch(errorOutput);
                return false;
            }

            // Assure that the file was created
            pgDumpOutputFile.Refresh();

            if (pgDumpOutputFile.LastWriteTime > existingData)
            {
                // Parse the pgDump output file and create a separate file for each object
                // Overloaded functions will be stored in the same file, with comments indicating the overload number

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
                        if (!mOptions.KeepPgDumpFile)
                        {
                            pgDumpOutputFile.Delete();
                        }
                    }
                    catch (Exception ex)
                    {
                        OnWarningEvent("Unable to delete {0}: {1}", pgDumpOutputFile.FullName, ex.Message);
                    }
                }

                return true;
            }

            if (pgDumpOutputFile.Exists)
                OnWarningEvent("{0} did not replace {1}", pgDump.Name, pgDumpOutputFile.FullName);
            else
                OnWarningEvent("{0} did not create {1}", pgDump.Name, pgDumpOutputFile.FullName);

            return false;
        }

        /// <summary>
        /// Export data from the specified table (if it exists)
        /// </summary>
        /// <remarks>If the table does not exist, will still return true</remarks>
        /// <param name="databaseName">Database name</param>
        /// <param name="tableInfo">Table info</param>
        /// <param name="maxRowsToExport">Maximum rows to export</param>
        /// <param name="workingParams">Working parameters</param>
        /// <returns>True if success, false if an error</returns>
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
                        SetLocalError(
                            DBSchemaExportErrorCodes.GeneralError,
                            string.Format("Error in DBSchemaExporterPostgreSQL.ExportDBTableData; database not found: " + databaseName));

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

                var tableSchemaNameMatch = mTableNameAndSchemaMatcher.Match(tableInfo.SourceTableName);

                if (tableSchemaNameMatch.Success)
                {
                    string quotedName;

                    // Possibly quote the schema name (if present) and the table name in tableInfo.SourceTableName

                    if (tableSchemaNameMatch.Groups["SchemaName"].Value.Length > 0)
                    {
                        quotedName = string.Format("{0}.{1}",
                            PossiblyQuoteName(tableSchemaNameMatch.Groups["SchemaName"].Value),
                            PossiblyQuoteName(tableSchemaNameMatch.Groups["TableName"].Value));
                    }
                    else
                    {
                        quotedName = PossiblyQuoteName(tableSchemaNameMatch.Groups["TableName"].Value);
                    }

                    if (!quotedName.Equals(tableInfo.SourceTableName))
                        tableNamesToFind.Add(quotedName);
                }

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
                SetLocalError(DBSchemaExportErrorCodes.GeneralError, "Error in DBSchemaExporterPostgreSQL.ExportDBTableData", ex);
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
            var sqlGetTableDataSelectAll = GetDataExportSql(sourceTableNameWithSchema, tableInfo, maxRowsToExport);

            var sqlFirstRow = "SELECT * FROM " + sourceTableNameWithSchema + " limit 1";

            var sqlGeneratedStoredColumns = string.Format(
                "SELECT attname AS column_name FROM pg_attribute WHERE attrelid = '{0}'::regclass::oid AND attnum > 0 AND attgenerated = 's'",
                sourceTableNameWithSchema);

            var sqlIdentityColumn = string.Format(
                "SELECT attname AS column_name, attnum AS column_position FROM pg_attribute WHERE attrelid = '{0}'::regclass::oid AND attnum > 0 AND attidentity = 'a'",
                sourceTableNameWithSchema);

            var sourceTableSchema = GetSchemaName(sourceTableNameWithSchema, out var sourceTableName);

            string xidTableQuerySchemaAndTable;

            if (string.IsNullOrWhiteSpace(sourceTableSchema))
            {
                xidTableQuerySchemaAndTable = string.Format("table_name = '{0}'",
                    tableInfo.SourceTableName);
            }
            else
            {
                xidTableQuerySchemaAndTable = string.Format("table_schema = '{0}' AND table_name = '{1}'",
                    sourceTableSchema, sourceTableName);
            }

            var sqlXidColumns = string.Format(
                "SELECT column_name FROM information_schema.columns WHERE {0} AND data_type = 'xid'",
                xidTableQuerySchemaAndTable);

            if (mOptions.PreviewExport)
            {
                OnStatusEvent("Preview querying database {0} with {1}", databaseName, sqlGetTableDataSelectAll);
                return true;
            }

            var pgInsertEnabled = mOptions.PgInsertUpdateOnConflict;

            var dataExportParams = new DataExportWorkingParams(pgInsertEnabled, "null")
            {
                SourceTableNameWithSchema = sourceTableNameWithSchema,
            };

            // Get the target table name
            // If the target schema is "dbo", "public", or an empty string, dataExportParams.TargetTableNameWithSchema will not include the schema
            dataExportParams.TargetTableNameWithSchema = GetTargetTableName(dataExportParams, tableInfo);

            if (string.IsNullOrWhiteSpace(dataExportParams.TargetTableNameWithSchema))
            {
                // Skip this table
                OnStatusEvent("Could not determine the target table name for table {0} in database {1}; skipping data export", dataExportParams.SourceTableNameWithSchema, databaseName);
                return false;
            }

            const bool quoteWithSquareBrackets = false;

            // Get the target table name to use when exporting data; schema name will be included if not "dbo" or "public"
            dataExportParams.QuotedTargetTableNameWithSchema = GetQuotedTargetTableName(dataExportParams, tableInfo, quoteWithSquareBrackets);

            // Store the column names and data types in dataExportParams.ColumnInfoByType
            // Also look for identity columns using .IsIdentity, though this is not reliable (it is always false, as of September 2025)

            var firstRowCommand = new NpgsqlCommand(sqlFirstRow, mPgConnection);
            var identityColumnIndex = -1;

            var quotedColumnNames = new List<string>();
            var skippedColumnCount = 0;

            using (var firstRowReader = firstRowCommand.ExecuteReader())
            {
                if (!firstRowReader.HasRows)
                {
                    OnStatusEvent("No data found for table {0} in database {1}; skipping data export", sourceTableNameWithSchema, databaseName);
                    return true;
                }

                var columnCount = firstRowReader.FieldCount;

                for (var columnIndex = 0; columnIndex < columnCount; columnIndex++)
                {
                    var currentColumnName = firstRowReader.GetName(columnIndex);

                    try
                    {
                        var currentColumnType = firstRowReader.GetFieldType(columnIndex);
                        dataExportParams.ColumnInfoByType.Add(new KeyValuePair<string, Type>(currentColumnName, currentColumnType));

                        quotedColumnNames.Add(PossiblyQuoteName(currentColumnName));
                    }
                    catch (InvalidCastException)
                    {
                        OnStatusEvent("Skipping column {0} in table {1} in database {2} since it is an unsupported data type (likely User-Defined)",
                            currentColumnName, sourceTableNameWithSchema, databaseName);

                        skippedColumnCount++;
                    }
                }

                var index = -1;

                foreach (var dbColumn in firstRowReader.GetColumnSchema())
                {
                    index++;

                    if (dbColumn.IsIdentity != true && dbColumn.IsAutoIncrement != true)
                        continue;

                    dataExportParams.IdentityColumnFound = true;

                    if (identityColumnIndex >= 0)
                        continue;

                    dataExportParams.IdentityColumnName = dbColumn.ColumnName;
                    identityColumnIndex = index;
                }
            }

            // Look for an identity column by querying pg_attribute

            var identityColumnCommand = new NpgsqlCommand(sqlIdentityColumn, mPgConnection);

            using (var identityColumnReader = identityColumnCommand.ExecuteReader())
            {
                if (identityColumnReader.HasRows)
                {
                    while (identityColumnReader.Read())
                    {
                        var columnName = identityColumnReader.GetString(0);
                        var columnPosition = identityColumnReader.GetInt32(1);

                        dataExportParams.IdentityColumnFound = true;

                        if (identityColumnIndex >= 0)
                            continue;

                        dataExportParams.IdentityColumnName = columnName;
                        identityColumnIndex = columnPosition - 1;
                    }
                }
            }

            if (quotedColumnNames.Count == 0)
            {
                OnStatusEvent("All of the columns in table {0} in database {1} are an unsupported data type; skipping data export", sourceTableNameWithSchema, databaseName);
                return true;
            }

            string columnList;

            if (skippedColumnCount == 0)
            {
                columnList = "*";
            }
            else
            {
                columnList = string.Join(", ", quotedColumnNames);
            }

            var sqlGetTableData = GetDataExportSql(sourceTableNameWithSchema, tableInfo, maxRowsToExport, columnList);

            if (!GetTableDataAsDataSet(sqlGetTableData, out var queryResults))
            {
                // Skip this table
                OnStatusEvent("Error retrieving data from table {0} in database {1}; skipping data export", sourceTableNameWithSchema, databaseName);
                return false;
            }

            var dataRowCount = queryResults.Tables[0].Rows.Count;

            if (dataRowCount == 0)
            {
                OnStatusEvent("No data found for table {0} in database {1}; skipping data export", sourceTableNameWithSchema, databaseName);
                return true;
            }

            if (mOptions.ScriptingOptions.SaveDataAsInsertIntoStatements || tableInfo.UsePgInsert)
            {
                // Skip any computed columns since exporting data using PostgreSQL compatible INSERT INTO statements

                var mapInfoToUse = mOptions.ColumnMapForDataExport.TryGetValue(tableInfo.SourceTableName, out var currentColumnMapInfo)
                    ? currentColumnMapInfo
                    : new ColumnMapInfo(tableInfo.SourceTableName);

                var skippedColumn = false;

                var generatedColumnCommand = new NpgsqlCommand(sqlGeneratedStoredColumns, mPgConnection);

                using (var generatedColumnReader = generatedColumnCommand.ExecuteReader())
                {
                    while (generatedColumnReader.Read())
                    {
                        var currentColumn = generatedColumnReader.GetString(0);

                        OnStatusEvent("Skipping computed column {0} on table {1}", currentColumn, tableInfo.SourceTableName);

                        mapInfoToUse.SkipColumn(currentColumn);
                        skippedColumn = true;
                    }
                }

                var xidColumnCommand = new NpgsqlCommand(sqlXidColumns, mPgConnection);

                using (var xidColumnReader = xidColumnCommand.ExecuteReader())
                {
                    while (xidColumnReader.Read())
                    {
                        var currentColumn = xidColumnReader.GetString(0);

                        OnStatusEvent("Skipping xid column {0} on table {1}", currentColumn, tableInfo.SourceTableName);

                        mapInfoToUse.SkipColumn(currentColumn);
                        skippedColumn = true;
                    }
                }

                if (skippedColumn && !mOptions.ColumnMapForDataExport.ContainsKey(tableInfo.SourceTableName))
                {
                    mOptions.ColumnMapForDataExport.Add(tableInfo.SourceTableName, mapInfoToUse);
                }
            }

            var headerRows = new List<string>();

            var header = COMMENT_START_TEXT + "Object:  Table " + dataExportParams.TargetTableNameWithSchema;

            if (mOptions.ScriptingOptions.IncludeTimestampInScriptFileHeader)
            {
                header += "    " + COMMENT_SCRIPT_DATE_TEXT + GetTimeStamp();
            }

            header += COMMENT_END_TEXT;
            headerRows.Add(header);

            if (tableInfo.FilterByDate)
            {
                headerRows.Add(string.Format("{0}Date filter: {1} >= '{2:yyyy-MM-dd}'{3}",
                    COMMENT_START_TEXT, tableInfo.DateColumnName, tableInfo.MinimumDate, COMMENT_END_TEXT));
            }

            var columnMapInfo = ConvertDataTableColumnInfo(dataExportParams.SourceTableNameWithSchema, quoteWithSquareBrackets, dataExportParams);

            var tableDataOutputFile = GetTableDataOutputFile(tableInfo, dataExportParams, workingParams, out var relativeFilePath);

            if (tableDataOutputFile == null)
            {
                // Skip this table (a warning message should have already been shown)
                return false;
            }

            var insertIntoLine = string.Empty;

            if (mOptions.ScriptingOptions.SaveDataAsInsertIntoStatements)
            {
                // For tables with an identity column that uses "GENERATED BY DEFAULT", we can explicitly set the value of the identity field

                // However, for tables with "GENERATED ALWAYS", we need to use "OVERRIDING SYSTEM VALUE"
                //
                // INSERT INTO UserDetail (UserDetailId, UserName, Password)
                // OVERRIDING SYSTEM VALUE
                //   VALUES(1,'admin', 'password');

                // Simple Insert Into
                // insertIntoLine = string.Format("INSERT INTO {0} VALUES (", dataExportParams.TargetTableNameWithSchema);

                insertIntoLine = ExportDBTableDataInit(tableInfo, columnMapInfo, dataExportParams, headerRows, workingParams, queryResults, tableDataOutputFile, relativeFilePath, out _);

                headerRows.Add(COMMENT_START_TEXT + "Columns: " + dataExportParams.HeaderRowValues + COMMENT_END_TEXT);

                dataExportParams.ColSepChar = ',';
            }
            else
            {
                // Export data as a tab-delimited table
                headerRows.Add(dataExportParams.HeaderRowValues.ToString());
                dataExportParams.ColSepChar = '\t';
            }

            if (mOptions.ScriptPgLoadCommands)
            {
                workingParams.AddDataLoadScriptFile(relativeFilePath);
            }

            using var writer = new StreamWriter(new FileStream(tableDataOutputFile.FullName, FileMode.Create, FileAccess.Write, FileShare.ReadWrite));

            // Note that the following method will set the session_replication_role to "replica" if PgInsertEnabled is true
            PossiblyDisableTriggers(dataExportParams, mOptions, writer);

            foreach (var headerRow in headerRows)
            {
                writer.WriteLine(headerRow);
            }

            ExportDBTableDataWork(writer, queryResults, insertIntoLine, dataExportParams);

            if (dataExportParams.PgInsertEnabled)
            {
                AppendPgExportFooters(writer, dataExportParams);

                if (dataExportParams.IdentityColumnFound)
                {
                    AppendPgExportSetSequenceValue(writer, dataExportParams, identityColumnIndex);
                }
            }
            else if (dataExportParams.IdentityColumnFound && mOptions.ScriptingOptions.SaveDataAsInsertIntoStatements)
            {
                if (!mOptions.PgDumpTableData)
                {
                    writer.WriteLine("SET IDENTITY_INSERT " + dataExportParams.QuotedTargetTableNameWithSchema + " OFF;");
                }

                writer.WriteLine();
                writer.WriteLine("-- If a table has an identity value, after inserting data with explicit identities,");
                writer.WriteLine("-- the sequence will need to be synchronized up with the table");
                writer.WriteLine();
                writer.WriteLine("-- Option 1, for columns that use a serial for the identity field");
                writer.WriteLine("-- select setval(pg_get_serial_sequence('{0}', 'my_serial_column'),", dataExportParams.TargetTableNameWithSchema);
                writer.WriteLine("--               (select max(my_serial_column) from {0}) );", dataExportParams.TargetTableNameWithSchema);
                writer.WriteLine();
                writer.WriteLine("-- Option 2, for columns that get their default value from a sequence");
                writer.WriteLine("-- select setval('my_sequence_name', (select max(my_serial_column) from {0});", dataExportParams.TargetTableNameWithSchema);
            }

            // Note that the following method will set the session_replication_role to "origin" if PgInsertEnabled is true
            PossiblyEnableTriggers(dataExportParams, mOptions, writer);

            return true;
        }

        private bool ExportDBTableDataUsingPgDump(
            string databaseName,
            WorkingParams workingParams,
            TableDataExportInfo tableInfo,
            string sourceTableNameWithSchema)
        {
            var dataExportParams = new DataExportWorkingParams(false, "null")
            {
                SourceTableNameWithSchema = sourceTableNameWithSchema,
            };

            dataExportParams.TargetTableNameWithSchema = GetTargetTableName(dataExportParams, tableInfo);

            if (string.IsNullOrWhiteSpace(dataExportParams.TargetTableNameWithSchema))
            {
                // Skip this table
                OnStatusEvent("Could not determine the target table name for table {0} in database {1}", dataExportParams.SourceTableNameWithSchema, databaseName);
                return false;
            }

            var tableDataOutputFile = GetTableDataOutputFile(tableInfo, dataExportParams, workingParams, out _);

            if (tableDataOutputFile == null)
            {
                // Skip this table (a warning message should have already been shown)
                return false;
            }

            var existingData = tableDataOutputFile.Exists ? tableDataOutputFile.LastWriteTime : DateTime.MinValue;

            var serverInfoArgs = GetPgDumpServerInfoArgs(string.Empty);

            // pg_dump -h host -p port -U user -W PasswordIfDefined -d database --data-only --table=TableName --format=p --file=OutFilePath
            var cmdArgs = string.Format("{0} -d {1} --data-only --table={2} --format=p --file={3}",
                                        serverInfoArgs, databaseName, dataExportParams.SourceTableNameWithSchema, tableDataOutputFile.FullName);
            const int maxRuntimeSeconds = 60;

            var pgDump = FindPgDumpExecutable();

            if (pgDump == null)
            {
                return false;
            }

            if (mOptions.PreviewExport)
            {
                OnStatusEvent("Preview running {0} {1}", pgDump.FullName, cmdArgs);
                return true;
            }

            var success = mProgramRunner.RunCommand(
                pgDump.FullName, cmdArgs, workingParams.OutputDirectory.FullName,
                      out var consoleOutput, out _, maxRuntimeSeconds);

            if (!success)
            {
                OnWarningEvent("Error reported for {0}: {1}", pgDump.Name, consoleOutput);
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
                if (string.IsNullOrWhiteSpace(mOptions.PgDumpTableDataSortOrderFile))
                {
                    return true;
                }

                // Possibly sort the table data (if the table is not defined in the sort order file, the data is not sorted)
                return PgDumpDataSorter.SortPgDumpTableData(tableInfo.SourceTableName, sourceTableNameWithSchema, tableDataOutputFile);
            }

            // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression

            if (tableDataOutputFile.Exists)
                OnWarningEvent("{0} did not replace {1}", pgDump.Name, tableDataOutputFile.FullName);
            else
                OnWarningEvent("{0} did not create {1}", pgDump.Name, tableDataOutputFile.FullName);

            return false;
        }

        /// <summary>
        /// Look for the executable in the given directory or any of its subdirectories
        /// </summary>
        /// <param name="baseDirectory">Base directory</param>
        /// <param name="exeName">Executable name</param>
        /// <param name="preferredDirectoryName">Preferred subdirectory</param>
        /// <returns>Newest version of the executable</returns>
        private FileInfo FindNewestExecutable(DirectoryInfo baseDirectory, string exeName, string preferredDirectoryName)
        {
            var foundFiles = baseDirectory.GetFileSystemInfos(exeName, SearchOption.AllDirectories);

            if (foundFiles.Length == 0)
                return null;

            FileInfo newestItem = null;
            FileInfo newestItemPreferredDirectory = null;

            foreach (var item in foundFiles)
            {
                if (item is not FileInfo matchingExe)
                    continue;

                if (newestItem == null || item.LastWriteTime > newestItem.LastWriteTime)
                {
                    newestItem = matchingExe;
                }

                if (string.IsNullOrWhiteSpace(preferredDirectoryName) || matchingExe.Directory?.Name.Equals(preferredDirectoryName) != true)
                {
                    continue;
                }

                if (newestItemPreferredDirectory == null || item.LastWriteTime > newestItemPreferredDirectory.LastWriteTime)
                {
                    newestItemPreferredDirectory = matchingExe;
                }
            }

            return newestItemPreferredDirectory ?? newestItem;
        }

        private FileInfo FindPgDumpExecutable()
        {
            return FindPgExecutable("pg_dump", "pg_dump.exe");
        }

        private FileInfo FindPgDumpAllExecutable()
        {
            return FindPgExecutable("pg_dumpall", "pg_dumpall.exe");
        }

        private FileInfo FindPgExecutable(string executableNameLinux, string executableNameWindows)
        {
            return SystemInfo.IsLinux ? FindPgExecutableLinux(executableNameLinux) : FindPgExecutableWindows(executableNameWindows);
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
                    var foundFile = FindNewestExecutable(userDirectory, exeName, "bin");

                    if (foundFile != null)
                    {
                        mCachedExecutables.Add(exeName, foundFile);
                        return foundFile;
                    }
                }

                var workingDirectory = new DirectoryInfo(".");
                var foundWorkDirFile = FindNewestExecutable(workingDirectory, exeName, "bin");

                if (foundWorkDirFile != null)
                {
                    mCachedExecutables.Add(exeName, foundWorkDirFile);
                    return foundWorkDirFile;
                }

                OnWarningEvent("Could not find {0} in {1}, below {2}, or below the working directory", exeName, alternativesDir.FullName, userDirectory.FullName);
                return null;
            }
            catch (Exception ex)
            {
                SetLocalError(
                    DBSchemaExportErrorCodes.GeneralError,
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
                    // Find the newest file named exeName, preferably choosing the .exe in the bin directory vs. the pgAdmin 4 runtime directory
                    var foundFile = FindNewestExecutable(postgresDirectory, exeName, "bin");

                    if (foundFile != null)
                    {
                        mCachedExecutables.Add(exeName, foundFile);
                        return foundFile;
                    }
                }

                var workingDirectory = new DirectoryInfo(".");
                var foundWorkDirFile = FindNewestExecutable(workingDirectory, exeName, "bin");

                if (foundWorkDirFile != null)
                {
                    mCachedExecutables.Add(exeName, foundWorkDirFile);
                    return foundWorkDirFile;
                }

                OnWarningEvent("Could not find {0} in either a subdirectory of {1} or below the working directory", exeName, postgresDirectory.FullName);
                return null;
            }
            catch (Exception ex)
            {
                SetLocalError(
                    DBSchemaExportErrorCodes.GeneralError,
                    string.Format("Error in FindPgExecutableWindows finding {0}", exeName), ex);

                return null;
            }
        }

        /// <summary>
        /// Retrieve a list of tables in the given database
        /// </summary>
        /// <param name="databaseName">Database to query</param>
        /// <param name="includeTableRowCounts">When true, determines the row count in each table</param>
        /// <param name="includeSystemObjects">When true, also returns system tables</param>
        /// <returns>Dictionary where keys are instances of TableDataExportInfo and values are row counts (if includeTableRowCounts = true)</returns>
        public override Dictionary<TableDataExportInfo, long> GetDatabaseTables(
            string databaseName,
            bool includeTableRowCounts,
            bool includeSystemObjects)
        {
            return GetPgServerDatabaseTables(databaseName, includeTableRowCounts, includeSystemObjects, true, out _);
        }

        private string GetDataExportSql(string sourceTableNameWithSchema, TableDataExportInfo tableInfo, long maxRowsToExport, string columnList = "*")
        {
            var sql = string.Format("SELECT {0} FROM {1}", columnList, sourceTableNameWithSchema);

            if (tableInfo.FilterByDate)
            {
                sql += string.Format(" WHERE {0} >= '{1:yyyy-MM-dd}'", tableInfo.DateColumnName, tableInfo.MinimumDate);
            }

            if (maxRowsToExport > 0)
            {
                sql += " limit " + maxRowsToExport;
            }

            return sql;
        }

        private string GetFunctionOrProcedureName(
            DatabaseObjectInfo currentObject,
            string objectDescription,
            ref bool unhandledScriptingCommands)
        {
            var nameMatch = mFunctionOrProcedureNameMatcher.Match(currentObject.Name);

            if (nameMatch.Success)
            {
                return nameMatch.Value;
            }

            OnWarningEvent("Did not find a {0} name in: {1}", objectDescription, currentObject.Name);
            unhandledScriptingCommands = true;
            return currentObject.Name;
        }

        /// <summary>
        /// Look for the "Name" and "Type" groups in the RegEx match
        /// Combine them, but excluding any arguments after the object name
        /// </summary>
        /// <param name="match">Regex match</param>
        /// <returns>Text of the form "FUNCTION_udf_timestamp_text"</returns>
        private string GetObjectTypeNameCode(Match match)
        {
            var objectNameWithArguments = match.Groups["Name"].Value;
            var objectType = match.Groups["Type"].Value;

            var nameMatch = mFunctionOrProcedureNameMatcher.Match(objectNameWithArguments);

            if (nameMatch.Success)
            {
                return objectType + "_" + nameMatch.Groups["Name"];
            }

            return objectType + "_" + objectNameWithArguments;
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

            return string.Format(
                "-h {0} -p {1} -U {2} {3}{4}",
                mOptions.ServerName, mOptions.PgPort, mOptions.DBUser, passwordArgument, databaseArgument);
        }

        /// <summary>
        /// Lookup the tables in the specified database, optionally also determining table row counts
        /// </summary>
        /// <param name="databaseName">Database to query</param>
        /// <param name="includeTableRowCounts">When true, determines the row count in each table</param>
        /// <param name="includeSystemObjects">When true, also returns system tables</param>
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
                        OnWarningEvent("Database {0} not found on sever {1}", databaseName, mCurrentServerInfo.ServerName);
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
                SetLocalError(DBSchemaExportErrorCodes.DatabaseConnectionError, string.Format("Error connecting to server {0}", mCurrentServerInfo.ServerName), ex);
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
                OnDebugEvent("Obtaining list of tables in database {0} on server {1}", databaseName, mCurrentServerInfo.ServerName);

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

                            var tableNameWithSchema = PossiblyQuoteName(schemaName) + "." + PossiblyQuoteName(tableName);

                            if (SkipSchema(schemaName))
                            {
                                ShowTrace(string.Format(
                                    "Skipping schema export from table {0}.{1} due to a schema name filter", schemaName, tableName));

                                continue;
                            }

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

                        var rowCountSql = string.Format(
                            "SELECT relname, reltuples::bigint as ApproximateRowCount " +
                            "FROM pg_class " +
                            "WHERE oid = '{0}'::regclass", item.SourceTableName);

                        // ReSharper restore StringLiteralTypo

                        var rowCountCmd = new NpgsqlCommand(rowCountSql, mPgConnection);
                        using (var reader = rowCountCmd.ExecuteReader())
                        {
                            if (reader.HasRows && reader.Read())
                            {
                                // Read the approximate row count
                                databaseTableInfo[item] = reader.GetInt64(1);
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
                                // Read the row count
                                databaseTableInfo[item] = reader.GetInt64(0);
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
                OnStatusEvent("Found {0} tables in database {1}", databaseTableInfo.Count, databaseName);

                return databaseTableInfo;
            }
            catch (Exception ex)
            {
                SetLocalError(
                    DBSchemaExportErrorCodes.GeneralError,
                    string.Format("Error obtaining list of tables in database {0} on server {1}", databaseName, mCurrentServerInfo.ServerName), ex);

                return new Dictionary<TableDataExportInfo, long>();
            }
        }

        /// <summary>
        /// Query the database to obtain the primary key information for every table
        /// Store in workingParams.PrimaryKeysByTable
        /// </summary>
        /// <param name="workingParams">Working parameters</param>
        /// <returns>True if successful, false if an error</returns>
        public override bool GetPrimaryKeyInfoFromDatabase(WorkingParams workingParams)
        {
            try
            {
                // Query to view primary key columns, by table, listed by schema name, table name, and ordinal position

                workingParams.PrimaryKeysByTable.Clear();

                var sql = string.Format(
                    "SELECT U.Table_Schema, " +
                    "       U.Table_Name, " +
                    "       U.Column_Name, " +
                    "       C.Ordinal_Position " +
                    "FROM INFORMATION_SCHEMA.Table_Constraints T " +
                    "     INNER JOIN INFORMATION_SCHEMA.Constraint_Column_Usage U " +
                    "       ON U.Constraint_Name = T.Constraint_Name AND " +
                    "          U.Constraint_Schema = T.Constraint_Schema " +
                    "     INNER JOIN INFORMATION_SCHEMA.Columns C " +
                    "       ON U.Table_Schema = C.Table_Schema AND " +
                    "          U.Table_Name = C.Table_Name AND " +
                    "          U.Column_Name = C.Column_Name " +
                    "WHERE U.Table_Catalog = '{0}' AND " +
                    "      T.Constraint_Type = 'PRIMARY KEY' " +
                    "ORDER BY U.Table_Schema, U.Table_Name, C.Ordinal_Position, U.Column_Name;",
                    mCurrentServerInfo.DatabaseName);

                var constraintInfoCommand = new NpgsqlCommand(sql, mPgConnection);
                using var reader = constraintInfoCommand.ExecuteReader();

                const bool quoteWithSquareBrackets = false;
                const bool alwaysQuoteNames = false;

                if (reader.HasRows)
                {
                    while (reader.Read())
                    {
                        var tableSchema = reader.GetString(0);
                        var tableName = reader.GetString(1);
                        var columnName = reader.GetString(2);

                        // var ordinalPosition = reader.GetInt32(3);

                        var tableNameWithSchema = GetTableNameToUse(tableSchema, tableName, quoteWithSquareBrackets, alwaysQuoteNames);

                        if (workingParams.PrimaryKeysByTable.TryGetValue(tableNameWithSchema, out var existingPrimaryKeyColumns))
                        {
                            existingPrimaryKeyColumns.Add(columnName);
                            continue;
                        }

                        var primaryKeyColumns = new List<string>
                        {
                            columnName
                        };

                        workingParams.PrimaryKeysByTable.Add(tableNameWithSchema, primaryKeyColumns);
                    }
                }

                workingParams.PrimaryKeysRetrieved = true;
                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent(string.Format("Error obtaining primary key columns for tables in database {0} on the current server", mCurrentServerInfo.DatabaseName), ex);
                return false;
            }
        }

        private string GetServerConnectionInfo()
        {
            return string.Format("{0}, port {1}, user {2}", mOptions.ServerName, mOptions.PgPort, mOptions.DBUser);
        }

        /// <summary>
        /// Retrieve a list of database names on the server defined in mOptions
        /// </summary>
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
        /// <remarks>Assumes we already have an active server connection</remarks>
        /// <returns>Enumerable list of database names</returns>
        protected override IEnumerable<string> GetServerDatabasesCurrentConnection()
        {
            return GetServerDatabasesWork();
        }

        /// <summary>
        /// Get the list of databases from the current server
        /// </summary>
        private IEnumerable<string> GetServerDatabasesWork()
        {
            var databaseNames = new List<string>();

            // ReSharper disable StringLiteralTypo
            const string sql = "SELECT datname FROM pg_database WHERE datistemplate=false and datallowconn=true ORDER BY datname";
            // ReSharper restore StringLiteralTypo

            var cmd = new NpgsqlCommand(sql, mPgConnection);
            using var reader = cmd.ExecuteReader();

            if (reader.HasRows)
            {
                while (reader.Read())
                {
                    databaseNames.Add(reader.GetString(0));
                }
            }

            return databaseNames;
        }

        private bool GetTableDataAsDataSet(string sqlGetTableData, out DataSet queryResults)
        {
            queryResults = new DataSet();

            try
            {
                var tableListCommand = new NpgsqlCommand(sqlGetTableData, mPgConnection);

                using var adapter = new NpgsqlDataAdapter(tableListCommand);
                adapter.Fill(queryResults);     // Fill the DataSet with the results of the query

                return true;
            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.GeneralError, "Error in DBSchemaExporterPostgreSQL.GetTableDataAsDataSet", ex);
                return false;
            }
        }

        private string GetTargetTableName(
            DataExportWorkingParams dataExportParams,
            TableDataExportInfo tableInfo)
        {
            const bool quoteWithSquareBrackets = false;
            const bool alwaysQuoteNames = false;

            return GetTargetTableName(dataExportParams, tableInfo, quoteWithSquareBrackets, alwaysQuoteNames);
        }

        /// <summary>
        /// Login to the server
        /// </summary>
        /// <param name="databaseName">PostgreSQL database to connect to</param>
        /// <param name="pgConnection">Output: Npgsql connection instance</param>
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
                var connectionString = string.Format(
                    "Host={0};Username={1};Password={2};Database={3}",
                    mOptions.ServerName, mOptions.DBUser, userPassword, databaseName);

                var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(connectionString, "DBSchemaExportTool");

                pgConnection = new NpgsqlConnection(connectionStringToUse);
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

        // ReSharper disable once GrammarMistakeInComment

        /// <summary>
        /// Look for a .pgpass file (if Linux) or a pgpass.conf file (if Windows)
        /// If a file is found, parse it to look for the given user and database on the server defined by mOptions.ServerName
        /// </summary>
        /// <remarks>This method will return an empty string if a match is found to an entry in a pgpass file in the standard location for this OS</remarks>
        /// <param name="pgUser">Username</param>
        /// <param name="currentDatabase">Database name</param>
        /// <param name="definedInPgPassFile">Output: true if the user has an entry for the given database in the .pgpass file</param>
        /// <returns>An empty string if the password file is in the standard location; otherwise, the password (if found)</returns>
        private string LookupUserPasswordFromDisk(string pgUser, string currentDatabase, out bool definedInPgPassFile)
        {
            try
            {
                // Keys in this dictionary are file paths, values are true if this is the standard location for a pgpass file
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
                    var appDataDirectory = Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData);

                    // Standard location: %APPDATA%\postgresql\pgpass.conf
                    candidateFilePaths.Add(Path.Combine(appDataDirectory, "postgresql", passwordFileName), true);

                    // pgpass.conf in the current directory
                    candidateFilePaths.Add(passwordFileName, false);
                }

                var passwordFileExists = false;

                // First look for a matching server, database, and username using a case-sensitive match
                // If no match, try again with case-insensitive matching

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
                            var passwordToReturn = LookupUserPasswordFromDisk(
                                candidateFile, isStandardLocation,
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
                    OnWarningEvent("Could not find the {0} file; unable to determine the password for user {1}", passwordFileName, pgUser);
                    definedInPgPassFile = false;
                    return string.Empty;
                }

                OnWarningEvent("Could not find a valid password for user {0} in file {1}", pgUser, passwordFileName);
                definedInPgPassFile = false;
                return string.Empty;
            }
            catch (Exception ex)
            {
                SetLocalError(
                    DBSchemaExportErrorCodes.DatabaseConnectionError,
                    string.Format("Error looking up the password for user {0} on server {1}", pgUser, GetServerConnectionInfo()), ex);

                definedInPgPassFile = false;
                return string.Empty;
            }
        }

        /// <summary>
        /// Parse the given pgpass file to look for the given user and database on the server defined by mOptions.ServerName
        /// </summary>
        /// <remarks>Will update mOptions.ServerName and/or mOptions.DBUser if there is a case mismatch</remarks>
        /// <param name="passwordFile">Password file info</param>
        /// <param name="isStandardLocation">True if the password file is in the standard location for this computer</param>
        /// <param name="pgUser">Username</param>
        /// <param name="currentDatabase">Database name</param>
        /// <param name="caseSensitive">When true, use case-sensitive matching</param>
        /// <param name="definedInPgPassFile">Output: true if the user has an entry for the given database in the .pgpass file</param>
        /// <returns>An empty string if the password file is in the standard location; otherwise, the password (if found)</returns>
        private string LookupUserPasswordFromDisk(
            FileSystemInfo passwordFile,
            bool isStandardLocation,
            string pgUser,
            string currentDatabase,
            bool caseSensitive,
            out bool definedInPgPassFile)
        {
            using var reader = new StreamReader(new FileStream(passwordFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

            var comparisonType = caseSensitive ? StringComparison.Ordinal : StringComparison.OrdinalIgnoreCase;

            while (!reader.EndOfStream)
            {
                var dataLine = reader.ReadLine();

                if (string.IsNullOrWhiteSpace(dataLine))
                    continue;

                // dataLine is of the form
                // hostname:port:database:username:password
                // The first four fields can contain a *
                var lineParts = dataLine.Split([':'], 5);

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

                if (!string.IsNullOrWhiteSpace(currentDatabase) && !string.Equals(database, currentDatabase, comparisonType) && !database.Equals("*"))
                    continue;

                if (!string.Equals(username, pgUser, comparisonType) && !username.Equals("*"))
                    continue;

                if (string.IsNullOrWhiteSpace(password))
                {
                    OnWarningEvent(
                        "The {0} file has a blank password for user {1}, server {2}, database {3}; ignoring this entry",
                        passwordFile.Name, username, hostname, database);

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

                OnDebugEvent("Determined password for user {0} using {1}", pgUser, passwordFile.FullName);

                if (isStandardLocation)
                {
                    definedInPgPassFile = true;
                    return string.Empty;
                }

                definedInPgPassFile = false;
                return password;
            }

            definedInPgPassFile = false;
            return string.Empty;
        }

        /// <summary>
        /// If objectName contains characters other than A-Z, a-z, 0-9, or an underscore, surround the name with double quotes
        /// </summary>
        /// <remarks>Also quote if the name is a keyword</remarks>
        /// <param name="objectName">Object name</param>
        private string PossiblyQuoteName(string objectName)
        {
            return PossiblyQuoteName(objectName, false);
        }

        private string ProcessAndStoreCachedLinesTargetScriptFile(
            IDictionary<string, List<string>> scriptInfoByObject,
            string databaseName,
            List<string> cachedLines,
            DatabaseObjectInfo currentObject,
            string previousTargetScriptFile,
            ref bool unhandledScriptingCommands)
        {
            if (cachedLines.Count == 0)
                return previousTargetScriptFile;

            ProcessCachedLines(databaseName, cachedLines,
                currentObject,
                previousTargetScriptFile,
                out var targetScriptFile,
                out var skipExportCachedLines,
                ref unhandledScriptingCommands);

            if (skipExportCachedLines)
                return targetScriptFile;

            StoreCachedLinesForObject(scriptInfoByObject, cachedLines, targetScriptFile, currentObject);
            return targetScriptFile;
        }

        private void ProcessCachedLines(
            string databaseName,
            IList<string> cachedLines,
            DatabaseObjectInfo currentObject,
            string previousTargetScriptFile,
            out string targetScriptFile,
            out bool skipExportCachedLines,
            ref bool unhandledScriptingCommands)
        {
            skipExportCachedLines = false;

            if (string.IsNullOrEmpty(currentObject.Name))
            {
                targetScriptFile = string.Format("DatabaseInfo_{0}.sql", databaseName);
                return;
            }

            var schemaToUse = currentObject.Schema;
            var nameToUse = currentObject.Name;

            if (SkipSchema(schemaToUse))
            {
                skipExportCachedLines = true;
                targetScriptFile = schemaToUse + "." + nameToUse + ".sql";
                return;
            }

            switch (currentObject.Type)
            {
                case "TABLE":
                case "VIEW":
                case "FOREIGN TABLE":
                    break;

                case "ACL":
                    var aclFunctionOrProcedureMatch = mAclMatcherFunctionOrProcedure.Match(currentObject.Name);
                    var aclSchemaMatch = mAclMatcherSchema.Match(currentObject.Name);
                    var aclTableMatch = mAclMatcherTable.Match(currentObject.Name);
                    var aclColumnMatch = mAclMatcherColumn.Match(currentObject.Name);

                    if (aclTableMatch.Success)
                    {
                        nameToUse = aclTableMatch.Groups["TableName"].Value;
                    }
                    else if (aclColumnMatch.Success)
                    {
                        nameToUse = aclColumnMatch.Groups["TableOrViewName"].Value;
                    }
                    else if (aclFunctionOrProcedureMatch.Success)
                    {
                        nameToUse = aclFunctionOrProcedureMatch.Groups["ObjectName"].Value;
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

                case "COLLATION":
                    nameToUse = "_Collation_" + currentObject.Name;
                    break;

                case "COMMENT":
                    var typeMatch = mNameTypeTargetMatcher.Match(currentObject.Name);

                    if (typeMatch.Success)
                    {
                        var targetObjectType = typeMatch.Groups["ObjectType"].Value;
                        var targetObjectName = UnquoteName(typeMatch.Groups["ObjectName"].Value);

                        if (targetObjectName.StartsWith("\""))
                        {
                            // The name is surrounded by double quotes; remove them
                        }

                        switch (targetObjectType)
                        {
                            case "EXTENSION":
                                nameToUse = "_Extension_" + targetObjectName;
                                break;

                            case "FUNCTION":
                            case "PROCEDURE":
                            case "TABLE":
                            case "VIEW":
                                nameToUse = targetObjectName;
                                break;

                            case "COLUMN":
                                // targetObjectName should be of the form "schema.table.column"
                                // Remove the column name

                                var columnMatchWithSchema = mTableColumnMatcherWithSchema.Match(targetObjectName);

                                if (columnMatchWithSchema.Success)
                                {
                                    nameToUse = columnMatchWithSchema.Groups["TableName"].Value;
                                    break;
                                }

                                var columnMatch = mTableColumnMatcher.Match(targetObjectName);

                                if (columnMatch.Success)
                                {
                                    nameToUse = columnMatch.Groups["TableName"].Value;
                                    break;
                                }

                                OnWarningEvent("Possibly add a custom object type handler for comments on target object " + targetObjectType);
                                nameToUse = targetObjectName;
                                unhandledScriptingCommands = true;
                                break;

                            case "DOMAIN":
                                targetScriptFile = previousTargetScriptFile;
                                return;

                            default:
                                OnWarningEvent("Possibly add a custom object type handler for comments on target object " + targetObjectType);
                                nameToUse = targetObjectName;
                                unhandledScriptingCommands = true;
                                break;
                        }
                    }
                    else
                    {
                        OnWarningEvent("Comment object type match failure for " + currentObject.Name);
                        unhandledScriptingCommands = true;
                    }

                    break;

                case "CONSTRAINT":
                case "FK CONSTRAINT":
                case "DEFAULT":
                case "SEQUENCE":
                case "SEQUENCE OWNED BY":

                    // Parse out the target table name from the Alter Table DDL

                    // The RegEx will match lines like this:
                    // ALTER TABLE mc.t_event_log
                    // ALTER TABLE ONLY mc.t_event_log

                    var alterTableMatcher = new Regex(
                        string.Format(@"ALTER TABLE.+{0}\.(?<TargetTable>.+)", schemaToUse),
                              RegexOptions.Compiled | RegexOptions.IgnoreCase);

                    var alterTableAlterColumnMatcher = new Regex(
                        string.Format(@"ALTER TABLE.+{0}\.(?<TargetTable>.+) ALTER COLUMN", schemaToUse),
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
                            if (cachedLine.StartsWith("CREATE SEQUENCE ") ||
                                cachedLine.StartsWith("ALTER SEQUENCE "))
                            {
                                targetScriptFile = previousTargetScriptFile;
                                return;
                            }

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
                        OnWarningEvent(
                            "Did not find a valid ALTER TABLE line in the cached lines for a {0}{1}",
                            currentObject.Type.StartsWith("SEQUENCE") ? "sequence near: " : "constraint against: ",
                            currentObject.Name);

                        unhandledScriptingCommands = true;
                    }

                    break;

                case "DOMAIN":
                    nameToUse = "_Domain_" + currentObject.Name;
                    break;

                case "EVENT TRIGGER":
                    nameToUse = "_EventTrigger_" + currentObject.Name;
                    break;

                case "EXTENSION":
                    nameToUse = "_Extension_" + currentObject.Name;
                    break;

                case "FUNCTION":
                    nameToUse = GetFunctionOrProcedureName(currentObject, "function", ref unhandledScriptingCommands);
                    break;

                case "PROCEDURE":
                    nameToUse = GetFunctionOrProcedureName(currentObject, "procedure", ref unhandledScriptingCommands);
                    break;

                case "INDEX":
                    var indexName = currentObject.Name;
                    var createIndexMatched = false;

                    for (var i = 1; i <= 2; i++)
                    {
                        Regex createIndexMatcher;

                        // These RegEx specs include ONLY as an optional group to allow for CREATE INDEX statements on partitioned tables
                        if (i == 1)
                        {
                            createIndexMatcher = new Regex(string.Format(
                                    @"CREATE.+INDEX {0} ON (ONLY )?{1}\.(?<TargetTable>.+) USING", indexName, currentObject.Schema),
                                RegexOptions.Compiled | RegexOptions.IgnoreCase);
                        }
                        else
                        {
                            createIndexMatcher = new Regex(string.Format(
                                    "CREATE.+INDEX {0} ON (ONLY )?(?<TargetTable>.+) USING", indexName),
                                RegexOptions.Compiled | RegexOptions.IgnoreCase);
                        }

                        foreach (var cachedLine in cachedLines)
                        {
                            var match = createIndexMatcher.Match(cachedLine);

                            if (!match.Success)
                                continue;

                            nameToUse = match.Groups["TargetTable"].Value;
                            createIndexMatched = true;
                            break;
                        }

                        if (createIndexMatched)
                            break;
                    }

                    if (!createIndexMatched)
                    {
                        OnWarningEvent("Did not find a valid CREATE INDEX line in the cached lines for index: " + currentObject.Name);
                        unhandledScriptingCommands = true;
                    }

                    break;

                case "INDEX ATTACH":

                    var alterIndexMatcher = new Regex(string.Format(
                            "ALTER.+INDEX (?<TargetIndex>) ATTACH PARTITION {0}.{1}", currentObject.Schema, currentObject.Name),
                        RegexOptions.Compiled | RegexOptions.IgnoreCase);

                    var alterIndexMatched = false;

                    foreach (var cachedLine in cachedLines)
                    {
                        var match = alterIndexMatcher.Match(cachedLine);

                        if (!match.Success)
                            continue;

                        var indexNameWithSchema = GetSchemaName(match.Groups["TargetIndex"].Value);
                        alterIndexMatched = true;

                        schemaToUse = GetSchemaName(indexNameWithSchema, out nameToUse);

                        if (SkipSchema(schemaToUse))
                        {
                            skipExportCachedLines = true;
                        }

                        break;
                    }

                    if (!alterIndexMatched)
                    {
                        OnWarningEvent("Did not find a valid ALTER INDEX line in the cached lines for index: " + currentObject.Name);
                        unhandledScriptingCommands = true;
                    }

                    break;

                case "SCHEMA":
                    nameToUse = "_Schema_" + currentObject.Name;
                    schemaToUse = string.Empty;
                    break;

                case "SERVER":
                    nameToUse = "_Server_" + currentObject.Name;
                    schemaToUse = string.Empty;
                    break;

                case "TRIGGER":
                    var triggerTableMatch = mTriggerTargetTableMatcher.Match(currentObject.Name);

                    if (triggerTableMatch.Success)
                    {
                        nameToUse = triggerTableMatch.Value;
                    }
                    else
                    {
                        OnWarningEvent("Did not find a valid table name for trigger: " + currentObject.Name);
                        unhandledScriptingCommands = true;
                    }
                    break;

                case "TYPE":
                    nameToUse = "_Type_" + currentObject.Name;
                    break;

                case "USER MAPPING":
                    nameToUse = "_" + currentObject.Name.Replace(" ", "_").Replace("USER_MAPPING", "User_Mapping").Replace("_SERVER_", "_Server_");
                    schemaToUse = string.Empty;
                    break;

                case "RULE":
                    // Older versions of pg_dump exported views as "Type: RULE"

                    // pg_dump v17.x does this for view v_separation_group_list_report, first creating a view where the value for each column is Null
                    // then altering the view to have the correct source columns

                    // Excerpts from the dump file:

                    // -- Name: v_separation_group_list_report; Type: VIEW; Schema: public; Owner: d3l243
                    // CREATE VIEW public.v_separation_group_list_report AS
                    // SELECT
                    //     NULL::public.citext AS separation_group,
                    //     NULL::public.citext AS comment,

                    // -- Name: v_separation_group_list_report _RETURN; Type: RULE; Schema: public; Owner: d3l243
                    // CREATE OR REPLACE VIEW public.v_separation_group_list_report AS
                    //  SELECT sg.separation_group,
                    //     sg.comment,

                    // Append this rule-style DDL to the corresponding file (e.g. v_separation_group_list_report.sql)
                    nameToUse = currentObject.Name.Replace(" _RETURN", "");
                    break;

                default:
                    OnWarningEvent("Unrecognized object type: " + currentObject.Type);
                    unhandledScriptingCommands = true;
                    break;
            }

            // If nameToUse is surround by double quotes, remove them
            if (nameToUse.StartsWith("\"") && nameToUse.EndsWith("\"") && nameToUse.Length > 2)
            {
                nameToUse = nameToUse.Substring(1, nameToUse.Length - 2);
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

        /// <summary>
        /// Process the database dump file created by pgDump
        /// </summary>
        /// <remarks>
        /// This file will have DDL for schemas, roles, permissions, extensions, functions, tables, and views
        /// </remarks>
        /// <param name="databaseName">Database name</param>
        /// <param name="pgDumpOutputFile">pgDump output file</param>
        /// <param name="unhandledScriptingCommands">Output: true if unrecognized script commands were found</param>
        private void ProcessPgDumpSchemaFile(string databaseName, FileInfo pgDumpOutputFile, out bool unhandledScriptingCommands)
        {
            unhandledScriptingCommands = false;

            if (pgDumpOutputFile.Directory == null)
            {
                OnErrorEvent("Could not determine the parent directory of " + pgDumpOutputFile.FullName);
                return;
            }

            // Keys in this dictionary are object names (preceded by schema name if not public)
            // Values are a list of DDL for creating the object
            var scriptInfoByObject = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);

            using var reader = new StreamReader(new FileStream(pgDumpOutputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

            var cachedLines = new List<string>();
            var currentObject = new DatabaseObjectInfo();

            var previousTargetScriptFile = string.Empty;

            while (!reader.EndOfStream)
            {
                var dataLine = reader.ReadLine();

                if (dataLine == null)
                    continue;

                if (dataLine.StartsWith("USER MAPPING"))
                {
                    Console.WriteLine("Check this code");
                }

                if (dataLine.StartsWith("-- PostgreSQL database dump complete"))
                {
                    // The previous cached line is likely "--"
                    // Remove it
                    if (cachedLines.Count > 0 && cachedLines.Last().Equals("--"))
                    {
                        cachedLines.RemoveAt(cachedLines.Count - 1);
                    }

                    ProcessAndStoreCachedLinesTargetScriptFile(
                        scriptInfoByObject,
                        databaseName,
                        cachedLines,
                        currentObject,
                        previousTargetScriptFile,
                        ref unhandledScriptingCommands);

                    cachedLines = new List<string>();
                    break;
                }

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

                            StoreCachedLinesForObject(
                                scriptInfoByObject,
                                setDefaultLine,
                                databaseInfoScriptFile,
                                new DatabaseObjectInfo(databaseName, "DATABASE"));
                            continue;
                        }

                        cachedLines.Add(dataLine);
                        continue;
                    }

                    previousTargetScriptFile = ProcessAndStoreCachedLinesTargetScriptFile(
                        scriptInfoByObject,
                        databaseName,
                        cachedLines,
                        currentObject,
                        previousTargetScriptFile,
                        ref unhandledScriptingCommands);

                    UpdateCachedObjectInfo(match, currentObject);

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

            ProcessAndStoreCachedLinesTargetScriptFile(
                scriptInfoByObject,
                databaseName,
                cachedLines,
                currentObject,
                previousTargetScriptFile,
                ref unhandledScriptingCommands);

            var outputDirectory = pgDumpOutputFile.Directory.FullName;
            WriteCachedLines(outputDirectory, scriptInfoByObject);
        }

        /// <summary>
        /// Determine the primary key column (or columns) for a table
        /// </summary>
        /// <param name="dataExportParams">Data export parameters</param>
        /// <param name="workingParams">Working parameters</param>
        /// <param name="tableInfo">Table info</param>
        /// <param name="columnMapInfo">Class tracking the source and target column names for the table</param>
        /// <returns>Comma separated list of primary key column names (using target column names)</returns>
        protected override string ResolvePrimaryKeys(
            DataExportWorkingParams dataExportParams,
            WorkingParams workingParams,
            TableNameInfo tableInfo,
            ColumnMapInfo columnMapInfo)
        {
            return ResolvePrimaryKeysBase(dataExportParams, workingParams, tableInfo, columnMapInfo);
        }

        /// <summary>
        /// Export PostgreSQL Server settings
        /// </summary>
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
                var cmdArgs = string.Format("{0} --schema-only --globals-only --file={1}", serverInfoArgs, outputFile.FullName);
                const int maxRuntimeSeconds = 60;

                var pgDumpAll = FindPgDumpAllExecutable();

                if (pgDumpAll == null)
                    return false;

                if (mOptions.PreviewExport)
                {
                    OnStatusEvent("Preview running {0} {1}", pgDumpAll.FullName, cmdArgs);
                    return true;
                }

                var success = mProgramRunner.RunCommand(
                    pgDumpAll.FullName, cmdArgs, serverInfoOutputDirectory.FullName,
                          out var consoleOutput, out _, maxRuntimeSeconds);

                if (!success)
                {
                    OnWarningEvent("Error reported for {0}: {1}", pgDumpAll.Name, consoleOutput);
                    return false;
                }

                // Assure that the file was created
                outputFile.Refresh();

                if (!outputFile.Exists)
                {
                    OnWarningEvent("{0} did not create {1}", pgDumpAll.Name, outputFile.FullName);
                    return false;
                }

                if (outputFile.LastWriteTime > existingData)
                    return true;

                OnWarningEvent("{0} did not replace {1}", pgDumpAll.Name, outputFile.FullName);
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
            string targetScriptFile,
            DatabaseObjectInfo currentObject)
        {
            if (cachedLines.Count == 0)
                return;

            var outputFileName = CleanNameForOS(targetScriptFile);

            // Remove the final "--" from cachedLines, plus any blank lines
            while (cachedLines.Count > 0 && string.IsNullOrWhiteSpace(cachedLines.Last()) || cachedLines.Last().Equals("--"))
            {
                cachedLines.RemoveAt(cachedLines.Count - 1);
            }

            switch (currentObject.Type)
            {
                case "PROCEDURE":
                case "FUNCTION":
                    // Change CREATE PROCEDURE or CREATE FUNCTION
                    // to CREATE OR REPLACE
                    for (var i = 0; i < cachedLines.Count; i++)
                    {
                        if (cachedLines[i].StartsWith("CREATE PROCEDURE") ||
                            cachedLines[i].StartsWith("CREATE FUNCTION"))
                        {
                            cachedLines[i] = "CREATE OR REPLACE" + cachedLines[i].Substring("CREATE".Length);
                            break;
                        }
                    }

                    break;
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

        /// <summary>
        /// If objectName is surrounded by double quotes, remove them
        /// </summary>
        /// <param name="objectName">Object name</param>
        /// <returns>Unquoted name</returns>
        private string UnquoteName(string objectName)
        {
            if (objectName.StartsWith("\"") && objectName.EndsWith("\""))
                return objectName.Trim('"');

            return objectName;
        }

        private void UpdateCachedObjectInfo(
            Match match,
            DatabaseObjectInfo currentObject)
        {
            currentObject.Name = match.Groups["Name"].Value;
            currentObject.Type = match.Groups["Type"].Value;
            currentObject.Schema = match.Groups["Schema"].Value;
            currentObject.Owner = match.Groups["Owner"].Value;
        }

        /// <summary>
        /// Return true if we have a valid server connection
        /// </summary>
        protected override bool ValidServerConnection()
        {
            return mConnectedToServer && mPgConnection != null &&
                   mPgConnection.State != ConnectionState.Broken &&
                   mPgConnection.State != ConnectionState.Closed;
        }

        /// <summary>
        /// Create .sql files in the output directory
        /// </summary>
        /// <param name="outputDirectory">Output directory</param>
        /// <param name="scriptInfoByObject">Dictionary where keys are the target file names and values are the DDL commands to create the object</param>
        private void WriteCachedLines(
            string outputDirectory,
            Dictionary<string, List<string>> scriptInfoByObject)
        {
            var previousDirectoryPath = string.Empty;

            foreach (var item in scriptInfoByObject)
            {
                var outputFileName = item.Key;
                var cachedLines = item.Value;

                string schemaName;
                string outputFileNameToUse;
                var baseName = Path.GetFileNameWithoutExtension(outputFileName);

                if (string.IsNullOrWhiteSpace(baseName))
                {
                    OnWarningEvent("Unable to determine the file name; skipping " + outputFileName);
                    continue;
                }

                // Check for an overloaded function or procedure
                // If it is overloaded, add a comment for each overloaded instance
                AddCommentsIfOverloaded(cachedLines);

                // Write files that are not in the public schema to a subdirectory below the database directory
                var periodIndex = baseName.IndexOf('.');

                if (periodIndex > 0 && periodIndex < baseName.Length - 1)
                {
                    schemaName = baseName.Substring(0, periodIndex);
                    outputFileNameToUse = schemaName + baseName.Substring(periodIndex) + Path.GetExtension(outputFileName);

                    var targetDirectoryPath = Path.Combine(outputDirectory, schemaName);

                    if (!previousDirectoryPath.Equals(targetDirectoryPath))
                    {
                        var targetDirectory = new DirectoryInfo(targetDirectoryPath);

                        if (!targetDirectory.Exists)
                        {
                            OnStatusEvent("Creating output directory, " + targetDirectory.FullName);
                            targetDirectory.Create();
                        }

                        previousDirectoryPath = targetDirectoryPath;
                    }
                }
                else
                {
                    schemaName = string.Empty;
                    outputFileNameToUse = outputFileName;
                }

                var outputFilePath = Path.Combine(outputDirectory, schemaName, outputFileNameToUse);

                OnDebugEvent("Writing " + outputFilePath);

                using var writer = new StreamWriter(new FileStream(outputFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite));

                // EditPad is configured to trim trailing whitespace when saving .sql files
                // To avoid extra whitespace being removed when manually editing .sql files, trim trailing whitespace

                foreach (var cachedLine in cachedLines)
                {
                    writer.WriteLine(cachedLine.TrimEnd());
                }

                // Add one blank line to the end of the file (provided the previous line was not empty)
                if (cachedLines.Count == 0 || !string.IsNullOrWhiteSpace(cachedLines.Last()))
                {
                    writer.WriteLine(string.Empty);
                }
            }
        }
    }
}
