using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using PRISM;
using TableNameMapContainer;

namespace DB_Schema_Export_Tool
{
    /// <summary>
    /// SQL Server schema and data exporter
    /// </summary>
    public sealed class DBSchemaExporterSQLServer : DBSchemaExporterBase
    {
        // ReSharper disable once CommentTypo

        // Ignore Spelling: accessor, crlf, currval, dbo, dt, Inline, mtuser, schemas
        // Ignore Spelling: Scripter, setval, smo, Sql, stdin, sysconstraints, sysobjects, syssegments, username, xtype

        // ReSharper disable UnusedMember.Global

        /// <summary>
        /// Pogo server
        /// </summary>
        public const string SQL_SERVER_NAME_DEFAULT = "Pogo";

        /// <summary>
        /// User for connecting to Pogo
        /// </summary>
        public const string SQL_SERVER_USERNAME_DEFAULT = "mtuser";

        /// <summary>
        /// Username for connecting to pogo
        /// </summary>
        public const string SQL_SERVER_PASSWORD_DEFAULT = "mt4fun";

        // ReSharper restore UnusedMember.Global

        /// <summary>
        /// Database definition file prefix
        /// </summary>
        public const string DB_DEFINITION_FILE_PREFIX = "DBDefinition_";

        private Database mCurrentDatabase;

        private readonly SortedSet<string> mSchemaToIgnore;

        private Server mSqlServer;

        /// <summary>
        /// This object is used to determine primary keys on tables when exporting table data
        /// </summary>
        private Scripter mTableDataScripter;

        private bool mTableDataScripterInitialized;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <remarks>
        /// Will auto-connect to the server if options contains a server name
        /// Otherwise, explicitly call ConnectToServer
        /// </remarks>
        /// <param name="options">Options</param>
        public DBSchemaExporterSQLServer(SchemaExportOptions options) : base(options)
        {
            mSchemaToIgnore = new SortedSet<string>(StringComparer.OrdinalIgnoreCase)
            {
                // ReSharper disable StringLiteralTypo
                "db_accessadmin",
                "db_backupoperator",
                "db_datareader",
                "db_datawriter",
                "db_ddladmin",
                "db_denydatareader",
                "db_denydatawriter",
                "db_owner",
                "db_securityadmin",
                "dbo",
                "guest",
                "information_schema",
                "sys"
                // ReSharper restore StringLiteralTypo
            };
            mTableDataScripter = null;
            mTableDataScripterInitialized = false;

            if (string.IsNullOrWhiteSpace(mOptions.ServerName))
                return;

            var success = ConnectToServer();

            if (!success)
            {
                OnWarningEvent("Unable to connect to server " + mOptions.ServerName);
            }
        }

        private void AppendPgExportFooters(TextWriter writer, DataExportWorkingParams dataExportParams)
        {
            if (!dataExportParams.FooterWriteRequired)
            {
                return;
            }

            if (dataExportParams.PgInsertFooters.Count == 0)
            {
                writer.WriteLine(";");
            }
            else
            {
                foreach (var line in dataExportParams.PgInsertFooters)
                {
                    writer.WriteLine(line);
                }
            }

            dataExportParams.FooterWriteRequired = false;
        }

        private void AppendPgExportHeaders(TextWriter writer, DataExportWorkingParams dataExportParams)
        {
            if (dataExportParams.PgInsertHeaders.Count == 0)
            {
                return;
            }

            foreach (var line in dataExportParams.PgInsertHeaders)
            {
                writer.WriteLine(line);
            }
        }

        private void AppendToList(ICollection<string> serverInfo, string propertyName, string propertyValue)
        {
            if (propertyName != null && propertyValue != null)
            {
                serverInfo.Add(propertyName + "=" + propertyValue);
            }
        }

        private void AppendToList(ICollection<string> serverInfo, string propertyName, int propertyValue)
        {
            if (propertyName != null)
            {
                serverInfo.Add(propertyName + "=" + propertyValue);
            }
        }

        private void AppendToList(ICollection<string> serverInfo, string propertyName, bool propertyValue)
        {
            if (propertyName != null)
            {
                serverInfo.Add(propertyName + "=" + propertyValue);
            }
        }

        private void AppendToList(ICollection<string> serverInfo, ConfigProperty configProperty)
        {
            if (configProperty?.DisplayName != null)
            {
                serverInfo.Add(configProperty.DisplayName + "=" + configProperty.ConfigValue);
            }
        }

        /// <summary>
        /// Determines the table names for which data will be exported
        /// </summary>
        /// <param name="currentDatabase">SQL Server database</param>
        /// <param name="tablesForDataExport">Table names that should be auto-selected</param>
        /// <returns>Dictionary where keys are instances of TableDataExportInfo and values are the maximum number of rows to export</returns>
        private Dictionary<TableDataExportInfo, long> AutoSelectTablesForDataExport(
            Database currentDatabase,
            IReadOnlyList<TableDataExportInfo> tablesForDataExport)
        {
            var dtTables = currentDatabase.EnumObjects(DatabaseObjectTypes.Table, SortOrder.Name);

            var tablesInDatabase = new List<TableDataExportInfo>();

            foreach (DataRow item in dtTables.Rows)
            {
                var schemaName = item["Schema"].ToString();

                if (SkipSchema(schemaName))
                    continue;

                tablesInDatabase.Add(new TableDataExportInfo(item["Name"].ToString()));
            }

            var tableText = tablesInDatabase.Count == 1 ? "table" : "tables";
            ShowTrace(string.Format(
                "Found {0} {1} in database {2}",
                tablesInDatabase.Count, tableText, currentDatabase.Name));

            return AutoSelectTablesForDataExport(currentDatabase.Name, tablesInDatabase, tablesForDataExport);
        }

        private IEnumerable<string> CleanSqlScript(IEnumerable<string> scriptInfo)
        {
            return CleanSqlScript(scriptInfo, false, false);
        }

        private List<string> CleanSqlScript(IEnumerable<string> scriptInfo, bool removeAllScriptDateOccurrences, bool removeDuplicateHeaderLine)
        {
            var cleanScriptInfo = new List<string>();

            try
            {
                if (mOptions.ScriptingOptions.IncludeTimestampInScriptFileHeader)
                {
                    return cleanScriptInfo;
                }

                // Look for and remove the timestamp from the first line of the Sql script
                // For example: "Script Date: 08/14/2006 20:14:31" prior to each "******/"
                //
                // If removeAllScriptDateOccurrences = True, searches for all occurrences
                // If removeAllScriptDateOccurrences = False, does not look past the first carriage return of each entry in scriptInfo
                foreach (var item in scriptInfo)
                {
                    var currentLine = item;

                    var indexStart = 0;
                    int finalSearchIndex;

                    if (removeAllScriptDateOccurrences)
                    {
                        finalSearchIndex = currentLine.Length - 1;
                    }
                    else
                    {
                        // Find the first CrLf after the first non-blank line in currentLine
                        // However, if the script starts with several SET statements, we need to skip those lines
                        var objectCommentStartIndex = currentLine.IndexOf(COMMENT_START_TEXT + "Object:", StringComparison.Ordinal);

                        if (currentLine.Trim().StartsWith("SET") && objectCommentStartIndex > 0)
                        {
                            indexStart = objectCommentStartIndex;
                        }

                        do
                        {
                            finalSearchIndex = currentLine.IndexOf("\r\n", indexStart, StringComparison.Ordinal);

                            if (finalSearchIndex == indexStart)
                            {
                                indexStart += 2;
                            }
                        }
                        while (finalSearchIndex >= 0 && finalSearchIndex < indexStart && indexStart < currentLine.Length);

                        if (finalSearchIndex < 0)
                        {
                            finalSearchIndex = currentLine.Length - 1;
                        }
                    }

                    while (true)
                    {
                        var indexStartCurrent = currentLine.IndexOf(COMMENT_SCRIPT_DATE_TEXT, indexStart, StringComparison.Ordinal);

                        if (indexStartCurrent > 0 && indexStartCurrent <= finalSearchIndex)
                        {
                            var indexEndCurrent = currentLine.IndexOf(COMMENT_END_TEXT_SHORT, indexStartCurrent, StringComparison.Ordinal);

                            if (indexEndCurrent > indexStartCurrent && indexEndCurrent <= finalSearchIndex)
                            {
                                currentLine =
                                    currentLine.Substring(0, indexStartCurrent).TrimEnd() + COMMENT_END_TEXT +
                                    currentLine.Substring(indexEndCurrent + COMMENT_END_TEXT_SHORT.Length);
                            }
                        }

                        if (!(removeAllScriptDateOccurrences && indexStartCurrent > 0))
                            break;
                    }

                    if (removeDuplicateHeaderLine)
                    {
                        var firstCrLf = currentLine.IndexOf("\r\n", 0, StringComparison.Ordinal);

                        if (firstCrLf > 0 && firstCrLf < currentLine.Length)
                        {
                            var nextCrLf = currentLine.IndexOf("\r\n", firstCrLf + 1, StringComparison.Ordinal);

                            if (nextCrLf > firstCrLf &&
                                currentLine.Substring(0, firstCrLf) ==
                                currentLine.Substring(firstCrLf + 2, nextCrLf - (firstCrLf - 2)))
                            {
                                currentLine = currentLine.Substring(firstCrLf + 2);
                            }
                        }
                    }

                    cleanScriptInfo.Add(currentLine);
                }

                return cleanScriptInfo;
            }
            catch (Exception ex)
            {
                OnWarningEvent("Error in CleanSqlScript: " + ex.Message);
                return cleanScriptInfo;
            }
        }

        /// <summary>
        /// Connect to the server specified in mOptions
        /// </summary>
        /// <returns>True if successfully connected, false if a problem</returns>
        public override bool ConnectToServer()
        {
            try
            {
                // Initialize the current connection options
                if (mSqlServer == null)
                {
                    ResetServerConnection();
                }
                else if (ValidServerConnection())
                {
                    if (string.Equals(mSqlServer.Name, mOptions.ServerName, StringComparison.OrdinalIgnoreCase))
                    {
                        // Already connected; no need to re-connect
                        ShowTrace("Already connected to " + mSqlServer.Name);
                        return true;
                    }
                }

                // Connect to server mOptions.ServerName
                var connected = LoginToServerWork(out mSqlServer);

                if (!connected && ErrorCode == DBSchemaExportErrorCodes.NoError)
                {
                    SetLocalError(DBSchemaExportErrorCodes.DatabaseConnectionError, "Error logging into the server: " + mOptions.ServerName);
                }

                ShowTrace("Connected to " + mSqlServer.Name);

                return connected;
            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.DatabaseConnectionError, "Error logging into the server: " + mOptions.ServerName, ex);
                mConnectedToServer = false;
                mSqlServer = null;
                mCurrentDatabase = null;
                return false;
            }
        }

        /// <summary>
        /// Export the tables, views, procedures, etc. in the given database
        /// Also export data from tables in tablesForDataExport
        /// </summary>
        /// <param name="databaseName">Database name</param>
        /// <param name="tablesForDataExport">Tables to export data from</param>
        /// <param name="tableDataExportOrder">Table data export order</param>
        /// <param name="databaseNotFound">Output: true if the database is not found</param>
        /// <param name="workingParams">Working parameters</param>
        /// <returns>True if successful, false if an error</returns>
        protected override bool ExportDBObjectsAndTableData(
            string databaseName,
            IReadOnlyList<TableDataExportInfo> tablesForDataExport,
            IReadOnlyList<string> tableDataExportOrder,
            out bool databaseNotFound,
            out WorkingParams workingParams)
        {
            return ExportDBObjectsUsingSMO(mSqlServer, databaseName, tablesForDataExport, tableDataExportOrder, out databaseNotFound, out workingParams);
        }

        /// <summary>
        /// Script the tables, views, function, etc. in the specified database
        /// Also export data from tables in tablesForDataExport
        /// </summary>
        /// <param name="sqlServer">SQL Server Accessor</param>
        /// <param name="databaseName">Database name</param>
        /// <param name="tablesForDataExport">Table names that should be auto-selected</param>
        /// <param name="tableDataExportOrder">List of table names that defines the order that table data should be exported</param>
        /// <param name="databaseNotFound">Output: true if the database does not exist on the server (or is inaccessible)</param>
        /// <param name="workingParams">Working parameters</param>
        /// <returns>True if successful, false if an error</returns>
        private bool ExportDBObjectsUsingSMO(
            Server sqlServer,
            string databaseName,
            IReadOnlyList<TableDataExportInfo> tablesForDataExport,
            IReadOnlyList<string> tableDataExportOrder,
            out bool databaseNotFound,
            out WorkingParams workingParams)
        {
            workingParams = new WorkingParams();

            ScriptingOptions scriptOptions;

            // Keys are information on tables to export
            // Values are the maximum number of rows to export
            Dictionary<TableDataExportInfo, long> tablesToExportData;

            OnDBExportStarting(databaseName);

            try
            {
                scriptOptions = GetDefaultScriptOptions();
                mCurrentDatabase = sqlServer.Databases[databaseName];
                databaseNotFound = false;
            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.DatabaseConnectionError, "Error connecting to database " + databaseName, ex);
                databaseNotFound = true;
                return false;
            }

            var isValid = ValidateOutputDirectoryForDatabaseExport(databaseName, workingParams);

            if (!isValid)
            {
                return false;
            }

            // Populate the TablesToSkip list in the working params
            foreach (var item in tablesForDataExport)
            {
                if (SkipTableForDataExport(item))
                {
                    ShowTrace("Skipping data export from table " + item.SourceTableName);
                    workingParams.TablesToSkip.Add(item.SourceTableName);
                }
            }

            try
            {
                if (mOptions.DisableDataExport)
                {
                    tablesToExportData = new Dictionary<TableDataExportInfo, long>();
                }
                else if (mOptions.ScriptingOptions.AutoSelectTablesForDataExport || mOptions.ExportAllData)
                {
                    tablesToExportData = AutoSelectTablesForDataExport(mCurrentDatabase, tablesForDataExport);
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
                    "Error auto selecting table names for data export from database" + databaseName, ex);

                databaseNotFound = false;
                return false;
            }

            try
            {
                OnDebugEvent("Counting number of objects to export");

                // Count the number of objects that will be exported
                workingParams.CountObjectsOnly = true;

                var successCounting = ExportDBObjectsWork(mCurrentDatabase, scriptOptions, workingParams);

                if (!successCounting)
                    return false;

                workingParams.ProcessCountExpected = workingParams.ProcessCount;

                bool success;

                if (mOptions.PreviewExport)
                {
                    Console.WriteLine();

                    OnStatusEvent("  Found {0} database objects to export", workingParams.ProcessCountExpected);

                    if (tablesToExportData.Count > 0)
                    {
                        OnStatusEvent("  Would export table data for {0} tables", tablesToExportData.Count);
                    }

                    success = true;
                }
                else
                {
                    workingParams.ProcessCountExpected += Math.Max(tablesForDataExport.Count, tablesToExportData.Count);

                    var pluralAddon = workingParams.ProcessCountExpected == 1 ? string.Empty : "s";
                    OnDebugEvent("Scripting {0} object{1}", workingParams.ProcessCountExpected, pluralAddon);

                    workingParams.CountObjectsOnly = false;

                    if (workingParams.ProcessCount > 0)
                    {
                        success = ExportDBObjectsWork(mCurrentDatabase, scriptOptions, workingParams);
                    }
                    else
                    {
                        success = true;
                    }
                }

                // Export data from tables specified by tablesToExportData (will preview the SQL to be used if mOptions.Preview is true)
                var dataSuccess = ExportDBTableData(mCurrentDatabase.Name, tablesToExportData, tableDataExportOrder, workingParams);

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
        /// <param name="currentDatabase">Database object</param>
        /// <param name="scriptOptions">Scripting options</param>
        /// <param name="workingParams">Working parameters</param>
        private bool ExportDBObjectsWork(Database currentDatabase, ScriptingOptions scriptOptions, WorkingParams workingParams)
        {
            // Reset ProcessCount
            workingParams.ProcessCount = 0;

            if (mOptions.NoSchema)
                return true;

            if (mOptions.ScriptingOptions.ExportDBSchemasAndRoles && mOptions.TableNameFilterSet.Count == 0)
            {
                var success = ExportDBSchemasAndRoles(currentDatabase, scriptOptions, workingParams);

                if (!success)
                    return false;

                if (mAbortProcessing)
                {
                    return true;
                }
            }

            if (mOptions.ScriptingOptions.ExportTables)
            {
                var success = ExportDBTables(currentDatabase, scriptOptions, workingParams);

                if (!success)
                    return false;

                if (mAbortProcessing)
                {
                    return true;
                }
            }

            if (mOptions.TableNameFilterSet.Count > 0)
            {
                // Do not export any other objects, since limiting operations to a single table (or small set of tables)
                return true;
            }

            if (mOptions.ScriptingOptions.ExportViews ||
                mOptions.ScriptingOptions.ExportUserDefinedFunctions ||
                mOptions.ScriptingOptions.ExportStoredProcedures ||
                mOptions.ScriptingOptions.ExportSynonyms)
            {
                var success = ExportDBViewsProceduresAndUDFs(currentDatabase, scriptOptions, workingParams);

                if (!success)
                    return false;

                if (mAbortProcessing)
                {
                    return true;
                }
            }

            if (mOptions.ScriptingOptions.ExportUserDefinedDataTypes)
            {
                var success = ExportDBUserDefinedDataTypes(currentDatabase, scriptOptions, workingParams);

                if (!success)
                    return false;

                if (mAbortProcessing)
                {
                    return true;
                }
            }

            if (mOptions.ScriptingOptions.ExportUserDefinedTypes)
            {
                var success = ExportDBUserDefinedTypes(currentDatabase, scriptOptions, workingParams);

                if (!success)
                    return false;
            }

            return true;
        }

        private bool ExportDBSchemasAndRoles(
            Database currentDatabase,
            ScriptingOptions scriptOptions,
            WorkingParams workingParams)
        {
            if (workingParams.CountObjectsOnly)
            {
                ShowTrace("Counting non built-in schemas and roles");

                workingParams.ProcessCount++;

                if (SqlServer2005OrNewer(currentDatabase))
                {
                    for (var index = 0; index < currentDatabase.Schemas.Count; index++)
                    {
                        if (ExportSchema(currentDatabase.Schemas[index]))
                        {
                            workingParams.ProcessCount++;
                        }
                    }
                }

                for (var index = 0; index < currentDatabase.Roles.Count; index++)
                {
                    if (ExportRole(currentDatabase.Roles[index]))
                    {
                        workingParams.ProcessCount++;
                    }
                }

                return true;
            }

            try
            {
                ShowTrace("Exporting non built-in schemas and roles");

                var scriptInfo = CleanSqlScript(StringCollectionToList(currentDatabase.Script(scriptOptions)));
                WriteTextToFile(workingParams.OutputDirectory, DB_DEFINITION_FILE_PREFIX + currentDatabase.Name, scriptInfo);
            }
            catch (Exception ex)
            {
                // User likely doesn't have privilege to script the DB; ignore the error
                OnWarningEvent("Unable to script database {0}: {1}", currentDatabase.Name, ex.Message);
                return false;
            }

            workingParams.ProcessCount++;

            if (SqlServer2005OrNewer(currentDatabase))
            {
                for (var index = 0; index < currentDatabase.Schemas.Count; index++)
                {
                    if (!ExportSchema(currentDatabase.Schemas[index]))
                    {
                        continue;
                    }

                    try
                    {
                        ShowTrace("Exporting schema " + currentDatabase.Schemas[index]);

                        var scriptInfo = CleanSqlScript(StringCollectionToList(currentDatabase.Schemas[index].Script(scriptOptions)));

                        WriteTextToFile(workingParams.OutputDirectory, "Schema_" + currentDatabase.Schemas[index].Name, scriptInfo);
                    }
                    catch (Exception ex)
                    {
                        // User likely doesn't have privilege to script the schema; ignore the error
                        OnWarningEvent("Unable to script schema {0}: {1}", currentDatabase.Schemas[index], ex.Message);
                    }

                    workingParams.ProcessCount++;
                    CheckPauseStatus();

                    if (mAbortProcessing)
                    {
                        OnWarningEvent("Aborted processing");
                        return true;
                    }
                }
            }

            for (var index = 0; index < currentDatabase.Roles.Count; index++)
            {
                if (!ExportRole(currentDatabase.Roles[index]))
                    continue;

                try
                {
                    var scriptInfo = CleanSqlScript(StringCollectionToList(currentDatabase.Roles[index].Script(scriptOptions)));
                    WriteTextToFile(workingParams.OutputDirectory, "Role_" + currentDatabase.Roles[index].Name, scriptInfo);
                }
                catch (Exception ex)
                {
                    // User likely doesn't have privilege to script the role; ignore the error
                    OnWarningEvent("Unable to script role {0}: {1}", currentDatabase.Roles[index], ex.Message);
                }

                workingParams.ProcessCount++;
                CheckPauseStatus();

                if (mAbortProcessing)
                {
                    OnWarningEvent("Aborted processing");
                    return true;
                }
            }

            return true;
        }

        private bool ExportDBTables(Database currentDatabase, ScriptingOptions scriptOptions, WorkingParams workingParams)
        {
            if (workingParams.CountObjectsOnly)
            {
                ShowTrace("Counting Tables");

                var tableCountPassingFilters = 0;

                foreach (Table databaseTable in currentDatabase.Tables)
                {
                    var includeTable = TablePassesFilters(workingParams, databaseTable, false);

                    if (includeTable)
                        tableCountPassingFilters++;
                }

                workingParams.ProcessCount += tableCountPassingFilters;
                return true;
            }

            ShowTrace("Scripting tables");

            var dtStartTime = DateTime.UtcNow;

            // Initialize the scripter and smoObjectArray
            var scripter = new Scripter(mSqlServer)
            {
                Options = scriptOptions
            };

            var tableExportCount = 0;

            foreach (Table databaseTable in currentDatabase.Tables)
            {
                var includeTable = TablePassesFilters(workingParams, databaseTable, true);

                if (!includeTable)
                    continue;

                var subTaskProgress = ComputeSubtaskProgress(workingParams.ProcessCount, workingParams.ProcessCountExpected);
                var percentComplete = ComputeIncrementalProgress(mPercentCompleteStart, mPercentCompleteEnd, subTaskProgress);

                OnProgressUpdate(string.Format("Scripting {0}.{1}.{2}", currentDatabase.Name, databaseTable.Schema, databaseTable.Name), percentComplete);

                var smoObjectArray = new SqlSmoObject[] {
                    databaseTable
                };

                var scriptInfo = CleanSqlScript(StringCollectionToList(scripter.Script(smoObjectArray)));
                WriteTextToFile(workingParams.OutputDirectory, databaseTable.Name, scriptInfo);

                tableExportCount++;
                workingParams.ProcessCount++;
                CheckPauseStatus();

                if (mAbortProcessing)
                {
                    OnWarningEvent("Aborted processing");
                    return true;
                }
            }

            if (mOptions.ShowStats && tableExportCount > 0)
            {
                OnDebugEvent(
                    "Exported {0} tables in {1:0.0} seconds",
                    tableExportCount,
                    DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds);
            }

            return true;
        }

        private bool ExportDBUserDefinedDataTypes(Database currentDatabase, ScriptingOptions scriptOptions, WorkingParams workingParams)
        {
            if (workingParams.CountObjectsOnly)
            {
                var processCount = currentDatabase.UserDefinedDataTypes.Cast<Schema>().Count(schemaItem => MatchesObjectsToProcess(schemaItem.Name));
                workingParams.ProcessCount += processCount;
            }
            else
            {
                ShowTrace("Scripting User Defined Data Types");
                var itemCount = ScriptCollectionOfObjects(
                    currentDatabase.UserDefinedDataTypes,
                    scriptOptions,
                    workingParams.OutputDirectory,
                    currentDatabase.Name,
                    "User Defined Data Type");

                workingParams.ProcessCount += itemCount;
            }

            return true;
        }

        private bool ExportDBUserDefinedTypes(Database currentDatabase, ScriptingOptions scriptOptions, WorkingParams workingParams)
        {
            if (!SqlServer2005OrNewer(currentDatabase))
                return true;

            if (workingParams.CountObjectsOnly)
            {
                var processCount = currentDatabase.UserDefinedTypes.Cast<Schema>().Count(schemaItem => MatchesObjectsToProcess(schemaItem.Name));
                workingParams.ProcessCount += processCount;
            }
            else
            {
                ShowTrace("Scripting User Defined Types");
                var itemCount = ScriptCollectionOfObjects(
                    currentDatabase.UserDefinedTypes,
                    scriptOptions,
                    workingParams.OutputDirectory,
                    currentDatabase.Name,
                    "User Defined Type");
                workingParams.ProcessCount += itemCount;
            }

            return true;
        }

        private bool ExportDBViewsProceduresAndUDFs(Database currentDatabase, ScriptingOptions scriptOptions, WorkingParams workingParams)
        {
            // Option 1) obtain the list of views, stored procedures, and UDFs is to use currentDatabase.EnumObjects
            // However, this only returns the object name, type, and URN, not whether or not it is a system object

            // Option 2) use currentDatabase.Views, currentDatabase.StoredProcedures, etc.
            // However, on Sql Server 2005 this returns many system views and system procedures that we typically don't want to export

            // Option 3) query the sysobjects table and filter on the xtype field
            // Possible values for XType:
            //   C = CHECK constraint
            //   D = Default or DEFAULT constraint
            //   F = FOREIGN KEY constraint
            //   IF = Inline Function
            //   FN = User Defined Function
            //   TF = Table Valued Function
            //   L = Log
            //   P = Stored procedure
            //   PK = PRIMARY KEY constraint (type is K)
            //   RF = Replication filter stored procedure
            //   S = System table
            //   SN = Synonym
            //   TR = Trigger
            //   U = User table
            //   UQ = UNIQUE constraint (type is K)
            //   V = View
            //   X = Extended stored procedure

            /*
                for (var i = 0; i <= 2; i++)
                {
                    var objectType = string.Empty;

                    switch (i)
                    {
                        case 0:
                            // Views
                            if (mOptions.ScriptingOptions.ExportViews )
                            {
                                objectType = " = 'V'";
                            }
                            break;

                        case 1:
                            // Stored procedures
                            if (mOptions.ScriptingOptions.ExportStoredProcedures)
                            {
                                objectType = " = 'P'";
                            }
                            break;

                        case 2: // User defined functions
                            if (mOptions.ScriptingOptions.ExportUserDefinedFunctions)
                            {
                                objectType = " IN ('IF', 'FN', 'TF')";
                            }
                            break;

                        default:
                            // Unknown value for i; skip it
                            objectType = string.Empty;
                            break;
                    }

                    if (objectType.Length > 0)
                    {
                        var sql = "SELECT name FROM sysobjects WHERE xtype " + objectType;

                        if (!mOptions.ScriptingOptions.IncludeSystemObjects)
                        {
                            sql += " AND category = 0";
                        }

                        sql += " ORDER BY Name";

                        var sysObjects = currentDatabase.ExecuteWithResults(sql);

                        if (workingParams.CountObjectsOnly)
                        {
                            workingParams.ProcessCount += sysObjects.Tables[0].Rows.Count;
                        }
                        else
                        {
                            // Initialize the scripter and smoObjects
                            var scripter2 = new Scripter(mSqlServer)
                            {
                                Options = scriptOptions
                            };

                            for each (DataRow currentRow in sysObjects.Tables[0].Rows)
                            {
                                var objectName = currentRow[0].ToString();

                                var subTaskProgress = ComputeSubtaskProgress(workingParams.ProcessCount, workingParams.ProcessCountExpected);
                                var percentComplete = ComputeIncrementalProgress(mPercentCompleteStart, mPercentCompleteEnd, subTaskProgress);

                                OnProgressUpdate(string.Format("Scripting {0}..{1}", currentDatabase.Name, objectName), percentComplete);

                                var smoObjects = i switch
                                {
                                    0 => new SqlSmoObject[] { currentDatabase.Views[objectName] },
                                    1 => new SqlSmoObject[] { currentDatabase.StoredProcedures[objectName] },
                                    2 => new SqlSmoObject[] { currentDatabase.UserDefinedFunctions[objectName] },
                                    _ => null
                                };

                                if (smoObjects != null)
                                {
                                    var scriptInfo = CleanSqlScript(StringCollectionToList(scripter2.Script(smoObjects)));
                                    WriteTextToFile(workingParams.OutputDirectory, objectName, scriptInfo);
                                }

                                workingParams.ProcessCount++;

                                CheckPauseStatus();
                                if (mAbortProcessing)
                                {
                                    OnWarningEvent("Aborted processing");
                                    return true;
                                }
                            }
                        }
                    }
                }
            */

            // Option 4) Query the INFORMATION_SCHEMA views

            // Initialize the scripter and smoObjects
            var scripter = new Scripter(mSqlServer)
            {
                Options = scriptOptions
            };

            for (var objectIterator = 0; objectIterator <= 3; objectIterator++)
            {
                var objectType = "unknown";
                var sql = string.Empty;
                switch (objectIterator)
                {
                    case 0:
                        // Views
                        objectType = "View";

                        if (mOptions.ScriptingOptions.ExportViews)
                        {
                            sql = "SELECT table_schema, table_name FROM INFORMATION_SCHEMA.tables WHERE table_type = 'view' ";

                            if (!mOptions.ScriptingOptions.IncludeSystemObjects)
                            {
                                sql += " AND table_name NOT IN ('sysconstraints', 'syssegments') ";
                            }

                            sql += " ORDER BY table_name";
                        }
                        break;

                    case 1:
                        // Stored procedures
                        objectType = "Stored procedure";

                        if (mOptions.ScriptingOptions.ExportStoredProcedures)
                        {
                            sql = "SELECT routine_schema, routine_name FROM INFORMATION_SCHEMA.routines WHERE routine_type = 'procedure'";

                            if (!mOptions.ScriptingOptions.IncludeSystemObjects)
                            {
                                sql += " AND routine_name NOT LIKE 'dt[_]%' ";
                            }

                            sql += " ORDER BY routine_name";
                        }
                        break;

                    case 2:
                        // User defined functions
                        objectType = "User defined function";

                        if (mOptions.ScriptingOptions.ExportUserDefinedFunctions)
                        {
                            sql = "SELECT routine_schema, routine_name FROM INFORMATION_SCHEMA.routines " +
                                  "WHERE routine_type = 'function' " +
                                  "ORDER BY routine_name";
                        }
                        break;

                    case 3:
                        // Synonyms
                        objectType = "Synonym";

                        if (mOptions.ScriptingOptions.ExportSynonyms)
                        {
                            sql = "SELECT B.name AS SchemaName, A.name FROM sys.synonyms A " +
                                  "INNER JOIN sys.schemas B ON A.schema_id = B.schema_id " +
                                  "ORDER BY A.Name";
                        }
                        break;
                }

                if (string.IsNullOrWhiteSpace(sql))
                    continue;

                // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
                if (workingParams.CountObjectsOnly)
                {
                    ShowTrace(string.Format("Counting {0}s", objectType));
                }
                else
                {
                    ShowTrace(string.Format("Scripting {0}s", objectType));
                }

                var dtStartTime = DateTime.UtcNow;
                var queryResults = currentDatabase.ExecuteWithResults(sql);

                if (workingParams.CountObjectsOnly)
                {
                    // In queryResults.Tables[0].Rows, the first column is the schema name and the second column is the object name

                    var foundItemCount = queryResults.Tables[0].Rows.Cast<DataRow>().Count(currentRow => MatchesObjectsToProcess(currentRow[1].ToString()));

                    workingParams.ProcessCount += foundItemCount;

                    var pluralAddon = foundItemCount == 1 ? string.Empty : "s";
                    ShowTrace(string.Format(" found {0} {1}{2}", foundItemCount, objectType.ToLower(), pluralAddon));
                }
                else
                {
                    foreach (DataRow currentRow in queryResults.Tables[0].Rows)
                    {
                        // The first column is the schema
                        // The second column is the name
                        var objectSchema = currentRow[0].ToString();
                        var objectName = currentRow[1].ToString();

                        if (!MatchesObjectsToProcess(objectName))
                        {
                            continue;
                        }

                        try
                        {
                            var subTaskProgress = ComputeSubtaskProgress(workingParams.ProcessCount, workingParams.ProcessCountExpected);
                            var percentComplete = ComputeIncrementalProgress(mPercentCompleteStart, mPercentCompleteEnd, subTaskProgress);

                            OnProgressUpdate(string.Format("Scripting {0}.{1}.{2}", currentDatabase.Name, objectSchema, objectName), percentComplete);

                            SqlSmoObject smoObjects = objectIterator switch
                            {
                                // Views
                                0 => currentDatabase.Views[objectName, objectSchema],

                                // Stored procedures
                                1 => currentDatabase.StoredProcedures[objectName, objectSchema],

                                // User defined functions
                                2 => currentDatabase.UserDefinedFunctions[objectName, objectSchema],

                                // Synonyms
                                3 => currentDatabase.Synonyms[objectName, objectSchema],

                                _ => null
                            };

                            if (smoObjects != null)
                            {
                                if (workingParams.TablesToSkip.Contains(objectName))
                                {
                                    // Skip this object
                                    OnDebugEvent("Skipping {0} {1} ", objectType, objectName);
                                }
                                else
                                {
                                    var smoObjectArray = new[]
                                    {
                                        smoObjects
                                    };

                                    var scriptInfo = CleanSqlScript(StringCollectionToList(scripter.Script(smoObjectArray)));
                                    WriteTextToFile(workingParams.OutputDirectory, objectName, scriptInfo);
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            HandleScriptingError(workingParams.OutputDirectory, currentDatabase.Name, objectType, objectSchema, objectName, ex);
                        }

                        workingParams.ProcessCount++;
                        CheckPauseStatus();

                        if (mAbortProcessing)
                        {
                            OnWarningEvent("Aborted processing");
                            return true;
                        }
                    }

                    if (mOptions.ShowStats)
                    {
                        OnDebugEvent(
                            "Exported {0} {1}s in {2:0.0} seconds",
                            queryResults.Tables[0].Rows.Count,
                            objectType,
                            DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds);
                    }
                }
            }

            return true;
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
                if (mCurrentDatabase == null || !mCurrentDatabase.Name.Equals(databaseName))
                {
                    try
                    {
                        mCurrentDatabase = mSqlServer.Databases[databaseName];
                        workingParams.PrimaryKeysByTable.Clear();
                    }
                    catch (Exception ex)
                    {
                        SetLocalError(
                            DBSchemaExportErrorCodes.DatabaseConnectionError,
                            "Error connecting to database " + databaseName, ex);

                        return false;
                    }
                }

                if (!mTableDataScripterInitialized)
                {
                    var scriptOptions = GetDefaultScriptOptions();
                    scriptOptions.Indexes = false;
                    scriptOptions.Permissions = false;
                    scriptOptions.Statistics = false;
                    scriptOptions.Triggers = false;

                    // Initialize the scripter and smoObjectArray
                    mTableDataScripter = new Scripter(mSqlServer)
                    {
                        Options = scriptOptions
                    };

                    mTableDataScripterInitialized = true;
                }

                Table databaseTable;

                if (mCurrentDatabase.Tables.Contains(tableInfo.SourceTableName))
                {
                    databaseTable = mCurrentDatabase.Tables[tableInfo.SourceTableName];
                }
                else if (mCurrentDatabase.Tables.Contains(tableInfo.SourceTableName, "dbo"))
                {
                    databaseTable = mCurrentDatabase.Tables[tableInfo.SourceTableName, "dbo"];
                }
                else
                {
                    // Table not found in this database
                    // Do not treat this as an error, so return true
                    return true;
                }

                var subTaskProgress = ComputeSubtaskProgress(workingParams.ProcessCount, workingParams.ProcessCountExpected);
                var percentComplete = ComputeIncrementalProgress(mPercentCompleteStart, mPercentCompleteEnd, subTaskProgress);

                OnProgressUpdate("Exporting data from " + tableInfo.SourceTableName, percentComplete);

                string nullValueFlag;

                if (mOptions.PgDumpTableData && !tableInfo.UsePgInsert)
                {
                    nullValueFlag = @"\N";
                }
                else if (mOptions.ScriptingOptions.SaveDataAsInsertIntoStatements || tableInfo.UsePgInsert)
                {
                    nullValueFlag = "null";
                }
                else
                {
                    nullValueFlag = string.Empty;
                }

                var dataExportParams = new DataExportWorkingParams(tableInfo.UsePgInsert, nullValueFlag);

                // See if any of the columns in the table is an identity column
                var identityColumnIndex = -1;

                var index = -1;

                foreach (Column currentColumn in databaseTable.Columns)
                {
                    index++;

                    if (!currentColumn.Identity)
                        continue;

                    dataExportParams.IdentityColumnFound = true;
                    dataExportParams.IdentityColumnName = currentColumn.Name;
                    identityColumnIndex = index;
                    break;
                }

                // Export the data from databaseTable, possibly limiting the number of rows to export
                var sql = "SELECT";

                if (maxRowsToExport > 0)
                {
                    sql += " TOP " + maxRowsToExport;
                }

                dataExportParams.SourceTableNameWithSchema = string.Format("{0}.{1}",
                    PossiblyQuoteName(databaseTable.Schema),
                    PossiblyQuoteName(databaseTable.Name));

                sql += " * FROM " + dataExportParams.SourceTableNameWithSchema;

                if (tableInfo.FilterByDate)
                {
                    sql += string.Format(" WHERE [{0}] >= '{1:yyyy-MM-dd}'", tableInfo.DateColumnName, tableInfo.MinimumDate);
                }

                if (SkipTableForDataExport(tableInfo))
                {
                    // Skip this table
                    OnStatusEvent(
                        "Skipping data export from table {0} in database {1}",
                        dataExportParams.SourceTableNameWithSchema, databaseName);

                    return true;
                }

                if (mOptions.PreviewExport)
                {
                    OnStatusEvent("Preview querying database {0} with {1}", databaseName, sql);
                    return true;
                }

                var queryResults = mCurrentDatabase.ExecuteWithResults(sql);

                var quoteWithSquareBrackets = !mOptions.PgDumpTableData && !dataExportParams.PgInsertEnabled;

                // Get the table name, with schema, in the form schema.table_name
                // The schema and table name will be quoted if necessary
                dataExportParams.TargetTableNameWithSchema = GetTargetTableName(
                    dataExportParams, tableInfo,
                    quoteWithSquareBrackets, false);

                if (string.IsNullOrWhiteSpace(dataExportParams.TargetTableNameWithSchema))
                {
                    // Skip this table
                    OnStatusEvent(
                        "Could not determine the target table name for table {0} in database {1}",
                        dataExportParams.SourceTableNameWithSchema, databaseName);

                    return false;
                }

                if (mOptions.ScriptingOptions.SaveDataAsInsertIntoStatements || tableInfo.UsePgInsert)
                {
                    // Skip any computed columns since exporting data using PostgreSQL compatible INSERT INTO statements

                    var mapInfoToUse = mOptions.ColumnMapForDataExport.TryGetValue(databaseTable.Name, out var currentColumnMapInfo)
                        ? currentColumnMapInfo
                        : new ColumnMapInfo(databaseTable.Name);

                    var skippedColumn = false;

                    foreach (Column currentColumn in databaseTable.Columns)
                    {
                        if (currentColumn.Computed)
                        {
                            OnStatusEvent("Skipping computed column {0} on table {1}", currentColumn.Name, databaseTable.Name);
                        }
                        else if (currentColumn.DataType.SqlDataType.Equals(SqlDataType.Timestamp))
                        {
                            OnStatusEvent("Skipping Timestamp column {0} on table {1}", currentColumn.Name, databaseTable.Name);
                        }
                        else
                        {
                            continue;
                        }

                        mapInfoToUse.SkipColumn(currentColumn.Name);
                        skippedColumn = true;
                    }

                    if (skippedColumn && !mOptions.ColumnMapForDataExport.ContainsKey(databaseTable.Name))
                    {
                        mOptions.ColumnMapForDataExport.Add(databaseTable.Name, mapInfoToUse);
                    }
                }

                // Get the table name, with schema, in the form schema.table_name
                // The schema and table name will always be quoted
                dataExportParams.QuotedTargetTableNameWithSchema = GetQuotedTargetTableName(dataExportParams, tableInfo, quoteWithSquareBrackets);

                var headerRows = new List<string>();

                var header = COMMENT_START_TEXT + "Object:  Table " + dataExportParams.QuotedTargetTableNameWithSchema;

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
                else
                {
                    headerRows.Add(COMMENT_START_TEXT + "RowCount: " + databaseTable.RowCount + COMMENT_END_TEXT);
                }

                var columnCount = queryResults.Tables[0].Columns.Count;

                for (var columnIndex = 0; columnIndex < columnCount; columnIndex++)
                {
                    var currentColumn = queryResults.Tables[0].Columns[columnIndex];

                    var currentColumnName = currentColumn.ColumnName;
                    var currentColumnType = currentColumn.DataType;

                    dataExportParams.ColumnInfoByType.Add(new KeyValuePair<string, Type>(currentColumnName, currentColumnType));
                }

                var columnMapInfo = ConvertDataTableColumnInfo(databaseTable.Name, quoteWithSquareBrackets, dataExportParams);

                var tableDataOutputFile = GetTableDataOutputFile(tableInfo, dataExportParams, workingParams, out var relativeFilePath);

                if (tableDataOutputFile == null)
                {
                    // Skip this table (a warning message should have already been shown)
                    return false;
                }

                var insertIntoLine = ExportDBTableDataInit(tableInfo, columnMapInfo, dataExportParams, headerRows, workingParams, queryResults, tableDataOutputFile, relativeFilePath, out var dataExportError);

                if (dataExportError)
                    return false;

                if (mOptions.ScriptPgLoadCommands)
                {
                    workingParams.AddDataLoadScriptFile(relativeFilePath);
                }

                using var writer = new StreamWriter(new FileStream(tableDataOutputFile.FullName, FileMode.Create, FileAccess.Write, FileShare.ReadWrite));

                if (mOptions.PgDumpTableData)
                {
                    writer.NewLine = "\n";
                }

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
                        var primaryKeyColumnName = identityColumnIndex >= 0
                            ? dataExportParams.ColumnNamesAndTypes[identityColumnIndex].Key
                            : dataExportParams.IdentityColumnName;

                        // Make an educated guess of the sequence name, for example
                        // mc.t_mgr_types_mt_type_id_seq
                        var sequenceName = GetIdentityColumnSequenceName(dataExportParams.TargetTableNameWithSchema, primaryKeyColumnName);

                        writer.WriteLine();
                        writer.WriteLine("-- Set the sequence's current value to the maximum current ID");
                        writer.WriteLine("SELECT setval('{0}', (SELECT MAX({1}) FROM {2}));",
                            sequenceName, PossiblyQuoteName(primaryKeyColumnName, false), dataExportParams.TargetTableNameWithSchema);
                        writer.WriteLine();
                        writer.WriteLine("-- Preview the ID that will be assigned to the next item");
                        writer.WriteLine("SELECT currval('{0}');", sequenceName);
                    }
                }
                else if (dataExportParams.IdentityColumnFound && mOptions.ScriptingOptions.SaveDataAsInsertIntoStatements && !mOptions.PgDumpTableData)
                {
                    writer.WriteLine("SET IDENTITY_INSERT " + dataExportParams.QuotedTargetTableNameWithSchema + " OFF");
                }

                // Note that the following method will set the session_replication_role to "origin" if PgInsertEnabled is true
                PossiblyEnableTriggers(dataExportParams, mOptions, writer);

                return true;
            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.GeneralError, "Error in ExportDBTableData for table " + tableInfo.SourceTableName, ex);
                return false;
            }
        }

        private string GetIdentityColumnSequenceName(string targetTableNameWithSchema, string primaryKeyColumnName)
        {
            var tableName = GetNameWithoutSchema(targetTableNameWithSchema).Replace("\"", string.Empty);

            var sequenceNameWithoutSchema = string.Format("{0}_{1}_seq", tableName, primaryKeyColumnName);

            if (sequenceNameWithoutSchema.Length <= DBSchemaExporterPostgreSQL.MAX_OBJECT_NAME_LENGTH)
            {
                return string.Format("{0}_{1}_seq", targetTableNameWithSchema.Replace("\"", string.Empty), primaryKeyColumnName);
            }

            // Shorten the table name, trimming any trailing underscores
            var tableNameLengthToUse = DBSchemaExporterPostgreSQL.MAX_OBJECT_NAME_LENGTH - primaryKeyColumnName.Length - 4;

            var truncatedTableName = tableName.Substring(0, tableNameLengthToUse).TrimEnd('_');

            var truncatedTableNameWithSchema = targetTableNameWithSchema.Replace(tableName, truncatedTableName).Replace("\"", string.Empty);

            return string.Format("{0}_{1}_seq", truncatedTableNameWithSchema, primaryKeyColumnName);
        }

        /// <summary>
        /// Generate SQL to delete extra rows from the target table
        /// </summary>
        /// <param name="dataExportParams">Data export parameters</param>
        /// <param name="queryResults">Query results</param>
        private void ExportDBTableDataDeleteExtraRows(DataExportWorkingParams dataExportParams, DataSet queryResults)
        {
            // If just one column in the table, use:
            //   DELETE FROM t_target_table
            //   WHERE NOT id in (1, 2, 3);

            // If multiple columns, use:
            //   DELETE FROM t_target_table
            //   WHERE NOT (
            //       id = 1 and value = 'Item A' Or
            //       id = 2 and value = 'Item B' Or
            //       id = 3 and value = 'Item C');

            var sql = new StringBuilder();

            sql.AppendFormat("DELETE FROM {0}", dataExportParams.QuotedTargetTableNameWithSchema).AppendLine();

            var columnCount = queryResults.Tables[0].Columns.Count;
            var pgInsertEnabled = dataExportParams.PgInsertEnabled;

            var filterValues = new StringBuilder();

            if (columnCount == 1)
            {
                sql.AppendFormat("WHERE NOT {0} IN (", dataExportParams.ColumnNameByIndex[0]);
            }
            else
            {
                sql.AppendLine("WHERE NOT (");
            }

            var rowNumber = 0;

            foreach (DataRow currentRow in queryResults.Tables[0].Rows)
            {
                filterValues.Clear();
                rowNumber++;

                var columnValues = GetColumnValues(columnCount, currentRow);

                for (var columnIndex = 0; columnIndex < columnCount; columnIndex++)
                {
                    if (columnCount > 1 && dataExportParams.ColumnNamesAndTypes[columnIndex].Value == DataColumnTypeConstants.SkipColumn)
                    {
                        // Skip this column
                        continue;
                    }

                    var formattedValue = FormatValueForInsert(dataExportParams.ColumnNamesAndTypes, columnValues, columnIndex, pgInsertEnabled);

                    if (columnCount == 1)
                    {
                        // Using the form "WHERE NOT id in (1, 2, 3);"
                        // Append to the list

                        if (rowNumber > 1)
                            sql.Append(", ");

                        sql.Append(columnValues[columnIndex] == null ? "NULL" : formattedValue);

                        continue;
                    }

                    if (filterValues.Length == 0)
                    {
                        if (rowNumber > 1)
                        {
                            // Add OR to the previous filter value
                            sql.AppendLine(" OR");
                        }

                        sql.Append("    ");
                    }

                    var quotedColumnName = dataExportParams.ColumnNameByIndex[columnIndex];

                    if (columnIndex > 0 && filterValues.Length > 0)
                    {
                        filterValues.Append(" AND ");
                    }

                    if (columnValues[columnIndex] == null)
                    {
                        filterValues.AppendFormat("{0} IS NULL", quotedColumnName);
                        continue;
                    }

                    filterValues.AppendFormat("{0} = {1}", quotedColumnName, formattedValue);
                }

                if (columnCount > 1)
                {
                    sql.Append(filterValues);
                }
            }

            sql.AppendLine(");");

            dataExportParams.PgInsertHeaders.Add(sql.ToString());
        }

        private IEnumerable<string> ExportDBTableDataGetSetStatements(
            TableNameInfo tableInfo,
            ColumnMapInfo columnMapInfo,
            DataExportWorkingParams dataExportParams,
            string primaryKeyColumnList,
            bool deleteExtrasThenAddNew)
        {
            var setStatements = new List<string>();

            for (var columnIndex = 0; columnIndex < dataExportParams.ColumnNamesAndTypes.Count; columnIndex++)
            {
                var currentColumn = dataExportParams.ColumnInfoByType[columnIndex];

                var currentColumnName = currentColumn.Key;

                var dataColumnType = DataColumnTypeConstants.Numeric;
                var targetColumnName = GetTargetColumnName(columnMapInfo, currentColumnName, ref dataColumnType);

                if (dataColumnType == DataColumnTypeConstants.SkipColumn)
                    continue;

                if (tableInfo.PrimaryKeyColumns.Contains(targetColumnName))
                {
                    // Skip this column
                    continue;
                }

                var optionalComma = columnIndex < dataExportParams.ColumnNamesAndTypes.Count - 1 ? "," : string.Empty;

                if (setStatements.Count == 0)
                {
                    setStatements.Add(string.Format("ON CONFLICT ({0})", PossiblyQuoteNameList(primaryKeyColumnList, false)));
                    setStatements.Add("DO UPDATE SET");
                }

                if (!deleteExtrasThenAddNew)
                {
                    setStatements.Add(string.Format("  {0} = EXCLUDED.{0}{1}", PossiblyQuoteName(targetColumnName, false), optionalComma));
                }
            }

            if (deleteExtrasThenAddNew)
            {
                if (setStatements.Count > 0)
                {
                    throw new Exception("Logic bug in ExportDBTableDataInit: deleteExtrasThenAddNew is true, but setStatements is not empty");
                }

                setStatements.Add(string.Format("ON CONFLICT ({0})", PossiblyQuoteNameList(primaryKeyColumnList, false)));
                setStatements.Add("DO NOTHING");
            }
            else
            {
                // Assure that the last line in setStatements does not end with a comma
                // This would be the case if the final column for the table is a <skip> column or an identity column
                var mostRecentLine = setStatements.LastOrDefault() ?? string.Empty;

                if (mostRecentLine.EndsWith(","))
                {
                    setStatements[setStatements.Count - 1] = mostRecentLine.Substring(0, mostRecentLine.Length - 1);
                }
            }

            return setStatements;
        }

        /// <summary>
        /// Construct the header rows then return the INSERT INTO line to use for each block of data
        /// </summary>
        /// <remarks>
        /// If DeleteExtraRowsBeforeImport is true and the table has a primary key, will create a file for deleting extra data rows in a target table
        /// </remarks>
        /// <param name="tableInfo">Table info</param>
        /// <param name="columnMapInfo">Class tracking the source and target column names for the table</param>
        /// <param name="dataExportParams">Data export parameters</param>
        /// <param name="headerRows">Header rows</param>
        /// <param name="workingParams">Working parameters</param>
        /// <param name="queryResults">Query results</param>
        /// <param name="tableDataOutputFile">Table data output file</param>
        /// <param name="tableDataOutputFileRelativePath">Table data output file relative path</param>
        /// <param name="dataExportError">Output: true if an error was encountered, otherwise false</param>
        /// <returns>Insert Into line to use when SaveDataAsInsertIntoStatements is true and PgInsertEnabled is false; otherwise, an empty string</returns>
        private string ExportDBTableDataInit(
            TableDataExportInfo tableInfo,
            ColumnMapInfo columnMapInfo,
            DataExportWorkingParams dataExportParams,
            List<string> headerRows,
            WorkingParams workingParams,
            DataSet queryResults,
            FileInfo tableDataOutputFile,
            string tableDataOutputFileRelativePath,
            out bool dataExportError)
        {
            string insertIntoLine;
            dataExportError = false;

            var dataRowCount = queryResults.Tables[0].Rows.Count;

            if (dataExportParams.PgInsertEnabled)
            {
                // Exporting data from SQL Server and using insert commands formatted as PostgreSQL compatible
                // INSERT INTO statements using the ON CONFLICT (key_column) DO UPDATE SET syntax

                var primaryKeyColumnList = ResolvePrimaryKeys(dataExportParams, workingParams, tableInfo, columnMapInfo);

                bool deleteExtrasThenAddNew;
                bool deleteExtrasUsingPrimaryKey;
                bool useTruncateTable;

                if (tableInfo.PrimaryKeyColumns.Count == dataExportParams.ColumnNamesAndTypes.Count)
                {
                    if (dataRowCount < 100 && tableInfo.PrimaryKeyColumns.Count <= 2)
                    {
                        // Every column in the table is part of the primary key

                        // For smaller tables, we can delete extra rows then add new rows

                        // This is preferable to TRUNCATE TABLE since the table might be referenced via a foreign key
                        // and PostgreSQL will not allow a table to be truncated when it is the target of a foreign key reference

                        deleteExtrasThenAddNew = true;
                        deleteExtrasUsingPrimaryKey = false;
                        useTruncateTable = false;

                        Console.WriteLine();

                        var message = string.Format(
                            "Every column in table {0} is part of the primary key; since this is a small table, will delete extra rows from the target table, then add missing rows",
                            dataExportParams.QuotedTargetTableNameWithSchema);

                        OnStatusEvent(message);
                    }
                    else
                    {
                        deleteExtrasThenAddNew = false;
                        deleteExtrasUsingPrimaryKey = false;
                        useTruncateTable = true;

                        Console.WriteLine();

                        var message = string.Format(
                            "Every column in table {0} is part of the primary key; will use TRUNCATE TABLE instead of ON CONFLICT ... DO UPDATE",
                            dataExportParams.QuotedTargetTableNameWithSchema);

                        OnStatusEvent(message);

                        workingParams.AddWarningMessage(message);
                    }
                }
                else if (tableInfo.PrimaryKeyColumns.Count == 0)
                {
                    deleteExtrasThenAddNew = false;
                    deleteExtrasUsingPrimaryKey = false;
                    useTruncateTable = true;

                    var warningMessage = string.Format(
                        "Table {0} does not have a primary key; will use TRUNCATE TABLE since ON CONFLICT ... DO UPDATE is not possible",
                        dataExportParams.QuotedTargetTableNameWithSchema);

                    OnWarningEvent(warningMessage);

                    workingParams.AddWarningMessage(warningMessage);
                }
                else
                {
                    deleteExtrasThenAddNew = false;
                    deleteExtrasUsingPrimaryKey = mOptions.DeleteExtraRowsBeforeImport;
                    useTruncateTable = false;
                }

                if (dataRowCount > 0 && !tableInfo.FilterByDate && mOptions.MaxRows <= 0)
                {
                    if (deleteExtrasThenAddNew)
                    {
                        ExportDBTableDataDeleteExtraRows(dataExportParams, queryResults);
                    }
                    else if (deleteExtrasUsingPrimaryKey)
                    {
                        var deleteExtrasScripter = new DeleteExtraDataRowsScripter(this, mOptions);

                        RegisterEvents(deleteExtrasScripter);

                        var success = deleteExtrasScripter.DeleteExtraRowsInTargetTable(
                            tableInfo, columnMapInfo,
                            dataExportParams, workingParams,
                            queryResults,
                            tableDataOutputFile, tableDataOutputFileRelativePath);

                        if (!success)
                        {
                            dataExportError = true;
                            return string.Empty;
                        }
                    }
                }

                if (useTruncateTable)
                {
                    var truncateTableCommand = string.Format("TRUNCATE TABLE {0};", dataExportParams.QuotedTargetTableNameWithSchema);

                    if (dataRowCount > 0)
                    {
                        headerRows.Add(truncateTableCommand);
                    }
                    else
                    {
                        dataExportParams.PgInsertHeaders.Add(string.Empty);
                        dataExportParams.PgInsertHeaders.Add(
                            "-- The following is commented out because the source table is empty (or all rows were filtered out by a date filter)");
                        dataExportParams.PgInsertHeaders.Add("-- " + truncateTableCommand);
                    }

                    dataExportParams.PgInsertHeaders.Add(string.Empty);
                }

                // Note that column names in HeaderRowValues should already be properly quoted

                var insertCommand = string.Format("INSERT INTO {0} ({1})",
                    dataExportParams.QuotedTargetTableNameWithSchema,
                    dataExportParams.HeaderRowValues);

                if (dataRowCount == 0)
                {
                    dataExportParams.PgInsertHeaders.Add(string.Empty);
                    dataExportParams.PgInsertHeaders.Add(
                        "-- The following is commented out because the source table is empty (or all rows were filtered out by a date filter)");
                    dataExportParams.PgInsertHeaders.Add("-- " + insertCommand);

                    headerRows.AddRange(dataExportParams.PgInsertHeaders);
                    return string.Empty;
                }

                dataExportParams.PgInsertHeaders.Add(insertCommand);
                dataExportParams.PgInsertHeaders.Add("OVERRIDING SYSTEM VALUE");
                dataExportParams.PgInsertHeaders.Add("VALUES");

                headerRows.AddRange(dataExportParams.PgInsertHeaders);

                dataExportParams.ColSepChar = ',';
                insertIntoLine = string.Empty;

                if (primaryKeyColumnList.Length == 0)
                    return string.Empty;

                if (useTruncateTable)
                {
                    return string.Empty;
                }

                var setStatements = ExportDBTableDataGetSetStatements(tableInfo, columnMapInfo, dataExportParams, primaryKeyColumnList, deleteExtrasThenAddNew);

                dataExportParams.PgInsertFooters.AddRange(setStatements);
                dataExportParams.PgInsertFooters.Add(";");
            }
            else if (mOptions.ScriptingOptions.SaveDataAsInsertIntoStatements && !mOptions.PgDumpTableData)
            {
                // Export as SQL Server compatible INSERT INTO statements

                if (dataExportParams.IdentityColumnFound)
                {
                    insertIntoLine = string.Format(
                        "INSERT INTO {0} ({1}) VALUES (",
                        dataExportParams.QuotedTargetTableNameWithSchema,
                        dataExportParams.HeaderRowValues);

                    headerRows.Add("SET IDENTITY_INSERT " + dataExportParams.QuotedTargetTableNameWithSchema + " ON");
                }
                else
                {
                    // Identity column not present; no need to explicitly list the column names
                    insertIntoLine = string.Format(
                        "INSERT INTO {0} VALUES (",
                        dataExportParams.QuotedTargetTableNameWithSchema);

                    headerRows.Add(COMMENT_START_TEXT + "Columns: " + dataExportParams.HeaderRowValues + COMMENT_END_TEXT);
                }

                dataExportParams.ColSepChar = ',';
            }
            else if (mOptions.PgDumpTableData)
            {
                // Use the T-SQL COPY command to export data from a SQL Server database

                // ReSharper disable once StringLiteralTypo
                var copyCommand = string.Format("COPY {0} ({1}) from stdin;",
                    dataExportParams.TargetTableNameWithSchema, dataExportParams.HeaderRowValues);

                headerRows.Add(copyCommand);
                dataExportParams.ColSepChar = '\t';
                insertIntoLine = string.Empty;
            }
            else
            {
                // Export data as a tab-delimited table
                headerRows.Add(dataExportParams.HeaderRowValues.ToString());
                dataExportParams.ColSepChar = '\t';
                insertIntoLine = string.Empty;
            }

            return insertIntoLine;
        }

        /// <summary>
        /// Step through the results in queryResults
        /// Append lines to the output file
        /// </summary>
        /// <param name="writer">Text file writer</param>
        /// <param name="queryResults">Query results dataset</param>
        /// <param name="insertIntoLine">Insert Into (Column1, Column2, Column3) line (used when SaveDataAsInsertIntoStatements is true and PgInsertEnabled is false)</param>
        /// <param name="dataExportParams">Data export parameters</param>
        private void ExportDBTableDataWork(
            TextWriter writer,
            DataSet queryResults,
            string insertIntoLine,
            DataExportWorkingParams dataExportParams)
        {
            var columnCount = queryResults.Tables[0].Columns.Count;

            var delimitedRowValues = new StringBuilder();

            var commandAndLfRequired = false;
            var startingNewChunk = false;

            var rowCountWritten = 0;

            var usingPgInsert = dataExportParams.PgInsertEnabled;
            dataExportParams.FooterWriteRequired = false;

            foreach (DataRow currentRow in queryResults.Tables[0].Rows)
            {
                delimitedRowValues.Clear();

                if (mOptions.ScriptingOptions.SaveDataAsInsertIntoStatements && !mOptions.PgDumpTableData && !usingPgInsert)
                {
                    delimitedRowValues.Append(insertIntoLine);
                }

                var columnValues = GetColumnValues(columnCount, currentRow);

                if (commandAndLfRequired)
                {
                    // Add a comma and a line feed
                    writer.WriteLine(",");
                }

                if (startingNewChunk)
                {
                    AppendPgExportHeaders(writer, dataExportParams);
                    startingNewChunk = false;
                }

                ExportDBTableDataRow(writer, dataExportParams, delimitedRowValues, columnCount, columnValues);

                if (usingPgInsert)
                    commandAndLfRequired = true;

                rowCountWritten++;

                if (mOptions.PgInsertChunkSize > 0 && rowCountWritten > mOptions.PgInsertChunkSize)
                {
                    dataExportParams.FooterWriteRequired = true;
                    writer.WriteLine();
                    AppendPgExportFooters(writer, dataExportParams);
                    rowCountWritten = 0;
                    commandAndLfRequired = false;
                    startingNewChunk = true;
                }
            }

            // Note that the calling method will call AppendPgExportFooters()

            if (commandAndLfRequired)
            {
                // Add a line feed (but no comma)
                writer.WriteLine();
                dataExportParams.FooterWriteRequired = true;
            }

            if (mOptions.PgDumpTableData && !usingPgInsert)
            {
                // Append a line with just backslash-period (\.)
                // This represents "End of data"
                writer.WriteLine(@"\.");

                // Append a semicolon to finalize the DDL
                writer.WriteLine(";");
            }

            // // Read method #2: Use a SqlDataReader to read row-by-row
            //using (var reader = mSqlServer.ConnectionContext.ExecuteReader(sql))
            //{
            //    if (reader.HasRows)
            //    {
            //        while (reader.Read())
            //        {
            //            if (reader.FieldCount > 0)
            //            {
            //                delimitedRowValues.Append(reader.GetValue(0)));
            //            }

            //            for (var columnIndex = 1; columnIndex < reader.FieldCount; columnIndex++)
            //            {
            //                delimitedRowValues.Append("\t" + reader.GetValue(columnIndex));
            //            }
            //        }
            //    }
            //}

        }

        private bool ExportRole(DatabaseRole databaseRole)
        {
            try
            {
                if (databaseRole.IsFixedRole)
                {
                    return false;
                }

                return !databaseRole.Name.Equals("public", StringComparison.OrdinalIgnoreCase);
            }
            catch
            {
                return false;
            }
        }

        private bool ExportSchema(NamedSmoObject databaseSchema)
        {
            try
            {
                return !mSchemaToIgnore.Contains(databaseSchema.Name);
            }
            catch
            {
                return false;
            }
        }

        private void ExportSQLServerConfiguration(Server sqlServer, ScriptingOptions scriptOptions, FileSystemInfo outputDirectoryPathCurrentServer)
        {
            OnProgressUpdate("Exporting server configuration info", 0);

            // Save SQL Server info to ServerInformation.ini
            ExportSQLServerInfoToIni(sqlServer, outputDirectoryPathCurrentServer);

            // Save the SQL Server configuration info to ServerConfiguration.ini
            ExportSQLServerConfigToIni(sqlServer, outputDirectoryPathCurrentServer);

            // Save the Mail settings to file ServerMail.sql
            // Can only do this for SQL Server 2005 or newer
            if (SqlServer2005OrNewer(sqlServer))
            {
                try
                {
                    var outputFile = new FileInfo(Path.Combine(
                        outputDirectoryPathCurrentServer.FullName, "ServerMail.sql"));

                    ShowTrace("Scripting database mail options to " + outputFile.FullName);

                    var mailInfo = StringCollectionToList(sqlServer.Mail.Script(scriptOptions));
                    var cleanedMailInfo = CleanSqlScript(mailInfo, false, false);
                    WriteTextToFile(outputFile, cleanedMailInfo);
                }
                catch (Exception ex)
                {
                    OnWarningEvent(
                        "Error scripting the SQL Server database mail settings; most likely the user is not a server administrator: {0}",
                        ex.Message);
                }
            }

            try
            {
                // Save the Registry Settings to file ServerRegistrySettings.sql

                var outputFile = new FileInfo(Path.Combine(
                    outputDirectoryPathCurrentServer.FullName, "ServerRegistrySettings.sql"));

                ShowTrace("Exporting registry settings to " + outputFile.FullName);

                var serverSettings = StringCollectionToList(sqlServer.Settings.Script(scriptOptions));
                var cleanedServerSettings = CleanSqlScript(serverSettings, false, false);
                cleanedServerSettings.Insert(0, "-- Registry Settings for " + sqlServer.Name);

                WriteTextToFile(outputFile, cleanedServerSettings, false);
            }
            catch (Exception ex)
            {
                OnWarningEvent(
                    "Error scripting the SQL Server registry settings; most likely the user is not a server admin: {0}",
                    ex.Message);
            }
        }

        private void ExportSQLServerInfoToIni(Server sqlServer, FileSystemInfo outputDirectoryPathCurrentServer)
        {
            var outputFile = new FileInfo(Path.Combine(
                outputDirectoryPathCurrentServer.FullName, "ServerInformation.ini"));

            ShowTrace("Exporting server information to " + outputFile.FullName);

            var serverInfo = new List<string>
                {
                    "[Server Information for " + sqlServer.Name + "]"
                };

            AppendToList(serverInfo, "BuildClrVersion", sqlServer.Information.BuildClrVersionString);
            AppendToList(serverInfo, "Collation", sqlServer.Information.Collation);
            AppendToList(serverInfo, "Edition", sqlServer.Information.Edition);
            AppendToList(serverInfo, "ErrorLogPath", sqlServer.Information.ErrorLogPath);
            AppendToList(serverInfo, "IsCaseSensitive", sqlServer.Information.IsCaseSensitive);
            AppendToList(serverInfo, "IsClustered", sqlServer.Information.IsClustered);
            AppendToList(serverInfo, "IsFullTextInstalled", sqlServer.Information.IsFullTextInstalled);
            AppendToList(serverInfo, "IsSingleUser", sqlServer.Information.IsSingleUser);
            AppendToList(serverInfo, "Language", sqlServer.Information.Language);
            AppendToList(serverInfo, "MasterDBLogPath", sqlServer.Information.MasterDBLogPath);
            AppendToList(serverInfo, "MasterDBPath", sqlServer.Information.MasterDBPath);
            AppendToList(serverInfo, "MaxPrecision", sqlServer.Information.MaxPrecision);
            AppendToList(serverInfo, "NetName", sqlServer.Information.NetName);
            AppendToList(serverInfo, "OSVersion", sqlServer.Information.OSVersion);
            AppendToList(serverInfo, "PhysicalMemory", sqlServer.Information.PhysicalMemory);
            AppendToList(serverInfo, "Platform", sqlServer.Information.Platform);
            AppendToList(serverInfo, "Processors", sqlServer.Information.Processors);
            AppendToList(serverInfo, "Product", sqlServer.Information.Product);
            AppendToList(serverInfo, "ProductLevel", sqlServer.Information.ProductLevel);
            AppendToList(serverInfo, "RootDirectory", sqlServer.Information.RootDirectory);
            AppendToList(serverInfo, "VersionString", sqlServer.Information.VersionString);

            WriteTextToFile(outputFile, serverInfo, false);
        }

        private void ExportSQLServerConfigToIni(Server sqlServer, FileSystemInfo outputDirectoryPathCurrentServer)
        {
            var outputFile = new FileInfo(Path.Combine(
                outputDirectoryPathCurrentServer.FullName, "ServerConfiguration.ini"));

            ShowTrace("Exporting server configuration to " + outputFile.FullName);

            var serverConfig = new List<string>
                {
                    "[Server Configuration for " + sqlServer.Name + "]"
                };

            AppendToList(serverConfig, sqlServer.Configuration.AdHocDistributedQueriesEnabled);
            AppendToList(serverConfig, sqlServer.Configuration.Affinity64IOMask);
            AppendToList(serverConfig, sqlServer.Configuration.Affinity64Mask);
            AppendToList(serverConfig, sqlServer.Configuration.AffinityIOMask);
            AppendToList(serverConfig, sqlServer.Configuration.AffinityMask);
            AppendToList(serverConfig, sqlServer.Configuration.AgentXPsEnabled);
            AppendToList(serverConfig, sqlServer.Configuration.AllowUpdates);
            AppendToList(serverConfig, sqlServer.Configuration.BlockedProcessThreshold);
            AppendToList(serverConfig, sqlServer.Configuration.C2AuditMode);
            AppendToList(serverConfig, sqlServer.Configuration.CommonCriteriaComplianceEnabled);
            AppendToList(serverConfig, sqlServer.Configuration.CostThresholdForParallelism);
            AppendToList(serverConfig, sqlServer.Configuration.CrossDBOwnershipChaining);
            AppendToList(serverConfig, sqlServer.Configuration.CursorThreshold);
            AppendToList(serverConfig, sqlServer.Configuration.DatabaseMailEnabled);
            AppendToList(serverConfig, sqlServer.Configuration.DefaultBackupCompression);
            AppendToList(serverConfig, sqlServer.Configuration.DefaultFullTextLanguage);
            AppendToList(serverConfig, sqlServer.Configuration.DefaultLanguage);
            AppendToList(serverConfig, sqlServer.Configuration.DefaultTraceEnabled);
            AppendToList(serverConfig, sqlServer.Configuration.DisallowResultsFromTriggers);
            AppendToList(serverConfig, sqlServer.Configuration.ExtensibleKeyManagementEnabled);
            AppendToList(serverConfig, sqlServer.Configuration.FilestreamAccessLevel);
            AppendToList(serverConfig, sqlServer.Configuration.FillFactor);
            AppendToList(serverConfig, sqlServer.Configuration.IndexCreateMemory);
            AppendToList(serverConfig, sqlServer.Configuration.InDoubtTransactionResolution);
            AppendToList(serverConfig, sqlServer.Configuration.IsSqlClrEnabled);
            AppendToList(serverConfig, sqlServer.Configuration.LightweightPooling);
            AppendToList(serverConfig, sqlServer.Configuration.Locks);
            AppendToList(serverConfig, sqlServer.Configuration.MaxDegreeOfParallelism);
            AppendToList(serverConfig, sqlServer.Configuration.MaxServerMemory);
            AppendToList(serverConfig, sqlServer.Configuration.MaxWorkerThreads);
            AppendToList(serverConfig, sqlServer.Configuration.MediaRetention);
            AppendToList(serverConfig, sqlServer.Configuration.MinMemoryPerQuery);
            AppendToList(serverConfig, sqlServer.Configuration.MinServerMemory);
            AppendToList(serverConfig, sqlServer.Configuration.NestedTriggers);
            AppendToList(serverConfig, sqlServer.Configuration.NetworkPacketSize);
            AppendToList(serverConfig, sqlServer.Configuration.OleAutomationProceduresEnabled);
            AppendToList(serverConfig, sqlServer.Configuration.OpenObjects);
            AppendToList(serverConfig, sqlServer.Configuration.OptimizeAdhocWorkloads);
            AppendToList(serverConfig, sqlServer.Configuration.PrecomputeRank);
            AppendToList(serverConfig, sqlServer.Configuration.PriorityBoost);
            AppendToList(serverConfig, sqlServer.Configuration.ProtocolHandlerTimeout);
            AppendToList(serverConfig, sqlServer.Configuration.QueryGovernorCostLimit);
            AppendToList(serverConfig, sqlServer.Configuration.QueryWait);
            AppendToList(serverConfig, sqlServer.Configuration.RecoveryInterval);
            AppendToList(serverConfig, sqlServer.Configuration.RemoteAccess);
            AppendToList(serverConfig, sqlServer.Configuration.RemoteDacConnectionsEnabled);
            AppendToList(serverConfig, sqlServer.Configuration.RemoteLoginTimeout);
            AppendToList(serverConfig, sqlServer.Configuration.RemoteProcTrans);
            AppendToList(serverConfig, sqlServer.Configuration.RemoteQueryTimeout);
            AppendToList(serverConfig, sqlServer.Configuration.ReplicationMaxTextSize);
            AppendToList(serverConfig, sqlServer.Configuration.ReplicationXPsEnabled);
            AppendToList(serverConfig, sqlServer.Configuration.ScanForStartupProcedures);
            AppendToList(serverConfig, sqlServer.Configuration.ServerTriggerRecursionEnabled);
            AppendToList(serverConfig, sqlServer.Configuration.SetWorkingSetSize);
            AppendToList(serverConfig, sqlServer.Configuration.ShowAdvancedOptions);
            AppendToList(serverConfig, sqlServer.Configuration.SmoAndDmoXPsEnabled);
            AppendToList(serverConfig, sqlServer.Configuration.SqlMailXPsEnabled);
            AppendToList(serverConfig, sqlServer.Configuration.TransformNoiseWords);
            AppendToList(serverConfig, sqlServer.Configuration.TwoDigitYearCutoff);
            AppendToList(serverConfig, sqlServer.Configuration.UserConnections);
            AppendToList(serverConfig, sqlServer.Configuration.UserOptions);
            AppendToList(serverConfig, sqlServer.Configuration.XPCmdShellEnabled);

            WriteTextToFile(outputFile, serverConfig, false);
        }

        /// <summary>
        /// Export the server logins
        /// </summary>
        /// <param name="sqlServer">SQL Server instance</param>
        /// <param name="scriptOptions">Scripting options</param>
        /// <param name="outputDirectoryPathCurrentServer">Output directory path for the current server</param>
        private void ExportSQLServerLogins(Server sqlServer, ScriptingOptions scriptOptions, FileSystemInfo outputDirectoryPathCurrentServer)
        {
            // Do not include a Try block in this Function; let the calling function handle errors

            OnProgressUpdate("Exporting SQL Server logins", 0);

            for (var index = 0; index < sqlServer.Logins.Count; index++)
            {
                var currentLogin = sqlServer.Logins[index].Name;
                OnDebugEvent("Exporting login " + currentLogin);

                var scriptInfo = CleanSqlScript(StringCollectionToList(sqlServer.Logins[index].Script(scriptOptions)), true, true);
                var success = WriteTextToFile(outputDirectoryPathCurrentServer, "Login_" + currentLogin, scriptInfo);

                CheckPauseStatus();

                if (mAbortProcessing)
                {
                    OnWarningEvent("Aborted processing");
                    break;
                }

                if (success)
                {
                    OnDebugEvent("Processing completed for login " + currentLogin);
                }
                else
                {
                    SetLocalError(
                        DBSchemaExportErrorCodes.GeneralError,
                        string.Format("Processing failed for server {0}; login {1}", mOptions.ServerName, currentLogin));
                }
            }
        }

        /// <summary>
        /// Export the SQL Server Agent jobs
        /// </summary>
        /// <param name="sqlServer">SQL Server instance</param>
        /// <param name="scriptOptions">Scripting options</param>
        /// <param name="outputDirectoryPathCurrentServer">Output directory path for the current server</param>
        private void ExportSQLServerAgentJobs(Server sqlServer, ScriptingOptions scriptOptions, FileSystemInfo outputDirectoryPathCurrentServer)
        {
            // Do not include a Try block in this Function; let the calling function handle errors

            OnProgressUpdate("Exporting SQL Server Agent jobs", 0);

            for (var index = 0; index < sqlServer.JobServer.Jobs.Count; index++)
            {
                var currentJob = sqlServer.JobServer.Jobs[index].Name;
                OnDebugEvent("Exporting job " + currentJob);

                var scriptInfo = CleanSqlScript(StringCollectionToList(sqlServer.JobServer.Jobs[index].Script(scriptOptions)), true, true);
                var success = WriteTextToFile(outputDirectoryPathCurrentServer, "AgentJob_" + currentJob, scriptInfo);

                CheckPauseStatus();

                if (mAbortProcessing)
                {
                    OnWarningEvent("Aborted processing");
                    break;
                }

                if (success)
                {
                    OnDebugEvent("Processing completed for job " + currentJob);
                }
                else
                {
                    SetLocalError(
                        DBSchemaExportErrorCodes.GeneralError,
                        string.Format("Processing failed for server {0}; job {1}", mOptions.ServerName, currentJob));
                }
            }
        }

        private static object[] GetColumnValues(int columnCount, DataRow currentRow)
        {
            var columnValues = new object[columnCount];

            for (var columnIndex = 0; columnIndex < columnCount; columnIndex++)
            {
                if (currentRow.IsNull(columnIndex))
                {
                    columnValues[columnIndex] = null;
                }
                else
                {
                    columnValues[columnIndex] = currentRow[columnIndex];
                }
            }

            return columnValues;
        }

        /// <summary>
        /// Get default SQL Server scripting options
        /// </summary>
        private ScriptingOptions GetDefaultScriptOptions()
        {
            return new ScriptingOptions
            {
                // scriptOptions.Bindings = True
                Default = true,
                DriAll = true,
                IncludeHeaders = true,
                IncludeDatabaseContext = false,
                IncludeIfNotExists = false,
                Indexes = true,
                NoCommandTerminator = false,
                Permissions = true,
                SchemaQualify = true,
                Statistics = true,
                Triggers = true,
                ToFileOnly = false,
                WithDependencies = false
            };
        }

        /// <summary>
        /// Retrieve a list of tables in the given database
        /// </summary>
        /// <param name="databaseName">Database to query</param>
        /// <param name="includeTableRowCounts">When true, determines the row count in each table</param>
        /// <param name="includeSystemObjects">When true, also returns system tables</param>
        /// <returns>Dictionary where keys are instances of TableDataExportInfo and values are row counts (if includeTableRowCounts = true)</returns>
        public override Dictionary<TableDataExportInfo, long> GetDatabaseTables(string databaseName, bool includeTableRowCounts, bool includeSystemObjects)
        {
            return GetSqlServerDatabaseTables(databaseName, includeTableRowCounts, includeSystemObjects);
        }

        /// <summary>
        /// Query the database to obtain the primary key information for every table
        /// Store in workingParams.PrimaryKeysByTable
        /// </summary>
        /// <param name="workingParams">Working parameters</param>
        /// <returns>True if successful, false if an error</returns>
        public bool GetPrimaryKeyInfoFromDatabase(WorkingParams workingParams)
        {
            try
            {
                // Query to view primary key columns, by table, listed by table name, then ordinal position

                // SELECT U.Table_Name,
                //        U.Column_Name,
                //        C.Ordinal_Position,
                //        Row_Number() OVER ( PARTITION BY U.Table_Name ORDER BY C.Ordinal_Position ) AS Column_Order,
                //        C.Data_Type
                // FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS T
                //      INNER JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE U
                //        ON U.Constraint_Name = T.Constraint_Name
                //      INNER JOIN INFORMATION_SCHEMA.COLUMNS C
                //        ON U.TABLE_NAME = C.TABLE_NAME AND
                //           U.Column_Name = C.COLUMN_NAME
                // WHERE U.Table_Catalog = 'dms5' AND
                //       T.CONSTRAINT_TYPE = 'PRIMARY KEY'
                //       AND U.TABLE_NAME In (                        -- Optional filter to only show tables with multi-column based primary keys
                //            SELECT DISTINCT Table_Name
                //            FROM ( SELECT U.Table_Name,
                //                          Row_Number() OVER ( PARTITION BY U.Table_Name ORDER BY C.Ordinal_Position ) AS
                //                            Column_Order
                //                   FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS T
                //                        INNER JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE U
                //                          ON U.Constraint_Name = T.Constraint_Name
                //                        INNER JOIN INFORMATION_SCHEMA.COLUMNS C
                //                          ON U.TABLE_NAME = C.TABLE_NAME AND
                //                             U.Column_Name = C.COLUMN_NAME
                //                   WHERE U.Table_Catalog = 'dms5' AND
                //                         T.CONSTRAINT_TYPE = 'PRIMARY KEY' ) LookupQ
                //            WHERE LookupQ.Column_Order > 1
                //        )
                // ORDER BY U.Table_Name, 4

                workingParams.PrimaryKeysByTable.Clear();

                var sql = string.Format(
                    "SELECT U.Table_Name, " +
                    "       U.Column_Name, " +
                    "       C.Ordinal_Position " +
                    "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS T " +
                    "     INNER JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE U " +
                    "       ON U.Constraint_Name = T.Constraint_Name " +
                    "     INNER JOIN INFORMATION_SCHEMA.COLUMNS C " +
                    "       ON U.TABLE_NAME = C.TABLE_NAME AND " +
                    "          U.Column_Name = C.COLUMN_NAME " +
                    "WHERE U.Table_Catalog = '{0}' AND " +
                    "      T.CONSTRAINT_TYPE = 'PRIMARY KEY' " +
                    "ORDER BY U.Table_Name, C.Ordinal_Position, U.Column_Name ",
                    mCurrentDatabase.Name);

                var queryResults = mCurrentDatabase.ExecuteWithResults(sql);

                foreach (DataRow currentRow in queryResults.Tables[0].Rows)
                {
                    var tableName = currentRow[0].ToString();
                    var columnName = currentRow[1].ToString();

                    // var ordinalPosition = currentRow[2].ToString();

                    if (workingParams.PrimaryKeysByTable.TryGetValue(tableName, out var existingPrimaryKeyColumns))
                    {
                        existingPrimaryKeyColumns.Add(columnName);
                        continue;
                    }

                    var primaryKeyColumns = new List<string>
                        {
                            columnName
                        };

                    workingParams.PrimaryKeysByTable.Add(tableName, primaryKeyColumns);
                }

                workingParams.PrimaryKeysRetrieved = true;
                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent(string.Format("Error obtaining primary key columns for tables in database {0} on the current server", mCurrentDatabase), ex);
                return false;
            }
        }

        /// <summary>
        /// Use mTableDataScripter to look for primary key column(s) for the table
        /// </summary>
        /// <param name="tableInfo">Table info</param>
        /// <returns>Comma separated list of primary key columns</returns>
        private IEnumerable<string> GetPrimaryKeysForTableViaScripter(TableNameInfo tableInfo)
        {
            if (!mCurrentDatabase.Tables.Contains(tableInfo.SourceTableName))
                return new SortedSet<string>();

            var databaseTable = mCurrentDatabase.Tables[tableInfo.SourceTableName];

            var smoObjectArray = new SqlSmoObject[] {
                databaseTable
            };

            var rowSplitter = new Regex(@"\r\n", RegexOptions.Compiled);

            foreach (var item in mTableDataScripter.Script(smoObjectArray))
            {
                if (!item.StartsWith("CREATE TABLE"))
                    continue;

                var createTableDDL = rowSplitter.Split(item).ToList();

                return GetPrimaryKeysForTableViaDDL(createTableDDL);
            }

            OnWarningEvent("Table DDL scripted for {0} did not have a line that starts with CREATE TABLE", tableInfo.SourceTableName);

            return new SortedSet<string>();
        }

        /// <summary>
        /// Examine the DDL for a table to look for primary key column(s) for the table
        /// </summary>
        /// <param name="createTableDDL">DDL to create the table</param>
        /// <returns>Comma separated list of primary key columns</returns>
        public static SortedSet<string> GetPrimaryKeysForTableViaDDL(List<string> createTableDDL)
        {
            var primaryKeyColumns = new SortedSet<string>();

            var columnNameMatcher = new Regex(@"\[(?<ColumnName>.+)\]", RegexOptions.Compiled);

            // Look for the constraint line, which will be followed by the primary key names, surrounded by an open and close parentheses
            // CONSTRAINT [PK_T_ParamValue] PRIMARY KEY CLUSTERED
            // (
            // 	[Entry_ID] ASC
            // )

            var insidePrimaryKey = false;

            foreach (var currentLine in createTableDDL)
            {
                if (currentLine.Trim().StartsWith("CONSTRAINT") && currentLine.Contains("PRIMARY KEY"))
                {
                    insidePrimaryKey = true;
                    continue;
                }

                if (!insidePrimaryKey)
                    continue;

                if (currentLine.Trim().StartsWith("("))
                {
                    // Skip this line
                    continue;
                }

                if (currentLine.Trim().StartsWith(")"))
                {
                    // End of the primary key section
                    insidePrimaryKey = false;
                    continue;
                }

                // Extract the column name from the square brackets
                var columnMatch = columnNameMatcher.Match(currentLine);

                if (columnMatch.Success)
                {
                    primaryKeyColumns.Add(columnMatch.Groups["ColumnName"].Value);
                }
            }

            return primaryKeyColumns;
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

                var databaseNames = GetServerDatabasesCurrentConnection();

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
            return GetSqlServerDatabasesWork();
        }

        /// <summary>
        /// Get the list of databases from the current server
        /// </summary>
        private IEnumerable<string> GetSqlServerDatabasesWork()
        {
            var databaseNames = new List<string>();
            var databases = mSqlServer.Databases;

            if (databases.Count <= 0)
                return databaseNames;

            for (var index = 0; index < databases.Count; index++)
            {
                databaseNames.Add(databases[index].Name);
            }

            databaseNames.Sort();

            var databaseText = databaseNames.Count == 1 ? "database" : "databases";
            ShowTrace(string.Format(
                "Found {0} {1} on server {2}", databaseNames.Count, databaseText, mSqlServer.Name));

            if (mOptions.Trace)
            {
                Console.WriteLine();
            }

            return databaseNames;
        }

        /// <summary>
        /// Lookup the table names in the specified database, optionally also determining table row counts
        /// </summary>
        /// <param name="databaseName">Database to query</param>
        /// <param name="includeTableRowCounts">When true, determines the row count in each table</param>
        /// <param name="includeSystemObjects">When true, also returns system tables</param>
        /// <returns>Dictionary where keys are instances of TableDataExportInfo and values are row counts (if includeTableRowCounts = true)</returns>
        public Dictionary<TableDataExportInfo, long> GetSqlServerDatabaseTables(string databaseName, bool includeTableRowCounts, bool includeSystemObjects)
        {
            // Keys are table names; values are row counts
            var databaseTableInfo = new Dictionary<TableDataExportInfo, long>();

            try
            {
                InitializeLocalVariables(true);

                if (!ConnectToServer())
                {
                    return databaseTableInfo;
                }

                if (!ValidServerConnection())
                {
                    OnWarningEvent("Not connected to a server; cannot retrieve the list of the tables in database " + databaseName);
                    return databaseTableInfo;
                }

                if (string.IsNullOrWhiteSpace(databaseName))
                {
                    OnWarningEvent("Empty database name sent to GetSqlServerDatabaseTables");
                    return databaseTableInfo;
                }

                if (!mSqlServer.Databases.Contains(databaseName))
                {
                    OnWarningEvent("Database {0} not found on sever {1}", databaseName, mCurrentServerInfo.ServerName);
                    return databaseTableInfo;
                }

                // Get the list of tables in this database
                OnDebugEvent("Obtaining list of tables in database {0} on server {1}", databaseName, mCurrentServerInfo.ServerName);

                // Connect to the database
                var currentDatabase = mSqlServer.Databases[databaseName];

                var databaseTables = currentDatabase.Tables;

                if (databaseTables.Count <= 0)
                    return databaseTableInfo;

                for (var index = 0; index < databaseTables.Count; index++)
                {
                    if (!includeSystemObjects && databaseTables[index].IsSystemObject)
                        continue;

                    var tableRowCount = includeTableRowCounts ? databaseTables[index].RowCount : 0;

                    databaseTableInfo.Add(new TableDataExportInfo(databaseTables[index].Name), tableRowCount);

                    var percentComplete = ComputeSubtaskProgress(index, databaseTables.Count);
                    OnProgressUpdate("Reading database tables", percentComplete);

                    if (mAbortProcessing)
                    {
                        OnWarningEvent("Aborted processing");
                        break;
                    }
                }

                OnProgressComplete();
                OnStatusEvent("Found {0} tables", databaseTableInfo.Count);

                return databaseTableInfo;
            }
            catch (Exception ex)
            {
                SetLocalError(
                    DBSchemaExportErrorCodes.DatabaseConnectionError,
                    string.Format("Error obtaining list of tables in database {0} on the current server", databaseName), ex);

                return new Dictionary<TableDataExportInfo, long>();
            }
        }

        private string GetTargetPrimaryKeyColumnNames(
            ColumnMapInfo columnMapInfo,
            IEnumerable<string> sourceColumnNames,
            out List<string> targetPrimaryKeyColumns)
        {
            targetPrimaryKeyColumns = new List<string>();

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var columnName in sourceColumnNames)
            {
                var targetColumnName = GetTargetColumnName(columnMapInfo, columnName);

                if (targetColumnName.Equals(NameMapReader.SKIP_FLAG))
                {
                    // Table T_Job_Steps in DMS_Capture has computed column "step" which is a synonym for primary key column Step_Number
                    // Column Step_Number gets renamed to "step" when converting to PostgreSQL

                    // For T_Job_Steps, names in in sourceColumnNames are "job" and "step" when this for loop is reached

                    // GetTargetColumnName() will thus convert "step" to "<skip>" since it is a computed column in the source table
                    // (and method SkipColumn() in ColumnMapInfo will have set the new name to "<skip>")

                    // Therefore, use the original column name as the primary key column
                    targetPrimaryKeyColumns.Add(columnName);
                    continue;
                }

                targetPrimaryKeyColumns.Add(targetColumnName);
            }

            return string.Join(",", targetPrimaryKeyColumns);
        }

        private void HandleScriptingError(FileSystemInfo outputDirectory, string databaseName, string objectType, string objectSchema, string objectName, Exception ex)
        {
            string scriptComment;
            string exceptionMessage;
            bool encryptedObject;

            if (ex is FailedOperationException && ex.InnerException is PropertyCannotBeRetrievedException)
            {
                scriptComment = "This object's content is encrypted and thus cannot be scripted";
                exceptionMessage = string.Empty;
                encryptedObject = true;
            }
            else
            {
                scriptComment = "Error scripting this object:";
                exceptionMessage = ex.Message;
                encryptedObject = false;
            }

            // This list is used to convert the object type name to a string where each word is capitalized and there are no spaces
            // For example, "StoredProcedure" or "UserDefinedFunction"
            var capitalizedObjectTypeWords = new List<string>();

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var item in objectType.Split(' '))
            {
                capitalizedObjectTypeWords.Add(char.ToUpper(item[0]) + item.Substring(1).ToLower());
            }

            var scriptInfo = new List<string>
            {
                string.Format("/****** Object:  {0} [{1}].[{2}] ******/", string.Concat(capitalizedObjectTypeWords), objectSchema, objectName),
                string.Empty,
                "--",
                "-- " + scriptComment,
            };

            if (!string.IsNullOrWhiteSpace(exceptionMessage))
            {
                scriptInfo.Add("-- " + exceptionMessage);
            }

            scriptInfo.Add("--");
            scriptInfo.Add(string.Empty);
            scriptInfo.Add("GO");

            WriteTextToFile(outputDirectory, objectName, scriptInfo, false);

            if (encryptedObject)
            {
                OnWarningEvent("Cannot script {0} {1} from database {2} because the object is encrypted", objectType.ToLower(), objectName, databaseName);
            }
            else
            {
                OnWarningEvent("Error scripting {0} {1} from database {2}: {3}", objectType.ToLower(), objectName, databaseName, ex.Message);
            }
        }

        /// <summary>
        /// Login to the server
        /// </summary>
        /// <param name="sqlServer">SQL Server instance</param>
        /// <returns>True if success, otherwise false</returns>
        private bool LoginToServerWork(out Server sqlServer)
        {
            try
            {
                var connectionInfo = new SqlConnectionInfo(mOptions.ServerName)
                {
                    ApplicationName = "DBSchemaExportTool",
                    ConnectionTimeout = 10
                };

                if (string.IsNullOrWhiteSpace(mOptions.DBUser))
                {
                    connectionInfo.UseIntegratedSecurity = true;
                }
                else
                {
                    connectionInfo.UseIntegratedSecurity = false;
                    connectionInfo.UserName = mOptions.DBUser;
                    connectionInfo.Password = mOptions.DBUserPassword;
                }

                var sqlServerConnection = new ServerConnection(connectionInfo);
                sqlServer = new Server(sqlServerConnection);

                // If no error occurred, set .Connected = True and duplicate the connection info
                mConnectedToServer = true;
                mCurrentServerInfo.UpdateInfo(mOptions, string.Empty);

                return true;
            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.DatabaseConnectionError, "Error logging into server " + mOptions.ServerName, ex);
                sqlServer = null;
                return false;
            }
        }

        internal static void PossiblyDisableTriggers(DataExportWorkingParams dataExportParams, SchemaExportOptions options, TextWriter writer)
        {
            if (dataExportParams.PgInsertEnabled)
            {
                // Set the replication role to replicate to disable triggers

                // By default, triggers will fire when the replication role is "origin" (the default) or "local", but will not fire if the replication role is "replica"
                // Triggers configured as ENABLE REPLICA will only fire if the session is in "replica" mode
                // Triggers configured as ENABLE ALWAYS will fire regardless of the current replication role

                writer.WriteLine("-- Setting the replication role to 'replica' will disable normal triggers on tables");
                writer.WriteLine("SET session_replication_role = replica;");
                writer.WriteLine();
            }
            else if (options.IncludeDisableTriggerCommands)
            {
                writer.WriteLine("ALTER TABLE {0} DISABLE TRIGGER ALL;", dataExportParams.TargetTableNameWithSchema);
                writer.WriteLine();
            }
        }

        internal static void PossiblyEnableTriggers(DataExportWorkingParams dataExportParams, SchemaExportOptions options, TextWriter writer)
        {
            if (dataExportParams.PgInsertEnabled)
            {
                writer.WriteLine();
                writer.WriteLine("SET session_replication_role = origin;");
                return;
            }

            if (!options.IncludeDisableTriggerCommands)
                return;

            writer.WriteLine();
            writer.WriteLine("ALTER TABLE {0} ENABLE TRIGGER ALL;", dataExportParams.TargetTableNameWithSchema);
            writer.WriteLine();
        }

        /// <summary>
        /// If objectName contains characters other than A-Z, a-z, 0-9, or an underscore, surround the name with square brackets
        /// </summary>
        /// <remarks>Also quote if the name is a keyword</remarks>
        /// <param name="objectName">Object name</param>
        private string PossiblyQuoteName(string objectName)
        {
            return PossiblyQuoteName(objectName, true);
        }

        /// <summary>
        /// Determine the primary key column (or columns) for a table
        /// </summary>
        /// <param name="dataExportParams">Data export parameters</param>
        /// <param name="workingParams">Working parameters</param>
        /// <param name="tableInfo">Table info</param>
        /// <param name="columnMapInfo">Class tracking the source and target column names for the table</param>
        /// <returns>Comma separated list of primary key column names (using target column names)</returns>
        private string ResolvePrimaryKeys(
            DataExportWorkingParams dataExportParams,
            WorkingParams workingParams,
            TableNameInfo tableInfo,
            ColumnMapInfo columnMapInfo)
        {
            if (!workingParams.PrimaryKeysRetrieved)
            {
                GetPrimaryKeyInfoFromDatabase(workingParams);
            }

            if (tableInfo.PrimaryKeyColumns.Count > 0)
            {
                return GetTargetPrimaryKeyColumnNames(columnMapInfo, tableInfo.PrimaryKeyColumns, out _);
            }

            if (workingParams.PrimaryKeysByTable.TryGetValue(tableInfo.SourceTableName, out var primaryKeys))
            {
                foreach (var item in primaryKeys)
                {
                    var targetColumnName = GetTargetColumnName(columnMapInfo, item);

                    if (targetColumnName.Equals(NameMapReader.SKIP_FLAG, StringComparison.OrdinalIgnoreCase))
                    {
                        OnWarningEvent("Ignoring primary key column {0} since it is flagged to be skipped", item);
                        continue;
                    }

                    tableInfo.AddPrimaryKeyColumn(targetColumnName);
                }
            }

            if (tableInfo.PrimaryKeyColumns.Count == 0 && dataExportParams.IdentityColumnFound)
            {
                var targetIdentityColumn = GetTargetColumnName(columnMapInfo, dataExportParams.IdentityColumnName);

                tableInfo.AddPrimaryKeyColumn(targetIdentityColumn);
            }
            else if (tableInfo.PrimaryKeyColumns.Count == 0 && mTableDataScripter != null)
            {
                var primaryKeyColumns = GetPrimaryKeysForTableViaScripter(tableInfo);

                GetTargetPrimaryKeyColumnNames(columnMapInfo, primaryKeyColumns, out var targetPrimaryKeyColumns);

                foreach (var targetColumnName in targetPrimaryKeyColumns)
                {
                    tableInfo.AddPrimaryKeyColumn(targetColumnName);
                }
            }

            if (tableInfo.PrimaryKeyColumns.Count > 0)
            {
                return GetTargetPrimaryKeyColumnNames(columnMapInfo, tableInfo.PrimaryKeyColumns, out _);
            }

            return string.Empty;
        }

        /// <summary>
        /// Script a list of objects
        /// </summary>
        /// <param name="schemaCollection">IEnumerable of type SchemaCollectionBase</param>
        /// <param name="scriptOptions">Script options</param>
        /// <param name="outputDirectory">Output directory</param>
        /// <param name="databaseName">Database name</param>
        /// <param name="objectType">Object type</param>
        /// <returns>The number of objects scripted</returns>
        private int ScriptCollectionOfObjects(
            IEnumerable schemaCollection,
            ScriptingOptions scriptOptions,
            FileSystemInfo outputDirectory,
            string databaseName,
            string objectType)
        {
            var processCount = 0;

            foreach (Schema schemaItem in schemaCollection)
            {
                if (!MatchesObjectsToProcess(schemaItem.Name))
                {
                    continue;
                }

                try
                {
                    var scriptInfo = CleanSqlScript(StringCollectionToList(schemaItem.Script(scriptOptions)));

                    WriteTextToFile(outputDirectory, schemaItem.Name, scriptInfo);
                }
                catch (Exception ex)
                {
                    HandleScriptingError(outputDirectory, databaseName, objectType, "dbo", schemaItem.Name, ex);
                }

                processCount++;
                CheckPauseStatus();

                if (mAbortProcessing)
                {
                    OnWarningEvent("Aborted processing");
                    return processCount;
                }
            }

            return processCount;
        }

        /// <summary>
        /// Export SQL Server settings and SQL Server Agent jobs
        /// </summary>
        protected override bool ScriptServerObjects()
        {
            return ScriptServerObjects(mSqlServer);
        }

        /// <summary>
        /// Export SQL Server settings and SQL Server Agent jobs
        /// </summary>
        /// <param name="sqlServer">SQL Server instance</param>
        private bool ScriptServerObjects(Server sqlServer)
        {
            try
            {
                var serverInfoOutputDirectory = GetServerInfoOutputDirectory(sqlServer.Name);

                if (serverInfoOutputDirectory == null)
                {
                    return false;
                }

                OnStatusEvent("Exporting Server objects to: " + PathUtils.CompactPathString(serverInfoOutputDirectory.FullName));

                var scriptOptions = GetDefaultScriptOptions();

                // Export the overall server configuration and options (this is quite fast, so we won't increment mProgressStep after this)
                ExportSQLServerConfiguration(sqlServer, scriptOptions, serverInfoOutputDirectory);

                if (mAbortProcessing)
                {
                    return true;
                }

                ExportSQLServerLogins(sqlServer, scriptOptions, serverInfoOutputDirectory);

                if (mAbortProcessing)
                {
                    return true;
                }

                try
                {
                    ExportSQLServerAgentJobs(sqlServer, scriptOptions, serverInfoOutputDirectory);
                }
                catch (Exception ex) when (ex.InnerException?.Message.IndexOf("permission was denied", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    OnWarningEvent("Error scripting the SQL Server Agent jobs; most likely the user is not a server administrator");
                    OnWarningEvent(ex.InnerException.Message);
                }

                return true;
            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.GeneralError, "Error scripting objects for server " + sqlServer.Name, ex);
                return false;
            }
        }

        private bool SqlServer2005OrNewer(Database currentDatabase)
        {
            return SqlServer2005OrNewer(currentDatabase.Parent);
        }

        private bool SqlServer2005OrNewer(Server currentServer)
        {
            return currentServer.Information.Version.Major >= 9;
        }

        private IEnumerable<string> StringCollectionToList(StringCollection items)
        {
            var scriptInfo = new List<string>();

            foreach (var item in items)
            {
                scriptInfo.Add(item);
            }

            return scriptInfo;
        }

        private bool TablePassesFilters(WorkingParams workingParams, Table databaseTable, bool showTraceMessages)
        {
            // ReSharper disable once StringLiteralTypo
            const string SYNC_OBJ_TABLE_PREFIX = "syncobj_0x";

            if (!mOptions.ScriptingOptions.IncludeSystemObjects)
            {
                if (databaseTable.IsSystemObject)
                {
                    return false;
                }

                if (databaseTable.Name.Length >= SYNC_OBJ_TABLE_PREFIX.Length &&
                    databaseTable.Name.Substring(0, SYNC_OBJ_TABLE_PREFIX.Length).Equals(SYNC_OBJ_TABLE_PREFIX, StringComparison.OrdinalIgnoreCase))
                {
                    return false;
                }
            }

            if (workingParams.TablesToSkip.Contains(databaseTable.Name))
            {
                if (showTraceMessages)
                {
                    ShowTrace(string.Format(
                        "Skipping schema export from table {0} since defined in TablesToSkip", databaseTable.Name));
                }

                return false;
            }

            if (mOptions.TableNameFilterSet.Count > 0 && !mOptions.TableNameFilterSet.Contains(databaseTable.Name))
            {
                if (showTraceMessages)
                {
                    ShowTrace(string.Format(
                        "Skipping schema export from table {0} since not in the list specified by TableFilterList", databaseTable.Name));
                }

                return false;
            }

            if (SkipSchema(databaseTable.Schema))
            {
                if (showTraceMessages)
                {
                    ShowTrace(string.Format(
                        "Skipping schema export from table {0}.{1} due to a schema name filter", databaseTable.Schema, databaseTable.Name));
                }

                return false;
            }

            if (!MatchesObjectsToProcess(databaseTable.Name))
            {
                if (showTraceMessages)
                {
                    ShowTrace(string.Format(
                        "Skipping schema export from table {0}.{1} since it does not match {2}",
                        databaseTable.Schema,
                        databaseTable.Name,
                        mObjectNameMatchers.Count == 1 ? mOptions.ObjectNameFilter : "any of the object name filters"));
                }

                return false;
            }

            return true;
        }

        /// <summary>
        /// Return true if we have a valid server connection
        /// </summary>
        protected override bool ValidServerConnection()
        {
            return mConnectedToServer && mSqlServer?.State == SqlSmoState.Existing;
        }
    }
}
