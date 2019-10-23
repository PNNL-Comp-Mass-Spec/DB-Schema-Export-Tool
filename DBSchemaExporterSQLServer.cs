using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using PRISM;

namespace DB_Schema_Export_Tool
{
    public sealed class DBSchemaExporterSQLServer : DBSchemaExporterBase
    {
        #region "Constants and Enums"

        // ReSharper disable UnusedMember.Global
        public const string SQL_SERVER_NAME_DEFAULT = "Pogo";
        public const string SQL_SERVER_USERNAME_DEFAULT = "mtuser";
        public const string SQL_SERVER_PASSWORD_DEFAULT = "mt4fun";
        // ReSharper restore UnusedMember.Global

        public const string DB_DEFINITION_FILE_PREFIX = "DBDefinition_";

        #endregion

        #region "Classwide Variables"

        private Database mCurrentDatabase;

        private readonly SortedSet<string> mSchemaToIgnore;

        private Server mSqlServer;

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="options"></param>
        /// <remarks>
        /// Will auto-connect to the server if options contains a server name
        /// Otherwise, explicitly call ConnectToServer
        /// </remarks>
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

            if (string.IsNullOrWhiteSpace(mOptions.ServerName))
                return;

            var success = ConnectToServer();
            if (!success)
            {
                OnWarningEvent("Unable to connect to server " + mOptions.ServerName);
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
        /// <returns>Dictionary where keys are table names and values are the maximum number of rows to export</returns>
        private Dictionary<TableDataExportInfo, long> AutoSelectTablesForDataExport(
            Database currentDatabase,
            IReadOnlyList<TableDataExportInfo> tablesForDataExport)
        {
            var dtTables = currentDatabase.EnumObjects(DatabaseObjectTypes.Table, SortOrder.Name);
            var tablesInDatabase = (from DataRow item in dtTables.Rows select new TableDataExportInfo(item["Name"].ToString())).ToList();

            var tablesToExport = AutoSelectTablesForDataExport(tablesInDatabase, tablesForDataExport);
            return tablesToExport;
        }

        private IEnumerable<string> CleanSqlScript(IEnumerable<string> scriptInfo)
        {
            return CleanSqlScript(scriptInfo, false, false);
        }

        private List<string> CleanSqlScript(IEnumerable<string> scriptInfo, bool removeAllScriptDateOccurrences, bool removeDuplicateHeaderLine)
        {
            var whitespaceChars = new[] {
                ' ',
                '\t'
            };

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
                    var currentLine = string.Copy(item);

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

                        while (true)
                        {
                            finalSearchIndex = currentLine.IndexOf("\r\n", indexStart, StringComparison.Ordinal);

                            if (finalSearchIndex == indexStart)
                            {
                                indexStart += 2;
                            }

                            if (!(finalSearchIndex >= 0 && finalSearchIndex < indexStart && indexStart < currentLine.Length))
                                break;
                        }

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
                                currentLine = currentLine.Substring(0, indexStartCurrent).TrimEnd(whitespaceChars) + COMMENT_END_TEXT +
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
                            if (nextCrLf > firstCrLf)
                            {
                                if (currentLine.Substring(0, firstCrLf) ==
                                    currentLine.Substring(firstCrLf + 2, nextCrLf - (firstCrLf - 2)))
                                {
                                    currentLine = currentLine.Substring(firstCrLf + 2);
                                }
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
                        return true;
                    }
                }

                // Connect to server mOptions.ServerName
                var connected = LoginToServerWork(out mSqlServer);
                if (!connected)
                {
                    if (ErrorCode == DBSchemaExportErrorCodes.NoError)
                    {
                        SetLocalError(DBSchemaExportErrorCodes.DatabaseConnectionError, "Error logging into the server: " + mOptions.ServerName);
                    }
                }

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
        /// <param name="databaseName"></param>
        /// <param name="tablesForDataExport"></param>
        /// <param name="databaseNotFound"></param>
        /// <returns>True if successful, false if an error</returns>
        protected override bool ExportDBObjectsAndTableData(
            string databaseName,
            IReadOnlyList<TableDataExportInfo> tablesForDataExport,
            out bool databaseNotFound)
        {
            return ExportDBObjectsUsingSMO(mSqlServer, databaseName, tablesForDataExport, out databaseNotFound);
        }

        /// <summary>
        /// Script the tables, views, function, etc. in the specified database
        /// Also export data from tables in tablesForDataExport
        /// </summary>
        /// <param name="sqlServer">SQL Server Accessor</param>
        /// <param name="databaseName">Database name</param>
        /// <param name="tablesForDataExport">Table names that should be auto-selected</param>
        /// <param name="databaseNotFound">Output: true if the database does not exist on the server (or is inaccessible)</param>
        /// <returns>True if successful, false if an error</returns>
        private bool ExportDBObjectsUsingSMO(
            Server sqlServer,
            string databaseName,
            IReadOnlyList<TableDataExportInfo> tablesForDataExport,
            out bool databaseNotFound)
        {
            var workingParams = new WorkingParams();

            ScriptingOptions scriptOptions;

            // Keys are table names to export
            // Values are the maximum number of rows to export
            Dictionary<TableDataExportInfo, long> tablesToExport;

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

            try
            {
                if (mOptions.ScriptingOptions.AutoSelectTablesForDataExport || mOptions.ExportAllData)
                {
                    tablesToExport = AutoSelectTablesForDataExport(mCurrentDatabase, tablesForDataExport);
                }
                else
                {
                    tablesToExport = new Dictionary<TableDataExportInfo, long>();
                    foreach (var item in tablesForDataExport)
                    {
                        tablesToExport.Add(item, 0);
                    }
                }
            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.GeneralError,
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

                    OnStatusEvent(string.Format("  Found {0} database objects to export", workingParams.ProcessCountExpected));

                    if (tablesToExport.Count > 0)
                    {
                        OnStatusEvent(string.Format("  Would export table data for {0} tables", tablesToExport.Count));
                    }

                    success = true;
                }
                else
                {
                    if (tablesForDataExport != null)
                    {
                        workingParams.ProcessCountExpected += Math.Max(tablesForDataExport.Count, tablesToExport.Count);
                    }

                    OnDebugEvent(string.Format("Scripting {0} objects", workingParams.ProcessCountExpected));

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

                // Export data from tables specified by tablesToExport (will preview the SQL to be used if mOptions.Preview is true)
                var dataSuccess = ExportDBTableData(mCurrentDatabase.Name, tablesToExport, workingParams);

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
        /// <param name="currentDatabase">Database object</param>
        /// <param name="scriptOptions">Scripting options</param>
        /// <param name="workingParams">Working parameters</param>
        /// <remarks>
        /// Do not include a Try block in this Function; let the calling function handle errors
        /// </remarks>
        private bool ExportDBObjectsWork(Database currentDatabase, ScriptingOptions scriptOptions, WorkingParams workingParams)
        {
            // Reset ProcessCount
            workingParams.ProcessCount = 0;

            if (mOptions.NoSchema)
                return true;

            if (mOptions.ScriptingOptions.ExportDBSchemasAndRoles)
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
                var scriptInfo = CleanSqlScript(StringCollectionToList(currentDatabase.Script(scriptOptions)));
                WriteTextToFile(workingParams.OutputDirectory, DB_DEFINITION_FILE_PREFIX + currentDatabase.Name, scriptInfo);
            }
            catch (Exception ex)
            {
                // User likely doesn't have privilege to script the DB; ignore the error
                OnErrorEvent("Unable to script DB " + currentDatabase.Name, ex);
                return false;
            }

            workingParams.ProcessCount++;
            if (SqlServer2005OrNewer(currentDatabase))
            {
                for (var index = 0; index < currentDatabase.Schemas.Count; index++)
                {
                    if (!ExportSchema(currentDatabase.Schemas[index]))
                        continue;

                    var scriptInfo = CleanSqlScript(StringCollectionToList(currentDatabase.Schemas[index].Script(scriptOptions)));

                    WriteTextToFile(workingParams.OutputDirectory, "Schema_" + currentDatabase.Schemas[index].Name, scriptInfo);
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

                var scriptInfo = CleanSqlScript(StringCollectionToList(currentDatabase.Roles[index].Script(scriptOptions)));
                WriteTextToFile(workingParams.OutputDirectory, "Role_" + currentDatabase.Roles[index].Name, scriptInfo);
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
            // ReSharper disable once StringLiteralTypo
            const string SYNC_OBJ_TABLE_PREFIX = "syncobj_0x";

            if (workingParams.CountObjectsOnly)
            {
                // Note: currentDatabase.Tables includes system tables, so workingParams.ProcessCount will be
                //       an overestimate if mOptions.ScriptingOptions.IncludeSystemObjects = False
                workingParams.ProcessCount += currentDatabase.Tables.Count;
                return true;
            }

            var dtStartTime = DateTime.UtcNow;

            // Initialize the scripter and smoObjectArray
            var scripter = new Scripter(mSqlServer)
            {
                Options = scriptOptions
            };

            foreach (Table databaseTable in currentDatabase.Tables)
            {
                var includeTable = true;
                if (!mOptions.ScriptingOptions.IncludeSystemObjects)
                {
                    if (databaseTable.IsSystemObject)
                    {
                        includeTable = false;
                    }
                    else if (databaseTable.Name.Length >= SYNC_OBJ_TABLE_PREFIX.Length)
                    {
                        if (databaseTable.Name.Substring(0, SYNC_OBJ_TABLE_PREFIX.Length)
                            .Equals(SYNC_OBJ_TABLE_PREFIX, StringComparison.OrdinalIgnoreCase))
                        {
                            includeTable = false;
                        }
                    }
                }

                if (includeTable)
                {
                    var subTaskProgress = ComputeSubtaskProgress(workingParams.ProcessCount, workingParams.ProcessCountExpected);
                    var percentComplete = ComputeIncrementalProgress(mPercentCompleteStart, mPercentCompleteEnd, subTaskProgress);

                    OnProgressUpdate(string.Format("Scripting {0}.{1}.{2}", currentDatabase.Name, databaseTable.Schema, databaseTable.Name), percentComplete);

                    var smoObjectArray = new SqlSmoObject[] {
                        databaseTable
                    };

                    var scriptInfo = CleanSqlScript(StringCollectionToList(scripter.Script(smoObjectArray)));
                    WriteTextToFile(workingParams.OutputDirectory, databaseTable.Name, scriptInfo);
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
                OnDebugEvent(string.Format(
                                 "Exported {0} tables in {1:0.0} seconds",
                                 currentDatabase.Tables.Count, DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds));
            }

            return true;
        }

        private bool ExportDBUserDefinedDataTypes(Database currentDatabase, ScriptingOptions scriptOptions, WorkingParams workingParams)
        {
            if (workingParams.CountObjectsOnly)
            {
                workingParams.ProcessCount += currentDatabase.UserDefinedDataTypes.Count;
            }
            else
            {
                var itemCount = ScriptCollectionOfObjects(currentDatabase.UserDefinedDataTypes, scriptOptions, workingParams.OutputDirectory);
                workingParams.ProcessCount += itemCount;
            }

            return true;
        }

        private bool ExportDBUserDefinedTypes(Database currentDatabase, ScriptingOptions scriptOptions, WorkingParams workingParams)
        {
            if (SqlServer2005OrNewer(currentDatabase))
            {
                if (workingParams.CountObjectsOnly)
                {
                    workingParams.ProcessCount += currentDatabase.UserDefinedTypes.Count;
                }
                else
                {
                    var itemCount = ScriptCollectionOfObjects(currentDatabase.UserDefinedTypes, scriptOptions, workingParams.OutputDirectory);
                    workingParams.ProcessCount += itemCount;
                }
            }

            return true;
        }

        private bool ExportDBViewsProceduresAndUDFs(Database currentDatabase, ScriptingOptions scriptOptions, WorkingParams workingParams)
        {
            // Option 1) obtain the list of views, stored procedures, and UDFs is to use currentDatabase.EnumObjects
            // However, this only returns the var name, type, and URN, not whether or not it is a system var
            //
            // Option 2) use currentDatabase.Views, currentDatabase.StoredProcedures, etc.
            // However, on Sql Server 2005 this returns many system views and system procedures that we typically don't want to export
            //
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

            // Dim strXType As String
            // For intObjectIterator = 0 To 2
            //    strXType = string.Empty
            //    Select Case intObjectIterator
            //        Case 0
            //            ' Views
            //            If mOptions.ScriptingOptions.ExportViews Then
            //                strXType = " = 'V'"
            //            End If
            //        Case 1
            //            ' Stored procedures
            //            If mOptions.ScriptingOptions.ExportStoredProcedures Then
            //                strXType = " = 'P'"
            //            End If
            //        Case 2
            //            ' User defined functions
            //            If mOptions.ScriptingOptions.ExportUserDefinedFunctions Then
            //                strXType = " IN ('IF', 'FN', 'TF')"
            //            End If
            //        Case Else
            //            ' Unknown value for intObjectIterator; skip it
            //    End Select
            //    If strXType.Length > 0 Then
            //        sql = "SELECT name FROM sysobjects WHERE xtype " & strXType
            //        If Not mOptions.ScriptingOptions.IncludeSystemObjects Then
            //            sql &= " AND category = 0"
            //        End If
            //        sql &= " ORDER BY Name"
            //        dsObjects = currentDatabase.ExecuteWithResults(sql)
            //        If workingParams.CountObjectsOnly Then
            //            workingParams.ProcessCount += dsObjects.Tables(0).Rows.Count
            //        Else
            //            For Each currentRow In dsObjects.Tables(0).Rows
            //                strObjectName = currentRow.Item(0).ToString
            //                mSubtaskProgressStepDescription = strObjectName
            //                UpdateSubtaskProgress(workingParams.ProcessCount, workingParams.ProcessCountExpected)
            //                Select Case intObjectIterator
            //                    Case 0
            //                        ' Views
            //                        smoObjectArray = currentDatabase.Views(strObjectName)
            //                    Case 1
            //                        ' Stored procedures
            //                        smoObjectArray = currentDatabase.StoredProcedures(strObjectName)
            //                    Case 2
            //                        ' User defined functions
            //                        smoObjectArray = currentDatabase.UserDefinedFunctions(strObjectName)
            //                    Case Else
            //                        ' Unknown value for intObjectIterator; skip it
            //                        smoObjectArray = Nothing
            //                End Select
            //                If Not smoObjectArray Is Nothing Then
            //                    WriteTextToFile(workingParams.OutputDirectoryPathCurrentDB, strObjectName,
            //                                      CleanSqlScript(scripter.Script(smoObjectArrayArray), schemaExportOptions)))
            //                End If
            //                workingParams.ProcessCount += 1
            //                CheckPauseStatus()
            //                If mAbortProcessing Then
            //                    UpdateProgress("Aborted processing")
            //                    Exit Function
            //                End If
            //            Next currentRow
            //        End If
            //    End If
            // Next intObjectIterator

            // Option 4) Query the INFORMATION_SCHEMA views

            // Initialize the scripter and smoObjectArrayArray()
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
                        objectType = "Views";
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
                        objectType = "Stored procedures";
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
                        objectType = "User defined functions";
                        if (mOptions.ScriptingOptions.ExportUserDefinedFunctions)
                        {
                            sql = "SELECT routine_schema, routine_name FROM INFORMATION_SCHEMA.routines " +
                                  "WHERE routine_type = 'function' " +
                                  "ORDER BY routine_name";
                        }

                        break;
                    case 3:
                        // Synonyms
                        objectType = "Synonyms";
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

                var dtStartTime = DateTime.UtcNow;
                var queryResults = currentDatabase.ExecuteWithResults(sql);
                if (workingParams.CountObjectsOnly)
                {
                    workingParams.ProcessCount += queryResults.Tables[0].Rows.Count;
                }
                else
                {
                    foreach (DataRow currentRow in queryResults.Tables[0].Rows)
                    {
                        // The first column is the schema
                        // The second column is the name
                        var objectSchema = currentRow[0].ToString();
                        var objectName = currentRow[1].ToString();

                        var subTaskProgress = ComputeSubtaskProgress(workingParams.ProcessCount, workingParams.ProcessCountExpected);
                        var percentComplete = ComputeIncrementalProgress(mPercentCompleteStart, mPercentCompleteEnd, subTaskProgress);

                        OnProgressUpdate(string.Format("Scripting {0}.{1}.{2}", currentDatabase.Name, objectSchema, objectName), percentComplete);

                        SqlSmoObject smoObjectArray;
                        switch (objectIterator)
                        {
                            case 0:
                                // Views
                                smoObjectArray = currentDatabase.Views[objectName, objectSchema];
                                break;
                            case 1:
                                // Stored procedures
                                smoObjectArray = currentDatabase.StoredProcedures[objectName, objectSchema];
                                break;
                            case 2:
                                // User defined functions
                                smoObjectArray = currentDatabase.UserDefinedFunctions[objectName, objectSchema];
                                break;
                            case 3:
                                // Synonyms
                                smoObjectArray = currentDatabase.Synonyms[objectName, objectSchema];
                                break;
                            default:
                                smoObjectArray = null;
                                break;
                        }

                        if (smoObjectArray != null)
                        {
                            var smoObjectArrayArray = new[] {
                                smoObjectArray
                            };

                            var scriptInfo = CleanSqlScript(StringCollectionToList(scripter.Script(smoObjectArrayArray)));
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

                    if (mOptions.ShowStats)
                    {
                        OnDebugEvent(string.Format(
                                         "Exported {0} {1} in {2:0.0} seconds",
                                         queryResults.Tables[0].Rows.Count, objectType, DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds));
                    }
                }
            }

            return true;
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
                if (mCurrentDatabase == null || !mCurrentDatabase.Name.Equals(databaseName))
                {
                    try
                    {
                        mCurrentDatabase = mSqlServer.Databases[databaseName];
                    }
                    catch (Exception ex)
                    {
                        SetLocalError(DBSchemaExportErrorCodes.DatabaseConnectionError, "Error connecting to database " + databaseName, ex);
                        return false;
                    }
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

                // See if any of the columns in the table is an identity column
                var identityColumnFound = false;
                foreach (Column currentColumn in databaseTable.Columns)
                {
                    if (currentColumn.Identity)
                    {
                        identityColumnFound = true;
                        break;
                    }

                }

                // Export the data from databaseTable, possibly limiting the number of rows to export
                var sql = "SELECT";
                if (maxRowsToExport > 0)
                {
                    sql += " TOP " + maxRowsToExport;
                }

                var sourceTableNameWithSchema = string.Format("{0}.{1}",
                                                             PossiblyQuoteName(databaseTable.Schema),
                                                             PossiblyQuoteName(databaseTable.Name));

                sql += " * FROM " + sourceTableNameWithSchema;

                if (mOptions.PreviewExport)
                {
                    OnStatusEvent(string.Format("Preview querying database {0} with {1}", databaseName, sql));
                    return true;
                }

                var queryResults = mCurrentDatabase.ExecuteWithResults(sql);

                var quoteWithSquareBrackets = !mOptions.PgDumpTableData;

                var targetTableNameWithSchema = GetTargetTableName(sourceTableNameWithSchema, tableInfo,
                                                                   quoteWithSquareBrackets, false,
                                                                   out var targetTableName);

                var quotedTargetTableNameWithSchema = GetTargetTableName(sourceTableNameWithSchema, tableInfo,
                                                                   quoteWithSquareBrackets, true,
                                                                   out var quotedTargetTableName);

                var headerRows = new List<string>();
                var header = COMMENT_START_TEXT + "Object:  Table " + quotedTargetTableNameWithSchema;

                if (mOptions.ScriptingOptions.IncludeTimestampInScriptFileHeader)
                {
                    header += "    " + COMMENT_SCRIPT_DATE_TEXT + GetTimeStamp();
                }

                header += COMMENT_END_TEXT;
                headerRows.Add(header);
                headerRows.Add(COMMENT_START_TEXT + "RowCount: " + databaseTable.RowCount + COMMENT_END_TEXT);

                var columnCount = queryResults.Tables[0].Columns.Count;

                var columnInfoByType = new List<KeyValuePair<string, Type>>();

                for (var columnIndex = 0; columnIndex < columnCount; columnIndex++)
                {
                    var currentColumn = queryResults.Tables[0].Columns[columnIndex];

                    var currentColumnName = currentColumn.ColumnName;
                    var currentColumnType = currentColumn.DataType;

                    columnInfoByType.Add(new KeyValuePair<string, Type>(currentColumnName, currentColumnType));
                }

                var columnTypes = ConvertDataTableColumnInfo(databaseTable.Name, columnInfoByType, quoteWithSquareBrackets, out var headerRowValues);

                var insertIntoLine = string.Empty;

                char colSepChar;
                if (mOptions.ScriptingOptions.SaveDataAsInsertIntoStatements && !mOptions.PgDumpTableData)
                {
                    if (identityColumnFound)
                    {
                        insertIntoLine = string.Format("INSERT INTO {0} ({1}) VALUES (", quotedTargetTableNameWithSchema, headerRowValues);

                        headerRows.Add("SET IDENTITY_INSERT " + quotedTargetTableNameWithSchema + " ON");
                    }
                    else
                    {
                        // Identity column not present; no need to explicitly list the column names
                        insertIntoLine = string.Format("INSERT INTO {0} VALUES (", quotedTargetTableNameWithSchema);

                        headerRows.Add(COMMENT_START_TEXT + "Columns: " + headerRowValues + COMMENT_END_TEXT);
                    }

                    colSepChar = ',';
                }
                else if (mOptions.PgDumpTableData)
                {
                    if (tableInfo.UseMergeStatement)
                    {
                        // ToDo: Code this
                        var mergeCommand = string.Format("MERGE ...");
                        headerRows.Add(mergeCommand);
                        colSepChar = ',';
                    }
                    else
                    {
                        // ReSharper disable once StringLiteralTypo
                        var copyCommand = string.Format("COPY {0} ({1}) from stdin;", targetTableNameWithSchema, headerRowValues);
                        headerRows.Add(copyCommand);
                        colSepChar = '\t';
                    }

                }
                else
                {
                    headerRows.Add(headerRowValues.ToString());
                    colSepChar = '\t';
                }

                var outFilePath = GetFileNameForTableDataExport(targetTableName, targetTableNameWithSchema, workingParams);
                OnDebugEvent("Writing table data to " + outFilePath);

                using (var writer = new StreamWriter(new FileStream(outFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
                {
                    if (mOptions.PgDumpTableData)
                    {
                        writer.NewLine = "\n";
                    }

                    foreach (var headerRow in headerRows)
                    {
                        writer.WriteLine(headerRow);
                    }

                    ExportDBTableDataWork(writer, queryResults, columnTypes, insertIntoLine, colSepChar);

                    if (identityColumnFound && mOptions.ScriptingOptions.SaveDataAsInsertIntoStatements && !mOptions.PgDumpTableData)
                    {
                        writer.WriteLine("SET IDENTITY_INSERT " + quotedTargetTableNameWithSchema + " OFF");
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.GeneralError, "Error in ExportDBTableData", ex);
                return false;
            }
        }

        /// <summary>
        /// Step through the results in queryResults
        /// Append lines to the output file
        /// </summary>
        /// <param name="writer"></param>
        /// <param name="queryResults"></param>
        /// <param name="columnTypes"></param>
        /// <param name="insertIntoLine"></param>
        /// <param name="colSepChar"></param>
        private void ExportDBTableDataWork(
            TextWriter writer,
            DataSet queryResults,
            IReadOnlyList<DataColumnTypeConstants> columnTypes,
            string insertIntoLine,
            char colSepChar)
        {
            var columnCount = queryResults.Tables[0].Columns.Count;

            var delimitedRowValues = new StringBuilder();

            var columnValues = new object[columnCount];

            foreach (DataRow currentRow in queryResults.Tables[0].Rows)
            {
                delimitedRowValues.Clear();
                if (mOptions.ScriptingOptions.SaveDataAsInsertIntoStatements && !mOptions.PgDumpTableData)
                {
                    delimitedRowValues.Append(insertIntoLine);
                }

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

                ExportDBTableDataRow(writer, colSepChar, delimitedRowValues, columnTypes, columnCount, columnValues);
            }

            if (mOptions.PgDumpTableData)
            {
                writer.WriteLine(@"\.");
                writer.WriteLine(@";");
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

                if (databaseRole.Name.Equals("public", StringComparison.OrdinalIgnoreCase))
                {
                    return false;
                }

                return true;
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

        private void ExportSQLServerConfiguration(Server sqlServer, ScriptingOptions scriptOptions, DirectoryInfo outputDirectoryPathCurrentServer)
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
                var mailInfo = StringCollectionToList(sqlServer.Mail.Script(scriptOptions));
                var cleanedMailInfo = CleanSqlScript(mailInfo, false, false);
                WriteTextToFile(outputDirectoryPathCurrentServer, "ServerMail", cleanedMailInfo);
            }

            // Save the Registry Settings to file ServerRegistrySettings.sql
            var serverSettings = StringCollectionToList(sqlServer.Settings.Script(scriptOptions));
            var cleanedServerSettings = CleanSqlScript(serverSettings, false, false);
            cleanedServerSettings.Insert(0, "-- Registry Settings for " + sqlServer.Name);
            WriteTextToFile(outputDirectoryPathCurrentServer, "ServerRegistrySettings", cleanedServerSettings, false);
        }

        private void ExportSQLServerInfoToIni(Server sqlServer, DirectoryInfo outputDirectoryPathCurrentServer)
        {
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

            WriteTextToFile(outputDirectoryPathCurrentServer, "ServerInformation", serverInfo, false, ".ini");
        }

        private void ExportSQLServerConfigToIni(Server sqlServer, DirectoryInfo outputDirectoryPathCurrentServer)
        {
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

            WriteTextToFile(outputDirectoryPathCurrentServer, "ServerConfiguration", serverConfig, false, ".ini");
        }

        /// <summary>
        /// Export the server logins
        /// </summary>
        /// <param name="sqlServer"></param>
        /// <param name="scriptOptions"></param>
        /// <param name="outputDirectoryPathCurrentServer"></param>
        private void ExportSQLServerLogins(Server sqlServer, ScriptingOptions scriptOptions, DirectoryInfo outputDirectoryPathCurrentServer)
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
                    SetLocalError(DBSchemaExportErrorCodes.GeneralError,
                                  string.Format("Processing failed for server {0}; login {1}", mOptions.ServerName, currentLogin));
                }

            }

        }

        /// <summary>
        /// Export the SQL Server Agent jobs
        /// </summary>
        /// <param name="sqlServer"></param>
        /// <param name="scriptOptions"></param>
        /// <param name="outputDirectoryPathCurrentServer"></param>
        private void ExportSQLServerAgentJobs(Server sqlServer, ScriptingOptions scriptOptions, DirectoryInfo outputDirectoryPathCurrentServer)
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
                    SetLocalError(DBSchemaExportErrorCodes.GeneralError,
                                  string.Format("Processing failed for server {0}; job {1}", mOptions.ServerName, currentJob));
                }

            }

        }

        private ScriptingOptions GetDefaultScriptOptions()
        {
            var scriptOptions = new ScriptingOptions
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

            return scriptOptions;
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
            return GetSqlServerDatabaseTables(databaseName, includeTableRowCounts, includeSystemObjects);
        }

        /// <summary>
        /// Retrieve a list of database names for the current server
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

        protected override IEnumerable<string> GetServerDatabasesCurrentConnection()
        {
            var databaseNames = GetSqlServerDatabasesWork();
            return databaseNames;
        }

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

            return databaseNames;
        }

        /// <summary>
        /// Lookup the table names in the specified database, optionally also determining table row counts
        /// </summary>
        /// <param name="databaseName">Database to query</param>
        /// <param name="includeTableRowCounts">When true, then determines the row count in each table</param>
        /// <param name="includeSystemObjects">When true, then also returns system var tables</param>
        /// <returns>Dictionary where keys are table names and values are row counts (if includeTableRowCounts = true)</returns>
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
                    OnWarningEvent(string.Format("Database {0} not found on sever {1}", databaseName, mCurrentServerInfo.ServerName));
                    return databaseTableInfo;
                }


                // Get the list of tables in this database
                OnDebugEvent(string.Format("Obtaining list of tables in database {0} on server {1}",
                                           databaseName, mCurrentServerInfo.ServerName));

                // Connect to database databaseName
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
                OnStatusEvent(string.Format("Found {0} tables", databaseTableInfo.Count));

                return databaseTableInfo;
            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.DatabaseConnectionError,
                              string.Format("Error obtaining list of tables in database {0} on the current server", databaseName), ex);

                return new Dictionary<TableDataExportInfo, long>();
            }
        }

        /// <summary>
        /// Login to the server
        /// </summary>
        /// <param name="sqlServer"></param>
        /// <returns>True if success, otherwise false</returns>
        private bool LoginToServerWork(out Server sqlServer)
        {
            try
            {
                var connectionInfo = new SqlConnectionInfo(mOptions.ServerName)
                {
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

        /// <summary>
        /// If objectName contains characters other than A-Z, a-z, 0-9, or an underscore, surround the name with square brackets
        /// </summary>
        /// <param name="objectName"></param>
        /// <returns></returns>
        private string PossiblyQuoteName(string objectName)
        {
            return PossiblyQuoteName(objectName, true);
        }

        /// <summary>
        /// Script a list of objects
        /// </summary>
        /// <param name="schemaCollection">IEnumerable of type SchemaCollectionBase</param>
        /// <param name="scriptOptions">Script options</param>
        /// <param name="outputDirectory">Output directory</param>
        /// <returns></returns>
        private int ScriptCollectionOfObjects(
            IEnumerable schemaCollection,
            ScriptingOptions scriptOptions,
            DirectoryInfo outputDirectory)
        {
            // Scripts the objects in schemaCollection
            // Returns the number of objects scripted
            var processCount = 0;
            foreach (Schema schemaItem in schemaCollection)
            {
                var scriptInfo = CleanSqlScript(StringCollectionToList(schemaItem.Script(scriptOptions)));

                WriteTextToFile(outputDirectory, schemaItem.Name, scriptInfo);
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

        protected override bool ScriptServerObjects()
        {
            return ScriptServerObjects(mSqlServer);
        }

        /// <summary>
        /// Export SQL Server settings and SQL Server Agent jobs
        /// </summary>
        /// <param name="sqlServer"></param>
        /// <returns></returns>
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

                ExportSQLServerAgentJobs(sqlServer, scriptOptions, serverInfoOutputDirectory);

                if (mAbortProcessing)
                {
                    return true;
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

        protected override bool ValidServerConnection()
        {
            return mConnectedToServer && mSqlServer != null && mSqlServer.State == SqlSmoState.Existing;
        }
    }
}
