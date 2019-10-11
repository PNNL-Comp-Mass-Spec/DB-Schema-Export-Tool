using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Windows.Forms;
using DB_Schema_Export_Tool;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using PRISM;

namespace DB_Schema_Export_Tool
{
    public sealed class DBSchemaExporterSQLServer : DBSchemaExporterBase
    {
        #region "Constants and Enums"

        public const string SQL_SERVER_NAME_DEFAULT = "Pogo";
        public const string SQL_SERVER_USERNAME_DEFAULT = "mtuser";
        public const string SQL_SERVER_PASSWORD_DEFAULT = "mt4fun";

        public const string DB_DEFINITION_FILE_PREFIX = "DBDefinition_";

        public const string COMMENT_START_TEXT = "/****** ";
        public const string COMMENT_END_TEXT = " ******/";
        public const string COMMENT_END_TEXT_SHORT = "*/";
        public const string COMMENT_SCRIPT_DATE_TEXT = "Script Date: ";

        public enum DataColumnTypeConstants
        {
            Numeric = 0,
            Text = 1,
            DateTime = 2,
            BinaryArray = 3,
            BinaryByte = 4,
            GUID = 5,
            SqlVariant = 6,
            ImageObject = 7,
            GeneralObject = 8,
        }

        #endregion

        #region "Classwide Variables"

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

            if (!string.IsNullOrWhiteSpace(mOptions.ServerName))
            {
                var success = ConnectToServer();
                OnWarningEvent("Unable to connect to server " + mOptions.ServerName);
            }
        }

        /// <summary>
        /// Determines the table names for which data will be exported
        /// </summary>
        /// <param name="objDatabase">SQL Server database</param>
        /// <param name="tableNamesForDataExport">Table names that should be auto-selected</param>
        /// <returns>Dictionary where keys are table names and values are the maximum number of rows to export</returns>
        private Dictionary<string, int> AutoSelectTableNamesForDataExport(
            Database objDatabase,
            IEnumerable<string> tableNamesForDataExport)
        {
            var tableNamesInDatabase = GetDatabaseTableNames(objDatabase);

            var tablesToExport = AutoSelectTableNamesForDataExport(tableNamesInDatabase, tableNamesForDataExport);
            return tablesToExport;
        }

        /// <summary>
        /// Connect to the server specified in mOptions
        /// </summary>
        /// <returns>True if successfully connected, false if a problem</returns>
        protected override bool ConnectToServer()
        {
            try
            {
                // Initialize the current connection options
                if (mSqlServer == null)
                {
                    ResetSqlServerConnection();
                }
                else if (mConnectedToServer && mSqlServer != null && mSqlServer.State == SqlSmoState.Existing)
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
                    if (ErrorCode == DBSchemaExportErrorCodes.NoError || string.IsNullOrWhiteSpace(mStatusMessage))
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
                return false;
            }

        }

        private bool ExportDBObjectsUsingSMO(
            Server sqlServer,
            string databaseName,
            IReadOnlyCollection<string> tableNamesForDataExport,
            out bool databaseNotFound)
        {
            var workingParams = new WorkingParams();

            ScriptingOptions scriptOptions;
            Database objDatabase;

            // Keys are table names to export
            // Values are the maximum number of rows to export
            Dictionary<string, int> tablesToExport;

            OnDBExportStarting(databaseName);

            try
            {
                scriptOptions = GetDefaultScriptOptions();
                objDatabase = sqlServer.Databases[databaseName];
                databaseNotFound = false;
            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.DatabaseConnectionError, "Error connecting to database " + databaseName, ex);
                databaseNotFound = true;
                return false;
            }

            try
            {
                // Construct the path to the output directory
                if (mOptions.CreateDirectoryForEachDB)
                {
                    workingParams.OutputDirectoryPathCurrentDB =
                        Path.Combine(mOptions.OutputDirectoryPath, mOptions.DatabaseSubdirectoryPrefix + objDatabase.Name);
                }
                else
                {
                    workingParams.OutputDirectoryPathCurrentDB = string.Copy(mOptions.OutputDirectoryPath);
                }

                workingParams.OutputDirectory = new DirectoryInfo(workingParams.OutputDirectoryPathCurrentDB);

                // Create the directory if it doesn't exist
                if (!workingParams.OutputDirectory.Exists && !mOptions.PreviewExport)
                {
                    workingParams.OutputDirectory.Create();
                }

                if (SchemaOutputDirectories.ContainsKey(databaseName))
                {
                    SchemaOutputDirectories[databaseName] = workingParams.OutputDirectoryPathCurrentDB;
                }
                else
                {
                    SchemaOutputDirectories.Add(databaseName, workingParams.OutputDirectoryPathCurrentDB);
                }

                if (mOptions.ScriptingOptions.AutoSelectTableNamesForDataExport)
                {
                    tablesToExport = AutoSelectTableNamesForDataExport(objDatabase, tableNamesForDataExport);
                }
                else
                {
                    tablesToExport = new Dictionary<string, int>();
                    foreach (var tableName in tableNamesForDataExport)
                    {
                        tablesToExport.Add(tableName, 0);
                    }

                }

            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.DatabaseConnectionError,
                              "Error validating or creating directory " + workingParams.OutputDirectoryPathCurrentDB);
                return false;
            }

            try
            {
                OnDebugEvent("Counting number of objects to export");

                // Count the number of objects that will be exported
                workingParams.CountObjectsOnly = true;
                ExportDBObjectsWork(objDatabase, scriptOptions, workingParams);

                workingParams.ProcessCountExpected = workingParams.ProcessCount;
                if (tableNamesForDataExport != null)
                {
                    workingParams.ProcessCountExpected += tableNamesForDataExport.Count;
                }

                if (mOptions.PreviewExport)
                {
                    OnStatusEvent(string.Format("  Found {0} database objects to export", workingParams.ProcessCountExpected));

                    if (tablesToExport.Count > 0)
                    {
                        OnStatusEvent(string.Format("  Would export table data for {0} tables", tablesToExport.Count));
                    }

                    return true;
                }

                workingParams.CountObjectsOnly = false;
                if (workingParams.ProcessCount > 0)
                {
                    ExportDBObjectsWork(objDatabase, scriptOptions, workingParams);
                }

                // Export data from tables specified by tablesToExport
                var success = ExportDBTableData(objDatabase, tablesToExport, workingParams);
                return success;
            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.DatabaseConnectionError, "Error scripting objects in database " + databaseName, ex);
                return false;
            }

        }

        private void ExportDBObjectsWork(Database objDatabase, ScriptingOptions scriptOptions, WorkingParams workingParams)
        {
            // Do not include a Try block in this Function; let the calling function handle errors
            // Reset ProcessCount
            workingParams.ProcessCount = 0;
            if (mOptions.ScriptingOptions.ExportDBSchemasAndRoles)
            {
                ExportDBSchemasAndRoles(objDatabase, scriptOptions, workingParams);
                if (mAbortProcessing)
                {
                    return;
                }
            }

            if (mOptions.ScriptingOptions.ExportTables)
            {
                ExportDBTables(objDatabase, scriptOptions, workingParams);
                if (mAbortProcessing)
                {
                    return;
                }
            }

            if (mOptions.ScriptingOptions.ExportViews ||
                mOptions.ScriptingOptions.ExportUserDefinedFunctions ||
                mOptions.ScriptingOptions.ExportStoredProcedures ||
                mOptions.ScriptingOptions.ExportSynonyms)
            {
                ExportDBViewsProceduresAndUDFs(objDatabase, scriptOptions, workingParams);
                if (mAbortProcessing)
                {
                    return;
                }
            }

            if (mOptions.ScriptingOptions.ExportUserDefinedDataTypes)
            {
                ExportDBUserDefinedDataTypes(objDatabase, scriptOptions, workingParams);
                if (mAbortProcessing)
                {
                    return;
                }
            }

            if (mOptions.ScriptingOptions.ExportUserDefinedTypes)
            {
                ExportDBUserDefinedTypes(objDatabase, scriptOptions, workingParams);
                if (mAbortProcessing)
                {
                    return;
                }

            }
        }

        private void ExportDBSchemasAndRoles(
            Database objDatabase,
            ScriptingOptions scriptOptions,
            WorkingParams workingParams)
        {
            if (workingParams.CountObjectsOnly)
            {
                workingParams.ProcessCount++;
                if (SqlServer2005OrNewer(objDatabase))
                {
                    for (var index = 0; index <= objDatabase.Schemas.Count - 1; index++)
                    {
                        if (ExportSchema(objDatabase.Schemas[index]))
                        {
                            workingParams.ProcessCount++;
                        }

                    }

                }

                for (var index = 0; index <= objDatabase.Roles.Count - 1; index++)
                {
                    if (ExportRole(objDatabase.Roles[index]))
                    {
                        workingParams.ProcessCount++;
                    }

                }

                return;
            }

            try
            {
                var scriptInfo = CleanSqlScript(StringCollectionToList(objDatabase.Script(scriptOptions)));
                WriteTextToFile(workingParams.OutputDirectory, DB_DEFINITION_FILE_PREFIX + objDatabase.Name, scriptInfo);
            }
            catch (Exception ex)
            {
                // User likely doesn't have privilege to script the DB; ignore the error
                OnErrorEvent("Unable to script DB " + objDatabase.Name, ex);
            }

            workingParams.ProcessCount++;
            if (SqlServer2005OrNewer(objDatabase))
            {
                for (var index = 0; index <= objDatabase.Schemas.Count - 1; index++)
                {
                    if (ExportSchema(objDatabase.Schemas[index]))
                    {
                        var scriptInfo = CleanSqlScript(StringCollectionToList(objDatabase.Schemas[index].Script(scriptOptions)));

                        WriteTextToFile(workingParams.OutputDirectory, "Schema_" + objDatabase.Schemas[index].Name, scriptInfo);
                        workingParams.ProcessCount++;
                        CheckPauseStatus();
                        if (mAbortProcessing)
                        {
                            OnWarningEvent("Aborted processing");
                            return;
                        }

                    }

                }

            }

            for (var index = 0; index <= objDatabase.Roles.Count - 1; index++)
            {
                if (ExportRole(objDatabase.Roles[index]))
                {
                    var scriptInfo = CleanSqlScript(StringCollectionToList(objDatabase.Roles[index].Script(scriptOptions)));
                    WriteTextToFile(workingParams.OutputDirectory, "Role_" + objDatabase.Roles[index].Name, scriptInfo);
                    workingParams.ProcessCount++;
                    CheckPauseStatus();
                    if (mAbortProcessing)
                    {
                        OnWarningEvent("Aborted processing");
                        return;
                    }

                }

            }

        }

        private void ExportDBTables(Database objDatabase, ScriptingOptions scriptOptions, WorkingParams workingParams)
        {
            const string SYNC_OBJ_TABLE_PREFIX = "syncobj_0x";

            if (workingParams.CountObjectsOnly)
            {
                // Note: objDatabase.Tables includes system tables, so workingParams.ProcessCount will be
                //       an overestimate if mOptions.ScriptingOptions.IncludeSystemObjects = False
                workingParams.ProcessCount +=  objDatabase.Tables.Count;
            }
            else
            {
                var dtStartTime = DateTime.UtcNow;

                // Initialize the scripter and objSMOObject()
                var objScripter = new Scripter(mSqlServer)
                {
                    Options = scriptOptions
                };

                foreach (Table objTable in objDatabase.Tables)
                {
                    var includeTable = true;
                    if (!mOptions.ScriptingOptions.IncludeSystemObjects)
                    {
                        if (objTable.IsSystemObject)
                        {
                            includeTable = false;
                        }
                        else if (objTable.Name.Length >= SYNC_OBJ_TABLE_PREFIX.Length)
                        {
                            if (objTable.Name.Substring(0, SYNC_OBJ_TABLE_PREFIX.Length)
                                .Equals(SYNC_OBJ_TABLE_PREFIX, StringComparison.OrdinalIgnoreCase))
                            {
                                includeTable = false;
                            }

                        }

                    }

                    if (includeTable)
                    {
                        var percentComplete = workingParams.ProcessCount / (float)workingParams.ProcessCountExpected * 100;

                        OnProgressUpdate("Scripting " + objTable.Name, percentComplete);

                        var smoObjectArray = new SqlSmoObject[] {
                            objTable
                        };

                        var scriptInfo = CleanSqlScript(StringCollectionToList(objScripter.Script(smoObjectArray)));
                        WriteTextToFile(workingParams.OutputDirectory, objTable.Name, scriptInfo);
                    }

                    workingParams.ProcessCount++;
                    CheckPauseStatus();
                    if (mAbortProcessing)
                    {
                        OnWarningEvent("Aborted processing");
                        return;
                    }

                }

                if (mOptions.ShowStats)
                {
                    OnDebugEvent(string.Format(
                                     "Exported {0} tables in {1:0.0} seconds",
                                     objDatabase.Tables.Count, DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds));
                }

            }

        }

        private void ExportDBUserDefinedDataTypes(Database objDatabase, ScriptingOptions scriptOptions, WorkingParams workingParams)
        {
            if (workingParams.CountObjectsOnly)
            {
                workingParams.ProcessCount += objDatabase.UserDefinedDataTypes.Count;
            }

            else
            {
                var intItemCount = ScriptCollectionOfObjects(objDatabase.UserDefinedDataTypes, scriptOptions,
                                                             workingParams.ProcessCountExpected, workingParams.OutputDirectory);
                workingParams.ProcessCount += intItemCount;
            }

        }


        private void ExportDBUserDefinedTypes(Database objDatabase, ScriptingOptions scriptOptions, WorkingParams workingParams)
        {
            if (SqlServer2005OrNewer(objDatabase))
            {
                if (workingParams.CountObjectsOnly)
                {
                    workingParams.ProcessCount += objDatabase.UserDefinedTypes.Count;
                }
                else
                {
                    var intItemCount = ScriptCollectionOfObjects(objDatabase.UserDefinedTypes, scriptOptions, workingParams.ProcessCountExpected, workingParams.OutputDirectory);
                    workingParams.ProcessCount += intItemCount;
                }

            }

        }

        private void ExportDBViewsProceduresAndUDFs(Database objDatabase, ScriptingOptions scriptOptions, WorkingParams workingParams)
        {
            // Option 1) obtain the list of views, stored procedures, and UDFs is to use objDatabase.EnumObjects
            // However, this only returns the var name, type, and URN, not whether or not it is a system var
            //
            // Option 2) use objDatabase.Views, objDatabase.StoredProcedures, etc.
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
            //    strXType = String.Empty
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
            //        dsObjects = objDatabase.ExecuteWithResults(sql)
            //        If workingParams.CountObjectsOnly Then
            //            workingParams.ProcessCount += dsObjects.Tables(0).Rows.Count
            //        Else
            //            For Each objRow In dsObjects.Tables(0).Rows
            //                strObjectName = objRow.Item(0).ToString
            //                mSubtaskProgressStepDescription = strObjectName
            //                UpdateSubtaskProgress(workingParams.ProcessCount, workingParams.ProcessCountExpected)
            //                Select Case intObjectIterator
            //                    Case 0
            //                        ' Views
            //                        smoObject = objDatabase.Views(strObjectName)
            //                    Case 1
            //                        ' Stored procedures
            //                        smoObject = objDatabase.StoredProcedures(strObjectName)
            //                    Case 2
            //                        ' User defined functions
            //                        smoObject = objDatabase.UserDefinedFunctions(strObjectName)
            //                    Case Else
            //                        ' Unknown value for intObjectIterator; skip it
            //                        smoObject = Nothing
            //                End Select
            //                If Not smoObject Is Nothing Then
            //                    WriteTextToFile(workingParams.OutputDirectoryPathCurrentDB, strObjectName,
            //                                      CleanSqlScript(objScripter.Script(objSMOObject), schemaExportOptions)))
            //                End If
            //                workingParams.ProcessCount += 1
            //                CheckPauseStatus()
            //                If mAbortProcessing Then
            //                    UpdateProgress("Aborted processing")
            //                    Exit Function
            //                End If
            //            Next objRow
            //        End If
            //    End If
            // Next intObjectIterator


            // Option 4) Query the INFORMATION_SCHEMA views
            // Initialize the scripter and objSMOObject()
            var objScripter = new Scripter(mSqlServer)
            {
                Options = scriptOptions
            };

            for (var objectIterator = 0; objectIterator <= 3; objectIterator++)
            {
                var objectType = "unknown";
                var sql = String.Empty;
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
                var dsObjects = objDatabase.ExecuteWithResults(sql);
                if (workingParams.CountObjectsOnly)
                {
                    workingParams.ProcessCount += dsObjects.Tables[0].Rows.Count;
                }
                else
                {
                    foreach (DataRow objRow in dsObjects.Tables[0].Rows)
                    {
                        // The first column is the schema
                        // The second column is the name
                        var objectSchema = objRow[0].ToString();
                        var objectName = objRow[1].ToString();

                        OnDebugEvent(string.Format("Processing {0}; {1} / {2}",
                                                   objectName, workingParams.ProcessCount, workingParams.ProcessCountExpected));

                        SqlSmoObject smoObject;
                        switch (objectIterator)
                        {
                            case 0:
                                // Views
                                smoObject = objDatabase.Views[objectName, objectSchema];
                                break;
                            case 1:
                                // Stored procedures
                                smoObject = objDatabase.StoredProcedures[objectName, objectSchema];
                                break;
                            case 2:
                                // User defined functions
                                smoObject = objDatabase.UserDefinedFunctions[objectName, objectSchema];
                                break;
                            case 3:
                                // Synonyms
                                smoObject = objDatabase.Synonyms[objectName, objectSchema];
                                break;
                            default:
                                smoObject = null;
                                break;
                        }

                        if (smoObject != null)
                        {
                            var smoObjectArray = new SqlSmoObject[] { smoObject };

                            var scriptInfo = CleanSqlScript(StringCollectionToList(objScripter.Script(smoObjectArray)));
                            WriteTextToFile(workingParams.OutputDirectory, objectName, scriptInfo);
                        }

                        workingParams.ProcessCount++;
                        CheckPauseStatus();
                        if (mAbortProcessing)
                        {
                            OnWarningEvent("Aborted processing");
                            return;
                        }

                    }

                    if (mOptions.ShowStats)
                    {
                        OnDebugEvent(string.Format(
                                         "Exported {0} {1} in {1:0.0} seconds",
                                         dsObjects.Tables[0].Rows.Count, objectType, DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds));
                    }

                }

            }
        }

        private bool ExportDBTableData(Database objDatabase, Dictionary<string, int> tablesToExport, WorkingParams workingParams)
        {
            try
            {
                if (tablesToExport == null || tablesToExport.Count == 0)
                {
                    return true;
                }

                var sbCurrentRow = new StringBuilder();
                foreach (var tableItem in tablesToExport)
                {
                    var maximumDataRowsToExport = tableItem.Value;
                    OnDebugEvent(string.Format("Exporting data from {0}; {1} / {2}",
                                               tableItem.Key, workingParams.ProcessCount, workingParams.ProcessCountExpected));

                    Table objTable;
                    if (objDatabase.Tables.Contains(tableItem.Key))
                    {
                        objTable = objDatabase.Tables[tableItem.Key];
                    }
                    else if (objDatabase.Tables.Contains(tableItem.Key, "dbo"))
                    {
                        objTable = objDatabase.Tables[tableItem.Key, "dbo"];
                    }
                    else
                    {
                        continue;
                    }

                    // See if any of the columns in the table is an identity column
                    var identityColumnFound = false;
                    foreach (Column objColumn in objTable.Columns)
                    {
                        if (objColumn.Identity)
                        {
                            identityColumnFound = true;
                            break;
                        }

                    }

                    // Export the data from objTable, possibly limiting the number of rows to export
                    var sql = "SELECT ";
                    if (maximumDataRowsToExport > 0)
                    {
                        sql += "TOP " + maximumDataRowsToExport;
                    }

                    sql += " * FROM [" + objTable.Name + "]";

                    var dsCurrentTable = objDatabase.ExecuteWithResults(sql);
                    var lstTableRows = new List<string>();
                    var header = COMMENT_START_TEXT + "var:  Table [" + objTable.Name + "]";

                    if (mOptions.ScriptingOptions.IncludeTimestampInScriptFileHeader)
                    {
                        header += "    " + COMMENT_SCRIPT_DATE_TEXT + GetTimeStamp();
                    }

                    header += COMMENT_END_TEXT;
                    lstTableRows.Add(header);
                    lstTableRows.Add(COMMENT_START_TEXT + "RowCount: " + objTable.RowCount + COMMENT_END_TEXT);

                    var columnCount = dsCurrentTable.Tables[0].Columns.Count;
                    var lstColumnTypes = new List<DataColumnTypeConstants>();
                    sbCurrentRow.Clear();

                    for (var columnIndex = 0; columnIndex <= columnCount - 1; columnIndex++)
                    {
                        var objColumn = dsCurrentTable.Tables[0].Columns[columnIndex];

                        // Initially assume the column's data type is numeric
                        var eDataColumnType = DataColumnTypeConstants.Numeric;
                        // Now check for other data types
                        if (objColumn.DataType == Type.GetType("System.String"))
                        {
                            eDataColumnType = DataColumnTypeConstants.Text;
                        }
                        else if (objColumn.DataType == Type.GetType("System.DateTime"))
                        {
                            // Date column
                            eDataColumnType = DataColumnTypeConstants.DateTime;
                        }
                        else if (objColumn.DataType == Type.GetType("System.Byte[]"))
                        {
                            switch (objColumn.DataType?.Name)
                            {
                                case "image":
                                    eDataColumnType = DataColumnTypeConstants.ImageObject;
                                    break;
                                case "timestamp":
                                    eDataColumnType = DataColumnTypeConstants.BinaryArray;
                                    break;
                                default:
                                    eDataColumnType = DataColumnTypeConstants.BinaryArray;
                                    break;
                            }
                        }
                        else if (objColumn.DataType == Type.GetType("System.Guid"))
                        {
                            eDataColumnType = DataColumnTypeConstants.GUID;
                        }
                        else if (objColumn.DataType == Type.GetType("System.Boolean"))
                        {
                            // This may be a binary column
                            switch (objColumn.DataType?.Name)
                            {
                                case "binary":
                                case "bit":
                                    eDataColumnType = DataColumnTypeConstants.BinaryByte;
                                    break;
                                default:
                                    eDataColumnType = DataColumnTypeConstants.Text;
                                    break;
                            }
                        }
                        else if (objColumn.DataType == Type.GetType("System.var"))
                        {
                            switch (objColumn.DataType?.Name)
                            {
                                case "sql_variant":
                                    eDataColumnType = DataColumnTypeConstants.SqlVariant;
                                    break;
                                default:
                                    eDataColumnType = DataColumnTypeConstants.GeneralObject;
                                    break;
                            }
                        }

                        lstColumnTypes.Add(eDataColumnType);
                        if (mOptions.ScriptingOptions.SaveDataAsInsertIntoStatements)
                        {
                            sbCurrentRow.Append(PossiblyQuoteColumnName(objColumn.ColumnName, true));
                            if (columnIndex < columnCount - 1)
                            {
                                sbCurrentRow.Append(", ");
                            }

                        }
                        else
                        {
                            sbCurrentRow.Append(objColumn.ColumnName);
                            if (columnIndex < columnCount - 1)
                            {
                                sbCurrentRow.Append("\t");
                            }

                        }

                    }

                    var insertIntoLine = string.Empty;
                    char chColSepChar;
                    if (mOptions.ScriptingOptions.SaveDataAsInsertIntoStatements)
                    {
                        // Future capability:
                        // 'Select Case mOptions.ScriptingOptions.DatabaseTypeForInsertInto
                        // '    Case eTargetDatabaseTypeConstants.SqlServer
                        // '    Case Else
                        // '        ' Unsupported mode
                        // 'End Select
                        if (identityColumnFound)
                        {
                            insertIntoLine = string.Format("INSERT INTO [{0}] ({1}) VALUES (", objTable.Name, sbCurrentRow.ToString());

                            lstTableRows.Add("SET IDENTITY_INSERT [" + objTable.Name + "] ON");
                        }
                        else
                        {
                            // Identity column not present; no need to explicitly list the column names
                            insertIntoLine = string.Format("INSERT INTO [{0}] VALUES (", objTable.Name);

                            lstTableRows.Add(COMMENT_START_TEXT + "Columns: " + sbCurrentRow + COMMENT_END_TEXT);
                        }

                        chColSepChar = ',';
                    }
                    else
                    {
                        lstTableRows.Add(sbCurrentRow.ToString());
                        chColSepChar = '\t';
                    }

                    foreach (DataRow objRow in dsCurrentTable.Tables[0].Rows)
                    {
                        sbCurrentRow.Clear();
                        if (mOptions.ScriptingOptions.SaveDataAsInsertIntoStatements)
                        {
                            sbCurrentRow.Append(insertIntoLine);
                        }

                        for (var columnIndex = 0; columnIndex <= columnCount - 1; columnIndex++)
                        {
                            switch (lstColumnTypes[columnIndex])
                            {
                                case DataColumnTypeConstants.Numeric:
                                    sbCurrentRow.Append(objRow[columnIndex]);
                                    break;

                                case DataColumnTypeConstants.Text:
                                case DataColumnTypeConstants.DateTime:
                                case DataColumnTypeConstants.GUID:
                                    if (mOptions.ScriptingOptions.SaveDataAsInsertIntoStatements)
                                    {
                                        sbCurrentRow.Append(PossiblyQuoteText(objRow[columnIndex].ToString()));
                                    }
                                    else
                                    {
                                        sbCurrentRow.Append(objRow[columnIndex].ToString());
                                    }
                                    break;

                                case DataColumnTypeConstants.BinaryArray:
                                    try
                                    {
                                        var bytData = (byte[])(Array)objRow[columnIndex];
                                        sbCurrentRow.Append("0x");
                                        var dataFound = false;
                                        for (var byteIndex = 0; byteIndex <= bytData.Length - 1; byteIndex++)
                                        {
                                            if (dataFound || bytData[byteIndex] != 0)
                                            {
                                                dataFound = true;
                                                sbCurrentRow.Append(bytData[byteIndex].ToString("X2"));
                                            }

                                        }

                                        if (!dataFound)
                                        {
                                            sbCurrentRow.Append("00");
                                        }

                                    }
                                    catch (Exception)
                                    {
                                        sbCurrentRow.Append("[Byte]");
                                    }
                                    break;

                                case DataColumnTypeConstants.BinaryByte:
                                    try
                                    {
                                        sbCurrentRow.Append(("0x" + Convert.ToByte(objRow[columnIndex]).ToString("X2")));
                                    }
                                    catch (Exception ex)
                                    {
                                        sbCurrentRow.Append("[Byte]");
                                    }

                                    break;

                                case DataColumnTypeConstants.ImageObject:
                                    sbCurrentRow.Append("[Image]");
                                    break;

                                case DataColumnTypeConstants.GeneralObject:
                                    sbCurrentRow.Append("[var]");
                                    break;

                                case DataColumnTypeConstants.SqlVariant:
                                    sbCurrentRow.Append("[Sql_Variant]");
                                    break;

                                default:
                                    sbCurrentRow.Append(objRow[columnIndex]);
                                    break;
                            }
                            if (columnIndex < columnCount - 1)
                            {
                                sbCurrentRow.Append(chColSepChar);
                            }

                        }

                        if (mOptions.ScriptingOptions.SaveDataAsInsertIntoStatements)
                        {
                            sbCurrentRow.Append(")");
                        }

                        lstTableRows.Add(sbCurrentRow.ToString());
                    }

                    if (identityColumnFound && mOptions.ScriptingOptions.SaveDataAsInsertIntoStatements)
                    {
                        lstTableRows.Add("SET IDENTITY_INSERT [" + objTable.Name + "] OFF");
                    }

                    // // Read method #2: Use a SqlDataReader to read row-by-row
                    // objReader = sqlServer.ConnectionContext.ExecuteReader(sql)
                    // If objReader.HasRows Then
                    //    Do While objReader.Read
                    //        If objReader.FieldCount > 0 Then
                    //            strCurrentRow = objReader.GetValue(0).ToString
                    //            objReader.GetDataTypeName()
                    //        End If
                    //        For columnIndex = 1 To objReader.FieldCount - 1
                    //            strCurrentRow &= ControlChars.Tab & objReader.GetValue(columnIndex).ToString
                    //        Next
                    //    Loop
                    // End If

                    WriteTextToFile(workingParams.OutputDirectory, objTable.Name + "_Data", lstTableRows, false);

                    workingParams.ProcessCount++;

                    CheckPauseStatus();
                    if (mAbortProcessing)
                    {
                        OnWarningEvent("Aborted processing");
                        return false;
                    }

                }

                return true;
            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExporterBase.DBSchemaExportErrorCodes.DatabaseConnectionError, "Error in ExportDBTableData", ex);
                return false;
            }
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

        private bool ExportSchema(NamedSmoObject objDatabaseSchema)
        {
            try
            {
                return !mSchemaToIgnore.Contains(objDatabaseSchema.Name);
            }
            catch
            {
                return false;
            }

        }

        private void AppendToList(ICollection<string> serverInfo, string propertyName, string propertyValue)
        {
            if (propertyName != null && propertyValue != null)
            {
                serverInfo.Add((propertyName + "=" + propertyValue));
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

        private void AppendToList(ICollection<string> serverInfo, ConfigProperty objConfigProperty)
        {
            if (objConfigProperty?.DisplayName != null)
            {
                serverInfo.Add(objConfigProperty.DisplayName + "=" + objConfigProperty.ConfigValue);
            }

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
                // If removeAllOccurrences = True, searches for all occurrences
                // If removeAllOccurrences = False, does not look past the first carriage return of each entry in scriptInfo
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
                        var objectCommentStartIndex = currentLine.IndexOf(COMMENT_START_TEXT + "var:", StringComparison.Ordinal);
                        if (currentLine.Trim().StartsWith("SET") && objectCommentStartIndex > 0)
                        {
                            indexStart = objectCommentStartIndex;
                        }

                        while (true)
                        {
                            finalSearchIndex = currentLine.IndexOf("\n", indexStart, StringComparison.Ordinal);

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
                                currentLine = currentLine.Substring(0, indexStartCurrent).TrimEnd(whitespaceChars) +
                                              COMMENT_END_TEXT +
                                              currentLine.Substring(indexEndCurrent + COMMENT_END_TEXT_SHORT.Length);
                            }

                        }

                        if (!(removeAllScriptDateOccurrences && indexStartCurrent > 0))
                            break;
                    }

                    if (removeDuplicateHeaderLine)
                    {
                        var firstCrLf = currentLine.IndexOf("\n", 0, StringComparison.Ordinal);
                        if (firstCrLf > 0 && firstCrLf < currentLine.Length)
                        {
                            var nextCrLf = currentLine.IndexOf("\n", firstCrLf + 1, StringComparison.Ordinal);
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

        private void ExportSQLServerConfiguration(Server sqlServer, ScriptingOptions scriptOptions, DirectoryInfo outputDirectoryPathCurrentServer)
        {
            // Save SQL Server info to ServerInformation.ini
            ExportSQLServerInfoToIni(sqlServer, scriptOptions, outputDirectoryPathCurrentServer);

            // Save the SQL Server configuration info to ServerConfiguration.ini
            ExportSQLServerConfigToIni(sqlServer, scriptOptions, outputDirectoryPathCurrentServer);

            // Save the Mail settings to file ServerMail.sql
            // Can only do this for SQL Server 2005 or newer
            if (SqlServer2005OrNewer(sqlServer))
            {
                var mailInfo = StringCollectionToList(sqlServer.Mail.Script(scriptOptions));
                var cleanedMailInfo = CleanSqlScript(mailInfo, false, false);
                WriteTextToFile(outputDirectoryPathCurrentServer, "ServerMail", cleanedMailInfo, true);
            }

            // Save the Registry Settings to file ServerRegistrySettings.sql
            var serverSettings = StringCollectionToList(sqlServer.Settings.Script(scriptOptions));
            var cleanedServerSettings = CleanSqlScript(serverSettings, false, false);
            cleanedServerSettings.Insert(0, "-- Registry Settings for " + sqlServer.Name);
            WriteTextToFile(outputDirectoryPathCurrentServer, "ServerRegistrySettings", cleanedServerSettings, false);
        }

        private void ExportSQLServerInfoToIni(Server sqlServer, ScriptingOptions scriptOptions, DirectoryInfo outputDirectoryPathCurrentServer)
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

        private void ExportSQLServerConfigToIni(Server sqlServer, ScriptingOptions scriptOptions, DirectoryInfo outputDirectoryPathCurrentServer)
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

        private void ExportSQLServerLogins(Server sqlServer, ScriptingOptions scriptOptions, DirectoryInfo outputDirectoryPathCurrentServer)
        {
            // Do not include a Try block in this Function; let the calling function handle errors
            // Export the server logins
            OnDebugEvent("Exporting SQL Server logins");

            for (var index = 0; index <= sqlServer.Logins.Count - 1; index++)
            {
                var currentLogin = sqlServer.Logins[index].Name;
                OnDebugEvent("Exporting login " + currentLogin);

                var scriptInfo = CleanSqlScript(StringCollectionToList(sqlServer.Logins[index].Script(scriptOptions)), true, true);
                var success = WriteTextToFile(outputDirectoryPathCurrentServer, ("Login_" + currentLogin), scriptInfo);

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
                    SetLocalError(DBSchemaExporterBase.DBSchemaExportErrorCodes.GeneralError,
                                  string.Format("Processing failed for server {0}; login {1}", mOptions.ServerName, currentLogin));
                }

            }

        }

        private void ExportSQLServerAgentJobs(Server sqlServer, ScriptingOptions scriptOptions, DirectoryInfo outputDirectoryPathCurrentServer)
        {
            // Do not include a Try block in this Function; let the calling function handle errors
            // Export the SQL Server Agent jobs

            OnStatusEvent("Exporting SQL Server Agent jobs");

            for (var index = 0; index <= sqlServer.JobServer.Jobs.Count - 1; index++)
            {
                var currentJob = sqlServer.JobServer.Jobs[index].Name;

                OnDebugEvent("Exporting job " + currentJob);
                var scriptInfo = CleanSqlScript(StringCollectionToList(sqlServer.JobServer.Jobs[index].Script(scriptOptions)), true, true);
                var success = WriteTextToFile(outputDirectoryPathCurrentServer, "AgentJob_" + currentJob, scriptInfo);

                base.CheckPauseStatus();
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
                    SetLocalError(DBSchemaExporterBase.DBSchemaExportErrorCodes.GeneralError,
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

        public IEnumerable<string> GetDatabaseTableNames(Database objDatabase)
        {
            // Step through the table names for this DB and compare to the RegEx values
            var dtTables = objDatabase.EnumObjects(DatabaseObjectTypes.Table, Microsoft.SqlServer.Management.Smo.SortOrder.Name);

            return (from DataRow item in dtTables.Rows select item["Name"].ToString());
        }

        /// <summary>
        /// Determines the databases on the current server
        /// </summary>
        /// <returns>List of databases</returns>
        public List<string> GetSqlServerDatabases()
        {
            try
            {
                InitializeLocalVariables();

                if (!mConnectedToServer || mSqlServer == null || mSqlServer.State != SqlSmoState.Existing)
                {
                    mStatusMessage = "Not connected to a server";
                    OnWarningEvent(string.Format("{0}; cannot retrieve the list of the server's databases", mStatusMessage));
                    return new List<string>();
                }

                // Obtain a list of all databases actually residing on the server (according to the Master database)
                OnStatusEvent("Obtaining list of databases on " + mCurrentServerInfo.ServerName);

                var databaseNames = GetSqlServerDatabasesWork();
                if (!mAbortProcessing)
                {
                    OnProgressUpdate("Done", 100);
                }

                return databaseNames;

            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExporterBase.DBSchemaExportErrorCodes.DatabaseConnectionError, "Error obtaining list of databases on current server", ex);
                return new List<string>();
            }

        }

        private List<string> GetSqlServerDatabasesWork()
        {
            var databaseNames = new List<string>();
            var objDatabases = mSqlServer.Databases;
            if (objDatabases.Count <= 0)
                return databaseNames;

            for (var index = 0; index <= objDatabases.Count - 1; index++)
            {
                databaseNames.Add(objDatabases[index].Name);
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
        /// <returns>Dictionary where key is table name and value is row counts (if includeTableRowCounts = true)</returns>
        /// <remarks></remarks>
        public Dictionary<string, long> GetSqlServerDatabaseTableNames(string databaseName, bool includeTableRowCounts, bool includeSystemObjects)
        {
            try
            {
                InitializeLocalVariables();
                var dctTables = new Dictionary<string, long>();
                if (databaseName == null)
                {
                    mStatusMessage = "Empty database name sent to GetSqlServerDatabaseTableNames";
                    OnWarningEvent(mStatusMessage);
                    return dctTables;
                }

                if (!mConnectedToServer || mSqlServer == null || mSqlServer.State != SqlSmoState.Existing)
                {
                    mStatusMessage = "Not connected to a server";
                    OnWarningEvent(mStatusMessage);
                    return dctTables;
                }

                if (!mSqlServer.Databases.Contains(databaseName))
                {
                    mStatusMessage = string.Format("Database {0} not found on sever {1}", databaseName, mCurrentServerInfo.ServerName);
                    OnWarningEvent(mStatusMessage);
                    return dctTables;
                }

                // Get the list of tables in this database
                OnStatusEvent(string.Format("Obtaining list of tables in database {0} on server {1}",
                                            databaseName, mCurrentServerInfo.ServerName));

                // Connect to database databaseName
                var objDatabase = mSqlServer.Databases[databaseName];

                var objTables = objDatabase.Tables;
                if (objTables.Count > 0)
                {
                    for (var index = 0; index <= objTables.Count - 1; index++)
                    {
                        if (includeSystemObjects || !objTables[index].IsSystemObject)
                        {
                            long tableRowCount = 0;
                            if (includeTableRowCounts)
                            {
                                tableRowCount = objTables[index].RowCount;
                            }

                            dctTables.Add(objTables[index].Name, tableRowCount);

                            if (mAbortProcessing)
                            {
                                OnWarningEvent("Aborted processing");
                                break;
                            }

                        }

                    }

                }

                return dctTables;
            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.DatabaseConnectionError,
                              string.Format("Error obtaining list of tables in database {0} on the current server", databaseName), ex);
            }

            return new Dictionary<string, long>();
        }

        private string GetTimeStamp()
        {
            // Return a timestamp in the form: 08/12/2006 23:01:20
            return DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss");
        }

        private bool LoginToServerWork(out Server sqlServer)
        {
            // Returns True if success, False otherwise
            try
            {
                var objConnectionInfo = new SqlConnectionInfo(mOptions.ServerName)
                {
                    ConnectionTimeout = 10
                };

                if (string.IsNullOrWhiteSpace(mOptions.DBUser))
                {
                    objConnectionInfo.UseIntegratedSecurity = true;
                }
                else
                {
                    objConnectionInfo.UseIntegratedSecurity = false;
                    objConnectionInfo.UserName = mOptions.DBUser;
                    objConnectionInfo.Password = mOptions.DBUserPassword;
                }


                var sqlServerConnection = new ServerConnection(objConnectionInfo);
                sqlServer = new Server(sqlServerConnection);

                // If no error occurred, set .Connected = True and duplicate the connection info
                mConnectedToServer = true;
                mCurrentServerInfo.UpdateInfo(mOptions);

                return true;
            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.DatabaseConnectionError, "Error logging into server " + mOptions.ServerName, ex);
                sqlServer = null;
                return false;
            }
        }

        private void ResetSqlServerConnection()
        {
            base.ResetServerConnection();
        }


        /// <summary>
        ///
        /// </summary>
        /// <param name="objSchemaCollection">IEnumerable of type SchemaCollectionBase</param>
        /// <param name="scriptOptions">Script options</param>
        /// <param name="processCountExpected">Expected number of items</param>
        /// <param name="outputDirectory">Output directory</param>
        /// <returns></returns>
        private int ScriptCollectionOfObjects(
            IEnumerable objSchemaCollection,
            ScriptingOptions scriptOptions,
            int processCountExpected,
            DirectoryInfo outputDirectory)
        {
            // Scripts the objects in objSchemaCollection
            // Returns the number of objects scripted
            var processCount = 0;
            foreach (Schema objItem in objSchemaCollection)
            {
                var scriptInfo = CleanSqlScript(StringCollectionToList(objItem.Script(scriptOptions)));

                WriteTextToFile(outputDirectory, objItem.Name, scriptInfo);
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


        private bool ScriptDBObjects(Server sqlServer, IReadOnlyCollection<string> databaseListToProcess, IReadOnlyCollection<string> tableNamesForDataExport)
        {
            var processedDBList = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);

            try
            {
                // Process each database in databaseListToProcess
                OnStatusEvent("Exporting DB objects to: " + PathUtils.CompactPathString(mOptions.OutputDirectoryPath));
                SchemaOutputDirectories.Clear();

                // Lookup the database names with the proper capitalization
                OnProgressUpdate("Obtaining list of databases on " + mCurrentServerInfo.ServerName, 0);

                var databaseNames = GetSqlServerDatabasesWork();

                // Populate a dictionary where keys are lower case database names and values are the properly capitalized database names
                var dctDatabasesOnServer = new Dictionary<string, string>();

                foreach (var item in databaseNames)
                {
                    dctDatabasesOnServer.Add(item.ToLower(), item);
                }

                foreach (var item in databaseListToProcess)
                {
                    var currentDB = string.Copy(item);

                    bool databaseNotFound;
                    if (string.IsNullOrWhiteSpace(currentDB))
                    {
                        // DB name is empty; this shouldn't happen
                        continue;
                    }

                    if (processedDBList.Contains(currentDB))
                    {
                        // DB has already been processed
                        continue;
                    }

                    processedDBList.Add(currentDB);
                    bool success;
                    if (dctDatabasesOnServer.TryGetValue(currentDB.ToLower(), out var currentDbName))
                    {
                        currentDB = string.Copy(currentDbName);
                        OnDebugEvent("Exporting objects from database " + currentDbName);
                        success = ExportDBObjectsUsingSMO(sqlServer, currentDbName, tableNamesForDataExport, out databaseNotFound);
                        if (!databaseNotFound)
                        {
                            if (!success)
                            {
                                break;
                            }
                        }
                    }
                    else
                    {
                        // Database not actually present on the server; skip it
                        databaseNotFound = true;
                        success = false;
                    }

                    var percentComplete = processedDBList.Count / (float)databaseListToProcess.Count * 100;

                    OnProgressUpdate("Exporting objects from database " + currentDB, percentComplete);

                    CheckPauseStatus();
                    if (mAbortProcessing)
                    {
                        OnWarningEvent("Aborted processing");
                        break;
                    }

                    if (success)
                    {
                        OnDebugEvent("Processing completed for database " + currentDB);
                    }
                    else
                    {
                        SetLocalError(DBSchemaExportErrorCodes.DatabaseConnectionError,
                                      string.Format("Database {0} not found on server {1}", currentDB, mOptions.ServerName));
                    }

                }

                return true;
            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.GeneralError,
                              "Error exporting DB schema objects: " + mOptions.OutputDirectoryPath, ex);
            }

            return false;
        }

        private bool ScriptServerObjects(Server sqlServer)
        {
            // Export the Server Settings and Sql Server Agent jobs

            var outputDirectoryPath = "??";

            DirectoryInfo outputDirectoryPathCurrentServer;
            var scriptOptions = GetDefaultScriptOptions();

            try
            {
                // Construct the path to the output directory
                outputDirectoryPath = Path.Combine(mOptions.OutputDirectoryPath, mOptions.ServerOutputDirectoryNamePrefix + sqlServer.Name);
                outputDirectoryPathCurrentServer = new DirectoryInfo(outputDirectoryPath);

                // Create the directory if it doesn't exist
                if (!outputDirectoryPathCurrentServer.Exists && !mOptions.PreviewExport)
                {
                    outputDirectoryPathCurrentServer.Create();
                }

            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExporterBase.DBSchemaExportErrorCodes.DatabaseConnectionError, "Error validating or creating directory " + outputDirectoryPath, ex);
                return false;
            }

            try
            {
                OnStatusEvent("Exporting Server objects to: " + PathUtils.CompactPathString(mOptions.OutputDirectoryPath));
                OnDebugEvent("Exporting server options");
                // Export the overall server configuration and options (this is quite fast, so we won't increment mProgressStep after this)
                ExportSQLServerConfiguration(sqlServer, scriptOptions, outputDirectoryPathCurrentServer);
                if (mAbortProcessing)
                {
                    return false;
                }

                ExportSQLServerLogins(sqlServer, scriptOptions, outputDirectoryPathCurrentServer);

                if (mAbortProcessing)
                {
                    return false;
                }

                ExportSQLServerAgentJobs(sqlServer, scriptOptions, outputDirectoryPathCurrentServer);

                if (mAbortProcessing)
                {
                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.DatabaseConnectionError, "Error scripting objects for server " + sqlServer.Name, ex);
                return false;
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
            InitializeLocalVariables();

            try
            {
                if (string.IsNullOrWhiteSpace(mOptions.ServerName))
                {
                    SetLocalError(DBSchemaExporterBase.DBSchemaExportErrorCodes.ConfigurationError, "Server name is not defined");
                    return false;
                }

                if (databaseList == null || databaseList.Count == 0)
                {
                    if (mOptions.ScriptingOptions.ExportServerSettingsLoginsAndJobs)
                    {
                        // No databases are defined, but we are exporting server settings; this is OK
                    }
                    else
                    {
                        SetLocalError(DBSchemaExporterBase.DBSchemaExportErrorCodes.ConfigurationError, "Database list to process is empty");
                        return false;
                    }
                }
                else
                {
                    if (databaseList.Count > 1)
                    {
                        // Force CreateDirectoryForEachDB to true
                        mOptions.CreateDirectoryForEachDB = true;
                    }
                }

            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExporterBase.DBSchemaExportErrorCodes.DatabaseConnectionError, "Error validating the Schema Export Options", ex);
                return false;
            }

            if (!base.ValidateOutputOptions())
                return false;

            OnStatusEvent("Exporting schema to: " + PathUtils.CompactPathString(mOptions.OutputDirectoryPath));
            OnDebugEvent("Connecting to " + mOptions.ServerName);

            if (!ConnectToServer())
            {
                return false;
            }

            if (mOptions.ExportServerInfo)
            {
                var success = ScriptServerObjects(mSqlServer);
                if (!success || mAbortProcessing)
                {
                    return false;
                }
            }

            if (databaseList != null && databaseList.Count > 0)
            {
                var success = ScriptDBObjects(mSqlServer, databaseList, tableNamesForDataExport);
                if (!success || mAbortProcessing)
                {
                    return false;
                }
            }

            return true;

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
    }
}
