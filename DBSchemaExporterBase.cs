using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;

using PRISM;

namespace DB_Schema_Export_Tool
{
    public abstract class DBSchemaExporterBase : EventNotifier
    {
        #region "Constants and Enums"

        // Note: this value defines the maximum number of data rows that will be exported
        // from tables that are auto-added to the table list for data export
        public const int DATA_ROW_COUNT_WARNING_THRESHOLD = 1000;

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

        public enum DBSchemaExportErrorCodes
        {
            NoError = 0,
            GeneralError = 1,
            ConfigurationError = 2,
            DatabaseConnectionError = 3,
            OutputDirectoryAccessError = 4,
        }

        public enum PauseStatusConstants
        {
            Unpaused = 0,
            PauseRequested = 1,
            Paused = 2,
            UnpauseRequested = 3,
        }

        #endregion

        #region "Classwide Variables"

        protected bool mAbortProcessing;

        protected readonly Regex mColumnCharNonStandardRegEx;

        protected readonly Regex mNonStandardOSChars;

        protected ServerConnectionInfo mCurrentServerInfo;

        protected bool mConnectedToServer;

        protected readonly SchemaExportOptions mOptions;

        protected float mPercentCompleteStart;

        protected float mPercentCompleteEnd;

        #endregion

        #region "Properties"

        public DBSchemaExportErrorCodes ErrorCode { get; private set; }

        public PauseStatusConstants PauseStatus { get; protected set; }

        public Dictionary<string, string> SchemaOutputDirectories { get; }

        public SortedSet<string> TableNamesToAutoSelect { get; }

        public SortedSet<string> TableNameAutoSelectRegEx { get; }

        #endregion

        #region "Events"

        public event DBExportStartingHandler DBExportStarting;

        public event PauseStatusChangeHandler PauseStatusChange;

        public event ProgressCompleteHandler ProgressComplete;

        /// <summary>
        /// Event is raised when we start exporting the objects from a database
        /// </summary>
        /// <param name="databaseName">Database name</param>
        public delegate void DBExportStartingHandler(string databaseName);

        public delegate void PauseStatusChangeHandler();

        public delegate void ProgressCompleteHandler();

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="options"></param>
        protected DBSchemaExporterBase(SchemaExportOptions options)
        {
            mOptions = options;

            SchemaOutputDirectories = new Dictionary<string, string>();
            TableNamesToAutoSelect = new SortedSet<string>();
            TableNameAutoSelectRegEx = new SortedSet<string>();

            var regExOptions = RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline;

            mColumnCharNonStandardRegEx = new Regex("[^a-z0-9_]", regExOptions);

            mNonStandardOSChars = new Regex(@"[^a-z0-9_ =+-,.';`~!@#$%^&(){}\[\]]", regExOptions);

            mConnectedToServer = false;
            mCurrentServerInfo = new ServerConnectionInfo(string.Empty, true);

        }

        /// <summary>
        /// Request that processing be aborted
        /// </summary>
        /// <remarks>Useful when the scripting is running in another thread</remarks>
        public void AbortProcessingNow()
        {
            mAbortProcessing = true;
            RequestUnpause();
        }

        /// <summary>
        /// Determines the table names for which data will be exported
        /// </summary>
        /// <param name="tableNamesInDatabase">Tables in the database</param>
        /// <param name="tableNamesForDataExport">Table names that should be auto-selected</param>
        /// <returns>Dictionary where keys are table names and values are the maximum number of rows to export</returns>
        protected Dictionary<string, long> AutoSelectTableNamesForDataExport(
            List<string> tableNamesInDatabase,
            IEnumerable<string> tableNamesForDataExport)
        {
            try
            {
                OnDebugEvent("Auto-selecting tables to export data from");

                // Tracks the table names and maximum number of data rows to export (0 means all rows)
                var tablesToExport = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);

                // Copy the table names from tableNamesForDataExport to tablesToExport
                // Store 0 for the hash value since we want to export all of the data rows from the tables in tableNamesForDataExport
                // Simultaneously, populate intMaximumDataRowsToExport
                if (tableNamesForDataExport != null)
                {
                    foreach (var tableName in tableNamesForDataExport)
                    {
                        tablesToExport.Add(tableName, 0);
                    }

                }

                // Copy the table names from mTableNamesToAutoSelect to tablesToExport (if not yet present)
                if (TableNamesToAutoSelect != null)
                {
                    foreach (var tableName in TableNamesToAutoSelect)
                    {
                        if (tableNamesInDatabase.Contains(tableName) && !tablesToExport.ContainsKey(tableName))
                        {
                            tablesToExport.Add(tableName, DATA_ROW_COUNT_WARNING_THRESHOLD);
                        }
                    }
                }

                const RegexOptions regExOptions = RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline;

                if (TableNameAutoSelectRegEx == null)
                    return tablesToExport;

                var regExMatchers = new List<Regex>();
                foreach (var regexItem in TableNameAutoSelectRegEx)
                {
                    regExMatchers.Add(new Regex(regexItem, regExOptions));
                }

                foreach (var tableName in tableNamesInDatabase)
                {
                    if (!regExMatchers.Any(matcher => matcher.Match(tableName).Success))
                        continue;

                    if (!tablesToExport.ContainsKey(tableName))
                    {
                        tablesToExport.Add(tableName, DATA_ROW_COUNT_WARNING_THRESHOLD);
                    }
                }

                return tablesToExport;
            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.ConfigurationError, "Error in AutoSelectTableNamesForDataExport", ex);
                return new Dictionary<string, long>();
            }
        }

        protected void CheckPauseStatus()
        {
            if (PauseStatus == PauseStatusConstants.PauseRequested)
            {
                SetPauseStatus(PauseStatusConstants.Paused);
            }

            while (PauseStatus == PauseStatusConstants.Paused && !mAbortProcessing)
            {
                Thread.Sleep(150);
            }

            SetPauseStatus(PauseStatusConstants.Unpaused);
        }

        protected string CleanNameForOS(string filename)
        {
            // Replace any invalid characters in strName with underscores
            return mNonStandardOSChars.Replace(filename, "_");
        }

        /// <summary>
        /// Computes the incremental progress that has been made beyond currentTaskProgressAtStart, based on the number of items processed and the next overall progress level
        /// </summary>
        /// <param name="currentTaskProgressAtStart">Progress at the start of the current subtask (value between 0 and 100)</param>
        /// <param name="currentTaskProgressAtEnd">Progress at the start of the current subtask (value between 0 and 100)</param>
        /// <param name="subTaskProgress">Progress of the current subtask (value between 0 and 100)</param>
        /// <returns>Overall progress (value between 0 and 100)</returns>
        /// <remarks></remarks>
        protected float ComputeIncrementalProgress(float currentTaskProgressAtStart, float currentTaskProgressAtEnd, float subTaskProgress)
        {
            if (subTaskProgress < 0)
            {
                return currentTaskProgressAtStart;
            }

            if (subTaskProgress >= 100)
            {
                return currentTaskProgressAtEnd;
            }

            return (float)(currentTaskProgressAtStart + (subTaskProgress / 100.0) * (currentTaskProgressAtEnd - currentTaskProgressAtStart));
        }

        protected float ComputeSubtaskProgress(int itemsProcessed, int totalItems)
        {
            if (totalItems <= 0)
                return 0;

            return itemsProcessed / (float)totalItems * 100;
        }

        /// <summary>
        /// Connect to the server specified in mOptions
        /// </summary>
        /// <param name="databaseName">PostgreSQL database to connect to; ignored for SQL Server</param>
        /// <returns>True if successfully connected, false if a problem</returns>
        public abstract bool ConnectToServer(string databaseName = "");

        /// <summary>
        /// Examine column types to populate a list of enum DataColumnTypeConstants
        /// </summary>
        /// <param name="columnInfoByType"></param>
        /// <param name="headerRowValues"></param>
        /// <returns></returns>
        protected List<DataColumnTypeConstants> ConvertDataTableColumnInfo(
            IReadOnlyCollection<KeyValuePair<string, Type>> columnInfoByType,
            out StringBuilder headerRowValues)
        {

            var columnTypes = new List<DataColumnTypeConstants>();
            headerRowValues = new StringBuilder();
            var columnIndex = 0;

            foreach (var item in columnInfoByType)
            {
                var currentColumnName = item.Key;
                var currentColumnType = item.Value;

                // Initially assume the column's data type is numeric
                var dataColumnType = DataColumnTypeConstants.Numeric;

                // Now check for other data types
                if (currentColumnType == Type.GetType("System.String"))
                {
                    dataColumnType = DataColumnTypeConstants.Text;
                }
                else if (currentColumnType == Type.GetType("System.DateTime"))
                {
                    // Date column
                    dataColumnType = DataColumnTypeConstants.DateTime;
                }
                else if (currentColumnType == Type.GetType("System.Byte[]"))
                {
                    switch (currentColumnType?.Name)
                    {
                        case "image":
                            dataColumnType = DataColumnTypeConstants.ImageObject;
                            break;
                        case "timestamp":
                            dataColumnType = DataColumnTypeConstants.BinaryArray;
                            break;
                        default:
                            dataColumnType = DataColumnTypeConstants.BinaryArray;
                            break;
                    }
                }
                else if (currentColumnType == Type.GetType("System.Guid"))
                {
                    dataColumnType = DataColumnTypeConstants.GUID;
                }
                else if (currentColumnType == Type.GetType("System.Boolean"))
                {
                    // This may be a binary column
                    switch (currentColumnType?.Name)
                    {
                        case "binary":
                        case "bit":
                            dataColumnType = DataColumnTypeConstants.BinaryByte;
                            break;
                        default:
                            dataColumnType = DataColumnTypeConstants.Text;
                            break;
                    }
                }
                else if (currentColumnType == Type.GetType("System.var"))
                {
                    switch (currentColumnType?.Name)
                    {
                        case "sql_variant":
                            dataColumnType = DataColumnTypeConstants.SqlVariant;
                            break;
                        default:
                            dataColumnType = DataColumnTypeConstants.GeneralObject;
                            break;
                    }
                }

                columnTypes.Add(dataColumnType);
                if (mOptions.ScriptingOptions.SaveDataAsInsertIntoStatements)
                {
                    headerRowValues.Append(PossiblyQuoteName(currentColumnName));
                    if (columnIndex < columnInfoByType.Count - 1)
                    {
                        headerRowValues.Append(", ");
                    }

                }
                else
                {
                    headerRowValues.Append(currentColumnName);
                    if (columnIndex < columnInfoByType.Count - 1)
                    {
                        headerRowValues.Append("\t");
                    }

                }

                columnIndex++;
            }

            return columnTypes;
        }

        /// <summary>
        /// Export the tables, views, procedures, etc. in the given database
        /// </summary>
        /// <param name="databaseName"></param>
        /// <param name="tableNamesForDataExport"></param>
        /// <param name="databaseNotFound"></param>
        /// <returns>True if successful, false if an error</returns>
        protected abstract bool ExportDBObjects(string databaseName, IReadOnlyCollection<string> tableNamesForDataExport, out bool databaseNotFound);

        /// <summary>
        /// Export data from the specified tables
        /// </summary>
        /// <param name="databaseName">Database name</param>
        /// <param name="tablesToExport">Dictionary with names of tables to export; values are the maximum rows to export from each table</param>
        /// <param name="workingParams">Working parameters</param>
        /// <returns></returns>
        protected bool ExportDBTableData(string databaseName, Dictionary<string, long> tablesToExport, WorkingParams workingParams)
        {
            try
            {
                if (tablesToExport == null || tablesToExport.Count == 0)
                {
                    return true;
                }

                foreach (var tableItem in tablesToExport)
                {
                    var tableName = tableItem.Key;
                    var maxRowsToExport = tableItem.Value;

                    var success = ExportDBTableData(databaseName, tableName, maxRowsToExport, workingParams);
                    if (!success)
                    {
                        return false;
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
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.GeneralError, "Error in ExportDBTableData", ex);
                return false;
            }
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
        protected abstract bool ExportDBTableData(string databaseName, string tableName, long maxRowsToExport, WorkingParams workingParams);

        /// <summary>
        /// Append a single row of results to the output file
        /// </summary>
        /// <param name="writer"></param>
        /// <param name="colSepChar"></param>
        /// <param name="delimitedRowValues"></param>
        /// <param name="columnTypes"></param>
        /// <param name="columnCount"></param>
        /// <param name="columnValues"></param>
        protected void ExportDBTableDataRow(
            TextWriter writer,
            char colSepChar,
            StringBuilder delimitedRowValues,
            IReadOnlyList<DataColumnTypeConstants> columnTypes,
            int columnCount,
            IReadOnlyList<object> columnValues)
        {
            for (var columnIndex = 0; columnIndex < columnCount; columnIndex++)
            {

                switch (columnTypes[columnIndex])
                {
                    case DataColumnTypeConstants.Numeric:
                        delimitedRowValues.Append(columnValues[columnIndex]);
                        break;

                    case DataColumnTypeConstants.Text:
                    case DataColumnTypeConstants.DateTime:
                    case DataColumnTypeConstants.GUID:
                        if (mOptions.ScriptingOptions.SaveDataAsInsertIntoStatements)
                        {
                            delimitedRowValues.Append(PossiblyQuoteText(columnValues[columnIndex].ToString()));
                        }
                        else
                        {
                            delimitedRowValues.Append(columnValues[columnIndex]);
                        }
                        break;

                    case DataColumnTypeConstants.BinaryArray:
                        try
                        {
                            var bytData = (byte[])(Array)columnValues[columnIndex];
                            delimitedRowValues.Append("0x");
                            var dataFound = false;
                            foreach (var value in bytData)
                            {
                                if (dataFound || value != 0)
                                {
                                    dataFound = true;
                                    delimitedRowValues.Append(value.ToString("X2"));
                                }
                            }

                            if (!dataFound)
                            {
                                delimitedRowValues.Append("00");
                            }

                        }
                        catch (Exception)
                        {
                            delimitedRowValues.Append("[Byte]");
                        }
                        break;

                    case DataColumnTypeConstants.BinaryByte:
                        try
                        {
                            delimitedRowValues.Append("0x" + Convert.ToByte(columnValues[columnIndex]).ToString("X2"));
                        }
                        catch (Exception)
                        {
                            delimitedRowValues.Append("[Byte]");
                        }

                        break;

                    case DataColumnTypeConstants.ImageObject:
                        delimitedRowValues.Append("[Image]");
                        break;

                    case DataColumnTypeConstants.GeneralObject:
                        delimitedRowValues.Append("[var]");
                        break;

                    case DataColumnTypeConstants.SqlVariant:
                        delimitedRowValues.Append("[Sql_Variant]");
                        break;

                    default:
                        delimitedRowValues.Append(columnValues[columnIndex]);
                        break;
                }

                if (columnIndex < columnCount - 1)
                {
                    delimitedRowValues.Append(colSepChar);
                }

            }

            if (mOptions.ScriptingOptions.SaveDataAsInsertIntoStatements)
            {
                // Include a semi-colon if creating INSERT INTO statements for databases other than SQL Server
                if (mOptions.PostgreSQL ||
                    mOptions.ScriptingOptions.DatabaseTypeForInsertInto != DatabaseScriptingOptions.TargetDatabaseTypeConstants.SqlServer)
                    delimitedRowValues.Append(");");
                else
                    delimitedRowValues.Append(")");
            }

            writer.WriteLine(delimitedRowValues.ToString());
        }

        /// <summary>
        /// Retrieve a list of tables in the given database
        /// </summary>
        /// <param name="databaseName">Database to query</param>
        /// <param name="includeTableRowCounts">When true, then determines the row count in each table</param>
        /// <param name="includeSystemObjects">When true, then also returns system var tables</param>
        /// <returns>Dictionary where keys are table names and values are row counts (if includeTableRowCounts = true)</returns>
        public abstract Dictionary<string, long> GetDatabaseTableNames(string databaseName, bool includeTableRowCounts, bool includeSystemObjects);

        /// <summary>
        /// Retrieve a list of database names for the current server
        /// </summary>
        /// <returns></returns>
        public abstract IEnumerable<string> GetServerDatabases();

        protected abstract IEnumerable<string> GetServerDatabasesCurrentConnection();

        /// <summary>
        /// Obtain a timestamp in the form: 08/12/2006 23:01:20
        /// </summary>
        /// <returns></returns>
        protected string GetTimeStamp()
        {
            return DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss");
        }

        protected void InitializeLocalVariables(bool clearSchemaOutputDirectories)
        {
            SetPauseStatus(PauseStatusConstants.Unpaused);

            mAbortProcessing = false;

            ErrorCode = DBSchemaExportErrorCodes.NoError;

            if (clearSchemaOutputDirectories)
            {
                SchemaOutputDirectories.Clear();
            }
        }

        protected void OnDBExportStarting(string databaseName)
        {
            DBExportStarting?.Invoke(databaseName);
        }

        private void OnPauseStatusChange()
        {
            PauseStatusChange?.Invoke();
        }

        protected void OnProgressComplete()
        {
            ProgressComplete?.Invoke();
        }

        protected abstract string PossiblyQuoteName(string objectName);

        /// <summary>
        /// If objectName contains characters other than A-Z, a-z, 0-9, or an underscore, surround the name with square brackets or double quotes
        /// </summary>
        /// <param name="objectName"></param>
        /// <param name="quoteWithSquareBrackets"></param>
        /// <returns></returns>
        protected string PossiblyQuoteName(string objectName, bool quoteWithSquareBrackets)
        {
            if (!mColumnCharNonStandardRegEx.Match(objectName).Success)
                return objectName;

            if (quoteWithSquareBrackets)
            {
                // SQL Server quotes names with square brackets
                return '[' + objectName + ']';
            }

            // PostgreSQL quotes names with double quotes
            return '"' + objectName + '"';

        }

        /// <summary>
        /// Surround text with single quotes
        /// Additionally, if text contains single quotes, replace them with two single quotes
        /// </summary>
        /// <param name="text"></param>
        /// <returns></returns>
        protected string PossiblyQuoteText(string text)
        {
            return string.Format("'{0}'", text.Replace("'", "''"));
        }

        /// <summary>
        /// Request that scripting be paused
        /// </summary>
        /// <remarks>Useful when the scripting is running in another thread</remarks>
        public void RequestPause()
        {
            if (!(PauseStatus == PauseStatusConstants.Paused || PauseStatus == PauseStatusConstants.PauseRequested))
            {
                SetPauseStatus(PauseStatusConstants.PauseRequested);
            }

        }

        /// <summary>
        /// Request that scripting be unpaused
        /// </summary>
        /// <remarks>Useful when the scripting is running in another thread</remarks>
        public void RequestUnpause()
        {
            if (!(PauseStatus == PauseStatusConstants.Unpaused || PauseStatus == PauseStatusConstants.UnpauseRequested))
            {
                SetPauseStatus(PauseStatusConstants.UnpauseRequested);
            }

        }

        protected void ResetServerConnection()
        {
            mConnectedToServer = false;
            mCurrentServerInfo.Reset();
        }

        /// <summary>
        /// Script the objects in each of the specified databases
        /// Also script data from the specified tables from any database that has the given table names
        /// </summary>
        /// <param name="databaseListToProcess"></param>
        /// <param name="tableNamesForDataExport"></param>
        /// <returns></returns>
        protected bool ScriptDBObjects(
            IReadOnlyCollection<string> databaseListToProcess,
            IReadOnlyCollection<string> tableNamesForDataExport)
        {
            var processedDBList = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);

            try
            {
                Console.WriteLine();
                OnStatusEvent("Exporting DB objects to: " + PathUtils.CompactPathString(mOptions.OutputDirectoryPath));
                SchemaOutputDirectories.Clear();

                // Lookup the database names with the proper capitalization
                OnProgressUpdate("Obtaining list of databases on " + mCurrentServerInfo.ServerName, 0);

                var databaseNames = GetServerDatabasesCurrentConnection();

                // Populate a dictionary where keys are lower case database names and values are the properly capitalized database names
                var databasesOnServer = new Dictionary<string, string>();

                foreach (var item in databaseNames)
                {
                    databasesOnServer.Add(item.ToLower(), item);
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

                    mPercentCompleteStart = processedDBList.Count / (float)databaseListToProcess.Count * 100;
                    mPercentCompleteEnd = (processedDBList.Count + 1) / (float)databaseListToProcess.Count * 100;

                    OnProgressUpdate("Exporting objects from database " + currentDB, mPercentCompleteStart);

                    processedDBList.Add(currentDB);
                    bool success;
                    if (databasesOnServer.TryGetValue(currentDB.ToLower(), out var currentDbName))
                    {
                        currentDB = string.Copy(currentDbName);
                        OnDebugEvent("Exporting objects from database " + currentDbName);
                        success = ExportDBObjects(currentDbName, tableNamesForDataExport, out databaseNotFound);
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

                    CheckPauseStatus();
                    if (mAbortProcessing)
                    {
                        OnWarningEvent("Aborted processing");
                        break;
                    }

                    if (success)
                    {
                        Console.WriteLine();
                        OnStatusEvent("Processing completed for database " + currentDB);
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

        public abstract bool ScriptServerAndDBObjects(List<string> databaseList, List<string> tableNamesForDataExport);

        protected void SetLocalError(DBSchemaExportErrorCodes eErrorCode, string message)
        {
            SetLocalError(eErrorCode, message, null);
        }

        protected void SetLocalError(DBSchemaExportErrorCodes eErrorCode, string message, Exception ex)
        {
            try
            {
                ErrorCode = eErrorCode;
                OnErrorEvent(message, ex);
            }
            catch (Exception)
            {
                // Ignore errors here
            }

        }

        private void SetPauseStatus(PauseStatusConstants newPauseStatus)
        {
            PauseStatus = newPauseStatus;
            OnPauseStatusChange();
        }

        public void StoreTableNameAutoSelectRegEx(SortedSet<string> tableNameRegExSpecs)
        {
            TableNameAutoSelectRegEx.Clear();
            foreach (var item in tableNameRegExSpecs)
            {
                TableNameAutoSelectRegEx.Add(item);
            }
        }

        public void StoreTableNamesToAutoSelect(SortedSet<string> tableNames)
        {
            TableNamesToAutoSelect.Clear();
            foreach (var item in tableNames)
            {
                TableNamesToAutoSelect.Add(item);
            }
        }

        /// <summary>
        /// Pause / unpause the scripting
        /// </summary>
        /// <remarks>Useful when the scripting is running in another thread</remarks>
        public void TogglePause()
        {
            if (PauseStatus == PauseStatusConstants.Unpaused)
            {
                SetPauseStatus(PauseStatusConstants.PauseRequested);
            }
            else if (PauseStatus == PauseStatusConstants.Paused)
            {
                SetPauseStatus(PauseStatusConstants.UnpauseRequested);
            }

        }

        protected bool ValidateOptionsToScriptServerAndDBObjects(List<string> databaseList)
        {
            InitializeLocalVariables(true);

            try
            {
                if (string.IsNullOrWhiteSpace(mOptions.ServerName))
                {
                    SetLocalError(DBSchemaExportErrorCodes.ConfigurationError, "Server name is not defined");
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
                        SetLocalError(DBSchemaExportErrorCodes.ConfigurationError, "Database list to process is empty");
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
                SetLocalError(DBSchemaExportErrorCodes.DatabaseConnectionError, "Error validating the Schema Export Options", ex);
                return false;
            }

            return ValidateOutputOptions();
        }

        protected bool ValidateOutputDirectoryForDatabaseExport(string databaseName, WorkingParams workingParams)
        {
            try
            {
                // Construct the path to the output directory
                if (mOptions.CreateDirectoryForEachDB)
                {
                    workingParams.OutputDirectoryPathCurrentDB = Path.Combine(mOptions.OutputDirectoryPath, mOptions.DatabaseSubdirectoryPrefix + databaseName);
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

                return true;
            }
            catch (Exception)
            {
                SetLocalError(DBSchemaExportErrorCodes.GeneralError,
                              "Error validating or creating directory " + workingParams.OutputDirectoryPathCurrentDB);
                return false;
            }

        }

        protected bool ValidateOutputOptions()
        {

            mOptions.ValidateOutputOptions();

            try
            {

                // Confirm that the output directory exists
                var outputDirectory = new DirectoryInfo(mOptions.OutputDirectoryPath);

                if (outputDirectory.Exists || mOptions.PreviewExport)
                    return true;

                // Try to create it
                outputDirectory.Create();

                return true;
            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.OutputDirectoryAccessError,
                              "Output directory could not be created: " + mOptions.OutputDirectoryPath, ex);
                return false;
            }

        }

        protected bool WriteTextToFile(
            DirectoryInfo outputDirectory,
            string objectName,
            IEnumerable<string> scriptInfo,
            bool autoAddGoStatements = true,
            string fileExtension = ".sql")
        {
            var outFilePath = "??";

            try
            {
                // Make sure objectName doesn't contain any invalid characters
                var cleanName = CleanNameForOS(objectName);
                outFilePath = Path.Combine(outputDirectory.FullName, cleanName + fileExtension);

                using (var writer = new StreamWriter(new FileStream(outFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
                {
                    foreach (var item in scriptInfo)
                    {
                        writer.WriteLine(item);
                        if (autoAddGoStatements)
                        {
                            writer.WriteLine("GO");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.OutputDirectoryAccessError, "Error creating file " + outFilePath, ex);
                return false;
            }

            return true;
        }
    }
}
