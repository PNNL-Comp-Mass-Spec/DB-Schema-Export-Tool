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
        public const int MAX_ROWS_DATA_TO_EXPORT = 1000;

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
            SkipColumn = 9
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

        private readonly Regex mAnyLowerRegex;
        private readonly Regex mCamelCaseRegex;

        protected readonly Regex mColumnCharNonStandardRegEx;

        protected readonly Regex mNonStandardOSChars;

        protected ServerConnectionInfo mCurrentServerInfo;

        protected bool mConnectedToServer;

        protected readonly SchemaExportOptions mOptions;

        protected float mPercentCompleteStart;

        protected float mPercentCompleteEnd;

        #endregion

        #region "Properties"

        /// <summary>
        /// Error code
        /// </summary>
        public DBSchemaExportErrorCodes ErrorCode { get; private set; }

        /// <summary>
        /// Current pause status
        /// </summary>
        public PauseStatusConstants PauseStatus { get; protected set; }

        /// <summary>
        /// Dictionary mapping database names to the directory where the schema files were saved
        /// </summary>
        public Dictionary<string, string> SchemaOutputDirectories { get; }

        /// <summary>
        /// Table names to auto-select for data export
        /// </summary>
        public SortedSet<string> TableNamesToAutoExportData { get; }

        /// <summary>
        /// Regex strings to use to select table names to auto-select for data export
        /// </summary>
        public SortedSet<string> TableNameRegexToAutoExportData { get; }

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
            TableNamesToAutoExportData = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);
            TableNameRegexToAutoExportData = new SortedSet<string>();

            var regExOptions = RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline;

            mAnyLowerRegex = new Regex("[a-z]", RegexOptions.Compiled | RegexOptions.Singleline);

            mCamelCaseRegex = new Regex("(?<Part1>.+?)(?<Part2Start>[A-Z]+)(?<Part3>.*)", RegexOptions.Compiled | RegexOptions.Singleline);

            mColumnCharNonStandardRegEx = new Regex("[^a-z0-9_]", regExOptions);

            mNonStandardOSChars = new Regex(@"[^a-z0-9_ =+-,.;~!@#$%^&(){}\[\]]", regExOptions);

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
        /// <param name="tablesInDatabase">Tables in the database</param>
        /// <param name="tablesForDataExport">Tables that should be auto-selected; also used to track tables that should be skipped if the TargetTableName is &lt;skip&gt;</param>
        /// <returns>Dictionary where keys are table names and values are the maximum number of rows to export</returns>
        protected Dictionary<TableDataExportInfo, long> AutoSelectTablesForDataExport(
            string databaseName,
            IReadOnlyList<TableDataExportInfo> tablesInDatabase,
            IReadOnlyList<TableDataExportInfo> tablesForDataExport)
        {
            try
            {
                OnDebugEvent("Auto-selecting tables for data export in database " + databaseName);

                // Tracks the table names and maximum number of data rows to export (0 means all rows)
                var tablesToExportData = new Dictionary<TableDataExportInfo, long>();

                var maxRowsToExportPerTable = mOptions.MaxRows;

                if (mOptions.ExportAllData && !mOptions.DisableDataExport)
                {
                    // Export data from every table in the database
                    // Skip any tables in tablesForDataExport where the TargetTableName is <skip>
                    foreach (var candidateTable in tablesInDatabase)
                    {
                        if (candidateTable.SourceTableName.Equals("sysdiagrams") ||
                            candidateTable.SourceTableName.Equals("dtproperties"))
                        {
                            continue;
                        }

                        if (SkipTableForDataExport(tablesForDataExport, candidateTable.SourceTableName, out var tableInfo))
                            continue;

                        candidateTable.UsePgInsert = mOptions.PgInsertTableData;
                        candidateTable.DefineDateFilter(tableInfo.DateColumnName, tableInfo.MinimumDate);

                        tablesToExportData.Add(candidateTable, maxRowsToExportPerTable);
                    }

                    return tablesToExportData;
                }

                var userDefinedTableNames = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);

                // Copy the table names from tablesForDataExport to tablesToExportData
                if (tablesForDataExport != null && tablesForDataExport.Count > 0)
                {
                    foreach (var item in tablesForDataExport)
                    {
                        if (SkipTableForDataExport(item))
                        {
                            continue;
                        }

                        tablesToExportData.Add(item, maxRowsToExportPerTable);

                        if (!userDefinedTableNames.Contains(item.SourceTableName))
                        {
                            userDefinedTableNames.Add(item.SourceTableName);
                        }
                    }
                }

                // Copy the table names from TableNamesToAutoExportData to tablesToExportData (if not yet present)
                if (TableNamesToAutoExportData != null && TableNamesToAutoExportData.Count > 0)
                {
                    foreach (var tableName in TableNamesToAutoExportData)
                    {
                        foreach (var candidateTable in tablesInDatabase)
                        {
                            if (candidateTable.SourceTableName.Equals(tableName, StringComparison.OrdinalIgnoreCase) &&
                                !userDefinedTableNames.Contains(tableName) &&
                                TableNamePassesFilters(tableName))
                            {
                                tablesToExportData.Add(candidateTable, maxRowsToExportPerTable);
                                userDefinedTableNames.Add(tableName);
                            }
                        }
                    }
                }

                const RegexOptions regExOptions = RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline;

                if (TableNameRegexToAutoExportData == null || TableNameRegexToAutoExportData.Count == 0)
                    return tablesToExportData;

                var regExMatchers = new List<Regex>();
                foreach (var regexItem in TableNameRegexToAutoExportData)
                {
                    regExMatchers.Add(new Regex(regexItem, regExOptions));
                }

                foreach (var candidateTable in tablesInDatabase)
                {
                    if (!regExMatchers.Any(matcher => matcher.Match(candidateTable.SourceTableName).Success))
                        continue;

                    if (!userDefinedTableNames.Contains(candidateTable.SourceTableName) &&
                        TableNamePassesFilters(candidateTable.SourceTableName))
                    {
                        tablesToExportData.Add(candidateTable, maxRowsToExportPerTable);
                        userDefinedTableNames.Add(candidateTable.SourceTableName);
                    }
                }

                return tablesToExportData;
            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.ConfigurationError, "Error in AutoSelectTablesForDataExport", ex);
                return new Dictionary<TableDataExportInfo, long>();
            }
        }

        /// <summary>
        /// Check for PauseStatus changing to PauseRequested
        /// </summary>
        /// <remarks>
        /// If a pause is requested, update the pause status and raise event PauseStatusChange
        /// Next, enter an infinite while loop, waiting for PauseStatus to change away from Paused
        /// </remarks>
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

        /// <summary>
        /// Quote backslash, newline, carriage return, and tab characters
        /// Required to write out data in a format compatible with the PostgreSQL COPY command
        /// </summary>
        /// <param name="columnValue"></param>
        /// <returns></returns>
        private string CleanForCopyCommand(object columnValue)
        {
            if (columnValue == null)
                return string.Empty;

            var columnText = columnValue.ToString();

            // Quote the following characters
            // backslash itself, newline, carriage return, and tab

            var cleanedText = columnText.
                Replace("\\", "\\\\").
                Replace("\r", "\\r").
                Replace("\n", "\\n").
                Replace("\t", "\\t");

            return cleanedText;
        }

        /// <summary>
        /// Replace any invalid characters in filename with underscores
        /// </summary>
        /// <param name="filename"></param>
        /// <returns>Updated filename</returns>
        /// <remarks>
        /// Valid characters are:
        /// a-z, 0-9, underscore, space, equals sign, plus sign, minus sign, comma, period,
        /// semicolon, tilde, exclamation mark, and the symbols @ # $ % ^ & ( ) { } [ ]
        ///</remarks>
        protected string CleanNameForOS(string filename)
        {
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

        /// <summary>
        /// Compute progress, given the number of items processed and the total number of items
        /// </summary>
        /// <param name="itemsProcessed"></param>
        /// <param name="totalItems"></param>
        /// <returns></returns>
        protected float ComputeSubtaskProgress(int itemsProcessed, int totalItems)
        {
            if (totalItems <= 0)
                return 0;

            return itemsProcessed / (float)totalItems * 100;
        }

        /// <summary>
        /// Connect to the server specified in mOptions
        /// </summary>
        /// <returns>True if successfully connected, false if a problem</returns>
        public abstract bool ConnectToServer();

        /// <summary>
        /// Examine column types to populate a list of enum DataColumnTypeConstants
        /// </summary>
        /// <param name="sourceTableName"></param>
        /// <param name="quoteWithSquareBrackets">When true, quote names using double quotes instead of square brackets</param>
        /// <param name="dataExportParams"></param>
        /// <returns>List of column names and column names (names are the column names in the target table)</returns>
        protected ColumnMapInfo ConvertDataTableColumnInfo(
            string sourceTableName,
            bool quoteWithSquareBrackets,
            DataExportWorkingParams dataExportParams)
        {

            var columnIndex = 0;

            mOptions.ColumnMapForDataExport.TryGetValue(sourceTableName, out var columnMapInfo);

            foreach (var item in dataExportParams.ColumnInfoByType)
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

                string delimiter;

                if (mOptions.ScriptingOptions.SaveDataAsInsertIntoStatements || mOptions.PgDumpTableData || dataExportParams.PgInsertEnabled)
                    delimiter = ", ";
                else
                    delimiter = "\t";

                var targetColumnName = GetTargetColumnName(columnMapInfo, currentColumnName, ref dataColumnType);

                if (dataColumnType != DataColumnTypeConstants.SkipColumn)
                {
                    if (columnIndex > 0 && dataExportParams.HeaderRowValues.Length > 0)
                    {
                        dataExportParams.HeaderRowValues.Append(delimiter);
                    }

                    if (mOptions.ScriptingOptions.SaveDataAsInsertIntoStatements || mOptions.PgDumpTableData || dataExportParams.PgInsertEnabled)
                    {
                        dataExportParams.HeaderRowValues.Append(PossiblyQuoteName(targetColumnName, quoteWithSquareBrackets));
                    }
                    else
                    {
                        dataExportParams.HeaderRowValues.Append(targetColumnName);
                    }
                }

                dataExportParams.ColumnNamesAndTypes.Add(new KeyValuePair<string, DataColumnTypeConstants>(targetColumnName, dataColumnType));

                columnIndex++;
            }

            return columnMapInfo;
        }

        /// <summary>
        /// Convert the object name to snake_case
        /// </summary>
        /// <param name="objectName"></param>
        /// <returns></returns>
        private string ConvertNameToSnakeCase(string objectName)
        {
            if (!mAnyLowerRegex.IsMatch(objectName))
            {
                // objectName contains no lowercase letters; simply change to lowercase and return
                return objectName.ToLower();
            }

            var updatedName = string.Copy(objectName);

            while (true)
            {
                var match = mCamelCaseRegex.Match(updatedName);
                if (!match.Success)
                    break;

                var part1 = match.Groups["Part1"].Value.TrimEnd('_');
                var part2 = match.Groups["Part2Start"].Value.TrimStart('_');
                var part3 = match.Groups["Part3"].Value;

                updatedName = part1.ToLower() + "_" + part2.ToLower() + part3;
            }

            return updatedName.ToLower();
        }

        /// <summary>
        /// Create a bash script for loading data into a PostgreSQL database
        /// </summary>
        /// <param name="workingParams"></param>
        private void CreateDataLoadScriptFile(WorkingParams workingParams)
        {
            var scriptFilePath = Path.Combine(workingParams.OutputDirectoryPathCurrentDB, "LoadData.sh");

            Console.WriteLine();
            OnStatusEvent("Creating file " + scriptFilePath);

            var currentUser = Environment.UserName.ToLower();

            using (var writer = new StreamWriter(new FileStream(scriptFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
            {
                // Use linux-compatible line feeds
                writer.NewLine = "\n";

                writer.WriteLine("#!/bin/sh");

                writer.WriteLine();
                writer.WriteLine("mkdir -p Done");

                foreach (var scriptFileName in workingParams.DataLoadScriptFiles)
                {
                    writer.WriteLine();
                    writer.WriteLine("echo Processing " + scriptFileName);
                    writer.WriteLine("psql -d dms -h localhost -U {0} -f {1}", currentUser, scriptFileName);

                    var targetFilePath = "Done/" + scriptFileName;

                    writer.WriteLine("test -f {0} && rm {0}", targetFilePath);
                    writer.WriteLine("mv {0} {1} && echo '   ... moved to {1}'", scriptFileName, targetFilePath);
                }
            }
        }

        /// <summary>
        /// Export the tables, views, procedures, etc. in the given database
        /// </summary>
        /// <param name="databaseName"></param>
        /// <param name="tablesForDataExport"></param>
        /// <param name="databaseNotFound"></param>
        /// <param name="workingParams"></param>
        /// <returns>True if successful, false if an error</returns>
        protected abstract bool ExportDBObjectsAndTableData(
            string databaseName,
            IReadOnlyList<TableDataExportInfo> tablesForDataExport,
            out bool databaseNotFound,
            out WorkingParams workingParams);

        /// <summary>
        /// Export data from the specified tables
        /// </summary>
        /// <param name="databaseName">Database name</param>
        /// <param name="tablesToExportData">Dictionary with names of tables to export; values are the maximum rows to export from each table</param>
        /// <param name="workingParams">Working parameters</param>
        /// <returns></returns>
        protected bool ExportDBTableData(
            string databaseName,
            Dictionary<TableDataExportInfo, long> tablesToExportData,
            WorkingParams workingParams)
        {
            try
            {
                if (tablesToExportData == null || tablesToExportData.Count == 0)
                {
                    return true;
                }

                if (tablesToExportData.Keys.Count == 1)
                    OnDebugEvent(string.Format("Exporting data from database {0}, table {1}", databaseName, tablesToExportData.First().Key));
                else if (tablesToExportData.Keys.Count > 1 && tablesToExportData.Keys.Count < 5)
                    OnDebugEvent(string.Format("Exporting data from database {0}, tables {1} and {2}", databaseName, tablesToExportData.First().Key, tablesToExportData.Last().Key));
                else
                    OnDebugEvent(string.Format("Exporting data from database {0}, tables {1}, ...", databaseName, string.Join(", ", tablesToExportData.Keys.Take(5))));

                foreach (var tableItem in tablesToExportData)
                {
                    var tableInfo = tableItem.Key;
                    var maxRowsToExport = tableItem.Value;

                    var success = ExportDBTableData(databaseName, tableInfo, maxRowsToExport, workingParams);
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

                if (mOptions.ScriptPgLoadCommands)
                {
                    CreateDataLoadScriptFile(workingParams);
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
        /// <param name="tableInfo">Table info</param>
        /// <param name="maxRowsToExport">Maximum rows to export</param>
        /// <param name="workingParams">Working parameters</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>If the table does not exist, will still return true</remarks>
        protected abstract bool ExportDBTableData(string databaseName, TableDataExportInfo tableInfo, long maxRowsToExport, WorkingParams workingParams);

        /// <summary>
        /// Append a single row of results to the output file
        /// </summary>
        /// <param name="writer">Text file writer</param>
        /// <param name="dataExportParams"></param>
        /// <param name="delimitedRowValues">Text to write to the current line</param>
        /// <param name="columnCount">Number of columns</param>
        /// <param name="columnValues">Column values</param>
        protected void ExportDBTableDataRow(
            TextWriter writer,
            DataExportWorkingParams dataExportParams,
            StringBuilder delimitedRowValues,
            int columnCount,
            IReadOnlyList<object> columnValues)
        {
            var colSepChar = dataExportParams.ColSepChar;
            var nullValue = dataExportParams.NullValue;
            var pgInsertEnabled = dataExportParams.PgInsertEnabled;

            for (var columnIndex = 0; columnIndex < columnCount; columnIndex++)
            {
                if (dataExportParams.ColumnNamesAndTypes[columnIndex].Value == DataColumnTypeConstants.SkipColumn)
                {
                    // Skip this column
                    continue;
                }

                if (columnIndex > 0 && delimitedRowValues.Length > 0)
                {
                    delimitedRowValues.Append(colSepChar);
                }

                if (columnValues[columnIndex] == null)
                {
                    delimitedRowValues.Append(nullValue);
                    continue;
                }

                switch (dataExportParams.ColumnNamesAndTypes[columnIndex].Value)
                {
                    case DataColumnTypeConstants.Numeric:
                        delimitedRowValues.Append(columnValues[columnIndex]);
                        break;

                    case DataColumnTypeConstants.Text:
                    case DataColumnTypeConstants.DateTime:
                    case DataColumnTypeConstants.GUID:
                        if (mOptions.PgDumpTableData && !pgInsertEnabled)
                        {
                            delimitedRowValues.Append(CleanForCopyCommand(columnValues[columnIndex]));
                        }
                        else if (mOptions.ScriptingOptions.SaveDataAsInsertIntoStatements || pgInsertEnabled)
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
                        // Ignore this column
                        break;
                }

            }

            if (dataExportParams.PgInsertEnabled)
            {
                // Do not include a linefeed here; we may need to append a comma
                writer.Write("  ({0})", delimitedRowValues);
            }
            else if (mOptions.PgDumpTableData)
            {
                writer.WriteLine(delimitedRowValues.ToString());
            }
            else
            {
                if (mOptions.ScriptingOptions.SaveDataAsInsertIntoStatements)
                {
                    // Include a semi-colon if creating INSERT INTO statements for databases other than SQL Server
                    if (mOptions.PostgreSQL)
                        delimitedRowValues.Append(");");
                    else
                        delimitedRowValues.Append(")");
                }

                writer.WriteLine(delimitedRowValues.ToString());
            }
        }

        /// <summary>
        /// Retrieve a list of tables in the given database
        /// </summary>
        /// <param name="databaseName">Database to query</param>
        /// <param name="includeTableRowCounts">When true, then determines the row count in each table</param>
        /// <param name="includeSystemObjects">When true, then also returns system var tables</param>
        /// <returns>Dictionary where keys are table names and values are row counts (if includeTableRowCounts = true)</returns>
        public abstract Dictionary<TableDataExportInfo, long> GetDatabaseTables(string databaseName, bool includeTableRowCounts, bool includeSystemObjects);

        /// <summary>
        /// Generate the file name to exporting table data
        /// </summary>
        /// <param name="tableInfo"></param>
        /// <param name="targetTableName"></param>
        /// <param name="dataExportParams"></param>
        /// <param name="workingParams"></param>
        /// <returns></returns>
        protected string GetFileNameForTableDataExport(
            TableDataExportInfo tableInfo,
            string targetTableName,
            DataExportWorkingParams dataExportParams,
            WorkingParams workingParams)
        {
            var targetTableNameWithSchema = dataExportParams.TargetTableNameWithSchema;

            // Make sure output file name doesn't contain any invalid characters
            string cleanName;
            if (targetTableNameWithSchema.StartsWith("dbo.") ||
                targetTableNameWithSchema.StartsWith("public."))
                cleanName = CleanNameForOS(targetTableName + "_Data");
            else
                cleanName = CleanNameForOS(targetTableNameWithSchema + "_Data");

            var suffix = tableInfo.FilterByDate ?
                             string.Format("_Since_{0:yyyy-MM-dd}", tableInfo.MinimumDate) :
                             string.Empty;

            var outFilePath = Path.Combine(workingParams.OutputDirectory.FullName, cleanName + suffix + ".sql");

            return outFilePath;
        }

        /// <summary>
        /// Retrieve a list of database names for the current server
        /// </summary>
        /// <returns></returns>
        public abstract IEnumerable<string> GetServerDatabases();

        /// <summary>
        /// Get the list of databases from the current server
        /// </summary>
        /// <returns></returns>
        protected abstract IEnumerable<string> GetServerDatabasesCurrentConnection();

        /// <summary>
        /// Get the output directory for the server info files
        /// </summary>
        /// <param name="serverName"></param>
        /// <returns></returns>
        protected DirectoryInfo GetServerInfoOutputDirectory(string serverName)
        {
            var outputDirectoryPath = "??";

            try
            {
                // Construct the path to the output directory
                outputDirectoryPath = Path.Combine(mOptions.OutputDirectoryPath, mOptions.ServerOutputDirectoryNamePrefix + serverName);
                var serverInfoOutputDirectory = new DirectoryInfo(outputDirectoryPath);

                // Create the directory if it doesn't exist
                if (!serverInfoOutputDirectory.Exists && !mOptions.PreviewExport)
                {
                    serverInfoOutputDirectory.Create();
                }

                return serverInfoOutputDirectory;
            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.GeneralError, "Error validating or creating directory " + outputDirectoryPath, ex);
                return null;
            }
        }

        /// <summary>
        /// Get the target column name to use when exporting data
        /// </summary>
        /// <param name="columnMapInfo"></param>
        /// <param name="columnName"></param>
        /// <returns></returns>
        protected string GetTargetColumnName(ColumnMapInfo columnMapInfo, string columnName)
        {
            var unusedDataColumnType = DataColumnTypeConstants.Numeric;
            return GetTargetColumnName(columnMapInfo, columnName, ref unusedDataColumnType);
        }

        /// <summary>
        /// Get the target column name to use when exporting data
        /// </summary>
        /// <param name="columnMapInfo"></param>
        /// <param name="currentColumnName"></param>
        /// <param name="dataColumnType"></param>
        /// <returns></returns>
        protected string GetTargetColumnName(ColumnMapInfo columnMapInfo, string currentColumnName, ref DataColumnTypeConstants dataColumnType)
        {
            string targetColumnName;

            // Rename the column if defined in mOptions.ColumnMapForDataExport or if mOptions.TableDataSnakeCase is true
            if (columnMapInfo != null && columnMapInfo.IsColumnDefined(currentColumnName))
            {
                targetColumnName = columnMapInfo.GetTargetColumnName(currentColumnName);
                if (targetColumnName.Equals("<skip>", StringComparison.OrdinalIgnoreCase))
                {
                    // Do not include this column in the output file
                    dataColumnType = DataColumnTypeConstants.SkipColumn;
                }
            }
            else if (mOptions.TableDataSnakeCase)
            {
                targetColumnName = ConvertNameToSnakeCase(currentColumnName);
            }
            else
            {
                targetColumnName = currentColumnName;
            }

            return targetColumnName;
        }

        /// <summary>
        /// Get the target table name to use when exporting data
        /// </summary>
        /// <param name="sourceTableNameAndSchema">Source table: schema.table_name</param>
        /// <param name="tableInfo">Table info object</param>
        /// <param name="quoteWithSquareBrackets">When true, quote with square brackets; otherwise, quote with double quotes</param>
        /// <param name="alwaysQuoteNames">When true, always returned quoted schema.table_name</param>
        /// <param name="targetTableName">Target table name, without quotes or square brackets (even if alwaysQuoteNames is true, this is unquoted)</param>
        /// <returns></returns>
        protected string GetTargetTableName(
            string sourceTableNameAndSchema,
            TableDataExportInfo tableInfo,
            bool quoteWithSquareBrackets,
            bool alwaysQuoteNames,
            out string targetTableName)
        {
            string targetTableSchema;

            if (string.IsNullOrWhiteSpace(tableInfo.TargetTableName))
            {
                var periodIndex = sourceTableNameAndSchema.IndexOf('.');

                var defaultSchemaName = mOptions.DefaultSchemaName ?? string.Empty;

                if (periodIndex == 0 && periodIndex < sourceTableNameAndSchema.Length - 1)
                {
                    targetTableSchema = defaultSchemaName;
                    targetTableName = sourceTableNameAndSchema.Substring(2);
                }
                else if (periodIndex > 0 && periodIndex < sourceTableNameAndSchema.Length - 1)
                {
                    if (string.IsNullOrWhiteSpace(defaultSchemaName))
                        targetTableSchema = sourceTableNameAndSchema.Substring(0, periodIndex);
                    else
                        targetTableSchema = defaultSchemaName;

                    targetTableName = sourceTableNameAndSchema.Substring(periodIndex + 1);
                }
                else
                {
                    targetTableSchema = defaultSchemaName;
                    targetTableName = sourceTableNameAndSchema;
                }

                if (mOptions.TableDataSnakeCase)
                {
                    targetTableName = ConvertNameToSnakeCase(targetTableName);
                }

            }
            else
            {
                targetTableSchema = tableInfo.TargetSchemaName;
                targetTableName = tableInfo.TargetTableName;
            }

            if (string.IsNullOrWhiteSpace(targetTableSchema) ||
                targetTableSchema.Equals("dbo", StringComparison.OrdinalIgnoreCase) ||
                targetTableSchema.Equals("public", StringComparison.OrdinalIgnoreCase))
            {
                return PossiblyQuoteName(targetTableName, quoteWithSquareBrackets, alwaysQuoteNames);
            }

            return string.Format("{0}.{1}",
                                 PossiblyQuoteName(targetTableSchema, quoteWithSquareBrackets, alwaysQuoteNames),
                                 PossiblyQuoteName(targetTableName, quoteWithSquareBrackets, alwaysQuoteNames));

        }

        /// <summary>
        /// Obtain a timestamp in the form: 08/12/2006 23:01:20
        /// </summary>
        /// <returns></returns>
        protected string GetTimeStamp()
        {
            return DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss");
        }

        /// <summary>
        /// Initialize local variables
        /// </summary>
        /// <param name="clearSchemaOutputDirectories">If true, clear SchemaOutputDirectories, which tracks the output directory for each database</param>
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

        /// <summary>
        /// If objectName contains characters other than A-Z, a-z, 0-9, or an underscore, surround the name with square brackets or double quotes
        /// </summary>
        /// <param name="objectName"></param>
        /// <param name="quoteWithSquareBrackets"></param>
        /// <param name="alwaysQuoteNames"></param>
        /// <returns></returns>
        protected string PossiblyQuoteName(string objectName, bool quoteWithSquareBrackets, bool alwaysQuoteNames = false)
        {
            if (!alwaysQuoteNames && !mColumnCharNonStandardRegEx.Match(objectName).Success)
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

        /// <summary>
        /// Update variables to indicate that we are not connected to a server
        /// </summary>
        /// <remarks>Sets mConnectedToServer to false and resets all properties in mCurrentServerInfo</remarks>
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
        /// <param name="tablesForDataExport"></param>
        /// <returns></returns>
        protected bool ScriptDBObjectsAndData(
            IReadOnlyCollection<string> databaseListToProcess,
            IReadOnlyList<TableDataExportInfo> tablesForDataExport)
        {
            var processedDBList = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);

            try
            {
                Console.WriteLine();
                OnStatusEvent("Exporting DB objects/data to: " + PathUtils.CompactPathString(mOptions.OutputDirectoryPath));
                SchemaOutputDirectories.Clear();

                // Lookup the database names with the proper capitalization
                OnProgressUpdate("Obtaining list of databases on " + mCurrentServerInfo.ServerName, 0);

                var databaseNames = GetServerDatabasesCurrentConnection();

                // Populate a dictionary where keys are lowercase database names and values are the properly capitalized database names
                var databasesOnServer = new Dictionary<string, string>();

                foreach (var item in databaseNames)
                {
                    databasesOnServer.Add(item.ToLower(), item);
                }

                var warningsByDatabase = new Dictionary<string, SortedSet<string>>();

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

                    var tasksToPerform = mOptions.NoSchema ? "Exporting data" : "Exporting objects and data";

                    OnProgressUpdate(tasksToPerform + " from database " + currentDB, mPercentCompleteStart);

                    processedDBList.Add(currentDB);
                    bool success;
                    if (databasesOnServer.TryGetValue(currentDB.ToLower(), out var currentDbName))
                    {
                        currentDB = string.Copy(currentDbName);
                        OnDebugEvent(tasksToPerform + " from database " + currentDbName);
                        success = ExportDBObjectsAndTableData(currentDbName, tablesForDataExport, out databaseNotFound, out var workingParams);

                        if (!warningsByDatabase.ContainsKey(currentDB))
                        {
                            warningsByDatabase.Add(currentDB, workingParams.WarningMessages);
                        }

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

                ShowDatabaseWarnings(warningsByDatabase);

                return true;
            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.GeneralError,
                              "Error exporting DB schema objects: " + mOptions.OutputDirectoryPath, ex);
            }

            return false;
        }

        /// <summary>
        /// Scripts out the objects on the current server, including server info, database schema, and table data
        /// </summary>
        /// <param name="databaseList">Database names to export></param>
        /// <param name="tablesForDataExport">Table names for which data should be exported</param>
        /// <returns>True if success, false if a problem</returns>
        public bool ScriptServerAndDBObjects(
            IReadOnlyList<string> databaseList,
            IReadOnlyList<TableDataExportInfo> tablesForDataExport)
        {
            if (mOptions.NoSchema && mOptions.DisableDataExport)
            {
                OnDebugEvent("Schema and data export are disabled; not processing " + mOptions.ServerName);
                return true;
            }

            var validated = ValidateOptionsToScriptServerAndDBObjects(databaseList);
            if (!validated)
                return false;

            OnStatusEvent("Exporting schema/data to: " + PathUtils.CompactPathString(mOptions.OutputDirectoryPath));
            OnDebugEvent("Connecting to " + mOptions.ServerName);

            // Connect to the server
            // ScriptDBObjects calls GetServerDatabasesWork to get a list of databases on the server
            if (!ConnectToServer())
            {
                return false;
            }

            if (!ValidServerConnection())
            {
                return false;
            }

            if (mOptions.ExportServerInfo && !mOptions.NoSchema)
            {
                var success = ScriptServerObjects();
                if (!success)
                {
                    return false;
                }

                if (mAbortProcessing)
                {
                    return true;
                }
            }

            if (databaseList != null && databaseList.Count > 0)
            {
                var success = ScriptDBObjectsAndData(databaseList, tablesForDataExport);
                if (!success)
                {
                    return false;
                }

                if (mAbortProcessing)
                {
                    return true;
                }
            }

            OnProgressComplete();

            return true;
        }

        /// <summary>
        /// Script server objects
        /// </summary>
        /// <returns></returns>
        protected abstract bool ScriptServerObjects();

        /// <summary>
        /// Set the local error code
        /// </summary>
        /// <param name="eErrorCode"></param>
        /// <param name="message"></param>
        protected void SetLocalError(DBSchemaExportErrorCodes eErrorCode, string message)
        {
            SetLocalError(eErrorCode, message, null);
        }

        /// <summary>
        /// Set the local error code; provide an exception instance
        /// </summary>
        /// <param name="eErrorCode"></param>
        /// <param name="message"></param>
        /// <param name="ex"></param>
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

        private void ShowDatabaseWarnings(Dictionary<string, SortedSet<string>> warningsByDatabase)
        {
            var query = from item in warningsByDatabase.Keys orderby item select item;

            foreach (var databaseName in query)
            {
                var warningMessages = warningsByDatabase[databaseName];

                if (warningMessages.Count <= 0)
                    continue;

                Console.WriteLine();
                OnWarningEvent(string.Format("Warning summary for database {0}:", databaseName));

                foreach (var message in warningMessages)
                {
                    OnWarningEvent("  " + message);
                }
            }
        }

        /// <summary>
        /// Determine whether this table should be skipped when exporting data
        /// </summary>
        /// <param name="tableInfo"></param>
        /// <returns>True (meaning to skip the table) if the table has "&lt;skip&gt;" for the TargetTableName</returns>
        public bool SkipTableForDataExport(TableDataExportInfo tableInfo)
        {
            return SkipTableForDataExport(mOptions, tableInfo);
        }

        /// <summary>
        /// Determine whether this table should be skipped when exporting data
        /// </summary>
        /// <param name="options"></param>
        /// <param name="tableInfo"></param>
        /// <returns>True (meaning to skip the table) if the table has "&lt;skip&gt;" for the TargetTableName</returns>
        public static bool SkipTableForDataExport(SchemaExportOptions options, TableDataExportInfo tableInfo)
        {
            if (!TableNamePassesFilters(options, tableInfo.SourceTableName))
                return true;

            return tableInfo.TargetTableName != null && tableInfo.TargetTableName.Equals("<skip>", StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Determine whether this table should be skipped when exporting data
        /// </summary>
        /// <param name="tablesForDataExport"></param>
        /// <param name="candidateTableSourceTableName"></param>
        /// <param name="tableInfo"></param>
        /// <returns>True (meaning to skip the table) if the table name is defined in tablesForDataExport and has "&lt;skip&gt;" for the TargetTableName</returns>
        protected bool SkipTableForDataExport(
            IReadOnlyCollection<TableDataExportInfo> tablesForDataExport,
            string candidateTableSourceTableName,
            out TableDataExportInfo tableInfo)
        {
            if (!TableNamePassesFilters(candidateTableSourceTableName))
            {
                tableInfo = null;
                return true;
            }

            if (tablesForDataExport == null)
            {
                tableInfo = new TableDataExportInfo(candidateTableSourceTableName);
                return false;
            }

            if (DBSchemaExportTool.GetTableByName(tablesForDataExport, candidateTableSourceTableName, out tableInfo))
            {
                return SkipTableForDataExport(tableInfo);
            }

            tableInfo = new TableDataExportInfo(candidateTableSourceTableName);
            return false;

        }

        /// <summary>
        /// Store the Regex specs to use to find tables from which data should be exported
        /// </summary>
        /// <param name="tableNameRegExSpecs"></param>
        public void StoreTableNameRegexToAutoExportData(SortedSet<string> tableNameRegExSpecs)
        {
            TableNameRegexToAutoExportData.Clear();
            foreach (var item in tableNameRegExSpecs)
            {
                TableNameRegexToAutoExportData.Add(item);
            }
        }

        /// <summary>
        /// Store the names of tables from which data should be exported
        /// </summary>
        /// <param name="tableNames"></param>
        public void StoreTableNamesToAutoExportData(SortedSet<string> tableNames)
        {
            TableNamesToAutoExportData.Clear();
            foreach (var item in tableNames)
            {
                if (!TableNamesToAutoExportData.Contains(item))
                {
                    TableNamesToAutoExportData.Add(item);
                }
            }
        }

        /// <summary>
        /// Check whether mOptions.TableNameFilterSet is empty, or contains the table name
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns>True if the filter set is empty, or contains the table name; otherwise false</returns>
        private bool TableNamePassesFilters(string tableName)
        {
            return TableNamePassesFilters(mOptions, tableName);
        }

        /// <summary>
        /// Check whether options.TableNameFilterSet is empty, or contains the table name
        /// </summary>
        /// <param name="options"></param>
        /// <param name="tableName"></param>
        /// <returns>True if the filter set is empty, or contains the table name; otherwise false</returns>
        protected static bool TableNamePassesFilters(SchemaExportOptions options, string tableName)
        {
            return options.TableNameFilterSet.Count == 0 ||
                   options.TableNameFilterSet.Contains(tableName);
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

        /// <summary>
        /// Validate options
        /// </summary>
        /// <param name="databaseList"></param>
        /// <returns></returns>
        protected bool ValidateOptionsToScriptServerAndDBObjects(IReadOnlyList<string> databaseList)
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
                    if (mOptions.ScriptingOptions.ExportServerSettingsLoginsAndJobs && !mOptions.NoSchema)
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

        /// <summary>
        /// Validate the output directory for the current database
        /// </summary>
        /// <param name="databaseName"></param>
        /// <param name="workingParams"></param>
        /// <returns></returns>
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

        /// <summary>
        /// Validate output options
        /// </summary>
        /// <returns></returns>
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

        /// <summary>
        /// Return true if we have a valid server connection
        /// </summary>
        /// <returns></returns>
        protected abstract bool ValidServerConnection();

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
