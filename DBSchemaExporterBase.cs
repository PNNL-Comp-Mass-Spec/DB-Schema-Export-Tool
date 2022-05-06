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
    /// <summary>
    /// Base class for exporting database schema and data
    /// </summary>
    public abstract class DBSchemaExporterBase : EventNotifier
    {
        // Ignore Spelling: dbo, dms, dtproperties, localhost, mkdir, mv, PostgreSQL, psql, sql, sysdiagrams, unpause, unpaused
        // Ignore Spelling: dd, yyyy, HH:mm:ss

        /// <summary>
        /// Maximum number of rows of data to export
        /// </summary>
        /// <remarks>
        /// This value defines the maximum number of data rows that will be exported
        /// from tables that are auto-added to the table list for data export
        /// </remarks>
        public const int MAX_ROWS_DATA_TO_EXPORT = 1000;

        /// <summary>
        /// Comment start
        /// </summary>
        public const string COMMENT_START_TEXT = "/****** ";

        /// <summary>
        /// Comment end
        /// </summary>
        public const string COMMENT_END_TEXT = " ******/";

        /// <summary>
        /// Compact comment end
        /// </summary>
        public const string COMMENT_END_TEXT_SHORT = "*/";

        /// <summary>
        /// Script date text
        /// </summary>
        public const string COMMENT_SCRIPT_DATE_TEXT = "Script Date: ";

        /// <summary>
        /// Data column types
        /// </summary>
        public enum DataColumnTypeConstants
        {
            /// <summary>
            /// Numeric
            /// </summary>
            Numeric = 0,

            /// <summary>
            /// Text
            /// </summary>
            Text = 1,

            /// <summary>
            /// DateTime
            /// </summary>
            DateTime = 2,

            /// <summary>
            /// BinaryArray
            /// </summary>
            BinaryArray = 3,

            /// <summary>
            /// BinaryByte
            /// </summary>
            BinaryByte = 4,

            /// <summary>
            /// GUID
            /// </summary>
            GUID = 5,

            /// <summary>
            /// SqlVariant
            /// </summary>
            SqlVariant = 6,

            /// <summary>
            /// ImageObject
            /// </summary>
            ImageObject = 7,

            /// <summary>
            /// GeneralObject
            /// </summary>
            GeneralObject = 8,

            /// <summary>
            /// SkipColumn
            /// </summary>
            SkipColumn = 9
        }

        /// <summary>
        /// Export error codes
        /// </summary>
        public enum DBSchemaExportErrorCodes
        {
            /// <summary>
            /// NoError
            /// </summary>
            NoError = 0,

            /// <summary>
            /// GeneralError
            /// </summary>
            GeneralError = 1,

            /// <summary>
            /// ConfigurationError
            /// </summary>
            ConfigurationError = 2,

            /// <summary>
            /// DatabaseConnectionError
            /// </summary>
            DatabaseConnectionError = 3,

            /// <summary>
            /// OutputDirectoryAccessError
            /// </summary>
            OutputDirectoryAccessError = 4
        }

        /// <summary>
        /// Pause status
        /// </summary>
        public enum PauseStatusConstants
        {
            /// <summary>
            /// Unpaused
            /// </summary>
            Unpaused = 0,

            /// <summary>
            /// PauseRequested
            /// </summary>
            PauseRequested = 1,

            /// <summary>
            /// Paused
            /// </summary>
            Paused = 2,

            /// <summary>
            /// UnpauseRequested
            /// </summary>
            UnpauseRequested = 3
        }

        /// <summary>
        /// Set to true to abort processing as soon as possible
        /// </summary>
        protected bool mAbortProcessing;

        /// <summary>
        /// Match any lowercase letter
        /// </summary>
        private readonly Regex mAnyLowerMatcher;

        /// <summary>
        /// Match a lowercase letter followed by an uppercase letter
        /// </summary>
        private readonly Regex mCamelCaseMatcher;

        /// <summary>
        /// Match any character that is not a letter, number, or underscore
        /// </summary>
        /// <remarks>
        /// If a match is found, quote the name with double quotes (PostgreSQL) or square brackets (SQL Server)
        /// </remarks>
        private readonly Regex mColumnCharNonStandardMatcher;

        /// <summary>
        /// Matches reserved words (key words)
        /// </summary>
        /// <remarks>
        /// If a match is found, quote the name with double quotes (PostgreSQL) or square brackets (SQL Server)
        /// </remarks>
        private readonly Regex mReservedWordMatcher;

        /// <summary>
        /// Disallowed characters for file names
        /// </summary>
        private readonly Regex mNonStandardOSChars;

        /// <summary>
        /// Object name Reg Ex
        /// </summary>
        protected Regex mObjectNameMatcher;

        /// <summary>
        /// Current server info
        /// </summary>
        protected ServerConnectionInfo mCurrentServerInfo;

        /// <summary>
        /// True when connected to a server
        /// </summary>
        protected bool mConnectedToServer;

        /// <summary>
        /// Export options
        /// </summary>
        protected readonly SchemaExportOptions mOptions;

        /// <summary>
        /// Progress percent complete, starting value for the current operation
        /// </summary>
        protected float mPercentCompleteStart;

        /// <summary>
        /// Progress percent complete, ending value for the current operation
        /// </summary>
        protected float mPercentCompleteEnd;

        /// <summary>
        /// Error code
        /// </summary>
        public DBSchemaExportErrorCodes ErrorCode { get; private set; }

        /// <summary>
        /// Current pause status
        /// </summary>
        public PauseStatusConstants PauseStatus { get; private set; }

        /// <summary>
        /// Dictionary mapping database names to the directory where the schema files were saved
        /// </summary>
        public Dictionary<string, string> SchemaOutputDirectories { get; }

        /// <summary>
        /// Table names to auto-select for data export
        /// </summary>
        public SortedSet<string> TableNamesToAutoExportData { get; }

        /// <summary>
        /// RegEx strings to use to select table names to auto-select for data export
        /// </summary>
        public SortedSet<string> TableNameRegexToAutoExportData { get; }

        /// <summary>
        /// Database export starting event
        /// </summary>
        public event DBExportStartingHandler DBExportStarting;

        /// <summary>
        /// Pause status change event
        /// </summary>
        public event PauseStatusChangeHandler PauseStatusChange;

        /// <summary>
        /// Progress complete event
        /// </summary>
        public event ProgressCompleteHandler ProgressComplete;

        /// <summary>
        /// Event is raised when we start exporting the objects from a database
        /// </summary>
        /// <param name="databaseName">Database name</param>
        public delegate void DBExportStartingHandler(string databaseName);

        /// <summary>
        /// Pause status change event delegate
        /// </summary>
        public delegate void PauseStatusChangeHandler();

        /// <summary>
        /// Progress complete event delegate
        /// </summary>
        public delegate void ProgressCompleteHandler();

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

            mAnyLowerMatcher = new Regex("[a-z]", RegexOptions.Compiled | RegexOptions.Singleline);

            mCamelCaseMatcher = new Regex("(?<LowerLetter>[a-z])(?<UpperLetter>[A-Z])", RegexOptions.Compiled);

            mColumnCharNonStandardMatcher = new Regex("[^a-z0-9_]", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);

            // The reserved words in this RegEx are a combination of PostgreSQL reserved words and SQL standard reserved words (SQL:2016)
            // See https://www.postgresql.org/docs/current/sql-keywords-appendix.html

            mReservedWordMatcher = new Regex(@"\b(ALL|ALTER|ANALYSE|ANALYZE|ARRAY|ASC|AVG|BEGIN|BETWEEN|CALL|CALLED|CASE|CAST|CHAR|CHECK|COLUMN|COMMIT|CONSTRAINT|CONTAINS|CONVERT|COPY|COUNT|CREATE|CROSS|CUBE|CURRENT|DATE|DAY|DEFAULT|DELETE|DESC|DISTINCT|DOUBLE|DROP|ELEMENT|EMPTY|EQUALS|ESCAPE|EXTERNAL|FALSE|FILTER|FROM|FULL|FUNCTION|GROUP|HAVING|IN|INNER|INTEGER|INTERVAL|INTO|JOIN|LANGUAGE|LATERIAL|LEADING|LEFT|LIKE|LIMIT|LOCAL|LOCALTIME|LOCALTIMESTAMP|MATCH|MAX|MEMBER|METHOD|MIN|MINUTE|MODULE|MONTH|NATURAL|NEW|NULL|NUMERIC|OFFSET|OLD|ON|ONE|ONLY|OPEN|OR|ORDER|OUT|OUTER|OVER|OVERLAPS|PARAMETER|PARTITION|PER|PERCENT|PERIOD|PLACING|POSITION|POWER|PRIMARY|RANGE|RANK|REFERENCES|RESULT|RIGHT|ROW|ROWS|SCOPE|SECOND|SELECT|SOME|START|SUM|SYMMETRIC|TABLE|THEN|TIMESTAMP|TO|TRAILING|TRUE|UNION|UNIQUE|UNKNOWN|UPDATE|UPPER|USER|USING|VALUE|VALUES|VERBOSE|WHEN|WHERE|WINDOW|WITH|YEAR)\b", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);

            mNonStandardOSChars = new Regex(@"[^a-z0-9_ =+-,.;~!@#$%^&(){}\[\]]", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);

            // This will get updated later if mOptions.ObjectNameFilter is not empty
            // We are not instantiating this here since we want to use OnErrorEvent() if there is as problem,
            // and that event is not subscribed to until after this class is instantiated
            mObjectNameMatcher = new Regex(".+", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);

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
        /// <param name="databaseName">Database name</param>
        /// <param name="tablesInDatabase">Tables in the database</param>
        /// <param name="tablesForDataExport">
        /// Tables that should be auto-selected; also used to track tables that should be skipped if the TargetTableName is &lt;skip&gt;
        /// </param>
        /// <returns>Dictionary where keys are information on tables to export and values are the maximum number of rows to export</returns>
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
                    ShowTrace("Exporting data from every table in the database (unless tagged with <skip> in the file specified by the DataTables parameter)");

                    // Export data from every table in the database
                    // Skip any tables in tablesForDataExport where the TargetTableName is <skip>
                    foreach (var candidateTable in tablesInDatabase)
                    {
                        if (candidateTable.SourceTableName.Equals("sysdiagrams") ||
                            candidateTable.SourceTableName.Equals("dtproperties"))
                        {
                            ShowTrace("Skipping data export from system table " + candidateTable.SourceTableName);
                            continue;
                        }

                        if (SkipTableForDataExport(tablesForDataExport, candidateTable.SourceTableName, out var tableInfo))
                        {
                            ShowTrace("Skipping data export from table " + candidateTable.SourceTableName);
                            continue;
                        }

                        candidateTable.UsePgInsert = mOptions.PgInsertTableData;
                        candidateTable.DefineDateFilter(tableInfo.DateColumnName, tableInfo.MinimumDate);

                        if (!string.IsNullOrWhiteSpace(tableInfo.TargetTableName) &&
                            !tableInfo.TargetTableName.Equals(tableInfo.SourceTableName, StringComparison.OrdinalIgnoreCase))
                        {
                            // This table has a new name in the target database
                            candidateTable.TargetTableName = tableInfo.TargetTableName;
                        }

                        if (!string.IsNullOrWhiteSpace(tableInfo.TargetSchemaName))
                        {
                            // This table has a new name in the target database
                            candidateTable.TargetSchemaName = tableInfo.TargetSchemaName;
                        }

                        tablesToExportData.Add(candidateTable, maxRowsToExportPerTable);
                    }

                    ShowTraceTableExportCount(tablesToExportData, databaseName);
                    return tablesToExportData;
                }

                var userDefinedTableNames = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);

                // Copy the table names from tablesForDataExport to tablesToExportData
                if (tablesForDataExport?.Count > 0)
                {
                    foreach (var item in tablesForDataExport)
                    {
                        if (SkipTableForDataExport(item))
                        {
                            ShowTrace("Skipping data export from table " + item.SourceTableName);
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
                if (TableNamesToAutoExportData?.Count > 0)
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
                {
                    ShowTraceTableExportCount(tablesToExportData, databaseName);
                    return tablesToExportData;
                }

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

                ShowTraceTableExportCount(tablesToExportData, databaseName);
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
        private string CleanForCopyCommand(object columnValue)
        {
            if (columnValue == null)
                return string.Empty;

            var columnText = columnValue.ToString();

            // Quote the following characters
            // backslash itself, newline, carriage return, and tab

            return columnText.
                Replace("\\", "\\\\").
                Replace("\r", "\\r").
                Replace("\n", "\\n").
                Replace("\t", "\\t");
        }

        /// <summary>
        /// Replace any invalid characters in filename with underscores
        /// </summary>
        /// <remarks>
        /// Valid characters are:
        /// a-z, 0-9, underscore, space, equals sign, plus sign, minus sign, comma, period,
        /// semicolon, tilde, exclamation mark, and the symbols @ # $ % ^ &amp; ( ) { } [ ]
        ///</remarks>
        /// <param name="filename"></param>
        /// <returns>Updated filename</returns>
        protected string CleanNameForOS(string filename)
        {
            return mNonStandardOSChars.Replace(filename, "_");
        }

        /// <summary>
        /// Computes the incremental progress that has been made beyond currentTaskProgressAtStart,
        /// based on the number of items processed and the next overall progress level
        /// </summary>
        /// <param name="currentTaskProgressAtStart">Progress at the start of the current subtask (value between 0 and 100)</param>
        /// <param name="currentTaskProgressAtEnd">Progress at the start of the current subtask (value between 0 and 100)</param>
        /// <param name="subTaskProgress">Progress of the current subtask (value between 0 and 100)</param>
        /// <returns>Overall progress (value between 0 and 100)</returns>
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
        /// <returns>Class tracking the source and target column names for the table</returns>
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
                    dataColumnType = currentColumnType?.Name switch
                    {
                        "image" => DataColumnTypeConstants.ImageObject,
                        "timestamp" => DataColumnTypeConstants.BinaryArray,
                        _ => DataColumnTypeConstants.BinaryArray
                    };
                }
                else if (currentColumnType == Type.GetType("System.Guid"))
                {
                    dataColumnType = DataColumnTypeConstants.GUID;
                }
                else if (currentColumnType == Type.GetType("System.Boolean"))
                {
                    // This might be a binary column
                    dataColumnType = currentColumnType?.Name switch
                    {
                        "binary" or "bit" => DataColumnTypeConstants.BinaryByte,
                        _ => DataColumnTypeConstants.Text,
                    };
                }
                else if (currentColumnType == Type.GetType("System.var"))
                {
                    dataColumnType = currentColumnType?.Name switch
                    {
                        "sql_variant" => DataColumnTypeConstants.SqlVariant,
                        _ => DataColumnTypeConstants.GeneralObject
                    };
                }

                string delimiter;

                if (mOptions.ScriptingOptions.SaveDataAsInsertIntoStatements || mOptions.PgDumpTableData || dataExportParams.PgInsertEnabled)
                    delimiter = ", ";
                else
                    delimiter = "\t";

                var targetColumnName = GetTargetColumnName(columnMapInfo, currentColumnName, ref dataColumnType);

                var quotedColumnName = PossiblyQuoteName(targetColumnName, quoteWithSquareBrackets);

                if (dataColumnType != DataColumnTypeConstants.SkipColumn)
                {
                    if (columnIndex > 0 && dataExportParams.HeaderRowValues.Length > 0)
                    {
                        dataExportParams.HeaderRowValues.Append(delimiter);
                    }

                    if (mOptions.ScriptingOptions.SaveDataAsInsertIntoStatements || mOptions.PgDumpTableData || dataExportParams.PgInsertEnabled)
                    {
                        dataExportParams.HeaderRowValues.Append(quotedColumnName);
                    }
                    else
                    {
                        dataExportParams.HeaderRowValues.Append(targetColumnName);
                    }
                }

                if (dataColumnType == DataColumnTypeConstants.SkipColumn)
                {
                    dataExportParams.ColumnNameByIndex.Add(columnIndex, currentColumnName);
                    dataExportParams.ColumnNamesAndTypes.Add(new KeyValuePair<string, DataColumnTypeConstants>(currentColumnName, dataColumnType));
                }
                else
                {
                    dataExportParams.ColumnNameByIndex.Add(columnIndex, quotedColumnName);
                    dataExportParams.ColumnNamesAndTypes.Add(new KeyValuePair<string, DataColumnTypeConstants>(targetColumnName, dataColumnType));
                }

                columnIndex++;
            }

            return columnMapInfo;
        }

        /// <summary>
        /// Convert the object name to snake_case
        /// </summary>
        /// <param name="objectName"></param>
        public string ConvertNameToSnakeCase(string objectName)
        {
            if (!mAnyLowerMatcher.IsMatch(objectName))
            {
                // objectName contains no lowercase letters; simply change to lowercase and return
                return objectName.ToLower();
            }

            var match = mCamelCaseMatcher.Match(objectName);

            var updatedName = match.Success
                ? mCamelCaseMatcher.Replace(objectName, "${LowerLetter}_${UpperLetter}")
                : objectName;

            return updatedName.ToLower();
        }

        /// <summary>
        /// Create a bash script for loading data into a PostgreSQL database
        /// </summary>
        /// <param name="workingParams"></param>
        /// <param name="tablesToExportData"></param>
        private void CreateDataLoadScriptFile(WorkingParams workingParams, IEnumerable<TableDataExportInfo> tablesToExportData)
        {
            var dateFilterApplied = tablesToExportData.Any(item => item.FilterByDate);

            string shellScriptFile;

            if (mOptions.TableNameFilterSet.Count == 1)
            {
                shellScriptFile = string.Format("LoadDataTable_{0}.sh", CleanNameForOS(mOptions.TableNameFilterSet.First()));
            }
            else if (dateFilterApplied)
            {
                shellScriptFile = "LoadDataAppend.sh";
            }
            else
            {
                shellScriptFile = "LoadData.sh";
            }

            var scriptFilePath = Path.Combine(workingParams.OutputDirectoryPathCurrentDB, shellScriptFile);

            Console.WriteLine();
            OnStatusEvent("Creating file " + scriptFilePath);

            var currentUser = Environment.UserName.ToLower();

            using var writer = new StreamWriter(new FileStream(scriptFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite))
            {
                // Use Linux-compatible line feeds
                NewLine = "\n"
            };

            writer.WriteLine("#!/bin/sh");

            writer.WriteLine();
            writer.WriteLine("mkdir -p Done");

            var subdirectories = new SortedSet<string>();

            foreach (var scriptFileName in workingParams.DataLoadScriptFiles)
            {
                var lastSlashIndex = scriptFileName.LastIndexOf('/');

                if (lastSlashIndex > 0)
                {
                    var parentDirectory = scriptFileName.Substring(0, lastSlashIndex);

                    if (!subdirectories.Contains(parentDirectory))
                    {
                        writer.WriteLine();
                        writer.WriteLine("mkdir -p Done/" + parentDirectory);

                        subdirectories.Add(parentDirectory);
                    }
                }

                writer.WriteLine();
                writer.WriteLine("echo Processing " + scriptFileName);
                writer.WriteLine("psql -d dms -h localhost -U {0} -f {1}", currentUser, scriptFileName);

                var targetFilePath = "Done/" + scriptFileName;

                writer.WriteLine("test -f {0} && rm {0}", targetFilePath);
                writer.WriteLine("mv {0} {1} && echo '   ... moved to {1}'", scriptFileName, targetFilePath);
            }
        }

        /// <summary>
        /// Export the tables, views, procedures, etc. in the given database
        /// </summary>
        /// <param name="databaseName"></param>
        /// <param name="tablesForDataExport"></param>
        /// <param name="tableDataExportOrder">List of table names that defines the order that table data should be exported</param>
        /// <param name="databaseNotFound"></param>
        /// <param name="workingParams"></param>
        /// <returns>True if successful, false if an error</returns>
        protected abstract bool ExportDBObjectsAndTableData(
            string databaseName,
            IReadOnlyList<TableDataExportInfo> tablesForDataExport,
            IReadOnlyList<string> tableDataExportOrder,
            out bool databaseNotFound,
            out WorkingParams workingParams);

        /// <summary>
        /// Export data from the specified tables
        /// </summary>
        /// <param name="databaseName">Database name</param>
        /// <param name="tablesToExportData">Dictionary where keys are information on tables to export and values are the maximum number of rows to export</param>
        /// <param name="tableDataExportOrder">List of table names that defines the order that table data should be exported</param>
        /// <param name="workingParams">Working parameters</param>
        /// <returns>True if successful, false if an error</returns>
        protected bool ExportDBTableData(
            string databaseName,
            Dictionary<TableDataExportInfo, long> tablesToExportData,
            IReadOnlyList<string> tableDataExportOrder,
            WorkingParams workingParams)
        {
            try
            {
                if (tablesToExportData == null || tablesToExportData.Count == 0)
                {
                    ShowTrace("Skipping table data export since no tables have been selected for data export");
                    return true;
                }

                switch (tablesToExportData.Keys.Count)
                {
                    case 1:
                        OnDebugEvent("Exporting data from database {0}, table {1}", databaseName, tablesToExportData.First().Key);
                        break;

                    case 2:
                        OnDebugEvent("Exporting data from database {0}, tables {1} and {2}", databaseName, tablesToExportData.First().Key, tablesToExportData.Last().Key);
                        break;

                    default:
                        OnDebugEvent("Exporting data from database {0}, tables {1}, ...", databaseName, string.Join(", ", tablesToExportData.Keys.Take(5)));
                        break;
                }

                // Use tableDataExportOrder to define the order with which table data will be exported

                // The KeyValuePairs in this list are instances of TableDataExportInfo and the maximum number of rows to export for the given table
                var tablesToExportOrdered = new List<KeyValuePair<TableDataExportInfo, long>>();

                var storedTableInfo = new SortedSet<string>();

                foreach (var tableNameToFind in tableDataExportOrder)
                {
                    var alternateNameToFind = mOptions.TableDataSnakeCase ? ConvertNameToSnakeCase(tableNameToFind) : string.Empty;

                    foreach (var item in tablesToExportData)
                    {
                        if (StoreDataExportTableIfMatch(item.Key, item.Value, tableNameToFind, alternateNameToFind, tablesToExportOrdered, storedTableInfo))
                            break;
                    }
                }

                // Add tables that were not in tableDataExportOrder
                foreach (var item in tablesToExportData)
                {
                    if (storedTableInfo.Contains(item.Key.SourceTableName))
                        continue;

                    tablesToExportOrdered.Add(new KeyValuePair<TableDataExportInfo, long>(item.Key, item.Value));
                }

                foreach (var tableItem in tablesToExportOrdered)
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
                    CreateDataLoadScriptFile(workingParams, tablesToExportData.Keys);
                }

                return true;
            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.GeneralError, "Error in ExportDBTableData", ex);
                return false;
            }
        }

        private bool StoreDataExportTableIfMatch(
            TableDataExportInfo tableInfo,
            long maxRowsToExport,
            string tableNameToFind,
            string alternateNameToFind,
            ICollection<KeyValuePair<TableDataExportInfo, long>> tablesToExportOrdered,
            ISet<string> storedTableInfo)
        {
            if (string.IsNullOrWhiteSpace(tableInfo.SourceTableName))
                return false;

            var tableNameWithoutSchema = GetNameWithoutSchema(tableInfo.SourceTableName);
            var namesToCheck = new List<string>
            {
                tableNameWithoutSchema
            };

            var sourceNameSnakeCase = ConvertNameToSnakeCase(tableNameWithoutSchema);

            if (!sourceNameSnakeCase.Equals(tableNameWithoutSchema, StringComparison.OrdinalIgnoreCase))
            {
                namesToCheck.Add(sourceNameSnakeCase);
            }

            if (!string.IsNullOrWhiteSpace(tableInfo.TargetTableName))
                namesToCheck.Add(tableInfo.TargetTableName);

            var storeTable = namesToCheck.Any(tableName =>
                tableName.Equals(tableNameToFind, StringComparison.OrdinalIgnoreCase) ||
                tableName.Equals(alternateNameToFind, StringComparison.OrdinalIgnoreCase));

            if (!storeTable)
            {
                return false;
            }

            if (storedTableInfo.Contains(tableInfo.SourceTableName))
            {
                OnWarningEvent("Table {0} has already been added to the list of tables to export; skipping duplicate", tableInfo.SourceTableName);
                return false;
            }

            tablesToExportOrdered.Add(new KeyValuePair<TableDataExportInfo, long>(tableInfo, maxRowsToExport));
            storedTableInfo.Add(tableInfo.SourceTableName);
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
        protected abstract bool ExportDBTableData(string databaseName, TableDataExportInfo tableInfo, long maxRowsToExport, WorkingParams workingParams);

        /// <summary>
        /// Append a single row of results to the output file
        /// </summary>
        /// <param name="writer">Text file writer</param>
        /// <param name="dataExportParams"></param>
        /// <param name="delimitedRowValues">Text to write to the current line</param>
        /// <param name="columnCount">Number of columns</param>
        /// <param name="columnValues">Column values</param>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
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

                delimitedRowValues.Append(FormatValueForInsert(dataExportParams.ColumnNamesAndTypes, columnValues, columnIndex, pgInsertEnabled));
            }

            if (dataExportParams.PgInsertEnabled)
            {
                // Do not include a line feed here; we may need to append a comma
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
                    // Include a semicolon if creating INSERT INTO statements for databases other than SQL Server
                    if (mOptions.PostgreSQL)
                        delimitedRowValues.Append(");");
                    else
                        delimitedRowValues.Append(")");
                }

                writer.WriteLine(delimitedRowValues.ToString());
            }
        }

        /// <summary>
        /// Format a column value as a string, based on the data type of the database column
        /// </summary>
        /// <param name="columnNamesAndTypes">Column names and types</param>
        /// <param name="columnValues">Column values, as objects</param>
        /// <param name="columnIndex">Column index</param>
        /// <param name="pgInsertEnabled">True if using insert commands formatted as PostgreSQL compatible INSERT INTO statements</param>
        /// <returns>Value as a string</returns>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        protected string FormatValueForInsert(
            IReadOnlyList<KeyValuePair<string, DataColumnTypeConstants>> columnNamesAndTypes,
            IReadOnlyList<object> columnValues,
            int columnIndex,
            bool pgInsertEnabled)
        {

            switch (columnNamesAndTypes[columnIndex].Value)
            {
                case DataColumnTypeConstants.Numeric:
                    return columnValues[columnIndex].ToString();

                case DataColumnTypeConstants.Text:
                case DataColumnTypeConstants.DateTime:
                case DataColumnTypeConstants.GUID:
                    if (mOptions.PgDumpTableData && !pgInsertEnabled)
                    {
                        return CleanForCopyCommand(columnValues[columnIndex]);
                    }

                    if (mOptions.ScriptingOptions.SaveDataAsInsertIntoStatements || pgInsertEnabled)
                    {
                        return PossiblyQuoteText(columnValues[columnIndex].ToString());
                    }

                    return columnValues[columnIndex].ToString();

                case DataColumnTypeConstants.BinaryArray:
                    try
                    {
                        var bytData = (byte[])(Array)columnValues[columnIndex];
                        var formattedValue = "0x";
                        var dataFound = false;

                        foreach (var value in bytData)
                        {
                            if (dataFound || value != 0)
                            {
                                dataFound = true;
                                formattedValue += value.ToString("X2");
                            }
                        }

                        if (dataFound)
                        {
                            return formattedValue;
                        }

                        return formattedValue + "00";
                    }
                    catch (Exception)
                    {
                        return "[Byte]";
                    }

                case DataColumnTypeConstants.BinaryByte:
                    try
                    {
                        return string.Format("0x{0:X2}", Convert.ToByte(columnValues[columnIndex]));
                    }
                    catch (Exception)
                    {
                        return "[Byte]";
                    }

                case DataColumnTypeConstants.ImageObject:
                    return "[Image]";

                case DataColumnTypeConstants.GeneralObject:
                    return "[var]";

                case DataColumnTypeConstants.SqlVariant:
                    return "[Sql_Variant]";

                case DataColumnTypeConstants.SkipColumn:
                    // Ignore this column
                    return string.Empty;

                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        /// <summary>
        /// Retrieve a list of tables in the given database
        /// </summary>
        /// <param name="databaseName">Database to query</param>
        /// <param name="includeTableRowCounts">When true, determines the row count in each table</param>
        /// <param name="includeSystemObjects">When true, also returns system tables</param>
        /// <returns>Dictionary where keys are information on database tables and values are row counts (if includeTableRowCounts = true)</returns>
        public abstract Dictionary<TableDataExportInfo, long> GetDatabaseTables(string databaseName, bool includeTableRowCounts, bool includeSystemObjects);

        /// <summary>
        /// Generate the file name to exporting table data
        /// </summary>
        /// <param name="tableInfo"></param>
        /// <param name="dataExportParams"></param>
        /// <returns>Relative path to the output file</returns>
        private string GetFileNameForTableDataExport(
            TableDataExportInfo tableInfo,
            DataExportWorkingParams dataExportParams)
        {
            // Make sure output file name doesn't contain any invalid characters
            string cleanName;
            var defaultOwnerSchema = IsDefaultOwnerSchema(dataExportParams.TargetTableSchema);

            if (defaultOwnerSchema)
            {
                cleanName = CleanNameForOS(dataExportParams.TargetTableName + "_Data");
            }
            else
            {
                cleanName = CleanNameForOS(dataExportParams.TargetTableNameWithSchema + "_Data");
            }

            var suffix = tableInfo.FilterByDate ?
                             string.Format("_Since_{0:yyyy-MM-dd}", tableInfo.MinimumDate) :
                             string.Empty;

            var fileName = cleanName + suffix + ".sql";

            return defaultOwnerSchema ? fileName : Path.Combine(dataExportParams.TargetTableSchema, fileName);
        }

        /// <summary>
        /// Get the object name, without the schema
        /// </summary>
        /// <param name="objectName">Object name (with or without schema)</param>
        public string GetNameWithoutSchema(string objectName)
        {
            GetSchemaName(objectName, out var objectNameWithoutSchema);
            return objectNameWithoutSchema;
        }

        /// <summary>
        /// Retrieve a list of database names for the current server
        /// </summary>
        public abstract IEnumerable<string> GetServerDatabases();

        /// <summary>
        /// Get the list of databases from the current server
        /// </summary>
        protected abstract IEnumerable<string> GetServerDatabasesCurrentConnection();

        /// <summary>
        /// Get the output directory for the server info files
        /// </summary>
        /// <param name="serverName"></param>
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
                    ShowTrace("Creating directory " + serverInfoOutputDirectory.FullName);
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
        /// Get a FileInfo object for table data
        /// </summary>
        /// <param name="tableInfo"></param>
        /// <param name="dataExportParams"></param>
        /// <param name="workingParams"></param>
        /// <param name="relativeFilePath"></param>
        protected FileInfo GetTableDataOutputFile(
            TableDataExportInfo tableInfo,
            DataExportWorkingParams dataExportParams,
            WorkingParams workingParams,
            out string relativeFilePath)
        {
            relativeFilePath = GetFileNameForTableDataExport(tableInfo, dataExportParams);
            var outFilePath = Path.Combine(workingParams.OutputDirectory.FullName, relativeFilePath);

            if (string.IsNullOrWhiteSpace(outFilePath))
            {
                // Skip this table
                OnStatusEvent(
                    "GetFileNameForTableDataExport returned an empty output file name for table {0} in database {1}",
                    dataExportParams.SourceTableNameWithSchema, dataExportParams.DatabaseName);

                return null;
            }

            var tableDataOutputFile = new FileInfo(outFilePath);

            if (tableDataOutputFile.Directory == null)
            {
                OnWarningEvent("Cannot determine the parent directory of " + outFilePath);
                return null;
            }

            if (!mOptions.PreviewExport && !tableDataOutputFile.Directory.Exists)
            {
                tableDataOutputFile.Directory.Create();
            }

            OnDebugEvent("Writing table data to " + PathUtils.CompactPathString(tableDataOutputFile.FullName, 120));

            return tableDataOutputFile;
        }

        /// <summary>
        /// Get the target column name to use when exporting data
        /// </summary>
        /// <param name="columnMapInfo"></param>
        /// <param name="columnName"></param>
        /// <returns>Target column name</returns>
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
        /// <returns>Target column name</returns>
        protected string GetTargetColumnName(ColumnMapInfo columnMapInfo, string currentColumnName, ref DataColumnTypeConstants dataColumnType)
        {
            string targetColumnName;

            // Rename the column if defined in mOptions.ColumnMapForDataExport or if mOptions.TableDataSnakeCase is true
            if (columnMapInfo?.IsColumnDefined(currentColumnName) == true)
            {
                targetColumnName = columnMapInfo.GetTargetColumnName(currentColumnName);

                if (targetColumnName.Equals(DBSchemaExportTool.SKIP_FLAG, StringComparison.OrdinalIgnoreCase))
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
        /// <param name="dataExportParams">Source table: schema.table_name</param>
        /// <param name="tableInfo">Table info object</param>
        /// <param name="quoteWithSquareBrackets">When true, quote with square brackets; otherwise, quote with double quotes</param>
        /// <returns>Quoted target table name, with schema</returns>
        protected string GetQuotedTargetTableName(
            DataExportWorkingParams dataExportParams,
            TableDataExportInfo tableInfo,
            bool quoteWithSquareBrackets)
        {
            return GetTargetTableName(dataExportParams, tableInfo, quoteWithSquareBrackets, true);
        }

        /// <summary>
        /// Get the schema name from an object name
        /// </summary>
        /// <param name="objectNameWithSchema"></param>
        protected string GetSchemaName(string objectNameWithSchema)
        {
            return GetSchemaName(objectNameWithSchema, out _);
        }

        /// <summary>
        /// Get the schema name from an object name
        /// </summary>
        /// <param name="objectNameWithSchema"></param>
        /// <param name="objectName"></param>
        protected string GetSchemaName(string objectNameWithSchema, out string objectName)
        {
            string schemaName;

            var periodIndex = objectNameWithSchema.IndexOf('.');

            if (periodIndex == 0 && periodIndex < objectNameWithSchema.Length - 1)
            {
                schemaName = string.Empty;
                objectName = objectNameWithSchema.Substring(2);
            }
            else if (periodIndex > 0 && periodIndex < objectNameWithSchema.Length - 1)
            {
                schemaName = objectNameWithSchema.Substring(0, periodIndex);
                objectName = objectNameWithSchema.Substring(periodIndex + 1);
            }
            else
            {
                schemaName = string.Empty;
                objectName = objectNameWithSchema;
            }

            return schemaName;
        }

        /// <summary>
        /// Get the target table name to use when exporting data
        /// </summary>
        /// <param name="dataExportParams">Data export parameters, including source and target table info</param>
        /// <param name="tableInfo">Table info object</param>
        /// <param name="quoteWithSquareBrackets">When true, quote with square brackets; otherwise, quote with double quotes</param>
        /// <param name="alwaysQuoteNames">When true, always returned quoted schema.table_name</param>
        /// <returns>Target table name, with schema</returns>
        protected string GetTargetTableName(
            DataExportWorkingParams dataExportParams,
            TableDataExportInfo tableInfo,
            bool quoteWithSquareBrackets,
            bool alwaysQuoteNames)
        {
            if (string.IsNullOrWhiteSpace(tableInfo.TargetTableName))
            {
                dataExportParams.TargetTableSchema = GetSchemaName(dataExportParams.SourceTableNameWithSchema, out var targetTableName);
                dataExportParams.TargetTableName = targetTableName;

                if (!string.IsNullOrWhiteSpace(mOptions.DefaultSchemaName))
                {
                    // Override the schema name
                    dataExportParams.TargetTableSchema = mOptions.DefaultSchemaName;
                }

                if (mOptions.TableDataSnakeCase)
                {
                    dataExportParams.TargetTableName = ConvertNameToSnakeCase(dataExportParams.TargetTableName);
                }
            }
            else
            {
                dataExportParams.TargetTableSchema = tableInfo.TargetSchemaName;
                dataExportParams.TargetTableName = tableInfo.TargetTableName;
            }

            if (IsDefaultOwnerSchema(dataExportParams.TargetTableSchema))
            {
                return PossiblyQuoteName(dataExportParams.TargetTableName, quoteWithSquareBrackets, alwaysQuoteNames);
            }

            return string.Format(
                "{0}.{1}",
                PossiblyQuoteName(dataExportParams.TargetTableSchema, quoteWithSquareBrackets, alwaysQuoteNames),
                PossiblyQuoteName(dataExportParams.TargetTableName, quoteWithSquareBrackets, alwaysQuoteNames));
        }

        /// <summary>
        /// Obtain a timestamp in the form: 08/12/2006 23:01:20
        /// </summary>
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

        /// <summary>
        /// Return True if schemaName is "blank", "dbo", or "public"
        /// </summary>
        /// <param name="schemaName"></param>
        private bool IsDefaultOwnerSchema(string schemaName)
        {
            return string.IsNullOrWhiteSpace(schemaName) ||
                   schemaName.Equals("dbo", StringComparison.OrdinalIgnoreCase) ||
                   schemaName.Equals("public", StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Invoke the DBExportStarting event
        /// </summary>
        /// <param name="databaseName"></param>
        protected void OnDBExportStarting(string databaseName)
        {
            DBExportStarting?.Invoke(databaseName);
        }

        /// <summary>
        /// Invoke the PauseStatusChange event
        /// </summary>
        private void OnPauseStatusChange()
        {
            PauseStatusChange?.Invoke();
        }

        /// <summary>
        /// Invoke the ProgressComplete event
        /// </summary>
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
        protected string PossiblyQuoteName(string objectName, bool quoteWithSquareBrackets, bool alwaysQuoteNames = false)
        {
            if (!alwaysQuoteNames &&
                !mColumnCharNonStandardMatcher.Match(objectName).Success &&
                !mReservedWordMatcher.Match(objectName).Success)
            {
                return objectName;
            }

            if (quoteWithSquareBrackets)
            {
                // SQL Server quotes names with square brackets
                return '[' + objectName + ']';
            }

            // PostgreSQL quotes names with double quotes
            return '"' + objectName + '"';
        }

        /// <summary>
        /// Examine object names in a comma separated list, quoting any that are keywords or have non standard characters
        /// </summary>
        /// <param name="objectNames"></param>
        /// <param name="quoteWithSquareBrackets"></param>
        /// <returns>Comma separated list of quoted names</returns>
        protected string PossiblyQuoteNameList(string objectNames, bool quoteWithSquareBrackets)
        {
            var quotedNames = new List<string>();

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var objectName in objectNames.Split(','))
            {
                quotedNames.Add(PossiblyQuoteName(objectName, quoteWithSquareBrackets));
            }

            return string.Join(",", quotedNames);
        }

        /// <summary>
        /// Surround text with single quotes
        /// Additionally, if text contains single quotes, replace them with two single quotes
        /// </summary>
        /// <param name="text"></param>
        private string PossiblyQuoteText(string text)
        {
            return string.Format("'{0}'", text.Replace("'", "''"));
        }

        /// <summary>
        /// Request that scripting be paused
        /// </summary>
        /// <remarks>
        /// Useful when the scripting is running in another thread
        /// </remarks>
        public void RequestPause()
        {
            if (!(PauseStatus is PauseStatusConstants.Paused or PauseStatusConstants.PauseRequested))
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
            if (!(PauseStatus is PauseStatusConstants.Unpaused or PauseStatusConstants.UnpauseRequested))
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
        /// <param name="tableDataExportOrder">List of table names that defines the order that table data should be exported</param>
        private bool ScriptDBObjectsAndData(
            IReadOnlyCollection<string> databaseListToProcess,
            IReadOnlyList<TableDataExportInfo> tablesForDataExport,
            IReadOnlyList<string> tableDataExportOrder)
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
                    var currentDB = item;

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
                        currentDB = currentDbName;
                        OnDebugEvent(tasksToPerform + " from database " + currentDbName);
                        success = ExportDBObjectsAndTableData(currentDbName, tablesForDataExport, tableDataExportOrder, out var databaseNotFound, out var workingParams);

                        if (!warningsByDatabase.ContainsKey(currentDB))
                        {
                            warningsByDatabase.Add(currentDB, workingParams.WarningMessages);
                        }

                        if (!databaseNotFound && !success)
                        {
                            break;
                        }
                    }
                    else
                    {
                        // Database not actually present on the server; skip it
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
                        SetLocalError(
                            DBSchemaExportErrorCodes.DatabaseConnectionError,
                            string.Format("Database {0} not found on server {1}", currentDB, mOptions.ServerName));
                    }
                }

                ShowDatabaseWarnings(warningsByDatabase);

                return true;
            }
            catch (Exception ex)
            {
                SetLocalError(
                    DBSchemaExportErrorCodes.GeneralError,
                    "Error exporting DB schema objects: " + mOptions.OutputDirectoryPath, ex);
            }

            return false;
        }

        /// <summary>
        /// Scripts out the objects on the current server, including server info, database schema, and table data
        /// </summary>
        /// <param name="databaseList">Database names to export></param>
        /// <param name="tablesForDataExport">Table names for which data should be exported</param>
        /// <param name="tableDataExportOrder">List of table names that defines the order that table data should be exported</param>
        /// <returns>True if success, false if a problem</returns>
        public bool ScriptServerAndDBObjects(
            IReadOnlyList<string> databaseList,
            IReadOnlyList<TableDataExportInfo> tablesForDataExport,
            IReadOnlyList<string> tableDataExportOrder)
        {
            if (mOptions.NoSchema && mOptions.DisableDataExport)
            {
                OnDebugEvent("Schema and data export are disabled; not processing " + mOptions.ServerName);
                return true;
            }

            var validated = ValidateOptionsToScriptServerAndDBObjects(databaseList);

            if (!validated)
                return false;

            if (mOptions.Trace)
            {
                Console.WriteLine();
            }

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

            if (databaseList?.Count > 0)
            {
                var success = ScriptDBObjectsAndData(databaseList, tablesForDataExport, tableDataExportOrder);

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
        protected abstract bool ScriptServerObjects();

        /// <summary>
        /// Set the local error code
        /// </summary>
        /// <param name="errorCode"></param>
        /// <param name="message"></param>
        protected void SetLocalError(DBSchemaExportErrorCodes errorCode, string message)
        {
            SetLocalError(errorCode, message, null);
        }

        /// <summary>
        /// Set the local error code; provide an exception instance
        /// </summary>
        /// <param name="errorCode"></param>
        /// <param name="message"></param>
        /// <param name="ex"></param>
        protected void SetLocalError(DBSchemaExportErrorCodes errorCode, string message, Exception ex)
        {
            try
            {
                ErrorCode = errorCode;
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
            foreach (var databaseName in from item in warningsByDatabase.Keys orderby item select item)
            {
                var warningMessages = warningsByDatabase[databaseName];

                if (warningMessages.Count == 0)
                    continue;

                Console.WriteLine();
                OnWarningEvent("Warning summary for database {0}:", databaseName);

                foreach (var message in warningMessages)
                {
                    OnWarningEvent("  " + message);
                }
            }
        }

        /// <summary>
        /// When true, show trace messages
        /// </summary>
        /// <param name="message"></param>
        protected void ShowTrace(string message)
        {
            if (mOptions.Trace)
            {
                OnDebugEvent(message);
            }
        }

        private void ShowTraceTableExportCount(Dictionary<TableDataExportInfo, long> tablesToExportData, string databaseName)
        {
            var tableText = tablesToExportData.Count == 1 ? "table" : "tables";
            ShowTrace(string.Format(
                "Will export data from {0} {1} in database {2}", tablesToExportData.Count, tableText, databaseName));
        }

        /// <summary>
        /// Check whether mOptions.SchemaNameSkipList contains the schema name
        /// </summary>
        /// <param name="schemaName"></param>
        /// <returns>True if the schema should be ignored</returns>
        protected bool SkipSchema(string schemaName)
        {
            return SkipSchema(mOptions, schemaName);
        }

        /// <summary>
        /// Check whether options.SchemaNameSkipList contains the schema name
        /// </summary>
        /// <param name="options"></param>
        /// <param name="schemaName"></param>
        /// <returns>True if the schema should be ignored</returns>
        private static bool SkipSchema(SchemaExportOptions options, string schemaName)
        {
            return options.SchemaNameSkipList.Contains(schemaName);
        }

        /// <summary>
        /// Determine whether this table should be skipped when exporting data
        /// </summary>
        /// <param name="tableInfo"></param>
        /// <returns>True (meaning to skip the table) if the table has "&lt;skip&gt;" for the TargetTableName</returns>
        public bool SkipTableForDataExport(TableDataExportInfo tableInfo)
        {
            var skipTable = SkipTableForDataExport(mOptions, tableInfo);

            if (skipTable)
                return true;

            return !mObjectNameMatcher.IsMatch(tableInfo.SourceTableName);
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

            return tableInfo.TargetTableName?.Equals(DBSchemaExportTool.SKIP_FLAG, StringComparison.OrdinalIgnoreCase) == true;
        }

        /// <summary>
        /// Determine whether this table should be skipped when exporting data
        /// </summary>
        /// <param name="tablesForDataExport"></param>
        /// <param name="candidateTableSourceTableName"></param>
        /// <param name="tableInfo"></param>
        /// <returns>
        /// True (meaning to skip the table) if the table name is defined in tablesForDataExport and has "&lt;skip&gt;" for the TargetTableName
        /// </returns>
        private bool SkipTableForDataExport(
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
        /// Store the RegEx specs to use to find tables from which data should be exported
        /// </summary>
        /// <param name="tableNameRegExSpecs"></param>
        public void StoreTableNameRegexToAutoExportData(SortedSet<string> tableNameRegExSpecs)
        {
            ShowTrace(string.Format("Storing {0} default RegEx specs for finding tables for data export", tableNameRegExSpecs.Count));

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
            ShowTrace(string.Format("Storing {0} default names for finding tables for data export", tableNames.Count));

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
            var passesFilter = TableNamePassesFilters(mOptions, tableName);
            return passesFilter && mObjectNameMatcher.IsMatch(tableName);
        }

        /// <summary>
        /// Check whether options.TableNameFilterSet is empty, or contains the table name
        /// </summary>
        /// <param name="options"></param>
        /// <param name="tableName"></param>
        /// <returns>True if the filter set is empty, or contains the table name; otherwise false</returns>
        private static bool TableNamePassesFilters(SchemaExportOptions options, string tableName)
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
        private bool ValidateOptionsToScriptServerAndDBObjects(IReadOnlyCollection<string> databaseList)
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
                else if (databaseList.Count > 1)
                {
                    // Force CreateDirectoryForEachDB to true
                    mOptions.CreateDirectoryForEachDB = true;
                }
            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.DatabaseConnectionError, "Error validating the Schema Export Options", ex);
                return false;
            }

            if (string.IsNullOrWhiteSpace(mOptions.ObjectNameFilter))
            {
                mObjectNameMatcher = new Regex(".+", RegexOptions.Compiled);
            }
            else
            {
                try
                {
                    mObjectNameMatcher = new Regex(mOptions.ObjectNameFilter, RegexOptions.Compiled | RegexOptions.IgnoreCase);
                }
                catch (Exception ex)
                {
                    OnErrorEvent(string.Format(
                        "Invalid text defined for the object name filter, '{0}'; " +
                        "should be a series of letters or valid RegEx", mOptions.ObjectNameFilter), ex);
                    return false;
                }
            }

            return ValidateOutputOptions();
        }

        /// <summary>
        /// Validate the output directory for the current database
        /// </summary>
        /// <param name="databaseName"></param>
        /// <param name="workingParams"></param>
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
                    workingParams.OutputDirectoryPathCurrentDB = mOptions.OutputDirectoryPath;
                }

                workingParams.OutputDirectory = new DirectoryInfo(workingParams.OutputDirectoryPathCurrentDB);

                // Create the directory if it doesn't exist
                if (!workingParams.OutputDirectory.Exists && !mOptions.PreviewExport)
                {
                    ShowTrace("Creating directory " + workingParams.OutputDirectory.FullName);
                    workingParams.OutputDirectory.Create();
                }

                // Add/update the dictionary
                SchemaOutputDirectories[databaseName] = workingParams.OutputDirectoryPathCurrentDB;

                return true;
            }
            catch (Exception)
            {
                SetLocalError(
                    DBSchemaExportErrorCodes.GeneralError,
                    "Error validating or creating directory " + workingParams.OutputDirectoryPathCurrentDB);

                return false;
            }
        }

        /// <summary>
        /// Validate output options
        /// </summary>
        private bool ValidateOutputOptions()
        {
            mOptions.ValidateOutputOptions();

            try
            {
                // Confirm that the output directory exists
                var outputDirectory = new DirectoryInfo(mOptions.OutputDirectoryPath);

                if (outputDirectory.Exists || mOptions.PreviewExport)
                    return true;

                ShowTrace("Creating directory " + outputDirectory.FullName);

                // Try to create it
                outputDirectory.Create();

                return true;
            }
            catch (Exception ex)
            {
                SetLocalError(
                    DBSchemaExportErrorCodes.OutputDirectoryAccessError,
                    "Output directory could not be created: " + mOptions.OutputDirectoryPath, ex);

                return false;
            }
        }

        /// <summary>
        /// Return true if we have a valid server connection
        /// </summary>
        protected abstract bool ValidServerConnection();

        /// <summary>
        /// Write lines text to a file
        /// </summary>
        /// <param name="outputFile"></param>
        /// <param name="scriptInfo"></param>
        /// <param name="autoAddGoStatements"></param>
        /// <returns>True if success, false if an error</returns>
        protected bool WriteTextToFile(
            FileInfo outputFile,
            IEnumerable<string> scriptInfo,
            bool autoAddGoStatements = true)
        {
            return WriteTextToFile(
                outputFile.Directory,
                Path.GetFileNameWithoutExtension(outputFile.Name),
                scriptInfo,
                autoAddGoStatements,
                outputFile.Extension);
        }

        /// <summary>
        /// Write lines text to a file
        /// </summary>
        /// <param name="outputDirectory"></param>
        /// <param name="objectName"></param>
        /// <param name="scriptInfo"></param>
        /// <param name="autoAddGoStatements"></param>
        /// <param name="fileExtension"></param>
        /// <returns>True if success, false if an error</returns>
        protected bool WriteTextToFile(
            FileSystemInfo outputDirectory,
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

                using var writer = new StreamWriter(new FileStream(outFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite));

                foreach (var item in scriptInfo)
                {
                    writer.WriteLine(item);

                    if (autoAddGoStatements)
                    {
                        writer.WriteLine("GO");
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
