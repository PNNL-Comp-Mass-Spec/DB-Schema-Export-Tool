using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using PRISM;
using TableNameMapContainer;

namespace DB_Schema_Export_Tool
{
    /// <summary>
    /// Base class for exporting database schema and data
    /// </summary>
    public abstract class DBSchemaExporterBase : EventNotifier
    {
        // Ignore Spelling: ag, dbo, dev, dms, dtproperties, fi, grep, localhost, Matchers, mkdir, mv, PostgreSQL, psql, sql, Subtask, sysdiagrams, unpause, unpaused
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
        /// Text to append to the end of file names with exported table data
        /// </summary>
        public const string TABLE_DATA_FILE_SUFFIX = "_Data";

        /// <summary>
        /// Text to append to the end of file names with commands to remove extra rows from the target table
        /// </summary>
        public const string DELETE_EXTRA_ROWS_FILE_SUFFIX = "_DeleteExtraRows.sql";

        /// <summary>
        /// Text to append to the end of file names with a command to truncate target table data
        /// </summary>
        public const string DELETE_ALL_ROWS_FILE_SUFFIX = "_DeleteAllRows.sql";

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
            /// IP Address
            /// </summary>
            IPAddress = 9,

            /// <summary>
            /// SkipColumn
            /// </summary>
            SkipColumn = 10
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
        /// Match any character that is not a letter, number, or underscore
        /// </summary>
        /// <remarks>
        /// If a match is found, quote the name with double quotes (PostgreSQL) or square brackets (SQL Server)
        /// </remarks>
        private readonly Regex mColumnCharNonStandardMatcher;

        /// <summary>
        /// True when connected to a server
        /// </summary>
        protected bool mConnectedToServer;

        /// <summary>
        /// Current server info
        /// </summary>
        protected ServerConnectionInfo mCurrentServerInfo;

        /// <summary>
        /// Disallowed characters for file names
        /// </summary>
        private readonly Regex mNonStandardOSChars;

        /// <summary>
        /// Object name Reg Ex match list
        /// </summary>
        protected readonly List<Regex> mObjectNameMatchers;

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
        /// Characters used to quote object names
        /// </summary>
        private readonly Regex mQuoteChars;

        /// <summary>
        /// Matches reserved words (keywords)
        /// </summary>
        /// <remarks>
        /// If a match is found, quote the name with double quotes (PostgreSQL) or square brackets (SQL Server)
        /// </remarks>
        private readonly Regex mReservedWordMatcher;

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
        /// <param name="options">Options</param>
        protected DBSchemaExporterBase(SchemaExportOptions options)
        {
            mOptions = options;

            SchemaOutputDirectories = new Dictionary<string, string>();
            TableNamesToAutoExportData = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);
            TableNameRegexToAutoExportData = new SortedSet<string>();

            mColumnCharNonStandardMatcher = new Regex("[^a-z0-9_]", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);

            // The reserved words in this RegEx are a combination of PostgreSQL reserved words and SQL standard reserved words (SQL:2016)
            // See https://www.postgresql.org/docs/current/sql-keywords-appendix.html

            mReservedWordMatcher = new Regex(@"\b(ALL|ALTER|ANALYSE|ANALYZE|ARRAY|ASC|AVG|BEGIN|BETWEEN|CALL|CALLED|CASE|CAST|CHAR|CHECK|COLUMN|COMMIT|CONSTRAINT|CONTAINS|CONVERT|COPY|COUNT|CREATE|CROSS|CUBE|CURRENT|DATE|DAY|DEFAULT|DELETE|DESC|DISTINCT|DOUBLE|DROP|ELEMENT|EMPTY|EQUALS|ESCAPE|EXTERNAL|FALSE|FILTER|FROM|FULL|FUNCTION|GROUP|HAVING|IN|INNER|INTEGER|INTERVAL|INTO|JOIN|LANGUAGE|LATERIAL|LEADING|LEFT|LIKE|LIMIT|LOCAL|LOCALTIME|LOCALTIMESTAMP|MATCH|MAX|MEMBER|METHOD|MIN|MINUTE|MODULE|MONTH|NATURAL|NEW|NULL|NUMERIC|OFFSET|OLD|ON|ONE|ONLY|OPEN|OR|ORDER|OUT|OUTER|OVER|OVERLAPS|PARAMETER|PARTITION|PER|PERCENT|PERIOD|PLACING|POSITION|POWER|PRIMARY|RANGE|RANK|REFERENCES|RESULT|RIGHT|ROW|ROWS|SCOPE|SECOND|SELECT|SOME|START|SUM|SYMMETRIC|TABLE|THEN|TIMESTAMP|TO|TRAILING|TRUE|UNION|UNIQUE|UNKNOWN|UPDATE|UPPER|USER|USING|VALUE|VALUES|VERBOSE|WHEN|WHERE|WINDOW|WITH|YEAR)\b", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);

            mNonStandardOSChars = new Regex(@"[^a-z0-9_ =+-,.;~!@#$%^&(){}\[\]]", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);

            // This list will get updated later if mOptions.ObjectNameFilter is not empty
            mObjectNameMatchers = new List<Regex>
            {
                new(".+", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline)
            };

            mQuoteChars = new Regex(@"[""\[\]]", RegexOptions.Compiled);

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
        /// Append footers that follow the value list for an Insert Into statement
        /// </summary>
        /// <param name="writer">Text file writer</param>
        /// <param name="dataExportParams">Data export parameters</param>
        protected void AppendPgExportFooters(TextWriter writer, DataExportWorkingParams dataExportParams)
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

        /// <summary>
        /// Append a SQL statement for setting a table's sequence value to the maximum current ID in the table
        /// </summary>
        /// <param name="writer">Text file writer</param>
        /// <param name="dataExportParams">Data export parameters</param>
        /// <param name="identityColumnIndex">Identity column index</param>
        protected void AppendPgExportSetSequenceValue(TextWriter writer, DataExportWorkingParams dataExportParams, int identityColumnIndex)
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
                    // In addition, if tablesForDataExport is not empty, skip any table not in the list

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

                        if (!string.IsNullOrWhiteSpace(tableInfo.TargetTableName))
                        {
                            // This table has a new name in the target database
                            // Alternatively, the new name is the same as the input name, but we want to avoid snake case errors
                            // (e.g. prevent T_MaxQuant_Mods converting to t_max_quant_mods)
                            candidateTable.TargetTableName = tableInfo.TargetTableName;
                        }

                        if (!string.IsNullOrWhiteSpace(tableInfo.TargetSchemaName))
                        {
                            // This table has a new schema name in the target database
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

                        // userDefinedTableNames is a sorted set, and thus we can call .Add() even if it already has the table name
                        userDefinedTableNames.Add(item.SourceTableName);
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
        /// <param name="columnValue">Column value</param>
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
        /// <param name="filename">Filename</param>
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
        /// <param name="itemsProcessed">Number of items processed</param>
        /// <param name="totalItems">Total item count</param>
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
        /// <param name="sourceTableName">Source table name</param>
        /// <param name="quoteWithSquareBrackets">When true, quote names with square brackets; otherwise, quote with double quotes</param>
        /// <param name="dataExportParams">Data export parameters</param>
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
                else if (currentColumnType.FullName == "System.Net.IPAddress")
                {
                    dataColumnType = DataColumnTypeConstants.IPAddress;
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
        /// <param name="objectName">Object name</param>
        public string ConvertNameToSnakeCase(string objectName)
        {
            return TableColumnNameMapContainer.NameUpdater.ConvertNameToSnakeCase(objectName);
        }

        /// <summary>
        /// Create a bash script for loading data into a PostgreSQL database
        /// </summary>
        /// <remarks>If table data export order file was provided, tables will have been added to workingParams.DataLoadScriptFiles in the specified order</remarks>
        /// <param name="workingParams">Working parameters</param>
        /// <param name="tablesToExportData">Dictionary where keys are information on tables to export and values are the maximum number of rows to export</param>
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

            var dbUser = string.IsNullOrWhiteSpace(mOptions.ScriptUser) ? Environment.UserName.ToLower() : mOptions.ScriptUser;
            var dbName = string.IsNullOrWhiteSpace(mOptions.ScriptDB) ? "dms" : mOptions.ScriptDB;
            var dbHost = string.IsNullOrWhiteSpace(mOptions.ScriptHost) ? "localhost" : mOptions.ScriptHost;
            var dbPort = mOptions.ScriptPort == DBSchemaExporterPostgreSQL.DEFAULT_PORT ? string.Empty : " -p " + mOptions.ScriptPort;

            // The following uses 2>&1 to redirect standard error to standard output
            // This is required to allow tee to store error messages to the text file
            const string PSQL_FORMAT_STRING = "psql -d {0} -h {1} -U {2} {3} -f {4} 2>&1 | tee -a {5}";

            var dataImportLogFile = string.Format("ImportLog_{0:yyyy-MM-dd_HHmm}.txt", DateTime.Now);

            using var writer = new StreamWriter(new FileStream(scriptFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite));

            // Use Linux-compatible line feeds
            writer.NewLine = "\n";

            writer.WriteLine("#!/bin/sh");

            writer.WriteLine();
            writer.WriteLine("mkdir -p Done");
            writer.WriteLine();

            var sortedScriptFiles = GetSortedDataLoadScriptFiles(workingParams);

            var statementLogControlFiles = new PgStatementLogControlFiles(workingParams.OutputDirectoryPathCurrentDB);

            var updatePgStatementLogDuration = sortedScriptFiles.Count > 0 && mOptions.DisableStatementLogging;

            if (updatePgStatementLogDuration)
            {
                CreateStatementLogControlFiles(statementLogControlFiles);

                writer.WriteLine(
                    "echo Changing PostgreSQL setting log_min_duration_statement to {0} on {1} | tee -a {2}",
                    -1, dbHost, dataImportLogFile);

                writer.WriteLine("echo ''");
                writer.WriteLine();

                writer.WriteLine(PSQL_FORMAT_STRING, dbName, dbHost, dbUser, dbPort, Path.GetFileName(statementLogControlFiles.DisablePgStatementLogging), dataImportLogFile);
                writer.WriteLine("sleep 1");
                writer.WriteLine(PSQL_FORMAT_STRING, dbName, dbHost, dbUser, dbPort, Path.GetFileName(statementLogControlFiles.ShowLogMinDurationValue), dataImportLogFile);
            }

            foreach (var relativeScriptFilePath in sortedScriptFiles)
            {
                writer.WriteLine();
                writer.WriteLine("if test -f {0}; then", relativeScriptFilePath);
                writer.WriteLine("    echo Processing {0} | tee -a {1}", relativeScriptFilePath, dataImportLogFile);

                writer.WriteLine("    " + PSQL_FORMAT_STRING, dbName, dbHost, dbUser, dbPort, relativeScriptFilePath, dataImportLogFile);

                var lastSlashIndex = relativeScriptFilePath.LastIndexOf('/');

                string targetFilePath;

                if (lastSlashIndex > 0)
                {
                    targetFilePath = "Done" + relativeScriptFilePath.Substring(lastSlashIndex);
                }
                else
                {
                    targetFilePath = "Done/" + relativeScriptFilePath;
                }

                writer.WriteLine("    test -f {0} && rm {0}", targetFilePath);
                writer.WriteLine("    mv {0} {1} && echo '   ... moved to {1}' | tee -a {2}", relativeScriptFilePath, targetFilePath, dataImportLogFile);
                writer.WriteLine("    sleep 0.33");
                writer.WriteLine("else");
                writer.WriteLine("    echo Skipping missing file: {0} | tee -a {1}", relativeScriptFilePath, dataImportLogFile);
                writer.WriteLine("    sleep 0.1");
                writer.WriteLine("fi");

                writer.WriteLine("echo ''");
            }

            writer.WriteLine();

            if (updatePgStatementLogDuration)
            {
                writer.WriteLine(
                    "echo Changing PostgreSQL setting log_min_duration_statement to {0} on {1} | tee -a {2}",
                    mOptions.StatementLoggingMinDurationAfterLoad, dbHost, dataImportLogFile);

                writer.WriteLine("echo ''");
                writer.WriteLine();

                writer.WriteLine(PSQL_FORMAT_STRING, dbName, dbHost, dbUser, dbPort, Path.GetFileName(statementLogControlFiles.EnablePgStatementLogging), dataImportLogFile);
                writer.WriteLine("sleep 1");
                writer.WriteLine(PSQL_FORMAT_STRING, dbName, dbHost, dbUser, dbPort, Path.GetFileName(statementLogControlFiles.ShowLogMinDurationValue), dataImportLogFile);
                writer.WriteLine();
            }

            // Show error messages in the data import log file
            // Use ag (The Silver Searcher) if it exists, otherwise use grep
            // If ag does exist, also move the .sql files back from the Done directory to the working directory

            writer.WriteLine("# Look for lines with \"error\"");
            writer.WriteLine();
            writer.WriteLine("if command -v ag > /dev/null; then");
            writer.WriteLine("    ag -i \"error\" {0}", dataImportLogFile);
            writer.WriteLine();
            writer.WriteLine("    # For tables that had an error, move the .sql file back to the working directory");
            writer.WriteLine("    # Use a RegEx to extract out the .sql file names and write them to a text file");
            writer.WriteLine("    ag -i -o \"(?<=psql:)([^:]+)(?=.+error)\" {0} | uniq | sort > FilesToRetry.txt", dataImportLogFile);
            writer.WriteLine();
            writer.WriteLine("    if [ ! -s FilesToRetry.txt ]; then");
            writer.WriteLine("        # The file is empty.");
            writer.WriteLine("        echo \"No errors were found\"");
            writer.WriteLine("        exit");
            writer.WriteLine("    fi");
            writer.WriteLine();
            writer.WriteLine("    # Remove directory names from lines in FilesToRetry.txt");
            writer.WriteLine("    ag -i -o \"[a-z._]+$\" FilesToRetry.txt > FilesToRetry2.txt");
            writer.WriteLine();
            writer.WriteLine("    # Read the text file line-by-line");
            writer.WriteLine("    while IFS= read -r line; do");
            writer.WriteLine("        if test -f \"Done/$line\"; then");
            writer.WriteLine("            echo \"Move $line back to this directory\"");
            writer.WriteLine("            mv Done/$line .");
            writer.WriteLine("        else");
            writer.WriteLine("            if test -f \"$line\"; then");
            writer.WriteLine("                echo \"File already exists: $line\"");
            writer.WriteLine("            else");
            writer.WriteLine("                echo \"File not found in subdirectory or the current directory: $line\"");
            writer.WriteLine("            fi");
            writer.WriteLine("        fi");
            writer.WriteLine("    done < FilesToRetry2.txt");
            writer.WriteLine();
            writer.WriteLine("    echo \"\"");
            writer.WriteLine("    echo \"Commands to retry loading data\"");
            writer.WriteLine();
            writer.WriteLine("    while IFS= read -r line; do");
            writer.WriteLine("        echo \"psql -d dmsdev -h localhost -U d3l243  -f $line 2>&1 | tee -a RetryLog.txt\"");
            writer.WriteLine("    done < FilesToRetry2.txt");
            writer.WriteLine();

            writer.WriteLine("else");
            writer.WriteLine("    grep -i \"error\" {0}", dataImportLogFile);
            writer.WriteLine("fi");
        }

        /// <summary>
        /// Create SQL files for changing PostgreSQL setting 'log_min_duration_statement'
        /// </summary>
        /// <param name="statementLogControlFiles">PgStatement log control files</param>
        private void CreateStatementLogControlFiles(PgStatementLogControlFiles statementLogControlFiles)
        {
            using (var writer = new StreamWriter(new FileStream(statementLogControlFiles.DisablePgStatementLogging, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
            {
                writer.WriteLine("-- Show the current setting");
                writer.WriteLine("select setting \"log_min_duration_statement (msec), original value\" from pg_settings where name = 'log_min_duration_statement';");
                writer.WriteLine();
                writer.WriteLine("-- Disable logging long queries");
                writer.WriteLine("alter system set log_min_duration_statement = -1;");
                writer.WriteLine();
                writer.WriteLine("-- Apply the changes");
                writer.WriteLine("select pg_reload_conf();");

                // Ideally we would now use a query to show the new value for the setting, but the change does not take place immediately
                // Therefore, the bash script will sleep for one second, then show the value for the setting
            }

            using (var writer = new StreamWriter(new FileStream(statementLogControlFiles.EnablePgStatementLogging, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
            {
                writer.WriteLine("-- Re-enable logging long queries");
                writer.WriteLine("alter system set log_min_duration_statement = {0};", mOptions.StatementLoggingMinDurationAfterLoad);
                writer.WriteLine();
                writer.WriteLine("-- Apply the changes");
                writer.WriteLine("select pg_reload_conf();");
            }

            using (var writer = new StreamWriter(new FileStream(statementLogControlFiles.ShowLogMinDurationValue, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
            {
                writer.WriteLine("-- Show the current value of the log_min_duration_statement setting ");
                writer.WriteLine("select setting as \"log_min_duration_statement (msec), new value\" from pg_settings where name = 'log_min_duration_statement';");
            }
        }

        /// <summary>
        /// Query the database to obtain the primary key information for every table
        /// Store in workingParams.PrimaryKeysByTable
        /// </summary>
        /// <param name="workingParams">Working parameters</param>
        /// <returns>True if successful, false if an error</returns>
        public abstract bool GetPrimaryKeyInfoFromDatabase(WorkingParams workingParams);

        /// <summary>
        /// If option DeleteExtraRows was enabled, workingParams.DataLoadScriptFiles will have a mix of scripts to remove extra rows and to load new data
        /// This method extracts the delete extra data scripts, reverses their order, then appends the load data scripts to the end
        /// The reason for this is to delete extra rows from tables in the reverse order specified by the table data export order file
        /// </summary>
        /// <param name="workingParams">Working parameters</param>
        /// <returns>List of non-interleaved relative file paths</returns>
        private List<string> GetSortedDataLoadScriptFiles(WorkingParams workingParams)
        {
            var dataLoadScriptFiles = new List<string>();
            var removeExtrasScriptFiles = new List<string>();

            foreach (var relativePath in workingParams.DataLoadScriptFiles)
            {
                if (relativePath.EndsWith(DELETE_EXTRA_ROWS_FILE_SUFFIX) || relativePath.EndsWith(DELETE_ALL_ROWS_FILE_SUFFIX))
                    removeExtrasScriptFiles.Add(relativePath);
                else
                    dataLoadScriptFiles.Add(relativePath);
            }

            removeExtrasScriptFiles.Reverse();

            var sortedScriptFiles = new List<string>();
            sortedScriptFiles.AddRange(removeExtrasScriptFiles);
            sortedScriptFiles.AddRange(dataLoadScriptFiles);

            return sortedScriptFiles;
        }

        /// <summary>
        /// Get target table primary key column names
        /// </summary>
        /// <param name="columnMapInfo">Class tracking the source and target column names for the table</param>
        /// <param name="sourceColumnNames">Primary key column names in the source table</param>
        /// <param name="targetPrimaryKeyColumns">Output: primary key column names in the target table</param>
        /// <returns>Target table primary key column names, as a comma-separated list</returns>
        protected string GetTargetPrimaryKeyColumnNames(
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

                    // For T_Job_Steps, names in sourceColumnNames are "job" and "step" when this for loop is reached

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

        /// <summary>
        /// Export the tables, views, procedures, etc. in the given database
        /// </summary>
        /// <param name="databaseName">Database name</param>
        /// <param name="tablesForDataExport">Tables to export data from</param>
        /// <param name="tableDataExportOrder">List of table names that defines the order that table data should be exported (table name only, not schema)</param>
        /// <param name="databaseNotFound">Output: true if the database does not exist on the server (or is inaccessible)</param>
        /// <param name="workingParams">Working parameters</param>
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
        /// <param name="tableDataExportOrder">List of table names that defines the order that table data should be exported (table name only, not schema)</param>
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

                var storedTableInfo = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);

                foreach (var tableNameToFind in tableDataExportOrder)
                {
                    var alternateNameToFind = mOptions.TableDataSnakeCase ? ConvertNameToSnakeCase(tableNameToFind) : string.Empty;

                    // ReSharper disable once ForeachCanBePartlyConvertedToQueryUsingAnotherGetEnumerator
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

                StreamWriter memoryLogger;
                if (mOptions.DataExportLogMemoryUsage)
                {
                    var memoryUsageLogFilePath = mOptions.GetMemoryUsageLogFilePath();

                    memoryLogger = new StreamWriter(new FileStream(memoryUsageLogFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
                    {
                        AutoFlush = true
                    };

                    memoryLogger.WriteLine("{0}\t{1}\t{2}\t{3}", "Table", "Elapsed Time (sec)", "Memory Usage (MB)", "Memory Usage (GB)");
                }
                else
                {
                    memoryLogger = null;
                }

                var startTime = DateTime.UtcNow;

                foreach (var tableItem in tablesToExportOrdered)
                {
                    var tableInfo = tableItem.Key;
                    var maxRowsToExport = tableItem.Value;

                    /*
                     * Uncomment to only script out certain tables

                    switch (tableItem.Key.SourceTableName)
                    {
                        case "public.t_yes_no":
                        case "public.t_misc_paths":
                        case "cap.t_local_processors":
                        case "cap.t_task_step_state_name":
                        case "sw.t_scripts":
                        case "sw.t_step_tools":
                            break;
                        default:
                            if (tableItem.Key.SourceTableName.StartsWith("squeeze."))
                                break;

                            continue;
                    }

                    */

                    var success = ExportDBTableData(databaseName, tableInfo, maxRowsToExport, workingParams);

                    if (!success)
                    {
                        if (mOptions.ScriptPgLoadCommands)
                        {
                            CreateDataLoadScriptFile(workingParams, tablesToExportData.Keys);
                        }

                        return false;
                    }

                    workingParams.ProcessCount++;

                    CheckPauseStatus();

                    if (mAbortProcessing)
                    {
                        OnWarningEvent("Aborted processing");
                        return true;
                    }

                    if (!mOptions.DataExportLogMemoryUsage)
                        continue;

                    var elapsedTime = DateTime.UtcNow.Subtract(startTime).TotalSeconds;

                    // ReSharper disable once InconsistentNaming
                    var memoryUsageMB = mOptions.SystemMemoryMB - SystemInfo.GetFreeMemoryMB() - mOptions.SystemMemoryUsageAtStart;

                    memoryLogger?.WriteLine("{0}\t{1:0.0}\t{2:0}\t{3:0.00}", tableInfo.SourceTableName, elapsedTime, memoryUsageMB, memoryUsageMB / 1024);
                }

                if (mOptions.ScriptPgLoadCommands)
                {
                    // Note that the script file info is tracked by List workingParams.DataLoadScriptFiles
                    // If TableDataExportOrder was defined, the items in DataLoadScriptFiles will match the specified order

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
        protected string ExportDBTableDataInit(
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
                // Exporting data from PostgreSQL or from SQL Server and using insert commands that are formatted as
                // PostgreSQL compatible INSERT INTO statements using the ON CONFLICT (key_column) DO UPDATE SET syntax

                var primaryKeyColumnList = ResolvePrimaryKeys(dataExportParams, workingParams, tableInfo, columnMapInfo);

                bool deleteExtrasThenAddNew;
                bool deleteExtrasUsingPrimaryKey;
                bool useTruncateTable;

                if (tableInfo.PrimaryKeyColumns.Count == dataExportParams.ColumnNamesAndTypes.Count)
                {
                    if (dataRowCount < 5000 && tableInfo.PrimaryKeyColumns.Count <= 2)
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

                    headerRows.Add("SET IDENTITY_INSERT " + dataExportParams.QuotedTargetTableNameWithSchema + " ON;");
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
                // Format data exported from SQL Server as PostgreSQL COPY commands

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
        /// Generate SQL to delete extra rows from the target table
        /// </summary>
        /// <param name="dataExportParams">Data export parameters</param>
        /// <param name="queryResults">Query results</param>
        protected void ExportDBTableDataDeleteExtraRows(DataExportWorkingParams dataExportParams, DataSet queryResults)
        {
            // If just one column in the table, use:
            //   DELETE FROM t_target_table
            //   WHERE NOT id in (1, 2, 3);

            // If multiple columns, use:
            //   DELETE FROM t_target_table
            //   WHERE NOT (
            //       id = 1 AND value = 'Item A' OR
            //       id = 2 AND value = 'Item B' OR
            //       id = 3 AND value = 'Item C');

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
                        // Using the form "WHERE NOT id IN (1, 2, 3);"
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
                    throw new Exception("Logic bug in ExportDBTableDataGetSetStatements: deleteExtrasThenAddNew is true, but setStatements is not empty");
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
        /// Append a single row of results to the output file
        /// </summary>
        /// <param name="writer">Text file writer</param>
        /// <param name="dataExportParams">Data export parameters</param>
        /// <param name="delimitedRowValues">Text to write to the current line</param>
        /// <param name="columnCount">Number of columns</param>
        /// <param name="columnValues">Column values</param>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        private void ExportDBTableDataRow(
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
        /// Step through the results in queryResults
        /// Append lines to the output file
        /// </summary>
        /// <param name="writer">Text file writer</param>
        /// <param name="queryResults">Query results dataset</param>
        /// <param name="insertIntoLine">Insert Into (Column1, Column2, Column3) line (used when SaveDataAsInsertIntoStatements is true and PgInsertEnabled is false)</param>
        /// <param name="dataExportParams">Data export parameters</param>
        protected void ExportDBTableDataWork(
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
                case DataColumnTypeConstants.GUID:
                case DataColumnTypeConstants.IPAddress:
                    return FormatValueForInsertAsString(columnValues[columnIndex], pgInsertEnabled);

                case DataColumnTypeConstants.DateTime:
                    var timeStamp = Convert.ToDateTime(columnValues[columnIndex]);
                    return string.Format("'{0:yyyy-MM-dd HH:mm:ss.fff}'", timeStamp);

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
        /// Format a column value as a string, based on the data type of the database column
        /// </summary>
        /// <param name="columnValue">Column value, as an object</param>
        /// <param name="pgInsertEnabled">True if using insert commands formatted as PostgreSQL compatible INSERT INTO statements</param>
        /// <returns>Value as a string</returns>
        /// <remarks>This method removes trailing spaces</remarks>
        public string FormatValueForInsertAsString(
           object columnValue,
           bool pgInsertEnabled)
        {
            if (mOptions.PgDumpTableData && !pgInsertEnabled)
            {
                return CleanForCopyCommand(columnValue);
            }

            if (mOptions.ScriptingOptions.SaveDataAsInsertIntoStatements || pgInsertEnabled)
            {
                return PossiblyQuoteText(columnValue.ToString().TrimEnd(' '));
            }

            return columnValue.ToString();
        }

        /// <summary>
        /// Store the values in the current row in an array, using null when the given column is null
        /// </summary>
        /// <param name="columnCount">Column count</param>
        /// <param name="currentRow">Current row from a DataSet</param>
        /// <returns>Column values, as an array of objects</returns>
        protected static object[] GetColumnValues(int columnCount, DataRow currentRow)
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
        /// Retrieve a list of tables in the given database
        /// </summary>
        /// <param name="databaseName">Database to query</param>
        /// <param name="includeTableRowCounts">When true, determines the row count in each table</param>
        /// <param name="includeSystemObjects">When true, also returns system tables</param>
        /// <returns>Dictionary where keys are information on database tables and values are row counts (if includeTableRowCounts = true)</returns>
        public abstract Dictionary<TableDataExportInfo, long> GetDatabaseTables(string databaseName, bool includeTableRowCounts, bool includeSystemObjects);

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
        /// Generate the file name to exporting table data
        /// </summary>
        /// <param name="tableInfo">Table info</param>
        /// <param name="dataExportParams">Data export parameters</param>
        /// <returns>Relative path to the output file</returns>
        private string GetFileNameForTableDataExport(
            TableDataExportInfo tableInfo,
            DataExportWorkingParams dataExportParams)
        {
            // Make sure output file name doesn't contain any invalid characters
            var defaultOwnerSchema = IsDefaultOwnerSchema(dataExportParams.TargetTableSchema);

            // When calling CleanNameForOS(), use UnquoteName() to remove double quotes and square brackets, which are used for quoting names with spaces or reserved words

            var cleanName = defaultOwnerSchema ?
                CleanNameForOS(UnquoteName(dataExportParams.TargetTableName) + TABLE_DATA_FILE_SUFFIX) :
                CleanNameForOS(UnquoteName(dataExportParams.TargetTableNameWithSchema) + TABLE_DATA_FILE_SUFFIX);

            var suffix = tableInfo.FilterByDate
                ? string.Format("_Since_{0:yyyy-MM-dd}", tableInfo.MinimumDate)
                : string.Empty;

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
        /// <param name="serverName">Server name</param>
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
        /// Get the expected name of the column in the source database;
        /// does not try to reverse engineer a snake-cased name
        /// </summary>
        /// <param name="columnMapInfo">Class tracking the source and target column names for the table</param>
        /// <param name="currentColumnName">Current column name</param>
        /// <returns>Source DB column name</returns>
        // ReSharper disable once UnusedMember.Global
        protected string GetSourceColumnName(ColumnMapInfo columnMapInfo, string currentColumnName)
        {
            return columnMapInfo == null ? currentColumnName : columnMapInfo.GetSourceColumnName(currentColumnName);
        }

        /// <summary>
        /// Get a FileInfo object for table data
        /// </summary>
        /// <param name="tableInfo">Table info</param>
        /// <param name="dataExportParams">Data export parameters</param>
        /// <param name="workingParams">Working parameters</param>
        /// <param name="relativeFilePath">Output: relative file path</param>
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

            ConsoleMsgUtils.ShowDebugCustom("Writing table data to " + PathUtils.CompactPathString(tableDataOutputFile.FullName, 120), "  ", 0);
            Console.WriteLine();

            return tableDataOutputFile;
        }

        /// <summary>
        /// Get the target column name to use when exporting data
        /// </summary>
        /// <param name="columnMapInfo">Class tracking the source and target column names for the table</param>
        /// <param name="columnName">Column name</param>
        /// <returns>Target column name</returns>
        public string GetTargetColumnName(ColumnMapInfo columnMapInfo, string columnName)
        {
            var unusedDataColumnType = DataColumnTypeConstants.Numeric;
            return GetTargetColumnName(columnMapInfo, columnName, ref unusedDataColumnType);
        }

        /// <summary>
        /// Get the target column name to use when exporting data
        /// </summary>
        /// <param name="columnMapInfo">Class tracking the source and target column names for the table</param>
        /// <param name="currentColumnName">Current column name</param>
        /// <param name="dataColumnType">Column data type</param>
        /// <returns>Target column name</returns>
        protected string GetTargetColumnName(ColumnMapInfo columnMapInfo, string currentColumnName, ref DataColumnTypeConstants dataColumnType)
        {
            string targetColumnName;

            // Rename the column if defined in mOptions.ColumnMapForDataExport or if mOptions.TableDataSnakeCase is true
            if (columnMapInfo?.IsColumnDefined(currentColumnName) == true)
            {
                targetColumnName = columnMapInfo.GetTargetColumnName(currentColumnName);

                if (targetColumnName.Equals(TableNameMapContainer.NameMapReader.SKIP_FLAG, StringComparison.OrdinalIgnoreCase))
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
        /// <param name="dataExportParams">Data export parameters, including source and target table info</param>
        /// <param name="tableInfo">Table info object</param>
        /// <param name="quoteWithSquareBrackets">When true, quote names with square brackets; otherwise, quote with double quotes</param>
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
        /// <param name="objectNameWithSchema">Object name, with schema</param>
        protected string GetSchemaName(string objectNameWithSchema)
        {
            return GetSchemaName(objectNameWithSchema, out _);
        }

        /// <summary>
        /// Get the schema name from an object name
        /// </summary>
        /// <param name="objectNameWithSchema">Object name, with schema</param>
        /// <param name="objectName">Object name</param>
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
        /// If the table schema is "dbo", "public", or an empty string, simply return the table name, quoted if necessary
        /// Otherwise, return the schema name and table name, separated by a period, quoted if necessary
        /// </summary>
        /// <param name="tableSchema">Table schema</param>
        /// <param name="tableName">Table name</param>
        /// <param name="quoteWithSquareBrackets">When true, quote names with square brackets; otherwise, quote with double quotes</param>
        /// <param name="alwaysQuoteNames">When true, always returned quoted schema.table_name</param>
        /// <param name="alwaysIncludePublicSchemaName">When true, always return the name as public.table_name if the table is in the public schema</param>
        /// <returns>Table name, with schema if required</returns>
        protected string GetTableNameToUse(
            string tableSchema,
            string tableName,
            bool quoteWithSquareBrackets,
            bool alwaysQuoteNames,
            bool alwaysIncludePublicSchemaName = false)
        {
            if (!alwaysIncludePublicSchemaName && IsDefaultOwnerSchema(tableSchema))
            {
                return PossiblyQuoteName(tableName, quoteWithSquareBrackets, alwaysQuoteNames);
            }

            return string.Format(
                "{0}.{1}",
                PossiblyQuoteName(tableSchema, quoteWithSquareBrackets, alwaysQuoteNames),
                PossiblyQuoteName(tableName, quoteWithSquareBrackets, alwaysQuoteNames));
        }

        /// <summary>
        /// Get the target table name to use when exporting data
        /// </summary>
        /// <param name="dataExportParams">Data export parameters, including source and target table info</param>
        /// <param name="tableInfo">Table info object</param>
        /// <param name="quoteWithSquareBrackets">When true, quote names with square brackets; otherwise, quote with double quotes</param>
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

            return GetTableNameToUse(dataExportParams.TargetTableSchema, dataExportParams.TargetTableName, quoteWithSquareBrackets, alwaysQuoteNames);
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
        /// <param name="schemaName">Schema name</param>
        private bool IsDefaultOwnerSchema(string schemaName)
        {
            return string.IsNullOrWhiteSpace(schemaName) ||
                   schemaName.Equals("dbo", StringComparison.OrdinalIgnoreCase) ||
                   schemaName.Equals("public", StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Return true if any of the RegEx matchers in mObjectNameMatchers match the object name
        /// </summary>
        /// <param name="objectName">Object name</param>
        protected bool MatchesObjectsToProcess(string objectName)
        {
            foreach (var matcher in mObjectNameMatchers)
            {
                if (matcher.IsMatch(objectName))
                    return true;
            }

            return false;
        }

        /// <summary>
        /// Invoke the DBExportStarting event
        /// </summary>
        /// <param name="databaseName">Database name</param>
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
        /// Disable triggers if PgInsertEnabled is true or IncludeDisableTriggerCommands is true
        /// </summary>
        /// <param name="dataExportParams">Data export parameters</param>
        /// <param name="options">Options</param>
        /// <param name="writer">Text file writer</param>
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

        /// <summary>
        /// Enable triggers if PgInsertEnabled is true or IncludeDisableTriggerCommands is true
        /// </summary>
        /// <param name="dataExportParams">Data export parameters</param>
        /// <param name="options">Options</param>
        /// <param name="writer">Text file writer</param>
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
        /// If objectName contains characters other than A-Z, a-z, 0-9, or an underscore, surround the name with square brackets or double quotes
        /// </summary>
        /// <remarks>Also quote if the name is a keyword</remarks>
        /// <param name="objectName">Object name</param>
        /// <param name="quoteWithSquareBrackets">When true, quote names with square brackets; otherwise, quote with double quotes</param>
        /// <param name="alwaysQuoteNames">When true, always quote names</param>
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
                if (objectName.Trim().StartsWith("["))
                    return objectName;

                // SQL Server quotes names with square brackets
                return '[' + objectName + ']';
            }

            if (objectName.Trim().StartsWith("\""))
                return objectName;

            // PostgreSQL quotes names with double quotes
            return '"' + objectName + '"';
        }

        /// <summary>
        /// Examine object names in a comma separated list, quoting any that are keywords or have non-standard characters
        /// </summary>
        /// <param name="objectNames">Object names</param>
        /// <param name="quoteWithSquareBrackets">When true, quote names with square brackets; otherwise, quote with double quotes</param>
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
        /// <param name="text">Text to possibly quote</param>
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
        /// Determine the primary key column (or columns) for a table
        /// </summary>
        /// <param name="dataExportParams">Data export parameters</param>
        /// <param name="workingParams">Working parameters</param>
        /// <param name="tableInfo">Table info</param>
        /// <param name="columnMapInfo">Class tracking the source and target column names for the table</param>
        /// <returns>Comma separated list of primary key column names (using target column names)</returns>
        protected abstract string ResolvePrimaryKeys(
            DataExportWorkingParams dataExportParams,
            WorkingParams workingParams,
            TableNameInfo tableInfo,
            ColumnMapInfo columnMapInfo);

        /// <summary>
        /// Determine the primary key column (or columns) for a table
        /// </summary>
        /// <param name="dataExportParams">Data export parameters</param>
        /// <param name="workingParams">Working parameters</param>
        /// <param name="tableInfo">Table info</param>
        /// <param name="columnMapInfo">Class tracking the source and target column names for the table</param>
        /// <returns>Comma separated list of primary key column names (using target column names)</returns>
        protected string ResolvePrimaryKeysBase(
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

            if (tableInfo.PrimaryKeyColumns.Count > 0)
            {
                return GetTargetPrimaryKeyColumnNames(columnMapInfo, tableInfo.PrimaryKeyColumns, out _);
            }

            return string.Empty;
        }

        /// <summary>
        /// Script the objects in each of the specified databases
        /// Also script data from the specified tables from any database that has the given table names
        /// </summary>
        /// <param name="databaseListToProcess">List of database names to process</param>
        /// <param name="tablesForDataExport">Tables to export data from</param>
        /// <param name="tableDataExportOrder">List of table names that defines the order that table data should be exported (table name only, not schema)</param>
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

                        success = ExportDBObjectsAndTableData(
                            currentDbName,
                            tablesForDataExport,
                            tableDataExportOrder,
                            out var databaseNotFound,
                            out var workingParams);

                        if (!warningsByDatabase.ContainsKey(currentDB) && workingParams.WarningMessages.Count > 0)
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
        /// <param name="databaseList">Database names to export</param>
        /// <param name="tablesForDataExport">Table names for which data should be exported</param>
        /// <param name="tableDataExportOrder">List of table names that defines the order that table data should be exported (table name only, not schema)</param>
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
        /// <param name="errorCode">Error code</param>
        /// <param name="message">Error message</param>
        protected void SetLocalError(DBSchemaExportErrorCodes errorCode, string message)
        {
            SetLocalError(errorCode, message, null);
        }

        /// <summary>
        /// Set the local error code; provide an exception instance
        /// </summary>
        /// <param name="errorCode">Error code</param>
        /// <param name="message">Error message</param>
        /// <param name="ex">Exception</param>
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
        /// <param name="message">Trace message</param>
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
        /// <param name="schemaName">Schema name</param>
        /// <returns>True if the schema should be ignored</returns>
        protected bool SkipSchema(string schemaName)
        {
            return SkipSchema(mOptions, schemaName);
        }

        /// <summary>
        /// Check whether options.SchemaNameSkipList contains the schema name
        /// </summary>
        /// <param name="options">Options</param>
        /// <param name="schemaName">Schema name</param>
        /// <returns>True if the schema should be ignored</returns>
        private static bool SkipSchema(SchemaExportOptions options, string schemaName)
        {
            return options.SchemaNameSkipList.Contains(schemaName);
        }

        /// <summary>
        /// Determine whether this table should be skipped when exporting data
        /// </summary>
        /// <param name="tableInfo">Table info</param>
        /// <returns>True (meaning to skip the table) if the table has "&lt;skip&gt;" for the TargetTableName</returns>
        public bool SkipTableForDataExport(TableDataExportInfo tableInfo)
        {
            var skipTable = SkipTableForDataExport(mOptions, tableInfo);

            if (skipTable)
                return true;

            return !MatchesObjectsToProcess(tableInfo.SourceTableName);
        }

        /// <summary>
        /// Determine whether this table should be skipped when exporting data
        /// </summary>
        /// <param name="options">Options</param>
        /// <param name="tableInfo">Table info</param>
        /// <returns>True (meaning to skip the table) if the table has "&lt;skip&gt;" for the TargetTableName</returns>
        public static bool SkipTableForDataExport(SchemaExportOptions options, TableDataExportInfo tableInfo)
        {
            if (!TableNamePassesFilters(options, tableInfo.SourceTableName))
                return true;

            return tableInfo.TargetTableName?.Equals(TableNameMapContainer.NameMapReader.SKIP_FLAG, StringComparison.OrdinalIgnoreCase) == true;
        }

        /// <summary>
        /// Determine whether this table should be skipped when exporting data
        /// </summary>
        /// <param name="tablesForDataExport">Tables to export data from</param>
        /// <param name="candidateTableSourceTableName">Source table name</param>
        /// <param name="tableInfo">Table info</param>
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
        /// Store the RegEx specs to use to find tables from which data should be exported
        /// </summary>
        /// <param name="tableNameRegExSpecs">Table name Regex specs</param>
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
        /// <param name="tableNames">Table names</param>
        public void StoreTableNamesToAutoExportData(SortedSet<string> tableNames)
        {
            ShowTrace(string.Format("Storing {0} default names for finding tables for data export", tableNames.Count));

            TableNamesToAutoExportData.Clear();

            foreach (var item in tableNames)
            {
                TableNamesToAutoExportData.Add(item);
            }
        }

        /// <summary>
        /// Check whether mOptions.TableNameFilterSet is empty, or contains the table name
        /// </summary>
        /// <param name="tableName">Table name</param>
        /// <returns>True if the filter set is empty, or contains the table name; otherwise false</returns>
        private bool TableNamePassesFilters(string tableName)
        {
            var passesFilter = TableNamePassesFilters(mOptions, tableName);
            return passesFilter && MatchesObjectsToProcess(tableName);
        }

        /// <summary>
        /// Check whether options.TableNameFilterSet is empty, or contains the table name
        /// </summary>
        /// <param name="options">Options</param>
        /// <param name="tableName">Table name</param>
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
        /// Remove double quotes and square brackets from an object name
        /// </summary>
        /// <param name="objectName">Object name</param>
        /// <returns>Unquoted name</returns>
        private object UnquoteName(string objectName)
        {
            return mQuoteChars.Replace(objectName, string.Empty);
        }

        /// <summary>
        /// Validate options
        /// </summary>
        /// <param name="databaseList">List of database names</param>
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

            mObjectNameMatchers.Clear();

            if (string.IsNullOrWhiteSpace(mOptions.ObjectNameFilter))
            {
                mObjectNameMatchers.Add(new Regex(".+", RegexOptions.Compiled));
            }
            else
            {
                var nameFilters = new List<string>();

                // Split on commas, but do not split if mOptions.ObjectNameFilter has square brackets
                if (mOptions.ObjectNameFilter.IndexOfAny(['[', ']']) >= 0)
                {
                    nameFilters.Add(mOptions.ObjectNameFilter);
                }
                else
                {
                    nameFilters.AddRange(mOptions.ObjectNameFilter.Split(','));
                }

                foreach (var filter in nameFilters)
                {
                    try
                    {
                        mObjectNameMatchers.Add(new Regex(filter.Trim(), RegexOptions.Compiled | RegexOptions.IgnoreCase));
                    }
                    catch (Exception ex)
                    {
                        OnErrorEvent(string.Format(
                            "Invalid text defined for the object name filter, '{0}'; " +
                            "should be a series of letters or valid RegEx", filter.Trim()), ex);

                        return false;
                    }
                }
            }

            return ValidateOutputOptions();
        }

        /// <summary>
        /// Validate the output directory for the current database
        /// </summary>
        /// <param name="databaseName">Database name</param>
        /// <param name="workingParams">Working parameters</param>
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
        /// <param name="outputFile">Output file</param>
        /// <param name="scriptInfo">List of SQL statements</param>
        /// <param name="autoAddGoStatements">When true, auto-add GO statements</param>
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
        /// <param name="outputDirectory">Output directory</param>
        /// <param name="objectName">Object name</param>
        /// <param name="scriptInfo">List of SQL statements</param>
        /// <param name="autoAddGoStatements">When true, auto-add GO statements</param>
        /// <param name="fileExtension">File extensions</param>
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
