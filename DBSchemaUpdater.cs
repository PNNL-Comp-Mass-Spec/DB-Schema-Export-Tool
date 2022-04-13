using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using PRISM;
using PRISM.Logging;

namespace DB_Schema_Export_Tool
{
    internal class DBSchemaUpdater : EventNotifier
    {
        // Ignore Spelling: dbo

        private const string FILE_SUFFIX_UPDATED_COLUMN_NAMES = "_UpdatedColumnNames";

        private readonly Regex mColumnNameMatcher;

        /// <summary>
        /// Options
        /// </summary>
        private readonly SchemaExportOptions mOptions;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="options"></param>
        public DBSchemaUpdater(SchemaExportOptions options)
        {
            mColumnNameMatcher = new Regex(@"\[(?<ColumnName>[^]]+)\]|(?<ColumnName>[^\s]+)", RegexOptions.Compiled);
            mOptions = options;
        }

        /// <summary>
        /// Show a status message at the console and optionally include in the log file
        /// </summary>
        /// <param name="statusMessage">Status message</param>
        /// <param name="isError">True if this is an error</param>
        /// <param name="writeToLog">True to write to the log file; false to only display at console</param>
        public void LogMessage(string statusMessage, bool isError = false, bool writeToLog = true)
        {
            OnStatusEvent(statusMessage);

            if (mOptions.LogMessagesToFile)
                LogTools.LogMessage(statusMessage, isError, writeToLog);
        }

        /// <summary>
        /// Log a warning message
        /// </summary>
        /// <param name="warningMessage">Warning message</param>
        /// <param name="logToDb">When true, log the message to the database and the local log file</param>
        protected void LogWarning(string warningMessage, bool logToDb = false)
        {
            OnWarningEvent(warningMessage);

            if (mOptions.LogMessagesToFile)
                LogTools.LogWarning(warningMessage, logToDb);
        }

        private void ShowTrace(string message)
        {
            if (mOptions.Trace)
            {
                OnDebugEvent(message);
            }
        }

        /// <summary>
        /// Advance the reader and update column names
        /// Write the updated text to the writer
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="writer"></param>
        /// <param name="options"></param>
        /// <param name="tablesForDataExport"></param>
        /// <param name="createItemMatch"></param>
        /// <param name="renameColumns"></param>
        private void UpdateColumnNamesInDDL(
            StreamReader reader,
            TextWriter writer,
            SchemaExportOptions options,
            IEnumerable<TableDataExportInfo> tablesForDataExport,
            Match createItemMatch,
            bool renameColumns = true)
        {
            var columnNameMatcher = new Regex(@"^(?<Prefix>[^[]+)\[(?<ColumnName>[^]]+)\](?<Suffix>.+)$", RegexOptions.Compiled);

            var foreignKeyReferenceMatcher = new Regex(@"^REFERENCES \[(?<SchemaName>[^[]+)\]\.\[(?<TableName>[^[]+)\] *\( *\[(?<ColumnName>[^[]+)\] *\).*", RegexOptions.Compiled);

            var outputLines = new List<string>();
            var writeOutput = true;

            var sourceTableName = createItemMatch.Groups["TableName"].ToString();

            // This group will exist for default constraints and foreign key constraints
            var constraintColumnName = createItemMatch.Groups["ColumnName"].ToString();

            if (DBSchemaExportTool.GetTableByName(tablesForDataExport, sourceTableName, out var tableInfo) &&
                DBSchemaExporterBase.SkipTableForDataExport(options, tableInfo))
            {
                // Skip this table
                writeOutput = false;
            }

            if (!options.ColumnMapForDataExport.TryGetValue(sourceTableName, out var columnMapInfo))
            {
                columnMapInfo = new ColumnMapInfo(sourceTableName);
            }

            if (!string.IsNullOrWhiteSpace(constraintColumnName))
            {
                // Matched a default constraint or foreign key constraint, e.g.
                // ALTER TABLE [dbo].[T_ParamValue] ADD  CONSTRAINT [DF_T_ParamValue_Last_Affected]  DEFAULT (GetDate()) FOR [Last_Affected]
                // or
                // ALTER TABLE [dbo].[T_Dataset]  WITH CHECK ADD  CONSTRAINT [FK_T_Dataset_T_Experiments] FOREIGN KEY([Exp_ID])
                // REFERENCES [dbo].[T_Experiments] ([Exp_ID])

                if (!columnMapInfo.IsColumnDefined(constraintColumnName))
                {
                    // Column has not been renamed
                    outputLines.Add(createItemMatch.Value);
                }
                else
                {
                    var newColumnName = columnMapInfo.GetTargetColumnName(constraintColumnName);

                    if (newColumnName.Equals(DBSchemaExportTool.SKIP_FLAG))
                    {
                        // The table definition in the output file will not have this column; skip it
                        OnDebugEvent("Skipping constraint since the target column is tagged with <skip> in the file specified by the DataTables parameter:\n" +
                                     createItemMatch.Value);
                    }
                    else
                    {
                        var updatedLine = createItemMatch.Value.Replace(
                            "[" + constraintColumnName + "]",
                            "[" + newColumnName + "]");

                        outputLines.Add(updatedLine);
                    }
                }
            }
            else
            {
                outputLines.Add(createItemMatch.Value);
            }

            while (!reader.EndOfStream)
            {
                var dataLine = reader.ReadLine();

                if (dataLine == null)
                    continue;

                if (dataLine.StartsWith("GO"))
                {
                    // End of block

                    if (!writeOutput)
                    {
                        writer.WriteLine("-- Skipping table: {0}", sourceTableName);
                        writer.WriteLine();
                        return;
                    }

                    outputLines.Add(dataLine);

                    WriteCreateTableDDL(writer, sourceTableName, outputLines);
                    return;
                }

                if (dataLine.StartsWith("REFERENCES"))
                {
                    var referenceMatch = foreignKeyReferenceMatcher.Match(dataLine);

                    if (referenceMatch.Success)
                    {
                        var foreignKeyTableName = referenceMatch.Groups["TableName"].ToString();
                        var foreignKeyColumnName = referenceMatch.Groups["ColumnName"].ToString();

                        if (options.ColumnMapForDataExport.TryGetValue(foreignKeyTableName, out var foreignKeyTableColumnMapInfo))
                        {
                            var newForeignKeyColumnName = foreignKeyTableColumnMapInfo.GetTargetColumnName(foreignKeyColumnName);
                            var updatedLine = dataLine.Replace(
                                "[" + foreignKeyColumnName + "]",
                                "[" + newForeignKeyColumnName + "]");

                            outputLines.Add(updatedLine);
                            continue;
                        }
                    }
                }

                var columnMatch = columnNameMatcher.Match(dataLine);

                if (!columnMatch.Success || !renameColumns)
                {
                    outputLines.Add(dataLine);
                    continue;
                }

                var columnName = columnMatch.Groups["ColumnName"].Value;

                if (!columnMapInfo.IsColumnDefined(columnName))
                {
                    outputLines.Add(dataLine);
                    continue;
                }

                var prefix = columnMatch.Groups["Prefix"].ToString();
                var suffix = columnMatch.Groups["Suffix"].ToString();

                var newColumnName = columnMapInfo.GetTargetColumnName(columnName);

                if (newColumnName.Equals(DBSchemaExportTool.SKIP_FLAG))
                {
                    // Skip this column
                    continue;
                }

                var updatedDataLine = string.Format("{0}[{1}]{2}", prefix, newColumnName, suffix);
                outputLines.Add(updatedDataLine);
            }

            // GO was not found; this is unexpected
            // Write out the cached lines anyway (ignore writeOutput to avoid unintentionally skipping DDL)

            WriteCreateTableDDL(writer, sourceTableName, outputLines);
        }

        /// <summary>
        /// Parse a schema file with CREATE TABLE and other database DDL statements
        /// Update table and column names using information in tablesForDataExport and options.ColumnMapForDataExport
        /// </summary>
        /// <param name="schemaFileToParse"></param>
        /// <param name="options"></param>
        /// <param name="tablesForDataExport"></param>
        /// <param name="updatedSchemaFilePath"></param>
        public bool UpdateColumnNamesInExistingSchemaFile(
            string schemaFileToParse,
            SchemaExportOptions options,
            IReadOnlyCollection<TableDataExportInfo> tablesForDataExport,
            out string updatedSchemaFilePath)
        {
            try
            {
                var existingSchemaFile = new FileInfo(schemaFileToParse);

                if (!existingSchemaFile.Exists)
                {
                    LogWarning("Existing schema file is missing; cannot update column names");
                    OnWarningEvent("File not found: " + existingSchemaFile.FullName);
                    updatedSchemaFilePath = string.Empty;
                    return false;
                }

                if (existingSchemaFile.Directory == null)
                {
                    LogWarning("Cannot update column names in an existing schema file");
                    OnWarningEvent("Unable to determine the parent directory of: " + existingSchemaFile.FullName);
                    updatedSchemaFilePath = string.Empty;
                    return false;
                }

                updatedSchemaFilePath = Path.Combine(
                    existingSchemaFile.Directory.FullName,
                    Path.GetFileNameWithoutExtension(existingSchemaFile.Name) + FILE_SUFFIX_UPDATED_COLUMN_NAMES +
                    existingSchemaFile.Extension);

                var tableNameMatcher = new Regex(@"^CREATE TABLE[^[]+\[(?<SchemaName>[^[]+)\]\.\[(?<TableName>[^[]+)\].*$", RegexOptions.Compiled | RegexOptions.IgnoreCase);
                var indexNameMatcher = new Regex(@"^CREATE.+INDEX[^[]+\[(?<IndexName>[^[]+)\] ON \[(?<SchemaName>[^[]+)\]\.\[(?<TableName>.+)\].*$", RegexOptions.Compiled | RegexOptions.IgnoreCase);
                var viewNameMatcher = new Regex(@"^CREATE VIEW[^[]+\[(?<SchemaName>[^[]+)\]\.\[(?<TableName>[^[]+)\].*$", RegexOptions.Compiled | RegexOptions.IgnoreCase);

                var defaultConstraintMatcher = new Regex(@"^ALTER TABLE[^[]+\[(?<SchemaName>[^[]+)\]\.\[(?<TableName>[^[]+)\][^[]+ADD[^[]+CONSTRAINT.+DEFAULT.+ FOR \[(?<ColumnName>[^[]+)\].*$", RegexOptions.Compiled | RegexOptions.IgnoreCase);
                var foreignKeyConstraintMatcher = new Regex(@"^ALTER TABLE[^[]+\[(?<SchemaName>[^[]+)\]\.\[(?<TableName>[^[]+)\][^[]+ADD[^[]+CONSTRAINT.+FOREIGN KEY.*\(\[(?<ColumnName>[^[]+)\]\).*$", RegexOptions.Compiled | RegexOptions.IgnoreCase);

                ShowTrace("Opening " + PathUtils.CompactPathString(existingSchemaFile.FullName, 120));

                using var reader = new StreamReader(new FileStream(existingSchemaFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));
                using var writer = new StreamWriter(new FileStream(updatedSchemaFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                    {
                        writer.WriteLine(dataLine);
                        continue;
                    }

                    var createTableMatch = tableNameMatcher.Match(dataLine);

                    if (createTableMatch.Success)
                    {
                        UpdateColumnNamesInDDL(reader, writer, options, tablesForDataExport, createTableMatch);
                        continue;
                    }

                    var createIndexMatch = indexNameMatcher.Match(dataLine);

                    if (createIndexMatch.Success)
                    {
                        UpdateColumnNamesInDDL(reader, writer, options, tablesForDataExport, createIndexMatch);
                        continue;
                    }

                    var createViewMatch = viewNameMatcher.Match(dataLine);

                    if (createViewMatch.Success)
                    {
                        // Note: only check for whether or not to skip the view; do not update column names in the view
                        // A separate application is used to update column names in views:
                        // https://github.com/PNNL-Comp-Mass-Spec/PgSQL-View-Creator-Helper
                        UpdateColumnNamesInDDL(reader, writer, options, tablesForDataExport, createViewMatch, false);
                        continue;
                    }

                    var defaultConstraintMatch = defaultConstraintMatcher.Match(dataLine);

                    if (defaultConstraintMatch.Success)
                    {
                        // Note: The target column name for the default constraint is tracked in defaultConstraintMatch as group "ColumnName"
                        UpdateColumnNamesInDDL(reader, writer, options, tablesForDataExport, defaultConstraintMatch, false);
                        continue;
                    }

                    var foreignKeyConstraintMatch = foreignKeyConstraintMatcher.Match(dataLine);

                    if (foreignKeyConstraintMatch.Success)
                    {
                        // Note: The target column name for the foreign key constraint is tracked in foreignKeyConstraintMatch as group "ColumnName"
                        UpdateColumnNamesInDDL(reader, writer, options, tablesForDataExport, foreignKeyConstraintMatch, false);
                        continue;
                    }

                    writer.WriteLine(dataLine);
                }

                LogMessage("Created " + PathUtils.CompactPathString(updatedSchemaFilePath, 120));

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in UpdateNamesInExistingSchemaFile", ex);
                updatedSchemaFilePath = string.Empty;
                return false;
            }
        }

        /// <summary>
        /// Examine the Create Table DDL to find the location of the primary key column
        /// Move it to the first column if not in the first column
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="createTableDDL"></param>
        /// <param name="primaryKeyColumns"></param>
        public List<string> UpdateCreateTablePrimaryKeyPosition(
            string tableName,
            List<string> createTableDDL,
            IEnumerable<string> primaryKeyColumns
        )
        {
            // Keys in this dictionary are column name; values are the position (1 for the first column, 2 for the second, ...)
            var tableColumns = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

            // Keys in this dictionary are column name; values are the DDL to for the column (data type, null / Not Null, collation, etc.)
            var tableColumnDDL = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            var definingColumns = false;
            var primaryKeyColumn = string.Empty;

            var currentColumnName = string.Empty;

            // This holds DDL up to the CREATE TABLE line
            var prefixLines = new List<string>();

            // This holds DDL after the last column
            var suffixLines = new List<string>();

            // Find the column names
            // Assume they are listed between the CREATE TABLE and CONSTRAINT lines
            // (or between CREATE TABLE and ) if the table does not have a primary key constraint)
            for (var i = 0; i < createTableDDL.Count; i++)
            {
                var dataLine = createTableDDL[i];
                var trimmedLine = dataLine.Trim();

                if (tableColumns.Count == 0 && trimmedLine.StartsWith("CREATE TABLE", StringComparison.OrdinalIgnoreCase))
                {
                    definingColumns = true;
                    prefixLines.Add(dataLine);
                    continue;
                }

                if (!definingColumns)
                {
                    if (tableColumns.Count == 0)
                        prefixLines.Add(dataLine);
                    else
                        suffixLines.Add(dataLine);

                    continue;
                }

                if (trimmedLine.StartsWith(")", StringComparison.OrdinalIgnoreCase))
                {
                    definingColumns = false;
                    suffixLines.Add(dataLine);
                    continue;
                }

                if (trimmedLine.StartsWith("CONSTRAINT", StringComparison.OrdinalIgnoreCase))
                {
                    definingColumns = false;
                    suffixLines.Add(dataLine);

                    if (trimmedLine.IndexOf("PRIMARY KEY", StringComparison.OrdinalIgnoreCase) > 0)
                    {
                        // The next line should be (
                        // The line after that should be the primary key column

                        while (i < createTableDDL.Count - 1)
                        {
                            i++;

                            var suffixLine = createTableDDL[i];
                            suffixLines.Add(suffixLine);

                            if (suffixLine.Trim().Equals("("))
                                continue;

                            var primaryKeyMatch = mColumnNameMatcher.Match(suffixLine);

                            if (primaryKeyMatch.Success)
                            {
                                primaryKeyColumn = primaryKeyMatch.Groups["ColumnName"].Value;
                            }

                            break;
                        }
                    }

                    continue;
                }

                var columnMatch = mColumnNameMatcher.Match(dataLine);

                if (!columnMatch.Success)
                {
                    OnDebugEvent("Appending DDL for column {0}: {1}", currentColumnName, dataLine);

                    tableColumnDDL[currentColumnName] = tableColumnDDL[currentColumnName] + Environment.NewLine + dataLine;

                    continue;
                }

                var columnName = columnMatch.Groups["ColumnName"].Value;

                if (tableColumns.ContainsKey(columnName))
                {
                    OnWarningEvent("Create Table DDL for {0} has column {1} listed more than once", tableName, columnName);
                    continue;
                }

                var columnPosition = tableColumns.Count + 1;
                currentColumnName = columnName;

                tableColumns.Add(columnName, columnPosition);
                tableColumnDDL.Add(columnName, dataLine);
            }

            var firstPrimaryKeyColumn = string.IsNullOrWhiteSpace(primaryKeyColumn) ? primaryKeyColumns.First() : primaryKeyColumn;

            if (!tableColumns.TryGetValue(firstPrimaryKeyColumn, out var primaryKeyColumnPosition))
            {
                OnWarningEvent("Table {0} does not have a primary key column", tableName);
                return createTableDDL;
            }

            if (primaryKeyColumnPosition == 1)
            {
                OnDebugEvent("Primary key column {0} for table {1} is already at position 1", firstPrimaryKeyColumn, tableName);

                return createTableDDL;
            }

            if (!tableColumnDDL.ContainsKey(firstPrimaryKeyColumn))
            {
                OnWarningEvent(
                    "tableColumnDDL dictionary does not have primary key column {0} for table {1}; this is unexpected",
                    firstPrimaryKeyColumn, tableName);

                return createTableDDL;
            }

            Console.WriteLine();

            OnStatusEvent(
                "Moving primary key column {0} from position {1} to position 1 for table {2}",
                firstPrimaryKeyColumn, primaryKeyColumnPosition, tableName);

            var outputLines = new List<string>();
            outputLines.AddRange(prefixLines);

            outputLines.Add(tableColumnDDL[firstPrimaryKeyColumn]);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var columnInfo in from item in tableColumns orderby item.Value select item)
            {
                if (columnInfo.Value == primaryKeyColumnPosition)
                {
                    // This is the primary key column; skip it
                    continue;
                }

                outputLines.Add(tableColumnDDL[columnInfo.Key]);
            }

            outputLines.AddRange(suffixLines);

            return outputLines;
        }

        private void WriteCreateTableDDL(TextWriter writer, string tableName, List<string> createTableDDL)
        {
            var primaryKeyColumns = DBSchemaExporterSQLServer.GetPrimaryKeysForTableViaDDL(createTableDDL);

            List<string> outputLines;

            if (primaryKeyColumns.Count == 0)
            {
                outputLines = createTableDDL;
            }
            else
            {
                outputLines = UpdateCreateTablePrimaryKeyPosition(tableName, createTableDDL, primaryKeyColumns);
            }

            foreach (var outputLine in outputLines)
            {
                writer.WriteLine(outputLine);
            }
        }
    }
}
