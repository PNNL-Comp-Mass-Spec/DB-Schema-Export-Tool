using PRISM;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using TableNameMapContainer;

namespace DB_Schema_Export_Tool
{
    /// <summary>
    /// This class creates files for deleting extra rows in tables in a target database
    /// It requires that the target table has one or more primary key columns
    /// </summary>
    /// <remarks>
    /// For small tables that do not have a primary key, class DBSchemaExporterSQLServer uses method ExportDBTableDataDeleteExtraRows to delete extra rows
    /// </remarks>
    internal class DeleteExtraDataRowsScripter : EventNotifier
    {
        // Ignore Spelling: subquery

        private readonly DBSchemaExporterSQLServer mDbSchemaExporter;

        private readonly SchemaExportOptions mOptions;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="dbSchemaExporter"></param>
        /// <param name="options"></param>
        public DeleteExtraDataRowsScripter(DBSchemaExporterSQLServer dbSchemaExporter, SchemaExportOptions options)
        {
            mDbSchemaExporter = dbSchemaExporter;
            mOptions = options;
        }

        /// <summary>
        /// Create a SQL script to truncate data in a table
        /// </summary>
        /// <param name="dataExportParams"></param>
        /// <param name="deleteExtrasFile"></param>
        /// <returns>True if successful, false if an error</returns>
        private bool CreateTruncateTableScriptFile(DataExportWorkingParams dataExportParams, FileSystemInfo deleteExtrasFile)
        {
            try
            {
                ShowDeleteExtrasFilePath(deleteExtrasFile);

                using var writer = new StreamWriter(new FileStream(deleteExtrasFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read));

                writer.WriteLine("-- Remove all rows from table {0}", dataExportParams.TargetTableNameWithSchema);
                writer.WriteLine();

                writer.WriteLine("TRUNCATE TABLE {0};", dataExportParams.QuotedTargetTableNameWithSchema);

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent(string.Format("Error in CreateTruncateTableScriptFile for table {0}", dataExportParams.SourceTableNameWithSchema), ex);
                return false;
            }
        }

        /// <summary>
        /// Create a file with commands to delete extra rows from the target table, filtering using the primary key column(s)
        /// </summary>
        /// <param name="tableInfo"></param>
        /// <param name="columnMapInfo"></param>
        /// <param name="dataExportParams"></param>
        /// <param name="workingParams"></param>
        /// <param name="queryResults"></param>
        /// <param name="tableDataOutputFile"></param>
        /// <param name="tableDataOutputFileRelativePath"></param>
        /// <returns>True if successful, false if an error</returns>
        public bool DeleteExtraRowsInTargetTable(
            TableDataExportInfo tableInfo,
            ColumnMapInfo columnMapInfo,
            DataExportWorkingParams dataExportParams,
            WorkingParams workingParams,
            DataSet queryResults,
            FileInfo tableDataOutputFile,
            string tableDataOutputFileRelativePath)
        {
            try
            {
                if (workingParams.PrimaryKeysByTable.Count == 0)
                {
                    var primaryKeysLoaded = mDbSchemaExporter.GetPrimaryKeyInfoFromDatabase(workingParams);

                    if (!primaryKeysLoaded)
                    {
                        // Treat this as a fatal error
                        return false;
                    }
                }

                if (tableInfo.FilterByDate)
                {
                    OnWarningEvent(
                        "Table {0} used a date filter when exporting the data; cannot create a file to delete extra rows (this method should not have been called)",
                        dataExportParams.SourceTableNameWithSchema);

                    // Treat this as a non-fatal error
                    return true;
                }

                if (tableInfo.PrimaryKeyColumns.Count < 1)
                {
                    OnWarningEvent(
                        "Table {0} does not have any primary keys; cannot create a file to delete extra rows (this method should not have been called)",
                        dataExportParams.SourceTableNameWithSchema);

                    // Treat this as a non-fatal error
                    return true;
                }

                if (tableDataOutputFile.DirectoryName == null)
                {
                    OnWarningEvent(
                        "Unable to determine the parent directory of: {0}; cannot create a file to delete extra rows",
                        tableDataOutputFile.FullName);

                    return false;
                }

                var baseName = Path.GetFileNameWithoutExtension(tableDataOutputFile.Name);

                if (!baseName.EndsWith(DBSchemaExporterBase.TABLE_DATA_FILE_SUFFIX))
                {
                    OnWarningEvent(
                        "Table data file base name, {0}, does not end in {1}; this is indicates a programming error",
                        baseName, DBSchemaExporterBase.TABLE_DATA_FILE_SUFFIX);

                    // Treat this as a fatal error
                    return false;
                }

                var truncateTableData =
                    mOptions.DataLoadTruncateTableList.Contains(tableInfo.SourceTableName) ||
                    mOptions.DataLoadTruncateTableList.Contains(tableInfo.TargetSchemaName);

                var fileSuffix = truncateTableData
                    ? DBSchemaExporterBase.DELETE_ALL_ROWS_FILE_SUFFIX
                    : DBSchemaExporterBase.DELETE_EXTRA_ROWS_FILE_SUFFIX;

                var outputFileName = baseName.Substring(0, baseName.Length - DBSchemaExporterBase.TABLE_DATA_FILE_SUFFIX.Length) + fileSuffix;

                var deleteExtrasFile = new FileInfo(Path.Combine(tableDataOutputFile.DirectoryName, outputFileName));

                // Delete extra rows with a query like this:

                // DELETE FROM t_target_table target
                // WHERE target.group_id BETWEEN 1 AND 5 AND
                //       NOT EXISTS ( SELECT S.group_id,
                //                           S.job
                //                    FROM t_target_table S
                //                    WHERE target.group_id = S.group_id AND
                //                          target.job = S.job AND
                //                          (
                //                              S.group_id = 1 and S.job = 20 OR
                //                              S.group_id = 2 and S.job = 21 OR
                //                              S.group_id = 4 and S.job = 23 OR
                //                              S.group_id = 5 and S.job = 24
                //                          )
                //                   )

                // For single-column primary keys, possibly use this, since better performance vs.
                // DELETE FROM t_target_table WHERE id BETWEEN 1 and 10 AND NOT id IN (3, 4, 5, 6, 7, 10);

                // DELETE FROM t_target_table target
                // WHERE target.id BETWEEN 1 AND 5 AND
                //       NOT EXISTS ( SELECT S.id
                //                    FROM t_target_table S
                //                    WHERE target.id = S.id AND
                //                          S.id in (1,2,4,5)
                //                   )

                // When truncateTableData is true, use
                // TRUNCATE TABLE t_target_table;

                bool success;

                // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
                if (truncateTableData)
                {
                    success = CreateTruncateTableScriptFile(dataExportParams, deleteExtrasFile);
                }
                else
                {
                    success = DeleteExtraRowsUsingPrimaryKey(tableInfo, columnMapInfo, dataExportParams, workingParams, queryResults, deleteExtrasFile);
                }

                if (!success || !mOptions.ScriptPgLoadCommands)
                    return success;

                var relativeFilePath = tableDataOutputFileRelativePath.Replace(tableDataOutputFile.Name, deleteExtrasFile.Name);

                workingParams.AddDataLoadScriptFile(relativeFilePath);

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent(string.Format("Error in DeleteExtraRowsInTargetTable for table {0}", dataExportParams.SourceTableNameWithSchema), ex);
                return false;
            }
        }

        /// <summary>
        /// Create a SQL script to delete extra rows in a table based on data in primary key column(s)
        /// </summary>
        /// <param name="tableInfo"></param>
        /// <param name="columnMapInfo"></param>
        /// <param name="dataExportParams"></param>
        /// <param name="workingParams"></param>
        /// <param name="queryResults"></param>
        /// <param name="deleteExtrasFile"></param>
        /// <returns>True if successful, false if an error</returns>
        private bool DeleteExtraRowsUsingPrimaryKey(
            TableNameInfo tableInfo,
            ColumnMapInfo columnMapInfo,
            DataExportWorkingParams dataExportParams,
            WorkingParams workingParams,
            DataSet queryResults,
            FileSystemInfo deleteExtrasFile)
        {

            // Verify that the expected primary key columns are in the result set

            var primaryKeyColumnsInSource = new List<string>();
            var primaryKeyColumnsInTarget = new List<string>();

            var primaryKeyColumnIndices = new List<int>();
            var primaryKeyColumnTypes = new List<DBSchemaExporterBase.DataColumnTypeConstants>();

            for (var i = 0; i < tableInfo.PrimaryKeyColumns.Count; i++)
            {
                // This should already be the column name in the target database
                // However, call GetTargetColumnName() to make sure that it is

                var primaryKeyColumn = tableInfo.PrimaryKeyColumns[i];

                var columnNameInTarget = mDbSchemaExporter.GetTargetColumnName(columnMapInfo, primaryKeyColumn);

                string currentPrimaryKeyColumn;

                if (string.Equals(primaryKeyColumn, columnNameInTarget, StringComparison.OrdinalIgnoreCase))
                {
                    primaryKeyColumnsInTarget.Add(primaryKeyColumn);
                    currentPrimaryKeyColumn = primaryKeyColumn;
                }
                else
                {
                    OnWarningEvent(
                        "Primary key column {0} for table {1} was not already the target column name; this indicates a logic error in the source code",
                        primaryKeyColumn, dataExportParams.SourceTableNameWithSchema);

                    primaryKeyColumnsInTarget.Add(columnNameInTarget);
                    currentPrimaryKeyColumn = columnNameInTarget;
                }

                if (queryResults.Tables[0].Columns.Contains(currentPrimaryKeyColumn))
                {
                    primaryKeyColumnsInSource.Add(queryResults.Tables[0].Columns[currentPrimaryKeyColumn].ColumnName);

                    var primaryKeyColumnIndex = queryResults.Tables[0].Columns[currentPrimaryKeyColumn].Ordinal;
                    primaryKeyColumnIndices.Add(primaryKeyColumnIndex);

                    primaryKeyColumnTypes.Add(dataExportParams.ColumnNamesAndTypes[primaryKeyColumnIndex].Value);

                    continue;
                }

                // Column name not found in the source data, indicating the column was renamed (either via a name map file or via conversion to snake_case)
                // Method ResolvePrimaryKeys should have contacted the source database to obtain the primary keys for every table
                // Use this to determine the correct source column name

                if (workingParams.PrimaryKeysByTable.TryGetValue(tableInfo.SourceTableName, out var primaryKeys))
                {
                    primaryKeyColumnsInSource.Add(primaryKeys[i]);

                    var primaryKeyColumnIndex = queryResults.Tables[0].Columns[primaryKeys[i]].Ordinal;
                    primaryKeyColumnIndices.Add(primaryKeyColumnIndex);

                    primaryKeyColumnTypes.Add(dataExportParams.ColumnNamesAndTypes[primaryKeyColumnIndex].Value);

                    continue;
                }

                OnWarningEvent(
                    "Primary key column {0} not found in the retrieved data for table {1}; cannot create a file to delete extra rows",
                    primaryKeyColumn, dataExportParams.SourceTableNameWithSchema);

                return false;
            }

            // Assure that each of the primary key columns is text or a valid numeric type
            for (var i = 0; i < primaryKeyColumnsInSource.Count; i++)
            {
                var primaryKeyColumnIndex = primaryKeyColumnIndices[i];

                switch (primaryKeyColumnTypes[i])
                {
                    case DBSchemaExporterBase.DataColumnTypeConstants.Text:
                        continue;

                    case DBSchemaExporterBase.DataColumnTypeConstants.Numeric:
                        var validType = IsDotNetCompatibleDataType(dataExportParams, primaryKeyColumnIndex);

                        if (validType)
                        {
                            continue;
                        }

                        break;
                }

                OnWarningEvent(
                    "Primary key column {0} in table {1} is not a number of text; cannot create a file to delete extra rows",
                    primaryKeyColumnsInSource[i], dataExportParams.SourceTableNameWithSchema);

                // Treat this as a non-fatal error
                return true;
            }

            if (primaryKeyColumnsInSource.Count > 1)
            {
                return DeleteUsingMultiColumnPrimaryKey(
                    dataExportParams, queryResults, deleteExtrasFile,
                    primaryKeyColumnIndices, primaryKeyColumnsInTarget, primaryKeyColumnTypes);
            }

            // Single column primary key
            var columnType = dataExportParams.ColumnNamesAndTypes[primaryKeyColumnIndices[0]].Value;

            var textDataType = columnType == DBSchemaExporterBase.DataColumnTypeConstants.Text;

            return DeleteUsingSingleColumnKey(dataExportParams, queryResults, deleteExtrasFile, primaryKeyColumnIndices[0], primaryKeyColumnsInTarget[0], textDataType);
        }

        /// <summary>
        /// Create a SQL script to delete extra rows in a table based on data in each of the table's primary key columns
        /// </summary>
        /// <param name="dataExportParams"></param>
        /// <param name="queryResults"></param>
        /// <param name="deleteExtrasFile"></param>
        /// <param name="primaryKeyColumnIndices"></param>
        /// <param name="primaryKeyColumnsInTarget"></param>
        /// <param name="primaryKeyColumnTypes"></param>
        /// <returns>True if successful, false if an error</returns>
        private bool DeleteUsingMultiColumnPrimaryKey(
            DataExportWorkingParams dataExportParams,
            DataSet queryResults,
            FileSystemInfo deleteExtrasFile,
            IReadOnlyList<int> primaryKeyColumnIndices,
            IReadOnlyList<string> primaryKeyColumnsInTarget,
            IReadOnlyList<DBSchemaExporterBase.DataColumnTypeConstants> primaryKeyColumnTypes)
        {
            // Generate commands of the form:

            // DELETE FROM t_target_table target
            // WHERE target.group_id BETWEEN 1 AND 5 AND
            //       NOT EXISTS ( SELECT S.group_id,
            //                           S.job
            //                    FROM t_target_table S
            //                    WHERE target.group_id = S.group_id AND
            //                          target.job = S.job AND
            //                          (
            //                              S.group_id = 1 and S.job = 20 OR
            //                              S.group_id = 1 and S.job = 21 OR
            //                              S.group_id = 1 and S.job = 23 OR
            //                              S.group_id = 2 and S.job = 21 OR
            //                              S.group_id = 2 and S.job = 35 OR
            //                              S.group_id = 3 and S.job = 20
            //                          )
            //                   )

            try
            {
                // This tracks the primary key values for the first primary key column, along the comparison criteria required for additional primary key columns
                // Keys are primary key values, values are a list of comparisons to perform (e.g. "S.job = 20", "S.job = 21", "S.job = 23")
                var primaryKeyComparisonCriteria = new SortedDictionary<dynamic, List<string>>();

                var additionalCriteria = new StringBuilder();

                var pgInsertEnabled = dataExportParams.PgInsertEnabled;

                foreach (DataRow currentRow in queryResults.Tables[0].Rows)
                {
                    if (currentRow.IsNull(primaryKeyColumnIndices[0]))
                    {
                        // Do not delete rows where the first-column primary key is null
                        continue;
                    }

                    var firstPrimaryKeyValue = currentRow[primaryKeyColumnIndices[0]];

                    additionalCriteria.Clear();

                    for (var i = 1; i < primaryKeyColumnIndices.Count; i++)
                    {
                        if (additionalCriteria.Length > 0)
                            additionalCriteria.Append(" AND ");

                        if (currentRow.IsNull(primaryKeyColumnIndices[i]))
                        {
                            additionalCriteria.AppendFormat("S.{0} Is Null", primaryKeyColumnsInTarget[i]);
                            continue;
                        }

                        additionalCriteria.AppendFormat(
                            "S.{0} = {1}",
                            primaryKeyColumnsInTarget[i],
                            GetFormattedValue(currentRow[primaryKeyColumnIndices[i]], primaryKeyColumnTypes[i], pgInsertEnabled));
                    }

                    if (primaryKeyComparisonCriteria.TryGetValue(firstPrimaryKeyValue, out var comparisonCriteria))
                    {
                        if (additionalCriteria.Length == 0)
                        {
                            OnWarningEvent(
                                "Encountered a duplicate primary key ({0}) but there is only one primary key column on table {1}; this should never happen and likely indicates a programming error",
                                firstPrimaryKeyValue, dataExportParams.SourceTableNameWithSchema);

                            continue;
                        }

                        comparisonCriteria.Add(additionalCriteria.ToString());
                        continue;
                    }

                    var newComparisonCriteria = new List<string>();

                    if (additionalCriteria.Length > 0)
                    {
                        newComparisonCriteria.Add(additionalCriteria.ToString());
                    }

                    primaryKeyComparisonCriteria.Add(firstPrimaryKeyValue, newComparisonCriteria);
                }

                ShowDeleteExtrasFilePath(deleteExtrasFile);

                using var writer = new StreamWriter(new FileStream(deleteExtrasFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read));

                writer.WriteLine("-- Commands to delete extra rows from table {0}", dataExportParams.TargetTableNameWithSchema);

                // Create delete commands in batches of size PgInsertChunkSize

                // The batch size is computed using the number of data values being compared in the WHERE clause of the subquery
                // For a single column primary key, it will be equivalent to the number of rows processed
                // For a multi-column primary key, it will be scaled down by the number of additional primary key columns

                var chunkSize = Math.Max(100, mOptions.PgInsertChunkSize / primaryKeyColumnIndices.Count);

                // Assign the first-column primary keys to groups, based on row counts in dictionary primaryKeyValues

                // This holds the comparison criteria for the current group
                var currentGroupComparisonCriteria = new Dictionary<dynamic, List<string>>();

                var currentGroupValueCount = 0;
                var startKey = default(dynamic);
                var previousKey = default(dynamic);

                foreach (var item in primaryKeyComparisonCriteria)
                {
                    // If the source table has a single primary key column, item.Value.Count will be 0
                    // Use Math.Max() to assure that countToAdd is at least 1

                    var countToAdd = Math.Max(1, item.Value.Count);

                    if (currentGroupValueCount == 0)
                    {
                        startKey = item.Key;
                        previousKey = item.Key;

                        currentGroupComparisonCriteria.Add(item.Key, item.Value);
                        currentGroupValueCount = countToAdd;
                        continue;
                    }

                    if (currentGroupValueCount + countToAdd >= chunkSize)
                    {
                        // Write out the delete query using primary key values in the current group
                        WriteMultiColumnPrimaryKeyDeleteQuery(
                            writer,
                            dataExportParams.TargetTableNameWithSchema,
                            primaryKeyColumnsInTarget,
                            primaryKeyColumnTypes,
                            startKey,
                            previousKey,
                            currentGroupComparisonCriteria,
                            pgInsertEnabled);

                        startKey = item.Key;
                        previousKey = item.Key;

                        currentGroupComparisonCriteria.Clear();

                        currentGroupComparisonCriteria.Add(item.Key, item.Value);
                        currentGroupValueCount = countToAdd;

                        continue;
                    }

                    currentGroupComparisonCriteria.Add(item.Key, item.Value);
                    currentGroupValueCount += countToAdd;

                    previousKey = item.Key;
                }

                if (currentGroupValueCount > 0)
                {
                    WriteMultiColumnPrimaryKeyDeleteQuery(
                        writer,
                        dataExportParams.TargetTableNameWithSchema,
                        primaryKeyColumnsInTarget,
                        primaryKeyColumnTypes,
                        startKey,
                        previousKey,
                        currentGroupComparisonCriteria,
                        pgInsertEnabled);
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent(string.Format("Error in DeleteUsingMultiColumnPrimaryKey for table {0}", dataExportParams.SourceTableNameWithSchema), ex);
                return false;
            }
        }

        /// <summary>
        /// Create a SQL script to delete extra rows in a table based on data in primary key column
        /// </summary>
        /// <param name="dataExportParams"></param>
        /// <param name="queryResults"></param>
        /// <param name="deleteExtrasFile"></param>
        /// <param name="primaryKeyColumnIndex"></param>
        /// <param name="primaryKeyTargetColumnName"></param>
        /// <param name="textDataType"></param>
        /// <returns>True if successful, false if an error</returns>
        private bool DeleteUsingSingleColumnKey(
            DataExportWorkingParams dataExportParams,
            DataSet queryResults,
            FileSystemInfo deleteExtrasFile,
            int primaryKeyColumnIndex,
            string primaryKeyTargetColumnName,
            bool textDataType)
        {
            // Generate commands of the either of these forms:

            // -- Option 1:
            // DELETE FROM t_target_table
            // WHERE id BETWEEN 1 AND 10 AND NOT id IN (1, 2, 3, 4, 5, 6, 8, 10);

            // -- Option 2:
            // DELETE FROM t_target_table target
            // WHERE target.id BETWEEN 1 AND 5 AND
            //       NOT EXISTS ( SELECT S.id
            //                    FROM t_target_table S
            //                    WHERE target.id = S.id AND
            //                          S.id in (1,2,4,5)
            //                   )

            // When textDataType is true, surround text values with single quotes, e.g.
            // DELETE FROM t_target_table
            // WHERE item BETWEEN 'apple' AND 'lemon' AND NOT item IN ('apple', 'banana', 'egg', 'grape', 'kiwi', 'lemon');

            try
            {
                var primaryKeyValues = new SortedSet<dynamic>();

                // Obtain a sorted list of primary keys
                foreach (DataRow currentRow in queryResults.Tables[0].Rows)
                {
                    if (currentRow.IsNull(primaryKeyColumnIndex))
                    {
                        // Ignore nulls
                        continue;
                    }

                    var columnValue = currentRow[primaryKeyColumnIndex];

                    if (primaryKeyValues.Contains(columnValue))
                    {
                        OnWarningEvent(
                            "Table {0} has a duplicate value in the primary key column ({1}); cannot create a file to delete extra rows",
                            dataExportParams.SourceTableNameWithSchema, columnValue);

                        return false;
                    }

                    primaryKeyValues.Add(columnValue);
                }

                ShowDeleteExtrasFilePath(deleteExtrasFile);

                using var writer = new StreamWriter(new FileStream(deleteExtrasFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read));

                writer.WriteLine("-- Commands to delete extra rows from table {0}", dataExportParams.TargetTableNameWithSchema);
                writer.WriteLine();

                // Create delete commands in batches of size PgInsertChunkSize

                var chunkSize = Math.Max(100, mOptions.PgInsertChunkSize);

                const int ITEMS_PER_ROW = 500;
                const bool SHOW_NOT_IN_OPTION = false;

                var pgInsertEnabled = dataExportParams.PgInsertEnabled;

                var currentList = new List<dynamic>();
                var valuesProcessed = 0;
                var chunksProcessed = 0;

                foreach (var item in primaryKeyValues)
                {
                    currentList.Add(item);
                    valuesProcessed++;

                    if (currentList.Count < chunkSize && valuesProcessed < primaryKeyValues.Count)
                        continue;

                    // ReSharper disable HeuristicUnreachableCode
                    // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                    if (chunksProcessed == 0 && SHOW_NOT_IN_OPTION)
#pragma warning disable CS0162
                    {
                        writer.WriteLine("-- Option 1:");
                        writer.WriteLine("--");
                        writer.WriteLine("-- DELETE FROM {0}", dataExportParams.QuotedTargetTableNameWithSchema);
                        writer.WriteLine("-- WHERE {0} BETWEEN {1} AND {2} AND NOT {0} IN (",
                            primaryKeyTargetColumnName,
                            GetFormattedValue(currentList[0], textDataType, pgInsertEnabled),
                            GetFormattedValue(item, textDataType, pgInsertEnabled));

                        var itemsAppended = 0;
                        while (itemsAppended < currentList.Count)
                        {
                            if (itemsAppended > 0)
                                writer.WriteLine(",");

                            var delimitedList = GetCommaSeparatedList(currentList.Skip(itemsAppended).Take(ITEMS_PER_ROW), textDataType, pgInsertEnabled);

                            writer.Write("-- {0}", delimitedList);

                            itemsAppended += ITEMS_PER_ROW;
                        }

                        writer.WriteLine();

                        writer.WriteLine("-- );");

                        writer.WriteLine();
                        writer.WriteLine("-- Option 2:");
                        writer.WriteLine("--");
                    }
#pragma warning restore CS0162
                    // ReSharper restore HeuristicUnreachableCode

                    writer.WriteLine("DELETE FROM {0} target", dataExportParams.QuotedTargetTableNameWithSchema);
                    writer.WriteLine("WHERE {0} BETWEEN {1} AND {2} AND",
                        primaryKeyTargetColumnName,
                        GetFormattedValue(currentList[0], textDataType, pgInsertEnabled),
                        GetFormattedValue(item, textDataType, pgInsertEnabled));

                    writer.WriteLine("      NOT EXISTS ( SELECT S.{0}", primaryKeyTargetColumnName);
                    writer.WriteLine("                   FROM {0} S", dataExportParams.QuotedTargetTableNameWithSchema);
                    writer.WriteLine("                   WHERE target.{0} = S.{0} AND", primaryKeyTargetColumnName);
                    writer.WriteLine("                         S.{0} in (", primaryKeyTargetColumnName);

                    var itemsWritten = 0;
                    while (itemsWritten < currentList.Count)
                    {
                        if (itemsWritten > 0)
                            writer.WriteLine(",");

                        var delimitedList = GetCommaSeparatedList(currentList.Skip(itemsWritten).Take(ITEMS_PER_ROW), textDataType, pgInsertEnabled);
                        writer.Write(delimitedList);

                        itemsWritten += ITEMS_PER_ROW;
                    }

                    writer.WriteLine();
                    writer.WriteLine("                         )");
                    writer.WriteLine("                  );");
                    writer.WriteLine();

                    chunksProcessed++;

                    currentList.Clear();
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent(string.Format("Error in DeleteUsingSingleColumnKey for table {0}", dataExportParams.SourceTableNameWithSchema), ex);
                return false;
            }
        }

        private string GetCommaSeparatedList(IEnumerable<dynamic> items, bool textDataType, bool pgInsertEnabled)
        {
            if (!textDataType)
                return string.Join(",", items);

            var quotedValues = new List<string>();

            foreach (var value in items)
            {
                quotedValues.Add(GetFormattedValue(value, true, pgInsertEnabled));
            }

            return string.Join(",", quotedValues);
        }

        private string GetFormattedValue<dynamic>(dynamic columnValue, bool textDataType, bool pgInsertEnabled)
        {
            if (textDataType)
            {
                return mDbSchemaExporter.FormatValueForInsertAsString(columnValue, pgInsertEnabled);
            }

            return columnValue.ToString();
        }

        private string GetFormattedValue<dynamic>(dynamic columnValue, DBSchemaExporterBase.DataColumnTypeConstants dataColumnTypeConstants, bool pgInsertEnabled)
        {
            if (dataColumnTypeConstants == DBSchemaExporterBase.DataColumnTypeConstants.Text)
            {
                return mDbSchemaExporter.FormatValueForInsertAsString(columnValue, pgInsertEnabled);
            }

            return columnValue.ToString();
        }

        /// <summary>
        /// Return true if the column data type is a data type that we can store in .NET dictionaries and sorted sets (integer, float, double, char, etc.)
        /// </summary>
        /// <param name="dataExportParams"></param>
        /// <param name="columnIndex"></param>
        private bool IsDotNetCompatibleDataType(DataExportWorkingParams dataExportParams, int columnIndex)
        {
            var columnInfo = dataExportParams.ColumnInfoByType[columnIndex];

            var columnName = columnInfo.Key;
            var systemType = columnInfo.Value;

            if (systemType == Type.GetType("System.Byte") ||
                systemType == Type.GetType("System.Int16") ||
                systemType == Type.GetType("System.Int32") ||
                systemType == Type.GetType("System.Int64") ||
                systemType == Type.GetType("System.Char") ||
                systemType == Type.GetType("System.Decimal") ||
                systemType == Type.GetType("System.Single") ||
                systemType == Type.GetType("System.Double") ||
                systemType == Type.GetType("System.String"))
            {
                return true;
            }

            OnWarningEvent("Column {0} in table {1} is not a supported data type (text or number); cannot create a file to delete extra rows", columnName, dataExportParams.SourceTableNameWithSchema);

            return false;
        }

        private void ShowDeleteExtrasFilePath(FileSystemInfo deleteExtrasFile)
        {
            ConsoleMsgUtils.ShowDebugCustom("Delete extras file at " + PathUtils.CompactPathString(deleteExtrasFile.FullName, 120), "  ", 0);
            Console.WriteLine();
        }

        private void WriteMultiColumnPrimaryKeyDeleteQuery(
            StreamWriter writer,
            string targetTableNameWithSchema,
            IReadOnlyList<string> primaryKeyColumnsInTarget,
            IReadOnlyList<DBSchemaExporterBase.DataColumnTypeConstants> primaryKeyColumnTypes,
            dynamic startKey,
            dynamic endKey,
            Dictionary<dynamic, List<string>> currentGroupComparisonCriteria,
            bool pgInsertEnabled)
        {
            // Generate a command of the form:

            // DELETE FROM t_target_table target
            // WHERE target.group_id BETWEEN 1 AND 5 AND
            //       NOT EXISTS ( SELECT S.group_id
            //                          ,S.job
            //                    FROM t_target_table S
            //                    WHERE target.group_id = S.group_id AND
            //                          target.job = S.job AND
            //                          (
            //                              S.group_id = 1 and S.job = 20 OR
            //                              S.group_id = 1 and S.job = 21 OR
            //                              S.group_id = 1 and S.job = 23 OR
            //                              S.group_id = 2 and S.job = 21 OR
            //                              S.group_id = 2 and S.job = 35 OR
            //                              S.group_id = 3 and S.job = 20
            //                          )
            //                   )

            var firstPrimaryKeyColumnName = primaryKeyColumnsInTarget[0];

            writer.WriteLine();
            writer.WriteLine("DELETE FROM {0} target", targetTableNameWithSchema);                                  // DELETE FROM t_target_table target

            writer.WriteLine("WHERE target.{0} BETWEEN {1} AND {2} AND",                                            // WHERE target.group_id BETWEEN 1 AND 5 AND
                firstPrimaryKeyColumnName,
                GetFormattedValue(startKey, primaryKeyColumnTypes[0], pgInsertEnabled),
                GetFormattedValue(endKey, primaryKeyColumnTypes[0], pgInsertEnabled));

            writer.WriteLine("      NOT EXISTS ( SELECT S.{0}", firstPrimaryKeyColumnName);                         //       NOT EXISTS ( SELECT S.group_id,

            for (var i = 1; i < primaryKeyColumnsInTarget.Count; i++)
            {
                writer.WriteLine("                         ,S.{0}", primaryKeyColumnsInTarget[i]);                  //                          ,S.job
            }

            writer.WriteLine("                   FROM {0} S", targetTableNameWithSchema);                           //                    FROM t_target_table S
            writer.WriteLine("                   WHERE target.{0} = S.{0} AND", firstPrimaryKeyColumnName);         // WHERE target.group_id = S.group_id AND

            for (var i = 1; i < primaryKeyColumnsInTarget.Count; i++)
            {
                writer.WriteLine("                         target.{0} = S.{0} AND", primaryKeyColumnsInTarget[i]);  //                          target.job = S.job AND
            }
            writer.WriteLine("                         (");                                                         //                          (

            var valuesWritten = 0;

            foreach (var primaryKeyComparisonList in currentGroupComparisonCriteria)
            {
                var formattedFirstColumnNameAndValue = string.Format(
                    "S.{0} = {1}",
                    firstPrimaryKeyColumnName,
                    GetFormattedValue(primaryKeyComparisonList.Key, primaryKeyColumnTypes[0], pgInsertEnabled));

                // If there is only one primary key column, primaryKeyComparisonList will be an empty list
                // Otherwise it is the formatted list of column names and values to filter on (along with the first-column primary key's value)

                if (primaryKeyComparisonList.Value.Count == 0)
                {
                    if (valuesWritten > 0)
                    {
                        writer.WriteLine(" OR");
                    }

                    writer.Write(formattedFirstColumnNameAndValue);                                                 //                              S.group_id = 1
                    valuesWritten++;

                    continue;
                }

                foreach (var comparisonCriteria in primaryKeyComparisonList.Value)
                {
                    if (valuesWritten > 0)
                    {
                        writer.WriteLine(" OR");
                    }

                    writer.Write("{0} AND {1}", formattedFirstColumnNameAndValue, comparisonCriteria);              //                              S.group_id = 1 and S.job = 20 OR
                    valuesWritten++;
                }
            }

            if (valuesWritten > 0)
            {
                writer.WriteLine();
            }

            // Add the required closing parentheses and semicolon
            writer.WriteLine("                         )");
            writer.WriteLine("                  );");
        }
    }
}
