using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using PRISM;
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
        // Ignore Spelling: scripter, subquery

        private readonly DBSchemaExporterBase mDbSchemaExporter;

        private readonly SchemaExportOptions mOptions;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="dbSchemaExporter">DB Schema Exporter instances</param>
        /// <param name="options">Options</param>
        public DeleteExtraDataRowsScripter(DBSchemaExporterBase dbSchemaExporter, SchemaExportOptions options)
        {
            mDbSchemaExporter = dbSchemaExporter;
            mOptions = options;
        }

        /// <summary>
        /// Create a SQL script to truncate data in a table
        /// </summary>
        /// <param name="dataExportParams">Data export parameters</param>
        /// <param name="deleteExtrasFile">File with delete statements for deleting extra rows</param>
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
        /// <param name="tableInfo">Table info</param>
        /// <param name="columnMapInfo">Class tracking the source and target column names for the table</param>
        /// <param name="dataExportParams">Data export parameters</param>
        /// <param name="workingParams">Working parameters</param>
        /// <param name="queryResults">Query results</param>
        /// <param name="tableDataOutputFile">Table data output file info</param>
        /// <param name="tableDataOutputFileRelativePath">Table data output file relative path</param>
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
        /// <param name="tableInfo">Table info</param>
        /// <param name="columnMapInfo">Class tracking the source and target column names for the table</param>
        /// <param name="dataExportParams">Data export parameters</param>
        /// <param name="workingParams">Working parameters</param>
        /// <param name="queryResults">Query results</param>
        /// <param name="deleteExtrasFile">File with delete statements for deleting extra rows</param>
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

                var targetColumnName = mDbSchemaExporter.GetTargetColumnName(columnMapInfo, primaryKeyColumn);

                // If targetColumnName is "<skip>", use the original column name instead of the name returned by GetTargetColumnName()
                // For more info, see the comments in method DBSchemaExporterSQLServer.GetTargetPrimaryKeyColumnNames

                var columnNameInTarget = targetColumnName.Equals(NameMapReader.SKIP_FLAG) ? primaryKeyColumn : targetColumnName;

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

                    case DBSchemaExporterBase.DataColumnTypeConstants.SkipColumn:
                        // Assume that this column is number or text
                        // This is the case for column Step_Number in table T_Job_Steps, which is renamed to "step"
                        continue;
                }

                OnWarningEvent(
                    "Primary key column {0} in table {1} is not a number or text; cannot create a file to delete extra rows",
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

            var primaryKeyIsNumeric = columnType == DBSchemaExporterBase.DataColumnTypeConstants.Numeric;
            var primaryKeyIsText = columnType == DBSchemaExporterBase.DataColumnTypeConstants.Text;

            return DeleteUsingSingleColumnKey(dataExportParams, queryResults, deleteExtrasFile, primaryKeyColumnIndices[0], primaryKeyColumnsInTarget[0], primaryKeyIsNumeric, primaryKeyIsText);
        }

        /// <summary>
        /// Create a SQL script to delete extra rows in a table based on data in each of the table's primary key columns
        /// </summary>
        /// <param name="dataExportParams">Data export parameters</param>
        /// <param name="queryResults">Query results</param>
        /// <param name="deleteExtrasFile">File with delete statements for deleting extra rows</param>
        /// <param name="primaryKeyColumnIndices">Primary key column indices</param>
        /// <param name="primaryKeyColumnsInTarget">Primary key column names</param>
        /// <param name="primaryKeyColumnTypes">Primary key column types</param>
        /// <returns>True if successful, false if an error</returns>
        private bool DeleteUsingMultiColumnPrimaryKey(
            DataExportWorkingParams dataExportParams,
            DataSet queryResults,
            FileSystemInfo deleteExtrasFile,
            List<int> primaryKeyColumnIndicesSrc,
            List<string> primaryKeyColumnsInTargetSrc,
            List<DBSchemaExporterBase.DataColumnTypeConstants> primaryKeyColumnTypesSrc)
        {
            // Generate commands to delete extra rows from the target table

            // Option 1, tables with a dual-column primary key
            // DELETE FROM t_target_table target
            // WHERE target.protein_collection_id = 1026 AND
            //       NOT EXISTS ( SELECT S.protein_collection_id
            //                          ,S.reference_id
            //                    FROM t_target_table S
            //                    WHERE target.protein_collection_id = S.protein_collection_id AND
            //                          target.reference_id = S.reference_id AND
            //                          S.protein_collection_id = 1026 AND
            //                          S.reference_id IN (
            //                              152213354, 152213355, 152213356, 152213357, 152213358
            //                          )
            //                   );

            // Option 2, for tables with primary keys involving three or more columns
            // DELETE FROM t_target_table target
            // WHERE target.group_id BETWEEN 1 AND 5 AND
            //       NOT EXISTS ( SELECT S.group_id,
            //                           S.job
            //                    FROM t_target_table S
            //                    WHERE target.group_id = S.group_id AND
            //                          target.job = S.job AND
            //                          (
            //                              S.group_id = 1 AND S.job = 20 OR
            //                              S.group_id = 1 AND S.job = 21 OR
            //                              S.group_id = 1 AND S.job = 23 OR
            //                              S.group_id = 2 AND S.job = 21 OR
            //                              S.group_id = 2 AND S.job = 35 OR
            //                              S.group_id = 3 AND S.job = 20
            //                          )
            //                   )

            try
            {
                // This tracks the primary key values for the first primary key column, along with the comparison criteria required for additional primary key columns
                // Keys are primary key values, values are a list of comparisons to perform (e.g. "S.job = 20", "S.job = 21", "S.job = 23")
                var primaryKeyComparisonCriteria = new SortedDictionary<dynamic, List<string>>();

                // This tracks the primary key values for the first primary key column in tables with dual-column primary keys
                // In the dictionary, keys are primary key values, values are a list of secondary key values
                var dualColumnPrimaryKeyComparisonValues = new SortedDictionary<dynamic, List<string>>();

                var additionalCriteria = new StringBuilder();

                // This is used to hold the value for the secondary primary key column, for the purposes of storing in dualColumnPrimaryKeyComparisonValues
                var additionalValue = new StringBuilder();

                var pgInsertEnabled = dataExportParams.PgInsertEnabled;

                // Define the order with which primary key columns should be processed
                // The primary key column with the fewest number of unique values is processed first, followed by the remaining primary key columns

                GetPrimaryKeyProcessingOrder(
                    queryResults,
                    primaryKeyColumnIndicesSrc, primaryKeyColumnsInTargetSrc, primaryKeyColumnTypesSrc,
                    out var primaryKeyColumnIndices, out var primaryKeyColumnsInTarget, out var primaryKeyColumnTypes);

                var rowCount = queryResults.Tables[0].Rows.Count;

                // If the table has a dual-column primary key, we can use a more efficient delete query, but only use this when the table has more than 1000 rows
                var useDualColumnPrimaryKeyDelete = primaryKeyColumnIndices.Count == 2 && rowCount > 1000;

                foreach (DataRow currentRow in queryResults.Tables[0].Rows)
                {
                    if (currentRow.IsNull(primaryKeyColumnIndices[0]))
                    {
                        // Do not delete rows where the first-column primary key is null
                        continue;
                    }

                    var firstPrimaryKeyValue = currentRow[primaryKeyColumnIndices[0]];

                    additionalCriteria.Clear();

                    // This is only used when dualColumnPrimaryKeyComparisonValues is true
                    additionalValue.Clear();

                    for (var i = 1; i < primaryKeyColumnIndices.Count; i++)
                    {
                        // Values in a multicolumn primary key constraint should never be null, but we'll check for this anyway
                        var valueIsNull = currentRow.IsNull(primaryKeyColumnIndices[i]);

                        var formattedValue = valueIsNull
                            ? string.Empty
                            : GetFormattedValue(currentRow[primaryKeyColumnIndices[i]], primaryKeyColumnTypes[i], pgInsertEnabled);

                        if (useDualColumnPrimaryKeyDelete && i == 1 && !valueIsNull)
                        {
                            additionalValue.Append(formattedValue);
                        }

                        if (additionalCriteria.Length > 0)
                            additionalCriteria.Append(" AND ");

                        if (valueIsNull)
                        {
                            additionalCriteria.AppendFormat("S.{0} Is Null", primaryKeyColumnsInTarget[i]);
                            continue;
                        }

                        additionalCriteria.AppendFormat(
                            "S.{0} = {1}",
                            primaryKeyColumnsInTarget[i],
                            formattedValue);
                    }

                    if (useDualColumnPrimaryKeyDelete)
                    {
                        if (additionalValue.Length == 0)
                        {
                            OnWarningEvent(
                                "Encountered a duplicate primary key ({0}) but there is only one primary key column on table {1}; this should never happen and likely indicates a programming error; see just above dualColumnPrimaryKeyComparisonValues.TryGetValue(firstPrimaryKeyValue, out var secondaryKeyValues)",
                                firstPrimaryKeyValue, dataExportParams.SourceTableNameWithSchema);

                            continue;
                        }

                        if (dualColumnPrimaryKeyComparisonValues.TryGetValue(firstPrimaryKeyValue, out var secondaryKeyValues))
                        {
                            secondaryKeyValues.Add(additionalValue.ToString());
                            continue;
                        }

                        var newSecondaryKeyValues = new List<string> { additionalValue.ToString() };
                        dualColumnPrimaryKeyComparisonValues.Add(firstPrimaryKeyValue, newSecondaryKeyValues);
                    }

                    if (additionalCriteria.Length == 0)
                    {
                        OnWarningEvent(
                            "Encountered a duplicate primary key ({0}) but there is only one primary key column on table {1}; this should never happen and likely indicates a programming error; see just above primaryKeyComparisonCriteria.TryGetValue(firstPrimaryKeyValue, out var comparisonCriteria)",
                            firstPrimaryKeyValue, dataExportParams.SourceTableNameWithSchema);

                        continue;
                    }

                    if (primaryKeyComparisonCriteria.TryGetValue(firstPrimaryKeyValue, out var comparisonCriteria))
                    {
                        comparisonCriteria.Add(additionalCriteria.ToString());
                    }
                    else
                    {
                        var newComparisonCriteria = new List<string> { additionalCriteria.ToString() };
                        primaryKeyComparisonCriteria.Add(firstPrimaryKeyValue, newComparisonCriteria);
                    }
                }

                ShowDeleteExtrasFilePath(deleteExtrasFile);

                using var writer = new StreamWriter(new FileStream(deleteExtrasFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read));

                writer.WriteLine("-- Commands to delete extra rows from table {0}", dataExportParams.TargetTableNameWithSchema);

                if (primaryKeyComparisonCriteria.Count > 0)
                {
                    // Delete rows with primary keys outside the expected range
                    // Generate a command of the form:

                    // DELETE FROM t_target_table target
                    // WHERE target.group_id < 1 OR target.group_id > 5;

                    var minimumValue = primaryKeyComparisonCriteria.Keys.First();
                    var maximumValue = primaryKeyComparisonCriteria.Keys.Last();

                    writer.WriteLine();
                    writer.WriteLine("DELETE FROM {0} target", dataExportParams.TargetTableNameWithSchema);

                    writer.WriteLine("WHERE target.{0} < {1} OR target.{0} > {2};",
                        primaryKeyColumnsInTarget[0],
                        GetFormattedValue(minimumValue, primaryKeyColumnTypes[0], pgInsertEnabled),
                        GetFormattedValue(maximumValue, primaryKeyColumnTypes[0], pgInsertEnabled));
                }

                // Create delete commands using either DeleteUsingDualColumnPrimaryKeys or DeleteUsingGroupedMultiColumnPrimaryKeys

                if (useDualColumnPrimaryKeyDelete)
                {
                    return DeleteUsingDualColumnPrimaryKeys(
                                dataExportParams,
                                primaryKeyColumnsInTarget,
                                primaryKeyColumnTypes,
                                primaryKeyComparisonCriteria,
                                dualColumnPrimaryKeyComparisonValues,
                                writer,
                                pgInsertEnabled);
                }

                return DeleteUsingGroupedMultiColumnPrimaryKeys(
                    dataExportParams,
                    primaryKeyColumnIndices,
                    primaryKeyColumnsInTarget,
                    primaryKeyColumnTypes,
                    primaryKeyComparisonCriteria,
                    writer,
                    pgInsertEnabled);
            }
            catch (Exception ex)
            {
                OnErrorEvent(string.Format("Error in DeleteUsingMultiColumnPrimaryKey for table {0}", dataExportParams.SourceTableNameWithSchema), ex);
                return false;
            }
        }

        /// <summary>
        /// Examine values in the primary key columns to determine the column with the lowest number of unique values
        /// </summary>
        /// <param name="queryResults">Query results</param>
        /// <param name="primaryKeyColumnIndicesSrc">Primary key column indices, as obtained from the database</param>
        /// <param name="primaryKeyColumnsInTargetSrc">Primary key column names (using target database names); this list is parallel to primaryKeyColumnIndicesSrc</param>
        /// <param name="primaryKeyColumnTypesSrc">Primary key column types; this list is parallel to primaryKeyColumnIndicesSrc</param>
        /// <param name="primaryKeyColumnIndices">Output: updated list of primary key column indices</param>
        /// <param name="primaryKeyColumnsInTarget">Output: updated list of primary key column names</param>
        /// <param name="primaryKeyColumnTypes">Output: updated list of primary key column types</param>
        private void GetPrimaryKeyProcessingOrder(
            DataSet queryResults,
            List<int> primaryKeyColumnIndicesSrc,
            List<string> primaryKeyColumnsInTargetSrc,
            List<DBSchemaExporterBase.DataColumnTypeConstants> primaryKeyColumnTypesSrc,
            out List<int> primaryKeyColumnIndices,
            out List<string> primaryKeyColumnsInTarget,
            out List<DBSchemaExporterBase.DataColumnTypeConstants> primaryKeyColumnTypes)

        {
            // Keys in this dictionary are the primary key array indices; values are the unique values for each primary key column
            var primaryKeyColumnValues = new Dictionary<int, SortedSet<dynamic>>();

            var primaryKeyCount = primaryKeyColumnIndicesSrc.Count;

            for (var i = 0; i < primaryKeyCount; i++)
            {
                primaryKeyColumnValues.Add(i, new SortedSet<dynamic>());
            }

            // Populate primaryKeyColumnValues with the unique values defined for each primary key column
            foreach (DataRow currentRow in queryResults.Tables[0].Rows)
            {
                if (currentRow.IsNull(primaryKeyColumnIndicesSrc[0]))
                {
                    // Do not delete rows where the first-column primary key is null
                    continue;
                }

                for (var i = 0; i < primaryKeyCount; i++)
                {
                    if (currentRow.IsNull(primaryKeyColumnIndicesSrc[i]))
                        continue;

                    primaryKeyColumnValues[i].Add(currentRow[primaryKeyColumnIndicesSrc[i]]);
                }
            }

            // Find the primary key column with the fewest number of unique values

            var lowestUniqueValueCount = int.MaxValue;
            var primaryKeyIndexWithLowestCount = -1;

            // ReSharper disable once ForeachCanBePartlyConvertedToQueryUsingAnotherGetEnumerator
            foreach (var item in primaryKeyColumnValues)
            {
                if (item.Value.Count >= lowestUniqueValueCount)
                    continue;

                lowestUniqueValueCount = item.Value.Count;
                primaryKeyIndexWithLowestCount = item.Key;
            }

            if (primaryKeyIndexWithLowestCount < 0)
            {
                // This is unexpected; use the source lists as-is
                primaryKeyColumnIndices = primaryKeyColumnIndicesSrc;
                primaryKeyColumnsInTarget = primaryKeyColumnsInTargetSrc;
                primaryKeyColumnTypes = primaryKeyColumnTypesSrc;
                return;
            }

            // Populate these three lists using the source lists, but add primary key index primaryKeyIndexWithLowestCount first
            primaryKeyColumnIndices = new List<int>();
            primaryKeyColumnsInTarget = new List<string>();
            primaryKeyColumnTypes = new List<DBSchemaExporterBase.DataColumnTypeConstants>();

            primaryKeyColumnIndices.Add(primaryKeyColumnIndicesSrc[primaryKeyIndexWithLowestCount]);
            primaryKeyColumnsInTarget.Add(primaryKeyColumnsInTargetSrc[primaryKeyIndexWithLowestCount]);
            primaryKeyColumnTypes.Add(primaryKeyColumnTypesSrc[primaryKeyIndexWithLowestCount]);

            for (var i = 0; i < primaryKeyCount; i++)
            {
                if (i == primaryKeyIndexWithLowestCount)
                    continue;

                primaryKeyColumnIndices.Add(primaryKeyColumnIndicesSrc[i]);
                primaryKeyColumnsInTarget.Add(primaryKeyColumnsInTargetSrc[i]);
                primaryKeyColumnTypes.Add(primaryKeyColumnTypesSrc[i]);
            }
        }

        /// <summary>
        /// Delete extra rows in a table with a dual-column primary key
        /// </summary>
        /// <param name="dataExportParams">Data export parameters</param>
        /// <param name="primaryKeyColumnsInTarget">Primary key column names</param>
        /// <param name="primaryKeyColumnTypes">Primary key column types</param>
        /// <param name="primaryKeyComparisonCriteria">Primary key comparison criteria; Keys are primary key values, values are a list of comparisons to perform (e.g. "S.job = 20", "S.job = 21", "S.job = 23")</param>
        /// <param name="dualColumnPrimaryKeyComparisonValues">Keys are primary key values, values are a list of secondary key values</param>
        /// <param name="writer">Text file writer</param>
        /// <param name="pgInsertEnabled">True if using insert commands formatted as PostgreSQL compatible INSERT INTO statements</param>
        /// <returns>True if successful, false if an error</returns>
        private bool DeleteUsingDualColumnPrimaryKeys(
            DataExportWorkingParams dataExportParams,
            IReadOnlyList<string> primaryKeyColumnsInTarget,
            IReadOnlyList<DBSchemaExporterBase.DataColumnTypeConstants> primaryKeyColumnTypes,
            SortedDictionary<dynamic, List<string>> primaryKeyComparisonCriteria,
            IReadOnlyDictionary<dynamic, List<string>> dualColumnPrimaryKeyComparisonValues,
            StreamWriter writer,
            bool pgInsertEnabled)
        {
            try
            {
                foreach (var item in primaryKeyComparisonCriteria)
                {
                    if (!dualColumnPrimaryKeyComparisonValues.TryGetValue(item.Key, out List<string> secondaryPrimaryKeyValues))
                    {
                        OnWarningEvent("Did not find key {0} in dualColumnPrimaryKeyComparisonValues for table dataExportParams.SourceTableNameWithSchema; this is unexpected", item.Key);
                        continue;
                    }

                    WriteDualColumnPrimaryKeyDeleteQuery(
                        writer,
                        dataExportParams.TargetTableNameWithSchema,
                        primaryKeyColumnsInTarget,
                        primaryKeyColumnTypes,
                        item.Key,
                        secondaryPrimaryKeyValues,
                        pgInsertEnabled);
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent(string.Format("Error in DeleteUsingDualColumnPrimaryKeys for table {0}", dataExportParams.SourceTableNameWithSchema), ex);

                return false;
            }
        }

        /// <summary>
        /// Delete extra rows in a table with a primary key that references multiple columns
        /// </summary>
        /// <param name="dataExportParams">Data export parameters</param>
        /// <param name="primaryKeyColumnIndices">Primary key column indices</param>
        /// <param name="primaryKeyColumnsInTarget">Primary key column names</param>
        /// <param name="primaryKeyColumnTypes">Primary key column types</param>
        /// <param name="primaryKeyComparisonCriteria">Primary key comparison criteria; Keys are primary key values, values are a list of comparisons to perform (e.g. "S.job = 20", "S.job = 21", "S.job = 23")</param>
        /// <param name="writer">Text file writer</param>
        /// <param name="pgInsertEnabled">True if using insert commands formatted as PostgreSQL compatible INSERT INTO statements</param>
        /// <returns>True if successful, false if an error</returns>
        private bool DeleteUsingGroupedMultiColumnPrimaryKeys(
            DataExportWorkingParams dataExportParams,
            IReadOnlyCollection<int> primaryKeyColumnIndices,
            IReadOnlyList<string> primaryKeyColumnsInTarget,
            IReadOnlyList<DBSchemaExporterBase.DataColumnTypeConstants> primaryKeyColumnTypes,
            SortedDictionary<dynamic, List<string>> primaryKeyComparisonCriteria,
            StreamWriter writer,
            bool pgInsertEnabled)
        {
            try
            {
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
                OnErrorEvent(string.Format("Error in DeleteUsingGroupedMultiColumnPrimaryKeys for table {0}", dataExportParams.SourceTableNameWithSchema), ex);

                return false;
            }
        }

        /// <summary>
        /// Create a SQL script to delete extra rows in a table based on data in primary key column
        /// </summary>
        /// <param name="dataExportParams">Data export parameters</param>
        /// <param name="queryResults">Query results</param>
        /// <param name="deleteExtrasFile">File with delete statements for deleting extra rows</param>
        /// <param name="primaryKeyColumnIndex">Primary key column index</param>
        /// <param name="primaryKeyTargetColumnName">Primary key column name</param>
        /// <param name="primaryKeyIsNumeric">True if the primary key column is a number (most likely an integer)</param>
        /// <param name="primaryKeyIsText">True if the primary key column is text</param>
        /// <returns>True if successful, false if an error</returns>
        private bool DeleteUsingSingleColumnKey(
            DataExportWorkingParams dataExportParams,
            DataSet queryResults,
            FileSystemInfo deleteExtrasFile,
            int primaryKeyColumnIndex,
            string primaryKeyTargetColumnName,
            bool primaryKeyIsNumeric,
            bool primaryKeyIsText)
        {
            // Generate commands of either of these forms:

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

            // When primaryKeyIsText is true, surround text values with single quotes, e.g.
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

                    // ReSharper disable once CanSimplifySetAddingWithSingleCall

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

                // Note that the following method will set the session_replication_role to "replica" if PgInsertEnabled is true
                DBSchemaExporterBase.PossiblyDisableTriggers(dataExportParams, mOptions, writer);

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

                    dynamic rangeStart;
                    dynamic rangeEnd;

                    if (chunksProcessed == 0 && primaryKeyIsNumeric)
                    {
                        // If the primary key column is an integer, use a very small integer for the lower bound to assure that extra rows get deleted
                        rangeStart = item switch
                        {
                            short => short.MinValue,
                            int => int.MinValue,
                            long => long.MinValue,
                            _ => currentList[0]
                        };
                    }
                    else
                    {
                        rangeStart = currentList[0];
                    }

                    if (valuesProcessed >= primaryKeyValues.Count && primaryKeyIsNumeric)
                    {
                        // If the primary key column is an integer, use a very large integer for the upper bound to assure that extra rows get deleted
                        rangeEnd = item switch
                        {
                            short => short.MaxValue,
                            int => int.MaxValue,
                            long => long.MaxValue,
                            _ => item * 10
                        };
                    }
                    else
                    {
                        rangeEnd = item;
                    }

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
                            GetFormattedValue(rangeStart, primaryKeyIsText, pgInsertEnabled),
                            GetFormattedValue(rangeEnd, primaryKeyIsText, pgInsertEnabled));

                        var itemsAppended = 0;
                        while (itemsAppended < currentList.Count)
                        {
                            if (itemsAppended > 0)
                                writer.WriteLine(",");

                            var delimitedList = GetCommaSeparatedList(currentList.Skip(itemsAppended).Take(ITEMS_PER_ROW), primaryKeyIsText, pgInsertEnabled);

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
                        GetFormattedValue(rangeStart, primaryKeyIsText, pgInsertEnabled),
                        GetFormattedValue(rangeEnd, primaryKeyIsText, pgInsertEnabled));

                    writer.WriteLine("      NOT EXISTS ( SELECT S.{0}", primaryKeyTargetColumnName);
                    writer.WriteLine("                   FROM {0} S", dataExportParams.QuotedTargetTableNameWithSchema);
                    writer.WriteLine("                   WHERE target.{0} = S.{0} AND", primaryKeyTargetColumnName);
                    writer.WriteLine("                         S.{0} in (", primaryKeyTargetColumnName);

                    var itemsWritten = 0;
                    while (itemsWritten < currentList.Count)
                    {
                        if (itemsWritten > 0)
                            writer.WriteLine(",");

                        var delimitedList = GetCommaSeparatedList(currentList.Skip(itemsWritten).Take(ITEMS_PER_ROW), primaryKeyIsText, pgInsertEnabled);
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

                // Note that the following method will set the session_replication_role to "origin" if PgInsertEnabled is true
                DBSchemaExporterBase.PossiblyEnableTriggers(dataExportParams, mOptions, writer);

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent(string.Format("Error in DeleteUsingSingleColumnKey for table {0}", dataExportParams.SourceTableNameWithSchema), ex);
                return false;
            }
        }

        private string GetCommaSeparatedList(IEnumerable<dynamic> items, bool primaryKeyIsText, bool pgInsertEnabled)
        {
            if (!primaryKeyIsText)
                return string.Join(",", items);

            var quotedValues = new List<string>();

            foreach (var value in items)
            {
                quotedValues.Add(GetFormattedValue(value, true, pgInsertEnabled));
            }

            return string.Join(",", quotedValues);
        }

        private string GetFormattedValue<dynamic>(dynamic columnValue, bool primaryKeyIsText, bool pgInsertEnabled)
        {
            if (primaryKeyIsText)
            {
                return mDbSchemaExporter.FormatValueForInsertAsString(columnValue, pgInsertEnabled);
            }

            return columnValue.ToString();
        }

        private string GetFormattedValue<dynamic>(dynamic columnValue, DBSchemaExporterBase.DataColumnTypeConstants dataColumnTypeConstants, bool pgInsertEnabled)
        {
            if (dataColumnTypeConstants is DBSchemaExporterBase.DataColumnTypeConstants.Text or DBSchemaExporterBase.DataColumnTypeConstants.IPAddress)
            {
                return mDbSchemaExporter.FormatValueForInsertAsString(columnValue, pgInsertEnabled);
            }

            return columnValue.ToString();
        }

        /// <summary>
        /// Return true if the column data type is a data type that we can store in .NET dictionaries and sorted sets (integer, float, double, char, etc.)
        /// </summary>
        /// <param name="dataExportParams">Data export parameters</param>
        /// <param name="columnIndex">Column index</param>
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

        /// <summary>
        /// Use this method to delete extra rows from large tables with a dual-column primary key
        /// </summary>
        /// <remarks>
        /// In particular t_analysis_job_processor_group_associations and pc.t_protein_collection_members_cached
        /// </remarks>
        /// <param name="writer">Text file writer</param>
        /// <param name="targetTableNameWithSchema">Target table name, with schema</param>
        /// <param name="primaryKeyColumnsInTarget">Primary key column names</param>
        /// <param name="primaryKeyColumnTypes">Primary key column types</param>
        /// <param name="firstPrimaryKeyValue">First primary key value</param>
        /// <param name="secondaryPrimaryKeyValues">Secondary primary key values</param>
        /// <param name="pgInsertEnabled">True if using insert commands formatted as PostgreSQL compatible INSERT INTO statements</param>
        private void WriteDualColumnPrimaryKeyDeleteQuery(
            StreamWriter writer,
            string targetTableNameWithSchema,
            IReadOnlyList<string> primaryKeyColumnsInTarget,
            IReadOnlyList<DBSchemaExporterBase.DataColumnTypeConstants> primaryKeyColumnTypes,
            dynamic firstPrimaryKeyValue,
            List<string> secondaryPrimaryKeyValues,
            bool pgInsertEnabled)
        {
            // Generate a command of the form:

            // DELETE FROM pc.t_protein_collection_members_cached target
            // WHERE target.protein_collection_id = 1026 AND
            //       NOT EXISTS ( SELECT S.protein_collection_id
            //                          ,S.reference_id
            //                    FROM pc.t_protein_collection_members_cached S
            //                    WHERE target.protein_collection_id = S.protein_collection_id AND
            //                          target.reference_id = S.reference_id AND
            //                          S.protein_collection_id = 1026 AND
            //                          S.reference_id IN (
            //                              152213354, 152213355, 152213356, 152213357, 152213358
            //                          )
            //                   );

            if (primaryKeyColumnsInTarget.Count != 2)
            {
                OnErrorEvent("Table {0} has {1} primary key column(s); method WriteDualColumnPrimaryKeyDeleteQuery should only be called for tables with two primary keys",
                    targetTableNameWithSchema, primaryKeyColumnsInTarget.Count);

                return;
            }

            var firstPrimaryKeyColumnName = primaryKeyColumnsInTarget[0];
            var secondPrimaryKeyColumnName = primaryKeyColumnsInTarget[1];

            var formattedPrimaryKeyValue = GetFormattedValue(firstPrimaryKeyValue, primaryKeyColumnTypes[0], pgInsertEnabled);

            writer.WriteLine();
            writer.WriteLine("DELETE FROM {0} target", targetTableNameWithSchema);                                  // DELETE FROM t_target_table target

            writer.WriteLine("WHERE target.{0} = {1} AND",                                                          // WHERE target.protein_collection_id = 1026 AND
                firstPrimaryKeyColumnName,
                formattedPrimaryKeyValue);

            writer.WriteLine("      NOT EXISTS ( SELECT S.{0}", firstPrimaryKeyColumnName);                         //       NOT EXISTS ( SELECT S.protein_collection_id

            for (var i = 1; i < primaryKeyColumnsInTarget.Count; i++)
            {
                writer.WriteLine("                         ,S.{0}", primaryKeyColumnsInTarget[i]);                  //                          ,S.reference_id
            }

            writer.WriteLine("                   FROM {0} S", targetTableNameWithSchema);                           //                    FROM t_target_table S
            writer.WriteLine("                   WHERE target.{0} = S.{0} AND", firstPrimaryKeyColumnName);         //                    WHERE target.protein_collection_id = S.protein_collection_id AND

            for (var i = 1; i < primaryKeyColumnsInTarget.Count; i++)
            {
                writer.WriteLine("                         target.{0} = S.{0} AND", primaryKeyColumnsInTarget[i]);  //                          target.reference_id = S.reference_id AND
            }
            writer.WriteLine("                             S.{0} = {1} AND",                                        //                          S.protein_collection_id = 1026 AND
                firstPrimaryKeyColumnName,
                formattedPrimaryKeyValue);

            if (secondaryPrimaryKeyValues.Count > 0)
            {
                writer.WriteLine("                             S.{0} IN (", //                          S.reference_id IN (
                    secondPrimaryKeyColumnName);

                var valuesWritten = 0;

                foreach (var secondaryKeyValue in secondaryPrimaryKeyValues)
                {
                    if (valuesWritten > 0)
                    {
                        if (valuesWritten % 1000 == 0)
                        {
                            writer.WriteLine(",");
                        }
                        else
                        {
                            writer.Write(", ");
                        }
                    }

                    writer.Write("{0}", secondaryKeyValue);
                    valuesWritten++;
                }

                writer.WriteLine();
            }

            // Add the required closing parentheses and semicolon
            writer.WriteLine("                         )");
            writer.WriteLine("                  );");
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

            // DELETE FROM t_analysis_job_processor_group_membership target
            // WHERE target.processor_id BETWEEN 210 AND 250 AND
            //       NOT EXISTS ( SELECT S.processor_id
            //                          ,S.group_id
            //                    FROM t_analysis_job_processor_group_membership S
            //                    WHERE target.processor_id = S.processor_id AND
            //                          target.group_id = S.group_id AND
            //                          (
            //                              S.processor_id = 210 AND S.group_id = 100 OR
            //                              S.processor_id = 240 AND S.group_id = 100 OR
            //                              S.processor_id = 241 AND S.group_id = 100 OR
            //                              S.processor_id = 243 AND S.group_id = 100 OR
            //                              S.processor_id = 244 AND S.group_id = 100 OR
            //                              S.processor_id = 246 AND S.group_id = 100 OR
            //                              S.processor_id = 247 AND S.group_id = 100 OR
            //                              S.processor_id = 248 AND S.group_id = 100 OR
            //                              S.processor_id = 249 AND S.group_id = 100 OR
            //                              S.processor_id = 250 AND S.group_id = 100
            //                          )
            //                   );

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
            writer.WriteLine("                   WHERE target.{0} = S.{0} AND", firstPrimaryKeyColumnName);         //                    WHERE target.group_id = S.group_id AND

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
