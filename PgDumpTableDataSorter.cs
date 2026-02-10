using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using PRISM;

namespace DB_Schema_Export_Tool
{
    internal class PgDumpTableDataSorter : EventNotifier
    {
        /// <summary>
        /// Tracks how to sort data exported by PgDump
        /// </summary>
        /// <remarks>Keys are table names (including schema), values are the column numbers to sort on</remarks>
        private readonly Dictionary<string, TableDataSortOrder> mTableDataSortOrder;

        /// <summary>
        /// When true, store the original PgDump table data files in directory Replaced_PgDump_Files
        /// </summary>
        public bool KeepPgDumpFiles { get; }

        private bool DefinePgDumpTableDataSortKeys(List<PgDumpTableDataRow> tableData, TableDataSortOrder sortOrder)
        {
            try
            {
                if (sortOrder.SortColumns.Count == 0)
                    return true;

                // Examine the sort columns to see if they should be sorted numerically
                for (var sortColumnIndex = 0; sortColumnIndex < sortOrder.SortColumns.Count; sortColumnIndex++)
                {
                    if (sortOrder.SortColumns[sortColumnIndex].Value)
                    {
                        // The column has already been identified as being numeric (column Sort_Numeric was True for this table in the Data Table Sort Order text file)
                        continue;
                    }

                    var columnNumber = sortOrder.SortColumns[sortColumnIndex].Key;

                    var numericValues = 0;

                    foreach (var row in tableData)
                    {
                        if (columnNumber > row.DataValues.Count)
                            continue;

                        if (string.IsNullOrWhiteSpace(row.DataValues[columnNumber - 1]))
                        {
                            // Treat an empty string as number (with a value of 0)
                            numericValues++;
                            continue;
                        }

                        if (double.TryParse(row.DataValues[columnNumber - 1], out _))
                        {
                            numericValues++;
                        }
                    }

                    if (numericValues == tableData.Count)
                    {
                        // Every row has a numeric value in column columnNumber
                        sortOrder.SortColumns[sortColumnIndex] = new KeyValuePair<int, bool>(columnNumber, true);
                    }
                }

                // Define the sort key values for each row
                foreach (var row in tableData)
                {
                    // ReSharper disable once ForeachCanBePartlyConvertedToQueryUsingAnotherGetEnumerator
                    foreach (var sortColumn in sortOrder.SortColumns)
                    {
                        var columnNumber = sortColumn.Key;
                        var isNumeric = sortColumn.Value;

                        if (columnNumber > row.DataValues.Count)
                            continue;

                        var sortKey = new SortKeyValue(row.DataValues[columnNumber - 1], isNumeric);
                        row.SortKeys.Add(sortKey);
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in DefinePgDumpTableDataSortKeys", ex);
                return false;
            }
        }

        private bool GetTableDataSortOrder(string tableName, string tableNameWithSchema, out TableDataSortOrder sortOrder)
        {
            if (mTableDataSortOrder.TryGetValue(tableName, out var sortColumnsFromTableName))
            {
                sortOrder = sortColumnsFromTableName;
                return true;
            }

            if (!string.IsNullOrWhiteSpace(tableNameWithSchema) && mTableDataSortOrder.TryGetValue(tableNameWithSchema, out var sortColumnsFromNameWithSchema))
            {
                sortOrder = sortColumnsFromNameWithSchema;
                return true;
            }

            sortOrder = new TableDataSortOrder();
            return false;
        }

        /// <summary>
        /// Load the PgDump table data sort order file
        /// </summary>
        /// <param name="pgDumpTableDataSortOrderFile">PgDump table data sort order file path</param>
        /// <returns>True if successful, false if an error</returns>
        public bool LoadPgDumpTableDataSortOrderFile(string pgDumpTableDataSortOrderFile)
        {
            try
            {
                mTableDataSortOrder.Clear();

                var sortOrderFile = new FileInfo(pgDumpTableDataSortOrderFile);

                if (!sortOrderFile.Exists)
                {
                    OnErrorEvent("PgDump table data sort order file not found: {0}", sortOrderFile.FullName);
                    return false;
                }

                using var reader = new StreamReader(new FileStream(sortOrderFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                var lineNumber = 0;
                var headersChecked = false;
                var errorCount = 0;

                while (!reader.EndOfStream)
                {
                    lineNumber++;

                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    var dataValues = dataLine.Split('\t');

                    if (dataValues.Length < 2)
                    {
                        OnDebugEvent("Skipping line {0} in file {1} since it does not have at least two tab-separated columns", lineNumber, sortOrderFile.Name);
                    }

                    if (!headersChecked)
                    {
                        headersChecked = true;

                        if (dataValues[0].Equals("Table_Name", StringComparison.OrdinalIgnoreCase))
                        {
                            // Header row; skip it
                            continue;
                        }
                    }

                    if (dataValues.Length < 2 || string.IsNullOrWhiteSpace(dataValues[1]))
                    {
                        // No sort columns are defined
                        continue;
                    }

                    var tableName = dataValues[0];
                    var sortColumnValues = dataValues[1].Split(',').ToList();
                    var sortColumns = new List<int>();

                    foreach (var value in sortColumnValues)
                    {
                        if (!int.TryParse(value, out var columnNumber))
                        {
                            OnErrorEvent("Line {0} in the PgDump table data sort order file has an invalid column number in the second column; should be a non-zero integer, not {1}; see {2}",
                                lineNumber, value, sortOrderFile.Name);

                            errorCount++;
                            continue;
                        }

                        sortColumns.Add(columnNumber);
                    }

                    var sortNumeric = false;

                    if (dataValues.Length > 2 && bool.TryParse(dataValues[2], out var useNumericSort))
                    {
                        sortNumeric = useNumericSort;
                    }

                    if (mTableDataSortOrder.ContainsKey(tableName))
                    {
                        OnDebugEvent("Ignoring duplicate sort order definition for table {0}", tableName);
                        continue;
                    }

                    mTableDataSortOrder.Add(tableName, new TableDataSortOrder(sortColumns, sortNumeric));
                }

                return errorCount == 0;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in LoadPgDumpTableDataSortOrderFile", ex);
                return false;
            }
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="keepPgDumpFiles">When true, store the original PgDump table data files in directory Replaced_PgDump_Files</param>
        public PgDumpTableDataSorter(bool keepPgDumpFiles)
        {
            mTableDataSortOrder = new Dictionary<string, TableDataSortOrder>(StringComparer.OrdinalIgnoreCase);
            KeepPgDumpFiles = keepPgDumpFiles;
        }

        private bool ReplacePgDumpTableDataFile(FileInfo tableDataOutputFile, List<string> startingFileContent, List<string> endingFileContent, List<PgDumpTableDataRow> tableData)
        {
            try
            {
                var updatedFile = new FileInfo(string.Format("{0}.tmp", tableDataOutputFile.FullName));

                using (var writer = new StreamWriter(new FileStream(updatedFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    // Use Unix line endings
                    writer.NewLine = "\n";

                    foreach (var row in startingFileContent)
                    {
                        writer.WriteLine(row);
                    }

                    foreach (var row in tableData)
                    {
                        writer.WriteLine(row.DataRow);
                    }

                    foreach (var row in endingFileContent)
                    {
                        writer.WriteLine(row);
                    }
                }

                // Replace the original file with the sorted data file
                var finalFilePath = tableDataOutputFile.FullName;

                var replacedFilesFolder = new DirectoryInfo(Path.Combine(tableDataOutputFile.Directory?.FullName ?? string.Empty, "Replaced_PgDump_Files"));

                if (KeepPgDumpFiles)
                {
                    if (!replacedFilesFolder.Exists)
                        replacedFilesFolder.Create();

                    var replacedFile = new FileInfo(Path.Combine(replacedFilesFolder.FullName, tableDataOutputFile.Name));

                    if (replacedFile.Exists)
                        replacedFile.Delete();

                    tableDataOutputFile.MoveTo(replacedFile.FullName);
                }
                else
                {
                    tableDataOutputFile.Delete();
                }

                updatedFile.MoveTo(finalFilePath);
                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ReplacePgDumpTableDataFile", ex);
                return false;
            }
        }

        /// <summary>
        /// Possibly sort table data in a PgDump output file
        /// </summary>
        /// <param name="tableName">Table name (possibly with schema)</param>
        /// <param name="tableNameWithSchema">Table name with schema</param>
        /// <param name="tableDataOutputFile">Table data output file from PgDump</param>
        /// <returns>True if successful, false if an error</returns>
        public bool SortPgDumpTableData(string tableName, string tableNameWithSchema, FileInfo tableDataOutputFile)
        {
            try
            {
                if (!GetTableDataSortOrder(tableName, tableNameWithSchema, out var sortOrder))
                {
                    string tableNameToShow;

                    if (string.IsNullOrWhiteSpace(tableNameWithSchema))
                    {
                        tableNameToShow = tableName;
                    } else if (tableName.Equals(tableNameWithSchema))
                    {
                        tableNameToShow = tableName;
                    }
                    else
                    {
                        tableNameToShow = string.Format("{0} ({1})", tableName, tableNameWithSchema);
                    }

                    OnDebugEvent("Sort order not defined for table {0}; not sorting table data", tableNameToShow);
                    return true;
                }

                // Cache the contents of tableDataOutputFile in memory

                var startingFileContent = new List<string>();
                var endingFileContent = new List<string>();

                var tableData = new List<PgDumpTableDataRow>();

                var copyCommandFound = false;
                var parsingTableData = false;

                using (var reader = new StreamReader(new FileStream(tableDataOutputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        if (dataLine == null)
                            continue;

                        if (copyCommandFound && !parsingTableData)
                        {
                            // Data line that occurs after the table data
                            endingFileContent.Add(dataLine);
                            continue;
                        }

                        if (dataLine.StartsWith("COPY "))
                        {
                            // Start of the table data
                            copyCommandFound = true;
                            parsingTableData = true;
                            startingFileContent.Add(dataLine);
                            continue;
                        }

                        if (!parsingTableData)
                        {
                            // Data line that occurs before the table data
                            startingFileContent.Add(dataLine);
                            continue;
                        }

                        if (dataLine.StartsWith(@"\."))
                        {
                            // End of the table data
                            parsingTableData = false;
                            endingFileContent.Add(dataLine);
                            continue;
                        }

                        // Table data row
                        tableData.Add(new PgDumpTableDataRow(dataLine));
                    }
                }

                // Define the sort key(s) for each data row
                // This includes updating IsNumeric for columns in sortOrder
                var sortKeysDefined = DefinePgDumpTableDataSortKeys(tableData, sortOrder);

                if (!sortKeysDefined)
                    return false;

                // Sort the data
                tableData.Sort(new PgDumpTableDataRowComparer());

                // Write out a new version of the file, with sorted data
                return ReplacePgDumpTableDataFile(tableDataOutputFile, startingFileContent, endingFileContent, tableData);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in SortPgDumpTableData", ex);
                return false;
            }
        }

        /// <summary>
        /// Custom comparer for sorting PgDump table data rows
        /// </summary>
        private class PgDumpTableDataRowComparer : IComparer<PgDumpTableDataRow>
        {
            public int Compare(PgDumpTableDataRow x, PgDumpTableDataRow y)
            {
                if (x?.SortKeys == null || y?.SortKeys == null)
                    return 0;

                for (var sortKeyIndex = 0; sortKeyIndex < x.SortKeys.Count; sortKeyIndex++)
                {
                    if (sortKeyIndex >= y.SortKeys.Count)
                    {
                        // x has more sort keys than y (this is unexpected)
                        return 1;
                    }

                    if (x.SortKeys[sortKeyIndex].IsNumeric)
                    {
                        if (x.SortKeys[sortKeyIndex].NumericValue < y.SortKeys[sortKeyIndex].NumericValue)
                        {
                            return -1;
                        }

                        if (x.SortKeys[sortKeyIndex].NumericValue > y.SortKeys[sortKeyIndex].NumericValue)
                        {
                            return 1;
                        }
                    }
                    else
                    {
                        var comparison = string.CompareOrdinal(x.SortKeys[sortKeyIndex].Value, y.SortKeys[sortKeyIndex].Value);

                        if (comparison != 0)
                            return comparison;
                    }
                }

                return 0;
            }
        }
    }
}
