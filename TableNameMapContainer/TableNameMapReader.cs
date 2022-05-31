using System;
using System.Collections.Generic;
using System.IO;
using PRISM;

namespace TableNameMapContainer;

public enum TableInfoFileColumns
{
    Undefined = 0,
    SourceTableName = 1,
    TargetTableName = 2,
    TargetSchemaName = 3,
    PgInsert = 4,
    KeyColumns = 5
}

public class NameMapReader : EventNotifier
{
    public const string SKIP_FLAG = "<skip>";

    /// <summary>
    /// Read a table name map file, which is typically sent to DB_Schema_Export_Tool.exe via the DataTables parameter
    /// It is a tab-delimited file that can have a varying number of columns
    /// <para>
    /// If the file does not have a header line, will infer the column order based on the number of columns found for a given row
    /// If a header line is found, the column names shown below will be used to track column position (the first column must be named SourceTableName)
    /// </para>
    /// <para>
    /// Option 1: SourceTableName
    /// </para>
    /// <para>
    /// Option 2: SourceTableName  TargetTableName
    /// </para>
    /// <para>
    /// Option 3: SourceTableName  TargetSchemaName  TargetTableName
    /// </para>
    /// <para>
    /// Option 4: SourceTableName  TargetSchemaName  TargetTableName  PgInsert
    /// </para>
    /// <para>
    /// Option 5: SourceTableName  TargetSchemaName  TargetTableName  PgInsert  KeyColumn(s)
    /// </para>
    /// </summary>
    /// <remarks>
    /// This file can also have views that should be renamed
    /// </remarks>
    /// <param name="tableDataFilePath">Tab-delimited text file to read</param>
    /// <param name="pgInsertTableData">Default value for PgInsert</param>
    /// <param name="abortProcessing">Output: true if an error, false if no errors</param>
    /// <returns>List of loaded column info</returns>
    public List<TableNameInfo> LoadTableNameMapFile(string tableDataFilePath, bool pgInsertTableData, out bool abortProcessing)
    {
        var tableNameMap = new List<TableNameInfo>();

        abortProcessing = false;

        try
        {
            if (string.IsNullOrWhiteSpace(tableDataFilePath))
            {
                return tableNameMap;
            }

            var dataFile = new FileInfo(tableDataFilePath);

            if (!dataFile.Exists)
            {
                OnErrorEvent("Table Data File not found: " + dataFile.FullName);
                abortProcessing = true;
                return tableNameMap;
            }

            OnDebugEvent("Reading table information from table data file {0}", dataFile.FullName);

            // If a header line is found, this dictionary is used to track the column positions
            // The first column must be named SourceTableName
            var columnMap = new Dictionary<TableInfoFileColumns, int>();

            var sourceTableNames = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);

            var targetTableNames = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);

            var invalidLineCount = 0;
            var linesRead = 0;

            using var dataReader = new StreamReader(new FileStream(dataFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

            while (!dataReader.EndOfStream)
            {
                var dataLine = dataReader.ReadLine();
                linesRead++;

                // Lines that start with # are treated as comment lines
                if (string.IsNullOrWhiteSpace(dataLine) || dataLine.Trim().StartsWith("#"))
                    continue;

                // Files edited with Excel will have column values surrounded by double quotes if they contain a comma
                // The following method splits the row on tabs, then removes double quotes from quoted values
                var lineParts = SplitLineAndUnquote(dataLine);

                if (columnMap.Count == 0)
                {
                    if (!lineParts[0].Equals("SourceTableName", StringComparison.OrdinalIgnoreCase))
                    {
                        columnMap.Add(TableInfoFileColumns.Undefined, 0);
                    }
                    else
                    {
                        columnMap.Add(TableInfoFileColumns.SourceTableName, 0);

                        for (var i = 1; i < lineParts.Count; i++)
                        {
                            if (lineParts[i].Equals("TargetTableName", StringComparison.OrdinalIgnoreCase))
                            {
                                columnMap.Add(TableInfoFileColumns.TargetTableName, i);
                            }
                            else if (lineParts[i].Equals("TargetSchemaName", StringComparison.OrdinalIgnoreCase))
                            {
                                columnMap.Add(TableInfoFileColumns.TargetSchemaName, i);
                            }
                            else if (lineParts[i].Equals("PgInsert", StringComparison.OrdinalIgnoreCase))
                            {
                                columnMap.Add(TableInfoFileColumns.PgInsert, i);
                            }
                            else if (lineParts[i].StartsWith("KeyColumn", StringComparison.OrdinalIgnoreCase))
                            {
                                columnMap.Add(TableInfoFileColumns.KeyColumns, i);
                            }
                            else
                            {
                                OnWarningEvent("Unrecognized header column in the Table Data File: " + lineParts[i]);
                                OnStatusEvent("See file " + dataFile.FullName);
                                abortProcessing = true;
                                return tableNameMap;
                            }
                        }

                        continue;
                    }
                }

                Dictionary<TableInfoFileColumns, int> columnMapCurrentLine;

                if (!columnMap.ContainsKey(TableInfoFileColumns.Undefined))
                {
                    columnMapCurrentLine = columnMap;
                }
                else
                {
                    columnMapCurrentLine = new Dictionary<TableInfoFileColumns, int>
                    {
                        { TableInfoFileColumns.SourceTableName, 0 }
                    };

                    switch (lineParts.Count)
                    {
                        case 2:
                            columnMapCurrentLine.Add(TableInfoFileColumns.TargetTableName, 1);
                            break;

                        case >= 3:
                            columnMapCurrentLine.Add(TableInfoFileColumns.TargetSchemaName, 1);
                            columnMapCurrentLine.Add(TableInfoFileColumns.TargetTableName, 2);
                            break;
                    }
                }

                var sourceTableName = lineParts[0].Trim();

                if (string.IsNullOrWhiteSpace(sourceTableName))
                {
                    OnWarningEvent("Source table name cannot be blank; see line {0} in {1}", linesRead, dataFile.Name);

                    // Set this to true and abort processing
                    abortProcessing = true;
                    break;
                }

                var tableInfo = new TableNameInfo(sourceTableName)
                {
                    UsePgInsert = pgInsertTableData
                };

                if (TryGetColumnValue(lineParts, columnMapCurrentLine, TableInfoFileColumns.TargetSchemaName, out var targetSchemaName))
                {
                    tableInfo.TargetSchemaName = targetSchemaName;
                }

                if (TryGetColumnValue(lineParts, columnMapCurrentLine, TableInfoFileColumns.TargetTableName, out var targetTableName))
                {
                    tableInfo.TargetTableName = targetTableName;
                }

                // Check for TargetTableName being "true" or "false"
                if (tableInfo.TargetTableName != null &&
                    (tableInfo.TargetTableName.Equals("true", StringComparison.OrdinalIgnoreCase) ||
                     tableInfo.TargetTableName.Equals("false", StringComparison.OrdinalIgnoreCase)))
                {
                    OnWarningEvent(
                        "Invalid line in the table data file; target table name cannot be {0}; see line {1}: {2}",
                        tableInfo.TargetTableName, linesRead, dataLine);

                    // Set this to true, but keep processing, showing up to 5 warnings
                    abortProcessing = true;

                    invalidLineCount++;

                    if (invalidLineCount >= 5)
                        break;

                    continue;
                }

                // Check for duplicate source and/or duplicate target table name

                if (sourceTableNames.Contains(sourceTableName))
                {
                    OnWarningEvent(
                        "Table {0} is listed more than once as a source table in file {1}",
                        sourceTableName, dataFile.Name);

                    // Set this to true, but keep processing, showing up to 5 warnings
                    abortProcessing = true;

                    invalidLineCount++;

                    if (invalidLineCount >= 5)
                        break;

                    continue;
                }

                if (!string.IsNullOrWhiteSpace(tableInfo.TargetTableName) &&
                    !tableInfo.TargetTableName.Equals(SKIP_FLAG) &&
                    targetTableNames.Contains(tableInfo.TargetTableName))
                {
                    OnWarningEvent(
                        "Table {0} is listed more than once as a target table in file {1}",
                        tableInfo.TargetTableName, dataFile.Name);

                    // Set this to true, but keep processing, showing up to 5 warnings
                    abortProcessing = true;

                    invalidLineCount++;

                    if (invalidLineCount >= 5)
                        break;

                    continue;
                }

                sourceTableNames.Add(sourceTableName);

                if (!string.IsNullOrWhiteSpace(tableInfo.TargetTableName))
                {
                    targetTableNames.Add(tableInfo.TargetTableName);
                }

                if (TryGetColumnValue(lineParts, columnMapCurrentLine, TableInfoFileColumns.PgInsert, out var pgInsertValue))
                {
                    // Treat the text "true" or a positive integer as true
                    if (bool.TryParse(pgInsertValue, out var parsedValue))
                    {
                        tableInfo.UsePgInsert = parsedValue;
                    }
                    else if (int.TryParse(pgInsertValue, out var parsedNumber))
                    {
                        tableInfo.UsePgInsert = parsedNumber > 0;
                    }
                }

                if (TryGetColumnValue(lineParts, columnMapCurrentLine, TableInfoFileColumns.KeyColumns, out var keyColumns))
                {
                    // One or more primary key columns
                    foreach (var primaryKeyColumn in keyColumns.Split(','))
                    {
                        tableInfo.PrimaryKeyColumns.Add(primaryKeyColumn);
                    }
                }

                tableNameMap.Add(tableInfo);
            }

            var tableText = tableNameMap.Count == 1 ? "table" : "tables";

            OnDebugEvent("Loaded information for {0} {1} from {2}", tableNameMap.Count, tableText, dataFile.Name);
        }
        catch (Exception ex)
        {
            OnErrorEvent("Error in LoadTablesForDataExport", ex);
        }

        return tableNameMap;
    }

    private static IReadOnlyList<string> SplitLineAndUnquote(string dataLine)
    {
        if (string.IsNullOrWhiteSpace(dataLine))
            return new List<string>();

        var lineParts = dataLine.Split('\t');

        for (var i = 0; i < lineParts.Length; i++)
        {
            if (lineParts[i].StartsWith("\"") && lineParts[i].EndsWith("\""))
                lineParts[i] = lineParts[i].Trim('"');
        }

        return lineParts;
    }

    private static bool TryGetColumnValue(
        IReadOnlyList<string> lineParts,
        IReadOnlyDictionary<TableInfoFileColumns, int> columnMapCurrentLine,
        TableInfoFileColumns column,
        out string value)
    {
        if (columnMapCurrentLine.TryGetValue(column, out var columnIndex) && columnIndex < lineParts.Count)
        {
            value = lineParts[columnIndex].Trim();
            return true;
        }

        value = null;
        return false;
    }
}