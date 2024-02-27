using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using PRISM;
using TableNameMapContainer;

namespace DB_Schema_Export_Tool
{
    /// <summary>
    /// SQL Server schema and data exporter
    /// </summary>
    public sealed class DBSchemaExporterSQLServer : DBSchemaExporterBase
    {
        // ReSharper disable once CommentTypo

        // Ignore Spelling: accessor, crlf, currval, dbo, dt, Inline, mtuser, schemas
        // Ignore Spelling: Scripter, setval, smo, Sql, stdin, sysconstraints, sysobjects, syssegments, username, xtype

        // ReSharper disable UnusedMember.Global

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
        /// <param name="tableDataOutputFile">Table data output file info</param>
        /// <param name="tableDataOutputFileRelativePath">Table data output file relative path</param>
        /// <param name="dataExportError">Output: true if an error was encountered, otherwise false</param>
        /// <returns>Insert Into line to use when SaveDataAsInsertIntoStatements is true and PgInsertEnabled is false; otherwise, an empty string</returns>
        private string ExportDBTableDataInit(
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
                // Exporting data from SQL Server and using insert commands formatted as PostgreSQL compatible
                // INSERT INTO statements using the ON CONFLICT (key_column) DO UPDATE SET syntax

                var primaryKeyColumnList = ResolvePrimaryKeys(dataExportParams, workingParams, tableInfo, columnMapInfo);

                bool deleteExtrasThenAddNew;
                bool deleteExtrasUsingPrimaryKey;
                bool useTruncateTable;

                if (tableInfo.PrimaryKeyColumns.Count == dataExportParams.ColumnNamesAndTypes.Count)
                {
                    if (dataRowCount < 100 && tableInfo.PrimaryKeyColumns.Count <= 2)
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

                if (dataRowCount > 0 && !tableInfo.FilterByDate)
                {
                    if (deleteExtrasThenAddNew)
                    {
                        ExportDBTableDataDeleteExtraRows(dataExportParams, queryResults);
                    }
                    else if (deleteExtrasUsingPrimaryKey)
                    {
                        var useGenerics = false;
                        bool success;

                        if (useGenerics)
                        {
                            var deleteExtrasScripter = new DeleteExtraDataRowsScripterGenericT(this, mOptions);

                            RegisterEvents(deleteExtrasScripter);

                            success = deleteExtrasScripter.DeleteExtraRowsInTargetTable(
                                tableInfo, columnMapInfo,
                                dataExportParams, workingParams,
                                queryResults,
                                tableDataOutputFile, tableDataOutputFileRelativePath);
                        }
                        else
                        {
                            var deleteExtrasScripter = new DeleteExtraDataRowsScripter(this, mOptions);

                            RegisterEvents(deleteExtrasScripter);

                            success = deleteExtrasScripter.DeleteExtraRowsInTargetTable(
                                tableInfo, columnMapInfo,
                                dataExportParams, workingParams,
                                queryResults,
                                tableDataOutputFile, tableDataOutputFileRelativePath);
                        }

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

                    headerRows.Add("SET IDENTITY_INSERT " + dataExportParams.QuotedTargetTableNameWithSchema + " ON");
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
                // Use the T-SQL COPY command to export data from a SQL Server database

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

    }
}
