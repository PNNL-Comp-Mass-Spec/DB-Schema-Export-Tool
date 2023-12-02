﻿using System;
using System.Collections.Generic;
using System.Text;

namespace DB_Schema_Export_Tool
{
    /// <summary>
    /// Data export working parameters
    /// </summary>
    public class DataExportWorkingParams
    {
        // Ignore Spelling: PostgreSQL, sep

        /// <summary>
        /// List of column names and column types in the source table
        /// </summary>
        public List<KeyValuePair<string, Type>> ColumnInfoByType { get; }

        /// <summary>
        /// Quoted target column names, by index
        /// </summary>
        /// <remarks>For skipped columns, uses the column name in the source database</remarks>
        public Dictionary<int, string> ColumnNameByIndex { get; }

        /// <summary>
        /// List of parsed columns
        /// Keys are the target column name, values are the data type
        /// </summary>
        /// <remarks>For skipped columns, uses the column name in the source database</remarks>
        public List<KeyValuePair<string, DBSchemaExporterBase.DataColumnTypeConstants>> ColumnNamesAndTypes { get; }

        /// <summary>
        /// Separation character for data columns
        /// </summary>
        public char ColSepChar { get; set; }

        /// <summary>
        /// Database name
        /// </summary>
        public string DatabaseName { get; set; }

        /// <summary>
        /// Set to true if the footer rows need to be written to the output file
        /// </summary>
        public bool FooterWriteRequired { get; set; }

        /// <summary>
        /// List of column names, separated by a comma or a tab
        /// </summary>
        public StringBuilder HeaderRowValues { get; set; }

        /// <summary>
        /// True if the identity column has been determined for a table
        /// </summary>
        public bool IdentityColumnFound { get; set; }

        /// <summary>
        /// Name of the identity column
        /// </summary>
        public string IdentityColumnName { get; set; }

        /// <summary>
        /// Target table schema and name, quoted with either double quotes or square brackets
        /// </summary>
        public string QuotedTargetTableNameWithSchema { get; set; }

        /// <summary>
        /// Source table name
        /// </summary>
        public string SourceTableNameWithSchema { get; set; }

        /// <summary>
        /// Target table schema and name; the name is only quoted if it has non-alphanumeric characters
        /// </summary>
        public string TargetTableNameWithSchema { get; set; }

        /// <summary>
        /// Target table schema
        /// </summary>
        /// <remarks>Will get defined based on TargetTableNameWithSchema</remarks>
        public string TargetTableSchema { get; set; }

        /// <summary>
        /// Target table name
        /// </summary>
        /// <remarks>Will get defined based on TargetTableNameWithSchema</remarks>
        public string TargetTableName { get; set; }

        /// <summary>
        /// SQL to add after the list of values to insert into a table (when PgInsertEnabled is true)
        /// </summary>
        public List<string> PgInsertFooters { get; }

        /// <summary>
        /// SQL to add before the list of values to insert into a table (when PgInsertEnabled is true)
        /// </summary>
        public List<string> PgInsertHeaders { get; }

        /// <summary>
        /// True when exporting data from SQL Server and using insert commands formatted as PostgreSQL compatible
        /// INSERT INTO statements using the ON CONFLICT (key_column) DO UPDATE SET syntax
        /// </summary>
        public bool PgInsertEnabled { get; set; }

        /// <summary>
        /// When exporting table data, this defines the text to write for null columns
        /// </summary>
        public string NullValue { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="pgInsertEnabled">True if using insert commands formatted as PostgreSQL compatible INSERT INTO statements</param>
        /// <param name="nullValueFlag">Text to write for null columns</param>
        public DataExportWorkingParams(bool pgInsertEnabled, string nullValueFlag)
        {
            ColumnInfoByType = new List<KeyValuePair<string, Type>>();
            ColumnNamesAndTypes = new List<KeyValuePair<string, DBSchemaExporterBase.DataColumnTypeConstants>>();

            ColumnNameByIndex = new Dictionary<int, string>();

            ColSepChar = ',';

            DatabaseName = string.Empty;

            HeaderRowValues = new StringBuilder();

            IdentityColumnFound = false;
            IdentityColumnName = string.Empty;

            PgInsertFooters = new List<string>();
            PgInsertHeaders = new List<string>();
            PgInsertEnabled = pgInsertEnabled;

            NullValue = nullValueFlag ?? string.Empty;

            SourceTableNameWithSchema = string.Empty;

            TargetTableNameWithSchema = string.Empty;

            TargetTableSchema = string.Empty;

            TargetTableName = string.Empty;
        }
    }
}
