using System;
using System.Collections.Generic;
using System.Text;

namespace DB_Schema_Export_Tool
{
    public class DataExportWorkingParams
    {
        /// <summary>
        /// List of column names and column types in the source table
        /// </summary>
        public List<KeyValuePair<string, Type>> ColumnInfoByType { get; }

        /// <summary>
        /// List of parsed columns
        /// </summary>
        public List<KeyValuePair<string, DBSchemaExporterBase.DataColumnTypeConstants>> ColumnNamesAndTypes { get; }

        /// <summary>
        /// Separation character for data columns
        /// </summary>
        public char ColSepChar { get; set; }

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
        /// Target table schema and name; the name is only quoted if it has non-alphanumeric characters
        /// </summary>
        public string TargetTableNameWithSchema { get; set; }

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
        /// INSERT INTO statements using the ON CONFLICT(key_column) DO UPDATE SET syntax
        /// </summary>
        public bool PgInsertEnabled { get; set; }

        /// <summary>
        /// When exporting table data, this defines the text to write for null columns
        /// </summary>
        public string NullValue { get;  }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="pgInsertEnabled">True using insert commands formatted as PostgreSQL compatible INSERT INTO statements</param>
        /// <param name="nullValueFlag">Text to write for null columns</param>
        public DataExportWorkingParams(bool pgInsertEnabled, string nullValueFlag)
        {
            ColumnInfoByType = new List<KeyValuePair<string, Type>>();
            ColumnNamesAndTypes = new List<KeyValuePair<string, DBSchemaExporterBase.DataColumnTypeConstants>>();

            ColSepChar = ',';

            HeaderRowValues = new StringBuilder();

            IdentityColumnFound = false;
            IdentityColumnName = string.Empty;

            PgInsertFooters = new List<string>();
            PgInsertHeaders = new List<string>();
            PgInsertEnabled = pgInsertEnabled;

            NullValue = nullValueFlag ?? string.Empty;
        }

    }
}
