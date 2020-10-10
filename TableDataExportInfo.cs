using System;
using System.Collections.Generic;

namespace DB_Schema_Export_Tool
{
    public class TableDataExportInfo
    {
        // Ignore Spelling: PostgreSQL

        /// <summary>
        /// Source table name
        /// </summary>
        /// <remarks>Can either be just the table name or SchemaName.TableName</remarks>
        public string SourceTableName { get; }

        /// <summary>
        /// Target schema name
        /// </summary>
        public string TargetSchemaName { get; set; }

        /// <summary>
        /// Target table name
        /// </summary>
        public string TargetTableName { get; set; }

        /// <summary>
        /// When true, export data from SQL Server using insert commands formatted as PostgreSQL compatible
        /// INSERT INTO statements using the ON CONFLICT(key_column) DO UPDATE SET syntax
        /// </summary>
        public bool UsePgInsert { get; set; }

        /// <summary>
        /// Primary key column (or columns)
        /// Only used when UsePgInsert is true
        /// </summary>
        /// <remarks>
        /// If UsePgInsert is true, but this list is empty, will auto-populate using the table's identity column
        /// </remarks>
        public SortedSet<string> PrimaryKeyColumns { get; }

        /// <summary>
        /// Column name for filtering rows by date when exporting data
        /// </summary>
        /// <remarks>Empty string if the filter is not enabled</remarks>
        public string DateColumnName { get; private set; }

        /// <summary>
        /// Minimum date to use when filtering rows by Date
        /// </summary>
        /// <remarks>Ignored if DateColumnName is empty</remarks>
        public DateTime MinimumDate { get; private set; }

        /// <summary>
        /// True if DateColumnName is defined and MinimumDate is greater than DateTime.MinValue; otherwise false
        /// </summary>
        public bool FilterByDate => !string.IsNullOrWhiteSpace(DateColumnName) && MinimumDate > DateTime.MinValue;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="sourceTableName">Either just the table name or SchemaName.TableName (depends on the circumstance)</param>
        public TableDataExportInfo(string sourceTableName)
        {
            SourceTableName = sourceTableName;
            PrimaryKeyColumns = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);

            DefineDateFilter(string.Empty, DateTime.MinValue);
        }

        /// <summary>
        /// Update the date column and minimum date
        /// </summary>
        /// <param name="dateColumnName"></param>
        /// <param name="minimumDate"></param>
        /// <remarks>To remove a filter, send an empty string for dateColumnName</remarks>
        public void DefineDateFilter(string dateColumnName, DateTime minimumDate)
        {
            if (string.IsNullOrWhiteSpace(dateColumnName))
            {
                DateColumnName = string.Empty;
                MinimumDate = DateTime.MinValue;
                return;
            }

            DateColumnName = dateColumnName;
            MinimumDate = minimumDate;
        }

        /// <summary>
        /// ToString text
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return SourceTableName;
        }
    }
}
