using System;
using TableNameMapContainer;

namespace DB_Schema_Export_Tool
{
    /// <summary>
    /// Table data export info
    /// </summary>
    public class TableDataExportInfo : TableNameInfo
    {
        // Ignore Spelling: PostgreSQL

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
        public TableDataExportInfo(string sourceTableName) : base(sourceTableName)
        {
            DefineDateFilter(string.Empty, DateTime.MinValue);
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="tableInfo">Instance of TableNameInfo to clone</param>
        public TableDataExportInfo(TableNameInfo tableInfo) : base(tableInfo)
        {
            DefineDateFilter(string.Empty, DateTime.MinValue);
        }

        /// <summary>
        /// Update the date column and minimum date
        /// </summary>
        /// <remarks>To remove a filter, send an empty string for dateColumnName</remarks>
        /// <param name="dateColumnName">Date column name</param>
        /// <param name="minimumDate">Minimum date</param>
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
        public override string ToString()
        {
            return SourceTableName;
        }
    }
}
