using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace DB_Schema_Export_Tool
{
    public class TableDataExportInfo
    {
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
        /// Constructor
        /// </summary>
        /// <param name="sourceTableName">Either just the table name or SchemaName.TableName (depends on the circumstance)</param>
        public TableDataExportInfo(string sourceTableName)
        {
            SourceTableName = sourceTableName;
            PrimaryKeyColumns = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);
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
