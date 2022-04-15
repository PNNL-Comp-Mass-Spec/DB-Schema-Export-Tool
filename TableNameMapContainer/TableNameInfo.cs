using System;
using System.Collections.Generic;

namespace TableNameMapContainer
{
    public class TableNameInfo
    {
        // Ignore Spelling: stdin

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
        /// <para>
        /// When true, export data from SQL Server using insert commands formatted as PostgreSQL compatible
        /// INSERT INTO statements using the syntax "ON CONFLICT(key_column) DO UPDATE SET"
        /// </para>
        /// <para>
        /// When false, use the syntax "COPY from stdin"
        /// </para>
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
        public TableNameInfo(string sourceTableName)
        {
            SourceTableName = sourceTableName;
            PrimaryKeyColumns = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="tableInfo">Instance of this class to clone</param>
        public TableNameInfo(TableNameInfo tableInfo)
        {
            SourceTableName = tableInfo.SourceTableName;
            TargetSchemaName = tableInfo.TargetSchemaName;
            TargetTableName = tableInfo.TargetTableName;
            UsePgInsert = tableInfo.UsePgInsert;
            PrimaryKeyColumns = tableInfo.PrimaryKeyColumns;
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
