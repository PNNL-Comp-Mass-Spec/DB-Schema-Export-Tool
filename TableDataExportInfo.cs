using System.Runtime.CompilerServices;

namespace DB_Schema_Export_Tool
{
    public class TableDataExportInfo
    {
        /// <summary>
        /// Source table name
        /// </summary>
        /// <remarks>Can either be just the table name or SchemaName.TableName</remarks>
        public string SourceTableName { get;}

        public string TargetSchemaName { get; set; }

        public string TargetTableName { get; set; }

        public bool UseMergeStatement { get; set; }
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
        /// <param name="sourceTableName">Can either be just the table name or SchemaName.TableName</param>
        public TableDataExportInfo(string sourceTableName)
        {
            SourceTableName = sourceTableName;
        }

        public override string ToString()
        {
            return SourceTableName;
        }
    }
}
