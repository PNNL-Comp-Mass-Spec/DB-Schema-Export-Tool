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
