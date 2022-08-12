using System;
using System.Collections.Generic;
using System.Data;

namespace TableNameMapContainer
{
    public class TableNameInfo
    {
        // Ignore Spelling: stdin

        /// <summary>
        /// Primary key column names, listed in the order that columns were added using AddPrimaryKeyColumn
        /// </summary>
        /// <remarks>
        /// If loaded from the database, the list order will match the order in the database table
        /// If loaded from a table name map file, the list order will match the order in the comma separated list
        /// </remarks>
        private readonly List<string> mPrimaryKeyColumns = new();

        /// <summary>
        /// Primary key column names, as a case-insensitive sorted set
        /// </summary>
        private readonly SortedSet<string> mPrimaryKeyColumnNames = new(StringComparer.OrdinalIgnoreCase);

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
        /// Primary key column (or column names), using target table names
        /// </summary>
        /// <remarks>
        /// <para>
        /// Listed in the order that columns were added using AddPrimaryKeyColumn;
        /// will match the ordinal position database table, or will match the order defined in a table name map file
        /// </para>
        /// <para>
        /// Only used when UsePgInsert is true
        /// </para>
        /// <para>
        /// If UsePgInsert is true, but this list is empty, will be auto-populated using the table's identity column
        /// </para>
        /// <para>
        /// Use ContainsPrimaryKey() to check whether the primary key list includes a given column
        /// </para>
        /// </remarks>
        public IReadOnlyList<string> PrimaryKeyColumns => mPrimaryKeyColumns;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="sourceTableName">Either just the table name or SchemaName.TableName (depends on the circumstance)</param>
        public TableNameInfo(string sourceTableName)
        {
            SourceTableName = sourceTableName;
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
        }

        /// <summary>
        /// Add a primary key column
        /// </summary>
        /// <param name="columnName"></param>
        /// <exception cref="DuplicateNameException"></exception>
        public void AddPrimaryKeyColumn(string columnName)
        {
            if (mPrimaryKeyColumnNames.Contains(columnName))
                throw new DuplicateNameException("Primary keys already include " + columnName);

            // This tracks primary key columns in the order that this method is called
            mPrimaryKeyColumns.Add(columnName);

            // This tracks column names using a SortedSet
            mPrimaryKeyColumnNames.Add(columnName);
        }

        /// <summary>
        /// Returns true if the primary keys include the specified column
        /// </summary>
        /// <param name="columnName"></param>
        public bool ContainsPrimaryKey(string columnName)
        {
            return mPrimaryKeyColumnNames.Contains(columnName);
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
