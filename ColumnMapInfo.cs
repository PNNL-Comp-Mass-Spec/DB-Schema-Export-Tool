using System;
using System.Collections.Generic;
using PRISM;

namespace DB_Schema_Export_Tool
{
    /// <summary>
    /// Database column map info
    /// </summary>
    public class ColumnMapInfo
    {
        /// <summary>
        /// Keys are source column name
        /// Values are target column name
        /// </summary>
        /// <remarks>Keys are not case sensitive</remarks>
        private readonly Dictionary<string, string> mColumnNameMap;

        /// <summary>
        /// Table name for the columns tracked by this class
        /// </summary>
        public string SourceTableName { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="sourceTableName">Table name</param>
        public ColumnMapInfo(string sourceTableName)
        {
            SourceTableName = sourceTableName;
            mColumnNameMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Add a column
        /// </summary>
        /// <param name="sourceColumnName">Source column name</param>
        /// <param name="targetColumnName">Name of the column in the target table</param>
        /// <remarks>If the Column Name Map, already has this column, will update the target name</remarks>
        public void AddColumn(string sourceColumnName, string targetColumnName)
        {
            if (!mColumnNameMap.TryGetValue(sourceColumnName, out var existingTargetName))
            {
                mColumnNameMap.Add(sourceColumnName, targetColumnName);
                return;
            }

            ConsoleMsgUtils.ShowDebug("Updating the target name for column {0} from {1} to {2} for table {3}",
                                      sourceColumnName, existingTargetName, targetColumnName, SourceTableName);

            mColumnNameMap[sourceColumnName] = targetColumnName;
        }

        /// <summary>
        /// Get the target column name for the given column
        /// </summary>
        /// <param name="sourceColumnName">Source column name</param>
        /// <returns>Returns the new name if defined in Column Name Map; otherwise returns the source column name</returns>
        public string GetTargetColumnName(string sourceColumnName)
        {
            return mColumnNameMap.TryGetValue(sourceColumnName, out var targetColumnName) ? targetColumnName : sourceColumnName;
        }

        /// <summary>
        /// Return true if the column is defined in Column Name Map
        /// </summary>
        /// <param name="sourceColumnName">Source column name</param>
        public bool IsColumnDefined(string sourceColumnName)
        {
            return mColumnNameMap.ContainsKey(sourceColumnName);
        }
    }
}
