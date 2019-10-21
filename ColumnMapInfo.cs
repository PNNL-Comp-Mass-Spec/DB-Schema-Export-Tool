using System;
using System.Collections.Generic;
using PRISM;

namespace DB_Schema_Export_Tool
{
    public class ColumnMapInfo
    {
        /// <summary>
        /// Keys are source column name
        /// Values are target column name
        /// </summary>
        private readonly Dictionary<string, string> mColumnNameMap;

        public string SourceTableName { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="sourceTableName"></param>
        public ColumnMapInfo(string sourceTableName)
        {
            SourceTableName = sourceTableName;
            mColumnNameMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        }

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

        public string GetTargetColumnName(string sourceColumnName)
        {
            return mColumnNameMap.TryGetValue(sourceColumnName, out var targetColumnName) ? targetColumnName : sourceColumnName;
        }

        public bool IsColumnDefined(string sourceColumnName)
        {
            return mColumnNameMap.ContainsKey(sourceColumnName);
        }
    }
}
