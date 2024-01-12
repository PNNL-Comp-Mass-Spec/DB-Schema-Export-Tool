﻿using System;
using System.Collections.Generic;
using PRISM;
using TableNameMapContainer;

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
        /// <remarks>Keys are not case-sensitive</remarks>
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
        /// <remarks>If the Column Name Map, already has this column, will update the target name</remarks>
        /// <param name="sourceColumnName">Source column name</param>
        /// <param name="targetColumnName">Name of the column in the target table</param>
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
        /// Look for the column name in the column name map dictionary's values and return the original (source) column name if found
        /// If not found, return the provided column name
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns>Source column name if in the name map dictionary, otherwise the provided column name</returns>
        public string GetSourceColumnName(string columnName)
        {
            foreach (var item in mColumnNameMap)
            {
                if (item.Value.Equals(columnName))
                    return item.Key;
            }

            return columnName;
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

        /// <summary>
        /// Update the target column name to be "&lt;skip&gt;"
        /// </summary>
        /// <param name="sourceColumnName"></param>
        public void SkipColumn(string sourceColumnName)
        {
            if (IsColumnDefined(sourceColumnName))
                mColumnNameMap[sourceColumnName] = NameMapReader.SKIP_FLAG;
            else
                AddColumn(sourceColumnName, NameMapReader.SKIP_FLAG);
        }
    }
}
