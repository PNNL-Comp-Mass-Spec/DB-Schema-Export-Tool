using System.Collections.Generic;

namespace DB_Schema_Export_Tool
{
    internal class TableDataSortOrder
    {
        /// <summary>
        /// List of 1-based column numbers to sort on
        /// </summary>
        /// <remarks>Keys are 1-based column number; values are true if the column is numeric, false if text</remarks>
        public List<KeyValuePair<int, bool>> SortColumns { get; }

        /// <summary>
        /// When true, treat the first column as numeric
        /// Subsequent columns will be treated as numeric if the value for every row is numeric
        /// </summary>
        public bool SortNumeric { get; set; }

        /// <summary>
        /// Parameterless constructor
        /// </summary>
        public TableDataSortOrder() : this(new List<int>(), false)
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <remarks>
        /// <para>
        /// If sortNumeric is true, the value for the first column in SortColumns will be set to true
        /// </para>
        /// <para>
        /// If SortColumns has more than one column, the value will be updated to True for the additional column(s) if every row for that column has a number
        /// </para>
        /// </remarks>
        /// <param name="sortColumns">List of 1-based column numbers to sort on</param>
        /// <param name="sortNumeric">True if the first column is presumed to be numeric</param>
        public TableDataSortOrder(List<int> sortColumns, bool sortNumeric)
        {
            SortColumns = new List<KeyValuePair<int, bool>>();
            SortNumeric = sortNumeric;

            var columnNumber = 0;

            foreach (var sortColumn in sortColumns)
            {
                columnNumber++;

                var isNumeric = columnNumber == 1 && sortNumeric;
                SortColumns.Add(new KeyValuePair<int, bool>(sortColumn, isNumeric));
            }
        }

        /// <summary>
        /// Show the comma-separated list of sort column numbers
        /// </summary>
        /// <returns>Sort column number(s)</returns>
        public override string ToString()
        {
            var sortColumns = string.Empty;

            // ReSharper disable once ForeachCanBeConvertedToQueryUsingAnotherGetEnumerator
            foreach (var sortColumn in SortColumns)
            {
                sortColumns = sortColumns.Length == 0
                    ? sortColumn.Key.ToString()
                    : string.Format("{0}, {1}", sortColumns, sortColumn.Key);
            }

            return string.Format("Sort column numbers: {0}", sortColumns);
        }
    }
}
