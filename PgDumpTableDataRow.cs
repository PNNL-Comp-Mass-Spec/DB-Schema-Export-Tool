using System.Collections.Generic;

namespace DB_Schema_Export_Tool
{
    internal class PgDumpTableDataRow
    {
        /// <summary>
        /// Tab-separated list of data values
        /// </summary>
        public string DataRow { get; }

        /// <summary>
        /// List of data values extracted from DataRow
        /// </summary>
        public List<string> DataValues { get; }

        /// <summary>
        /// Values to sort on
        /// </summary>
        public List<SortKeyValue> SortKeys { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="dataRow">Tab-separated list of data values</param>
        public PgDumpTableDataRow(string dataRow)
        {
            DataRow = dataRow;

            DataValues = new List<string>();
            DataValues.AddRange(dataRow.Split('\t'));

            SortKeys = new List<SortKeyValue>();
        }
    }
}
