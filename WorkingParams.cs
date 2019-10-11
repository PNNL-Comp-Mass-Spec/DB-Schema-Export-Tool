using System.IO;

namespace DB_Schema_Export_Tool
{
    internal class WorkingParams
    {
        public int ProcessCount { get; set; }
        public int ProcessCountExpected { get; set; }
        public string OutputDirectoryPathCurrentDB { get; set; }
        public DirectoryInfo OutputDirectory { get; set; }
        public bool CountObjectsOnly { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public WorkingParams()
        {
            Reset();
        }

        public void Reset()
        {
            ProcessCount = 0;
            ProcessCountExpected = 0;
            OutputDirectoryPathCurrentDB = string.Empty;
            CountObjectsOnly = true;
        }
    }
}
