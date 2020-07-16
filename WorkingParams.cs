using System.Collections.Generic;
using System.IO;

namespace DB_Schema_Export_Tool
{
    public class WorkingParams
    {
        /// <summary>
        /// If true, count the number of objects to script, but don't actually script them
        /// </summary>
        public bool CountObjectsOnly { get; set; }
        
        /// <summary>
        /// List of script file names (or relative paths) for loading data into a PostgreSQL database
        /// </summary>
        /// <remarks>We use Linux-style path separators when storing relative paths</remarks>
        public List<string> DataLoadScriptFiles { get; }

        /// <summary>
        /// Output directory for the current database
        /// </summary>
        public string OutputDirectoryPathCurrentDB { get; set; }

        /// <summary>
        /// Base output directory
        /// </summary>
        public DirectoryInfo OutputDirectory { get; set; }

        /// <summary>
        /// Number of script files created
        /// </summary>
        public int ProcessCount { get; set; }

        /// <summary>
        /// Number of scripts to create
        /// </summary>
        public int ProcessCountExpected { get; set; }

        /// <summary>
        /// Table names to skip for schema and/or data export
        /// </summary>
        public SortedSet<string> TablesToSkip { get; }

        /// <summary>
        /// Warning messages shown at the console
        /// </summary>
        /// <remarks>These messages are displayed again after all tables have been processed</remarks>
        public SortedSet<string> WarningMessages { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        public WorkingParams()
        {
            DataLoadScriptFiles = new List<string>();
            TablesToSkip = new SortedSet<string>();
            WarningMessages = new SortedSet<string>();
            Reset();
        }

        /// <summary>
        /// Add a file to be included in the bash script for loading data into a PostgreSQL database
        /// </summary>
        /// <param name="scriptFileName"></param>
        public void AddDataLoadScriptFile(string scriptFileName)
        {
            DataLoadScriptFiles.Add(scriptFileName);
        }

        /// <summary>
        /// Add a warning message
        /// </summary>
        /// <param name="message"></param>
        public void AddWarningMessage(string message)
        {
            if (WarningMessages.Contains(message))
                return;

            WarningMessages.Add(message);
        }

        /// <summary>
        /// Reset properties to their initial values
        /// </summary>
        public void Reset()
        {
            CountObjectsOnly = true;
            DataLoadScriptFiles.Clear();
            TablesToSkip.Clear();
            WarningMessages.Clear();
            OutputDirectoryPathCurrentDB = string.Empty;
            ProcessCount = 0;
            ProcessCountExpected = 0;
        }

    }
}
