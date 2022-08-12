using System;
using System.Collections.Generic;
using System.IO;

namespace DB_Schema_Export_Tool
{
    /// <summary>
    /// Working parameters
    /// </summary>
    public class WorkingParams
    {
        // Ignore Spelling: PostgreSQL

        /// <summary>
        /// If true, count the number of objects to script, but don't actually script them
        /// </summary>
        public bool CountObjectsOnly { get; set; }

        /// <summary>
        /// List of script file names (or relative paths) for loading data into a PostgreSQL database
        /// Keys are the source table name, values are the script file names (or relative paths)
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
        /// Dictionary tracking the primary key column(s) for each table, as obtained from the INFORMATION_SCHEMA views
        /// Keys are table names in the source database, values are lists of primary key column names (using column names from the source database)
        /// </summary>
        /// <remarks>The column order in the list matches the column order in the table</remarks>
        public Dictionary<string, List<string>> PrimaryKeysByTable { get; }

        /// <summary>
        /// Set this to true after populating <see cref="PrimaryKeysByTable"/>
        /// </summary>
        public bool PrimaryKeysRetrieved { get; set; }

        /// <summary>
        /// Number of script files created when exporting database schema
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
            PrimaryKeysByTable = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
            TablesToSkip = new SortedSet<string>();
            WarningMessages = new SortedSet<string>();
            Reset();
        }

        /// <summary>
        /// Add a file to be included in the bash script for loading data into a PostgreSQL database
        /// </summary>
        /// <remarks>Will also include files for removing extra rows if DeleteExtraRowsBeforeImport is true</remarks>
        /// <param name="relativeFilePath"></param>
        public void AddDataLoadScriptFile(string relativeFilePath)
        {
            DataLoadScriptFiles.Add(relativeFilePath.Replace('\\', '/'));
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

            PrimaryKeysByTable.Clear();
            PrimaryKeysRetrieved = false;
        }
    }
}
