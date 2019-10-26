using System.IO;
﻿using System.Collections.Generic;
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
        /// List of script file names for loading data into a PostgreSQL database
        /// </summary>
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
        /// Constructor
        /// </summary>
        public WorkingParams()
        {
            DataLoadScriptFiles = new List<string>();
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
        /// Reset properties to their initial values
        /// </summary>
        public void Reset()
        {
            CountObjectsOnly = true;
            DataLoadScriptFiles.Clear();
            OutputDirectoryPathCurrentDB = string.Empty;
            ProcessCount = 0;
            ProcessCountExpected = 0;
        }
    }
}
