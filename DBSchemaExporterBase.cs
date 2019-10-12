using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;

using PRISM;

namespace DB_Schema_Export_Tool
{
    public abstract class DBSchemaExporterBase : EventNotifier
    {
        #region "Constants and Enums"

        // Note: this value defines the maximum number of data rows that will be exported
        // from tables that are auto-added to the table list for data export
        public const int DATA_ROW_COUNT_WARNING_THRESHOLD = 1000;

        public enum DBSchemaExportErrorCodes
        {
            NoError = 0,
            GeneralError = 1,
            ConfigurationError = 2,
            DatabaseConnectionError = 3,
            OutputDirectoryAccessError = 4,
        }

        public enum PauseStatusConstants
        {
            Unpaused = 0,
            PauseRequested = 1,
            Paused = 2,
            UnpauseRequested = 3,
        }

        public enum SchemaObjectTypeConstants
        {
            SchemasAndRoles = 0,
            Tables = 1,
            Views = 2,
            StoredProcedures = 3,
            UserDefinedFunctions = 4,
            UserDefinedDataTypes = 5,
            UserDefinedTypes = 6,
            Synonyms = 7,
        }

        #endregion

        #region "Classwide Variables"

        protected bool mAbortProcessing;

        protected readonly Regex mColumnCharNonStandardRegEx;

        protected readonly Regex mNonStandardOSChars;

        protected ServerConnectionInfo mCurrentServerInfo;

        protected bool mConnectedToServer;

        protected readonly SchemaExportOptions mOptions;

        #endregion

        #region "Properties"

        public DBSchemaExportErrorCodes ErrorCode { get; private set; }

        public PauseStatusConstants PauseStatus { get; protected set; }

        public Dictionary<string, string> SchemaOutputDirectories { get; }

        public SortedSet<string> TableNamesToAutoSelect { get; }

        public SortedSet<string> TableNameAutoSelectRegEx { get; }

        #endregion

        #region "Events"

        public event DBExportStartingHandler DBExportStarting;

        public event PauseStatusChangeHandler PauseStatusChange;

        public event ProgressCompleteHandler ProgressComplete;

        /// <summary>
        /// Event is raised when we start exporting the objects from a database
        /// </summary>
        /// <param name="databaseName">Database name</param>
        public delegate void DBExportStartingHandler(string databaseName);

        public delegate void PauseStatusChangeHandler();

        public delegate void ProgressCompleteHandler();

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="options"></param>
        protected DBSchemaExporterBase(SchemaExportOptions options)
        {
            mOptions = options;

            SchemaOutputDirectories = new Dictionary<string, string>();
            TableNamesToAutoSelect = new SortedSet<string>();
            TableNameAutoSelectRegEx = new SortedSet<string>();


            var regExOptions = RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline;

            mColumnCharNonStandardRegEx = new Regex("[^a-z0-9_]", regExOptions);

            mNonStandardOSChars = new Regex(@"[^a-z0-9_ =+-,.';`~!@#$%^&(){}\\\[\]]", regExOptions);

            mConnectedToServer = false;
            mCurrentServerInfo = new ServerConnectionInfo(string.Empty, true);

        }


        /// <summary>
        /// Determines the table names for which data will be exported
        /// </summary>
        /// <param name="tableNamesInDatabase">Tables in the database</param>
        /// <param name="tableNamesForDataExport">Table names that should be auto-selected</param>
        /// <returns>Dictionary where keys are table names and values are the maximum number of rows to export</returns>
        protected Dictionary<string, long> AutoSelectTableNamesForDataExport(
            IEnumerable<string> tableNamesInDatabase,
            IEnumerable<string> tableNamesForDataExport)
        {
            try
            {
                OnDebugEvent("Auto-selecting tables to export data from");

                //  Tracks the table names and maximum number of data rows to export (0 means all rows)
                var tablesToExport = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);

                //  Copy the table names from tableNamesForDataExport to dctTablesNames
                //  Store 0 for the hash value since we want to export all of the data rows from the tables in tableNamesForDataExport
                //  Simultaneously, populate intMaximumDataRowsToExport
                if (tableNamesForDataExport != null)
                {
                    foreach (var tableName in tableNamesForDataExport)
                    {
                        tablesToExport.Add(tableName, 0);
                    }

                }

                //  Copy the table names from mTableNamesToAutoSelect to dctTablesNames (if not yet present)
                if (TableNamesToAutoSelect != null)
                {
                    foreach (var tableName in TableNamesToAutoSelect)
                    {
                        if (!tablesToExport.ContainsKey(tableName))
                        {
                            tablesToExport.Add(tableName, DATA_ROW_COUNT_WARNING_THRESHOLD);
                        }
                    }

                }

                const RegexOptions objRegExOptions = RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline;

                if (TableNameAutoSelectRegEx == null)
                    return tablesToExport;

                var regExMatchers = new List<Regex>();
                foreach (var regexItem in TableNameAutoSelectRegEx)
                {
                    regExMatchers.Add(new Regex(regexItem, objRegExOptions));
                }

                foreach (var tableName in tableNamesInDatabase)
                {
                    if (!regExMatchers.Any(matcher => matcher.Match(tableName).Success))
                        continue;

                    if (!tablesToExport.ContainsKey(tableName))
                    {
                        tablesToExport.Add(tableName, DATA_ROW_COUNT_WARNING_THRESHOLD);
                    }
                }

                return tablesToExport;
            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.ConfigurationError, "Error in AutoSelectTableNamesForDataExport", ex);
                return new Dictionary<string, long>();
            }
        }

        protected void CheckPauseStatus()
        {
            if (PauseStatus == PauseStatusConstants.PauseRequested)
            {
                SetPauseStatus(PauseStatusConstants.Paused);
            }

            while (PauseStatus == PauseStatusConstants.Paused && !mAbortProcessing)
            {
                Thread.Sleep(150);
            }

            SetPauseStatus(PauseStatusConstants.Unpaused);
        }

        private string CleanNameForOS(string filename)
        {
            //  Replace any invalid characters in strName with underscores
            return mNonStandardOSChars.Replace(filename, "_");
        }

        /// <summary>
        /// Connect to the server specified in mOptions
        /// </summary>
        /// <returns>True if successfully connected, false if a problem</returns>
        public abstract bool ConnectToServer();

        /// <summary>
        /// Retrieve a list of tables in the given database
        /// </summary>
        /// <param name="databaseName">Database to query</param>
        /// <param name="includeTableRowCounts">When true, then determines the row count in each table</param>
        /// <param name="includeSystemObjects">When true, then also returns system var tables</param>
        /// <returns>Dictionary where keys are table names and values are row counts (if includeTableRowCounts = true)</returns>
        public abstract Dictionary<string, long> GetDatabaseTableNames(string databaseName, bool includeTableRowCounts, bool includeSystemObjects);

        /// <summary>
        /// Retrieve a list of database names for the current server
        /// </summary>
        /// <returns></returns>
        public abstract IEnumerable<string> GetServerDatabases();

        protected void InitializeLocalVariables()
        {
            SetPauseStatus(PauseStatusConstants.Unpaused);

            mAbortProcessing = false;

            ErrorCode = DBSchemaExportErrorCodes.NoError;

            SchemaOutputDirectories.Clear();
        }

        protected void OnDBExportStarting(string databaseName)
        {
            DBExportStarting?.Invoke(databaseName);
        }

        private void OnPauseStatusChange()
        {
            PauseStatusChange?.Invoke();
        }

        protected void OnProgressComplete()
        {
            ProgressComplete?.Invoke();
        }

        protected string PossiblyQuoteColumnName(string strColumnName, bool quoteWithSquareBrackets)
        {
            if (!mColumnCharNonStandardRegEx.Match(strColumnName).Success)
                return strColumnName;

            if (quoteWithSquareBrackets)
            {
                // SQL Server quotes names with square brackets
                return "[" + strColumnName + "]";
            }

            // PostgreSQL quotes names with double quotes
            return "\"" + strColumnName + "\"";

        }

        /// <summary>
        /// Surround text with single quotes
        /// Additionally, if text contains single quotes, replace them with two single quotes
        /// </summary>
        /// <param name="text"></param>
        /// <returns></returns>
        protected string PossiblyQuoteText(string text)
        {
            return string.Format("'{0}'", text.Replace("'", "''"));
        }

        /// <summary>
        /// Request that scripting be paused
        /// </summary>
        /// <remarks>Useful when the scripting is running in another thread</remarks>
        public void RequestPause()
        {
            if (!(PauseStatus == PauseStatusConstants.Paused || PauseStatus == PauseStatusConstants.PauseRequested))
            {
                SetPauseStatus(PauseStatusConstants.PauseRequested);
            }

        }

        /// <summary>
        /// Request that scripting be unpaused
        /// </summary>
        /// <remarks>Useful when the scripting is running in another thread</remarks>
        public void RequestUnpause()
        {
            if (!(PauseStatus == PauseStatusConstants.Unpaused || PauseStatus == PauseStatusConstants.UnpauseRequested))
            {
                SetPauseStatus(PauseStatusConstants.UnpauseRequested);
            }

        }

        protected void ResetServerConnection()
        {
            mConnectedToServer = false;
            mCurrentServerInfo.Reset();
        }

        public abstract bool ScriptServerAndDBObjects(List<string> databaseList, List<string> tableNamesForDataExport);

        protected void SetLocalError(DBSchemaExportErrorCodes eErrorCode, string message)
        {
            SetLocalError(eErrorCode, message, null);
        }

        protected void SetLocalError(DBSchemaExportErrorCodes eErrorCode, string message, Exception ex)
        {
            try
            {
                ErrorCode = eErrorCode;
                OnErrorEvent(message, ex);
            }
            catch (Exception)
            {
                //  Ignore errors here
            }

        }

        private void SetPauseStatus(PauseStatusConstants newPauseStatus)
        {
            PauseStatus = newPauseStatus;
            OnPauseStatusChange();
        }

        public void StoreTableNameAutoSelectRegEx(SortedSet<string> tableNameRegExSpecs)
        {
            TableNameAutoSelectRegEx.Clear();
            foreach (var item in tableNameRegExSpecs)
            {
                TableNameAutoSelectRegEx.Add(item);
            }
        }

        public void StoreTableNamesToAutoSelect(SortedSet<string> tableNames)
        {
            TableNamesToAutoSelect.Clear();
            foreach (var item in tableNames)
            {
                TableNamesToAutoSelect.Add(item);
            }
        }

        /// <summary>
        /// Pause / unpause the scripting
        /// </summary>
        /// <remarks>Useful when the scripting is running in another thread</remarks>
        public void TogglePause()
        {
            if (PauseStatus == PauseStatusConstants.Unpaused)
            {
                SetPauseStatus(PauseStatusConstants.PauseRequested);
            }
            else if (PauseStatus == PauseStatusConstants.Paused)
            {
                SetPauseStatus(PauseStatusConstants.UnpauseRequested);
            }

        }

        protected bool ValidateOutputOptions()
        {

            mOptions.ValidateOutputOptions();

            try
            {

                //  Confirm that the output directory exists
                var outputDirectory = new DirectoryInfo(mOptions.OutputDirectoryPath);

                if (outputDirectory.Exists || mOptions.PreviewExport)
                    return true;

                // Try to create it
                outputDirectory.Create();

                return true;
            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.OutputDirectoryAccessError,
                              "Output directory could not be created: " + mOptions.OutputDirectoryPath, ex);
                return false;
            }

        }

        protected bool WriteTextToFile(
            DirectoryInfo outputDirectory,
            string objectName,
            IEnumerable<string> scriptInfo,
            bool autoAddGoStatements = true,
            string fileExtension = ".sql")
        {
            var outFilePath = "??";

            try
            {
                //  Make sure objectName doesn't contain any invalid characters
                var cleanName = CleanNameForOS(objectName);
                outFilePath = Path.Combine(outputDirectory.FullName, cleanName + fileExtension);

                using (var writer = new StreamWriter(new FileStream(outFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
                {
                    foreach (var item in scriptInfo)
                    {
                        writer.WriteLine(item);
                        if (autoAddGoStatements)
                        {
                            writer.WriteLine("GO");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.OutputDirectoryAccessError, "Error creating file " + outFilePath, ex);
                return false;
            }

            return true;
        }

    }
}
