﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using PRISM;

namespace DB_Schema_Export_Tool
{
    public class DBSchemaExportTool : EventNotifier
    {

        #region "Constants and Enums"

        private enum DifferenceReasonType
        {
            Unchanged = 0,
            NewFile = 1,
            Changed = 2
        }

        private enum RepoManagerType
        {
            Svn = 0,
            Hg = 1,
            Git = 2
        }

        #endregion

        #region "Classwide Variables"

        private readonly Regex mDateMatcher;

        private DBSchemaExporterBase mDBSchemaExporter;

        private readonly SchemaExportOptions mOptions;

        #endregion

        #region "Properties"

        public DBSchemaExporterBase.DBSchemaExportErrorCodes ErrorCode => mDBSchemaExporter?.ErrorCode ?? DBSchemaExporterBase.DBSchemaExportErrorCodes.NoError;

        public DBSchemaExporterBase.PauseStatusConstants PauseStatus => mDBSchemaExporter?.PauseStatus ?? DBSchemaExporterBase.PauseStatusConstants.Unpaused;

        /// <summary>
        /// Most recent Status, Warning, or Error message
        /// </summary>
        public string StatusMessage { get; private set; }

        #endregion


        #region "Events"

        public event DBSchemaExporterBase.DBExportStartingHandler DBExportStarting;

        public event DBSchemaExporterBase.PauseStatusChangeHandler PauseStatusChange;

        public event DBSchemaExporterBase.ProgressCompleteHandler ProgressComplete;

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="options"></param>
        public DBSchemaExportTool(SchemaExportOptions options)
        {
            mOptions = options;
            mDateMatcher = new Regex(@"'\d+/\d+/\d+ \d+:\d+:\d+ [AP]M'", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            if (mOptions.PostgreSQL)
            {
                mDBSchemaExporter = new DBSchemaExporterPostgreSQL(mOptions);
            }
            else
            {
                mDBSchemaExporter = new DBSchemaExporterSQLServer(mOptions);
            }
            RegisterEvents(mDBSchemaExporter);
        }

        /// <summary>
        /// Request that processing be aborted
        /// </summary>
        /// <remarks>Useful when the scripting is running in another thread</remarks>
        public void AbortProcessingNow()
        {
            mDBSchemaExporter?.AbortProcessingNow();
        }

        private static void AddToSortedSetIfNew(ISet<string> filteredNames, string value)
        {
            if (!string.IsNullOrWhiteSpace(value) && !filteredNames.Contains(value))
                filteredNames.Add(value);
        }

        private string CheckPlural(int value, string textIfOne, string textIfSeveral)
        {
            return value == 1 ? textIfOne : textIfSeveral;
        }

        /// <summary>
        /// Connect to the server specified in mOptions
        /// </summary>
        /// <returns>True if successfully connected, false if a problem</returns>
        public bool ConnectToServer()
        {
            var isValid = ValidateSchemaExporter();
            if (!isValid)
                return false;

            return mDBSchemaExporter.ConnectToServer();
        }

        /// <summary>
        /// Export database schema to the specified directory
        /// </summary>
        /// <param name="outputDirectoryPath">Output directory path</param>
        /// <param name="databaseNamesAndOutputPaths">Dictionary where keys are database names and values will be updated to have the output directory path used</param>
        /// <returns>True if success, false if a problem</returns>
        /// <remarks>
        /// If CreatedDirectoryForEachDB is true, or if databaseNamesAndOutputPaths contains more than one entry,
        /// then each database will be scripted to a subdirectory below the output directory
        /// </remarks>
        public bool ExportSchema(string outputDirectoryPath, ref Dictionary<string, string> databaseNamesAndOutputPaths)
        {

            try
            {
                if (string.IsNullOrWhiteSpace(outputDirectoryPath))
                {
                    throw new ArgumentException("Output directory cannot be empty", nameof(outputDirectoryPath));
                }

                if (!Directory.Exists(outputDirectoryPath))
                {
                    //  Try to create the missing directory
                    OnStatusEvent("Creating " + outputDirectoryPath);
                    Directory.CreateDirectory(outputDirectoryPath);
                }


                mOptions.OutputDirectoryPath = outputDirectoryPath;

                if (databaseNamesAndOutputPaths.Count > 1)
                {
                    mOptions.CreateDirectoryForEachDB = true;
                }

            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ExportSchema configuring the options", ex);
                return false;
            }

            try
            {
                var dtStartTime = DateTime.UtcNow;

                var isValid = ValidateSchemaExporter();
                if (!isValid)
                    return false;

                if (mOptions.DisableAutoDataExport)
                {
                    mDBSchemaExporter.TableNamesToAutoSelect.Clear();
                    mDBSchemaExporter.TableNameAutoSelectRegEx.Clear();
                }
                else
                {
                    mDBSchemaExporter.StoreTableNamesToAutoSelect(GetTableNamesToAutoExportData(mOptions.PostgreSQL));
                    mDBSchemaExporter.StoreTableNameAutoSelectRegEx(GetTableRegExToAutoExportData());
                }

                var databaseList = databaseNamesAndOutputPaths.Keys.ToList();
                var tableNamesForDataExport = new List<string>();

                if (!string.IsNullOrWhiteSpace(mOptions.TableDataToExportFile))
                {
                    tableNamesForDataExport = LoadTableNamesForDataExport(mOptions.TableDataToExportFile);
                }

                var success = ScriptServerAndDBObjectsWork(databaseList, tableNamesForDataExport);

                //  Populate a dictionary with the database names (properly capitalized) and the output directory path used for each
                var databaseNameToDirectoryMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                foreach (var exportedDatabase in mDBSchemaExporter.SchemaOutputDirectories)
                {
                    databaseNameToDirectoryMap.Add(exportedDatabase.Key, exportedDatabase.Value);
                }

                //  Add any other databases in databaseList that are missing (as would be the case if it doesn't exist on the server)
                foreach (var databaseName in databaseList)
                {
                    if (!databaseNameToDirectoryMap.ContainsKey(databaseName))
                    {
                        databaseNameToDirectoryMap.Add(databaseName, string.Empty);
                    }

                }

                //  Now update databaseNamesAndOutputPaths to match databaseNameToDirectoryMap (which has properly capitalized database names)
                databaseNamesAndOutputPaths = databaseNameToDirectoryMap;
                if (mOptions.ShowStats)
                {
                    OnStatusEvent(string.Format(
                                      "Exported database schema in {0:0.0} seconds",
                                      DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds));
                }

                return success;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ExportSchema configuring dbSchemaExporter", ex);
                return false;
            }

        }

        /// <summary>
        /// Compare the contents of the two files using a line-by-line comparison
        /// </summary>
        /// <param name="baseFile">Base file</param>
        /// <param name="comparisonFile">Comparison file</param>
        /// <param name="differenceReason">Output parameter: reason for the difference, or eDifferenceReasonType.Unchanged if identical</param>
        /// <returns>True if the files differ (i.e. if they do not match)</returns>
        /// <remarks>
        /// Several files are treated specially to ignore changing dates or numbers, in particular:
        /// In DBDefinition files, the database size values are ignored
        /// In T_Process_Step_Control_Data files, in the Insert Into lines, any date values or text after a date value is ignored
        /// In T_Signatures_Data files, in the Insert Into lines, any date values are ignored
        /// </remarks>
        private bool FilesDiffer(FileInfo baseFile, FileInfo comparisonFile, out DifferenceReasonType differenceReason)
        {
            try
            {
                differenceReason = DifferenceReasonType.Unchanged;
                if (!baseFile.Exists)
                {
                    return false;
                }

                if (!comparisonFile.Exists)
                {
                    differenceReason = DifferenceReasonType.NewFile;
                    return true;
                }

                var dbDefinitionFile = false;
                var dateIgnoreFiles = new SortedSet<string>(StringComparer.OrdinalIgnoreCase)
                {
                    "T_Process_Step_Control_Data.sql",
                    "T_Signatures_Data.sql",
                    "T_MTS_Peptide_DBs_Data.sql",
                    "T_MTS_MT_DBs_Data.sql",
                    "T_Processor_Tool_Data.sql",
                    "T_Processor_Tool_Group_Details_Data.sql",
                };

                var ignoreInsertIntoDates = false;
                if (baseFile.Name.StartsWith(DBSchemaExporterSQLServer.DB_DEFINITION_FILE_PREFIX))
                {
                    // DB Definition file; don't worry if file lengths differ
                    dbDefinitionFile = true;
                }
                else if (dateIgnoreFiles.Contains(baseFile.Name))
                {
                    //  Files where date values are being ignored; don't worry if file lengths differ
                    OnStatusEvent("Ignoring date values in file " + baseFile.Name);
                    ignoreInsertIntoDates = true;
                }
                else if (baseFile.Length != comparisonFile.Length)
                {
                    differenceReason = DifferenceReasonType.Changed;
                    return true;
                }

                //  Perform a line-by-line comparison
                using (var baseFileReader = new StreamReader(new FileStream(baseFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                using (var comparisonFileReader =
                    new StreamReader(new FileStream(comparisonFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!baseFileReader.EndOfStream)
                    {
                        var dataLine = baseFileReader.ReadLine();
                        if (comparisonFileReader.EndOfStream) continue;

                        var comparisonLine = comparisonFileReader.ReadLine();
                        var linesMatch = StringMatch(dataLine, comparisonLine);
                        if (linesMatch)
                        {
                            continue;
                        }

                        if (dataLine == null && comparisonLine != null)
                        {
                            differenceReason = DifferenceReasonType.Changed;
                            return true;
                        }

                        if (dataLine != null && comparisonLine == null)
                        {
                            differenceReason = DifferenceReasonType.Changed;
                            return true;
                        }

                        if (dataLine == null)
                        {
                            continue;
                        }

                        if (dbDefinitionFile && dataLine.StartsWith("( NAME =") && comparisonLine.StartsWith("( NAME ="))
                        {
                            //  DBDefinition file
                            var lstSourceCols = dataLine.Split(',').ToList();
                            var lstComparisonCols = comparisonLine.Split(',').ToList();

                            if (lstSourceCols.Count == lstComparisonCols.Count)
                            {
                                linesMatch = true;
                                for (var dataColumnIndex = 0; dataColumnIndex < lstSourceCols.Count; dataColumnIndex++)
                                {
                                    var sourceValue = lstSourceCols[dataColumnIndex].Trim();
                                    var comparisonValue = lstComparisonCols[dataColumnIndex].Trim();
                                    if (sourceValue.StartsWith("SIZE") && comparisonValue.StartsWith("SIZE"))
                                    {
                                        //  Don't worry if these values differ
                                    }
                                    else if (!StringMatch(sourceValue, comparisonValue))
                                    {
                                        linesMatch = false;
                                        break;
                                    }

                                }

                            }

                        }

                        if (ignoreInsertIntoDates && dataLine.StartsWith("INSERT INTO ") && comparisonLine.StartsWith("INSERT INTO "))
                        {
                            //  Data file where we're ignoring dates
                            //  Truncate each of the data lines at the first occurrence of a date
                            var matchBaseFile = mDateMatcher.Match(dataLine);
                            var matchComparisonFile = mDateMatcher.Match(comparisonLine);
                            if (matchBaseFile.Success && matchComparisonFile.Success)
                            {
                                dataLine = dataLine.Substring(0, matchBaseFile.Index);
                                comparisonLine = comparisonLine.Substring(0, matchComparisonFile.Index);
                                linesMatch = StringMatch(dataLine, comparisonLine);
                            }

                        }

                        if (!linesMatch)
                        {
                            //  Difference found
                            differenceReason = DifferenceReasonType.Changed;
                            return true;
                        }

                    }
                }

                return false;

            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in FilesDiffer", ex);
                differenceReason = DifferenceReasonType.Changed;
                return true;
            }

        }

        /// <summary>
        /// Retrieve a list of tables in the given database
        /// </summary>
        /// <param name="databaseName">Database to query</param>
        /// <param name="includeTableRowCounts">When true, then determines the row count in each table</param>
        /// <param name="includeSystemObjects">When true, then also returns system var tables</param>
        /// <returns>Dictionary where keys are table names and values are row counts (if includeTableRowCounts = true)</returns>
        public Dictionary<string, long> GetDatabaseTableNames(string databaseName, bool includeTableRowCounts, bool includeSystemObjects)
        {
            return mDBSchemaExporter.GetDatabaseTableNames(databaseName, includeTableRowCounts, includeSystemObjects);
        }

        /// <summary>
        /// Retrieve a list of database names for the current server
        /// </summary>
        /// <returns></returns>
        public IEnumerable<string> GetServerDatabases()
        {
            return mDBSchemaExporter.GetServerDatabases();
        }

        public static SortedSet<string> GetTableNamesToAutoExportData(bool postgreSQLNames)
        {
            // Keys are table names
            // Values are the equivalent PostgreSQL name (empty strings for table names that will not get ported in the near future, or ever)
            var tableNames = new Dictionary<string, string>
            {
                // ReSharper disable StringLiteralTypo

                // MT_Main
                {"T_Folder_Paths", ""},

                // MT DBs
                {"T_Peak_Matching_Defaults", ""},
                {"T_Process_Config", ""},
                {"T_Process_Config_Parameters", ""},

                // MTS_Master
                {"T_Quantitation_Defaults", ""},
                {"T_MTS_DB_Types", ""},
                {"T_MTS_MT_DBs", ""},
                {"T_MTS_Peptide_DBs", ""},
                {"T_MTS_Servers", ""},
                {"T_MyEMSL_Cache_Paths", ""},

                // Peptide DB
                {"T_Dataset_Scan_Type_Name", ""},

                // Prism_IFC
                {"T_Match_Methods", ""},
                {"T_SP_Categories", ""},
                {"T_SP_Column_Direction_Types", ""},
                {"T_SP_Glossary", ""},
                {"T_SP_List", ""},

                // Prism_RPT
                {"T_Analysis_Job_Processor_Tools", ""},
                {"T_Analysis_Job_Processors", ""},
                {"T_Status", ""},

                // DMS5
                {"T_Dataset_Rating_Name", "t_dataset_rating_name"},
                {"T_Default_PSM_Job_Types", "t_default_psm_job_types"},
                {"T_Enzymes", "t_enzymes"},
                {"T_Instrument_Ops_Role", "t_instrument_ops_role"},
                {"T_MiscPaths", "t_misc_paths"},
                {"T_Modification_Types", "t_modification_types"},
                {"T_MyEMSLState", "t_myemsl_state"},
                {"T_Predefined_Analysis_Scheduling_Rules", "t_predefined_analysis_scheduling_rules"},
                {"T_Research_Team_Roles", "t_research_team_roles"},
                {"T_Residues", "t_residues"},
                {"T_User_Operations", "t_user_operations"},

                // Data_Package
                {"T_Properties", "t_properties"},
                {"T_URI_Paths", "t_uri_paths"},

                // Ontology_Lookup
                {"ontology", "ontology"},
                {"T_Unimod_AminoAcids", "t_unimod_amino_acids"},
                {"T_Unimod_Bricks", "t_unimod_bricks"},
                {"T_Unimod_Specificity_NL", "t_unimod_specificity_nl"},

                // DMS_Pipeline and DMS_Capture
                {"T_Automatic_Jobs", "t_automatic_jobs"},
                {"T_Default_SP_Params", "t_default_sp_params"},
                {"T_Processor_Instrument", "t_processor_instrument"},
                {"T_Processor_Tool", "t_processor_tool"},
                {"T_Processor_Tool_Group_Details", "t_processor_tool_group_details"},
                {"T_Processor_Tool_Groups", "t_processor_tool_groups"},
                {"T_Scripts", "t_scripts"},
                {"T_Scripts_History", "t_scripts_history"},
                {"T_Signatures", "t_signatures"},
                {"T_Step_Tools", "t_step_tools"},

                // Protein Sequences
                {"T_Annotation_Types", "t_annotation_types"},
                {"T_Archived_File_Types", "t_archived_file_types"},
                {"T_Creation_Option_Keywords", "t_creation_option_keywords"},
                {"T_Creation_Option_Values", "t_creation_option_values"},
                {"T_Naming_Authorities", "t_naming_authorities"},
                {"T_Output_Sequence_Types", "t_output_sequence_types"},
                {"T_Protein_Collection_Types", "t_protein_collection_types"},

                // dba
                {"AlertContacts", ""},
                {"AlertSettings", ""}

                // ReSharper restore StringLiteralTypo
            };

            var filteredNames = new SortedSet<string>();

            foreach (var item in tableNames)
            {
                AddToSortedSetIfNew(filteredNames, postgreSQLNames ? item.Value : item.Key);
            }

            return filteredNames;
        }

        public static SortedSet<string> GetTableRegExToAutoExportData()
        {
            var regExSpecs = new SortedSet<string>
            {
                ".*_?Type_?Name",
                ".*_?State_?Name",
                ".*_State",
                ".*_States"
            };

            return regExSpecs;
        }


        private List<string> LoadTableNamesForDataExport(string tableDataFilePath)
        {
            var tableNames = new SortedSet<string>();
            try
            {
                if (string.IsNullOrWhiteSpace(tableDataFilePath))
                {
                    return tableNames.ToList();
                }

                var dataFile = new FileInfo(tableDataFilePath);
                if (!dataFile.Exists)
                {
                    Console.WriteLine();
                    OnStatusEvent("Table Data File not found; default tables will be used");
                    OnWarningEvent("File not found: " + dataFile.FullName);
                    return tableNames.ToList();
                }

                using (var dataReader = new StreamReader(new FileStream(dataFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!dataReader.EndOfStream)
                    {
                        var dataLine = dataReader.ReadLine();
                        if (string.IsNullOrWhiteSpace(dataLine)) continue;

                        var trimmedName = dataLine.Trim();
                        if (!tableNames.Contains(trimmedName))
                        {
                            tableNames.Add(trimmedName);
                        }

                    }
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in LoadTableNamesForDataExport", ex);
            }

            return tableNames.ToList();
        }

        protected new void OnErrorEvent(string message)
        {
            base.OnErrorEvent(message);
            StatusMessage = message;
        }

        protected new void OnErrorEvent(string message, Exception ex)
        {
            base.OnErrorEvent(message, ex);
            if (ex != null && !message.Contains(ex.Message))
            {
                StatusMessage = message + ": " + ex.Message;
            }
            else
            {
                StatusMessage = message;
            }
        }

        protected new void OnWarningEvent(string message)
        {
            base.OnWarningEvent(message);
            StatusMessage = message;
        }
        protected new void OnStatusEvent(string message)
        {
            base.OnStatusEvent(message);
            StatusMessage = message;
        }

        private bool ParseGitStatus(FileSystemInfo targetDirectory, string consoleOutput, out int modifiedFileCount)
        {
            //  Example output for Git with verbose output
            //      # On branch master
            //      # Your branch is behind 'origin/master' by 1 commit, and can be fast-forwarded.
            //      #   (use "git pull" to update your local branch)
            //      #
            //     # Changes not staged for commit:
            //     #   (use "git add <file>..." to update what will be committed)
            //     #   (use "git checkout -- <file>..." to discard changes in working directory)
            //     #
            //     #       modified:   PNNLOmics/Algorithms/Alignment/LcmsWarp/LcmsWarp.cs
            //     #
            //     # Untracked files:
            //     #   (use "git add <file>..." to include in what will be committed)
            //     #
            //     #       MyNewFile.txt
            //     no changes added to commit (use "git add" and/or "git commit -a")
            //  Example output with Git for short output (-s)
            //   M PNNLOmics/Algorithms/Alignment/LcmsWarp/LcmsWarp.cs
            //  ?? MyNewFile.txt

            var newOrModifiedStatusSymbols = new List<char>
            {
                'M',
                'A',
                'R'
            };

            modifiedFileCount = 0;
            using (var gitStatusReader = new StringReader(consoleOutput))
            {
                while (gitStatusReader.Peek() > -1)
                {
                    var statusLine = gitStatusReader.ReadLine();
                    if (string.IsNullOrWhiteSpace(statusLine)  || statusLine.Length < 4)
                    {
                        continue;
                    }

                    if (statusLine.StartsWith("fatal: Not a git repository"))
                    {
                        OnErrorEvent("Directory is not tracked by Git: " + targetDirectory.FullName);
                        return false;
                    }

                    var fileIndexStatus = statusLine[0];
                    var fileWorkTreeStatus = statusLine[1];

                    if (fileIndexStatus == '?')
                    {
                        //  New file; added by the calling function
                    }
                    else if (newOrModifiedStatusSymbols.Contains(fileIndexStatus) || newOrModifiedStatusSymbols.Contains(fileWorkTreeStatus))
                    {
                        modifiedFileCount++;
                    }

                }
            }

            return true;
        }

        private bool ParseSvnHgStatus(FileSystemInfo targetDirectory, string consoleOutput, RepoManagerType repoManagerType, out int modifiedFileCount)
        {
            //  Example output for Svn where M is modified, ? is new, and ! means deleted
            //         M       F:\Documents\Projects\DataMining\Database_Schema\DMS\DMS5\UpdateAnalysisJobStateNameCached.sql
            //         ?       F:\Documents\Projects\DataMining\Database_Schema\DMS\DMS5\UpdateAnalysisJobToolNameCached.sql
            //         M       F:\Documents\Projects\DataMining\Database_Schema\DMS\DMS5\V_Analysis_Job_List_Report_2.sql
            //         M       F:\Documents\Projects\DataMining\Database_Schema\DMS\DMS5\V_GetPipelineJobParameters.sql
            //         !       F:\Documents\Projects\DataMining\Database_Schema\DMS\DMS5\x_V_Analysis_Job.sql
            //  Example output for Hg where M is modified, ? is new, and ! means deleted
            //         M F:\Documents\Projects\DataMining\Database_Schema\DMS\DMS5\UpdateAnalysisJobStateNameCached.sql
            //         ? F:\Documents\Projects\DataMining\Database_Schema\DMS\DMS5\UpdateAnalysisJobToolNameCached.sql
            //         M F:\Documents\Projects\DataMining\Database_Schema\DMS\DMS5\V_Analysis_Job_List_Report_2.sql
            //         M F:\Documents\Projects\DataMining\Database_Schema\DMS\DMS5\V_GetPipelineJobParameters.sql
            var newOrModifiedStatusSymbols = new List<char>
                {
                    'M',
                    'A',
                    'R'
                };

            int minimumLineLength;
            if (repoManagerType == RepoManagerType.Svn)
            {
                minimumLineLength = 8;
            }
            else
            {
                minimumLineLength = 3;
            }

            modifiedFileCount = 0;
            using (var hgStatusReader = new StringReader(consoleOutput))
            {
                while (hgStatusReader.Peek() > -1)
                {
                    var statusLine = hgStatusReader.ReadLine();
                    if (string.IsNullOrWhiteSpace(statusLine) || statusLine.Length < minimumLineLength)
                    {
                        continue;
                    }

                    if (statusLine.StartsWith("svn: warning") && statusLine.Contains("is not a working copy"))
                    {
                        OnErrorEvent("Directory is not tracked by SVN: " + targetDirectory.FullName);
                        return false;
                    }

                    if (statusLine.StartsWith("abort: no repository found in "))
                    {
                        OnErrorEvent("Directory is not tracked by Hg: " + targetDirectory.FullName);
                        return false;
                    }

                    var fileModStatus = statusLine[0];
                    var filePropertyStatus = ' ';

                    if (repoManagerType == RepoManagerType.Svn)
                    {
                        filePropertyStatus = statusLine[1];
                        //  var filePath = statusLine.Substring[8].Trim()
                    }
                    else
                    {
                        //  var filePath = statusLine.Substring[2].Trim()
                    }

                    if (newOrModifiedStatusSymbols.Contains(fileModStatus))
                    {
                        modifiedFileCount++;
                    }
                    else if (filePropertyStatus == 'M')
                    {
                        modifiedFileCount++;
                    }
                    else if (fileModStatus == '?')
                    {
                        //  New file; added by the calling function
                    }

                }
            }

            return true;
        }

        /// <summary>
        /// Export the schema for the databases defined in databasesToProcess
        /// </summary>
        /// <returns></returns>
        public bool ProcessDatabases(SchemaExportOptions options)
        {
            return ProcessDatabases(options.OutputDirectoryPath, options.ServerName, options.DatabasesToProcess);
        }

        /// <summary>
        /// Export the schema for the databases defined in databaseList
        /// </summary>
        /// <returns></returns>
        public bool ProcessDatabases(string outputDirectoryPath, string serverName, SortedSet<string> databaseList)
        {
            if (string.IsNullOrWhiteSpace(outputDirectoryPath))
            {
                throw new ArgumentException("Output directory path must be defined", nameof(outputDirectoryPath));
            }

            if (string.IsNullOrWhiteSpace(serverName))
            {
                throw new ArgumentException("Server name must be defined", nameof(serverName));
            }

            if (databaseList.Count == 0)
            {
                throw new ArgumentException("Database list cannot be empty", nameof(databaseList));
            }

            try
            {
                //  Keys in this dictionary are database names
                //  Values are the output directory path used (values will be defined by ExportSchema then used by SyncSchemaFiles)
                var databaseNamesAndOutputPaths = new Dictionary<string, string>();
                foreach (var databaseName in databaseList)
                {
                    databaseNamesAndOutputPaths.Add(databaseName, string.Empty);
                }

                var success = ExportSchema(outputDirectoryPath, ref databaseNamesAndOutputPaths);
                if (success && mOptions.Sync)
                {
                    success = SyncSchemaFiles(databaseNamesAndOutputPaths, mOptions.SyncDirectoryPath);
                }

                return success;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ProcessDatabases", ex);
                return false;
            }
        }

        /// <summary>
        /// Register the schema exporter's events
        /// </summary>
        /// <param name="schemaExporter"></param>
        protected void RegisterEvents(DBSchemaExporterBase schemaExporter)
        {
            base.RegisterEvents(schemaExporter);

            schemaExporter.DBExportStarting += DBExportStarting;
            schemaExporter.PauseStatusChange += PauseStatusChange;
            schemaExporter.ProgressComplete += ProgressComplete;
        }

        /// <summary>
        /// Request that scripting be paused
        /// </summary>
        /// <remarks>Useful when the scripting is running in another thread</remarks>
        public void RequestPause()
        {
            mDBSchemaExporter?.RequestPause();
        }

        /// <summary>
        /// Request that scripting be unpaused
        /// </summary>
        /// <remarks>Useful when the scripting is running in another thread</remarks>
        public void RequestUnpause()
        {
            mDBSchemaExporter?.RequestUnpause();
        }

        private bool RunCommand(
            string exePath,
            string cmdArgs,
            string workDirPath,
            out string consoleOutput,
            out string errorOutput,
            int maxRuntimeSeconds)
        {
            consoleOutput = string.Empty;
            errorOutput = string.Empty;
            try
            {
                var programRunner = new ProgRunner
                {
                    Arguments = cmdArgs,
                    CreateNoWindow = true,
                    MonitoringInterval = 100,
                    Name = Path.GetFileNameWithoutExtension(exePath),
                    Program = exePath,
                    Repeat = false,
                    RepeatHoldOffTime = 0,
                    WorkDir = workDirPath,
                    CacheStandardOutput = true,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = false,
                    ConsoleOutputFilePath = string.Empty
                };

                RegisterEvents(programRunner);
                var dtStartTime = DateTime.UtcNow;
                var dtLastStatus = DateTime.UtcNow;
                var executionAborted = false;
                programRunner.StartAndMonitorProgram();

                //  Wait for it to exit
                if (maxRuntimeSeconds < 10)
                {
                    maxRuntimeSeconds = 10;
                }

                //  Loop until program is complete, or until maxRuntimeSeconds seconds elapses
                while (programRunner.State != ProgRunner.States.NotMonitoring)
                {
                    System.Threading.Thread.Sleep(100);
                    var elapsedSeconds = DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds;
                    if (elapsedSeconds > maxRuntimeSeconds)
                    {
                        OnErrorEvent(string.Format("Program execution has surpassed {0} seconds; aborting {1}", maxRuntimeSeconds, exePath));

                        programRunner.StopMonitoringProgram(true);
                        executionAborted = true;
                    }

                    if (DateTime.UtcNow.Subtract(dtLastStatus).TotalSeconds > 15)
                    {
                        dtLastStatus = DateTime.UtcNow;
                        OnDebugEvent(string.Format("Waiting for {0}, {1:0} seconds elapsed",
                                                   Path.GetFileName(exePath), elapsedSeconds));
                    }

                }

                if (executionAborted)
                {
                    OnWarningEvent("ProgramRunner was aborted for " + exePath);
                    return true;
                }

                consoleOutput = programRunner.CachedConsoleOutput;
                errorOutput = programRunner.CachedConsoleError;
                return true;

            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in RunCommand", ex);
                return false;
            }

        }

        public bool ScriptServerAndDBObjects(List<string> databaseList, List<string> tableNamesForDataExport)
        {
            var isValid = ValidateSchemaExporter();
            if (!isValid)
                return false;

            var success = ScriptServerAndDBObjectsWork(databaseList, tableNamesForDataExport);
            return success;
        }

        private bool ScriptServerAndDBObjectsWork(List<string> databaseList, List<string> tableNamesForDataExport)
        {
            var success = mDBSchemaExporter.ScriptServerAndDBObjects(databaseList, tableNamesForDataExport);
            return success;
        }


        public void StoreTableNameAutoSelectRegEx(SortedSet<string> tableNameRegExSpecs)
        {
            mDBSchemaExporter.StoreTableNameAutoSelectRegEx(tableNameRegExSpecs);
        }

        public void StoreTableNamesToAutoSelect(SortedSet<string> tableNames)
        {
            mDBSchemaExporter.StoreTableNameAutoSelectRegEx(tableNames);
        }

        /// <summary>
        /// Compare two strings
        /// </summary>
        /// <param name="text1"></param>
        /// <param name="text2"></param>
        /// <returns>True if the strings match, otherwise false</returns>
        /// <remarks>Case sensitive comparison</remarks>
        private static bool StringMatch(string text1, string text2)
        {
            return text1.Equals(text2, StringComparison.Ordinal);
        }

        private bool SyncSchemaFiles(ICollection<KeyValuePair<string, string>> DatabaseNamesAndOutputPaths, string directoryPathForSync)
        {
            try
            {
                var dtStartTime = DateTime.UtcNow;
                OnProgressUpdate("Synchronizing with " + directoryPathForSync, 0);

                var intDBsProcessed = 0;
                var includeDbNameInCommitMessage = DatabaseNamesAndOutputPaths.Count > 1;
                foreach (var dbEntry in DatabaseNamesAndOutputPaths)
                {
                    var databaseName = dbEntry.Key;
                    var schemaOutputDirectory = dbEntry.Value;
                    if (string.IsNullOrWhiteSpace(schemaOutputDirectory))
                    {
                        OnErrorEvent("Schema output directory was not reported for " + databaseName + "; unable to synchronize");
                        continue;
                    }

                    var percentComplete = intDBsProcessed / ((float)DatabaseNamesAndOutputPaths.Count * 100);
                    OnProgressUpdate("Synchronizing database " + databaseName, percentComplete);

                    var diSourceDirectory = new DirectoryInfo(schemaOutputDirectory);
                    var targetDirectoryPath = string.Copy(directoryPathForSync);
                    if (DatabaseNamesAndOutputPaths.Count > 1 || mOptions.CreateDirectoryForEachDB)
                    {
                        targetDirectoryPath = Path.Combine(targetDirectoryPath, databaseName);
                    }

                    var targetDirectory = new DirectoryInfo(targetDirectoryPath);
                    if (!diSourceDirectory.Exists)
                    {
                        OnErrorEvent("Source directory not found; cannot synchronize: " + diSourceDirectory.FullName);
                        return false;
                    }

                    if (!targetDirectory.Exists)
                    {
                        OnStatusEvent("Creating target directory for synchronization: " + targetDirectory.FullName);
                        targetDirectory.Create();
                    }

                    if (diSourceDirectory.FullName == targetDirectory.FullName)
                    {
                        OnErrorEvent("Sync directory is identical to the output SchemaFileDirectory; cannot synchronize");
                        return false;
                    }

                    // ReSharper disable once NotAccessedVariable
                    var fileProcessCount = 0;
                    var fileCopyCount = 0;

                    //  This list holds the the files that are copied from diSourceDirectory to targetDirectory
                    var newFilePaths = new List<string>();
                    var filesToCopy = diSourceDirectory.GetFiles().ToList();

                    foreach (var sourceFile in filesToCopy)
                    {
                        if (sourceFile.Name.StartsWith("x_", StringComparison.OrdinalIgnoreCase) ||
                            sourceFile.Name.StartsWith("t_tmp_", StringComparison.OrdinalIgnoreCase) ||
                            sourceFile.Name.StartsWith("t_CandidateModsSeqWork_", StringComparison.OrdinalIgnoreCase) ||
                            sourceFile.Name.StartsWith("t_CandidateSeqWork_", StringComparison.OrdinalIgnoreCase))
                        {
                            OnStatusEvent("Skipping "
                                            + databaseName + " var " + sourceFile.Name);
                            continue;
                        }

                        var fiTargetFile = new FileInfo(Path.Combine(targetDirectory.FullName, sourceFile.Name));

                        if (FilesDiffer(sourceFile, fiTargetFile, out var differenceReason))
                        {
                            // var subtaskPercentComplete = fileProcessCount / ((float)filesToCopy.Count * 100);

                            switch (differenceReason)
                            {
                                case DifferenceReasonType.NewFile:
                                    OnDebugEvent("  Copying new file " + sourceFile.Name);
                                    newFilePaths.Add(fiTargetFile.FullName);
                                    break;
                                case DifferenceReasonType.Changed:
                                    OnDebugEvent("  Copying changed file " + sourceFile.Name);
                                    break;
                                default:
                                    OnDebugEvent("  Copying file " + sourceFile.Name);
                                    break;
                            }
                            sourceFile.CopyTo(fiTargetFile.FullName, true);
                            fileCopyCount++;
                        }

                        fileProcessCount++;
                    }

                    var commitMessageAppend = string.Empty;
                    if (includeDbNameInCommitMessage)
                    {
                        commitMessageAppend = databaseName;
                    }

                    if (mOptions.SvnUpdate)
                    {
                        UpdateRepoChanges(targetDirectory, fileCopyCount, newFilePaths, RepoManagerType.Svn, commitMessageAppend);
                    }

                    if (mOptions.HgUpdate)
                    {
                        UpdateRepoChanges(targetDirectory, fileCopyCount, newFilePaths, RepoManagerType.Hg, commitMessageAppend);
                    }

                    if (mOptions.GitUpdate)
                    {
                        UpdateRepoChanges(targetDirectory, fileCopyCount, newFilePaths, RepoManagerType.Git, commitMessageAppend);
                    }

                    intDBsProcessed++;
                }

                if (mOptions.ShowStats)
                {
                    OnDebugEvent(string.Format(
                                     "Synchronized schema files in {0:0.0} seconds", DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds));
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in SyncSchemaFiles", ex);
                return false;
            }

        }

        /// <summary>
        /// Pause / unpause the scripting
        /// </summary>
        /// <remarks>Useful when the scripting is running in another thread</remarks>
        public void TogglePause()
        {
            mDBSchemaExporter?.TogglePause();
        }

        private bool UpdateRepoChanges(
            FileSystemInfo diTargetDirectory,
            int fileCopyCount, ICollection<string> newFilePaths,
            RepoManagerType repoManagerType,
            string commitMessageAppend)
        {
            const string SVN_EXE_PATH = @"C:\Program Files\TortoiseSVN\bin\svn.exe";
            const string SVN_SOURCE = "Installed with 64-bit Tortoise SVN, available at https://tortoisesvn.net/downloads.html";

            const string HG_EXE_PATH = @"C:\Program Files\TortoiseHg\hg.exe";
            const string HG_SOURCE = "Installed with 64-bit Tortoise Hg, available at https://tortoisehg.bitbucket.io/download/index.html";

            const string GIT_EXE_PATH = @"C:\Program Files\Git\bin\git.exe";
            const string GIT_SOURCE = "Installed with 64-bit Git for Windows, available at https://git-scm.com/download/win";

            var toolName = "Unknown";
            try
            {
                FileInfo repoExe;
                string repoSource;

                switch (repoManagerType)
                {
                    case RepoManagerType.Svn:
                        repoExe = new FileInfo(SVN_EXE_PATH);
                        repoSource = SVN_SOURCE;
                        toolName = "SVN";
                        break;
                    case RepoManagerType.Hg:
                        repoExe = new FileInfo(HG_EXE_PATH);
                        repoSource = HG_SOURCE;
                        toolName = "Hg";
                        break;
                    case RepoManagerType.Git:
                        repoExe = new FileInfo(GIT_EXE_PATH);
                        repoSource = GIT_SOURCE;
                        toolName = "Git";
                        break;
                    default:
                        OnErrorEvent("Unsupported RepoManager type: " + repoManagerType);
                        return false;
                }

                if (!repoExe.Exists)
                {
                    OnErrorEvent("Repo exe not found at " + repoExe.FullName);
                    OnStatusEvent(repoSource);
                    return false;
                }

                string cmdArgs;
                int maxRuntimeSeconds;

                bool success;
                Console.WriteLine();
                if (newFilePaths.Count > 0)
                {
                    OnStatusEvent(string.Format("Adding {0} new {1} for tracking by {2}",
                                                newFilePaths.Count,
                                                CheckPlural(newFilePaths.Count, "file", "files"),
                                                toolName));

                    //  Add each of the new files
                    foreach (var newFilePath in newFilePaths)
                    {
                        var fileToAdd = new FileInfo(newFilePath);
                        if (fileToAdd.Directory == null)
                        {
                            OnErrorEvent(string.Format("Cannot determine the parent directory of {0}; skipping", fileToAdd.FullName));
                            continue;
                        }

                        cmdArgs = string.Format(" add \"{0}\"", fileToAdd.FullName);
                        maxRuntimeSeconds = 30;

                        success = RunCommand(repoExe.FullName, cmdArgs, fileToAdd.Directory.FullName,
                                             out var addConsoleOutput, out var addErrorOutput, maxRuntimeSeconds);

                        if (!success)
                        {
                            OnWarningEvent(string.Format("Error reported for {0}: {1}", toolName, addConsoleOutput));
                            return false;
                        }

                        if (repoManagerType == RepoManagerType.Git && addErrorOutput.StartsWith("fatal", StringComparison.OrdinalIgnoreCase))
                        {
                            OnWarningEvent(string.Format("Error reported for {0}: {1}", toolName, addErrorOutput));
                        }

                    }

                    Console.WriteLine();
                }

                OnStatusEvent(string.Format("Looking for modified files tracked by {0} at {1}", toolName, diTargetDirectory.FullName));

                //  Count the number of new or modified files
                cmdArgs = string.Format(" status \"{0}\"", diTargetDirectory.FullName);
                maxRuntimeSeconds = 300;

                if (repoManagerType == RepoManagerType.Git)
                {
                    cmdArgs = " status -s -u";
                }

                success = RunCommand(repoExe.FullName, cmdArgs, diTargetDirectory.FullName,
                                     out var statusConsoleOutput, out var statusErrorOutput, maxRuntimeSeconds);
                if (!success)
                {
                    return false;
                }

                if (repoManagerType == RepoManagerType.Git && statusErrorOutput.StartsWith("fatal", StringComparison.OrdinalIgnoreCase))
                {
                    OnWarningEvent(string.Format("Error reported for {0}: {1}", toolName, statusErrorOutput));
                }

                Console.WriteLine();
                var modifiedFileCount = 0;
                if (repoManagerType == RepoManagerType.Svn || repoManagerType == RepoManagerType.Hg)
                {
                    success = ParseSvnHgStatus(diTargetDirectory, statusConsoleOutput, repoManagerType, out modifiedFileCount);
                }
                else
                {
                    //  Git
                    success = ParseGitStatus(diTargetDirectory, statusConsoleOutput, out modifiedFileCount);
                }

                if (!success)
                {
                    return false;
                }

                if (fileCopyCount > 0 && modifiedFileCount == 0)
                {
                    OnWarningEvent(string.Format
                                       ("Note: File Copy Count is {0} yet the Modified File Count reported by {1} is zero; " +
                                        "this may indicate a problem", fileCopyCount, toolName));

                    Console.WriteLine();
                }

                if (modifiedFileCount > 0 || newFilePaths.Count > 0)
                {
                    if (modifiedFileCount > 0)
                    {
                        OnStatusEvent(string.Format("Found {0} modified {1}",
                                                    modifiedFileCount,
                                                    CheckPlural(modifiedFileCount, "file", "files")));
                    }

                    var commitMessage = string.Format("{0:yyyy-MM-dd} auto-commit", DateTime.Now);

                    if (!string.IsNullOrWhiteSpace(commitMessageAppend))
                    {
                        if (commitMessageAppend.StartsWith(" "))
                        {
                            commitMessage += commitMessageAppend;
                        }
                        else
                        {
                            commitMessage += " " + commitMessageAppend;
                        }

                    }

                    if (!mOptions.CommitUpdates)
                    {
                        OnStatusEvent("Use /Commit to commit changes with commit message: " + commitMessage);
                    }
                    else
                    {
                        OnStatusEvent(string.Format("Commiting changes to {0}: {1}", toolName, commitMessage));

                        //  Commit the changes
                        cmdArgs = string.Format(" commit \"{0}\" --message \"{1}\"", diTargetDirectory.FullName, commitMessage);
                        maxRuntimeSeconds = 120;

                        success = RunCommand(repoExe.FullName, cmdArgs, diTargetDirectory.FullName,
                                             out var commitConsoleOutput, out var commitErrorOutput, maxRuntimeSeconds);

                        if (!success)
                        {
                            OnErrorEvent(string.Format("Commit error:\n{0}", commitConsoleOutput));
                            return false;
                        }

                        if (repoManagerType == RepoManagerType.Git && commitErrorOutput.StartsWith("fatal", StringComparison.OrdinalIgnoreCase))
                        {
                            OnWarningEvent(string.Format("Error reported for {0}: {1}", toolName, commitErrorOutput));
                            return false;
                        }

                        if (repoManagerType == RepoManagerType.Svn && commitErrorOutput.IndexOf("Commit failed", StringComparison.OrdinalIgnoreCase) >= 0)
                        {
                            OnWarningEvent(string.Format("Error reported for {0}: {1}", toolName, commitErrorOutput));
                            return false;
                        }

                        if (repoManagerType == RepoManagerType.Hg || repoManagerType == RepoManagerType.Git)
                        {
                            for (var iteration = 1; iteration <= 2; iteration++)
                            {
                                if (repoManagerType == RepoManagerType.Hg)
                                {
                                    //  Push the changes
                                    cmdArgs = " push";
                                }
                                else
                                {
                                    //  Push the changes to the master, both on origin and GitHub
                                    if (iteration == 1)
                                    {
                                        cmdArgs = " push origin";
                                    }
                                    else
                                    {
                                        cmdArgs = " push GitHub";
                                    }

                                }

                                maxRuntimeSeconds = 300;
                                success = RunCommand(repoExe.FullName, cmdArgs, diTargetDirectory.FullName,
                                                     out var pushConsoleOutput, out _, maxRuntimeSeconds);

                                if (!success)
                                {
                                    OnErrorEvent(string.Format("Push error:\n{0}", pushConsoleOutput));
                                    return false;
                                }

                                if (repoManagerType == RepoManagerType.Hg)
                                {
                                    break;
                                }

                            }

                        }

                        Console.WriteLine();
                    }

                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in UpdateRepoChanges for tool " + toolName, ex);
                return false;
            }

        }

        private bool ValidateSchemaExporter()
        {
            StatusMessage = string.Empty;

            try
            {
                if (mOptions.PostgreSQL)
                {
                    if (!(mDBSchemaExporter is DBSchemaExporterPostgreSQL))
                    {
                        mDBSchemaExporter = new DBSchemaExporterPostgreSQL(mOptions);
                        RegisterEvents(mDBSchemaExporter);
                    }
                }
                else
                {
                    if (!(mDBSchemaExporter is DBSchemaExporterSQLServer))
                    {
                        mDBSchemaExporter = new DBSchemaExporterSQLServer(mOptions);
                        RegisterEvents(mDBSchemaExporter);
                    }
                }
                return true;
            }
            catch (Exception ex)
            {
                if (string.IsNullOrWhiteSpace(StatusMessage))
                {
                    OnErrorEvent("Error in ValidateSchemaExporter", ex);
                }
                return false;
            }
        }

    }
}