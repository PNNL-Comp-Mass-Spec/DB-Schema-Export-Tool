using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using PRISM;
using PRISM.Logging;

// ReSharper disable UnusedMember.Global

namespace DB_Schema_Export_Tool
{
    /// <summary>
    /// Database schema export tool
    /// </summary>
    public class DBSchemaExportTool : LoggerBase
    {
        // ReSharper disable CommentTypo

        // Ignore Spelling: dba, lcms, myemsl, PostgreSQL, psm, Quantitation, Repo, Svn, tmp, unimod, unpause, unpaused, uri

        // ReSharper restore CommentTypo

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

        private readonly Regex mDateMatcher;

        private DBSchemaExporterBase mDBSchemaExporter;

        private readonly SchemaExportOptions mOptions;

        private readonly Regex mVersionExtractor;

        /// <summary>
        /// Error code
        /// </summary>
        public DBSchemaExporterBase.DBSchemaExportErrorCodes ErrorCode => mDBSchemaExporter?.ErrorCode ?? DBSchemaExporterBase.DBSchemaExportErrorCodes.NoError;

        /// <summary>
        /// Pause status
        /// </summary>
        public DBSchemaExporterBase.PauseStatusConstants PauseStatus => mDBSchemaExporter?.PauseStatus ?? DBSchemaExporterBase.PauseStatusConstants.Unpaused;

        /// <summary>
        /// Most recent Status, Warning, or Error message
        /// </summary>
        public string StatusMessage { get; private set; }

        /// <summary>
        /// Database export starting event
        /// </summary>
        public event DBSchemaExporterBase.DBExportStartingHandler DBExportStarting;

        /// <summary>
        /// Pause status changed event
        /// </summary>
        public event DBSchemaExporterBase.PauseStatusChangeHandler PauseStatusChange;

        /// <summary>
        /// Processing complete event
        /// </summary>
        public event DBSchemaExporterBase.ProgressCompleteHandler ProgressComplete;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="options">Options</param>
        public DBSchemaExportTool(SchemaExportOptions options) : base(options)
        {
            mOptions = options;

            mDateMatcher = new Regex(@"'\d+/\d+/\d+ \d+:\d+:\d+ [AP]M'", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            mVersionExtractor = new Regex(@"version (?<Major>\d+)\.(?<Minor>\d+)");

            if (mOptions.PostgreSQL)
            {
                mDBSchemaExporter = new DBSchemaExporterPostgreSQL(mOptions);
            }
            else
            {
                mDBSchemaExporter = new DBSchemaExporterSQLServer(mOptions);
            }

            RegisterEvents(mDBSchemaExporter);

            InitializeLogFile(options);
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
            // filteredNames is a set, and thus we can call .Add() even if it already has the value

            if (!string.IsNullOrWhiteSpace(value))
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
        /// <remarks>
        /// If CreatedDirectoryForEachDB is true, or if databaseNamesAndOutputPaths contains more than one entry,
        /// then each database will be scripted to a subdirectory below the output directory
        /// </remarks>
        /// <param name="outputDirectoryPath">Output directory path</param>
        /// <param name="databaseNamesAndOutputPaths">
        /// Dictionary where keys are database names and values will be updated to have the output directory path used
        /// </param>
        /// <returns>True if success, false if a problem</returns>
        /// <exception cref="ArgumentException"></exception>
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
                    // Try to create the missing directory
                    OnStatusEvent("Creating output directory: " + outputDirectoryPath);
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
                var startTime = DateTime.UtcNow;

                var isValid = ValidateSchemaExporter();

                if (!isValid)
                    return false;

                if (mOptions.DisableAutoDataExport)
                {
                    ShowTrace("Auto selection of tables for data export is disabled");
                    mDBSchemaExporter.TableNamesToAutoExportData.Clear();
                    mDBSchemaExporter.TableNameRegexToAutoExportData.Clear();
                }
                else
                {
                    mDBSchemaExporter.StoreTableNamesToAutoExportData(GetTableNamesToAutoExportData(mOptions.PostgreSQL));
                    mDBSchemaExporter.StoreTableNameRegexToAutoExportData(GetTableRegExToAutoExportData());
                }

                var databaseList = databaseNamesAndOutputPaths.Keys.ToList();
                List<TableDataExportInfo> tablesForDataExport;

                if (string.IsNullOrWhiteSpace(mOptions.TableDataToExportFile))
                {
                    tablesForDataExport = new List<TableDataExportInfo>();
                }
                else
                {
                    tablesForDataExport = LoadTablesForDataExport(mOptions.TableDataToExportFile, out var abortProcessing);

                    if (abortProcessing)
                        return false;
                }

                // Append any tables defined in TableNameFilterSet
                foreach (var item in mOptions.TableNameFilterSet)
                {
                    // Skip this table if LoadTablesForDataExport already added it to tablesForDataExport
                    var skipTable = tablesForDataExport.Any(existingItem => existingItem.SourceTableName.Equals(item, StringComparison.OrdinalIgnoreCase));

                    if (skipTable)
                        continue;

                    var sourceTable = new TableDataExportInfo(item)
                    {
                        UsePgInsert = mOptions.PgInsertTableData
                    };

                    tablesForDataExport.Add(sourceTable);
                }

                List<string> tableDataExportOrder;

                if (string.IsNullOrWhiteSpace(mOptions.TableDataExportOrderFile))
                {
                    tableDataExportOrder = new List<string>();
                }
                else
                {
                    tableDataExportOrder = LoadTableDataExportOrderFile(mOptions.TableDataExportOrderFile, out var abortProcessing);

                    if (abortProcessing)
                        return false;
                }

                if (!string.IsNullOrWhiteSpace(mOptions.TableDataColumnMapFile))
                {
                    // This method updates mOptions.ColumnMapForDataExport
                    LoadColumnMapInfo(mOptions.TableDataColumnMapFile);
                }

                if (!string.IsNullOrWhiteSpace(mOptions.ExistingSchemaFileToParse))
                {
                    var schemaUpdater = new DBSchemaUpdater(mOptions);
                    RegisterEvents(schemaUpdater);

                    var successUpdatingColumnNames = schemaUpdater.UpdateColumnNamesInExistingSchemaFile(
                        mOptions.ExistingSchemaFileToParse, mOptions, tablesForDataExport, out var updatedSchemaFilePath);

                    if (!successUpdatingColumnNames)
                        return false;

                    var renamedTablesAndViews = new Dictionary<string, string>();

                    // ReSharper disable once ForeachCanBeConvertedToQueryUsingAnotherGetEnumerator
                    foreach (var item in tablesForDataExport)
                    {
                        if (!string.IsNullOrWhiteSpace(item.TargetTableName) &&
                            !item.SourceTableName.Equals(item.TargetTableName, StringComparison.Ordinal))
                        {
                            renamedTablesAndViews.Add(item.SourceTableName, item.TargetTableName);
                        }
                    }

                    if (renamedTablesAndViews.Count > 0)
                    {
                        // Need to rename one or more tables and views
                        var successUpdatingTableNames = schemaUpdater.UpdateTableAndViewNamesInExistingSchemaFile(updatedSchemaFilePath, renamedTablesAndViews);

                        if (!successUpdatingTableNames)
                            return false;
                    }
                }

                if (!string.IsNullOrWhiteSpace(mOptions.TableDataColumnFilterFile))
                {
                    var columnFilterSuccess = LoadColumnFiltersForTableData(mOptions.TableDataColumnFilterFile);

                    if (!columnFilterSuccess)
                        return false;
                }

                if (!string.IsNullOrWhiteSpace(mOptions.TableDataDateFilterFile))
                {
                    // This method updates mOptions.ColumnMapForDataExport
                    var dateFilterSuccess = LoadDateFiltersForTableData(mOptions.TableDataDateFilterFile, tablesForDataExport);

                    if (!dateFilterSuccess)
                        return false;
                }

                var success = ScriptServerAndDBObjectsWork(databaseList, tablesForDataExport, tableDataExportOrder);

                // Populate a dictionary with the database names (properly capitalized) and the output directory path used for each
                var databaseNameToDirectoryMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

                foreach (var exportedDatabase in mDBSchemaExporter.SchemaOutputDirectories)
                {
                    databaseNameToDirectoryMap.Add(exportedDatabase.Key, exportedDatabase.Value);
                }

                // Add any other databases in databaseList that are missing (as would be the case if it doesn't exist on the server)
                foreach (var databaseName in databaseList)
                {
                    if (!databaseNameToDirectoryMap.ContainsKey(databaseName))
                    {
                        databaseNameToDirectoryMap.Add(databaseName, string.Empty);
                    }
                }

                // Now update databaseNamesAndOutputPaths to match databaseNameToDirectoryMap (which has properly capitalized database names)
                databaseNamesAndOutputPaths = databaseNameToDirectoryMap;

                if (mOptions.ShowStats)
                {
                    OnStatusEvent("Exported database schema in {0:0.0} seconds", DateTime.UtcNow.Subtract(startTime).TotalSeconds);
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
        /// <remarks>
        /// Several files are treated specially to ignore changing dates or numbers, in particular:
        /// In DBDefinition files, the database size values are ignored
        /// In T_Process_Step_Control_Data files, in the Insert Into lines, any date values or text after a date value is ignored
        /// In T_Signatures_Data files, in the Insert Into lines, any date values are ignored
        /// In PostgreSQL database dump files, the database version and pg_dump versions are ignored if they are a minor version difference
        /// </remarks>
        /// <param name="baseFile">Base file</param>
        /// <param name="comparisonFile">Comparison file</param>
        /// <param name="differenceReason">Output parameter: reason for the difference, or DifferenceReasonType.Unchanged if identical</param>
        /// <returns>True if the files differ (i.e. if they do not match)</returns>
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
                    // Files where date values are being ignored; don't worry if file lengths differ
                    OnStatusEvent("Ignoring date values in file " + baseFile.Name);
                    ignoreInsertIntoDates = true;
                }
                else if (baseFile.Length != comparisonFile.Length)
                {
                    differenceReason = DifferenceReasonType.Changed;
                    return true;
                }

                // Perform a line-by-line comparison
                using var baseFileReader = new StreamReader(new FileStream(baseFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));
                using var comparisonFileReader = new StreamReader(new FileStream(comparisonFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

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

                    if (dataLine.StartsWith(@"\restrict") && comparisonLine.StartsWith(@"\restrict") ||
                        dataLine.StartsWith(@"\unrestrict") && comparisonLine.StartsWith(@"\unrestrict"))
                    {
                        // Ignore lines that start with \restrict or \unrestrict
                        continue;
                    }

                    if (dbDefinitionFile && dataLine.StartsWith("( NAME =") && comparisonLine.StartsWith("( NAME ="))
                    {
                        // DBDefinition file, example line:
                        // NAME = N'Protein_Sequences_Data', FILENAME = N'J:\SQLServerData\Protein_Sequences.mdf' , SIZE = 174425088KB , MAXSIZE = UNLIMITED

                        // Split on commas
                        var sourceCols = dataLine.Split(',').ToList();
                        var comparisonCols = comparisonLine.Split(',').ToList();

                        if (sourceCols.Count == comparisonCols.Count)
                        {
                            linesMatch = true;

                            for (var dataColumnIndex = 0; dataColumnIndex < sourceCols.Count; dataColumnIndex++)
                            {
                                var sourceValue = sourceCols[dataColumnIndex].Trim();
                                var comparisonValue = comparisonCols[dataColumnIndex].Trim();

                                if (sourceValue.StartsWith("SIZE") && comparisonValue.StartsWith("SIZE"))
                                {
                                    // Example: SIZE = 186294784KB  vs.  SIZE = 174425088KB
                                    // Don't worry if these values differ
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
                        // Data file where we're ignoring dates
                        // Truncate each of the data lines at the first occurrence of a date
                        var matchBaseFile = mDateMatcher.Match(dataLine);
                        var matchComparisonFile = mDateMatcher.Match(comparisonLine);

                        if (matchBaseFile.Success && matchComparisonFile.Success)
                        {
                            dataLine = dataLine.Substring(0, matchBaseFile.Index);
                            comparisonLine = comparisonLine.Substring(0, matchComparisonFile.Index);
                            linesMatch = StringMatch(dataLine, comparisonLine);
                        }
                    }

                    if (dataLine.StartsWith("-- Dumped from database version") && comparisonLine.StartsWith("-- Dumped from") ||
                        dataLine.StartsWith("-- Dumped by pg_dump version") && comparisonLine.StartsWith("-- Dumped by"))
                    {
                        if (MajorVersionsMatch(dataLine, comparisonLine))
                            continue;
                    }

                    if (!linesMatch)
                    {
                        // Difference found
                        differenceReason = DifferenceReasonType.Changed;
                        return true;
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
        /// <param name="includeTableRowCounts">When true, determines the row count in each table</param>
        /// <param name="includeSystemObjects">When true, also returns system tables</param>
        /// <returns>Dictionary where keys are table names and values are row counts (if includeTableRowCounts = true)</returns>
        public Dictionary<TableDataExportInfo, long> GetDatabaseTables(string databaseName, bool includeTableRowCounts, bool includeSystemObjects)
        {
            return mDBSchemaExporter.GetDatabaseTables(databaseName, includeTableRowCounts, includeSystemObjects);
        }

        /// <summary>
        /// Retrieve a list of database names for the current server
        /// </summary>
        public IEnumerable<string> GetServerDatabases()
        {
            return mDBSchemaExporter.GetServerDatabases();
        }

        /// <summary>
        /// Look for the table named sourceTableName in tablesForDataExport
        /// </summary>
        /// <param name="tablesForDataExport">Tables to export data from</param>
        /// <param name="sourceTableName">Source table name</param>
        /// <param name="tableInfo">Output: tableInfo for the named table, or null if not found</param>
        /// <returns>True if found, otherwise false</returns>
        public static bool GetTableByName(IEnumerable<TableDataExportInfo> tablesForDataExport, string sourceTableName, out TableDataExportInfo tableInfo)
        {
            foreach (var candidateTable in tablesForDataExport)
            {
                if (!candidateTable.SourceTableName.Equals(sourceTableName, StringComparison.OrdinalIgnoreCase))
                    continue;

                tableInfo = candidateTable;
                return true;
            }

            tableInfo = null;
            return false;
        }

#pragma warning disable VSSpell001 // Spell Check

        /// <summary>
        /// Get a list of table names to auto-export data
        /// </summary>
        /// <param name="isPostgreSQL">When true, return PostgreSQL table names</param>
        public static SortedSet<string> GetTableNamesToAutoExportData(bool isPostgreSQL)
#pragma warning restore VSSpell001 // Spell Check
        {
            // Keys are table names
            // Values are the equivalent PostgreSQL name (empty strings for table names that will not get ported in the near future, or ever)
            var tableNames = new Dictionary<string, string>
            {
                // ReSharper disable StringLiteralTypo

                // MT_Main
                {"T_Folder_Paths", string.Empty},

                // MT DBs
                {"T_Peak_Matching_Defaults", string.Empty},
                {"T_Process_Config", string.Empty},
                {"T_Process_Config_Parameters", string.Empty},

                // MTS_Master
                {"T_Quantitation_Defaults", string.Empty},
                {"T_MTS_DB_Types", string.Empty},
                {"T_MTS_MT_DBs", string.Empty},
                {"T_MTS_Peptide_DBs", string.Empty},
                {"T_MTS_Servers", string.Empty},
                {"T_MyEMSL_Cache_Paths", string.Empty},

                // Peptide DB
                {"T_Dataset_Scan_Type_Name", string.Empty},

                // Prism_IFC
                {"T_Match_Methods", string.Empty},
                {"T_SP_Categories", string.Empty},
                {"T_SP_Column_Direction_Types", string.Empty},
                {"T_SP_Glossary", string.Empty},
                {"T_SP_List", string.Empty},

                // Prism_RPT
                {"T_Analysis_Job_Processor_Tools", string.Empty},
                {"T_Analysis_Job_Processors", string.Empty},
                {"T_Status", string.Empty},

                // DMS5
                {"T_Dataset_Rating_Name", "public.t_dataset_rating_name"},
                {"T_Default_PSM_Job_Types", "public.t_default_psm_job_types"},
                {"T_Enzymes", "public.t_enzymes"},
                {"T_Instrument_Ops_Role", "public.t_instrument_ops_role"},
                {"T_MiscPaths", "public.t_misc_paths"},
                {"T_Modification_Types", "public.t_modification_types"},
                {"T_MyEMSLState", "public.t_myemsl_state"},
                {"T_Predefined_Analysis_Scheduling_Rules", "public.t_predefined_analysis_scheduling_rules"},
                {"T_Research_Team_Roles", "public.t_research_team_roles"},
                {"T_Residues", "public.t_residues"},
                {"T_User_Operations", "public.t_user_operations"},

                // Data_Package
                {"T_Properties", "dpkg.t_properties"},
                {"T_URI_Paths", "public.t_uri_paths"},

                // Ontology_Lookup
                {"T_Unimod_AminoAcids", "ont.t_unimod_amino_acids"},
                {"T_Unimod_Bricks", "ont.t_unimod_bricks"},
                {"T_Unimod_Specificity_NL", "ont.t_unimod_specificity_nl"},

                // DMS_Pipeline and DMS_Capture
                {"T_Automatic_Jobs", "cap.t_automatic_jobs"},
                {"T_Default_SP_Params", "cap.t_default_sp_params"},
                {"T_Processor_Instrument", "cap.t_processor_instrument"},
                {"T_Processor_Tool", "cap.t_processor_tool"},
                {"T_Processor_Tool_Group_Details", "cap.t_processor_tool_group_details"},
                {"T_Processor_Tool_Groups", "cap.t_processor_tool_groups"},
                {"T_Scripts", "cap.t_scripts"},
                {"T_Scripts_History", "cap.t_scripts_history"},
                {"T_Signatures", "cap.t_signatures"},
                {"T_Step_Tools", "cap.t_step_tools"},

                // Protein Sequences
                {"T_Annotation_Types", "pc.t_annotation_types"},
                {"T_Archived_File_Types", "pc.t_archived_file_types"},
                {"T_Creation_Option_Keywords", "pc.t_creation_option_keywords"},
                {"T_Creation_Option_Values", "pc.t_creation_option_values"},
                {"T_Naming_Authorities", "pc.t_naming_authorities"},
                {"T_Output_Sequence_Types", "pc.t_output_sequence_types"},
                {"T_Protein_Collection_Types", "pc.t_protein_collection_types"},

                // dba
                {"AlertContacts", string.Empty},
                {"AlertSettings", string.Empty},

                // pg_timetable
                {"timetable.chain", "timetable.chain"},
                {"timetable.task", "timetable.task"}

                // ReSharper restore StringLiteralTypo
            };

            var filteredNames = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var item in tableNames)
            {
                AddToSortedSetIfNew(filteredNames, isPostgreSQL ? item.Value : item.Key);
            }

            return filteredNames;
        }

        /// <summary>
        /// Get a list of RegEx expressions for auto-exporting data
        /// </summary>
        public static SortedSet<string> GetTableRegExToAutoExportData()
        {
            return new SortedSet<string>
            {
                ".*_?Type_?Name",
                ".*_?State_?Name",
                ".*_State",
                ".*_States"
            };
        }

        private void InitializeLogFile(SchemaExportOptions options)
        {
            if (!options.LogMessagesToFile)
                return;

            var baseLogFilePath = SchemaExportOptions.GetLogFilePath(options, "DB_Schema_Export_Tool_log");

            LogTools.CreateFileLogger(baseLogFilePath, BaseLogger.LogLevels.DEBUG);

            // Move log files over 32 days old into a year-based subdirectory
            FileLogger.ArchiveOldLogFilesNow();

            ConsoleMsgUtils.ShowDebug("Log file path: " + LogTools.CurrentLogFilePath);
        }

        /// <summary>
        /// Return true if the text in headerName matches one of the supported names for the Table Name column
        /// </summary>
        /// <param name="headerName">Header name</param>
        private bool IsHeaderRowTableColumn(string headerName)
        {
            headerName = headerName.Trim();

            return headerName.Equals("Table", StringComparison.OrdinalIgnoreCase) ||
                   headerName.Equals("TableName", StringComparison.OrdinalIgnoreCase) ||
                   headerName.Equals("Table Name", StringComparison.OrdinalIgnoreCase) ||
                   headerName.Equals("Table_Name", StringComparison.OrdinalIgnoreCase) ||
                   headerName.Equals("SourceTableName", StringComparison.OrdinalIgnoreCase);
        }

        private void LoadColumnMapInfo(string columnMapFilePath)
        {
            mOptions.ColumnMapForDataExport.Clear();

            var currentTable = string.Empty;
            var currentTableColumns = new ColumnMapInfo(string.Empty);

            try
            {
                if (string.IsNullOrWhiteSpace(columnMapFilePath))
                {
                    return;
                }

                var dataFile = new FileInfo(columnMapFilePath);

                if (!dataFile.Exists)
                {
                    Console.WriteLine();
                    OnStatusEvent("Column Map File not found");
                    OnWarningEvent("File not found: " + dataFile.FullName);
                    return;
                }

                ShowTrace(string.Format("Reading column information from {0}", dataFile.FullName));

                var headerLineChecked = false;

                using var dataReader = new StreamReader(new FileStream(dataFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                while (!dataReader.EndOfStream)
                {
                    var dataLine = dataReader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    var lineParts = dataLine.Split('\t');

                    if (!headerLineChecked)
                    {
                        headerLineChecked = true;

                        if (IsHeaderRowTableColumn(lineParts[0]))
                            continue;
                    }

                    if (lineParts.Length < 3)
                    {
                        OnDebugEvent("Skipping line with fewer than three columns: " + dataLine);
                        continue;
                    }

                    var sourceTableName = lineParts[0].Trim();
                    var sourceColumnName = lineParts[1].Trim();
                    var targetColumnName = lineParts[2].Trim();

                    if (!currentTable.Equals(sourceTableName, StringComparison.OrdinalIgnoreCase))
                    {
                        if (!mOptions.ColumnMapForDataExport.TryGetValue(sourceTableName, out currentTableColumns))
                        {
                            currentTableColumns = new ColumnMapInfo(sourceTableName);
                            mOptions.ColumnMapForDataExport.Add(sourceTableName, currentTableColumns);
                        }

                        currentTable = sourceTableName;
                    }

                    currentTableColumns.AddColumn(sourceColumnName, targetColumnName);
                }

                var tableText = mOptions.ColumnMapForDataExport.Count == 1 ? "table" : "tables";

                ShowTrace(string.Format(
                    "Loaded column information for {0} {1} from {2}",
                    mOptions.ColumnMapForDataExport.Count, tableText, dataFile.Name));
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in LoadColumnMapInfo", ex);
            }
        }

        private bool LoadColumnFiltersForTableData(string columnFilterFilePath)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(columnFilterFilePath))
                {
                    return true;
                }

                var filterFile = new FileInfo(columnFilterFilePath);

                if (!filterFile.Exists)
                {
                    Console.WriteLine();
                    OnStatusEvent("Table Data Column Filter File not found");
                    OnWarningEvent("File not found: " + filterFile.FullName);
                    return false;
                }

                ShowTrace(string.Format("Reading date filter information from {0}", filterFile.FullName));

                var headerLineChecked = false;
                var tableCountWithFilters = 0;

                using var dataReader = new StreamReader(new FileStream(filterFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                while (!dataReader.EndOfStream)
                {
                    var dataLine = dataReader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    var lineParts = dataLine.Split('\t');

                    if (!headerLineChecked)
                    {
                        headerLineChecked = true;

                        if (IsHeaderRowTableColumn(lineParts[0]))
                            continue;
                    }

                    if (lineParts.Length < 2)
                    {
                        OnDebugEvent("Skipping line with fewer than two columns: " + dataLine);
                        continue;
                    }

                    var tableName = lineParts[0].Trim();
                    var columnName = lineParts[1].Trim();

                    if (mOptions.ColumnMapForDataExport.TryGetValue(tableName, out var currentTableColumns))
                    {
                        currentTableColumns.SkipColumn(columnName);
                    }
                    else
                    {
                        var tableColumns = new ColumnMapInfo(columnName);
                        tableColumns.SkipColumn(columnName);

                        mOptions.ColumnMapForDataExport.Add(tableName, tableColumns);
                    }

                    tableCountWithFilters++;
                }

                var tableText = tableCountWithFilters == 1 ? "table" : "tables";
                ShowTrace(string.Format(
                    "Loaded column filters for {0} {1} from {2}",
                    tableCountWithFilters, tableText, filterFile.Name));

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in LoadColumnFiltersForTableData", ex);
                return false;
            }
        }

        /// <summary>
        /// Parse a file that defines date filters to use when exporting table data
        /// </summary>
        /// <param name="dateFilterFilePath">Date filter file path</param>
        /// <param name="tablesForDataExport">Tables to export data from</param>
        /// <returns>True if success (or if dateFilterFilePath is an empty string); false if an error</returns>
        private bool LoadDateFiltersForTableData(string dateFilterFilePath, ICollection<TableDataExportInfo> tablesForDataExport)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(dateFilterFilePath))
                {
                    return true;
                }

                var filterFile = new FileInfo(dateFilterFilePath);

                if (!filterFile.Exists)
                {
                    Console.WriteLine();
                    OnStatusEvent("Table Data Date Filter File not found");
                    OnWarningEvent("File not found: " + filterFile.FullName);
                    return false;
                }

                ShowTrace(string.Format("Reading date filter information from {0}", filterFile.FullName));

                var headerLineChecked = false;
                var tableCountWithFilters = 0;

                using var dataReader = new StreamReader(new FileStream(filterFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                while (!dataReader.EndOfStream)
                {
                    var dataLine = dataReader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    var lineParts = dataLine.Split('\t');

                    if (!headerLineChecked)
                    {
                        headerLineChecked = true;

                        if (IsHeaderRowTableColumn(lineParts[0]))
                            continue;
                    }

                    if (lineParts.Length < 3)
                    {
                        OnDebugEvent("Skipping line with fewer than three columns: " + dataLine);
                        continue;
                    }

                    var sourceTableName = lineParts[0].Trim();
                    var dateColumnName = lineParts[1].Trim();
                    var minimumDateText = lineParts[2].Trim();

                    if (!DateTime.TryParse(minimumDateText, out var minimumDate))
                    {
                        OnDebugEvent("Date filter for column {0} in table {1} is not a valid date: {2}", dateColumnName, sourceTableName, minimumDateText);

                        continue;
                    }

                    TableDataExportInfo tableInfo;

                    if (GetTableByName(tablesForDataExport, sourceTableName, out var matchingTableInfo))
                    {
                        tableInfo = matchingTableInfo;
                    }
                    else
                    {
                        tableInfo = new TableDataExportInfo(sourceTableName);
                        tablesForDataExport.Add(tableInfo);
                    }

                    tableInfo.DefineDateFilter(dateColumnName, minimumDate);
                    tableCountWithFilters++;
                }

                var tableText = tableCountWithFilters == 1 ? "table" : "tables";
                ShowTrace(string.Format(
                    "Loaded date filters for {0} {1} from {2}",
                    tableCountWithFilters, tableText, filterFile.Name));

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in LoadDateFiltersForTableData", ex);
                return false;
            }
        }

        private List<string> LoadTableDataExportOrderFile(string dataExportOrderFilePath, out bool abortProcessing)
        {
            var tableDataExportOrder = new List<string>();

            // This SortedSet is used to assure that tableDataExportOrder does not have any duplicate table names
            var tableNames = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);

            // This SortedSet is used to check for duplicate table names
            var tableNamesWithSchema = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);

            abortProcessing = false;

            try
            {
                if (string.IsNullOrWhiteSpace(dataExportOrderFilePath))
                {
                    return tableDataExportOrder;
                }

                var dataFile = new FileInfo(dataExportOrderFilePath);

                if (!dataFile.Exists)
                {
                    LogWarning("Table Data Export Order File not found: " + dataFile.FullName);
                    abortProcessing = true;
                    return tableDataExportOrder;
                }

                ShowTrace(string.Format("Reading table data export order from file {0}", dataFile.FullName));

                // This is not incremented for blank lines or comment lines
                var linesRead = 0;

                using var dataReader = new StreamReader(new FileStream(dataFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                while (!dataReader.EndOfStream)
                {
                    var dataLine = dataReader.ReadLine();

                    // Lines that start with # are treated as comment lines
                    if (string.IsNullOrWhiteSpace(dataLine) || dataLine.Trim().StartsWith("#"))
                        continue;

                    linesRead++;

                    // There should only be one column, but split on tab anyway in case other columns were included
                    var lineParts = dataLine.Split('\t');

                    if (linesRead == 1 && IsHeaderRowTableColumn(lineParts[0]))
                    {
                        // Header line
                        continue;
                    }

                    var tableNamePossiblyWithSchema = lineParts[0].Trim();

                    // ReSharper disable once CanSimplifySetAddingWithSingleCall
                    if (tableNamesWithSchema.Contains(tableNamePossiblyWithSchema))
                    {
                        OnWarningEvent("Table {0} is listed more than once in file {1}", tableNamePossiblyWithSchema, dataFile.Name);
                        continue;
                    }

                    tableNamesWithSchema.Add(tableNamePossiblyWithSchema);

                    var tableName = mDBSchemaExporter.GetNameWithoutSchema(tableNamePossiblyWithSchema);

                    if (tableNames.Contains(tableName))
                    {
                        // This table has already been stored in tableDataExportOrder
                        continue;
                    }

                    tableDataExportOrder.Add(tableName);
                    tableNames.Add(tableName);
                }

                var tableText = tableDataExportOrder.Count == 1 ? "table name" : "table names";

                ShowTrace(string.Format(
                    "Loaded {0} {1} for data export sort order",
                    tableDataExportOrder.Count, tableText));
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in LoadTableDataExportOrderFile", ex);
            }

            return tableDataExportOrder;
        }

        /// <summary>
        /// Read the table data export file
        /// </summary>
        /// <remarks>This file can also have views that should be renamed</remarks>
        /// <param name="tableDataFilePath">Table data file path</param>
        /// <param name="abortProcessing">Output: if true, abort processing</param>
        /// <returns>List of loaded column info</returns>
        private List<TableDataExportInfo> LoadTablesForDataExport(string tableDataFilePath, out bool abortProcessing)
        {
            var tablesForDataExport = new List<TableDataExportInfo>();
            abortProcessing = false;

            try
            {
                if (string.IsNullOrWhiteSpace(tableDataFilePath))
                {
                    return tablesForDataExport;
                }

                var dataFile = new FileInfo(tableDataFilePath);

                if (!dataFile.Exists)
                {
                    LogWarning("Table Data File not found: " + dataFile.FullName);
                    abortProcessing = true;
                    return tablesForDataExport;
                }

                var reader = new TableNameMapContainer.NameMapReader();
                RegisterEvents(reader);

                var tableNameMap = reader.LoadTableNameMapFile(dataFile.FullName, mOptions.PgInsertTableData, out abortProcessing);

                if (abortProcessing)
                {
                    return tablesForDataExport;
                }

                foreach (var tableNameInfo in tableNameMap)
                {
                    if (mOptions.TableNameFilterSet.Count > 0)
                    {
                        bool exportTable;

                        if (mOptions.TableNameFilterSet.Contains(tableNameInfo.SourceTableName))
                        {
                            exportTable = true;
                        }
                        else if (mOptions.TableNameFilterSet.Contains(mDBSchemaExporter.ConvertNameToSnakeCase(tableNameInfo.SourceTableName)))
                        {
                            exportTable = true;
                        }
                        else if (!string.IsNullOrWhiteSpace(tableNameInfo.TargetTableName) && mOptions.TableNameFilterSet.Contains(tableNameInfo.TargetTableName))
                        {
                            exportTable = true;
                        }
                        else
                        {
                            exportTable = false;
                        }

                        if (!exportTable)
                        {
                            LogDebug(string.Format(
                                "Skipping table {0} in file {1} since not present in the TableFilterList option", tableNameInfo.SourceTableName, dataFile.Name));

                            continue;
                        }
                    }

                    var tableInfo = new TableDataExportInfo(tableNameInfo);

                    tablesForDataExport.Add(tableInfo);
                }

                var tableText = tablesForDataExport.Count == 1 ? "table" : "tables";

                ShowTrace(string.Format(
                    "Loaded information for {0} {1} from {2}",
                    tablesForDataExport.Count, tableText, dataFile.Name));
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in LoadTablesForDataExport", ex);
            }

            return tablesForDataExport;
        }

        private bool MajorVersionsMatch(string line1, string line2)
        {
            var match1 = mVersionExtractor.Match(line1);
            var match2 = mVersionExtractor.Match(line2);

            return match1.Success && match2.Success &&
                   match1.Groups["Major"].Value.Equals(match2.Groups["Major"].Value);
        }

        /// <summary>
        /// Invoke event DebugEvent and show a debug message at the console; log if enabled
        /// </summary>
        /// <param name="message">Debug message</param>
        protected new void OnDebugEvent(string message)
        {
            LogDebug(message);
        }

        /// <summary>
        /// Invoke event ErrorEvent and show an error message at the console; log if enabled
        /// </summary>
        /// <param name="message">Error message</param>
        protected new void OnErrorEvent(string message)
        {
            LogError(message);
            StatusMessage = message;
        }

        /// <summary>
        /// Invoke event ErrorEvent and show an error message and stack trace at the console; log if enabled
        /// </summary>
        /// <param name="message">Error message</param>
        /// <param name="ex">Exception</param>
        protected new void OnErrorEvent(string message, Exception ex)
        {
            LogError(message, ex);

            if (ex != null && !message.Contains(ex.Message))
            {
                StatusMessage = message + ": " + ex.Message;
            }
            else
            {
                StatusMessage = message;
            }
        }

        /// <summary>
        /// Invoke event StatusEvent and show the message at the console; log if enabled
        /// </summary>
        /// <param name="message">Status message</param>
        protected new void OnStatusEvent(string message)
        {
            LogMessage(message);
            StatusMessage = message;
        }

        /// <summary>
        /// Invoke event WarningEvent and show the warning at the console; log if enabled
        /// </summary>
        /// <param name="message">Warning message</param>
        protected new void OnWarningEvent(string message)
        {
            LogWarning(message);
            StatusMessage = message;
        }

        private bool ParseGitStatus(FileSystemInfo targetDirectory, string consoleOutput, out int modifiedFileCount)
        {
            // Example output for Git with verbose output
            //     # On branch master
            //     # Your branch is behind 'origin/master' by 1 commit, and can be fast-forwarded.
            //     #   (use "git pull" to update your local branch)
            //     #
            //    # Changes not staged for commit:
            //    #   (use "git add <file>..." to update what will be committed)
            //    #   (use "git checkout -- <file>..." to discard changes in working directory)
            //    #
            //    #       modified:   PNNLOmics/Algorithms/Alignment/LcmsWarp/LcmsWarp.cs
            //    #
            //    # Untracked files:
            //    #   (use "git add <file>..." to include in what will be committed)
            //    #
            //    #       MyNewFile.txt
            //    no changes added to commit (use "git add" and/or "git commit -a")
            // Example output with Git for short output (-s)
            //  M PNNLOmics/Algorithms/Alignment/LcmsWarp/LcmsWarp.cs
            // ?? MyNewFile.txt

            var newOrModifiedStatusSymbols = new List<char>
            {
                'M',
                'A',
                'R'
            };

            modifiedFileCount = 0;
            using var gitStatusReader = new StringReader(consoleOutput);

            while (gitStatusReader.Peek() > -1)
            {
                var statusLine = gitStatusReader.ReadLine();

                if (string.IsNullOrWhiteSpace(statusLine) || statusLine.Length < 4)
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
                    // New file; added by the calling function
                }
                else if (newOrModifiedStatusSymbols.Contains(fileIndexStatus) || newOrModifiedStatusSymbols.Contains(fileWorkTreeStatus))
                {
                    modifiedFileCount++;
                }
            }

            return true;
        }

        private bool ParseSvnHgStatus(FileSystemInfo targetDirectory, string consoleOutput, RepoManagerType repoManagerType, out int modifiedFileCount)
        {
            // ReSharper disable GrammarMistakeInComment

            // Example output for Svn where M is modified, ? is new, and ! means deleted
            //        M       F:\Documents\Projects\DataMining\Database_Schema\DMS\DMS5\UpdateAnalysisJobStateNameCached.sql
            //        ?       F:\Documents\Projects\DataMining\Database_Schema\DMS\DMS5\UpdateAnalysisJobToolNameCached.sql
            //        M       F:\Documents\Projects\DataMining\Database_Schema\DMS\DMS5\V_Analysis_Job_List_Report_2.sql
            //        M       F:\Documents\Projects\DataMining\Database_Schema\DMS\DMS5\V_GetPipelineJobParameters.sql
            //        !       F:\Documents\Projects\DataMining\Database_Schema\DMS\DMS5\x_V_Analysis_Job.sql

            // Example output for Hg where M is modified, ? is new, and ! means deleted
            //        M F:\Documents\Projects\DataMining\Database_Schema\DMS\DMS5\UpdateAnalysisJobStateNameCached.sql
            //        ? F:\Documents\Projects\DataMining\Database_Schema\DMS\DMS5\UpdateAnalysisJobToolNameCached.sql
            //        M F:\Documents\Projects\DataMining\Database_Schema\DMS\DMS5\V_Analysis_Job_List_Report_2.sql
            //        M F:\Documents\Projects\DataMining\Database_Schema\DMS\DMS5\V_GetPipelineJobParameters.sql

            // ReSharper restore GrammarMistakeInComment
            var newOrModifiedStatusSymbols = new List<char>
                {
                    'M',
                    'A',
                    'R'
                };

            var minimumLineLength = repoManagerType == RepoManagerType.Svn ? 8 : 3;

            modifiedFileCount = 0;
            using var hgStatusReader = new StringReader(consoleOutput);

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
                    // New file; added by the calling function
                }
            }

            return true;
        }

        /// <summary>
        /// Export the schema or data for the databases defined in databasesToProcess
        /// </summary>
        /// <param name="options">Options</param>
        /// <returns>True if successful, false if an error</returns>
        /// <exception cref="ArgumentException"></exception>
        public bool ProcessDatabases(SchemaExportOptions options)
        {
            return ProcessDatabases(options.OutputDirectoryPath, options.ServerName, options.DatabasesToProcess);
        }

        /// <summary>
        /// Export the schema or data for the databases defined in databaseList
        /// </summary>
        /// <param name="outputDirectoryPath">Output directory path</param>
        /// <param name="serverName">Server name</param>
        /// <param name="databaseList">List of database names</param>
        /// <returns>True if successful, false if an error</returns>
        /// <exception cref="ArgumentException"></exception>
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
                // Keys in this dictionary are database names
                // Values are the output directory path used (values will be defined by ExportSchema then used by SyncSchemaFiles)
                var databaseNamesAndOutputPaths = databaseList.ToDictionary(databaseName => databaseName, _ => string.Empty);

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
        /// <param name="schemaExporter">DB Schema Exporter instance</param>
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

        /// <summary>
        /// Start scripting server and database objects
        /// </summary>
        /// <param name="databaseList">List of database names</param>
        /// <param name="tablesForDataExport">Tables to export data from</param>
        /// <returns>True if successful; false if an error</returns>
        public bool ScriptServerAndDBObjects(
            List<string> databaseList,
            List<TableDataExportInfo> tablesForDataExport)
        {
            var isValid = ValidateSchemaExporter();

            if (!isValid)
                return false;

            return ScriptServerAndDBObjectsWork(databaseList, tablesForDataExport, new List<string>());
        }

        /// <summary>
        /// Script server and database objects, plus optionally table data
        /// </summary>
        /// <param name="databaseList">List of database names</param>
        /// <param name="tablesForDataExport">Tables to export data from</param>
        /// <param name="tableDataExportOrder">List of table names that defines the order that table data should be exported (table name only, not schema)</param>
        /// <returns>True if successful; false if an error</returns>
        private bool ScriptServerAndDBObjectsWork(
            IReadOnlyList<string> databaseList,
            IReadOnlyList<TableDataExportInfo> tablesForDataExport,
            IReadOnlyList<string> tableDataExportOrder)
        {
            return mDBSchemaExporter.ScriptServerAndDBObjects(databaseList, tablesForDataExport, tableDataExportOrder);
        }

        private void ShowTrace(string message)
        {
            if (mOptions.Trace)
            {
                OnDebugEvent(message);
            }
        }

        /// <summary>
        /// Store a list of table name RegEx patterns for auto-exporting table data
        /// </summary>
        /// <param name="tableNameRegExSpecs">Table name Regex specs</param>
        public void StoreTableNameRegexToAutoExportData(SortedSet<string> tableNameRegExSpecs)
        {
            mDBSchemaExporter.StoreTableNameRegexToAutoExportData(tableNameRegExSpecs);
        }

        /// <summary>
        /// Store a list of table names for auto-exporting table data
        /// </summary>
        /// <param name="tableNames">Table names</param>
        public void StoreTableNamesToAutoExportData(SortedSet<string> tableNames)
        {
            mDBSchemaExporter.StoreTableNameRegexToAutoExportData(tableNames);
        }

        /// <summary>
        /// Compare two strings
        /// </summary>
        /// <remarks>Case sensitive comparison</remarks>
        /// <param name="text1">Item 1</param>
        /// <param name="text2">Item 2</param>
        /// <returns>True if the strings match, otherwise false</returns>
        private static bool StringMatch(string text1, string text2)
        {
            return text1.Equals(text2, StringComparison.Ordinal);
        }

        private bool SyncSchemaFiles(ICollection<KeyValuePair<string, string>> databaseNamesAndOutputPaths, string directoryPathForSync)
        {
            try
            {
                var startTime = DateTime.UtcNow;
                OnProgressUpdate("Synchronizing with " + directoryPathForSync, 0);

                var dbsProcessed = 0;
                var includeDbNameInCommitMessage = databaseNamesAndOutputPaths.Count > 1;

                foreach (var dbEntry in databaseNamesAndOutputPaths)
                {
                    var databaseName = dbEntry.Key;
                    var schemaOutputDirectory = dbEntry.Value;

                    if (string.IsNullOrWhiteSpace(schemaOutputDirectory))
                    {
                        OnErrorEvent("Schema output directory was not reported for " + databaseName + "; unable to synchronize");
                        continue;
                    }

                    var percentComplete = dbsProcessed / ((float)databaseNamesAndOutputPaths.Count * 100);
                    OnProgressUpdate("Synchronizing database " + databaseName, percentComplete);

                    var sourceDirectory = new DirectoryInfo(schemaOutputDirectory);
                    var createSubdirectoryOnSync = !mOptions.NoSubdirectoryOnSync;

                    string targetDirectoryPath;

                    if (databaseNamesAndOutputPaths.Count > 1 || createSubdirectoryOnSync)
                    {
                        targetDirectoryPath = Path.Combine(directoryPathForSync, databaseName);
                    }
                    else
                    {
                        targetDirectoryPath = directoryPathForSync;
                    }

                    var targetDirectory = new DirectoryInfo(targetDirectoryPath);

                    if (!sourceDirectory.Exists)
                    {
                        OnErrorEvent("Source directory not found; cannot synchronize: " + sourceDirectory.FullName);
                        return false;
                    }

                    if (sourceDirectory.FullName == targetDirectory.FullName)
                    {
                        OnErrorEvent("Sync directory is identical to the output SchemaFileDirectory; cannot synchronize");
                        return false;
                    }

                    var fileCopyCount = 0;

                    // This list holds the files that are copied from sourceDirectory to targetDirectory
                    var newFilePaths = new List<string>();

                    SyncSchemaFilesRecursive(sourceDirectory, targetDirectory, databaseName, newFilePaths, ref fileCopyCount);

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

                    dbsProcessed++;
                }

                if (mOptions.ShowStats)
                {
                    OnDebugEvent("Synchronized schema files in {0:0.0} seconds", DateTime.UtcNow.Subtract(startTime).TotalSeconds);
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in SyncSchemaFiles", ex);
                return false;
            }
        }

        private void SyncSchemaFilesRecursive(
            DirectoryInfo sourceDirectory,
            DirectoryInfo targetDirectory,
            string databaseName,
            ICollection<string> newFilePaths,
            ref int fileCopyCount)
        {
            if (!targetDirectory.Exists)
            {
                OnStatusEvent("Creating target directory for synchronization: " + targetDirectory.FullName);
                targetDirectory.Create();
            }

            foreach (var sourceFile in sourceDirectory.GetFiles())
            {
                if (sourceFile.Name.StartsWith("x_", StringComparison.OrdinalIgnoreCase) ||
                    sourceFile.Name.StartsWith("t_tmp_", StringComparison.OrdinalIgnoreCase) ||
                    sourceFile.Name.StartsWith("t_CandidateModsSeqWork_", StringComparison.OrdinalIgnoreCase) ||
                    sourceFile.Name.StartsWith("t_CandidateSeqWork_", StringComparison.OrdinalIgnoreCase) ||
                    sourceFile.Name.Equals("_AllObjects_.sql", StringComparison.OrdinalIgnoreCase))
                {
                    OnStatusEvent("Skipping {0} object {1}", databaseName, sourceFile.Name);
                    continue;
                }

                var targetFile = new FileInfo(Path.Combine(targetDirectory.FullName, sourceFile.Name));

                if (!FilesDiffer(sourceFile, targetFile, out var differenceReason))
                    continue;

                // var subtaskPercentComplete = fileProcessCount / ((float)filesToCopy.Count * 100);

                switch (differenceReason)
                {
                    case DifferenceReasonType.NewFile:
                        OnDebugEvent("  Copying new file " + sourceFile.Name);
                        newFilePaths.Add(targetFile.FullName);
                        break;
                    case DifferenceReasonType.Changed:
                        OnDebugEvent("  Copying changed file " + sourceFile.Name);
                        break;
                    default:
                        OnDebugEvent("  Copying file " + sourceFile.Name);
                        break;
                }

                sourceFile.CopyTo(targetFile.FullName, true);
                fileCopyCount++;
            }

            // Recursively call this method for each subdirectory
            foreach (var subdirectory in sourceDirectory.GetDirectories())
            {
                var targetSubdirectory = new DirectoryInfo(Path.Combine(targetDirectory.FullName, subdirectory.Name));
                SyncSchemaFilesRecursive(subdirectory, targetSubdirectory, databaseName, newFilePaths, ref fileCopyCount);
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

        private void UpdateRepoChanges(
            FileSystemInfo targetDirectory,
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
                        return;
                }

                if (!repoExe.Exists)
                {
                    OnErrorEvent("Repo exe not found at " + repoExe.FullName);
                    OnStatusEvent(repoSource);
                    return;
                }

                var programRunner = new ProgramRunner();
                RegisterEvents(programRunner);

                int maxRuntimeSeconds;

                bool success;
                Console.WriteLine();

                if (newFilePaths.Count > 0)
                {
                    OnStatusEvent(
                        "Adding {0} new {1} for tracking by {2}",
                        newFilePaths.Count,
                        CheckPlural(newFilePaths.Count, "file", "files"),
                        toolName);

                    // Add each of the new files
                    foreach (var newFilePath in newFilePaths)
                    {
                        var fileToAdd = new FileInfo(newFilePath);

                        if (fileToAdd.Directory == null)
                        {
                            OnErrorEvent("Cannot determine the parent directory of {0}; skipping", fileToAdd.FullName);
                            continue;
                        }

                        var addArgs = string.Format("add \"{0}\"", fileToAdd.FullName);
                        maxRuntimeSeconds = 30;

                        if (mOptions.PreviewExport)
                        {
                            OnStatusEvent("Preview running {0} {1}", repoExe.FullName, addArgs);
                            continue;
                        }

                        success = programRunner.RunCommand(
                            repoExe.FullName, addArgs, fileToAdd.Directory.FullName,
                            out var addConsoleOutput, out var addErrorOutput, maxRuntimeSeconds);

                        if (!success)
                        {
                            LogWarning(string.Format("Error reported for {0}: {1}", toolName, addConsoleOutput));
                            return;
                        }

                        if (repoManagerType == RepoManagerType.Git && addErrorOutput.StartsWith("fatal", StringComparison.OrdinalIgnoreCase))
                        {
                            LogWarning(string.Format("Error reported for {0}: {1}", toolName, addErrorOutput));

                            if (addErrorOutput.Contains("not a git repository"))
                            {
                                return;
                            }
                        }
                    }

                    Console.WriteLine();
                }

                OnStatusEvent("Looking for modified files tracked by {0} at {1}", toolName, targetDirectory.FullName);

                // Count the number of new or modified files
                maxRuntimeSeconds = 300;

                var statusArgs = repoManagerType == RepoManagerType.Git
                    ? "status -s -u"
                    : string.Format("status \"{0}\"", targetDirectory.FullName);

                if (mOptions.PreviewExport)
                {
                    OnStatusEvent("Preview running {0} {1}", repoExe.FullName, statusArgs);
                    return;
                }

                success = programRunner.RunCommand(
                    repoExe.FullName, statusArgs, targetDirectory.FullName,
                    out var statusConsoleOutput, out var statusErrorOutput, maxRuntimeSeconds);

                if (!success)
                {
                    return;
                }

                if (repoManagerType == RepoManagerType.Git && statusErrorOutput.StartsWith("fatal", StringComparison.OrdinalIgnoreCase))
                {
                    LogWarning(string.Format("Error reported for {0}: {1}", toolName, statusErrorOutput));
                }

                Console.WriteLine();
                int modifiedFileCount;

                if (repoManagerType is RepoManagerType.Svn or RepoManagerType.Hg)
                {
                    success = ParseSvnHgStatus(targetDirectory, statusConsoleOutput, repoManagerType, out modifiedFileCount);
                }
                else
                {
                    // Git
                    success = ParseGitStatus(targetDirectory, statusConsoleOutput, out modifiedFileCount);
                }

                if (!success)
                {
                    return;
                }

                if (fileCopyCount > 0 && modifiedFileCount == 0)
                {
                    LogWarning(string.Format(
                        "Note: File Copy Count is {0} yet the Modified File Count reported by {1} is zero; " +
                        "this may indicate a problem", fileCopyCount, toolName));

                    Console.WriteLine();
                }

                if (modifiedFileCount <= 0 && newFilePaths.Count == 0)
                    return;

                if (modifiedFileCount > 0)
                {
                    OnStatusEvent(
                        "Found {0} modified {1}",
                        modifiedFileCount,
                        CheckPlural(modifiedFileCount, "file", "files"));
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
                    OnStatusEvent("Committing changes to {0}: {1}", toolName, commitMessage);

                    // Commit the changes
                    var commitArgs = string.Format("commit \"{0}\" --message \"{1}\"", targetDirectory.FullName, commitMessage);
                    maxRuntimeSeconds = 120;

                    success = programRunner.RunCommand(
                        repoExe.FullName, commitArgs, targetDirectory.FullName,
                        out var commitConsoleOutput, out var commitErrorOutput, maxRuntimeSeconds);

                    if (!success)
                    {
                        OnErrorEvent("Commit error:\n{0}", commitConsoleOutput);
                        return;
                    }

                    if (repoManagerType == RepoManagerType.Git && commitErrorOutput.StartsWith("fatal", StringComparison.OrdinalIgnoreCase))
                    {
                        LogWarning(string.Format("Error reported for {0}: {1}", toolName, commitErrorOutput));
                        return;
                    }

                    if (repoManagerType == RepoManagerType.Svn && commitErrorOutput.IndexOf("Commit failed", StringComparison.OrdinalIgnoreCase) >= 0)
                    {
                        LogWarning(string.Format("Error reported for {0}: {1}", toolName, commitErrorOutput));
                        return;
                    }

                    if (repoManagerType is RepoManagerType.Hg or RepoManagerType.Git)
                    {
                        for (var iteration = 1; iteration <= 2; iteration++)
                        {
                            string pushArgs;

                            if (repoManagerType == RepoManagerType.Hg)
                            {
                                // Push the changes
                                pushArgs = "push";
                            }
                            else
                            {
                                // Push the changes to the master, both on origin and GitHub
                                if (iteration == 1)
                                {
                                    pushArgs = "push origin";
                                }
                                else
                                {
                                    pushArgs = "push GitHub";
                                }
                            }

                            maxRuntimeSeconds = 300;
                            success = programRunner.RunCommand(
                                repoExe.FullName, pushArgs, targetDirectory.FullName,
                                out var pushConsoleOutput, out _, maxRuntimeSeconds);

                            if (!success)
                            {
                                OnErrorEvent("Push error:\n{0}", pushConsoleOutput);
                                return;
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
            catch (Exception ex)
            {
                OnErrorEvent("Error in UpdateRepoChanges for tool " + toolName, ex);
            }
        }

        private bool ValidateSchemaExporter()
        {
            StatusMessage = string.Empty;

            try
            {
                if (mOptions.PostgreSQL)
                {
                    if (mDBSchemaExporter is DBSchemaExporterPostgreSQL)
                        return true;

                    mDBSchemaExporter = new DBSchemaExporterPostgreSQL(mOptions);
                    RegisterEvents(mDBSchemaExporter);
                }
                else
                {
                    if (mDBSchemaExporter is DBSchemaExporterSQLServer)
                        return true;

                    mDBSchemaExporter = new DBSchemaExporterSQLServer(mOptions);
                    RegisterEvents(mDBSchemaExporter);
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
