namespace DB_Schema_Export_Tool
{

    public class DatabaseScriptingOptions
    {
        public bool IncludeSystemObjects { get; set; }
        public bool IncludeTimestampInScriptFileHeader { get; set; }
        public bool ExportServerSettingsLoginsAndJobs { get; set; }
        public bool SaveDataAsInsertIntoStatements { get; set; }
        public bool AutoSelectTablesForDataExport { get; set; }
        public bool ExportDBSchemasAndRoles { get; set; }
        public bool ExportTables { get; set; }
        public bool ExportViews { get; set; }
        public bool ExportStoredProcedures { get; set; }
        public bool ExportUserDefinedFunctions { get; set; }
        public bool ExportUserDefinedDataTypes { get; set; }
        public bool ExportUserDefinedTypes { get; set; }
        public bool ExportSynonyms { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public DatabaseScriptingOptions()
        {
            IncludeSystemObjects = false;
            IncludeTimestampInScriptFileHeader = false;
            ExportServerSettingsLoginsAndJobs = false;
            SaveDataAsInsertIntoStatements = true;
            AutoSelectTablesForDataExport = true;
            ExportDBSchemasAndRoles = true;
            ExportTables = true;
            ExportViews = true;
            ExportStoredProcedures = true;
            ExportUserDefinedFunctions = true;
            ExportUserDefinedDataTypes = true;
            ExportUserDefinedTypes = true;
            ExportSynonyms = true;
        }
    }
}
