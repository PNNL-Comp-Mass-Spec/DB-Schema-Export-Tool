namespace DB_Schema_Export_Tool
{
    /// <summary>
    /// Database scripting options
    /// </summary>
    public class DatabaseScriptingOptions
    {
        // Ignore Spelling: Schemas

        /// <summary>
        /// When true, script system objects
        /// </summary>
        public bool IncludeSystemObjects { get; set; }

        /// <summary>
        /// When true, include timestamps in the script file header
        /// </summary>
        public bool IncludeTimestampInScriptFileHeader { get; set; }

        /// <summary>
        ///  When true, export server settings, logins, and SQL Server Agent jobs
        /// </summary>
        public bool ExportServerSettingsLoginsAndJobs { get; set; }

        /// <summary>
        /// Save data as insert into statements
        /// </summary>
        public bool SaveDataAsInsertIntoStatements { get; set; }

        /// <summary>
        /// Auto select tables for data export
        /// </summary>
        public bool AutoSelectTablesForDataExport { get; set; }

        /// <summary>
        /// Export DB Schemas And Roles
        /// </summary>
        public bool ExportDBSchemasAndRoles { get; set; }

        /// <summary>
        /// Export Tables
        /// </summary>
        public bool ExportTables { get; set; }

        /// <summary>
        /// Export Views
        /// </summary>
        public bool ExportViews { get; set; }

        /// <summary>
        /// Export Stored Procedures
        /// </summary>
        public bool ExportStoredProcedures { get; set; }

        /// <summary>
        /// Export User Defined Functions
        /// </summary>
        public bool ExportUserDefinedFunctions { get; set; }

        /// <summary>
        /// Export User Defined Data Types
        /// </summary>
        public bool ExportUserDefinedDataTypes { get; set; }

        /// <summary>
        /// Export User Defined Types
        /// </summary>
        public bool ExportUserDefinedTypes { get; set; }

        /// <summary>
        /// Export Synonyms
        /// </summary>
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
