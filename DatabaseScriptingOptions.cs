namespace DB_Schema_Export_Tool
{

    public class DatabaseScriptingOptions
    {

        /// <summary>
        /// Target database type
        /// </summary>
        /// <remarks>Currently only SqlServer is supported</remarks>
        public enum TargetDatabaseTypeConstants
        {
            SqlServer = 0,
            MySql = 1,
            PostgreSQL = 2,
            SqlLite = 3,
        }

        public bool IncludeSystemObjects { get; set; }
        public bool IncludeTimestampInScriptFileHeader { get; set; }
        public bool ExportServerSettingsLoginsAndJobs { get; set; }
        public bool SaveDataAsInsertIntoStatements { get; set; }
        public TargetDatabaseTypeConstants DatabaseTypeForInsertInto { get; set; }
        public bool AutoSelectTableNamesForDataExport { get; set; }
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
            DatabaseTypeForInsertInto = TargetDatabaseTypeConstants.SqlServer;
            AutoSelectTableNamesForDataExport = true;
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
