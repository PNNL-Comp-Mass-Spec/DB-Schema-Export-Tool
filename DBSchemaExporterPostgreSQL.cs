using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DB_Schema_Export_Tool
{
    public class DBSchemaExporterPostgreSQL : DBSchemaExporterBase
    {
        public const int DEFAULT_PORT = 5432;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="options"></param>
        public DBSchemaExporterPostgreSQL(SchemaExportOptions options) : base(options)
        {

        }

        public override bool ConnectToServer(string databaseName = "")
        {
            throw new NotImplementedException();
        }

        public override Dictionary<string, long> GetDatabaseTableNames(string databaseName, bool includeTableRowCounts, bool includeSystemObjects)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<string> GetServerDatabases()
        {
            throw new NotImplementedException();
        }

        public override bool ScriptServerAndDBObjects(List<string> databaseList, List<string> tableNamesForDataExport)
        {
            throw new NotImplementedException();
        }
    }
}
