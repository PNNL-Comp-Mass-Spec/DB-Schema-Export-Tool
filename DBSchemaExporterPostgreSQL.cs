using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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

        protected override bool ConnectToServer()
        {
            throw new NotImplementedException();
        }

        public override bool ScriptServerAndDBObjects(List<string> databaseList, List<string> tableNamesForDataExport)
        {
            throw new NotImplementedException();
        }
    }
}
