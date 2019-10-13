using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using Npgsql;
using PRISM;

namespace DB_Schema_Export_Tool
{
    public class DBSchemaExporterPostgreSQL : DBSchemaExporterBase
    {
        #region "Constants and Enums"

        public const int DEFAULT_PORT = 5432;

        public const string POSTGRES_DATABASE = "postgres";

        #endregion

        #region "Classwide Variables"

        private NpgsqlConnection mPgConnection;

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="options"></param>
        public DBSchemaExporterPostgreSQL(SchemaExportOptions options) : base(options)
        {

        }

        /// <summary>
        /// Connect to the server specified in mOptions
        /// </summary>
        /// <param name="databaseName">PostgreSQL database to connect to</param>
        /// <returns>True if successfully connected, false if a problem</returns>
        public override bool ConnectToServer(string databaseName = "")
        {
            var success = ConnectToPgServer(databaseName);
            return success;
        }

        /// <summary>
        /// Connect to the server specified in mOptions
        /// </summary>
        /// <param name="databaseName">PostgreSQL database to connect to</param>
        /// <returns>True if successfully connected, false if a problem</returns>
        private bool ConnectToPgServer(string databaseName)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(databaseName))
                {
                    databaseName = POSTGRES_DATABASE;
                }

                // Initialize the current connection options
                if (mPgConnection == null)
                {
                    ResetServerConnection();
                }
                else if (ValidServerConnection())
                {
                    var expectedNameAndPort = string.Format("tcp://{0}:{1}", mOptions.ServerName, mOptions.PgPort);
                    if (string.Equals(mPgConnection.DataSource, expectedNameAndPort, StringComparison.OrdinalIgnoreCase))
                    {
                        if (mCurrentServerInfo.DatabaseName.Equals(databaseName) &&
                            mCurrentServerInfo.UserName.Equals(mOptions.DBUser))
                        {
                            // Already connected; no need to re-connect
                            return true;
                        }
                    }

                    try
                    {
                        mPgConnection.Close();
                    }
                    catch (Exception)
                    {
                        // Ignore errors here
                    }

                }

                // Connect to server mOptions.ServerName
                var connected = LoginToServerWork(databaseName, out mPgConnection);
                if (!connected)
                {
                    if (ErrorCode == DBSchemaExportErrorCodes.NoError)
                    {
                        SetLocalError(DBSchemaExportErrorCodes.DatabaseConnectionError, "Error logging into server " + GetServerConnectionInfo());
                    }
                }

                return connected;
            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.DatabaseConnectionError, "Error logging into server " + GetServerConnectionInfo(), ex);
                mConnectedToServer = false;
                mPgConnection = null;
                return false;
            }

        }

        /// <summary>
        /// Retrieve a list of tables in the given database
        /// </summary>
        /// <param name="databaseName">Database to query</param>
        /// <param name="includeTableRowCounts">When true, then determines the row count in each table</param>
        /// <param name="includeSystemObjects">When true, then also returns system var tables</param>
        /// <returns>Dictionary where keys are table names and values are row counts (if includeTableRowCounts = true)</returns>
        public override Dictionary<string, long> GetDatabaseTableNames(string databaseName, bool includeTableRowCounts, bool includeSystemObjects)
        {
            return GetPgServerDatabaseTableNames(databaseName, includeTableRowCounts, includeSystemObjects);
        }

        /// <summary>
        /// Lookup the table names in the specified database, optionally also determining table row counts
        /// </summary>
        /// <param name="databaseName">Database to query</param>
        /// <param name="includeTableRowCounts">When true, then determines the row count in each table</param>
        /// <param name="includeSystemObjects">When true, then also returns system var tables</param>
        /// <returns>Dictionary where keys are table names and values are row counts (if includeTableRowCounts = true)</returns>
        public Dictionary<string, long> GetPgServerDatabaseTableNames(string databaseName, bool includeTableRowCounts, bool includeSystemObjects)
        {
            // Keys are table names; values are row counts
            var databaseTableInfo = new Dictionary<string, long>();

            try
            {
                InitializeLocalVariables();

                if (!ConnectToServer(databaseName))
                {
                    var databaseList = GetServerDatabases();
                    if (!databaseList.Contains(databaseName))
                    {
                        OnWarningEvent(string.Format("Database {0} not found on sever {1}", databaseName, mCurrentServerInfo.ServerName));
                    }

                    return databaseTableInfo;
                }

                if (!ValidServerConnection())
                {
                    OnWarningEvent("Not connected to a server; cannot retrieve the list of the tables in database " + databaseName);
                    return databaseTableInfo;
                }

                if (string.IsNullOrWhiteSpace(databaseName))
                {
                    OnWarningEvent("Empty database name sent to GetSqlServerDatabaseTableNames");
                    return databaseTableInfo;
                }

                // Get the list of tables in this database
                OnDebugEvent(string.Format("Obtaining list of tables in database {0} on server {1}",
                                           databaseName, mCurrentServerInfo.ServerName));

                const string sql = "SELECT schemaname, tablename, tableowner FROM pg_tables";

                var tableListCommand = new NpgsqlCommand(sql, mPgConnection);
                using (var reader = tableListCommand.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            var schemaName = reader.GetString(0);
                            var tableName = reader.GetString(1);
                            // var tableOwner = reader.GetString(2);

                            var isSystemObject = schemaName.Equals("pg_catalog") || schemaName.Equals("information_schema");

                            if (!includeSystemObjects && isSystemObject)
                                continue;

                            var tableNameWithSchema = PossiblyQuoteColumnName(schemaName, false) + "." +
                                                      PossiblyQuoteColumnName(tableName, false);

                            databaseTableInfo.Add(tableNameWithSchema, 0);

                        }
                    }
                }

                if (includeTableRowCounts)
                {
                    var tableNames = databaseTableInfo.Keys.ToList();
                    var index = 0;

                    foreach (var tableNameWithSchema in tableNames)
                    {

                        // ReSharper disable StringLiteralTypo
                        var rowCountSql = string.Format("SELECT relname, reltuples::bigint as ApproximateRowCount " +
                                                        "FROM pg_class " +
                                                        "WHERE oid = '{0}'::regclass", tableNameWithSchema);
                        // ReSharper restore StringLiteralTypo

                        var rowCountCmd = new NpgsqlCommand(rowCountSql, mPgConnection);
                        using (var reader = rowCountCmd.ExecuteReader())
                        {
                            if (reader.HasRows && reader.Read())
                            {
                                // var tableName = reader.GetString(0);
                                var approximateRowCount = reader.GetInt64(1);

                                databaseTableInfo[tableNameWithSchema] = approximateRowCount;
                            }
                        }

                        index++;
                        var subTaskProgress = ComputeSubtaskProgress(index, tableNames.Count);
                        var percentComplete = ComputeIncrementalProgress(0, 50, subTaskProgress);
                        OnProgressUpdate("Reading database tables", percentComplete);

                        if (mAbortProcessing)
                        {
                            OnWarningEvent("Aborted processing");
                            break;
                        }
                    }

                    // Re-query tables with a row count of 0, since they likely were not properly listed in pg_class
                    index = 0;
                    foreach (var tableNameWithSchema in tableNames)
                    {
                        if (databaseTableInfo[tableNameWithSchema] > 0)
                            continue;

                        var rowCountSql = string.Format("SELECT count(*) FROM {0} ", tableNameWithSchema);

                        var rowCountCmd = new NpgsqlCommand(rowCountSql, mPgConnection);
                        using (var reader = rowCountCmd.ExecuteReader())
                        {
                            if (reader.HasRows && reader.Read())
                            {
                                var rowCount = reader.GetInt64(0);
                                databaseTableInfo[tableNameWithSchema] = rowCount;
                            }
                        }

                        index++;
                        var subTaskProgress = ComputeSubtaskProgress(index, tableNames.Count);
                        var percentComplete = ComputeIncrementalProgress(50, 100, subTaskProgress);
                        OnProgressUpdate("Reading database tables", percentComplete);

                        if (mAbortProcessing)
                        {
                            OnWarningEvent("Aborted processing");
                            break;
                        }
                    }

                }

                OnProgressComplete();
                OnStatusEvent(string.Format("Found {0} tables in database {1}", databaseTableInfo.Count, databaseName));

                return databaseTableInfo;
            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.GeneralError,
                              string.Format("Error obtaining list of tables in database {0} on server {1}",
                                            databaseName, mCurrentServerInfo.ServerName), ex);

                return new Dictionary<string, long>();
            }
        }

        private string GetServerConnectionInfo()
        {
            return string.Format("{0}, port {1}, user {2}", mOptions.ServerName, mOptions.PgPort, mOptions.DBUser);
        }

        public override IEnumerable<string> GetServerDatabases()
        {
            try
            {
                InitializeLocalVariables();

                if (!ConnectToServer())
                {
                    return new List<string>();
                }

                if (!ValidServerConnection())
                {
                    OnWarningEvent("Not connected to a server; cannot retrieve the list of the server's databases");
                    return new List<string>();
                }

                // Obtain a list of all databases actually residing on the server (according to the Master database)
                OnStatusEvent("Obtaining list of databases on " + mCurrentServerInfo.ServerName);

                var databaseNames = GetServerDatabasesWork();
                if (!mAbortProcessing)
                {
                    OnProgressUpdate("Done", 100);
                }

                return databaseNames;

            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.DatabaseConnectionError, "Error obtaining list of databases on current server", ex);
                return new List<string>();
            }
        }

        protected override IEnumerable<string> GetServerDatabasesCurrentConnection()
        {
            var databaseNames = GetServerDatabasesWork();
            return databaseNames;
        }

        private IEnumerable<string> GetServerDatabasesWork()
        {
            var databaseNames = new List<string>();

            // ReSharper disable StringLiteralTypo
            const string sql = "SELECT datname FROM pg_database WHERE datistemplate=false and datallowconn=true ORDER BY datname";
            // ReSharper restore StringLiteralTypo

            var cmd = new NpgsqlCommand(sql, mPgConnection);
            using (var reader = cmd.ExecuteReader())
            {
                if (reader.HasRows)
                {
                    while (reader.Read())
                    {
                        databaseNames.Add(reader.GetString(0));
                    }
                }
            }

            return databaseNames;
        }

        /// <summary>
        /// Login to the server
        /// </summary>
        /// <param name="databaseName">PostgreSQL database to connect to</param>
        /// <param name="pgConnection"></param>
        /// <returns>True if success, otherwise false</returns>
        private bool LoginToServerWork(string databaseName, out NpgsqlConnection pgConnection)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(mOptions.DBUser))
                {
                    SetLocalError(DBSchemaExportErrorCodes.DatabaseConnectionError, "Cannot use an empty username to login to server " + mOptions.ServerName);
                    pgConnection = null;
                    return false;
                }

                string userPassword;
                if (string.IsNullOrWhiteSpace(mOptions.DBUserPassword))
                {
                    userPassword = LookupUserPasswordFromDisk(mOptions.DBUser, databaseName);
                    if (string.IsNullOrWhiteSpace(userPassword))
                    {
                        // A warning or error should have already been logged
                        pgConnection = null;
                        return false;
                    }
                }
                else
                {
                    userPassword = mOptions.DBUserPassword;
                }

                var connectionString = string.Format("Host={0};Username={1};Password={2};Database={3}",
                    mOptions.ServerName, mOptions.DBUser, userPassword, databaseName);

                pgConnection = new NpgsqlConnection(connectionString);
                pgConnection.Open();

                // If no error occurred, set .Connected = True and duplicate the connection info
                mConnectedToServer = true;
                mCurrentServerInfo.UpdateInfo(mOptions, databaseName);

                return true;
            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.DatabaseConnectionError, "Error logging into server " + GetServerConnectionInfo(), ex);
                pgConnection = null;
                return false;
            }
        }

        private string LookupUserPasswordFromDisk(string pgUser, string currentDatabase)
        {
            try
            {
                var candidateFilePaths = new List<string>();
                string passwordFileName;

                if (SystemInfo.IsLinux)
                {
                    passwordFileName = ".pgpass";
                    candidateFilePaths.Add(Path.Combine("~", passwordFileName));
                    candidateFilePaths.Add(passwordFileName);
                }
                else
                {
                    passwordFileName = "pgpass.conf";
                    var appdataDirectory = Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData);

                    candidateFilePaths.Add(Path.Combine(appdataDirectory, "postgresql", passwordFileName));
                    candidateFilePaths.Add(passwordFileName);
                }

                FileInfo passwordFile = null;

                foreach (var candidatePath in candidateFilePaths)
                {
                    var candidateFile = new FileInfo(candidatePath);
                    if (candidateFile.Exists)
                    {
                        passwordFile = candidateFile;
                        break;
                    }
                }

                if (passwordFile == null)
                {
                    OnWarningEvent(string.Format("Could not find the {0} file; unable to determine the password for user {1}",
                                                 passwordFileName, pgUser));
                    return string.Empty;
                }

                using (var reader = new StreamReader(new FileStream(passwordFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        // dataLine is of the form
                        // hostname:port:database:username:password
                        // The first four fields can contain a *
                        var lineParts = dataLine.Split(new[] { ':' }, 5);

                        if (lineParts.Length < 5)
                            continue;

                        var hostname = lineParts[0];
                        var port = lineParts[1];
                        var database = lineParts[2];
                        var username = lineParts[3];

                        // Passwords are allowed to contain a colon; it should be preceded by a backslash in the pgpass file
                        var password = lineParts[4].Replace(@"\:", ":");

                        if (!string.Equals(hostname, mOptions.ServerName, StringComparison.OrdinalIgnoreCase) && !hostname.Equals("*"))
                            continue;

                        if (!port.Equals("*"))
                        {
                            if (!int.TryParse(port, out var portValue))
                                continue;

                            if (portValue != mOptions.PgPort)
                                continue;
                        }

                        if (!string.IsNullOrWhiteSpace(currentDatabase))
                        {
                            if (!string.Equals(database, currentDatabase, StringComparison.OrdinalIgnoreCase) && !database.Equals("*"))
                                continue;
                        }

                        if (!string.Equals(username, pgUser, StringComparison.OrdinalIgnoreCase) && !username.Equals("*"))
                            continue;

                        if (string.IsNullOrWhiteSpace(password))
                        {
                            OnWarningEvent(string.Format("The {0} file has a blank password for user {1}, server {2}, database {3}; ignoring this entry",
                                                         passwordFileName, username, hostname, database));
                            continue;
                        }

                        // If we get here, this password is valid for the current connection
                        OnDebugEvent(string.Format("Determined password for user {0} using {1}", pgUser, passwordFile.FullName));
                        return password;
                    }
                }

                OnWarningEvent(string.Format("Could not find a valid password for user {0} in file {1}", pgUser, passwordFile.FullName));
                return string.Empty;
            }
            catch (Exception ex)
            {
                SetLocalError(DBSchemaExportErrorCodes.DatabaseConnectionError,
                              string.Format("Error looking up the password for user {0} on server {1}",
                                            pgUser, GetServerConnectionInfo()), ex);

                return string.Empty;
            }
        }

        public override bool ScriptServerAndDBObjects(List<string> databaseList, List<string> tableNamesForDataExport)
        {
            throw new NotImplementedException();
        }

        private bool ValidServerConnection()
        {
            return mConnectedToServer && mPgConnection != null &&
                   mPgConnection.State != ConnectionState.Broken &&
                   mPgConnection.State != ConnectionState.Closed;
        }

    }
}
