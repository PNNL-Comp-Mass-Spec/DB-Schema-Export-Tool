using System.IO;

namespace DB_Schema_Export_Tool
{
    internal class PgStatementLogControlFiles
    {
        public string DisablePgStatementLogging { get; }

        public string EnablePgStatementLogging { get; }

        public string ShowLogMinDurationValue { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="outputDirectoryPath">Output directory path</param>
        public PgStatementLogControlFiles(string outputDirectoryPath)
        {
            DisablePgStatementLogging = Path.Combine(outputDirectoryPath, "DisablePgStatementLogging.sql");
            EnablePgStatementLogging = Path.Combine (outputDirectoryPath, "EnablePgStatementLogging.sql");
            ShowLogMinDurationValue = Path.Combine(outputDirectoryPath, "ShowPgStatementMinLogDuration.sql");
        }
    }
}
