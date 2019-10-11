using System;
using System.IO;
using System.Reflection;
using System.Threading;
using System.Windows.Forms;
using PRISM;

namespace DB_Schema_Export_Tool
{
    static class Program
    {

        private static DateTime mLastProgressTime;

        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        static int Main(string[] args)
        {
            mLastProgressTime = DateTime.UtcNow;

            var asmName = typeof(Program).GetTypeInfo().Assembly.GetName();
            var exeName = Path.GetFileName(Assembly.GetExecutingAssembly().Location);       // Alternatively: System.AppDomain.CurrentDomain.FriendlyName
            var version = SchemaExportOptions.GetAppVersion();

            var parser = new CommandLineParser<SchemaExportOptions>(asmName.Name, version)
            {
                ProgramInfo = "This program ...",

                ContactInfo = "Program written by Matthew Monroe for the Department of Energy" + Environment.NewLine +
                              "(PNNL, Richland, WA) in 2019" +
                              Environment.NewLine + Environment.NewLine +
                              "E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov" + Environment.NewLine +
                              "Website: https://panomics.pnnl.gov/ or https://omics.pnl.gov",

                // ReSharper disable StringLiteralTypo
                UsageExamples = {
                        exeName +
                        " C:\\Cached_DBSchema /Server:Proteinseqs" +
                        " /DBList:Manager_Control,Protein_Sequences" +
                        " /sync:\"F:\\Projects\\Database_Schema\\DMS\\\" /Git /Commit" +
                        " /L /LogDir:Logs /Data:ProteinSeqs_Data_Tables.txt",
                        exeName +
                        " C:\\Cached_DBSchema /Server:Prismweb3" +
                        " /DB:dms /PgUser:dmsreader" +
                        " /sync:\"F:\\Projects\\Database_Schema\\PostgreSQL\\\" /Git /Commit" +
                        " /L /LogDir:Logs /Data:ProteinSeqs_Data_Tables.txt"
                    }
                // ReSharper restore StringLiteralTypo
            };

            if (args.Length == 0)
            {
                // Show the GUI

                Application.EnableVisualStyles();
                Application.SetCompatibleTextRenderingDefault(false);
                Application.Run(new frmMain());
                return 0;
            }

            var parseResults = parser.ParseArgs(args);
            var options = parseResults.ParsedResults;

            try
            {
                if (!parseResults.Success)
                {
                    Thread.Sleep(1500);
                    return -1;
                }

                if (!options.ValidateArgs(out var errorMessage))
                {
                    parser.PrintHelp();

                    Console.WriteLine();
                    ConsoleMsgUtils.ShowWarning("Validation error:");
                    ConsoleMsgUtils.ShowWarning(errorMessage);

                    Thread.Sleep(1500);
                    return -1;
                }

                options.OutputSetOptions();

            }
            catch (Exception e)
            {
                Console.WriteLine();
                Console.Write($"Error running {exeName}");
                Console.WriteLine(e.Message);
                Console.WriteLine($"See help with {exeName} --help");
                return -1;
            }

            try
            {
                var schemaExporter = new DBSchemaExportTool(options);

                schemaExporter.ErrorEvent += Processor_ErrorEvent;
                schemaExporter.StatusEvent += Processor_StatusEvent;
                schemaExporter.WarningEvent += Processor_WarningEvent;

                var success = schemaExporter.ProcessDatabases(options);

                if (success)
                {
                    Console.WriteLine();
                    Console.WriteLine("Processing complete");
                    Thread.Sleep(1500);
                    return 0;
                }

                ConsoleMsgUtils.ShowWarning("Processing error");
                Thread.Sleep(2000);
                return -1;

            }
            catch (Exception ex)
            {
                ConsoleMsgUtils.ShowError("Error occurred in Program->Main", ex);
                Thread.Sleep(2000);
                return -1;
            }

        }


        private static void Processor_DebugEvent(string message)
        {
            ConsoleMsgUtils.ShowDebug(message);
        }

        private static void Processor_ErrorEvent(string message, Exception ex)
        {
            ConsoleMsgUtils.ShowErrorCustom(message, ex, false);
        }

        private static void Processor_StatusEvent(string message)
        {
            Console.WriteLine(message);
        }

        private static void Processor_ProgressUpdate(string progressMessage, float percentComplete)
        {
            if (DateTime.UtcNow.Subtract(mLastProgressTime).TotalSeconds < 5)
                return;

            Console.WriteLine();
            mLastProgressTime = DateTime.UtcNow;
            Processor_DebugEvent(percentComplete.ToString("0.0") + "%, " + progressMessage);
        }

        private static void Processor_WarningEvent(string message)
        {
            ConsoleMsgUtils.ShowWarning(message);
        }
    }
}
