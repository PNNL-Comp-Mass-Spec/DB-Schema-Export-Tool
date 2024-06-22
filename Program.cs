using System;
using System.IO;
using System.Reflection;
using System.Threading;
#if ENABLE_GUI
using System.Windows.Forms;
#endif
using PRISM;
using PRISM.Logging;

namespace DB_Schema_Export_Tool
{
    internal static class Program
    {
        // ReSharper disable CommentTypo

        // Ignore Spelling: conf, dms, dmsreader, PostgreSQL, Proteinseqs, seqs

        // ReSharper restore CommentTypo

        private static DateTime mLastProgressTime;

        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        /// <param name="args">Command line arguments</param>
        /// <returns>0 if successful, -1 if an error</returns>
        [STAThread]
        private static int Main(string[] args)
        {
            mLastProgressTime = DateTime.UtcNow;

            var asmName = typeof(Program).GetTypeInfo().Assembly.GetName();
            var exeName = Path.GetFileName(Assembly.GetExecutingAssembly().Location);       // Alternatively: System.AppDomain.CurrentDomain.FriendlyName
            var version = SchemaExportOptions.GetAppVersion();

            var parser = new CommandLineParser<SchemaExportOptions>(asmName.Name, version)
            {
                ProgramInfo = "This program exports SQL Server or PostgreSQL database objects as schema files. " +
                              "Exported objects include tables, views, stored procedures, functions, and synonyms, " +
                              "plus also database properties including database roles and logins.",

                ContactInfo = "Program written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA)" +
                              Environment.NewLine + Environment.NewLine +
                              "E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov" + Environment.NewLine +
                              "Website: https://github.com/PNNL-Comp-Mass-Spec/ or https://www.pnnl.gov/integrative-omics",

                // ReSharper disable StringLiteralTypo
                UsageExamples = {
                        exeName +
                        @" C:\Cached_DBSchema /Server:Proteinseqs" +
                        " /DBList:Manager_Control,Protein_Sequences" +
                        @" /sync:""F:\Projects\Database_Schema\DMS\"" /Git /Commit" +
                        " /L /LogDir:Logs /Data:ProteinSeqs_Data_Tables.txt",
                        exeName +
                        @" C:\Cached_DBSchema /Server:Prismweb3" +
                        " /DB:dms /PgUser:dmsreader" +
                        @" /sync:""F:\Projects\Database_Schema\PostgreSQL\"" /Git /Commit" +
                        " /L /LogDir:Logs /Data:ProteinSeqs_Data_Tables.txt"
                    }
                // ReSharper restore StringLiteralTypo
            };

            parser.AddParamFileKey("Conf");
            parser.AddParamFileKey("P");

            parser.ParamKeysFieldWidth = 25;
            parser.ParamDescriptionFieldWidth = 70;

            if (args.Length == 0)
            {
#if ENABLE_GUI
                // Show the GUI

                Application.EnableVisualStyles();
                Application.SetCompatibleTextRenderingDefault(false);
                Application.Run(new frmMain());
#else
                parser.PrintHelp();
                Thread.Sleep(1500);
#endif
                return 0;
            }

            var result = parser.ParseArgs(args);
            var options = result.ParsedResults;

            try
            {
                if (!result.Success)
                {
                    if (parser.CreateParamFileProvided)
                    {
                        return 0;
                    }

                    // Delay for 1500 msec in case the user double-clicked this file from within Windows Explorer (or started the program via a shortcut)
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

                RegisterEvents(schemaExporter);

                var success = schemaExporter.ProcessDatabases(options);

                if (success)
                {
                    Console.WriteLine();
                    Console.WriteLine("Processing complete");
                    Thread.Sleep(500);
                    return 0;
                }

                ConsoleMsgUtils.ShowWarning("Processing error");
                Thread.Sleep(1500);
                return -1;
            }
            catch (Exception ex)
            {
                ConsoleMsgUtils.ShowError("Error occurred in Program->Main", ex);
                Thread.Sleep(1500);
                return -1;
            }
        }

        /// <summary>
        /// Use this method to chain events between classes
        /// </summary>
        /// <param name="sourceClass">Class to register</param>
        private static void RegisterEvents(IEventNotifier sourceClass)
        {
            sourceClass.DebugEvent += OnDebugEvent;
            sourceClass.StatusEvent += OnStatusEvent;
            sourceClass.ErrorEvent += OnErrorEvent;
            sourceClass.WarningEvent += OnWarningEvent;
            sourceClass.ProgressUpdate += ProgressChanged;
        }

        private static void OnDebugEvent(string message)
        {
            ConsoleMsgUtils.ShowDebug(message);
        }

        private static void OnErrorEvent(string message, Exception ex)
        {
            ConsoleMsgUtils.ShowError(message, ex);
        }

        private static void OnStatusEvent(string message)
        {
            Console.WriteLine(message);
        }

        private static void OnWarningEvent(string message)
        {
            ConsoleMsgUtils.ShowWarning(message);
        }

        private static void ProgressChanged(string progressMessage, float percentComplete)
        {
            if (DateTime.UtcNow.Subtract(mLastProgressTime).TotalSeconds < 3)
                return;

            Console.WriteLine();
            mLastProgressTime = DateTime.UtcNow;
            OnDebugEvent(percentComplete.ToString("0.0") + "%, " + progressMessage);
        }
    }
}
