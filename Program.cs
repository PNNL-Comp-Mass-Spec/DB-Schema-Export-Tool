﻿using System;
using System.IO;
using System.Reflection;
using System.Threading;
#if ENABLE_GUI
using System.Windows.Forms;
#endif
using PRISM;

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
        [STAThread]
        private static int Main(string[] args)
        {
            mLastProgressTime = DateTime.UtcNow;

            var asmName = typeof(Program).GetTypeInfo().Assembly.GetName();
            var exeName = Path.GetFileName(Assembly.GetExecutingAssembly().Location);       // Alternatively: System.AppDomain.CurrentDomain.FriendlyName
            var version = SchemaExportOptions.GetAppVersion();

            var parser = new CommandLineParser<SchemaExportOptions>(asmName.Name, version)
            {
                ProgramInfo = "This program exports SQL Server or PostgreSQL database objects as schema files." +
                              "Exported objects include tables, views, stored procedures, functions, and synonyms, "+
                              "plus also database properties including database roles and logins.",

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

            parser.AddParamFileKey("Conf");
            parser.AddParamFileKey("P");

            parser.ParamKeysFieldWidth = 20;
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

                schemaExporter.ProgressUpdate += Processor_ProgressUpdate;

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

        private static void Processor_ProgressUpdate(string progressMessage, float percentComplete)
        {
            if (DateTime.UtcNow.Subtract(mLastProgressTime).TotalSeconds < 3)
                return;

            Console.WriteLine();
            mLastProgressTime = DateTime.UtcNow;
            Processor_DebugEvent(percentComplete.ToString("0.0") + "%, " + progressMessage);
        }
    }
}
