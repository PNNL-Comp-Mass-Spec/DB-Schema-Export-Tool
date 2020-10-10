using System;
using System.IO;
using PRISM;

namespace DB_Schema_Export_Tool
{
    public class ProgramRunner : EventNotifier
    {

        public bool RunCommand(
            string exePath,
            string cmdArgs,
            string workDirPath,
            out string consoleOutput,
            out string errorOutput,
            int maxRuntimeSeconds)
        {
            consoleOutput = string.Empty;
            errorOutput = string.Empty;

            try
            {
                var programRunner = new ProgRunner
                {
                    Arguments = cmdArgs,
                    CreateNoWindow = true,
                    MonitoringInterval = 100,
                    Name = Path.GetFileNameWithoutExtension(exePath),
                    Program = exePath,
                    Repeat = false,
                    RepeatHoldOffTime = 0,
                    WorkDir = workDirPath,
                    CacheStandardOutput = true,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = false,
                    ConsoleOutputFilePath = string.Empty
                };

                RegisterEvents(programRunner);
                var dtStartTime = DateTime.UtcNow;
                var dtLastStatus = DateTime.UtcNow;
                var executionAborted = false;

                OnDebugEvent(string.Format("Running {0} {1}", exePath, cmdArgs));

                programRunner.StartAndMonitorProgram();

                // Wait for it to exit
                if (maxRuntimeSeconds < 10)
                {
                    maxRuntimeSeconds = 10;
                }

                // Loop until program is complete, or until maxRuntimeSeconds elapses
                while (programRunner.State != ProgRunner.States.NotMonitoring)
                {
                    System.Threading.Thread.Sleep(100);
                    var elapsedSeconds = DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds;
                    if (elapsedSeconds > maxRuntimeSeconds)
                    {
                        OnErrorEvent(string.Format("Program execution has surpassed {0} seconds; aborting {1}", maxRuntimeSeconds, exePath));

                        programRunner.StopMonitoringProgram(true);
                        executionAborted = true;
                    }

                    if (DateTime.UtcNow.Subtract(dtLastStatus).TotalSeconds > 15)
                    {
                        dtLastStatus = DateTime.UtcNow;
                        OnDebugEvent(string.Format("Waiting for {0}, {1:0} seconds elapsed",
                                                   Path.GetFileName(exePath), elapsedSeconds));
                    }

                }

                if (executionAborted)
                {
                    OnWarningEvent("ProgramRunner was aborted for " + exePath);
                    return true;
                }

                consoleOutput = programRunner.CachedConsoleOutput;
                errorOutput = programRunner.CachedConsoleError;
                return true;

            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in RunCommand", ex);
                return false;
            }

        }
    }
}
