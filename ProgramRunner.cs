using System;
using System.IO;
using PRISM;

namespace DB_Schema_Export_Tool
{
    /// <summary>
    /// Class for running an external program and monitoring its progress
    /// </summary>
    public class ProgramRunner : EventNotifier
    {
        /// <summary>
        /// Run the specified external program (.exe)
        /// </summary>
        /// <param name="exePath"></param>
        /// <param name="cmdArgs"></param>
        /// <param name="workDirPath"></param>
        /// <param name="consoleOutput"></param>
        /// <param name="errorOutput"></param>
        /// <param name="maxRuntimeSeconds"></param>
        /// <returns>True if successful, false if an error</returns>
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

                OnDebugEvent("Running {0} {1}", exePath, cmdArgs);

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
                        OnErrorEvent("Program execution has surpassed {0} seconds; aborting {1}", maxRuntimeSeconds, exePath);

                        programRunner.StopMonitoringProgram(true);
                        executionAborted = true;
                    }

                    if (DateTime.UtcNow.Subtract(dtLastStatus).TotalSeconds > 15)
                    {
                        dtLastStatus = DateTime.UtcNow;
                        OnDebugEvent("Waiting for {0}, {1:0} seconds elapsed", Path.GetFileName(exePath), elapsedSeconds);
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
