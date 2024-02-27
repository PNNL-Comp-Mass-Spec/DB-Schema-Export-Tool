using System;
using PRISM;
using PRISM.Logging;

namespace DB_Schema_Export_Tool
{
    /// <summary>
    /// Base class for logging
    /// </summary>
    public abstract class LoggerBase : EventNotifier
    {
        private readonly SchemaExportOptions mOptions;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="options">Options</param>
        protected LoggerBase(SchemaExportOptions options)
        {
            mOptions = options;

            // The LogTools class displays messages at the console; we don't need the EventNotifier class also doing this
            WriteToConsoleIfNoListener = false;
        }

        /// <summary>
        /// Show a status message at the console and optionally include in the log file, tagging it as a debug message
        /// </summary>
        /// <remarks>The message is shown in dark gray in the console.</remarks>
        /// <param name="statusMessage">Status message</param>
        /// <param name="writeToLog">True to write to the log file; false to only display at console</param>
        protected void LogDebug(string statusMessage, bool writeToLog = true)
        {
            OnDebugEvent(statusMessage);

            if (mOptions.LogMessagesToFile && mOptions.Trace)
                LogTools.LogDebug(statusMessage, writeToLog);
        }

        /// <summary>
        /// Log an error message
        /// </summary>
        /// <param name="errorMessage">Error message</param>
        /// <param name="logToDb">When true, log the message to the database and the local log file</param>
        protected void LogError(string errorMessage, bool logToDb = false)
        {
            OnErrorEvent(errorMessage);

            if (mOptions.LogMessagesToFile)
                LogTools.LogError(errorMessage, null, logToDb);
        }

        /// <summary>
        /// Log an error message and exception
        /// </summary>
        /// <param name="errorMessage">Error message (do not include ex.message)</param>
        /// <param name="ex">Exception to log</param>
        protected void LogError(string errorMessage, Exception ex)
        {
            OnErrorEvent(errorMessage, ex);

            if (mOptions.LogMessagesToFile)
                LogTools.LogError(errorMessage, ex);
        }

        /// <summary>
        /// Show a status message at the console and optionally include in the log file
        /// </summary>
        /// <param name="statusMessage">Status message</param>
        /// <param name="isError">True if this is an error</param>
        /// <param name="writeToLog">True to write to the log file; false to only display at console</param>
        public void LogMessage(string statusMessage, bool isError = false, bool writeToLog = true)
        {
            OnStatusEvent(statusMessage);

            if (mOptions.LogMessagesToFile)
                LogTools.LogMessage(statusMessage, isError, writeToLog);
        }

        /// <summary>
        /// Log a warning message
        /// </summary>
        /// <param name="warningMessage">Warning message</param>
        /// <param name="logToDb">When true, log the message to the database and the local log file</param>
        protected void LogWarning(string warningMessage, bool logToDb = false)
        {
            OnWarningEvent(warningMessage);

            if (mOptions.LogMessagesToFile)
                LogTools.LogWarning(warningMessage, logToDb);
        }

        /// <summary>
        /// Register event handlers
        /// However, does not subscribe to .ProgressUpdate
        /// Note: the DatasetInfoPlugin does subscribe to .ProgressUpdate
        /// </summary>
        /// <param name="processingClass">Processing class</param>
        /// <param name="writeDebugEventsToLog">If true, write debug events to the log</param>
        protected void RegisterEvents(EventNotifier processingClass, bool writeDebugEventsToLog = true)
        {
            if (writeDebugEventsToLog)
            {
                processingClass.DebugEvent += DebugEventHandler;
            }
            else
            {
                processingClass.DebugEvent += DebugEventHandlerConsoleOnly;
            }

            processingClass.StatusEvent += StatusEventHandler;
            processingClass.ErrorEvent += ErrorEventHandler;
            processingClass.WarningEvent += WarningEventHandler;
            // Ignore: processingClass.ProgressUpdate += ProgressUpdateHandler;
        }

        /// <summary>
        /// Unregister the event handler for the given LogLevel
        /// </summary>
        /// <param name="processingClass">Processing class</param>
        /// <param name="messageType">Message type</param>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        // ReSharper disable once UnusedMember.Global
        protected void UnregisterEventHandler(EventNotifier processingClass, BaseLogger.LogLevels messageType)
        {
            switch (messageType)
            {
                case BaseLogger.LogLevels.DEBUG:
                    processingClass.DebugEvent -= DebugEventHandler;
                    processingClass.DebugEvent -= DebugEventHandlerConsoleOnly;
                    break;

                case BaseLogger.LogLevels.ERROR:
                    processingClass.ErrorEvent -= ErrorEventHandler;
                    break;

                case BaseLogger.LogLevels.WARN:
                    processingClass.WarningEvent -= WarningEventHandler;
                    break;

                case BaseLogger.LogLevels.INFO:
                    processingClass.StatusEvent -= StatusEventHandler;
                    break;

                case BaseLogger.LogLevels.FATAL:
                case BaseLogger.LogLevels.NOLOGGING:
                    throw new Exception("Log level not supported for unregistering");

                default:
                    throw new ArgumentOutOfRangeException(nameof(messageType), messageType, null);
            }
        }

        /// <summary>
        /// Show a debug event at the console, but do not log
        /// </summary>
        /// <param name="statusMessage">Debug message</param>
        protected void DebugEventHandlerConsoleOnly(string statusMessage)
        {
            LogDebug(statusMessage, writeToLog: false);
        }

        /// <summary>
        /// Show a debug event at the console (and log if enabled)
        /// </summary>
        /// <param name="statusMessage">Debug message</param>
        protected void DebugEventHandler(string statusMessage)
        {
            LogDebug(statusMessage);
        }

        /// <summary>
        /// Show a status event at the console (and log if enabled)
        /// </summary>
        /// <param name="statusMessage">message</param>
        protected void StatusEventHandler(string statusMessage)
        {
            LogMessage(statusMessage);
        }

        /// <summary>
        /// Show an error event at the console (and log if enabled)
        /// </summary>
        /// <param name="errorMessage">Error message</param>
        /// <param name="ex">Exception</param>
        protected void ErrorEventHandler(string errorMessage, Exception ex)
        {
            LogError(errorMessage, ex);
        }

        /// <summary>
        /// Show a warning event at the console (and log if enabled)
        /// </summary>
        /// <param name="warningMessage">Warning message</param>
        protected void WarningEventHandler(string warningMessage)
        {
            LogWarning(warningMessage);
        }
    }
}
