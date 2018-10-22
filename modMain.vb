Option Strict On

' -------------------------------------------------------------------------------
' Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA) in 2006
'
' E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov
' Website: https://omics.pnl.gov/ or https://www.pnnl.gov/sysbio/ or https://panomics.pnnl.gov/
' -------------------------------------------------------------------------------
'
' Licensed under the 2-Clause BSD License; you may not use this file except
' in compliance with the License.  You may obtain a copy of the License at
' https://opensource.org/licenses/BSD-2-Clause
'
' Copyright 2018 Battelle Memorial Institute

Imports PRISM
Imports PRISM.FileProcessor

''' <summary>
''' Entry class for the DB Schema Export Tool
''' See clsMTSAutomation for additional information
''' </summary>
Module modMain
    Public Const PROGRAM_DATE As String = "September 21, 2018"

    Private mOutputDirectoryPath As String

    ' Private mParameterFilePath As String                    ' Not used in this app
    ' Private mRecurseDirectories As Boolean                  ' Not used in this app
    ' Private mMaxLevelsToRecurse As Integer                  ' Not used in this app

    Private mServer As String
    Private mDatabaseList As SortedSet(Of String)

    Private mDisableAutoDataExport As Boolean               ' Set to True to auto-select tables for which data should be exported
    Private mTableDataToExportFile As String                ' File with table names for which data should be exported

    Private mDatabaseSubdirectoryPrefix As String
    Private mCreateDirectoryForEachDB As Boolean

    Private mSync As Boolean
    Private mSyncDirectoryPath As String
    Private mSvnUpdate As Boolean
    Private mGitUpdate As Boolean
    Private mHgUpdate As Boolean

    Private mCommitUpdates As Boolean

    Private mLogMessagesToFile As Boolean
    Private mLogFilePath As String = String.Empty
    Private mLogDirectoryPath As String = String.Empty

    Private mPreviewExport As Boolean
    Private mShowStats As Boolean

    Private mProgressDescription As String = String.Empty
    Private mSubtaskDescription As String = String.Empty

    Private mSchemaExportTool As clsDBSchemaExportTool

    ''' <summary>
    ''' Program entry point
    ''' </summary>
    ''' <returns>0 if no error, error code if an error</returns>
    ''' <remarks></remarks>
    Public Function Main() As Integer

        Dim returnCode As Integer
        Dim commandLineParser As New clsParseCommandLine
        Dim proceed As Boolean

        Dim success As Boolean

        ' Initialize the options
        returnCode = 0
        mOutputDirectoryPath = String.Empty

        ' mParameterFilePath = String.Empty     ' Not used in this app
        ' mRecurseDirectories = False           ' Not used in this app
        ' mMaxLevelsToRecurse = 0               ' Not used in this app

        mServer = String.Empty
        mDatabaseList = New SortedSet(Of String)

        mDisableAutoDataExport = False
        mTableDataToExportFile = String.Empty

        mDatabaseSubdirectoryPrefix = clsExportDBSchema.DEFAULT_DB_OUTPUT_DIRECTORY_NAME_PREFIX
        mCreateDirectoryForEachDB = True

        mSync = False
        mSyncDirectoryPath = String.Empty

        mSvnUpdate = False
        mGitUpdate = False
        mHgUpdate = False

        mCommitUpdates = False

        mPreviewExport = False
        mShowStats = False

        Try
            proceed = False
            If commandLineParser.ParseCommandLine Then
                If SetOptionsUsingCommandLineParameters(commandLineParser) Then proceed = True
            Else
                proceed = True
            End If

            If Not proceed OrElse
               commandLineParser.NeedToShowHelp Then
                ShowProgramHelp()
                returnCode = -1
            Else

                If commandLineParser.ParameterCount + commandLineParser.NonSwitchParameterCount = 0 Then
                    ' Show the GUI

                    Dim objMain As New frmMain
                    objMain.ShowDialog()

                    Return 0
                End If

                If String.IsNullOrWhiteSpace(mOutputDirectoryPath) Then
                    ShowErrorMessage("Output directory path must be defined (use . for the current directory)")
                    Return -4
                End If

                If String.IsNullOrWhiteSpace(mServer) Then
                    ShowErrorMessage("Server must be defined using /Server")
                    Return -5
                End If


                If mDatabaseList.Count = 0 Then
                    ShowErrorMessage("Database must be defined using /DB or /DBList")
                    Return -6
                End If

                If mSync AndAlso String.IsNullOrWhiteSpace(mSyncDirectoryPath) Then
                    ShowErrorMessage("Sync directory must be defined when using /Sync")
                    Return -7
                End If

                mSchemaExportTool = New clsDBSchemaExportTool()
                RegisterEvents(mSchemaExportTool)

                AddHandler mSchemaExportTool.ProgressUpdate, AddressOf ProcessingClass_ProgressUpdate
                AddHandler mSchemaExportTool.ProgressComplete, AddressOf ProcessingClass_ProgressComplete
                AddHandler mSchemaExportTool.ProgressReset, AddressOf ProcessingClass_ProgressReset
                AddHandler mSchemaExportTool.SubtaskProgressChanged, AddressOf ProcessingClass_SubtaskProgressChanged

                With mSchemaExportTool
                    .ReThrowEvents = False
                    .LogMessagesToFile = mLogMessagesToFile
                    .LogFilePath = mLogFilePath
                    .LogDirectoryPath = mLogDirectoryPath

                    .PreviewExport = mPreviewExport
                    .ShowStats = mShowStats

                    .AutoSelectTableDataToExport = Not mDisableAutoDataExport
                    .TableDataToExportFile = mTableDataToExportFile

                    .DatabaseSubdirectoryPrefix = mDatabaseSubdirectoryPrefix

                    If mDatabaseList.Count = 1 AndAlso mCreateDirectoryForEachDB Then
                        If mSync AndAlso mSyncDirectoryPath.ToLower().EndsWith("\" & mDatabaseList.First.ToLower()) Then
                            ' Auto-disable mCreateDirectoryForEachDB
                            mCreateDirectoryForEachDB = False
                        End If
                    End If

                    .CreateDirectoryForEachDB = mCreateDirectoryForEachDB

                    .Sync = mSync
                    .SyncDirectoryPath = mSyncDirectoryPath

                    .SvnUpdate = mSvnUpdate
                    .GitUpdate = mGitUpdate
                    .HgUpdate = mHgUpdate

                    .CommitUpdates = mCommitUpdates

                End With

                success = mSchemaExportTool.ProcessDatabase(mOutputDirectoryPath, mServer, mDatabaseList)

                If Not success Then
                    returnCode = mSchemaExportTool.ErrorCode
                    If returnCode = 0 Then
                        ShowErrorMessage("Error while processing   : Unknown error (return code is 0)")
                    Else
                        ShowErrorMessage("Error while processing   : " & mSchemaExportTool.GetErrorMessage())
                    End If
                End If

                Threading.Thread.Sleep(2000)

            End If

        Catch ex As Exception
            ShowErrorMessage("Error occurred in modMain->Main: " & Environment.NewLine & ex.Message, ex)
            returnCode = -1
        End Try

        Return returnCode

    End Function

    Private Function GetAppVersion() As String
        Return ProcessFilesOrDirectoriesBase.GetAppVersion(PROGRAM_DATE)
    End Function

    Private Function SetOptionsUsingCommandLineParameters(commandLineParser As clsParseCommandLine) As Boolean
        ' Returns True if no problems; otherwise, returns false

        Dim value As String = String.Empty
        Dim lstValidParameters = New List(Of String) From {
          "O", "Server", "DB", "DBList", "DirectoryPrefix", "NoSubdirectory", "NoAutoData", "Data",
          "Sync", "Svn", "Git", "Hg", "Commit",
          "L", "LogDir", "Preview", "Stats"}

        Try
            ' Make sure no invalid parameters are present
            If commandLineParser.InvalidParametersPresent(lstValidParameters) Then
                ShowErrorMessage("Invalid command line parameters",
                  (From item In commandLineParser.InvalidParameters(lstValidParameters) Select "/" + item).ToList())
                Return False
            Else
                ' Query commandLineParser to see if various parameters are present
                If commandLineParser.RetrieveValueForParameter("O", value) Then
                    mOutputDirectoryPath = value
                ElseIf commandLineParser.NonSwitchParameterCount > 0 Then
                    mOutputDirectoryPath = commandLineParser.RetrieveNonSwitchParameter(0)
                End If

                If commandLineParser.RetrieveValueForParameter("Server", value) Then mServer = value

                If commandLineParser.RetrieveValueForParameter("DB", value) Then
                    If Not mDatabaseList.Contains(value) Then
                        mDatabaseList.Add(value)
                    End If
                End If

                If commandLineParser.RetrieveValueForParameter("DBList", value) Then
                    Dim lstDatabaseNames = value.Split(","c).ToList()

                    For Each databaseName In lstDatabaseNames
                        If Not mDatabaseList.Contains(databaseName) Then
                            mDatabaseList.Add(databaseName)
                        End If
                    Next
                End If

                If commandLineParser.RetrieveValueForParameter("DirectoryPrefix", value) Then mDatabaseSubdirectoryPrefix = value
                If commandLineParser.RetrieveValueForParameter("NoSubdirectory", value) Then mCreateDirectoryForEachDB = False
                If commandLineParser.RetrieveValueForParameter("NoAutoData", value) Then mDisableAutoDataExport = True
                If commandLineParser.RetrieveValueForParameter("Data", value) Then mTableDataToExportFile = value

                If commandLineParser.RetrieveValueForParameter("Sync", value) Then
                    mSync = True
                    mSyncDirectoryPath = value
                End If

                If commandLineParser.IsParameterPresent("Svn") Then mSvnUpdate = True

                If commandLineParser.IsParameterPresent("Git") Then mGitUpdate = True

                If commandLineParser.IsParameterPresent("Hg") Then mHgUpdate = True

                If commandLineParser.IsParameterPresent("Commit") Then mCommitUpdates = True

                ' If commandLineParser.RetrieveValueForParameter("P", value) Then mParameterFilePath = value

                If commandLineParser.RetrieveValueForParameter("L", value) Then
                    mLogMessagesToFile = True
                    If Not String.IsNullOrEmpty(value) Then
                        mLogFilePath = value
                    End If
                End If

                If commandLineParser.RetrieveValueForParameter("LogDir", value) Then
                    mLogMessagesToFile = True
                    If Not String.IsNullOrEmpty(value) Then
                        mLogDirectoryPath = value
                    End If
                End If

                If commandLineParser.RetrieveValueForParameter("Preview", value) Then mPreviewExport = True

                If commandLineParser.RetrieveValueForParameter("Stats", value) Then mShowStats = True


                Return True
            End If

        Catch ex As Exception
            ShowErrorMessage("Error parsing the command line parameters: " & Environment.NewLine & ex.Message)
        End Try

        Return False

    End Function

    Private Sub ShowErrorMessage(message As String, Optional ex As Exception = Nothing)
        ConsoleMsgUtils.ShowError(message, ex)
    End Sub

    Private Sub ShowErrorMessage(title As String, messages As IEnumerable(Of String))
        ConsoleMsgUtils.ShowErrors(title, messages)
    End Sub

    Private Sub ShowProgramHelp()

        Try

            Console.WriteLine(ConsoleMsgUtils.WrapParagraph(
                "This program exports Sql Server database objects as schema files. Starting this program without any parameters will show the GUI"))
            Console.WriteLine()
            Console.WriteLine("Command line syntax:" & Environment.NewLine & IO.Path.GetFileName(Reflection.Assembly.GetExecutingAssembly().Location))
            Console.WriteLine(" SchemaFileDirectory /Server:ServerName")
            Console.WriteLine(" /DB:Database /DBList:CommaSeparatedDatabaseName")
            Console.WriteLine(" [/DirectoryPrefix:PrefixText] [/NoSubdirectory]")
            Console.WriteLine(" [/Data:TableDataToExport.txt] [/NoAutoData] ")
            Console.WriteLine(" [/Sync:TargetDirectoryPath] [/Git] [/Svn] [/Hg] [/Commit]")
            Console.WriteLine(" [/L[:LogFilePath]] [/LogDir:LogDirectoryPath] [/Preview] [/Stats]")
            Console.WriteLine()
            Console.WriteLine(ConsoleMsgUtils.WrapParagraph(
                "SchemaFileDirectory is the path to the directory where the schema files will be saved; use a period for the current directory. " &
                "To process a single database, use /Server and /DB"))
            Console.WriteLine()
            Console.WriteLine("Use /DBList to process several databases (separate names with commas)")
            Console.WriteLine()
            Console.WriteLine(ConsoleMsgUtils.WrapParagraph(
                "By default, a subdirectory named " & clsExportDBSchema.DEFAULT_DB_OUTPUT_DIRECTORY_NAME_PREFIX & "DatabaseName will be created below SchemaFileDirectory. " &
                "Customize this the prefix text using /DirectoryPrefix"))
            Console.WriteLine()
            Console.WriteLine("Use /NoSubdirectory to disable auto creating a subdirectory for the database being exported")
            Console.WriteLine("Note: subdirectories will always be created if you use /DBList and specify more than one database")
            Console.WriteLine()
            Console.WriteLine(ConsoleMsgUtils.WrapParagraph(
                "Use /Data to define a text file with table names (one name per line) for which the data should be exported"))
            Console.WriteLine()
            Console.WriteLine(ConsoleMsgUtils.WrapParagraph(
                "In addition to table names defined in /Data, there are default tables which will have their data exported. " &
                "Disable the defaults using /NoAutoData"))
            Console.WriteLine()
            Console.WriteLine(ConsoleMsgUtils.WrapParagraph(
                "Use /Sync to copy new/changed files from the output directory to an alternative directory. " &
                "This is advantageous to prevent file timestamps from getting updated every time the schema is exported"))
            Console.WriteLine()
            Console.WriteLine("Use /Git to auto-update any new or changed files using Git")
            Console.WriteLine("Use /Svn to auto-update any new or changed files using Subversion")
            Console.WriteLine("Use /Hg  to auto-update any new or changed files using Mercurial")
            Console.WriteLine("Use /Commit to commit any updates to the repository")
            Console.WriteLine()

            Console.WriteLine("Use /L to log messages to a file; you can optionally specify a log file name using /L:LogFilePath.")
            Console.WriteLine(ConsoleMsgUtils.WrapParagraph(
                "Use /LogDir to specify the Directory to save the log file in. By default, the log file is created in the current working directory."))
            Console.WriteLine()
            Console.WriteLine("Use /Preview to count the number of database objects that would be exported")
            Console.WriteLine("Use /Stats to show (but not log) export stats")
            Console.WriteLine()
            Console.WriteLine("Program written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA) in 2006")
            Console.WriteLine("Command line interface added in 2014")
            Console.WriteLine("Version: " & GetAppVersion())
            Console.WriteLine()

            Console.WriteLine("E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov")
            Console.WriteLine("Website: https://omics.pnl.gov/ or https://panomics.pnnl.gov/")
            Console.WriteLine()

            ' Delay for 750 msec in case the user double clicked this file from within Windows Explorer (or started the program via a shortcut)
            Threading.Thread.Sleep(750)

        Catch ex As Exception
            ShowErrorMessage("Error displaying the program syntax: " & ex.Message)
        End Try

    End Sub

    Private Sub RegisterEvents(processor As EventNotifier)
        AddHandler processor.DebugEvent, AddressOf Processor_DebugEvent
        AddHandler processor.ErrorEvent, AddressOf Processor_ErrorEvent
        AddHandler processor.StatusEvent, AddressOf Processor_StatusEvent
        AddHandler processor.WarningEvent, AddressOf Processor_WarningEvent
    End Sub

    Private Sub Processor_DebugEvent(message As String)
        ConsoleMsgUtils.ShowDebug(message)
    End Sub

    Private Sub Processor_ErrorEvent(message As String, ex As Exception)
        ShowErrorMessage(message, ex)
    End Sub

    Private Sub Processor_StatusEvent(message As String)
        Console.WriteLine(message)
    End Sub

    Private Sub Processor_WarningEvent(message As String)
        ConsoleMsgUtils.ShowWarning(message)
    End Sub

    Private Sub ShowProgressDescriptionIfChanged(taskDescription As String)
        If Not String.Equals(taskDescription, mProgressDescription) Then
            mProgressDescription = String.Copy(taskDescription)
            Console.WriteLine(taskDescription)
        End If
    End Sub

    Private Sub ProcessingClass_ProgressUpdate(taskDescription As String, percentComplete As Single)
        ShowProgressDescriptionIfChanged(taskDescription)
    End Sub

    Private Sub ProcessingClass_ProgressComplete()
        Console.WriteLine("Processing complete")
    End Sub

    Private Sub ProcessingClass_ProgressReset()
        ShowProgressDescriptionIfChanged(mSchemaExportTool.ProgressStepDescription)
    End Sub

    Private Sub ProcessingClass_SubtaskProgressChanged(taskDescription As String, percentComplete As Single)

        If Not String.Equals(taskDescription, mSubtaskDescription) Then
            mSubtaskDescription = String.Copy(taskDescription)
            If taskDescription.StartsWith("  ") Then
                Console.WriteLine(taskDescription)
            Else
                Console.WriteLine("  " & taskDescription)
            End If

        End If

    End Sub
End Module