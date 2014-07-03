Option Strict On
'
' -------------------------------------------------------------------------------
' Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA)
' Program started April 11, 2006
'
' E-mail: matthew.monroe@pnnl.gov or matt@alchemistmatt.com
' Website: http://panomics.pnl.gov/ or http://www.sysbio.org/resources/staff/
' -------------------------------------------------------------------------------
' 
' See clsMTSAutomation for additional information

Module modMain
    Public Const PROGRAM_DATE As String = "July 3, 2014"

	Private mOutputFolderPath As String

	Private mParameterFilePath As String						' Not used in this app

	Private mRecurseFolders As Boolean							' Not used in this app
	Private mRecurseFoldersMaxLevels As Integer					' Not used in this app

	Private mServer As String
	Private mDatabaseList As SortedSet(Of String)

	Private mDisableAutoDataExport As Boolean				' Set to True to auto-select tables for which data should be exported
	Private mTableDataToExportFile As String				' File with table names for which data should be exported

	Private mDatabaseSubfolderPrefix As String
	Private mCreateFolderForEachDB As Boolean

	Private mSync As Boolean
	Private mSyncFolderPath As String
    Private mSvnUpdate As Boolean
    Private mGitUpdate As Boolean
    Private mHgUpdate As Boolean

    Private mCommitUpdates As Boolean

    Private mLogMessagesToFile As Boolean
    Private mLogFilePath As String = String.Empty
    Private mLogFolderPath As String = String.Empty

    Private mQuietMode As Boolean

    Private mProgressDescription As String = String.Empty
    Private mSubtaskDescription As String = String.Empty

    Private WithEvents mProcessingClass As clsDBSchemaExportTool

    ''' <summary>
    ''' Program entry point
    ''' </summary>
    ''' <returns>0 if no error, error code if an error</returns>
    ''' <remarks></remarks>
    Public Function Main() As Integer

        Dim intReturnCode As Integer
        Dim objParseCommandLine As New clsParseCommandLine
        Dim blnProceed As Boolean

        Dim blnSuccess As Boolean

        ' Initialize the options
        intReturnCode = 0
        mOutputFolderPath = String.Empty

        mParameterFilePath = String.Empty

        mRecurseFolders = False                 ' Not used in this app    
        mRecurseFoldersMaxLevels = 0            ' Not used in this app    

        mServer = String.Empty
        mDatabaseList = New SortedSet(Of String)

        mDisableAutoDataExport = False
        mTableDataToExportFile = String.Empty

        mDatabaseSubfolderPrefix = clsExportDBSchema.DEFAULT_DB_OUTPUT_FOLDER_NAME_PREFIX
        mCreateFolderForEachDB = True

        mSync = False
        mSyncFolderPath = String.Empty

        mSvnUpdate = False
        mGitUpdate = False
        mHgUpdate = False

        mCommitUpdates = False

        Try
            blnProceed = False
            If objParseCommandLine.ParseCommandLine Then
                If SetOptionsUsingCommandLineParameters(objParseCommandLine) Then blnProceed = True
            End If

            If Not blnProceed OrElse _
               objParseCommandLine.NeedToShowHelp Then
                ShowProgramHelp()
                intReturnCode = -1
            Else

                If objParseCommandLine.ParameterCount + objParseCommandLine.NonSwitchParameterCount = 0 Then
                    ' Show the GUI

                    Dim objMain As New frmMain
                    objMain.ShowDialog()

                    Return 0
                End If

                If String.IsNullOrWhiteSpace(mServer) Then
                    ShowErrorMessage("Server must be defined using /Server")
                    Return -5
                End If


                If mDatabaseList.Count = 0 Then
                    ShowErrorMessage("Database must be defined using /DB or /DBList")
                    Return -6
                End If

                If mSync AndAlso String.IsNullOrWhiteSpace(mSyncFolderPath) Then
                    ShowErrorMessage("Sync folder must be defined when using /Sync")
                    Return -7
                End If

                mProcessingClass = New clsDBSchemaExportTool

                With mProcessingClass
                    .ShowMessages = Not mQuietMode
                    .LogMessagesToFile = mLogMessagesToFile
                    .LogFilePath = mLogFilePath
                    .LogFolderPath = mLogFolderPath

                    .AutoSelectTableDataToExport = Not mDisableAutoDataExport
                    .TableDataToExportFile = mTableDataToExportFile

                    .DatabaseSubfolderPrefix = mDatabaseSubfolderPrefix
                    .CreateFolderForEachDB = mCreateFolderForEachDB

                    .Sync = mSync
                    .SyncFolderPath = mSyncFolderPath

                    .SvnUpdate = mSvnUpdate
                    .GitUpdate = mGitUpdate
                    .HgUpdate = mHgUpdate

                    .CommitUpdates = mCommitUpdates

                End With

                blnSuccess = mProcessingClass.ProcessDatabase(mOutputFolderPath, mServer, mDatabaseList)

                If Not blnSuccess And Not mQuietMode Then
                    intReturnCode = mProcessingClass.ErrorCode
                    If intReturnCode = 0 Then
                        ShowErrorMessage("Error while processing   : Unknown error (return code is 0)")
                    Else
                        ShowErrorMessage("Error while processing   : " & mProcessingClass.GetErrorMessage())
                    End If
                End If

                Threading.Thread.Sleep(2000)

            End If

        Catch ex As Exception
            ShowErrorMessage("Error occurred in modMain->Main: " & Environment.NewLine & ex.Message)
            intReturnCode = -1
        End Try

        Return intReturnCode

    End Function

    Private Sub DisplayProgressPercent(ByVal intPercentComplete As Integer, ByVal blnAddCarriageReturn As Boolean)
        If blnAddCarriageReturn Then
            Console.WriteLine()
        End If
        If intPercentComplete > 100 Then intPercentComplete = 100
        Console.Write("Processing: " & intPercentComplete.ToString() & "% ")
        If blnAddCarriageReturn Then
            Console.WriteLine()
        End If
    End Sub

    Private Function GetAppVersion() As String
        Return clsProcessFoldersBaseClass.GetAppVersion(PROGRAM_DATE)
    End Function

    Private Function SetOptionsUsingCommandLineParameters(ByVal objParseCommandLine As clsParseCommandLine) As Boolean
        ' Returns True if no problems; otherwise, returns false

        Dim strValue As String = String.Empty
        Dim lstValidParameters As List(Of String) = New List(Of String) From {
          "O", "Server", "DB", "DBList", "FolderPrefix", "NoSubfolder", "NoAutoData", "Data",
          "Sync", "Svn", "Git", "Hg", "Commit",
          "P", "L", "LogFolder", "Q"}

        Try
            ' Make sure no invalid parameters are present
            If objParseCommandLine.InvalidParametersPresent(lstValidParameters) Then
                ShowErrorMessage("Invalid commmand line parameters",
                  (From item In objParseCommandLine.InvalidParameters(lstValidParameters) Select "/" + item).ToList())
                Return False
            Else
                With objParseCommandLine
                    ' Query objParseCommandLine to see if various parameters are present
                    If .RetrieveValueForParameter("O", strValue) Then
                        mOutputFolderPath = strValue
                    ElseIf .NonSwitchParameterCount > 0 Then
                        mOutputFolderPath = .RetrieveNonSwitchParameter(0)
                    End If

                    If .RetrieveValueForParameter("Server", strValue) Then mServer = strValue

                    If .RetrieveValueForParameter("DB", strValue) Then
                        If Not mDatabaseList.Contains(strValue) Then
                            mDatabaseList.Add(strValue)
                        End If
                    End If

                    If .RetrieveValueForParameter("DBList", strValue) Then
                        Dim lstDatabaseNames = strValue.Split(","c).ToList()

                        For Each databaseName In lstDatabaseNames
                            If Not mDatabaseList.Contains(databaseName) Then
                                mDatabaseList.Add(databaseName)
                            End If
                        Next
                    End If

                    If .RetrieveValueForParameter("FolderPrefix", strValue) Then mDatabaseSubfolderPrefix = strValue
                    If .RetrieveValueForParameter("NoSubfolder", strValue) Then mCreateFolderForEachDB = False

                    If .RetrieveValueForParameter("NoAutoData", strValue) Then mDisableAutoDataExport = True
                    If .RetrieveValueForParameter("Data", strValue) Then mTableDataToExportFile = strValue

                    If .RetrieveValueForParameter("Sync", strValue) Then
                        mSync = True
                        mSyncFolderPath = strValue
                    End If

                    If .IsParameterPresent("Svn") Then mSvnUpdate = True

                    If .IsParameterPresent("Git") Then mGitUpdate = True

                    If .IsParameterPresent("Hg") Then mHgUpdate = True

                    If .IsParameterPresent("Commit") Then mCommitUpdates = True

                    If .RetrieveValueForParameter("P", strValue) Then mParameterFilePath = strValue

                    If .RetrieveValueForParameter("L", strValue) Then
                        mLogMessagesToFile = True
                        If Not String.IsNullOrEmpty(strValue) Then
                            mLogFilePath = strValue
                        End If
                    End If

                    If .RetrieveValueForParameter("LogFolder", strValue) Then
                        mLogMessagesToFile = True
                        If Not String.IsNullOrEmpty(strValue) Then
                            mLogFolderPath = strValue
                        End If
                    End If

                    If .RetrieveValueForParameter("Q", strValue) Then mQuietMode = True
                End With

                Return True
            End If

        Catch ex As Exception
            ShowErrorMessage("Error parsing the command line parameters: " & Environment.NewLine & ex.Message)
        End Try

        Return False

    End Function


    Private Sub ShowErrorMessage(ByVal strMessage As String)
        Const strSeparator As String = "------------------------------------------------------------------------------"

        Console.WriteLine()
        Console.WriteLine(strSeparator)
        Console.WriteLine(strMessage)
        Console.WriteLine(strSeparator)
        Console.WriteLine()

        WriteToErrorStream(strMessage)
    End Sub

    Private Sub ShowErrorMessage(ByVal strTitle As String, ByVal items As IEnumerable(Of String))
        Const strSeparator As String = "------------------------------------------------------------------------------"
        Dim strMessage As String

        Console.WriteLine()
        Console.WriteLine(strSeparator)
        Console.WriteLine(strTitle)
        strMessage = strTitle & ":"

        For Each item As String In items
            Console.WriteLine("   " + item)
            strMessage &= " " & item
        Next
        Console.WriteLine(strSeparator)
        Console.WriteLine()

        WriteToErrorStream(strMessage)
    End Sub

    Private Sub ShowProgramHelp()

        Try

            Console.WriteLine("This program exports Sql Server database objects as schema files. Starting this program without any parameters will show the GUI")
            Console.WriteLine()
            Console.WriteLine("Command line syntax:" & Environment.NewLine & IO.Path.GetFileName(Reflection.Assembly.GetExecutingAssembly().Location))
            Console.WriteLine(" SchemaFileFolder /Server:ServerName")
            Console.WriteLine(" /DB:Database /DBList:CommaSeparatedDatabaseName")
            Console.WriteLine(" [/FolderPrefix:PrefixText] [/NoSubfolder]")
            Console.WriteLine(" [/Data:TableDataToExport.txt] [/NoAutoData] ")
            Console.WriteLine(" [/Sync:TargetFolderPath] [/Svn] [/Git] [/Hg] [/Commit]")
            Console.WriteLine(" [/L[:LogFilePath]] [/LogFolder:LogFolderPath]")
            Console.WriteLine()
            Console.WriteLine("SchemaFileFolder is the path to the folder where the schema files will be saved")
            Console.WriteLine("To process a single database, use /Server and /DB")
            Console.WriteLine("Use /DBList to process several databases (separate names with commas)")
            Console.WriteLine()
            Console.WriteLine("By default, a subfolder named " & clsExportDBSchema.DEFAULT_DB_OUTPUT_FOLDER_NAME_PREFIX & "DatabaseName will be created below SchemaFileFolder")
            Console.WriteLine("Customize this the prefix text using /FolderPrefix")
            Console.WriteLine()
            Console.WriteLine("Use /NoSubfolder to disable auto creating a subfolder for the database being exported.")
            Console.WriteLine("Note: subfolders will always be created if you use /DBList and specify more than one database")
            Console.WriteLine()
            Console.WriteLine("Use /Data to define a text file with table names (one name per line) for which the data should be exported. " &
              "In addition to table names defined in /Data, there are default tables which will have their data exported; disable the defaults using /NoAutoData")
            Console.WriteLine()
            Console.WriteLine("Use /Sync to copy new/changed files from the output folder to an alternative folder.  This is advantageous to prevent file timestamps from getting updated every time the schema is exported")
            Console.WriteLine()
            Console.WriteLine("Use /Svn to auto-update any new or changed files to Svn")
            Console.WriteLine("Use /Git to auto-update any new or changed files to Git")
            Console.WriteLine("Use /Hg to auto-update any new or changed files to Mercurial")
            Console.WriteLine("Use /Commit to commit any updates to the repository")
            Console.WriteLine()

            Console.WriteLine("Use /L to log messages to a file; you can optionally specify a log file name using /L:LogFilePath.")
            Console.WriteLine("Use /LogFolder to specify the folder to save the log file in. By default, the log file is created in the current working directory.")
            Console.WriteLine()
            Console.WriteLine("Program written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA) in 2014")
            Console.WriteLine("Version: " & GetAppVersion())
            Console.WriteLine()

            Console.WriteLine("E-mail: matthew.monroe@pnnl.gov or matt@alchemistmatt.com")
            Console.WriteLine("Website: http://panomics.pnnl.gov/ or http://omics.pnl.gov")
            Console.WriteLine()

            ' Delay for 750 msec in case the user double clicked this file from within Windows Explorer (or started the program via a shortcut)
            Threading.Thread.Sleep(750)

        Catch ex As Exception
            ShowErrorMessage("Error displaying the program syntax: " & ex.Message)
        End Try

    End Sub

	Private Sub ShowProgressDescriptionIfChanged(taskDescription As String)
		If Not String.Equals(taskDescription, mProgressDescription) Then
			mProgressDescription = String.Copy(taskDescription)
			Console.WriteLine(taskDescription)
		End If
	End Sub

	Private Sub WriteToErrorStream(strErrorMessage As String)
		Try
			Using swErrorStream As IO.StreamWriter = New IO.StreamWriter(Console.OpenStandardError())
				swErrorStream.WriteLine(strErrorMessage)
			End Using
		Catch ex As Exception
			' Ignore errors here
		End Try
	End Sub

	Private Sub mProcessingClass_ProgressChanged(taskDescription As String, percentComplete As Single) Handles mProcessingClass.ProgressChanged
		ShowProgressDescriptionIfChanged(taskDescription)
	End Sub

	Private Sub mProcessingClass_ProgressComplete() Handles mProcessingClass.ProgressComplete
		Console.WriteLine("Processing complete")
	End Sub

	Private Sub mProcessingClass_ProgressReset() Handles mProcessingClass.ProgressReset
		ShowProgressDescriptionIfChanged(mProcessingClass.ProgressStepDescription)
	End Sub

	Private Sub mProcessingClass_SubtaskProgressChanged(taskDescription As String, percentComplete As Single) Handles mProcessingClass.SubtaskProgressChanged

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