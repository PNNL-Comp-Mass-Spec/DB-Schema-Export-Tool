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
	Public Const PROGRAM_DATE As String = "May 21, 2014"

	Private mOutputFolderPath As String

	Private mParameterFilePath As String

	Private mRecurseFolders As Boolean							' Not used in this app
	Private mRecurseFoldersMaxLevels As Integer					' Not used in this app

	Private mServer As String
	Private mDatabase As String

	Private mAutoSelectTableDataToExport As Boolean			' Set to True to auto-select tables for which data should be exported
	Private mTableDataToExportFile As String				' File with table names for which data should be exported
	
	Private mSync As Boolean
	Private mSyncFolderPath As String
	Private mSvnCommit As Boolean

	Private mLogMessagesToFile As Boolean
	Private mLogFilePath As String = String.Empty
	Private mLogFolderPath As String = String.Empty

	Private mQuietMode As Boolean

	Private WithEvents mProcessingClass As clsDBSchemaExportTool
	Private mLastProgressReportTime As System.DateTime
	Private mLastProgressReportValue As Integer

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

		mRecurseFolders = False					' Not used in this app    
		mRecurseFoldersMaxLevels = 0			' Not used in this app    

		mServer = String.Empty
		mDatabase = String.Empty

		mAutoSelectTableDataToExport = True
		mTableDataToExportFile = String.Empty

		mSync = False
		mSyncFolderPath = String.Empty
		mSvnCommit = False

		Try
			blnProceed = False
			If objParseCommandLine.ParseCommandLine Then
				If SetOptionsUsingCommandLineParameters(objParseCommandLine) Then blnProceed = True
			End If

			If objParseCommandLine.ParameterCount + objParseCommandLine.NonSwitchParameterCount = 0 Then
				' Show the GUI

				' This could be used to show frmMain
				' Instead, frmMain has been set as the startup object

				Dim objMain As New frmMain
				objMain.ShowDialog()

				Return 0
			End If

			If Not blnProceed OrElse _
			   objParseCommandLine.NeedToShowHelp Then
				ShowProgramHelp()
				intReturnCode = -1
			Else

				mProcessingClass = New clsDBSchemaExportTool

				With mProcessingClass
					.ShowMessages = Not mQuietMode
					.LogMessagesToFile = mLogMessagesToFile
					.LogFilePath = mLogFilePath
					.LogFolderPath = mLogFolderPath

					.AutoSelectTableDataToExport = mAutoSelectTableDataToExport
					.TableDataToExportFile = mTableDataToExportFile

					.Sync = mSync
					.SyncFolderPath = mSyncFolderPath

					.SvnCommit = mSvnCommit
				End With

				blnSuccess = mProcessingClass.ProcessDatabase(mOutputFolderPath, mServer, mDatabase)

				If Not blnSuccess And Not mQuietMode Then
					intReturnCode = mProcessingClass.ErrorCode
					If intReturnCode = 0 Then
						ShowErrorMessage("Error while processing   : Unknown error (return code is 0)")
					Else
						ShowErrorMessage("Error while processing   : " & mProcessingClass.GetErrorMessage())
					End If
				End If

			End If

		Catch ex As Exception
			ShowErrorMessage("Error occurred in modMain->Main: " & System.Environment.NewLine & ex.Message)
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
		Dim lstValidParameters As Generic.List(Of String) = New Generic.List(Of String) From {
		  "O", "Server", "DB", "AutoData", "DataTableFile", "Sync", "SvnCommit",
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
					If .RetrieveValueForParameter("DB", strValue) Then mDatabase = strValue

					If .RetrieveValueForParameter("AutoData", strValue) Then mAutoSelectTableDataToExport = True
					If .RetrieveValueForParameter("DataTableFile", strValue) Then mTableDataToExportFile = strValue

					If .RetrieveValueForParameter("Sync", strValue) Then
						mSync = True
						mSyncFolderPath = strValue
					End If

					If .IsParameterPresent("SvnCommit") Then mSvnCommit = True

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
			ShowErrorMessage("Error parsing the command line parameters: " & System.Environment.NewLine & ex.Message)
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

			Console.WriteLine("This program exports database objects as schema files. Starting this program without any parameters will show the GUI")
			Console.WriteLine()
			Console.WriteLine("Command line syntax:" & Environment.NewLine & IO.Path.GetFileName(Reflection.Assembly.GetExecutingAssembly().Location))
			Console.WriteLine(" SchemaFileFolder [/Server:ServerName] [/DB:Database]")
			Console.WriteLine(" [/AutoData] [/DataTableFile:TableDataToExport.txt]")
			Console.WriteLine(" [/Sync:TargetFolderPath] [/SvnCommit]")

			Console.WriteLine(" [/L[:LogFilePath]] [/LogFolder:LogFolderPath]")
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

	Private Sub WriteToErrorStream(strErrorMessage As String)
		Try
			Using swErrorStream As System.IO.StreamWriter = New System.IO.StreamWriter(Console.OpenStandardError())
				swErrorStream.WriteLine(strErrorMessage)
			End Using
		Catch ex As Exception
			' Ignore errors here
		End Try
	End Sub

	Private Sub mProcessingClass_ProgressChanged(ByVal taskDescription As String, ByVal percentComplete As Single) Handles mProcessingClass.ProgressChanged
		Const PERCENT_REPORT_INTERVAL As Integer = 25
		Const PROGRESS_DOT_INTERVAL_MSEC As Integer = 250

		If percentComplete >= mLastProgressReportValue Then
			If mLastProgressReportValue > 0 Then
				Console.WriteLine()
			End If
			DisplayProgressPercent(mLastProgressReportValue, False)
			mLastProgressReportValue += PERCENT_REPORT_INTERVAL
			mLastProgressReportTime = DateTime.UtcNow
		Else
			If DateTime.UtcNow.Subtract(mLastProgressReportTime).TotalMilliseconds > PROGRESS_DOT_INTERVAL_MSEC Then
				mLastProgressReportTime = DateTime.UtcNow
				Console.Write(".")
			End If
		End If
	End Sub

	Private Sub mProcessingClass_ProgressReset() Handles mProcessingClass.ProgressReset
		mLastProgressReportTime = DateTime.UtcNow
		mLastProgressReportValue = 0
	End Sub

	Private Sub mProcessingClass_ErrorEvent(ByVal strMessage As String) Handles mProcessingClass.ErrorEvent
		' LogMessageNow(strSessage, eError)
	End Sub

	Private Sub mProcessingClass_MessageEvent(ByVal strMessage As String) Handles mProcessingClass.MessageEvent
		' If m_DebugLevel >= 2 Then
		'    LogMessageNow(strSessage, eNormal)
		' End If
	End Sub

	Private Sub mProcessingClass_WarningEvent(ByVal strMessage As String) Handles mProcessingClass.WarningEvent
		' LogMessageNow(strSessage, eWarning)
	End Sub
End Module