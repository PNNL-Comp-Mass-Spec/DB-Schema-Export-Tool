Option Strict On

' This class exports the schema for one or more databases on a server
'
' -------------------------------------------------------------------------------
' Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA)
' Class started April 7, 2014
'
' E-mail: matthew.monroe@pnnl.gov or matt@alchemistmatt.com
' Website: http://panomics.pnl.gov/ or http://www.sysbio.org/resources/staff/
' -------------------------------------------------------------------------------

Imports PRISM.Files
Imports System.IO

Public Class clsDBSchemaExportTool
	Inherits clsProcessFoldersBaseClass

	Public Sub New()
		MyBase.mFileDate = "July 1, 2014"
		InitializeLocalVariables()
	End Sub

#Region "Constants and Enums"

	' Error codes specialized for this class
	Public Enum eDBSchemaExportTool As Integer
		NoError = 0
		UnspecifiedError = -1
	End Enum

	Protected Enum eDifferenceReasonType
		Unchanged = 0
		NewFile = 1
		Changed = 2
	End Enum

#End Region

#Region "Structures"

#End Region

#Region "Classwide Variables"

	Protected mServer As String
	Protected mDatabase As String

	'Protected mUseIntegratedAuthentication As Boolean
	'Protected mUsername As String
	'Protected mPassword As String

	Protected mSchemaExportOptions As clsExportDBSchema.udtSchemaExportOptionsType

	Protected mSubtaskDescription As String
	Protected mSubtaskPercentComplete As Single

	Public Event SubtaskProgressChanged(ByVal taskDescription As String, ByVal percentComplete As Single)	   ' PercentComplete ranges from 0 to 100, but can contain decimal percentage values

	Protected mLocalErrorCode As eDBSchemaExportTool

	Protected WithEvents mDBSchemaExporter As clsExportDBSchema

#End Region

#Region "Properties"

	Public Property AutoSelectTableDataToExport As Boolean

	Public Property TableDataToExportFile As String

	Public Property CreateFolderForEachDB() As Boolean

	Public Property DatabaseSubfolderPrefix() As String

	Public Property Sync As Boolean

	Public Property SyncFolderPath As String

	Public Property GitCommit As Boolean

	Public Property SvnCommit As Boolean


#End Region


	Public Function ExportSchema(
	  ByVal strOutputFolderPath As String,
	  ByVal serverName As String,
	  ByRef dctDatabaseNamesAndOutputPaths As Dictionary(Of String, String)) As Boolean

		Return ExportSchema(strOutputFolderPath, serverName, dctDatabaseNamesAndOutputPaths, True, "", "")

	End Function

	Public Function ExportSchema(
	  ByVal strOutputFolderPath As String,
	  ByVal serverName As String,
	  ByRef dctDatabaseNamesAndOutputPaths As Dictionary(Of String, String),
	  ByVal useIntegratedAuthentication As Boolean,
	  ByVal loginUsername As String,
	  ByVal loginPassword As String) As Boolean

		Try


			If Not Directory.Exists(strOutputFolderPath) Then
				' Try to create the missing folder
				ShowMessage("Creating " & strOutputFolderPath)
				Directory.CreateDirectory(strOutputFolderPath)
			End If

			With mSchemaExportOptions
				.OutputFolderPath = strOutputFolderPath

				.OutputFolderNamePrefix = Me.DatabaseSubfolderPrefix
				.CreateFolderForEachDB = Me.CreateFolderForEachDB

				If dctDatabaseNamesAndOutputPaths.Count > 0 Then
					.CreateFolderForEachDB = True
				End If

				.AutoSelectTableNamesForDataExport = Me.AutoSelectTableDataToExport

				' Note: mDBSchemaExporter & mTableNameAutoSelectRegEx will be passed to mDBSchemaExporter below

				With .ConnectionInfo
					.ServerName = serverName
					.UseIntegratedAuthentication = useIntegratedAuthentication
					.UserName = loginUsername
					.Password = loginPassword
				End With

			End With

		Catch ex As Exception
			HandleException("Error in ExportSchema configuring the options", ex)
			Exit Function
		End Try

		Try
			If mDBSchemaExporter Is Nothing Then
				mDBSchemaExporter = New clsExportDBSchema
			End If

			mDBSchemaExporter.TableNamesToAutoSelect = GetTableNamesToAutoExportData()
			mDBSchemaExporter.TableNameAutoSelectRegEx = GetTableRegExToAutoExportData()

			If Not AutoSelectTableDataToExport Then
				mDBSchemaExporter.TableNamesToAutoSelect.Clear()
				mDBSchemaExporter.TableNameAutoSelectRegEx.Clear()
			End If

			Dim lstDatabaseList = dctDatabaseNamesAndOutputPaths.Keys().ToList()
			Dim lstTableNamesForDataExport = New List(Of String)

			If Not String.IsNullOrWhiteSpace(Me.TableDataToExportFile) Then
				lstTableNamesForDataExport = LoadTableNamesForDataExport(Me.TableDataToExportFile)
			End If

			Dim blnSuccess = mDBSchemaExporter.ScriptServerAndDBObjects(mSchemaExportOptions, lstDatabaseList, lstTableNamesForDataExport)

			' Update dctDatabaseNamesAndOutputPaths with the output folder paths used

			' First make sure the output paths are blank
			' Additionally, populate a lookup table with lowercase database names as the keys and the actual name as the values
			Dim dctdatabaseNameLookup = New Dictionary(Of String, String)

			For Each databaseName In lstDatabaseList
				dctDatabaseNamesAndOutputPaths(databaseName) = String.Empty

				If Not dctdatabaseNameLookup.ContainsKey(databaseName.ToLower()) Then
					dctdatabaseNameLookup.Add(databaseName.ToLower(), databaseName)
				End If

			Next

			' Now update the paths
			For Each exportedDatabase In mDBSchemaExporter.SchemaOutputFolders
				Dim dbNameMatch As String = String.Empty

				If dctdatabaseNameLookup.TryGetValue(exportedDatabase.Key.ToLower(), dbNameMatch) Then
					dctDatabaseNamesAndOutputPaths(dbNameMatch) = exportedDatabase.Value
				End If

			Next

			Return blnSuccess

		Catch ex As Exception
			HandleException("Error in ExportSchema configuring mDBSchemaExporter", ex)
			Return False
		End Try

	End Function

	Public Overrides Function GetErrorMessage() As String
		' Returns "" if no error

		Dim strErrorMessage As String

		If MyBase.ErrorCode = clsProcessFoldersBaseClass.eProcessFoldersErrorCodes.LocalizedError Or _
		   MyBase.ErrorCode = clsProcessFoldersBaseClass.eProcessFoldersErrorCodes.NoError Then
			Select Case mLocalErrorCode
				Case eDBSchemaExportTool.NoError
					strErrorMessage = ""

				Case eDBSchemaExportTool.UnspecifiedError
					strErrorMessage = "Unspecified localized error"

				Case Else
					' This shouldn't happen
					strErrorMessage = "Unknown error state"
			End Select
		Else
			strErrorMessage = MyBase.GetBaseClassErrorMessage()
		End If

		Return strErrorMessage
	End Function

	Public Shared Function GetTableNamesToAutoExportData() As List(Of String)
		Dim lstTableNames = New List(Of String)

		lstTableNames.Add("T_Dataset_Process_State")
		lstTableNames.Add("T_Process_State")
		lstTableNames.Add("T_Event_Target")
		lstTableNames.Add("T_Process_Config")
		lstTableNames.Add("T_Process_Config_Parameters")
		lstTableNames.Add("T_Process_Step_Control")
		lstTableNames.Add("T_Process_Step_Control_States")
		lstTableNames.Add("T_Histogram_Mode_Name")
		lstTableNames.Add("T_Peak_Matching_Defaults")
		lstTableNames.Add("T_Quantitation_Defaults")
		lstTableNames.Add("T_Folder_Paths")

		Return lstTableNames

	End Function

	Public Shared Function GetTableRegExToAutoExportData() As List(Of String)
		Dim lstRegExSpecs = New List(Of String)
		lstRegExSpecs.Add(".*_?Type_?Name")
		lstRegExSpecs.Add(".*_?State_?Name")
		lstRegExSpecs.Add(".*_State")

		Return lstRegExSpecs
	End Function

	Private Sub InitializeLocalVariables()

		MyBase.ShowMessages = True
		MyBase.LogMessagesToFile = False

		mLocalErrorCode = eDBSchemaExportTool.NoError

		mServer = String.Empty
		mDatabase = String.Empty

		'mUseIntegratedAuthentication = True
		'mUsername = String.Empty
		'mPassword = String.Empty

		mSchemaExportOptions = clsExportDBSchema.GetDefaultSchemaExportOptions()

		mSubtaskDescription = String.Empty
		mSubtaskPercentComplete = 0

		Me.AutoSelectTableDataToExport = True

		Me.TableDataToExportFile = String.Empty

		Me.CreateFolderForEachDB = True

		Me.DatabaseSubfolderPrefix = clsExportDBSchema.DEFAULT_DB_OUTPUT_FOLDER_NAME_PREFIX

		Me.Sync = False

		Me.SyncFolderPath = String.Empty

		Me.GitCommit = False

		Me.SvnCommit = False

	End Sub

	Private Function LoadParameterFileSettings(ByVal strParameterFilePath As String) As Boolean

		Const OPTIONS_SECTION As String = "DBSchemaExportTool"

		Dim objSettingsFile As New XmlSettingsFileAccessor
		Dim strValue As String

		Try

			If strParameterFilePath Is Nothing OrElse strParameterFilePath.Length = 0 Then
				' No parameter file specified; nothing to load
				Return True
			End If

			If Not File.Exists(strParameterFilePath) Then
				' See if strParameterFilePath points to a file in the same directory as the application
				strParameterFilePath = Path.Combine(Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location), Path.GetFileName(strParameterFilePath))
				If Not File.Exists(strParameterFilePath) Then
					MyBase.SetBaseClassErrorCode(clsProcessFoldersBaseClass.eProcessFoldersErrorCodes.ParameterFileNotFound)
					Return False
				End If
			End If

			If objSettingsFile.LoadSettings(strParameterFilePath) Then
				If Not objSettingsFile.SectionPresent(OPTIONS_SECTION) Then
					ShowErrorMessage("The node '<section name=""" & OPTIONS_SECTION & """> was not found in the parameter file: " & strParameterFilePath)
					MyBase.SetBaseClassErrorCode(clsProcessFoldersBaseClass.eProcessFoldersErrorCodes.InvalidParameterFile)
					Return False
				Else
					If objSettingsFile.GetParam(OPTIONS_SECTION, "LogMessages", False) Then
						MyBase.LogMessagesToFile = True
					End If

					strValue = objSettingsFile.GetParam(OPTIONS_SECTION, "LogFolder", String.Empty)
					If Not String.IsNullOrEmpty(strValue) Then
						mLogFolderPath = strValue
					End If


				End If
			End If

		Catch ex As Exception
			HandleException("Error in LoadParameterFileSettings", ex)
			Return False
		End Try

		Return True

	End Function


	Private Function LoadTableNamesForDataExport(ByVal tableDataFilePath As String) As List(Of String)

		Dim lstTableNames = New SortedSet(Of String)

		Try
			If String.IsNullOrWhiteSpace(tableDataFilePath) Then
				Return lstTableNames.ToList()
			End If

			Dim fiDatafile = New FileInfo(tableDataFilePath)

			If Not fiDatafile.Exists Then
				Console.WriteLine()
				ShowMessage("Table Data File not found; default tables will be used")
				ShowErrorMessage("File not found: " & fiDatafile.FullName)
				Return lstTableNames.ToList()
			End If

			Using srReader = New StreamReader(New FileStream(fiDatafile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
				While srReader.Peek > -1
					Dim strLineIn = srReader.ReadLine()

					If Not String.IsNullOrWhiteSpace(strLineIn) Then
						If Not lstTableNames.Contains(strLineIn) Then
							lstTableNames.Add(strLineIn)
						End If
					End If

				End While
			End Using


		Catch ex As Exception
			HandleException("Error in LoadTableNamesForDataExport", ex)
		End Try

		Return lstTableNames.ToList()

	End Function

	Public Overloads Overrides Function ProcessFolder(ByVal strInputFolderPath As String, _
	  ByVal strOutputFolderAlternatePath As String, _
	  ByVal strParameterFilePath As String, _
	  ByVal blnResetErrorCode As Boolean) As Boolean
		' Returns True if success, False if failure

		' Assume the input folder points to the target folder where the database schema files will be created

		Return ProcessDatabase(strInputFolderPath, mServer, mDatabase)

	End Function

	Public Shadows Function ProcessAndRecurseFolders(ByVal strInputFolderPath As String) As Boolean
		Return ProcessAndRecurseFolders(strInputFolderPath, String.Empty)
	End Function

	Public Shadows Function ProcessAndRecurseFolders(ByVal strInputFolderPath As String, ByVal intRecurseFoldersMaxLevels As Integer) As Boolean
		Return ProcessAndRecurseFolders(strInputFolderPath, String.Empty, String.Empty, intRecurseFoldersMaxLevels)
	End Function

	Public Shadows Function ProcessAndRecurseFolders(ByVal strInputFolderPath As String, ByVal strOutputFolderAlternatePath As String) As Boolean
		Return ProcessAndRecurseFolders(strInputFolderPath, strOutputFolderAlternatePath, String.Empty)
	End Function

	Public Shadows Function ProcessAndRecurseFolders(ByVal strInputFolderPath As String, ByVal strOutputFolderAlternatePath As String, ByVal strParameterFilePath As String) As Boolean
		Return ProcessAndRecurseFolders(strInputFolderPath, strOutputFolderAlternatePath, strParameterFilePath, 0)
	End Function

	Public Shadows Function ProcessAndRecurseFolders(ByVal strInputFolderPath As String, _
	 ByVal strOutputFolderAlternatePath As String, _
	 ByVal strParameterFilePath As String, _
	 ByVal intRecurseFoldersMaxLevels As Integer) As Boolean
		' Returns True if success, False if failure

		Return ProcessFolder(strInputFolderPath, strOutputFolderAlternatePath, strParameterFilePath, True)

	End Function

	Public Function ProcessDatabase(ByVal outputFolderPath As String, ByVal serverName As String, ByVal databaseList As IEnumerable(Of String)) As Boolean
		Dim blnSuccess As Boolean

		Try
			' Keys in this dictionary are database names
			' Values are the output folder path used

			Dim dctDatabaseNamesAndOutputPaths = New Dictionary(Of String, String)
			For Each databaseName In databaseList
				dctDatabaseNamesAndOutputPaths.Add(databaseName, String.Empty)
			Next

			blnSuccess = ExportSchema(outputFolderPath, serverName, dctDatabaseNamesAndOutputPaths)

			If blnSuccess AndAlso Sync Then
				blnSuccess = SyncSchemaFiles(dctDatabaseNamesAndOutputPaths, SyncFolderPath)
			End If
		Catch ex As Exception
			HandleException("Error in ProcessDatabase", ex)
			Return False
		End Try

		Return True
	End Function

	Public Function ProcessDatabase(ByVal outputFolderPath As String, ByVal serverName As String, ByVal databaseName As String) As Boolean

		Dim databaseList As New List(Of String) From {databaseName}

		Return ProcessDatabase(outputFolderPath, serverName, databaseList)

	End Function

	Private Function SyncSchemaFiles(ByVal dctDatabaseNamesAndOutputPaths As Dictionary(Of String, String), ByVal folderPathForSync As String) As Boolean
		Try
			ResetProgress("Synchronizing with " & folderPathForSync)

			Dim intDBsProcessed As Integer = 0

			For Each dbEntry In dctDatabaseNamesAndOutputPaths

				If String.IsNullOrWhiteSpace(dbEntry.Value) Then
					ShowErrorMessage("Schema output folder was not reported for " & dbEntry.Key & "; unable to synchronize")
					Continue For
				End If

				Dim percentComplete As Single = intDBsProcessed / CSng(dctDatabaseNamesAndOutputPaths.Count) * 100

				UpdateProgress("Synchronizing database " & dbEntry.Key, percentComplete)

				Dim diSourceFolder = New DirectoryInfo(dbEntry.Value)

				Dim targetFolderPath = String.Copy(folderPathForSync)
				If dctDatabaseNamesAndOutputPaths.Count > 1 OrElse CreateFolderForEachDB Then
					targetFolderPath = Path.Combine(targetFolderPath, diSourceFolder.Name)
				End If

				Dim diTargetFolder = New DirectoryInfo(targetFolderPath)

				If Not diSourceFolder.Exists Then
					ShowErrorMessage("Source folder not found; cannot synchronize: " & diSourceFolder.FullName)
					Return False
				End If

				If Not diTargetFolder.Exists Then
					ShowMessage("Creating target folder for synchronization: " & diTargetFolder.FullName)
					diTargetFolder.Create()
				End If

				If diSourceFolder.FullName = diTargetFolder.FullName Then
					ShowErrorMessage("Sync folder is identical to the output SchemaFileFolder; cannot synchronize")
					Return False
				End If

				Dim intFilesProcessed As Integer = 0

				Dim lstfilesToCopy = diSourceFolder.GetFiles()

				For Each fiFile As FileInfo In lstFilesToCopy
					Dim fiTargetFile = New FileInfo(Path.Combine(diTargetFolder.FullName, fiFile.Name))
					Dim eDifferenceReason As eDifferenceReasonType

					If FilesDiffer(fiFile, fiTargetFile, eDifferenceReason) Then

						Dim subtaskPercentComplete As Single = intFilesProcessed / CSng(lstfilesToCopy.Count) * 100

						Select Case eDifferenceReason
							Case eDifferenceReasonType.NewFile
								UpdateSubtaskProgress("  Copying new file " & fiFile.Name, subtaskPercentComplete)
							Case eDifferenceReasonType.Changed
								UpdateSubtaskProgress("  Copying changed file " & fiFile.Name, subtaskPercentComplete)
							Case Else
								UpdateSubtaskProgress("  Copying file " & fiFile.Name, subtaskPercentComplete)
						End Select

						fiFile.CopyTo(fiTargetFile.FullName, True)
					End If

					intFilesProcessed += 1
				Next

				If Me.GitCommit Then
					'ToDo: Implement this: CommitChangesWithGit(folderPathForSync)
				End If

				If Me.SvnCommit Then
					'ToDo: Implement this: CommitChangesWithSvn(folderPathForSync)
				End If

				intDBsProcessed += 1
			Next

			Return True

		Catch ex As Exception
			HandleException("Error in SyncSchemaFiles", ex)
			Return False
		End Try

	End Function

	Private Function FilesDiffer(ByVal fiBase As FileInfo, ByVal fiComparison As FileInfo, ByRef eDifferenceReason As eDifferenceReasonType) As Boolean
		Try
			eDifferenceReason = eDifferenceReasonType.Unchanged

			If Not fiBase.Exists Then Return False

			If Not fiComparison.Exists Then
				eDifferenceReason = eDifferenceReasonType.NewFile
				Return True
			End If

			If fiBase.Length <> fiComparison.Length Then Return True

			' Perform a line-by-line comparison

			Using srBaseFile = New StreamReader(New FileStream(fiBase.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

				Using srComparisonFile = New StreamReader(New FileStream(fiComparison.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

					While srBaseFile.Peek > -1
						Dim strLineIn = srBaseFile.ReadLine()

						If srComparisonFile.Peek > -1 Then
							Dim strComparisonLine = srComparisonFile.ReadLine()

							If String.Compare(strLineIn, strComparisonLine) <> 0 Then
								' Difference found								
								eDifferenceReason = eDifferenceReasonType.Changed
								Return True
							End If
						End If

					End While
				End Using
			End Using

			Return False

		Catch ex As Exception
			HandleException("Error in FilesDiffer", ex)
			Return True
		End Try

	End Function

	Protected Sub UpdateSubtaskProgress(ByVal taskDescription As String, ByVal percentComplete As Single)
		Dim blnDescriptionChanged = Not String.Equals(taskDescription, mSubtaskDescription)

		mSubtaskDescription = String.Copy(taskDescription)
		If percentComplete < 0 Then
			percentComplete = 0
		ElseIf percentComplete > 100 Then
			percentComplete = 100
		End If
		mSubtaskPercentComplete = percentComplete

		If blnDescriptionChanged Then
			If mSubtaskPercentComplete < Single.Epsilon Then
				LogMessage(mSubtaskDescription.Replace(Environment.NewLine, "; "))
			Else
				LogMessage(mSubtaskDescription & " (" & mSubtaskPercentComplete.ToString("0.0") & "% complete)".Replace(Environment.NewLine, "; "))
			End If
		End If

		RaiseEvent SubtaskProgressChanged(taskDescription, percentComplete)

	End Sub

#Region "Event Handlers"

	Private Sub mDBSchemaExporter_ProgressChanged(taskDescription As String, percentComplete As Single) Handles mDBSchemaExporter.ProgressChanged
		UpdateProgress(taskDescription, percentComplete)
	End Sub

	Private Sub mDBSchemaExporter_ProgressComplete() Handles mDBSchemaExporter.ProgressComplete
		OperationComplete()
	End Sub

	Private Sub mDBSchemaExporter_ProgressReset() Handles mDBSchemaExporter.ProgressReset
		ShowMessage(mDBSchemaExporter.ProgressStepDescription)
	End Sub

	Private Sub mDBSchemaExporter_SubtaskProgressChanged(taskDescription As String, percentComplete As Single) Handles mDBSchemaExporter.SubtaskProgressChanged
		UpdateSubtaskProgress(taskDescription, percentComplete)
	End Sub

	Private Sub mDBSchemaExporter_SubtaskProgressReset() Handles mDBSchemaExporter.SubtaskProgressReset
		ShowMessage("  " & mDBSchemaExporter.SubtaskProgressStepDescription)
	End Sub

#End Region

End Class
