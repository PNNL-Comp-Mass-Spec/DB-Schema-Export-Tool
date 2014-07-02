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

#End Region

#Region "Structures"
	
#End Region

#Region "Classwide Variables"

	Protected mServer As String
	Protected mDatabase As String

	Protected mUseIntegratedAuthentication As Boolean
	Protected mUsername As String
	Protected mPassword As String

	Protected mSchemaExportOptions As clsExportDBSchema.udtSchemaExportOptionsType

	Protected mLocalErrorCode As eDBSchemaExportTool

	Protected WithEvents mDBSchemaExporter As clsExportDBSchema

#End Region

#Region "Properties"

	Public Property SchemaExportOptions As clsExportDBSchema.udtSchemaExportOptionsType

	Public Property AutoSelectTableDataToExport As Boolean

	Public Property TableDataToExportFile As String

	Public Property Sync As Boolean

	Public Property SyncFolderPath As String

	Public Property SvnCommit As Boolean

#End Region


	Public Function ExportSchema(ByVal strOutputFolderPath As String, ByVal serverName As String, ByVal databaseName As String) As Boolean
		Return ExportSchema(strOutputFolderPath, serverName, databaseName, True, "", "")
	End Function

	Public Function ExportSchema(
	  ByVal strOutputFolderPath As String,
	  ByVal serverName As String,
	  ByVal databaseName As String,
	  ByVal useIntegratedAuthentication As Boolean,
	  ByVal loginUsername As String,
	  ByVal loginPassword As String) As Boolean

		Try

			If Not Directory.Exists(strOutputFolderPath) Then
				' Try to create the missing folder
				Directory.CreateDirectory(strOutputFolderPath)
			End If


			With mSchemaExportOptions
				.OutputFolderPath = strOutputFolderPath
				.OutputFolderNamePrefix = String.Empty
				.CreateFolderForEachDB = False

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

			If AutoSelectTableDataToExport Then
				mDBSchemaExporter.TableNamesToAutoSelect = GetTableNamesToAutoExportData()
				mDBSchemaExporter.TableNameAutoSelectRegEx = GetTableRegExToAutoExportData()
			End If

			Dim lstDatabaseList = New List(Of String) From {databaseName}
			Dim lstTableNamesForDataExport = New List(Of String)

			Dim blnSuccess = mDBSchemaExporter.ScriptServerAndDBObjects(mSchemaExportOptions, lstDatabaseList, lstTableNamesForDataExport)

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

		mUseIntegratedAuthentication = True
		mUsername = String.Empty
		mPassword = String.Empty

		mSchemaExportOptions = clsExportDBSchema.GetDefaultSchemaExportOptions()

		Me.AutoSelectTableDataToExport = True

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

	Public Function ProcessDatabase(ByVal outputFolderPath As String, ByVal serverName As String, ByVal databaseName As String) As Boolean
		Dim blnSuccess As Boolean

		Try
			blnSuccess = ExportSchema(outputFolderPath, serverName, databaseName)

			If blnSuccess AndAlso Sync Then
				blnSuccess = SyncSchemaFiles(outputFolderPath, SyncFolderPath)
			End If
		Catch ex As Exception
			HandleException("Error in ProcessDatabase", ex)
			Return False
		End Try

		Return True
	End Function

	Private Function SyncSchemaFiles(ByVal outputFolderPath As String, ByVal folderPathForSync As String) As Boolean
		Throw New NotImplementedException()
	End Function
End Class
