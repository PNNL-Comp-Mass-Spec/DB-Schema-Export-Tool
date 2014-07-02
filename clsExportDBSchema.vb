Option Strict On

' This class will export all of the specified object types 
' from a specific Sql Server database
'
' -------------------------------------------------------------------------------
' Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA)
' Program started August 11, 2006
' Copyright 2006, Battelle Memorial Institute.  All Rights Reserved.

' E-mail: matthew.monroe@pnnl.gov or matt@alchemistmatt.com
' Website: http://panomics.pnl.gov/ or http://www.sysbio.org/resources/staff/
' -------------------------------------------------------------------------------
'
' Licensed under the Apache License, Version 2.0; you may not use this file except
' in compliance with the License.  You may obtain a copy of the License at 
' http://www.apache.org/licenses/LICENSE-2.0
'
' Notice: This computer software was prepared by Battelle Memorial Institute, 
' hereinafter the Contractor, under Contract No. DE-AC05-76RL0 1830 with the 
' Department of Energy (DOE).  All rights in the computer software are reserved 
' by DOE on behalf of the United States Government and the Contractor as 
' provided in the Contract.  NEITHER THE GOVERNMENT NOR THE CONTRACTOR MAKES ANY 
' WARRANTY, EXPRESS OR IMPLIED, OR ASSUMES ANY LIABILITY FOR THE USE OF THIS 
' SOFTWARE.  This notice including this sentence must appear on any copies of 
' this computer software.
'
' Last updated May 21, 2014

Imports System.IO
Imports Microsoft.SqlServer.Management.Common
Imports Microsoft.SqlServer.Management.Smo
Imports System.Text.RegularExpressions
Imports System.Collections.Specialized
Imports System.Threading
Imports System.Text
Imports SharedVBNetRoutines

Public Class clsExportDBSchema

	Public Sub New()
		InitializeLocalVariables(True)
	End Sub

	Public Sub New(ByVal udtServerConnectionInfo As udtServerConnectionInfoType)
		Me.New()
		ConnectToServer(udtServerConnectionInfo)
	End Sub

#Region "Constants and Enums"
	' SQL Server Login Info
	Public Const SQL_SERVER_NAME_DEFAULT As String = "Albert"
	Public Const SQL_SERVER_USERNAME_DEFAULT As String = "mtuser"
	Public Const SQL_SERVER_PASSWORD_DEFAULT As String = "mt4fun"

	Public Const DEFAULT_DB_OUTPUT_FOLDER_NAME_PREFIX As String = "DBSchema__"
	Public Const DEFAULT_SERVER_OUTPUT_FOLDER_NAME_PREFIX As String = "ServerSchema__"

	' Note: this value defines the maximum number of data rows that will be exported 
	' from tables that are auto-added to the table list for data export
	Public Const DATA_ROW_COUNT_WARNING_THRESHOLD As Integer = 1000

	Private Const COMMENT_START_TEXT As String = "/****** "
	Private Const COMMENT_END_TEXT As String = " ******/"
	Private Const COMMENT_END_TEXT_SHORT As String = "*/"
	Private Const COMMENT_SCRIPT_DATE_TEXT As String = "Script Date: "

	Public Enum eDBSchemaExportErrorCodes
		NoError = 0
		GeneralError = 1
		ConfigurationError = 2
		DatabaseConnectionError = 3
		OutputFolderAccessError = 4
	End Enum

	Public Enum ePauseStatusConstants
		Unpaused = 0
		PauseRequested = 1
		Paused = 2
		UnpauseRequested = 3
	End Enum

	Public Enum eMessageTypeConstants As Short
		Normal = 0
		HeaderLine = 1
		ErrorMessage = 2
	End Enum

	Public Enum eSchemaObjectTypeConstants
		SchemasAndRoles = 0
		Tables = 1
		Views = 2
		StoredProcedures = 3
		UserDefinedFunctions = 4
		UserDefinedDataTypes = 5
		UserDefinedTypes = 6
	End Enum

	Public Enum eDataColumnTypeConstants
		Numeric = 0
		Text = 1
		DateTime = 2
		BinaryArray = 3
		BinaryByte = 4
		GUID = 5
		SqlVariant = 6
		ImageObject = 7
		GeneralObject = 8
	End Enum

	' Note: Currenly only SqlServer is supported
	Public Enum eTargetDatabaseTypeConstants
		SqlServer = 0
		MySql = 1
		Postgres = 2
		SqlLite = 3
	End Enum

#End Region

#Region "Structures"
	Public Structure udtSchemaExportOptionsType
		Public OutputFolderPath As String
		Public OutputFolderNamePrefix As String
		Public CreateFolderForEachDB As Boolean
		Public IncludeSystemObjects As Boolean
		Public IncludeTimestampInScriptFileHeader As Boolean

		Public ExportServerSettingsLoginsAndJobs As Boolean
		Public ServerOutputFolderNamePrefix As String

		Public SaveDataAsInsertIntoStatements As Boolean
		Public DatabaseTypeForInsertInto As eTargetDatabaseTypeConstants
		Public AutoSelectTableNamesForDataExport As Boolean

		Public ExportDBSchemasAndRoles As Boolean
		Public ExportTables As Boolean
		Public ExportViews As Boolean
		Public ExportStoredProcedures As Boolean
		Public ExportUserDefinedFunctions As Boolean
		Public ExportUserDefinedDataTypes As Boolean
		Public ExportUserDefinedTypes As Boolean							   ' Only supported in Sql Server 2005 or newer; see SqlServer2005OrNewer

		Public ConnectionInfo As udtServerConnectionInfoType
	End Structure

	Public Structure udtServerConnectionInfoType
		Public ServerName As String
		Public UserName As String
		Public Password As String
		Public UseIntegratedAuthentication As Boolean
	End Structure

	Private Structure udtServerConnectionSingleType
		Public Connected As Boolean
		Public ConnectionInfo As udtServerConnectionInfoType
	End Structure

	Private Structure udtDBExportWorkingParamsType
		Public ProcessCount As Integer
		Public ProcessCountExpected As Integer
		Public OutputFolderPathCurrentDB As String
		Public CountObjectsOnly As Boolean
	End Structure
#End Region

#Region "Classwide Variables"
	Public Event NewMessage(ByVal strMessage As String, ByVal eMessageType As eMessageTypeConstants)
	Public Event DBExportStarting(ByVal strDatabaseName As String)
	Public Event PauseStatusChange()

	Private mSqlServer As Server
	Private mSqlServerOptionsCurrent As udtServerConnectionSingleType

	Private mColumnCharNonStandardRegEx As Regex
	Private mNonStandardOSChars As Regex

	Protected mTableNamesToAutoSelect As List(Of String)

	' Note: Must contain valid RegEx statements (tested case-insensitive)
	Protected mTableNameAutoSelectRegEx As List(Of String)

	' Keys in the dictionary are DatabaseName
	' Values are the output folder path that was used
	Protected mSchemaOutputFolders As Dictionary(Of String, String)

	Private mErrorCode As eDBSchemaExportErrorCodes
	Private mStatusMessage As String

	Private mAbortProcessing As Boolean
	Private mPauseStatus As ePauseStatusConstants
#End Region

#Region "Progress Events and Variables"
	Public Event ProgressReset()
	Public Event ProgressChanged(ByVal taskDescription As String, ByVal percentComplete As Single)	   ' PercentComplete ranges from 0 to 100, but can contain decimal percentage values
	Public Event ProgressComplete()

	Public Event SubtaskProgressReset()
	Public Event SubtaskProgressChanged(ByVal taskDescription As String, ByVal percentComplete As Single)	  ' PercentComplete ranges from 0 to 100, but can contain decimal percentage values
	Public Event SubtaskProgressComplete()

	Protected mProgressStepDescription As String = String.Empty
	Protected mProgressPercentComplete As Single				' Ranges from 0 to 100, but can contain decimal percentage values
	Protected mProgressStep As Integer
	Protected mProgressStepCount As Integer

	Protected mSubtaskProgressStepDescription As String = String.Empty
	Protected mSubtaskProgressPercentComplete As Single			' Ranges from 0 to 100, but can contain decimal percentage values
#End Region

#Region "Properties"

	Public Property TableNamesToAutoSelect() As List(Of String)
		Get
			Return mTableNamesToAutoSelect
		End Get
		Set(ByVal value As List(Of String))
			mTableNamesToAutoSelect = value
		End Set
	End Property

	Public Property TableNameAutoSelectRegEx() As List(Of String)
		Get
			Return mTableNameAutoSelectRegEx
		End Get
		Set(ByVal value As List(Of String))
			mTableNameAutoSelectRegEx = value
		End Set
	End Property

	Public ReadOnly Property ConnectedToServer() As Boolean
		Get
			Return mSqlServerOptionsCurrent.Connected
		End Get
	End Property

	Public ReadOnly Property ErrorCode() As eDBSchemaExportErrorCodes
		Get
			Return mErrorCode
		End Get
	End Property

	Public ReadOnly Property PauseStatus() As ePauseStatusConstants
		Get
			Return mPauseStatus
		End Get
	End Property

	Public ReadOnly Property SchemaOutputFolders() As Dictionary(Of String, String)
		Get
			Return mSchemaOutputFolders
		End Get
	End Property

	Public ReadOnly Property StatusMessage() As String
		Get
			If mStatusMessage Is Nothing Then
				Return String.Empty
			Else
				Return mStatusMessage
			End If
		End Get
	End Property

	Public ReadOnly Property ProgressStep() As Integer
		Get
			Return mProgressStep
		End Get
	End Property

	Public ReadOnly Property ProgressStepCount() As Integer
		Get
			Return mProgressStepCount
		End Get
	End Property

	Public ReadOnly Property ProgressStepDescription() As String
		Get
			Return mProgressStepDescription
		End Get
	End Property

	' ProgressPercentComplete ranges from 0 to 100, but can contain decimal percentage values
	Public ReadOnly Property ProgressPercentComplete() As Single
		Get
			Return CType(Math.Round(mProgressPercentComplete, 2), Single)
		End Get
	End Property

	Public ReadOnly Property SubtaskProgressStepDescription() As String
		Get
			Return mSubtaskProgressStepDescription
		End Get
	End Property

	' SubtaskProgressPercentComplete ranges from 0 to 100, but can contain decimal percentage values
	Public ReadOnly Property SubtaskProgressPercentComplete() As Single
		Get
			Return CType(Math.Round(mSubtaskProgressPercentComplete, 2), Single)
		End Get
	End Property

#End Region

	Public Sub AbortProcessingNow()
		mAbortProcessing = True
		Me.RequestUnpause()
	End Sub

	Private Function AutoSelectTableNamesForDataExport(
	  ByVal objDatabase As Database,
	  ByVal lstTableNamesForDataExport As IEnumerable(Of String)) As Dictionary(Of String, Int64)

		Try
			ResetSubtaskProgress("Auto-selecting tables to export data from")

			' Stores the table names and maximum number of data rows to export (0 means all rows)
			Dim dctTableNames = New Dictionary(Of String, Int64)(System.StringComparison.CurrentCultureIgnoreCase)

			' Copy the table names from lstTableNamesForDataExport to dctTablesNames
			' Store 0 for the hash value since we want to export all of the data rows from the tables in lstTableNamesForDataExport
			' Simultaneously, populate intMaximumDataRowsToExport
			If Not lstTableNamesForDataExport Is Nothing Then
				For Each tableName In lstTableNamesForDataExport
					dctTableNames.Add(tableName, 0)
				Next
			End If

			' Copy the table names from mTableNamesToAutoSelect to dctTablesNames (if not yet present)
			If Not mTableNamesToAutoSelect Is Nothing Then
				For Each tableName In mTableNamesToAutoSelect
					If Not dctTableNames.ContainsKey(tableName) Then
						dctTableNames.Add(tableName, DATA_ROW_COUNT_WARNING_THRESHOLD)
					End If
				Next
			End If

			' Initialize lstRegEx (we'll fill it below if blnAutoHiglightRows = True)
			Const objRegExOptions As RegexOptions = RegexOptions.Compiled Or _
			 RegexOptions.IgnoreCase Or _
			 RegexOptions.Singleline

			If Not mTableNameAutoSelectRegEx Is Nothing Then
				Dim lstRegExSpecs = New List(Of Regex)

				For Each regexItem In mTableNameAutoSelectRegEx
					lstRegExSpecs.Add(New Regex(regexItem, objRegExOptions))
				Next

				' Step through the table names for this DB and compare to the RegEx values
				Dim dtTables = objDatabase.EnumObjects(DatabaseObjectTypes.Table, SortOrder.Name)

				For Each objRow As DataRow In dtTables.Rows
					Dim strTableName = objRow.Item("Name").ToString

					For Each regexMatcher In lstRegExSpecs
						If regexMatcher.Match(strTableName).Success Then
							If Not dctTableNames.ContainsKey(strTableName) Then
								dctTableNames.Add(strTableName, DATA_ROW_COUNT_WARNING_THRESHOLD)
							End If
							Exit For
						End If
					Next
				Next objRow
			End If

			Return dctTableNames

		Catch ex As Exception
			SetLocalError(eDBSchemaExportErrorCodes.ConfigurationError, "Error in AutoSelectTableNamesForDataExport")
			Return New Dictionary(Of String, Int64)
		End Try

	End Function

	Private Sub CheckPauseStatus()
		If mPauseStatus = ePauseStatusConstants.PauseRequested Then
			SetPauseStatus(ePauseStatusConstants.Paused)
		End If

		Do While mPauseStatus = ePauseStatusConstants.Paused And Not mAbortProcessing
			Thread.Sleep(150)
		Loop

		SetPauseStatus(ePauseStatusConstants.Unpaused)
	End Sub

	Private Function CleanNameForOS(ByVal strName As String) As String
		' Replace any invalid characters in strName with underscores

		Return mNonStandardOSChars.Replace(strName, "_")
	End Function

	Private Function CleanSqlScript(ByVal lstInfo As IEnumerable(Of String), ByVal udtSchemaExportOptions As udtSchemaExportOptionsType) As List(Of String)
		' Calls CleanSqlScript with blnRemoveAllOccurrences = False and blnRemoveDuplicateHeaderLine = FAlse
		Return CleanSqlScript(lstInfo, udtSchemaExportOptions, False, False)
	End Function

	Private Function CleanSqlScript(ByVal lstInfo As IEnumerable(Of String), ByVal udtSchemaExportOptions As udtSchemaExportOptionsType, ByVal blnRemoveAllScriptDateOccurrences As Boolean, ByVal blnRemoveDuplicateHeaderLine As Boolean) As List(Of String)

		Dim chWhiteSpaceChars() As Char = New Char() {" "c, ControlChars.Tab}

		Dim lstInfoClean = New List(Of String)

		Try
			If Not udtSchemaExportOptions.IncludeTimestampInScriptFileHeader Then
				' Look for and remove the timestamp from the first line of the Sql script
				' For example: "Script Date: 08/14/2006 20:14:31" prior to each "******/"
				'
				' If blnRemoveAllOccurrences = True, then searches for all occurrences
				' If blnRemoveAllOccurrences = False, then does not look past the first 
				'   carriage return of each entry in lstInfo

				For Each strText In lstInfo

					Dim intIndexStart = 0
					Dim intFinalSearchIndex As Integer

					If blnRemoveAllScriptDateOccurrences Then
						intFinalSearchIndex = strText.Length - 1
					Else
						' Find the first CrLf after the first non-blank line in strText
						' However, if the script starts with several SET statements then we need to skip those lines
						Dim objectCommentStartIndex As Integer = strText.IndexOf(COMMENT_START_TEXT & "Object:", System.StringComparison.Ordinal)
						If strText.Trim().StartsWith("SET") AndAlso objectCommentStartIndex > 0 Then
							intIndexStart = objectCommentStartIndex
						End If

						Do
							intFinalSearchIndex = strText.IndexOf(ControlChars.NewLine, intIndexStart, System.StringComparison.Ordinal)
							If intFinalSearchIndex = intIndexStart Then
								intIndexStart += 2
							Else
								Exit Do
							End If
						Loop While intFinalSearchIndex >= 0 AndAlso intFinalSearchIndex < intIndexStart AndAlso intIndexStart < strText.Length

						If intFinalSearchIndex < 0 Then intFinalSearchIndex = strText.Length - 1
					End If

					Dim intIndexStartCurrent As Integer

					Do
						intIndexStartCurrent = strText.IndexOf(COMMENT_SCRIPT_DATE_TEXT, intIndexStart, System.StringComparison.Ordinal)
						If intIndexStartCurrent > 0 AndAlso intIndexStartCurrent <= intFinalSearchIndex Then
							Dim intIndexEndCurrent = strText.IndexOf(COMMENT_END_TEXT_SHORT, intIndexStartCurrent, System.StringComparison.Ordinal)

							If intIndexEndCurrent > intIndexStartCurrent And intIndexEndCurrent <= intFinalSearchIndex Then
								strText = strText.Substring(0, intIndexStartCurrent).TrimEnd(chWhiteSpaceChars) &
								  COMMENT_END_TEXT &
								  strText.Substring(intIndexEndCurrent + COMMENT_END_TEXT_SHORT.Length)
							End If
						End If
					Loop While blnRemoveAllScriptDateOccurrences And intIndexStartCurrent > 0

					If blnRemoveDuplicateHeaderLine Then
						Dim intFirstCrLf = strText.IndexOf(ControlChars.NewLine, 0, System.StringComparison.Ordinal)
						If intFirstCrLf > 0 AndAlso intFirstCrLf < strText.Length Then
							Dim intIndexNextCrLf = strText.IndexOf(ControlChars.NewLine, intFirstCrLf + 1, System.StringComparison.Ordinal)

							If intIndexNextCrLf > intFirstCrLf Then
								If strText.Substring(0, intFirstCrLf) = strText.Substring(intFirstCrLf + 2, intIndexNextCrLf - intFirstCrLf - 2) Then
									strText = strText.Substring(intFirstCrLf + 2)
								End If
							End If
						End If
					End If

					lstInfoClean.Add(strText)

				Next
			End If

		Catch ex As Exception
			' Return lstInfo; not lstInfoClean
			Return lstInfoClean
		End Try


		Return lstInfoClean

	End Function

	Public Function ConnectToServer(ByVal udtServerConnectionInfo As udtServerConnectionInfoType) As Boolean
		Dim blnConnected As Boolean

		Try
			blnConnected = False

			' Initialize the current connection options
			If mSqlServer Is Nothing Then
				ResetSqlServerConnection(mSqlServerOptionsCurrent)
			Else
				If mSqlServerOptionsCurrent.Connected AndAlso Not mSqlServer Is Nothing AndAlso mSqlServer.State = SqlSmoState.Existing Then
					If mSqlServer.Name.ToLower = udtServerConnectionInfo.ServerName.ToLower Then
						' Already connected; no need to re-connect
						blnConnected = True
					End If
				End If
			End If

			If Not blnConnected Then
				' Connect to server udtServerConnectionInfo.ServerName
				blnConnected = LoginToServerWork(mSqlServer, mSqlServerOptionsCurrent, udtServerConnectionInfo)
				If Not blnConnected Then
					SetLocalError(eDBSchemaExportErrorCodes.DatabaseConnectionError, "Error logging into the server: " & udtServerConnectionInfo.ServerName)
				End If
			End If
		Catch ex As Exception
			SetLocalError(eDBSchemaExportErrorCodes.DatabaseConnectionError, "Error logging into the server: " & udtServerConnectionInfo.ServerName, ex)

			mSqlServerOptionsCurrent.Connected = False
			mSqlServer = Nothing
			blnConnected = False
		End Try

		Return blnConnected

	End Function

	Private Function ExportDBObjectsUsingSMO(
	  ByRef objSqlServer As Server,
	  ByVal strDatabaseName As String,
	  ByVal lstTableNamesForDataExport As List(Of String),
	  ByRef udtSchemaExportOptions As udtSchemaExportOptionsType,
	  ByRef blnDBNotFoundReturn As Boolean) As Boolean

		Dim udtWorkingParams As udtDBExportWorkingParamsType
		Dim objScriptOptions As ScriptingOptions
		Dim objDatabase As Database

		Dim dctTablesToExport As Dictionary(Of String, Int64)

		Try
			' Initialize udtWorkingParams
			With udtWorkingParams
				.ProcessCount = 0
				.ProcessCountExpected = 0
				.OutputFolderPathCurrentDB = String.Empty
				.CountObjectsOnly = True
			End With

			blnDBNotFoundReturn = False

			objScriptOptions = GetDefaultScriptOptions()

			objDatabase = objSqlServer.Databases(strDatabaseName)
			blnDBNotFoundReturn = False
		Catch ex As Exception
			SetLocalError(eDBSchemaExportErrorCodes.DatabaseConnectionError, "Error connecting to database " & strDatabaseName)
			blnDBNotFoundReturn = True
			Return False
		End Try

		Try
			' Validate the strings in udtSchemaExportOptions
			ValidateSchemaExportOptions(udtSchemaExportOptions)

			' Construct the path to the output folder
			If udtSchemaExportOptions.CreateFolderForEachDB Then
				udtWorkingParams.OutputFolderPathCurrentDB = Path.Combine(udtSchemaExportOptions.OutputFolderPath, udtSchemaExportOptions.OutputFolderNamePrefix & objDatabase.Name)
			Else
				udtWorkingParams.OutputFolderPathCurrentDB = String.Copy(udtSchemaExportOptions.OutputFolderPath)
			End If

			' Create the folder if it doesn't exist
			If Not Directory.Exists(udtWorkingParams.OutputFolderPathCurrentDB) Then
				Directory.CreateDirectory(udtWorkingParams.OutputFolderPathCurrentDB)
			End If

			If mSchemaOutputFolders.ContainsKey(strDatabaseName) Then
				mSchemaOutputFolders(strDatabaseName) = udtWorkingParams.OutputFolderPathCurrentDB
			Else
				mSchemaOutputFolders.Add(strDatabaseName, udtWorkingParams.OutputFolderPathCurrentDB)
			End If

			If udtSchemaExportOptions.AutoSelectTableNamesForDataExport Then
				dctTablesToExport = AutoSelectTableNamesForDataExport(objDatabase, lstTableNamesForDataExport)
			Else
				dctTablesToExport = New Dictionary(Of String, Int64)
				For Each tableName In lstTableNamesForDataExport
					dctTablesToExport.Add(tableName, 0)
				Next
			End If

		Catch ex As Exception
			SetLocalError(eDBSchemaExportErrorCodes.DatabaseConnectionError, "Error validating or creating folder " & udtWorkingParams.OutputFolderPathCurrentDB)
			Return False
		End Try

		Try
			ResetSubtaskProgress("Counting number of objects to export")

			' Preview the number of objects to export
			udtWorkingParams.CountObjectsOnly = True
			ExportDBObjectsWork(objDatabase, objScriptOptions, udtSchemaExportOptions, udtWorkingParams)
			udtWorkingParams.ProcessCountExpected = udtWorkingParams.ProcessCount

			If Not lstTableNamesForDataExport Is Nothing Then			
				udtWorkingParams.ProcessCountExpected += lstTableNamesForDataExport.Count
			End If

			udtWorkingParams.CountObjectsOnly = False
			If udtWorkingParams.ProcessCount > 0 Then
				ExportDBObjectsWork(objDatabase, objScriptOptions, udtSchemaExportOptions, udtWorkingParams)
			End If

			' Export data from tables specified by dctTablesToExport
			Dim blnSuccess = ExportDBTableData(objDatabase, dctTablesToExport, udtSchemaExportOptions, udtWorkingParams)
			Return blnSuccess

		Catch ex As Exception
			SetLocalError(eDBSchemaExportErrorCodes.DatabaseConnectionError, "Error scripting objects in database " & strDatabaseName)
			Return False
		End Try

	End Function

	Private Sub ExportDBObjectsWork(
	  ByRef objDatabase As Database,
	  ByRef objScriptOptions As ScriptingOptions,
	  ByRef udtSchemaExportOptions As udtSchemaExportOptionsType,
	  ByRef udtWorkingParams As udtDBExportWorkingParamsType)

		' Do not include a Try block in this Function; let the calling function handle errors

		' Reset ProcessCount
		udtWorkingParams.ProcessCount = 0

		If udtSchemaExportOptions.ExportDBSchemasAndRoles Then
			ExportDBSchemasAndRoles(objDatabase, udtSchemaExportOptions, objScriptOptions, udtWorkingParams)
			If mAbortProcessing Then Exit Sub
		End If

		If udtSchemaExportOptions.ExportTables Then
			ExportDBTables(objDatabase, udtSchemaExportOptions, objScriptOptions, udtWorkingParams)
			If mAbortProcessing Then Exit Sub
		End If

		If udtSchemaExportOptions.ExportViews Or _
		   udtSchemaExportOptions.ExportUserDefinedFunctions Or _
		   udtSchemaExportOptions.ExportStoredProcedures Then
			ExportDBViewsProcsAndUDFs(objDatabase, udtSchemaExportOptions, objScriptOptions, udtWorkingParams)
			If mAbortProcessing Then Exit Sub
		End If

		If udtSchemaExportOptions.ExportUserDefinedDataTypes Then
			ExportDBUserDefinedDataTypes(objDatabase, udtSchemaExportOptions, objScriptOptions, udtWorkingParams)
			If mAbortProcessing Then Exit Sub
		End If

		If udtSchemaExportOptions.ExportUserDefinedTypes Then
			ExportDBUserDefinedTypes(objDatabase, udtSchemaExportOptions, objScriptOptions, udtWorkingParams)
			If mAbortProcessing Then Exit Sub
		End If

	End Sub

	Private Sub ExportDBSchemasAndRoles(ByRef objDatabase As Database, _
	   ByRef udtSchemaExportOptions As udtSchemaExportOptionsType, _
	   ByRef objScriptOptions As ScriptingOptions, _
	   ByRef udtWorkingParams As udtDBExportWorkingParamsType)
		Dim intIndex As Integer

		If udtWorkingParams.CountObjectsOnly Then
			udtWorkingParams.ProcessCount = 1

			If SqlServer2005OrNewer(objDatabase) Then
				For intIndex = 0 To objDatabase.Schemas.Count - 1
					If ExportSchema(objDatabase.Schemas(intIndex)) Then
						udtWorkingParams.ProcessCount += 1
					End If
				Next intIndex
			End If

			For intIndex = 0 To objDatabase.Roles.Count - 1
				If ExportRole(objDatabase.Roles(intIndex)) Then
					udtWorkingParams.ProcessCount += 1
				End If
			Next intIndex
		Else
			Try
				WriteTextToFile(udtWorkingParams.OutputFolderPathCurrentDB, "DBDefinition_" & objDatabase.Name, _
				 CleanSqlScript(StringCollectionToList(objDatabase.Script(objScriptOptions)), udtSchemaExportOptions))
			Catch ex As Exception
				' User likely doesn't have privilege to script the DB; ignore the error
				RaiseEvent NewMessage("Unable to script DB " & objDatabase.Name & ": " & ex.Message, eMessageTypeConstants.ErrorMessage)
			End Try
			udtWorkingParams.ProcessCount += 1

			If SqlServer2005OrNewer(objDatabase) Then
				For intIndex = 0 To objDatabase.Schemas.Count - 1
					If ExportSchema(objDatabase.Schemas(intIndex)) Then
						WriteTextToFile(udtWorkingParams.OutputFolderPathCurrentDB, "Schema_" & objDatabase.Schemas(intIndex).Name, _
						 CleanSqlScript(StringCollectionToList(objDatabase.Schemas(intIndex).Script(objScriptOptions)), udtSchemaExportOptions))

						udtWorkingParams.ProcessCount += 1
						CheckPauseStatus()
						If mAbortProcessing Then
							UpdateProgress("Aborted processing")
							Exit Sub
						End If
					End If
				Next intIndex
			End If

			For intIndex = 0 To objDatabase.Roles.Count - 1
				If ExportRole(objDatabase.Roles(intIndex)) Then
					WriteTextToFile(udtWorkingParams.OutputFolderPathCurrentDB, "Role_" & objDatabase.Roles(intIndex).Name, _
					 CleanSqlScript(StringCollectionToList(objDatabase.Roles(intIndex).Script(objScriptOptions)), udtSchemaExportOptions))

					udtWorkingParams.ProcessCount += 1
					CheckPauseStatus()
					If mAbortProcessing Then
						UpdateProgress("Aborted processing")
						Exit Sub
					End If
				End If
			Next intIndex
		End If

	End Sub

	Private Sub ExportDBTables(ByRef objDatabase As Database, _
	   ByRef udtSchemaExportOptions As udtSchemaExportOptionsType, _
	   ByRef objScriptOptions As ScriptingOptions, _
	   ByRef udtWorkingParams As udtDBExportWorkingParamsType)

		Const SYNC_OBJ_TABLE_PREFIX As String = "syncobj_0x"

		Dim objScripter As Scripter
		Dim objSMOObject() As SqlSmoObject
		Dim objTable As Table

		Dim blnIncludeTable As Boolean

		If udtWorkingParams.CountObjectsOnly Then
			' Note: objDatabase.Tables includes system tables, so udtWorkingParams.ProcessCount will be 
			'       an overestimate if udtSchemaExportOptions.IncludeSystemObjects = False
			udtWorkingParams.ProcessCount += objDatabase.Tables.Count
		Else
			' Initialize the scripter and objSMOObject()
			objScripter = New Scripter(mSqlServer)
			objScripter.Options = objScriptOptions
			ReDim objSMOObject(0)

			For Each objTable In objDatabase.Tables
				blnIncludeTable = True
				If Not udtSchemaExportOptions.IncludeSystemObjects Then
					If objTable.IsSystemObject Then
						blnIncludeTable = False
					Else
						If objTable.Name.Length >= SYNC_OBJ_TABLE_PREFIX.Length Then
							If objTable.Name.ToLower.Substring(0, SYNC_OBJ_TABLE_PREFIX.Length) = SYNC_OBJ_TABLE_PREFIX.ToLower Then
								blnIncludeTable = False
							End If
						End If
					End If
				End If

				If blnIncludeTable Then
					mSubtaskProgressStepDescription = objTable.Name
					UpdateSubtaskProgress(udtWorkingParams.ProcessCount, udtWorkingParams.ProcessCountExpected)

					objSMOObject(0) = objTable
					WriteTextToFile(udtWorkingParams.OutputFolderPathCurrentDB, objTable.Name, _
					 CleanSqlScript(StringCollectionToList(objScripter.Script(objSMOObject)), udtSchemaExportOptions))
				End If

				udtWorkingParams.ProcessCount += 1
				CheckPauseStatus()
				If mAbortProcessing Then
					UpdateProgress("Aborted processing")
					Exit Sub
				End If
			Next objTable
		End If
	End Sub

	Private Sub ExportDBUserDefinedDataTypes(ByRef objDatabase As Database, _
	  ByRef udtSchemaExportOptions As udtSchemaExportOptionsType, _
	  ByRef objScriptOptions As ScriptingOptions, _
	  ByRef udtWorkingParams As udtDBExportWorkingParamsType)

		Dim intItemCount As Integer

		If udtWorkingParams.CountObjectsOnly Then
			udtWorkingParams.ProcessCount += objDatabase.UserDefinedDataTypes.Count
		Else
			intItemCount = ScriptCollectionOfObjects(objDatabase.UserDefinedDataTypes, udtSchemaExportOptions, objScriptOptions, udtWorkingParams.ProcessCountExpected, udtWorkingParams.OutputFolderPathCurrentDB)
			udtWorkingParams.ProcessCount += intItemCount
		End If
	End Sub

	Private Sub ExportDBUserDefinedTypes(ByRef objDatabase As Database, _
	 ByRef udtSchemaExportOptions As udtSchemaExportOptionsType, _
	 ByRef objScriptOptions As ScriptingOptions, _
	 ByRef udtWorkingParams As udtDBExportWorkingParamsType)
		Dim intItemCount As Integer

		If SqlServer2005OrNewer(objDatabase) Then
			If udtWorkingParams.CountObjectsOnly Then
				udtWorkingParams.ProcessCount += objDatabase.UserDefinedTypes.Count
			Else
				intItemCount = ScriptCollectionOfObjects(objDatabase.UserDefinedTypes, udtSchemaExportOptions, objScriptOptions, udtWorkingParams.ProcessCountExpected, udtWorkingParams.OutputFolderPathCurrentDB)
				udtWorkingParams.ProcessCount += intItemCount
			End If
		End If

	End Sub

	Private Sub ExportDBViewsProcsAndUDFs(ByRef objDatabase As Database, _
	  ByRef udtSchemaExportOptions As udtSchemaExportOptionsType, _
	  ByRef objScriptOptions As ScriptingOptions, _
	  ByRef udtWorkingParams As udtDBExportWorkingParamsType)

		' Option 1) obtain the list of views, stored procedures, and UDFs is to use objDatabase.EnumObjects
		' However, this only returns the object name, type, and URN, not whether or not it is a system object
		'
		' Option 2) use objDatabase.Views, objDatabase.StoredProcedures, etc.
		' However, on Sql Server 2005 this returns many system views and system procedures that we typically don't want to export
		'
		' Option 3) query the sysobjects table and filter on the xtype field
		' Possible values for XType:
		'   C = CHECK constraint
		'   D = Default or DEFAULT constraint
		'   F = FOREIGN KEY constraint
		'   IF = Inline Function
		'   FN = User Defined Function
		'   TF = Table Valued Function
		'   L = Log
		'   P = Stored procedure
		'   PK = PRIMARY KEY constraint (type is K)
		'   RF = Replication filter stored procedure
		'   S = System table
		'   TR = Trigger
		'   U = User table
		'   UQ = UNIQUE constraint (type is K)
		'   V = View
		'   X = Extended stored procedure

		''Dim strXType As String
		''For intObjectIterator = 0 To 2
		''    strXType = String.Empty
		''    Select Case intObjectIterator
		''        Case 0
		''            ' Views
		''            If udtSchemaExportOptions.ExportViews Then
		''                strXType = " = 'V'"
		''            End If
		''        Case 1
		''            ' Stored procedures
		''            If udtSchemaExportOptions.ExportStoredProcedures Then
		''                strXType = " = 'P'"
		''            End If
		''        Case 2
		''            ' User defined functions
		''            If udtSchemaExportOptions.ExportUserDefinedFunctions Then
		''                strXType = " IN ('IF', 'FN', 'TF')"
		''            End If
		''        Case Else
		''            ' Unknown value for intObjectIterator; skip it
		''    End Select

		''    If strXType.Length > 0 Then
		''        strSql = "SELECT name FROM sysobjects WHERE xtype " & strXType
		''        If Not udtSchemaExportOptions.IncludeSystemObjects Then
		''            strSql &= " AND category = 0"
		''        End If
		''        strSql &= " ORDER BY Name"
		''        dsObjects = objDatabase.ExecuteWithResults(strSql)

		''        If udtWorkingParams.CountObjectsOnly Then
		''            udtWorkingParams.ProcessCount += dsObjects.Tables(0).Rows.Count
		''        Else
		''            For Each objRow In dsObjects.Tables(0).Rows
		''                strObjectName = objRow.Item(0).ToString
		''                mSubtaskProgressStepDescription = strObjectName
		''                UpdateSubtaskProgress(udtWorkingParams.ProcessCount, udtWorkingParams.ProcessCountExpected)

		''                Select Case intObjectIterator
		''                    Case 0
		''                        ' Views
		''                        objSMOObject(0) = objDatabase.Views(strObjectName)
		''                    Case 1
		''                        ' Stored procedures
		''                        objSMOObject(0) = objDatabase.StoredProcedures(strObjectName)
		''                    Case 2
		''                        ' User defined functions
		''                        objSMOObject(0) = objDatabase.UserDefinedFunctions(strObjectName)
		''                    Case Else
		''                        ' Unknown value for intObjectIterator; skip it
		''                        objSMOObject(0) = Nothing
		''                End Select

		''                If Not objSMOObject(0) Is Nothing Then
		''                    WriteTextToFile(udtWorkingParams.OutputFolderPathCurrentDB, strObjectName, _
		''                                      CleanSqlScript(objScripter.Script(objSMOObject), udtSchemaExportOptions)))
		''                End If

		''                udtWorkingParams.ProcessCount += 1
		''                CheckPauseStatus()
		''                If mAbortProcessing Then
		''                    UpdateProgress("Aborted processing")
		''                    Exit Function
		''                End If
		''            Next objRow
		''        End If
		''    End If
		''Next intObjectIterator


		' Option 4) Query the INFORMATION_SCHEMA views

		Dim objScripter As Scripter
		Dim objSMOObject() As SqlSmoObject

		''Dim objURNList() As Microsoft.SqlServer.Management.Smo.Urn
		''ReDim objURNList(0)

		Dim dsObjects As DataSet
		Dim objRow As DataRow
		Dim strObjectSchema As String
		Dim strObjectName As String

		Dim intObjectIterator As Integer

		Dim strSql As String

		' Initialize the scripter and objSMOObject()
		objScripter = New Scripter(mSqlServer)
		objScripter.Options = objScriptOptions

		ReDim objSMOObject(0)

		For intObjectIterator = 0 To 2
			strSql = String.Empty
			Select Case intObjectIterator
				Case 0
					' Views
					If udtSchemaExportOptions.ExportViews Then
						strSql = "SELECT table_schema, table_name FROM INFORMATION_SCHEMA.tables WHERE table_type = 'view' "
						If Not udtSchemaExportOptions.IncludeSystemObjects Then
							strSql &= " AND table_name NOT IN ('sysconstraints', 'syssegments') "
						End If
						strSql &= " ORDER BY table_name"
					End If
				Case 1
					' Stored procedures
					If udtSchemaExportOptions.ExportStoredProcedures Then
						strSql = "SELECT routine_schema, routine_name FROM INFORMATION_SCHEMA.routines WHERE routine_type = 'procedure' "
						If Not udtSchemaExportOptions.IncludeSystemObjects Then
							strSql &= " AND routine_name NOT LIKE 'dt[_]%' "
						End If
						strSql &= " ORDER BY routine_name"
					End If
				Case 2
					' User defined functions
					If udtSchemaExportOptions.ExportUserDefinedFunctions Then
						strSql = "SELECT routine_schema, routine_name FROM INFORMATION_SCHEMA.routines WHERE routine_type = 'function' "
						strSql &= " ORDER BY routine_name"
					End If
				Case Else
					' Unknown value for intObjectIterator; skip it
			End Select

			If strSql.Length > 0 Then
				dsObjects = objDatabase.ExecuteWithResults(strSql)

				If udtWorkingParams.CountObjectsOnly Then
					udtWorkingParams.ProcessCount += dsObjects.Tables(0).Rows.Count
				Else
					For Each objRow In dsObjects.Tables(0).Rows
						' The first column is the schema
						' The second column is the name
						strObjectSchema = objRow.Item(0).ToString
						strObjectName = objRow.Item(1).ToString
						mSubtaskProgressStepDescription = strObjectName
						UpdateSubtaskProgress(udtWorkingParams.ProcessCount, udtWorkingParams.ProcessCountExpected)

						Select Case intObjectIterator
							Case 0
								' Views
								objSMOObject(0) = objDatabase.Views(strObjectName, strObjectSchema)
							Case 1
								' Stored procedures
								objSMOObject(0) = objDatabase.StoredProcedures(strObjectName, strObjectSchema)
							Case 2
								' User defined functions
								objSMOObject(0) = objDatabase.UserDefinedFunctions(strObjectName, strObjectSchema)
							Case Else
								' Unknown value for intObjectIterator; skip it
								objSMOObject(0) = Nothing
						End Select

						If Not objSMOObject(0) Is Nothing Then
							WriteTextToFile(udtWorkingParams.OutputFolderPathCurrentDB, strObjectName, _
							 CleanSqlScript(StringCollectionToList(objScripter.Script(objSMOObject)), udtSchemaExportOptions))
						End If

						udtWorkingParams.ProcessCount += 1
						CheckPauseStatus()
						If mAbortProcessing Then
							UpdateProgress("Aborted processing")
							Exit Sub
						End If
					Next objRow
				End If
			End If
		Next intObjectIterator
	End Sub

	Private Function ExportDBTableData(
	  ByRef objDatabase As Database, _
	  ByVal dctTablesToExport As Dictionary(Of String, Int64),
	  ByVal udtSchemaExportOptions As udtSchemaExportOptionsType, _
	  ByRef udtWorkingParams As udtDBExportWorkingParamsType) As Boolean

		Try
			If dctTablesToExport Is Nothing OrElse dctTablesToExport.Count = 0 Then
				Return True
			End If


			Dim sbCurrentRow = New StringBuilder

			For Each tableItem In dctTablesToExport

				Dim intMaximumDataRowsToExport = tableItem.Value

				mSubtaskProgressStepDescription = "Exporting data from " & tableItem.Key
				UpdateSubtaskProgress(udtWorkingParams.ProcessCount, udtWorkingParams.ProcessCountExpected)

				Dim objTable As Table

				If objDatabase.Tables.Contains(tableItem.Key) Then
					objTable = objDatabase.Tables(tableItem.Key)
				ElseIf objDatabase.Tables.Contains(tableItem.Key, "dbo") Then
					objTable = objDatabase.Tables(tableItem.Key, "dbo")
				Else
					Continue For
				End If

				' See if any of the columns in the table is an identity column
				Dim blnIdentityColumnFound = False
				For Each objColumn As Column In objTable.Columns
					If objColumn.Identity Then
						blnIdentityColumnFound = True
						Exit For
					End If
				Next

				' Export the data from objTable, possibly limiting the number of rows to export
				Dim strSql = "SELECT "

				If intMaximumDataRowsToExport > 0 Then
					strSql &= "TOP " & intMaximumDataRowsToExport.ToString
				End If

				strSql &= " * FROM [" & objTable.Name & "]"

				' Read method #1: Populate a DataSet
				Dim dsCurrentTable As DataSet = objDatabase.ExecuteWithResults(strSql)

				Dim lstTableRows = New List(Of String)

				Dim strHeader = COMMENT_START_TEXT & "Object:  Table [" & objTable.Name & "]"
				If udtSchemaExportOptions.IncludeTimestampInScriptFileHeader Then
					strHeader &= "    " & COMMENT_SCRIPT_DATE_TEXT & GetTimeStamp()
				End If
				strHeader &= COMMENT_END_TEXT
				lstTableRows.Add(strHeader)

				lstTableRows.Add(COMMENT_START_TEXT & "RowCount: " & objTable.RowCount & COMMENT_END_TEXT)

				Dim intColumnCount = dsCurrentTable.Tables(0).Columns.Count
				Dim lstColumnTypes = New List(Of eDataColumnTypeConstants)

				' Construct the column name list and determine the column data types
				sbCurrentRow.Clear()
				For intColumnIndex As Integer = 0 To intColumnCount - 1
					Dim objColumn As DataColumn = dsCurrentTable.Tables(0).Columns(intColumnIndex)

					' Initially assume the column's data type is numeric
					Dim eDataColumnType = eDataColumnTypeConstants.Numeric

					' Now check for other data types
					If objColumn.DataType Is Type.GetType("System.String") Then
						eDataColumnType = eDataColumnTypeConstants.Text

					ElseIf objColumn.DataType Is Type.GetType("System.DateTime") Then
						' Date column
						eDataColumnType = eDataColumnTypeConstants.DateTime

					ElseIf objColumn.DataType Is Type.GetType("System.Byte[]") Then
						Select Case objColumn.DataType.Name
							Case "image"
								eDataColumnType = eDataColumnTypeConstants.ImageObject
							Case "timestamp"
								eDataColumnType = eDataColumnTypeConstants.BinaryArray
							Case Else
								eDataColumnType = eDataColumnTypeConstants.BinaryArray
						End Select

					ElseIf objColumn.DataType Is Type.GetType("System.Guid") Then
						eDataColumnType = eDataColumnTypeConstants.GUID

					ElseIf objColumn.DataType Is Type.GetType("System.Boolean") Then
						' This may be a binary column
						Select Case objColumn.DataType.Name
							Case "binary", "bit"
								eDataColumnType = eDataColumnTypeConstants.BinaryByte
							Case Else
								eDataColumnType = eDataColumnTypeConstants.Text
						End Select

					ElseIf objColumn.DataType Is Type.GetType("System.Object") Then
						Select Case objColumn.DataType.Name
							Case "sql_variant"
								eDataColumnType = eDataColumnTypeConstants.SqlVariant
							Case Else
								eDataColumnType = eDataColumnTypeConstants.GeneralObject
						End Select

					End If

					lstColumnTypes.Add(eDataColumnType)

					If udtSchemaExportOptions.SaveDataAsInsertIntoStatements Then
						sbCurrentRow.Append(PossiblyQuoteColumnName(objColumn.ColumnName))
						If intColumnIndex < intColumnCount - 1 Then
							sbCurrentRow.Append(", ")
						End If
					Else
						sbCurrentRow.Append(objColumn.ColumnName)
						If intColumnIndex < intColumnCount - 1 Then
							sbCurrentRow.Append(ControlChars.Tab)
						End If
					End If

				Next

				Dim strInsertIntoLine As String = String.Empty
				Dim chColSepChar As Char

				If udtSchemaExportOptions.SaveDataAsInsertIntoStatements Then
					' Future capability:
					''Select Case udtSchemaExportOptions.DatabaseTypeForInsertInto
					''    Case eTargetDatabaseTypeConstants.SqlServer
					''    Case Else
					''        ' Unsupported mode
					''End Select

					If blnIdentityColumnFound Then
						strInsertIntoLine = "INSERT INTO [" & objTable.Name & "] (" & sbCurrentRow.ToString & ") VALUES ("
						lstTableRows.Add("SET IDENTITY_INSERT [" & objTable.Name & "] ON")
					Else
						' Identity column not present; no need to explicitly list the column names
						strInsertIntoLine = "INSERT INTO [" & objTable.Name & "] VALUES ("

						' However, we'll display the column names in the output file
						lstTableRows.Add(COMMENT_START_TEXT & "Columns: " & sbCurrentRow.ToString & COMMENT_END_TEXT)
					End If
					chColSepChar = ","c
				Else
					lstTableRows.Add(sbCurrentRow.ToString)
					chColSepChar = ControlChars.Tab
				End If


				For Each objRow As DataRow In dsCurrentTable.Tables(0).Rows
					sbCurrentRow.Clear()
					If udtSchemaExportOptions.SaveDataAsInsertIntoStatements Then
						sbCurrentRow.Append(strInsertIntoLine)
					End If

					For intColumnIndex = 0 To intColumnCount - 1
						Select Case lstColumnTypes(intColumnIndex)
							Case eDataColumnTypeConstants.Numeric
								sbCurrentRow.Append(objRow.Item(intColumnIndex).ToString)
							Case eDataColumnTypeConstants.Text, eDataColumnTypeConstants.DateTime, eDataColumnTypeConstants.GUID
								If udtSchemaExportOptions.SaveDataAsInsertIntoStatements Then
									sbCurrentRow.Append(PossiblyQuoteText(objRow.Item(intColumnIndex).ToString))
								Else
									sbCurrentRow.Append(objRow.Item(intColumnIndex).ToString)
								End If

							Case eDataColumnTypeConstants.BinaryArray
								Try
									Dim bytData() As Byte = CType(CType(objRow.Item(intColumnIndex), Array), Byte())

									' Convert the bytes to a string; however, do not write any leading zeroes
									' The string will be of the form '0x020D89'
									sbCurrentRow.Append("0x")

									Dim blnDataFound = False
									For intByteIndex = 0 To bytData.Length - 1
										If blnDataFound OrElse bytData(intByteIndex) <> 0 Then
											blnDataFound = True
											' Convert the byte to Hex (0 to 255 -> 00 to FF)
											sbCurrentRow.Append(bytData(intByteIndex).ToString("X2"))
										End If
									Next intByteIndex

									If Not blnDataFound Then
										sbCurrentRow.Append("00")
									End If

								Catch ex As Exception
									sbCurrentRow.Append("[Byte]")
								End Try

							Case eDataColumnTypeConstants.BinaryByte
								Try
									sbCurrentRow.Append("0x" & Convert.ToByte(objRow.Item(intColumnIndex)).ToString("X2"))
								Catch ex As Exception
									sbCurrentRow.Append("[Byte]")
								End Try
							Case eDataColumnTypeConstants.ImageObject
								sbCurrentRow.Append("[Image]")
							Case eDataColumnTypeConstants.GeneralObject
								sbCurrentRow.Append("[Object]")
							Case eDataColumnTypeConstants.SqlVariant
								sbCurrentRow.Append("[Sql_Variant]")

							Case Else
								' No need to quote
								sbCurrentRow.Append(objRow.Item(intColumnIndex).ToString)
						End Select

						If intColumnIndex < intColumnCount - 1 Then
							sbCurrentRow.Append(chColSepChar)
						End If
					Next
					If udtSchemaExportOptions.SaveDataAsInsertIntoStatements Then
						sbCurrentRow.Append(")")
					End If

					lstTableRows.Add(sbCurrentRow.ToString)
				Next

				If blnIdentityColumnFound AndAlso udtSchemaExportOptions.SaveDataAsInsertIntoStatements Then
					lstTableRows.Add("SET IDENTITY_INSERT [" & objTable.Name & "] OFF")
				End If

				' '' Read method #2: Use a SqlDataReader to read row-by-row
				''objReader = objSqlServer.ConnectionContext.ExecuteReader(strSql)

				''If objReader.HasRows Then
				''    Do While objReader.Read
				''        If objReader.FieldCount > 0 Then
				''            strCurrentRow = objReader.GetValue(0).ToString
				''            objReader.GetDataTypeName()
				''        End If

				''        For intColumnIndex = 1 To objReader.FieldCount - 1
				''            strCurrentRow &= ControlChars.Tab & objReader.GetValue(intColumnIndex).ToString
				''        Next intColumnIndex
				''    Loop
				''End If

				WriteTextToFile(udtWorkingParams.OutputFolderPathCurrentDB, _
				 objTable.Name & "_Data", _
				 lstTableRows, False)


				udtWorkingParams.ProcessCount += 1
				CheckPauseStatus()
				If mAbortProcessing Then
					UpdateProgress("Aborted processing")
					Exit Function
				End If
			Next

			SetSubtaskProgressComplete()

			Return True

		Catch ex As Exception
			SetLocalError(eDBSchemaExportErrorCodes.DatabaseConnectionError, "Error in ExportDBTableData", ex)
			Return False
		End Try

	End Function

	Private Function ExportRole(ByVal objDatabaseRole As DatabaseRole) As Boolean
		Dim blnExportRole As Boolean

		Try
			If objDatabaseRole.IsFixedRole Then
				blnExportRole = False
			ElseIf objDatabaseRole.Name.ToLower = "public" Then
				blnExportRole = False
			Else
				blnExportRole = True
			End If
		Catch ex As Exception
			blnExportRole = False
		End Try

		Return blnExportRole

	End Function

	Private Function ExportSchema(ByVal objDatabaseSchema As Schema) As Boolean
		Static lstSchemaToIgnore As SortedSet(Of String)
		Dim blnExportSchema As Boolean

		If lstSchemaToIgnore Is Nothing Then
			lstSchemaToIgnore = New SortedSet(Of String)(StringComparer.CurrentCultureIgnoreCase)

			' Make sure each of these names is lowercase since we convert 
			'  the schema name to lower case when searching lstSchemaToIgnore
			lstSchemaToIgnore.Add("db_accessadmin")
			lstSchemaToIgnore.Add("db_backupoperator")
			lstSchemaToIgnore.Add("db_datareader")
			lstSchemaToIgnore.Add("db_datawriter")
			lstSchemaToIgnore.Add("db_ddladmin")
			lstSchemaToIgnore.Add("db_denydatareader")
			lstSchemaToIgnore.Add("db_denydatawriter")
			lstSchemaToIgnore.Add("db_owner")
			lstSchemaToIgnore.Add("db_securityadmin")
			lstSchemaToIgnore.Add("dbo")
			lstSchemaToIgnore.Add("guest")
			lstSchemaToIgnore.Add("information_schema")
			lstSchemaToIgnore.Add("sys")

		End If

		Try
			blnExportSchema = Not lstSchemaToIgnore.Contains(objDatabaseSchema.Name)
		Catch ex As Exception
			blnExportSchema = False
		End Try

		Return blnExportSchema

	End Function

	Private Sub AppendToList(ByRef lstInfo As List(Of String), ByVal PropertyName As String, ByVal PropertyValue As String)
		If Not (PropertyName Is Nothing Or PropertyValue Is Nothing) Then
			lstInfo.Add(PropertyName & "=" & PropertyValue)
		End If
	End Sub

	Private Sub AppendToList(ByRef lstInfo As List(Of String), ByVal PropertyName As String, ByVal PropertyValue As Integer)
		If Not PropertyName Is Nothing Then
			lstInfo.Add(PropertyName & "=" & PropertyValue.ToString)
		End If
	End Sub

	Private Sub AppendToList(ByRef lstInfo As List(Of String), ByVal PropertyName As String, ByVal PropertyValue As Boolean)
		If Not PropertyName Is Nothing Then
			lstInfo.Add(PropertyName & "=" & PropertyValue.ToString)
		End If
	End Sub

	Private Sub AppendToList(ByRef lstInfo As List(Of String), ByRef objConfigProperty As ConfigProperty)
		If Not objConfigProperty Is Nothing AndAlso Not objConfigProperty.DisplayName Is Nothing Then
			lstInfo.Add(objConfigProperty.DisplayName & "=" & objConfigProperty.ConfigValue)
		End If
	End Sub

	Private Sub ExportSQLServerConfiguration(ByRef objSqlServer As Server, _
	  ByRef udtSchemaExportOptions As udtSchemaExportOptionsType, _
	  ByRef objScriptOptions As ScriptingOptions, _
	  ByVal strOutputFolderPathCurrentServer As String)

		Dim lstInfo As New List(Of String)

		' Do not include a Try block in this Function; let the calling function handle errors

		' First save the Server Information to file ServerInformation
		lstInfo.Clear()
		With objSqlServer.Information
			lstInfo.Add("[Server Information for " & objSqlServer.Name & "]")
			AppendToList(lstInfo, "BuildClrVersion", .BuildClrVersionString)
			AppendToList(lstInfo, "Collation", .Collation)
			AppendToList(lstInfo, "Edition", .Edition)
			AppendToList(lstInfo, "ErrorLogPath", .ErrorLogPath)
			AppendToList(lstInfo, "IsCaseSensitive", .IsCaseSensitive)
			AppendToList(lstInfo, "IsClustered", .IsClustered)
			AppendToList(lstInfo, "IsFullTextInstalled", .IsFullTextInstalled)
			AppendToList(lstInfo, "IsSingleUser", .IsSingleUser)
			AppendToList(lstInfo, "Language", .Language)
			AppendToList(lstInfo, "MasterDBLogPath", .MasterDBLogPath)
			AppendToList(lstInfo, "MasterDBPath", .MasterDBPath)
			AppendToList(lstInfo, "MaxPrecision", .MaxPrecision)
			AppendToList(lstInfo, "NetName", .NetName)
			AppendToList(lstInfo, "OSVersion", .OSVersion)
			AppendToList(lstInfo, "PhysicalMemory", .PhysicalMemory)
			AppendToList(lstInfo, "Platform", .Platform)
			AppendToList(lstInfo, "Processors", .Processors)
			AppendToList(lstInfo, "Product", .Product)
			AppendToList(lstInfo, "ProductLevel", .ProductLevel)
			AppendToList(lstInfo, "RootDirectory", .RootDirectory)
			AppendToList(lstInfo, "VersionString", .VersionString)
		End With

		WriteTextToFile(strOutputFolderPathCurrentServer, "ServerInformation", lstInfo, False, ".ini")


		' Next save the Server Configuration to file ServerConfiguration
		lstInfo.Clear()
		With objSqlServer.Configuration
			lstInfo.Add("[Server Configuration for " & objSqlServer.Name & "]")
			AppendToList(lstInfo, .AdHocDistributedQueriesEnabled)
			AppendToList(lstInfo, .Affinity64IOMask)
			AppendToList(lstInfo, .Affinity64Mask)
			AppendToList(lstInfo, .AffinityIOMask)
			AppendToList(lstInfo, .AffinityMask)
			AppendToList(lstInfo, .AgentXPsEnabled)
			AppendToList(lstInfo, .AllowUpdates)
			AppendToList(lstInfo, .BlockedProcessThreshold)
			AppendToList(lstInfo, .C2AuditMode)
			AppendToList(lstInfo, .CommonCriteriaComplianceEnabled)
			AppendToList(lstInfo, .CostThresholdForParallelism)
			AppendToList(lstInfo, .CrossDBOwnershipChaining)
			AppendToList(lstInfo, .CursorThreshold)
			AppendToList(lstInfo, .DatabaseMailEnabled)
			AppendToList(lstInfo, .DefaultBackupCompression)
			AppendToList(lstInfo, .DefaultFullTextLanguage)
			AppendToList(lstInfo, .DefaultLanguage)
			AppendToList(lstInfo, .DefaultTraceEnabled)
			AppendToList(lstInfo, .DisallowResultsFromTriggers)
			AppendToList(lstInfo, .ExtensibleKeyManagementEnabled)
			AppendToList(lstInfo, .FilestreamAccessLevel)
			AppendToList(lstInfo, .FillFactor)
			AppendToList(lstInfo, .IndexCreateMemory)
			AppendToList(lstInfo, .InDoubtTransactionResolution)
			AppendToList(lstInfo, .IsSqlClrEnabled)
			AppendToList(lstInfo, .LightweightPooling)
			AppendToList(lstInfo, .Locks)
			AppendToList(lstInfo, .MaxDegreeOfParallelism)
			AppendToList(lstInfo, .MaxServerMemory)
			AppendToList(lstInfo, .MaxWorkerThreads)
			AppendToList(lstInfo, .MediaRetention)
			AppendToList(lstInfo, .MinMemoryPerQuery)
			AppendToList(lstInfo, .MinServerMemory)
			AppendToList(lstInfo, .NestedTriggers)
			AppendToList(lstInfo, .NetworkPacketSize)
			AppendToList(lstInfo, .OleAutomationProceduresEnabled)
			AppendToList(lstInfo, .OpenObjects)
			AppendToList(lstInfo, .OptimizeAdhocWorkloads)
			AppendToList(lstInfo, .PrecomputeRank)
			AppendToList(lstInfo, .PriorityBoost)
			AppendToList(lstInfo, .ProtocolHandlerTimeout)
			AppendToList(lstInfo, .QueryGovernorCostLimit)
			AppendToList(lstInfo, .QueryWait)
			AppendToList(lstInfo, .RecoveryInterval)
			AppendToList(lstInfo, .RemoteAccess)
			AppendToList(lstInfo, .RemoteDacConnectionsEnabled)
			AppendToList(lstInfo, .RemoteLoginTimeout)
			AppendToList(lstInfo, .RemoteProcTrans)
			AppendToList(lstInfo, .RemoteQueryTimeout)
			AppendToList(lstInfo, .ReplicationMaxTextSize)
			AppendToList(lstInfo, .ReplicationXPsEnabled)
			AppendToList(lstInfo, .ScanForStartupProcedures)
			AppendToList(lstInfo, .ServerTriggerRecursionEnabled)
			AppendToList(lstInfo, .SetWorkingSetSize)
			AppendToList(lstInfo, .ShowAdvancedOptions)
			AppendToList(lstInfo, .SmoAndDmoXPsEnabled)
			AppendToList(lstInfo, .SqlMailXPsEnabled)
			AppendToList(lstInfo, .TransformNoiseWords)
			AppendToList(lstInfo, .TwoDigitYearCutoff)
			AppendToList(lstInfo, .UserConnections)
			AppendToList(lstInfo, .UserOptions)
			AppendToList(lstInfo, .XPCmdShellEnabled)
		End With

		WriteTextToFile(strOutputFolderPathCurrentServer, "ServerConfiguration", lstInfo, False, ".ini")


		' Next save the Mail settings to file ServerMail
		' Can only do this for Sql Server 2005 or newer
		If SqlServer2005OrNewer(objSqlServer) Then
			lstInfo = CleanSqlScript(StringCollectionToList(objSqlServer.Mail.Script(objScriptOptions)), udtSchemaExportOptions, False, False)
			WriteTextToFile(strOutputFolderPathCurrentServer, "ServerMail", lstInfo, True)
		End If


		' Next save the Registry Settings to file ServerRegistrySettings
		lstInfo = CleanSqlScript(StringCollectionToList(objSqlServer.Settings.Script(objScriptOptions)), udtSchemaExportOptions, False, False)
		lstInfo.Insert(0, "-- Registry Settings for " & objSqlServer.Name)

		WriteTextToFile(strOutputFolderPathCurrentServer, "ServerRegistrySettings", lstInfo, False)


	End Sub

	Private Sub ExportSQLServerLogins(ByRef objSqlServer As Server, _
	 ByRef udtSchemaExportOptions As udtSchemaExportOptionsType, _
	 ByRef objScriptOptions As ScriptingOptions, _
	 ByVal strOutputFolderPathCurrentServer As String)

		Dim intProcessCountExpected As Integer
		Dim intIndex As Integer

		Dim strCurrentLogin As String
		Dim blnSuccess As Boolean

		' Do not include a Try block in this Function; let the calling function handle errors

		' Export the server logins
		intProcessCountExpected = objSqlServer.Logins.Count
		ResetSubtaskProgress("Exporting SQL Server logins")
		For intIndex = 0 To objSqlServer.Logins.Count - 1
			strCurrentLogin = objSqlServer.Logins.Item(intIndex).Name
			UpdateSubtaskProgress("Exporting login " & strCurrentLogin, mSubtaskProgressPercentComplete)

			blnSuccess = WriteTextToFile(strOutputFolderPathCurrentServer, "Login_" & strCurrentLogin, _
			 CleanSqlScript(StringCollectionToList(objSqlServer.Logins.Item(intIndex).Script(objScriptOptions)), udtSchemaExportOptions, True, True))

			UpdateSubtaskProgress(intIndex + 1, intProcessCountExpected)
			CheckPauseStatus()
			If mAbortProcessing Then
				UpdateProgress("Aborted processing")
				Exit For
			End If

			If blnSuccess Then
				RaiseEvent NewMessage("Processing completed for login " & strCurrentLogin, eMessageTypeConstants.HeaderLine)
			Else
				SetLocalError(eDBSchemaExportErrorCodes.GeneralError, "Processing failed for server " & udtSchemaExportOptions.ConnectionInfo.ServerName & "; login " & strCurrentLogin)
			End If
		Next intIndex

	End Sub


	Private Sub ExportSQLServerAgentJobs(ByRef objSqlServer As Server, _
	 ByRef udtSchemaExportOptions As udtSchemaExportOptionsType, _
	 ByRef objScriptOptions As ScriptingOptions, _
	 ByVal strOutputFolderPathCurrentServer As String)

		Dim intProcessCountExpected As Integer
		Dim intIndex As Integer

		Dim strCurrentJob As String
		Dim blnSuccess As Boolean

		' Do not include a Try block in this Function; let the calling function handle errors

		' Export the SQL Server Agent jobs
		intProcessCountExpected = objSqlServer.JobServer.Jobs.Count
		ResetSubtaskProgress("Exporting SQL Server Agent jobs")
		For intIndex = 0 To objSqlServer.JobServer.Jobs.Count - 1
			strCurrentJob = objSqlServer.JobServer.Jobs(intIndex).Name
			UpdateSubtaskProgress("Exporting job " & strCurrentJob, mSubtaskProgressPercentComplete)

			blnSuccess = WriteTextToFile(strOutputFolderPathCurrentServer, "AgentJob_" & strCurrentJob, _
			 CleanSqlScript(StringCollectionToList(objSqlServer.JobServer.Jobs(intIndex).Script(objScriptOptions)), udtSchemaExportOptions, True, True))

			UpdateSubtaskProgress(intIndex + 1, intProcessCountExpected)
			CheckPauseStatus()
			If mAbortProcessing Then
				UpdateProgress("Aborted processing")
				Exit For
			End If

			If blnSuccess Then
				RaiseEvent NewMessage("Processing completed for job " & strCurrentJob, eMessageTypeConstants.HeaderLine)
			Else
				SetLocalError(eDBSchemaExportErrorCodes.GeneralError, "Processing failed for server " & udtSchemaExportOptions.ConnectionInfo.ServerName & "; job " & strCurrentJob)
			End If

		Next intIndex

	End Sub

	Private Function GetDefaultScriptOptions() As ScriptingOptions

		Dim objScriptOptions As New ScriptingOptions

		With objScriptOptions
			'.Bindings = True
			.Default = True
			.DriAll = True
			'.DriAllKeys = True
			'.ExtendedProperties = True
			.IncludeHeaders = True			' If True, then includes a line of the form: /****** Object:  Table [dbo].[T_Analysis_Description]    Script Date: 08/14/2006 12:14:31 ******/
			.IncludeDatabaseContext = False
			.IncludeIfNotExists = False		' If True, then the entire SP is placed inside an nvarchar variable
			.Indexes = True
			'.NoCollation = True
			.NoCommandTerminator = False
			.Permissions = True
			'.PrimaryObject = True
			.SchemaQualify = True			' If True, then adds extra [dbo]. prefixes
			'.ScriptDrops = True            ' If True, the script only contains Drop commands, not Create commands
			.Statistics = True
			.Triggers = True
			.ToFileOnly = False
			.WithDependencies = False		' Scripting speed will be much slower if this is set to true
		End With

		Return objScriptOptions
	End Function

	Public Function GetSqlServerDatabases() As List(Of String)

		Dim lstDatabases = New List(Of String)

		Try
			InitializeLocalVariables(False)

			If Not mSqlServerOptionsCurrent.Connected OrElse mSqlServer Is Nothing OrElse mSqlServer.State <> SqlSmoState.Existing Then
				mStatusMessage = "Not connected to a server"
			Else
				' Obtain a list of all databases actually residing on the server (according to the Master database)
				ResetProgress("Obtaining list of databases on " & mSqlServerOptionsCurrent.ConnectionInfo.ServerName)

				' Obtain the databases collection
				Dim objDatabases = mSqlServer.Databases

				If objDatabases.Count > 0 Then

					ResetProgressStepCount(objDatabases.Count)

					For intIndex = 0 To objDatabases.Count - 1
						lstDatabases.Add(objDatabases(intIndex).Name)

						mProgressStep = intIndex + 1
						UpdateProgress(mProgressStep)
						If mAbortProcessing Then
							UpdateProgress("Aborted processing")
							Exit For
						End If
					Next intIndex

					lstDatabases.Sort()

				End If

				If Not mAbortProcessing Then
					UpdateProgress("Done")
					SetProgressComplete()
				End If
			End If
		Catch ex As Exception
			SetLocalError(eDBSchemaExportErrorCodes.DatabaseConnectionError, "Error obtaining list of databases on current server", ex)
		End Try

		Return lstDatabases

	End Function

	''' <summary>
	''' Lookup the table names in the specified database, optionally also determining table row counts
	''' </summary>
	''' <param name="strDatabaseName">Database to query</param>
	''' <param name="blnIncludeTableRowCounts">When true, then determines the row count in each table</param>
	''' <param name="blnIncludeSystemObjects">When true, then also returns system object tables</param>
	''' <returns>Dictionary where key is table name and value is row counts (if blnIncludeTableRowCounts = true)</returns>
	''' <remarks></remarks>
	Public Function GetSqlServerDatabaseTableNames(
	  ByVal strDatabaseName As String,
	  ByVal blnIncludeTableRowCounts As Boolean,
	  ByVal blnIncludeSystemObjects As Boolean) As Dictionary(Of String, Int64)

		Dim blnSuccess As Boolean

		Try
			blnSuccess = False
			InitializeLocalVariables(False)

			Dim dctTables = New Dictionary(Of String, Int64)

			If strDatabaseName Is Nothing Then
				mStatusMessage = "Empty database name sent to GetSqlServerDatabaseTableNames"
				Return dctTables
			ElseIf Not mSqlServerOptionsCurrent.Connected OrElse mSqlServer Is Nothing OrElse mSqlServer.State <> SqlSmoState.Existing Then
				mStatusMessage = "Not connected to a server"
				Return dctTables
			ElseIf Not mSqlServer.Databases.Contains(strDatabaseName) Then
				mStatusMessage = "Database " & strDatabaseName & " not found on server " & mSqlServerOptionsCurrent.ConnectionInfo.ServerName
				Return dctTables
			End If

			' Obtain a list of all databases actually residing on the server (according to the Master database)
			ResetProgress("Obtaining list of tables in database " & strDatabaseName & " on server " & mSqlServerOptionsCurrent.ConnectionInfo.ServerName)

			' Connect to database strDatabaseName
			Dim objDatabase = mSqlServer.Databases(strDatabaseName)

			' Obtain a list of the tables in objDatabase
			Dim objTables = objDatabase.Tables
			If objTables.Count > 0 Then

				ResetProgressStepCount(objTables.Count)

				For intIndex = 0 To objTables.Count - 1
					If blnIncludeSystemObjects OrElse Not objTables(intIndex).IsSystemObject Then

						Dim tableRowCount As Int64 = 0

						If blnIncludeTableRowCounts Then
							tableRowCount = objTables(intIndex).RowCount
						End If

						dctTables.Add(objTables(intIndex).Name, tableRowCount)

						mProgressStep = intIndex + 1
						UpdateProgress(mProgressStep)
						If mAbortProcessing Then
							UpdateProgress("Aborted processing")
							Exit For
						End If

					End If
				Next intIndex

			End If

			If Not mAbortProcessing Then
				UpdateProgress("Done")
				SetProgressComplete()
				blnSuccess = True
			End If

			Return dctTables

		Catch ex As Exception
			SetLocalError(eDBSchemaExportErrorCodes.DatabaseConnectionError, "Error obtaining list of tables in database " & strDatabaseName & " on current server", ex)
		End Try

		Return New Dictionary(Of String, Int64)

	End Function

	Private Function GetTimeStamp() As String
		' Return a timestamp in the form: 08/12/2006 23:01:20

		Return DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss")

	End Function

	Public Shared Function GetDefaultSchemaExportOptions() As udtSchemaExportOptionsType

		Dim udtSchemaExportOptions As udtSchemaExportOptionsType = New udtSchemaExportOptionsType()

		With udtSchemaExportOptions
			.OutputFolderPath = String.Empty
			.OutputFolderNamePrefix = DEFAULT_DB_OUTPUT_FOLDER_NAME_PREFIX

			.CreateFolderForEachDB = True			' This will be forced to true if more than one DB is to be scripted
			.IncludeSystemObjects = False
			.IncludeTimestampInScriptFileHeader = False

			.ExportServerSettingsLoginsAndJobs = False
			.ServerOutputFolderNamePrefix = DEFAULT_SERVER_OUTPUT_FOLDER_NAME_PREFIX

			.SaveDataAsInsertIntoStatements = True
			.DatabaseTypeForInsertInto = eTargetDatabaseTypeConstants.SqlServer
			.AutoSelectTableNamesForDataExport = True

			.ExportDBSchemasAndRoles = True
			.ExportTables = True
			.ExportViews = True
			.ExportStoredProcedures = True
			.ExportUserDefinedFunctions = True
			.ExportUserDefinedDataTypes = True
			.ExportUserDefinedTypes = True

			ResetSqlServerConnection(.ConnectionInfo)
		End With

		Return udtSchemaExportOptions

	End Function

	Private Sub InitializeLocalVariables(ByVal blnResetServerConnection As Boolean)
		mErrorCode = eDBSchemaExportErrorCodes.NoError
		mStatusMessage = String.Empty

		Dim objRegExOptions As RegexOptions
		objRegExOptions = RegexOptions.Compiled Or _
		   RegexOptions.IgnoreCase Or _
		   RegexOptions.Singleline

		mColumnCharNonStandardRegEx = New Regex("[^a-z0-9_]", objRegExOptions)

		mNonStandardOSChars = New Regex("[^a-z0-9_ =+-,.';`~!@#$%^&(){}\[\]]", objRegExOptions)

		If blnResetServerConnection Then
			ResetSqlServerConnection(mSqlServerOptionsCurrent)
		End If

		mTableNamesToAutoSelect = clsDBSchemaExportTool.GetTableNamesToAutoExportData()
		mTableNameAutoSelectRegEx = clsDBSchemaExportTool.GetTableRegExToAutoExportData()

		mSchemaOutputFolders = New Dictionary(Of String, String)

		mAbortProcessing = False
		SetPauseStatus(ePauseStatusConstants.Unpaused)
	End Sub

	Private Function LoginToServerWork(ByRef objSQLServer As Server, ByRef udtServerOptionsToUpdate As udtServerConnectionSingleType, ByVal udtServerConnectionInfo As udtServerConnectionInfoType) As Boolean
		' Returns True if success, False otherwise

		Dim objServerConnection As ServerConnection
		Dim objConnectionInfo As SqlConnectionInfo

		Try
			objConnectionInfo = New SqlConnectionInfo(udtServerConnectionInfo.ServerName)
			With objConnectionInfo
				.UseIntegratedSecurity = udtServerConnectionInfo.UseIntegratedAuthentication
				If Not .UseIntegratedSecurity Then
					.UserName = udtServerConnectionInfo.UserName
					.Password = udtServerConnectionInfo.Password
				End If
				.ConnectionTimeout = 10
			End With

			objServerConnection = New ServerConnection(objConnectionInfo)

			If Not objSQLServer Is Nothing Then
				objSQLServer = Nothing
			End If
			objSQLServer = New Server(objServerConnection)

			' If no error occurred, set .Connected = True and duplicate the connection info
			With udtServerOptionsToUpdate
				.Connected = True
				.ConnectionInfo = udtServerConnectionInfo
			End With

		Catch ex As Exception
			SetLocalError(eDBSchemaExportErrorCodes.DatabaseConnectionError, "Error logging in to server " & udtServerConnectionInfo.ServerName, ex)
			Return False
		End Try

		Return True

	End Function

	Private Function PossiblyQuoteColumnName(ByVal strColumnName As String) As String

		If mColumnCharNonStandardRegEx.Match(strColumnName).Success Then
			Return "[" & strColumnName & "]"
		Else
			Return strColumnName
		End If

	End Function

	Private Function PossiblyQuoteText(ByVal strText As String) As String
		Return "'" & strText.Replace("'", "''") & "'"
	End Function

	Public Sub TogglePause()
		If mPauseStatus = ePauseStatusConstants.Unpaused Then
			SetPauseStatus(ePauseStatusConstants.PauseRequested)
		ElseIf mPauseStatus = ePauseStatusConstants.Paused Then
			SetPauseStatus(ePauseStatusConstants.UnpauseRequested)
		End If
	End Sub

	Public Sub RequestPause()
		If Not (mPauseStatus = ePauseStatusConstants.Paused OrElse _
		  mPauseStatus = ePauseStatusConstants.PauseRequested) Then
			SetPauseStatus(ePauseStatusConstants.PauseRequested)
		End If
	End Sub

	Public Sub RequestUnpause()
		If Not (mPauseStatus = ePauseStatusConstants.Unpaused OrElse _
		  mPauseStatus = ePauseStatusConstants.UnpauseRequested) Then
			SetPauseStatus(ePauseStatusConstants.UnpauseRequested)
		End If
	End Sub

	Protected Sub ResetProgress()
		ResetProgress(String.Empty)
		ResetSubtaskProgress(String.Empty)
	End Sub

	Protected Sub ResetProgress(ByVal strProgressStepDescription As String)
		ResetProgress(strProgressStepDescription, mProgressStepCount)
	End Sub

	Protected Sub ResetProgress(ByVal strProgressStepDescription As String, ByVal intStepCount As Integer)
		mProgressStepDescription = String.Copy(strProgressStepDescription)
		mProgressPercentComplete = 0
		ResetProgressStepCount(intStepCount)
		RaiseEvent ProgressReset()
	End Sub

	Protected Sub ResetProgressStepCount(ByVal intStepCount As Integer)
		mProgressStep = 0
		mProgressStepCount = intStepCount
	End Sub

	Protected Sub ResetSubtaskProgress()
		RaiseEvent SubtaskProgressReset()
	End Sub

	Protected Sub ResetSubtaskProgress(ByVal strSubtaskProgressStepDescription As String)
		mSubtaskProgressStepDescription = String.Copy(strSubtaskProgressStepDescription)
		mSubtaskProgressPercentComplete = 0
		RaiseEvent SubtaskProgressReset()
	End Sub

	Private Sub ResetSqlServerConnection(ByRef udtServerConnectionInfo As udtServerConnectionSingleType)
		With udtServerConnectionInfo
			.Connected = False
			ResetSqlServerConnection(.ConnectionInfo)
		End With
	End Sub

	Public Shared Sub ResetSqlServerConnection(ByRef udtConnectionInfo As udtServerConnectionInfoType)
		With udtConnectionInfo
			.ServerName = String.Empty
			.UserName = String.Empty
			.Password = String.Empty
			.UseIntegratedAuthentication = True
		End With
	End Sub

	Private Function ScriptCollectionOfObjects(ByRef objSchemaCollection As SchemaCollectionBase, _
	 ByRef udtSchemaExportOptions As udtSchemaExportOptionsType, _
	 ByRef objScriptOptions As ScriptingOptions, _
	 ByVal intProcessCountExpected As Integer, _
	 ByVal strOutputFolderPathCurrentDB As String) As Integer

		' Scripts the objects in objSchemaCollection
		' Returns the number of objects scripted

		Dim objItem As Schema
		Dim intProcessCount As Integer

		intProcessCount = 0
		For Each objItem In objSchemaCollection
			mSubtaskProgressStepDescription = objItem.Name
			UpdateSubtaskProgress(intProcessCount, intProcessCountExpected)

			WriteTextToFile(strOutputFolderPathCurrentDB, objItem.Name, _
			 CleanSqlScript(StringCollectionToList(objItem.Script(objScriptOptions)), udtSchemaExportOptions))

			intProcessCount += 1

			CheckPauseStatus()
			If mAbortProcessing Then
				UpdateProgress("Aborted processing")
				Exit Function
			End If
		Next

		Return intProcessCount

	End Function

	Private Function ScriptDBObjects(
	  ByRef objSqlServer As Server,
	  ByVal udtSchemaExportOptions As udtSchemaExportOptionsType,
	  ByVal lstDatabaseListToProcess As List(Of String),
	  ByVal lstTableNamesForDataExport As List(Of String)) As Boolean

		Dim lstProcessedDBList = New SortedSet(Of String)

		Try

			' Process each database in lstDatabaseListToProcess			
			ResetProgress("Exporting DB objects to: " & VBNetRoutines.CompactPathString(udtSchemaExportOptions.OutputFolderPath), lstDatabaseListToProcess.Count)

			mSchemaOutputFolders.Clear()

			For Each strCurrentDB In lstDatabaseListToProcess
				Dim blnDBNotFoundReturn = True

				If String.IsNullOrWhiteSpace(strCurrentDB) Then
					' DB name is empty; this shouldn't happen
					Continue For
				End If

				If lstProcessedDBList.Contains(strCurrentDB) Then
					' DB has already been processed
					Continue For
				End If

				lstProcessedDBList.Add(strCurrentDB)
				Dim blnSuccess As Boolean

				If objSqlServer.Databases.Contains(strCurrentDB) Then

					UpdateProgress("Exporting objects from database " & strCurrentDB)

					blnSuccess = ExportDBObjectsUsingSMO(objSqlServer, strCurrentDB, lstTableNamesForDataExport, udtSchemaExportOptions, blnDBNotFoundReturn)

					If Not blnDBNotFoundReturn Then
						If Not blnSuccess Then Exit For

						If Not mAbortProcessing Then
							SetSubtaskProgressComplete()
						End If
					End If
				Else
					' Database not actually present on the server; skip it
					blnDBNotFoundReturn = True
				End If

				mProgressStep = lstProcessedDBList.Count
				UpdateProgress(mProgressStep)
				CheckPauseStatus()
				If mAbortProcessing Then
					UpdateProgress("Aborted processing")
					Exit For
				End If

				If blnSuccess Then
					RaiseEvent NewMessage("Processing completed for database " & strCurrentDB, eMessageTypeConstants.HeaderLine)
				ElseIf blnDBNotFoundReturn Then
					SetLocalError(eDBSchemaExportErrorCodes.DatabaseConnectionError, "Database " & strCurrentDB & " not found on server " & udtSchemaExportOptions.ConnectionInfo.ServerName)
				Else
					SetLocalError(eDBSchemaExportErrorCodes.GeneralError, "Processing failed for server " & udtSchemaExportOptions.ConnectionInfo.ServerName)
				End If

			Next

			Return True

		Catch ex As Exception
			SetLocalError(eDBSchemaExportErrorCodes.GeneralError, "Error exporting DB schema objects: " & udtSchemaExportOptions.OutputFolderPath, ex)
		End Try

		Return False

	End Function

	Private Function ScriptServerObjects(ByRef objSqlServer As Server, ByVal udtSchemaExportOptions As udtSchemaExportOptionsType) As Boolean
		Const PROGRESS_STEP_COUNT As Integer = 2

		' Export the Server Settings and Sql Server Agent jobs

		Dim objScriptOptions As ScriptingOptions

		Dim strOutputFolderPathCurrentServer As String = String.Empty
		Dim blnSuccess As Boolean

		objScriptOptions = GetDefaultScriptOptions()

		Try
			' Construct the path to the output folder
			strOutputFolderPathCurrentServer = Path.Combine(udtSchemaExportOptions.OutputFolderPath, udtSchemaExportOptions.ServerOutputFolderNamePrefix & objSqlServer.Name)

			' Create the folder if it doesn't exist
			If Not Directory.Exists(strOutputFolderPathCurrentServer) Then
				Directory.CreateDirectory(strOutputFolderPathCurrentServer)
			End If

		Catch ex As Exception
			SetLocalError(eDBSchemaExportErrorCodes.DatabaseConnectionError, "Error validating or creating folder " & strOutputFolderPathCurrentServer)
			Return False
		End Try

		Try
			ResetProgress("Exporting Server objects to: " & VBNetRoutines.CompactPathString(udtSchemaExportOptions.OutputFolderPath), PROGRESS_STEP_COUNT)
			ResetSubtaskProgress("Exporting server options")

			' Export the overall server configuration and options (this is quite fast, so we won't increment mProgressStep after this)
			ExportSQLServerConfiguration(objSqlServer, udtSchemaExportOptions, objScriptOptions, strOutputFolderPathCurrentServer)
			If mAbortProcessing Then Exit Try

			' Export the logins
			ExportSQLServerLogins(objSqlServer, udtSchemaExportOptions, objScriptOptions, strOutputFolderPathCurrentServer)
			mProgressStep += 1
			UpdateProgress(mProgressStep)
			If mAbortProcessing Then Exit Try

			' Export the Sql Server Agent Jobs
			ExportSQLServerAgentJobs(objSqlServer, udtSchemaExportOptions, objScriptOptions, strOutputFolderPathCurrentServer)
			mProgressStep += 1
			UpdateProgress(mProgressStep)
			If mAbortProcessing Then Exit Try

			blnSuccess = True

		Catch ex As Exception
			SetLocalError(eDBSchemaExportErrorCodes.DatabaseConnectionError, "Error scripting objects for server " & objSqlServer.Name)
			blnSuccess = False
		End Try

		Return blnSuccess

	End Function

	Public Function ScriptServerAndDBObjects(
	  ByVal udtSchemaExportOptions As udtSchemaExportOptionsType,
	  ByVal lstDatabaseListToProcess As List(Of String),
	  ByVal lstTableNamesForDataExport As List(Of String)) As Boolean

		Dim blnSuccess As Boolean = False

		InitializeLocalVariables(False)

		Try
			blnSuccess = False
			If udtSchemaExportOptions.ConnectionInfo.ServerName Is Nothing OrElse udtSchemaExportOptions.ConnectionInfo.ServerName.Length = 0 Then
				SetLocalError(eDBSchemaExportErrorCodes.ConfigurationError, "Server name is not defined")
			ElseIf lstDatabaseListToProcess Is Nothing OrElse lstDatabaseListToProcess.Count = 0 Then
				If udtSchemaExportOptions.ExportServerSettingsLoginsAndJobs Then
					' No databases are defined, but we are exporting server settings; this is OK
					blnSuccess = True
				Else
					SetLocalError(eDBSchemaExportErrorCodes.ConfigurationError, "Database list to process is empty")
				End If
			Else
				If lstDatabaseListToProcess.Count > 1 Then
					' Force CreateFolderForEachDB to true
					udtSchemaExportOptions.CreateFolderForEachDB = True
				End If
				blnSuccess = True
			End If
		Catch ex As Exception
			SetLocalError(eDBSchemaExportErrorCodes.DatabaseConnectionError, "Error validating the Schema Export Options", ex)
		End Try

		If blnSuccess Then
			' Validate the strings in udtSchemaExportOptions
			ValidateSchemaExportOptions(udtSchemaExportOptions)

			' Confirm that the output folder exists
			If Not Directory.Exists(udtSchemaExportOptions.OutputFolderPath) Then
				SetLocalError(eDBSchemaExportErrorCodes.OutputFolderAccessError, "Output folder not found: " & udtSchemaExportOptions.OutputFolderPath)
				blnSuccess = False
			End If
		End If

		If blnSuccess Then
			ResetProgress("Exporting schema to: " & VBNetRoutines.CompactPathString(udtSchemaExportOptions.OutputFolderPath), 1)
			ResetSubtaskProgress("Connecting to " & udtSchemaExportOptions.ConnectionInfo.ServerName)

			blnSuccess = ConnectToServer(udtSchemaExportOptions.ConnectionInfo)
		End If

		If blnSuccess Then
			If udtSchemaExportOptions.ExportServerSettingsLoginsAndJobs Then
				blnSuccess = ScriptServerObjects(mSqlServer, udtSchemaExportOptions)
				If mAbortProcessing Then blnSuccess = False
			Else
				blnSuccess = True
			End If

			If blnSuccess AndAlso Not lstDatabaseListToProcess Is Nothing AndAlso lstDatabaseListToProcess.Count > 0 Then
				blnSuccess = ScriptDBObjects(mSqlServer, udtSchemaExportOptions, lstDatabaseListToProcess, lstTableNamesForDataExport)
				If mAbortProcessing Then blnSuccess = False
			End If

			' Set the overall progress to Complete
			If blnSuccess AndAlso Not mAbortProcessing Then
				UpdateProgress("Done", 100)
				SetProgressComplete()
			End If
		End If

		Return blnSuccess

	End Function

	Private Sub SetLocalError(ByVal eErrorCode As eDBSchemaExportErrorCodes, ByVal strMessage As String)
		SetLocalError(eErrorCode, strMessage, Nothing)
	End Sub

	Private Sub SetLocalError(ByVal eErrorCode As eDBSchemaExportErrorCodes, ByVal strMessage As String, ByVal exException As Exception)
		Try
			mErrorCode = eErrorCode

			If strMessage Is Nothing Then strMessage = String.Empty
			mStatusMessage = String.Copy(strMessage)

			If Not exException Is Nothing Then
				mStatusMessage &= ": " & exException.Message
			End If

			RaiseEvent NewMessage("Error -- " & mStatusMessage, eMessageTypeConstants.ErrorMessage)
		Catch ex As Exception
			' Ignore errors here
		End Try
	End Sub

	Private Sub SetPauseStatus(ByVal eNewPauseStatus As ePauseStatusConstants)
		mPauseStatus = eNewPauseStatus
		RaiseEvent PauseStatusChange()
	End Sub

	Protected Sub SetProgressComplete()
		UpdateProgress(100)
		RaiseEvent ProgressComplete()
	End Sub

	Protected Sub SetSubtaskProgressComplete()
		UpdateSubtaskProgress(100)
		RaiseEvent SubtaskProgressComplete()
	End Sub

	Private Function SqlServer2005OrNewer(ByVal objDatabase As Database) As Boolean
		Return SqlServer2005OrNewer(objDatabase.Parent)
	End Function

	Private Function SqlServer2005OrNewer(ByVal objServer As Server) As Boolean
		If objServer.Information.Version.Major >= 9 Then
			Return True
		Else
			Return False
		End If
	End Function

	Private Function StringCollectionToList(ByVal objItems As StringCollection) As List(Of String)

		Dim lstInfo = New List(Of String)

		For Each entry As String In objItems
			lstInfo.Add(entry)
		Next

		Return lstInfo

	End Function

	Protected Sub UpdateProgress(ByVal intStepNumber As Integer)
		If mProgressStepCount <= 0 Then
			UpdateProgress(0)
		Else
			UpdateProgress(intStepNumber / CSng(mProgressStepCount) * 100.0!)
		End If
	End Sub
	Protected Sub UpdateProgress(ByVal intStepNumber As Integer, ByVal intStepCount As Integer)
		If intStepCount <= 0 Then
			UpdateProgress(0)
		Else
			UpdateProgress(intStepNumber / CSng(intStepCount) * 100.0!)
		End If
	End Sub

	Protected Sub UpdateProgress(ByVal sngPercentComplete As Single)
		UpdateProgress(Me.ProgressStepDescription, sngPercentComplete)
	End Sub

	Protected Sub UpdateProgress(ByVal strProgressStepDescription As String)
		UpdateProgress(strProgressStepDescription, mProgressPercentComplete)
	End Sub

	Protected Sub UpdateProgress(ByVal strProgressStepDescription As String, ByVal sngPercentComplete As Single)
		mProgressStepDescription = String.Copy(strProgressStepDescription)
		If sngPercentComplete < 0 Then
			sngPercentComplete = 0
		ElseIf sngPercentComplete >= 100 Then
			sngPercentComplete = 100
			mProgressStep = mProgressStepCount
		End If
		mProgressPercentComplete = sngPercentComplete

		RaiseEvent ProgressChanged(Me.ProgressStepDescription, Me.ProgressPercentComplete)
	End Sub

	Protected Sub UpdateSubtaskProgress(ByVal intStepNumber As Integer, ByVal intStepCount As Integer)
		If intStepCount <= 0 Then
			UpdateSubtaskProgress(0)
		Else
			UpdateSubtaskProgress(intStepNumber / CSng(intStepCount) * 100.0!)
		End If
	End Sub

	Protected Sub UpdateSubtaskProgress(ByVal sngPercentComplete As Single)
		UpdateSubtaskProgress(Me.SubtaskProgressStepDescription, sngPercentComplete)
	End Sub

	Protected Sub UpdateSubtaskProgress(ByVal strSubtaskProgressStepDescription As String, ByVal sngPercentComplete As Single)
		mSubtaskProgressStepDescription = String.Copy(strSubtaskProgressStepDescription)
		If sngPercentComplete < 0 Then
			sngPercentComplete = 0
		ElseIf sngPercentComplete > 100 Then
			sngPercentComplete = 100
		End If
		mSubtaskProgressPercentComplete = sngPercentComplete

		RaiseEvent SubtaskProgressChanged(Me.SubtaskProgressStepDescription, Me.SubtaskProgressPercentComplete)
	End Sub

	Private Sub ValidateSchemaExportOptions(ByRef udtSchemaExportOptions As udtSchemaExportOptionsType)
		With udtSchemaExportOptions
			If .OutputFolderPath Is Nothing Then
				.OutputFolderPath = String.Empty
			End If

			If .OutputFolderNamePrefix Is Nothing Then
				.OutputFolderNamePrefix = DEFAULT_DB_OUTPUT_FOLDER_NAME_PREFIX
			End If

			If .ServerOutputFolderNamePrefix Is Nothing Then
				.ServerOutputFolderNamePrefix = DEFAULT_SERVER_OUTPUT_FOLDER_NAME_PREFIX
			End If
		End With
	End Sub


	Private Function WriteTextToFile(ByVal strOutputFolderPath As String, ByVal strObjectName As String, ByVal lstInfo As IEnumerable(Of String)) As Boolean
		' Calls WriteTextToFile with blnAutoAddGoStatements = True
		Return WriteTextToFile(strOutputFolderPath, strObjectName, lstInfo, True)
	End Function

	Private Function WriteTextToFile(ByVal strOutputFolderPath As String, ByVal strObjectName As String, ByVal lstInfo As IEnumerable(Of String), ByVal blnAutoAddGoStatements As Boolean) As Boolean
		Return WriteTextToFile(strOutputFolderPath, strObjectName, lstInfo, blnAutoAddGoStatements, ".sql")
	End Function

	Private Function WriteTextToFile(ByVal strOutputFolderPath As String, ByVal strObjectName As String, ByVal lstInfo As IEnumerable(Of String), ByVal blnAutoAddGoStatements As Boolean, ByVal strFileExtension As String) As Boolean

		Dim strOutFilePath As String = "??"
		Dim swOutFile As StreamWriter

		Try
			' Make sure strObjectName doesn't contain any invalid characters
			strObjectName = CleanNameForOS(strObjectName)

			strOutFilePath = Path.Combine(strOutputFolderPath, strObjectName & strFileExtension)
			swOutFile = New StreamWriter(strOutFilePath, False)

			For Each sqlItem In lstInfo
				swOutFile.WriteLine(sqlItem)

				If blnAutoAddGoStatements Then
					swOutFile.WriteLine("GO")
				End If
			Next
			swOutFile.Close()

		Catch ex As Exception
			SetLocalError(eDBSchemaExportErrorCodes.OutputFolderAccessError, "Error saving file " & strOutFilePath)
			Return False
		End Try

		Return True

	End Function

End Class