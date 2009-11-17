Option Strict On

' This class will export all of the specified object types 
' from a specific Sql Server database
'
' -------------------------------------------------------------------------------
' Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA)
' Program started August 11, 2006
' Copyright 2006, Battelle Memorial Institute.  All Rights Reserved.

' E-mail: matthew.monroe@pnl.gov or matt@alchemistmatt.com
' Website: http://ncrr.pnl.gov/ or http://www.sysbio.org/resources/staff/
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
' Last updated December 6, 2006

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
        Public ExportUserDefinedTypes As Boolean                               ' Only supported in Sql Server 2005 or newer; see SqlServer2005OrNewer

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

    Private mSqlServer As Microsoft.SqlServer.Management.Smo.Server
    Private mSqlServerOptionsCurrent As udtServerConnectionSingleType

    Private mColumnCharNonStandardRegEx As System.Text.RegularExpressions.Regex
    Private mNonStandardOSChars As System.Text.RegularExpressions.Regex

    Protected mTableNamesToAutoSelect() As String

    ' Note: Must contain valid RegEx statements (tested case-insensitive)
    Protected mTableNameAutoSelectRegEx() As String

    Private mErrorCode As eDBSchemaExportErrorCodes
    Private mStatusMessage As String

    Private mAbortProcessing As Boolean
    Private mPauseStatus As ePauseStatusConstants
#End Region

#Region "Progress Events and Variables"
    Public Event ProgressReset()
    Public Event ProgressChanged(ByVal taskDescription As String, ByVal percentComplete As Single)     ' PercentComplete ranges from 0 to 100, but can contain decimal percentage values
    Public Event ProgressComplete()

    Public Event SubtaskProgressReset()
    Public Event SubtaskProgressChanged(ByVal taskDescription As String, ByVal percentComplete As Single)     ' PercentComplete ranges from 0 to 100, but can contain decimal percentage values
    Public Event SubtaskProgressComplete()

    Protected mProgressStepDescription As String = String.Empty
    Protected mProgressPercentComplete As Single                ' Ranges from 0 to 100, but can contain decimal percentage values
    Protected mProgressStep As Integer
    Protected mProgressStepCount As Integer

    Protected mSubtaskProgressStepDescription As String = String.Empty
    Protected mSubtaskProgressPercentComplete As Single         ' Ranges from 0 to 100, but can contain decimal percentage values
#End Region

#Region "Properties"

    Public Property TableNamesToAutoSelect() As String()
        Get
            Return mTableNamesToAutoSelect
        End Get
        Set(ByVal value As String())
            If value Is Nothing Then
                ReDim mTableNamesToAutoSelect(-1)
            Else
                ReDim mTableNamesToAutoSelect(value.Length - 1)
                Array.Copy(value, mTableNamesToAutoSelect, value.Length)
            End If
        End Set
    End Property

    Public Property TableNameAutoSelectRegEx() As String()
        Get
            Return mTableNameAutoSelectRegEx
        End Get
        Set(ByVal value As String())
            If value Is Nothing Then
                ReDim mTableNameAutoSelectRegEx(-1)
            Else
                ReDim mTableNameAutoSelectRegEx(value.Length - 1)
                Array.Copy(value, mTableNameAutoSelectRegEx, value.Length)
            End If
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

    Public Overridable ReadOnly Property ProgressStepDescription() As String
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

    Public Overridable ReadOnly Property SubtaskProgressStepDescription() As String
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

    Private Function AutoSelectTableNamesForDataExport(ByVal objDatabase As Microsoft.SqlServer.Management.Smo.Database, ByRef strTableNamesForDataExport() As String, ByRef intMaximumDataRowsToExport() As Integer, ByRef udtSchemaExportOptions As udtSchemaExportOptionsType) As Boolean

        Dim objRegExOptions As System.Text.RegularExpressions.RegexOptions
        Dim objRegExArray() As System.Text.RegularExpressions.Regex

        ' Stores the table names and maximum number of data rows to export (0 means all rows)
        Dim htTableNames As Hashtable
        Dim objEnum As System.Collections.IDictionaryEnumerator

        Dim intIndex As Integer
        Dim intCompareIndex As Integer

        Dim blnNewTableFound As Boolean

        Dim strTableName As String

        Dim dtTables As System.Data.DataTable
        Dim objRow As System.Data.DataRow

        Try
            ResetSubtaskProgress("Auto-selecting tables to export data from")

            blnNewTableFound = False

            htTableNames = New Hashtable

            ' Copy the table names from strTableNamesForDataExport to htTableNames
            ' Store 0 for the hash value since we want to export all of the data rows from the tables in strTableNamesForDataExport
            ' Simultaneously, populate intMaximumDataRowsToExport
            If Not strTableNamesForDataExport Is Nothing Then
                ReDim intMaximumDataRowsToExport(strTableNamesForDataExport.Length - 1)

                For intIndex = 0 To strTableNamesForDataExport.Length - 1
                    htTableNames.Add(strTableNamesForDataExport(intIndex), 0)
                Next intIndex
            End If

            ' Copy the table names from mTableNamesToAutoSelect to htTableNames (if not yet present)
            If Not mTableNamesToAutoSelect Is Nothing Then
                For intIndex = 0 To mTableNamesToAutoSelect.Length - 1
                    If Not htTableNames.Contains(mTableNamesToAutoSelect(intIndex)) Then
                        htTableNames.Add(mTableNamesToAutoSelect(intIndex), DATA_ROW_COUNT_WARNING_THRESHOLD)
                        blnNewTableFound = True
                    End If
                Next intIndex
            End If

            ' Initialize objRegExArray (we'll fill it below if blnAutoHiglightRows = True)
            objRegExOptions = System.Text.RegularExpressions.RegexOptions.Compiled Or _
                              System.Text.RegularExpressions.RegexOptions.IgnoreCase Or _
                              System.Text.RegularExpressions.RegexOptions.Singleline

            If Not mTableNameAutoSelectRegEx Is Nothing Then
                ReDim objRegExArray(mTableNameAutoSelectRegEx.Length - 1)
                For intIndex = 0 To mTableNameAutoSelectRegEx.Length - 1
                    objRegExArray(intIndex) = New System.Text.RegularExpressions.Regex(mTableNameAutoSelectRegEx(intIndex), objRegExOptions)
                Next intIndex

                ' Step through the table names for this DB and compare to the RegEx values
                dtTables = objDatabase.EnumObjects(Microsoft.SqlServer.Management.Smo.DatabaseObjectTypes.Table, Microsoft.SqlServer.Management.Smo.SortOrder.Name)

                For Each objRow In dtTables.Rows
                    strTableName = objRow.Item("Name").ToString

                    For intCompareIndex = 0 To mTableNameAutoSelectRegEx.Length - 1
                        If objRegExArray(intCompareIndex).Match(strTableName).Success Then
                            If Not htTableNames.Contains(strTableName) Then
                                htTableNames.Add(strTableName, DATA_ROW_COUNT_WARNING_THRESHOLD)
                                blnNewTableFound = True
                            End If
                            Exit For
                        End If
                    Next intCompareIndex
                Next objRow
            End If

            If blnNewTableFound Then
                ReDim strTableNamesForDataExport(htTableNames.Count - 1)
                ReDim intMaximumDataRowsToExport(htTableNames.Count - 1)

                intIndex = 0
                objEnum = htTableNames.GetEnumerator
                Do While objEnum.MoveNext
                    strTableNamesForDataExport(intIndex) = CStr(objEnum.Key)
                    intMaximumDataRowsToExport(intIndex) = CInt(objEnum.Value)
                    intIndex += 1
                Loop

                ' Sort strTableNamesForDataExport and sort intMaximumDataRowsToExport parallel to it
                Array.Sort(strTableNamesForDataExport, intMaximumDataRowsToExport)
            End If

        Catch ex As Exception
            SetLocalError(eDBSchemaExportErrorCodes.ConfigurationError, "Error in AutoSelectTableNamesForDataExport")
            Return False
        End Try

        Return True
    End Function

    Private Sub CheckPauseStatus()
        If mPauseStatus = ePauseStatusConstants.PauseRequested Then
            SetPauseStatus(ePauseStatusConstants.Paused)
        End If

        Do While mPauseStatus = ePauseStatusConstants.Paused And Not mAbortProcessing
            System.Threading.Thread.Sleep(150)
        Loop

        SetPauseStatus(ePauseStatusConstants.Unpaused)
    End Sub

    Private Function CleanNameForOS(ByVal strName As String) As String
        ' Replace any invalid characters in strName with underscores

        Return mNonStandardOSChars.Replace(strName, "_")
    End Function

    Private Function CleanSqlScript(ByRef objStringCollection As System.Collections.Specialized.StringCollection, ByRef udtSchemaExportOptions As udtSchemaExportOptionsType) As System.Collections.Specialized.StringCollection
        ' Calls CleanSqlScript with blnRemoveAllOccurrences = False and blnRemoveDuplicateHeaderLine = FAlse
        Return CleanSqlScript(objStringCollection, udtSchemaExportOptions, False, False)
    End Function

    Private Function CleanSqlScript(ByRef objStringCollection As System.Collections.Specialized.StringCollection, ByRef udtSchemaExportOptions As udtSchemaExportOptionsType, ByVal blnRemoveAllScriptDateOccurrences As Boolean, ByVal blnRemoveDuplicateHeaderLine As Boolean) As System.Collections.Specialized.StringCollection

        Dim intIndex As Integer
        Dim intIndexStart As Integer
        Dim intIndexStartCurrent As Integer
        Dim intIndexEndCurrent As Integer
        Dim intFinalSearchIndex As Integer

        Dim intIndexNextCrLf As Integer
        Dim intFirstCrLf As Integer

        Dim strText As String
        Dim blnTextChanged As Boolean

        Dim chWhiteSpaceChars() As Char = New Char() {" "c, ControlChars.Tab}

        Try
            If Not udtSchemaExportOptions.IncludeTimestampInScriptFileHeader Then
                ' Look for and remove the timestamp from the first line of the Sql script
                If Not objStringCollection Is Nothing AndAlso objStringCollection.Count > 0 Then
                    ' Look for and remove the text  "Script Date: 08/14/2006 20:14:31" prior to the each "******/"
                    ' If blnRemoveAllOccurrences = True, then searches for all occurrences
                    ' If blnRemoveAllOccurrences = False, then does not look past the first 
                    '   carriage return of each entry in objStringCollection

                    For intIndex = 0 To objStringCollection.Count - 1
                        strText = objStringCollection(intIndex)

                        intIndexStart = 0
                        If blnRemoveAllScriptDateOccurrences Then
                            intFinalSearchIndex = strText.Length - 1
                        Else
                            ' Find the first CrLf after the first non-blank line in strText
                            Do
                                intFinalSearchIndex = strText.IndexOf(ControlChars.NewLine, intIndexStart)
                                If intFinalSearchIndex = intIndexStart Then
                                    intIndexStart += 2
                                Else
                                    Exit Do
                                End If
                            Loop While intFinalSearchIndex >= 0 AndAlso intFinalSearchIndex < intIndexStart AndAlso intIndexStart < strText.Length

                            If intFinalSearchIndex < 0 Then intFinalSearchIndex = strText.Length - 1
                        End If


                        Do
                            intIndexStartCurrent = strText.IndexOf(COMMENT_SCRIPT_DATE_TEXT, intIndexStart)
                            If intIndexStartCurrent > 0 AndAlso intIndexStartCurrent <= intFinalSearchIndex Then
                                intIndexNextCrLf = strText.IndexOf(ControlChars.NewLine, intIndexStartCurrent)
                                If intIndexNextCrLf <= 0 Then
                                    intIndexNextCrLf = strText.Length - 1
                                End If
                                intIndexEndCurrent = strText.IndexOf(COMMENT_END_TEXT_SHORT, intIndexStartCurrent)

                                If intIndexEndCurrent > intIndexStartCurrent And intIndexEndCurrent <= intFinalSearchIndex Then
                                    strText = strText.Substring(0, intIndexStartCurrent).TrimEnd(chWhiteSpaceChars) & COMMENT_END_TEXT & _
                                              strText.Substring(intIndexEndCurrent + COMMENT_END_TEXT_SHORT.Length)
                                    blnTextChanged = True
                                End If
                            End If
                        Loop While blnRemoveAllScriptDateOccurrences And intIndexStartCurrent > 0

                        If blnRemoveDuplicateHeaderLine Then
                            intFirstCrLf = strText.IndexOf(ControlChars.NewLine, 0)
                            If intFirstCrLf > 0 AndAlso intFirstCrLf < strText.Length Then
                                intIndexNextCrLf = strText.IndexOf(ControlChars.NewLine, intFirstCrLf + 1)

                                If intIndexNextCrLf > intFirstCrLf Then
                                    If strText.Substring(0, intFirstCrLf) = strText.Substring(intFirstCrLf + 2, intIndexNextCrLf - intFirstCrLf - 2) Then
                                        strText = strText.Substring(intFirstCrLf + 2)
                                        blnTextChanged = True
                                    End If
                                End If
                            End If
                        End If

                        If blnTextChanged Then
                            objStringCollection(intIndex) = String.Copy(strText)
                        End If

                    Next intIndex
                End If
            End If

        Catch ex As Exception
            ' Leave objStringCollection unchanged
        End Try


        Return objStringCollection

    End Function

    Public Function ConnectToServer(ByVal udtServerConnectionInfo As udtServerConnectionInfoType) As Boolean
        Dim blnConnected As Boolean

        Try
            blnConnected = False

            ' Initialize the current connection options
            If mSqlServer Is Nothing Then
                ResetSqlServerConnection(mSqlServerOptionsCurrent)
            Else
                If mSqlServerOptionsCurrent.Connected AndAlso Not mSqlServer Is Nothing AndAlso mSqlServer.State = Microsoft.SqlServer.Management.Smo.SqlSmoState.Existing Then
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

    Private Function ExportDBObjectsUsingSMO(ByRef objSqlServer As Microsoft.SqlServer.Management.Smo.Server, ByVal strDatabaseName As String, ByVal strTableNamesForDataExport() As String, ByRef udtSchemaExportOptions As udtSchemaExportOptionsType, ByRef blnDBNotFoundReturn As Boolean) As Boolean

        Dim objDatabase As Microsoft.SqlServer.Management.Smo.Database

        Dim objScriptOptions As Microsoft.SqlServer.Management.Smo.ScriptingOptions

        Dim intMaximumDataRowsToExport() As Integer
        ReDim intMaximumDataRowsToExport(-1)

        Dim udtWorkingParams As udtDBExportWorkingParamsType

        Dim blnSuccess As Boolean

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
                udtWorkingParams.OutputFolderPathCurrentDB = System.IO.Path.Combine(udtSchemaExportOptions.OutputFolderPath, udtSchemaExportOptions.OutputFolderNamePrefix & objDatabase.Name)
            Else
                udtWorkingParams.OutputFolderPathCurrentDB = String.Copy(udtSchemaExportOptions.OutputFolderPath)
            End If

            ' Create the folder if it doesn't exist
            If Not System.IO.Directory.Exists(udtWorkingParams.OutputFolderPathCurrentDB) Then
                System.IO.Directory.CreateDirectory(udtWorkingParams.OutputFolderPathCurrentDB)
            End If

            If udtSchemaExportOptions.AutoSelectTableNamesForDataExport Then
                blnSuccess = AutoSelectTableNamesForDataExport(objDatabase, strTableNamesForDataExport, intMaximumDataRowsToExport, udtSchemaExportOptions)
                If Not blnSuccess Then
                    Return False
                End If
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

            If Not strTableNamesForDataExport Is Nothing Then
                udtWorkingParams.ProcessCountExpected += strTableNamesForDataExport.Length
            End If

            udtWorkingParams.CountObjectsOnly = False
            If udtWorkingParams.ProcessCount > 0 Then
                ExportDBObjectsWork(objDatabase, objScriptOptions, udtSchemaExportOptions, udtWorkingParams)
            End If

            ' Export data from tables specified by strTableNamesForDataExport; maximum row counts are specified by intMaximumDataRowsToExport
            blnSuccess = ExportDBTableData(objDatabase, strTableNamesForDataExport, intMaximumDataRowsToExport, udtSchemaExportOptions, udtWorkingParams)

        Catch ex As Exception
            SetLocalError(eDBSchemaExportErrorCodes.DatabaseConnectionError, "Error scripting objects in database " & strDatabaseName)
            blnSuccess = False
        End Try

        Return blnSuccess
    End Function

    Private Sub ExportDBObjectsWork(ByRef objDatabase As Microsoft.SqlServer.Management.Smo.Database, _
                                     ByRef objScriptOptions As Microsoft.SqlServer.Management.Smo.ScriptingOptions, _
                                     ByRef udtSchemaExportOptions As udtSchemaExportOptionsType, _
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

    Private Sub ExportDBSchemasAndRoles(ByRef objDatabase As Microsoft.SqlServer.Management.Smo.Database, _
                                        ByRef udtSchemaExportOptions As udtSchemaExportOptionsType, _
                                        ByRef objScriptOptions As Microsoft.SqlServer.Management.Smo.ScriptingOptions, _
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
                                CleanSqlScript(objDatabase.Script(objScriptOptions), udtSchemaExportOptions))
            Catch ex As Exception
                ' User likely doesn't have privilege to script the DB; ignore the error
                RaiseEvent NewMessage("Unable to script DB " & objDatabase.Name & ": " & ex.Message, eMessageTypeConstants.ErrorMessage)
            End Try
            udtWorkingParams.ProcessCount += 1

            If SqlServer2005OrNewer(objDatabase) Then
                For intIndex = 0 To objDatabase.Schemas.Count - 1
                    If ExportSchema(objDatabase.Schemas(intIndex)) Then
                        WriteTextToFile(udtWorkingParams.OutputFolderPathCurrentDB, "Schema_" & objDatabase.Schemas(intIndex).Name, _
                                        CleanSqlScript(objDatabase.Schemas(intIndex).Script(objScriptOptions), udtSchemaExportOptions))

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
                                    CleanSqlScript(objDatabase.Roles(intIndex).Script(objScriptOptions), udtSchemaExportOptions))

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

    Private Sub ExportDBTables(ByRef objDatabase As Microsoft.SqlServer.Management.Smo.Database, _
                               ByRef udtSchemaExportOptions As udtSchemaExportOptionsType, _
                               ByRef objScriptOptions As Microsoft.SqlServer.Management.Smo.ScriptingOptions, _
                               ByRef udtWorkingParams As udtDBExportWorkingParamsType)

        Const SYNC_OBJ_TABLE_PREFIX As String = "syncobj_0x"

        Dim objScripter As Microsoft.SqlServer.Management.Smo.Scripter
        Dim objSMOObject() As Microsoft.SqlServer.Management.Smo.SqlSmoObject
        Dim objTable As Microsoft.SqlServer.Management.Smo.Table

        Dim blnIncludeTable As Boolean

        If udtWorkingParams.CountObjectsOnly Then
            ' Note: objDatabase.Tables includes system tables, so udtWorkingParams.ProcessCount will be 
            '       an overestimate if udtSchemaExportOptions.IncludeSystemObjects = False
            udtWorkingParams.ProcessCount += objDatabase.Tables.Count
        Else
            ' Initialize the scripter and objSMOObject()
            objScripter = New Microsoft.SqlServer.Management.Smo.Scripter(mSqlServer)
            objScripter.Options = objScriptOptions
            ReDim objSMOObject(0)

            For Each objTable In objDatabase.Tables
                blnIncludeTable = True
                If Not udtSchemaExportOptions.IncludeSystemObjects Then
                    If objTable.IsSystemObject Then
                        blnincludetable = False
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
                                    CleanSqlScript(objScripter.Script(objSMOObject), udtSchemaExportOptions))
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

    Private Sub ExportDBUserDefinedDataTypes(ByRef objDatabase As Microsoft.SqlServer.Management.Smo.Database, _
                                             ByRef udtSchemaExportOptions As udtSchemaExportOptionsType, _
                                             ByRef objScriptOptions As Microsoft.SqlServer.Management.Smo.ScriptingOptions, _
                                             ByRef udtWorkingParams As udtDBExportWorkingParamsType)

        Dim intItemCount As Integer

        If udtWorkingParams.CountObjectsOnly Then
            udtWorkingParams.ProcessCount += objDatabase.UserDefinedDataTypes.Count
        Else
            intItemCount = ScriptCollectionOfObjects(objDatabase.UserDefinedDataTypes, udtSchemaExportOptions, objScriptOptions, udtWorkingParams.ProcessCountExpected, udtWorkingParams.OutputFolderPathCurrentDB)
            udtWorkingParams.ProcessCount += intItemCount
        End If
    End Sub

    Private Sub ExportDBUserDefinedTypes(ByRef objDatabase As Microsoft.SqlServer.Management.Smo.Database, _
                                         ByRef udtSchemaExportOptions As udtSchemaExportOptionsType, _
                                         ByRef objScriptOptions As Microsoft.SqlServer.Management.Smo.ScriptingOptions, _
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

    Private Sub ExportDBViewsProcsAndUDFs(ByRef objDatabase As Microsoft.SqlServer.Management.Smo.Database, _
                                          ByRef udtSchemaExportOptions As udtSchemaExportOptionsType, _
                                          ByRef objScriptOptions As Microsoft.SqlServer.Management.Smo.ScriptingOptions, _
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

        Dim objScripter As Microsoft.SqlServer.Management.Smo.Scripter
        Dim objSMOObject() As Microsoft.SqlServer.Management.Smo.SqlSmoObject

        ''Dim objURNList() As Microsoft.SqlServer.Management.Smo.Urn
        ''ReDim objURNList(0)

        Dim dsObjects As DataSet
        Dim objRow As DataRow
        Dim strObjectSchema As String
        Dim strObjectName As String

        Dim intObjectIterator As Integer

        Dim strSql As String

        ' Initialize the scripter and objSMOObject()
        objScripter = New Microsoft.SqlServer.Management.Smo.Scripter(mSqlServer)
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
                                            CleanSqlScript(objScripter.Script(objSMOObject), udtSchemaExportOptions))
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

    Private Function ExportDBTableData(ByRef objDatabase As Microsoft.SqlServer.Management.Smo.Database, _
                                       ByRef strTableNamesForDataExport() As String, _
                                       ByRef intMaximumDataRowsToExport() As Integer, _
                                       ByRef udtSchemaExportOptions As udtSchemaExportOptionsType, _
                                       ByRef udtWorkingParams As udtDBExportWorkingParamsType) As Boolean

        Dim objTable As Microsoft.SqlServer.Management.Smo.Table
        Dim objColumn As Microsoft.SqlServer.Management.Smo.Column

        Dim intTableIndex As Integer
        Dim intByteIndex As Integer

        Dim strSql As String
        Dim objTableRows As System.Collections.Specialized.StringCollection

        Dim dsCurrentTable As System.Data.DataSet
        Dim eColumnType() As eDataColumnTypeConstants
        Dim objRow As System.Data.DataRow

        Dim objCurrentRow As System.Text.StringBuilder

        Dim strHeader As String
        Dim strInsertIntoLine As String = String.Empty
        Dim chColSepChar As Char

        Dim bytData() As Byte

        Dim intColumnCount As Integer
        Dim intColumnIndex As Integer

        Dim blnIdentityColumnFound As Boolean
        Dim blnTableFound As Boolean
        Dim blnDataFound As Boolean
        Dim blnSuccess As Boolean

        Try
            If Not strTableNamesForDataExport Is Nothing AndAlso strTableNamesForDataExport.Length > 0 Then
                objCurrentRow = New System.Text.StringBuilder

                For intTableIndex = 0 To strTableNamesForDataExport.Length - 1

                    mSubtaskProgressStepDescription = "Exporting data from " & strTableNamesForDataExport(intTableIndex)
                    UpdateSubtaskProgress(udtWorkingParams.ProcessCount, udtWorkingParams.ProcessCountExpected)

                    blnTableFound = False
                    If objDatabase.Tables.Contains(strTableNamesForDataExport(intTableIndex)) Then
                        objTable = objDatabase.Tables(strTableNamesForDataExport(intTableIndex))
                        blnTableFound = True
                    ElseIf objDatabase.Tables.Contains(strTableNamesForDataExport(intTableIndex), "dbo") Then
                        objTable = objDatabase.Tables(strTableNamesForDataExport(intTableIndex), "dbo")
                        blnTableFound = True
                    End If

                    If blnTableFound Then
                        ' See if any of the columns in the table is an identity column
                        blnIdentityColumnFound = False
                        For Each objColumn In objTable.Columns
                            If objColumn.Identity Then
                                blnIdentityColumnFound = True
                                Exit For
                            End If
                        Next

                        ' Export the data from objTable, possibly limiting the number of rows to export
                        strSql = "SELECT "
                        If Not intMaximumDataRowsToExport Is Nothing AndAlso intMaximumDataRowsToExport.Length > intTableIndex Then
                            If intMaximumDataRowsToExport(intTableIndex) > 0 Then
                                strSql &= "TOP " & intMaximumDataRowsToExport(intTableIndex).ToString
                            End If
                        End If
                        strSql &= " * FROM [" & objTable.Name & "]"

                        ' Read method #1: Populate a DataSet
                        dsCurrentTable = objDatabase.ExecuteWithResults(strSql)

                        objTableRows = New System.Collections.Specialized.StringCollection()

                        strHeader = COMMENT_START_TEXT & "Object:  Table [" & objTable.Name & "]"
                        If udtSchemaExportOptions.IncludeTimestampInScriptFileHeader Then
                            strHeader &= "    " & COMMENT_SCRIPT_DATE_TEXT & GetTimeStamp()
                        End If
                        strHeader &= COMMENT_END_TEXT
                        objTableRows.Add(strHeader)

                        objTableRows.Add(COMMENT_START_TEXT & "RowCount: " & objTable.RowCount & COMMENT_END_TEXT)

                        intColumnCount = dsCurrentTable.Tables(0).Columns.Count
                        ReDim eColumnType(intColumnCount - 1)

                        ' Construct the column name list and determine the column data types
                        objCurrentRow.Length = 0
                        For intColumnIndex = 0 To intColumnCount - 1
                            ' Initially assume the column's data type is numeric
                            eColumnType(intColumnIndex) = eDataColumnTypeConstants.Numeric

                            ' Now check for other data types
                            If dsCurrentTable.Tables(0).Columns(intColumnIndex).DataType Is System.Type.GetType("System.String") Then
                                eColumnType(intColumnIndex) = eDataColumnTypeConstants.Text

                            ElseIf dsCurrentTable.Tables(0).Columns(intColumnIndex).DataType Is System.Type.GetType("System.DateTime") Then
                                ' Date column
                                eColumnType(intColumnIndex) = eDataColumnTypeConstants.DateTime

                            ElseIf dsCurrentTable.Tables(0).Columns(intColumnIndex).DataType Is System.Type.GetType("System.Byte[]") Then
                                Select Case objTable.Columns(intColumnIndex).DataType.Name
                                    Case "image"
                                        eColumnType(intColumnIndex) = eDataColumnTypeConstants.ImageObject
                                    Case "timestamp"
                                        eColumnType(intColumnIndex) = eDataColumnTypeConstants.BinaryArray
                                    Case Else
                                        eColumnType(intColumnIndex) = eDataColumnTypeConstants.BinaryArray
                                End Select

                            ElseIf dsCurrentTable.Tables(0).Columns(intColumnIndex).DataType Is System.Type.GetType("System.Guid") Then
                                eColumnType(intColumnIndex) = eDataColumnTypeConstants.GUID

                            ElseIf dsCurrentTable.Tables(0).Columns(intColumnIndex).DataType Is System.Type.GetType("System.Boolean") Then
                                ' This may be a binary column
                                Select Case objTable.Columns(intColumnIndex).DataType.Name
                                    Case "binary", "bit"
                                        eColumnType(intColumnIndex) = eDataColumnTypeConstants.BinaryByte
                                    Case Else
                                        eColumnType(intColumnIndex) = eDataColumnTypeConstants.Text
                                End Select

                            ElseIf dsCurrentTable.Tables(0).Columns(intColumnIndex).DataType Is System.Type.GetType("System.Object") Then
                                Select Case objTable.Columns(intColumnIndex).DataType.Name
                                    Case "sql_variant"
                                        eColumnType(intColumnIndex) = eDataColumnTypeConstants.SqlVariant
                                    Case Else
                                        eColumnType(intColumnIndex) = eDataColumnTypeConstants.GeneralObject
                                End Select

                            End If

                            If udtSchemaExportOptions.SaveDataAsInsertIntoStatements Then
                                objCurrentRow.Append(PossiblyQuoteColumnName(dsCurrentTable.Tables(0).Columns(intColumnIndex).ColumnName))
                                If intColumnIndex < intColumnCount - 1 Then
                                    objCurrentRow.Append(", ")
                                End If
                            Else
                                objCurrentRow.Append(dsCurrentTable.Tables(0).Columns(intColumnIndex).ColumnName)
                                If intColumnIndex < intColumnCount - 1 Then
                                    objCurrentRow.Append(ControlChars.Tab)
                                End If
                            End If
                        Next

                        If udtSchemaExportOptions.SaveDataAsInsertIntoStatements Then
                            ' Future capability:
                            ''Select Case udtSchemaExportOptions.DatabaseTypeForInsertInto
                            ''    Case eTargetDatabaseTypeConstants.SqlServer
                            ''    Case Else
                            ''        ' Unsupported mode
                            ''End Select

                            If blnIdentityColumnFound Then
                                strInsertIntoLine = "INSERT INTO [" & objTable.Name & "] (" & objCurrentRow.ToString & ") VALUES ("
                                objTableRows.Add("SET IDENTITY_INSERT [" & objTable.Name & "] ON")
                            Else
                                ' Identity column not present; no need to explicitly list the column names
                                strInsertIntoLine = "INSERT INTO [" & objTable.Name & "] VALUES ("

                                ' However, we'll display the column names in the output file
                                objTableRows.Add(COMMENT_START_TEXT & "Columns: " & objCurrentRow.ToString & COMMENT_END_TEXT)
                            End If
                            chColSepChar = ","c
                        Else
                            objTableRows.Add(objCurrentRow.ToString)
                            chColSepChar = ControlChars.Tab
                        End If


                        For Each objRow In dsCurrentTable.Tables(0).Rows
                            objCurrentRow.Length = 0
                            If udtSchemaExportOptions.SaveDataAsInsertIntoStatements Then
                                objCurrentRow.Append(strInsertIntoLine)
                            End If

                            For intColumnIndex = 0 To intColumnCount - 1
                                Select Case eColumnType(intColumnIndex)
                                    Case eDataColumnTypeConstants.Numeric
                                        objCurrentRow.Append(objRow.Item(intColumnIndex).ToString)
                                    Case eDataColumnTypeConstants.Text, eDataColumnTypeConstants.DateTime, eDataColumnTypeConstants.GUID
                                        If udtSchemaExportOptions.SaveDataAsInsertIntoStatements Then
                                            objCurrentRow.Append(PossiblyQuoteText(objRow.Item(intColumnIndex).ToString))
                                        Else
                                            objCurrentRow.Append(objRow.Item(intColumnIndex).ToString)
                                        End If

                                    Case eDataColumnTypeConstants.BinaryArray
                                        Try
                                            bytData = CType(CType(objRow.Item(intColumnIndex), Array), Byte())

                                            ' Convert the bytes to a string; however, do not write any leading zeroes
                                            ' The string will be of the form '0x020D89'
                                            objCurrentRow.Append("0x")

                                            blnDataFound = False
                                            For intByteIndex = 0 To bytData.Length - 1
                                                If blnDataFound OrElse bytData(intByteIndex) <> 0 Then
                                                    blnDataFound = True
                                                    ' Convert the byte to Hex (0 to 255 -> 00 to FF)
                                                    objCurrentRow.Append(bytData(intByteIndex).ToString("X2"))
                                                End If
                                            Next intByteIndex

                                            If Not blnDataFound Then
                                                objCurrentRow.Append("00")
                                            End If

                                        Catch ex As Exception
                                            objCurrentRow.Append("[Byte]")
                                        End Try

                                    Case eDataColumnTypeConstants.BinaryByte
                                        Try
                                            objCurrentRow.Append("0x" & System.Convert.ToByte(objRow.Item(intColumnIndex)).ToString("X2"))
                                        Catch ex As Exception
                                            objCurrentRow.Append("[Byte]")
                                        End Try
                                    Case eDataColumnTypeConstants.ImageObject
                                        objCurrentRow.Append("[Image]")
                                    Case eDataColumnTypeConstants.GeneralObject
                                        objCurrentRow.Append("[Object]")
                                    Case eDataColumnTypeConstants.SqlVariant
                                        objCurrentRow.Append("[Sql_Variant]")

                                    Case Else
                                        ' No need to quote
                                        objCurrentRow.Append(objRow.Item(intColumnIndex).ToString)
                                End Select

                                If intColumnIndex < intColumnCount - 1 Then
                                    objCurrentRow.Append(chColSepChar)
                                End If
                            Next
                            If udtSchemaExportOptions.SaveDataAsInsertIntoStatements Then
                                objCurrentRow.Append(")")
                            End If

                            objTableRows.Add(objCurrentRow.ToString)
                        Next

                        If blnIdentityColumnFound AndAlso udtSchemaExportOptions.SaveDataAsInsertIntoStatements Then
                            objTableRows.Add("SET IDENTITY_INSERT [" & objTable.Name & "] OFF")
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
                                        objTableRows, False)
                    End If

                    udtWorkingParams.ProcessCount += 1
                    CheckPauseStatus()
                    If mAbortProcessing Then
                        UpdateProgress("Aborted processing")
                        Exit Function
                    End If
                Next intTableIndex
                SetSubtaskProgressComplete()
            End If

            blnSuccess = True
        Catch ex As Exception
            SetLocalError(eDBSchemaExportErrorCodes.DatabaseConnectionError, "Error in ExportDBTableData", ex)
            blnSuccess = False
        End Try

        Return blnSuccess

    End Function

    Private Function ExportRole(ByVal objDatabaseRole As Microsoft.SqlServer.Management.Smo.DatabaseRole) As Boolean
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

    Private Function ExportSchema(ByVal objDatabaseSchema As Microsoft.SqlServer.Management.Smo.Schema) As Boolean
        Static strSchemaToIgnore() As String
        Dim blnExportSchema As Boolean

        If strSchemaToIgnore Is Nothing Then
            ReDim strSchemaToIgnore(12)

            ' Make sure each of these names is lowercase since we convert 
            '  the schema name to lower case when searching strSchemaToIgnore
            strSchemaToIgnore(0) = "db_accessadmin"
            strSchemaToIgnore(1) = "db_backupoperator"
            strSchemaToIgnore(2) = "db_datareader"
            strSchemaToIgnore(3) = "db_datawriter"
            strSchemaToIgnore(4) = "db_ddladmin"
            strSchemaToIgnore(5) = "db_denydatareader"
            strSchemaToIgnore(6) = "db_denydatawriter"
            strSchemaToIgnore(7) = "db_owner"
            strSchemaToIgnore(8) = "db_securityadmin"
            strSchemaToIgnore(9) = "dbo"
            strSchemaToIgnore(10) = "guest"
            strSchemaToIgnore(11) = "information_schema"
            strSchemaToIgnore(12) = "sys"

            Array.Sort(strSchemaToIgnore)
        End If

        Try
            If Array.BinarySearch(strSchemaToIgnore, objDatabaseSchema.Name.ToLower) < 0 Then
                blnExportSchema = True
            Else
                blnExportSchema = False
            End If
        Catch ex As Exception
            blnExportSchema = False
        End Try

        Return blnExportSchema

    End Function
    Private Sub AppendToStringCollection(ByRef objStringCollection As System.Collections.Specialized.StringCollection, ByVal PropertyName As String, ByVal PropertyValue As String)
        If Not (PropertyName Is Nothing Or PropertyValue Is Nothing) Then
            objStringCollection.Add(PropertyName & "=" & PropertyValue)
        End If
    End Sub

    Private Sub AppendToStringCollection(ByRef objStringCollection As System.Collections.Specialized.StringCollection, ByVal PropertyName As String, ByVal PropertyValue As Integer)
        If Not PropertyName Is Nothing Then
            objStringCollection.Add(PropertyName & "=" & PropertyValue.ToString)
        End If
    End Sub

    Private Sub AppendToStringCollection(ByRef objStringCollection As System.Collections.Specialized.StringCollection, ByVal PropertyName As String, ByVal PropertyValue As Boolean)
        If Not PropertyName Is Nothing Then
            objStringCollection.Add(PropertyName & "=" & PropertyValue.ToString)
        End If
    End Sub

    Private Sub AppendToStringCollection(ByRef objStringCollection As System.Collections.Specialized.StringCollection, ByRef objConfigProperty As Microsoft.SqlServer.Management.Smo.ConfigProperty)
        If Not objConfigProperty Is Nothing AndAlso Not objConfigProperty.DisplayName Is Nothing Then
            objStringCollection.Add(objConfigProperty.DisplayName & "=" & objConfigProperty.ConfigValue)
        End If
    End Sub

    Private Sub ExportSQLServerConfiguration(ByRef objSqlServer As Microsoft.SqlServer.Management.Smo.Server, _
                                             ByRef udtSchemaExportOptions As udtSchemaExportOptionsType, _
                                             ByRef objScriptOptions As Microsoft.SqlServer.Management.Smo.ScriptingOptions, _
                                             ByVal strOutputFolderPathCurrentServer As String)

        Dim objStringCollection As New System.Collections.Specialized.StringCollection

        ' Do not include a Try block in this Function; let the calling function handle errors

        ' First save the Server Information to file ServerInformation
        objStringCollection.Clear()
        With objSqlServer.Information
            objStringCollection.Add("[Server Information for " & objSqlServer.Name & "]")
            AppendToStringCollection(objStringCollection, "Collation", .Collation)
            AppendToStringCollection(objStringCollection, "Edition", .Edition)
            AppendToStringCollection(objStringCollection, "ErrorLogPath", .ErrorLogPath)
            AppendToStringCollection(objStringCollection, "IsCaseSensitive", .IsCaseSensitive)
            AppendToStringCollection(objStringCollection, "IsClustered", .IsClustered)
            AppendToStringCollection(objStringCollection, "IsFullTextInstalled", .IsFullTextInstalled)
            AppendToStringCollection(objStringCollection, "IsSingleUser", .IsSingleUser)
            AppendToStringCollection(objStringCollection, "Language", .Language)
            AppendToStringCollection(objStringCollection, "MasterDBLogPath", .MasterDBLogPath)
            AppendToStringCollection(objStringCollection, "MasterDBPath", .MasterDBPath)
            AppendToStringCollection(objStringCollection, "MaxPrecision", .MaxPrecision)
            AppendToStringCollection(objStringCollection, "NetName", .NetName)
            AppendToStringCollection(objStringCollection, "OSVersion", .OSVersion)
            AppendToStringCollection(objStringCollection, "PhysicalMemory", .PhysicalMemory)
            AppendToStringCollection(objStringCollection, "Platform", .Platform)
            AppendToStringCollection(objStringCollection, "Processors", .Processors)
            AppendToStringCollection(objStringCollection, "Product", .Product)
            AppendToStringCollection(objStringCollection, "ProductLevel", .ProductLevel)
            AppendToStringCollection(objStringCollection, "RootDirectory", .RootDirectory)
            AppendToStringCollection(objStringCollection, "VersionString", .VersionString)
        End With

        WriteTextToFile(strOutputFolderPathCurrentServer, "ServerInformation", objStringCollection, False, ".ini")


        ' Next save the Server Configuration to file ServerConfiguration
        objStringCollection.Clear()
        With objSqlServer.Configuration
            objStringCollection.Add("[Server Configuration for " & objSqlServer.Name & "]")
            AppendToStringCollection(objStringCollection, .AdHocDistributedQueriesEnabled)
            AppendToStringCollection(objStringCollection, .Affinity64Mask)
            AppendToStringCollection(objStringCollection, .AffinityIOMask)
            AppendToStringCollection(objStringCollection, .AffinityMask)
            AppendToStringCollection(objStringCollection, .AgentXPsEnabled)
            AppendToStringCollection(objStringCollection, .AllowUpdates)
            AppendToStringCollection(objStringCollection, .AweEnabled)
            AppendToStringCollection(objStringCollection, .C2AuditMode)
            AppendToStringCollection(objStringCollection, .CostThresholdForParallelism)
            AppendToStringCollection(objStringCollection, .CrossDBOwnershipChaining)
            AppendToStringCollection(objStringCollection, .CursorThreshold)
            AppendToStringCollection(objStringCollection, .DatabaseMailEnabled)
            AppendToStringCollection(objStringCollection, .DefaultFullTextLanguage)
            AppendToStringCollection(objStringCollection, .DefaultLanguage)
            AppendToStringCollection(objStringCollection, .FillFactor)
            AppendToStringCollection(objStringCollection, .IndexCreateMemory)
            AppendToStringCollection(objStringCollection, .IsSqlClrEnabled)
            AppendToStringCollection(objStringCollection, .LightweightPooling)
            AppendToStringCollection(objStringCollection, .Locks)
            AppendToStringCollection(objStringCollection, .MaxDegreeOfParallelism)
            AppendToStringCollection(objStringCollection, .MaxServerMemory)
            AppendToStringCollection(objStringCollection, .MaxWorkerThreads)
            AppendToStringCollection(objStringCollection, .MediaRetention)
            AppendToStringCollection(objStringCollection, .MinMemoryPerQuery)
            AppendToStringCollection(objStringCollection, .MinServerMemory)
            AppendToStringCollection(objStringCollection, .NestedTriggers)
            AppendToStringCollection(objStringCollection, .NetworkPacketSize)
            AppendToStringCollection(objStringCollection, .OleAutomationProceduresEnabled)
            AppendToStringCollection(objStringCollection, .OpenObjects)
            AppendToStringCollection(objStringCollection, .PrecomputeRank)
            AppendToStringCollection(objStringCollection, .PriorityBoost)
            AppendToStringCollection(objStringCollection, .ProtocolHandlerTimeout)
            AppendToStringCollection(objStringCollection, .QueryGovernorCostLimit)
            AppendToStringCollection(objStringCollection, .QueryWait)
            AppendToStringCollection(objStringCollection, .RecoveryInterval)
            AppendToStringCollection(objStringCollection, .RemoteAccess)
            AppendToStringCollection(objStringCollection, .RemoteDacConnectionsEnabled)
            AppendToStringCollection(objStringCollection, .RemoteLoginTimeout)
            AppendToStringCollection(objStringCollection, .RemoteProcTrans)
            AppendToStringCollection(objStringCollection, .RemoteQueryTimeout)
            AppendToStringCollection(objStringCollection, .ReplicationMaxTextSize)
            AppendToStringCollection(objStringCollection, .ReplicationXPsEnabled)
            AppendToStringCollection(objStringCollection, .ScanForStartupProcedures)
            AppendToStringCollection(objStringCollection, .SetWorkingSetSize)
            AppendToStringCollection(objStringCollection, .ShowAdvancedOptions)
            AppendToStringCollection(objStringCollection, .SmoAndDmoXPsEnabled)
            AppendToStringCollection(objStringCollection, .SqlMailXPsEnabled)
            AppendToStringCollection(objStringCollection, .TransformNoiseWords)
            AppendToStringCollection(objStringCollection, .TwoDigitYearCutoff)
            AppendToStringCollection(objStringCollection, .UserConnections)
            AppendToStringCollection(objStringCollection, .UserOptions)
            AppendToStringCollection(objStringCollection, .WebXPsEnabled)
            AppendToStringCollection(objStringCollection, .XPCmdShellEnabled)
        End With

        WriteTextToFile(strOutputFolderPathCurrentServer, "ServerConfiguration", objStringCollection, False, ".ini")


        ' Next save the Mail settings to file ServerMail
        ' Can only do this for Sql Server 2005 or newer
        If SqlServer2005OrNewer(objSqlServer) Then
            objStringCollection.Clear()
            objStringCollection = CleanSqlScript(objSqlServer.Mail.Script(objScriptOptions), udtSchemaExportOptions, False, False)
            WriteTextToFile(strOutputFolderPathCurrentServer, "ServerMail", objStringCollection, True)
        End If


        ' Next save the Registry Settings to file ServerRegistrySettings
        objStringCollection.Clear()
        objStringCollection = CleanSqlScript(objSqlServer.Settings.Script(objScriptOptions), udtSchemaExportOptions, False, False)
        objStringCollection.Insert(0, "-- Registry Settings for " & objSqlServer.Name)

        WriteTextToFile(strOutputFolderPathCurrentServer, "ServerRegistrySettings", objStringCollection, False)


    End Sub

    Private Sub ExportSQLServerLogins(ByRef objSqlServer As Microsoft.SqlServer.Management.Smo.Server, _
                                      ByRef udtSchemaExportOptions As udtSchemaExportOptionsType, _
                                      ByRef objScriptOptions As Microsoft.SqlServer.Management.Smo.ScriptingOptions, _
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
                            CleanSqlScript(objSqlServer.Logins.Item(intIndex).Script(objScriptOptions), udtSchemaExportOptions, True, True))

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


    Private Sub ExportSQLServerAgentJobs(ByRef objSqlServer As Microsoft.SqlServer.Management.Smo.Server, _
                                         ByRef udtSchemaExportOptions As udtSchemaExportOptionsType, _
                                         ByRef objScriptOptions As Microsoft.SqlServer.Management.Smo.ScriptingOptions, _
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
                            CleanSqlScript(objSqlServer.JobServer.Jobs(intIndex).Script(objScriptOptions), udtSchemaExportOptions, True, True))

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

    Private Function GetDefaultScriptOptions() As Microsoft.SqlServer.Management.Smo.ScriptingOptions

        Dim objScriptOptions As New Microsoft.SqlServer.Management.Smo.ScriptingOptions

        With objScriptOptions
            '.Bindings = True
            .Default = True
            .DriAll = True
            '.DriAllKeys = True
            '.ExtendedProperties = True
            .IncludeHeaders = True          ' If True, then includes a line of the form: /****** Object:  Table [dbo].[T_Analysis_Description]    Script Date: 08/14/2006 12:14:31 ******/
            .IncludeDatabaseContext = False
            .IncludeIfNotExists = False     ' If True, then the entire SP is placed inside an nvarchar variable
            .Indexes = True
            '.NoCollation = True
            .NoCommandTerminator = False
            .Permissions = True
            '.PrimaryObject = True
            .SchemaQualify = True           ' If True, then adds extra [dbo]. prefixes
            '.ScriptDrops = True            ' If True, the script only contains Drop commands, not Create commands
            .Statistics = True
            .Triggers = True
            .ToFileOnly = False
            .WithDependencies = False       ' Scripting speed will be much slower if this is set to true
        End With

        Return objScriptOptions
    End Function

    Public Function GetSqlServerDatabases(ByRef strDatabaseList() As String) As Boolean
        Dim intIndex As Integer
        Dim objDatabases As Microsoft.SqlServer.Management.Smo.DatabaseCollection
        Dim blnSuccess As Boolean

        Try
            blnSuccess = False
            InitializeLocalVariables(False)

            ReDim strDatabaseList(-1)

            If Not mSqlServerOptionsCurrent.Connected OrElse mSqlServer Is Nothing OrElse mSqlServer.State <> Microsoft.SqlServer.Management.Smo.SqlSmoState.Existing Then
                mStatusMessage = "Not connected to a server"
            Else
                ' Obtain a list of all databases actually residing on the server (according to the Master database)
                ResetProgress("Obtaining list of databases on " & mSqlServerOptionsCurrent.ConnectionInfo.ServerName)

                ' Obtain the databases collection
                objDatabases = mSqlServer.Databases

                If objDatabases.Count > 0 Then
                    ReDim strDatabaseList(objDatabases.Count - 1)
                    ResetProgressStepCount(strDatabaseList.Length)

                    intIndex = 0
                    For intIndex = 0 To objDatabases.Count - 1
                        strDatabaseList(intIndex) = objDatabases(intIndex).Name

                        mProgressStep = intIndex + 1
                        UpdateProgress(mProgressStep)
                        If mAbortProcessing Then
                            UpdateProgress("Aborted processing")
                            Exit For
                        End If
                    Next intIndex

                    Array.Sort(strDatabaseList)
                End If

                If Not mAbortProcessing Then
                    UpdateProgress("Done")
                    SetProgressComplete()
                    blnSuccess = True
                End If
            End If
        Catch ex As Exception
            SetLocalError(eDBSchemaExportErrorCodes.DatabaseConnectionError, "Error obtaining list of databases on current server", ex)
        End Try

        Return blnSuccess
    End Function

    Public Function GetSqlServerDatabaseTableNames(ByVal strDatabaseName As String, ByRef strTableList() As String, ByRef lngRowCounts() As Long, ByVal blnIncludeTableRowCounts As Boolean, ByVal blnIncludeSystemObjects As Boolean) As Boolean
        Dim intIndex As Integer
        Dim intTargetIndex As Integer

        Dim objDatabase As Microsoft.SqlServer.Management.Smo.Database
        Dim objTables As Microsoft.SqlServer.Management.Smo.TableCollection
        Dim blnSuccess As Boolean

        Try
            blnSuccess = False
            InitializeLocalVariables(False)

            If strDatabaseName Is Nothing Then
                strDatabaseName = String.Empty
            End If

            ReDim strTableList(-1)
            ReDim lngRowCounts(-1)

            If Not mSqlServerOptionsCurrent.Connected OrElse mSqlServer Is Nothing OrElse mSqlServer.State <> Microsoft.SqlServer.Management.Smo.SqlSmoState.Existing Then
                mStatusMessage = "Not connected to a server"
            ElseIf Not mSqlServer.Databases.Contains(strDatabaseName) Then
                mStatusMessage = "Database " & strDatabaseName & " not found on server " & mSqlServerOptionsCurrent.ConnectionInfo.ServerName
            Else
                ' Obtain a list of all databases actually residing on the server (according to the Master database)
                ResetProgress("Obtaining list of tables in database " & strDatabaseName & " on server " & mSqlServerOptionsCurrent.ConnectionInfo.ServerName)

                ' Connect to database strDatabaseName
                objDatabase = mSqlServer.Databases(strDatabaseName)

                ' Obtain a list of the tables in objDatabase
                objTables = objDatabase.Tables
                If objTables.Count > 0 Then
                    ReDim strTableList(objTables.Count - 1)
                    ReDim lngRowCounts(objTables.Count - 1)
                    ResetProgressStepCount(strTableList.Length)

                    intTargetIndex = 0
                    For intIndex = 0 To objTables.Count - 1
                        If blnIncludeSystemObjects OrElse Not objTables(intIndex).IsSystemObject Then
                            strTableList(intTargetIndex) = objTables(intIndex).Name
                            If blnIncludeTableRowCounts Then
                                lngRowCounts(intTargetIndex) = objTables(intIndex).RowCount
                            End If
                            intTargetIndex += 1

                            mProgressStep = intIndex + 1
                            UpdateProgress(mProgressStep)
                            If mAbortProcessing Then
                                UpdateProgress("Aborted processing")
                                Exit For
                            End If

                        End If
                    Next intIndex

                    If intTargetIndex < strTableList.Length Then
                        ReDim Preserve strTableList(intTargetIndex - 1)
                    End If

                    Array.Sort(strTableList, lngRowCounts)
                End If

                If Not mAbortProcessing Then
                    UpdateProgress("Done")
                    SetProgressComplete()
                    blnSuccess = True
                End If

            End If
        Catch ex As Exception
            SetLocalError(eDBSchemaExportErrorCodes.DatabaseConnectionError, "Error obtaining list of tables in database " & strDatabaseName & " on current server", ex)
        End Try

        Return blnSuccess
    End Function

    Private Function GetTimeStamp() As String
        ' Return a timestamp in the form: 08/12/2006 23:01:20

        Return System.DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss")

    End Function

    Public Shared Sub InitializeSchemaExportOptions(ByRef udtSchemaExportOptions As udtSchemaExportOptionsType)

        With udtSchemaExportOptions
            .OutputFolderPath = String.Empty
            .OutputFolderNamePrefix = DEFAULT_DB_OUTPUT_FOLDER_NAME_PREFIX

            .CreateFolderForEachDB = True           ' This will be forced to true if more than one DB is to be scripted
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

    End Sub

    Public Shared Sub InitializeAutoSelectTableNames(ByRef strTableNames() As String)
        ReDim strTableNames(10)
        strTableNames(0) = "T_Dataset_Process_State"
        strTableNames(1) = "T_Process_State"
        strTableNames(2) = "T_Event_Target"
        strTableNames(3) = "T_Process_Config"
        strTableNames(4) = "T_Process_Config_Parameters"
        strTableNames(5) = "T_Process_Step_Control"
        strTableNames(6) = "T_Process_Step_Control_States"
        strTableNames(7) = "T_Histogram_Mode_Name"
        strTableNames(8) = "T_Peak_Matching_Defaults"
        strTableNames(9) = "T_Quantitation_Defaults"
        strTableNames(10) = "T_Folder_Paths"
    End Sub

    Public Shared Sub InitializeAutoSelectTableRegEx(ByRef strRegExSpecs() As String)
        ReDim strRegExSpecs(2)
        strRegExSpecs(0) = ".*_?Type_?Name"
        strRegExSpecs(1) = ".*_?State_?Name"
        strRegExSpecs(2) = ".*_State"
    End Sub

    Private Sub InitializeLocalVariables(ByVal blnResetServerConnection As Boolean)
        mErrorCode = eDBSchemaExportErrorCodes.NoError
        mStatusMessage = String.Empty

        Dim objRegExOptions As System.Text.RegularExpressions.RegexOptions
        objRegExOptions = System.Text.RegularExpressions.RegexOptions.Compiled Or _
                          System.Text.RegularExpressions.RegexOptions.IgnoreCase Or _
                          System.Text.RegularExpressions.RegexOptions.Singleline

        mColumnCharNonStandardRegEx = New System.Text.RegularExpressions.Regex("[^a-z0-9_]", objRegExOptions)

        mNonStandardOSChars = New System.Text.RegularExpressions.Regex("[^a-z0-9_ =+-,.';`~!@#$%^&(){}\[\]]", objRegExOptions)

        If blnResetServerConnection Then
            ResetSqlServerConnection(mSqlServerOptionsCurrent)
        End If

        InitializeAutoSelectTableNames(mTableNamesToAutoSelect)
        InitializeAutoSelectTableRegEx(mTableNameAutoSelectRegEx)

        mAbortProcessing = False
        SetPauseStatus(ePauseStatusConstants.Unpaused)
    End Sub

    Private Function LoginToServerWork(ByRef objSQLServer As Microsoft.SqlServer.Management.Smo.Server, ByRef udtServerOptionsToUpdate As udtServerConnectionSingleType, ByVal udtServerConnectionInfo As udtServerConnectionInfoType) As Boolean
        ' Returns True if success, False otherwise

        Dim objServerConnection As Microsoft.SqlServer.Management.Common.ServerConnection
        Dim objConnectionInfo As Microsoft.SqlServer.Management.Common.SqlConnectionInfo

        Try
            objConnectionInfo = New Microsoft.SqlServer.Management.Common.SqlConnectionInfo(udtServerConnectionInfo.ServerName)
            With objConnectionInfo
                .UseIntegratedSecurity = udtServerConnectionInfo.UseIntegratedAuthentication
                If Not .UseIntegratedSecurity Then
                    .UserName = udtServerConnectionInfo.UserName
                    .Password = udtServerConnectionInfo.Password
                End If
                .ConnectionTimeout = 10
            End With

            objServerConnection = New Microsoft.SqlServer.Management.Common.ServerConnection(objConnectionInfo)

            If Not objSQLServer Is Nothing Then
                objSQLServer = Nothing
            End If
            objSQLServer = New Microsoft.SqlServer.Management.Smo.Server(objServerConnection)

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

    Private Function ScriptCollectionOfObjects(ByRef objSchemaCollection As Microsoft.SqlServer.Management.Smo.SchemaCollectionBase, _
                                               ByRef udtSchemaExportOptions As udtSchemaExportOptionsType, _
                                               ByRef objScriptOptions As Microsoft.SqlServer.Management.Smo.ScriptingOptions, _
                                               ByVal intProcessCountExpected As Integer, _
                                               ByVal strOutputFolderPathCurrentDB As String) As Integer

        ' Scripts the objects in objSchemaCollection
        ' Returns the number of objects scripted

        Dim objItem As Microsoft.SqlServer.Management.Smo.Schema
        Dim intProcessCount As Integer

        intProcessCount = 0
        For Each objItem In objSchemaCollection
            mSubtaskProgressStepDescription = objItem.Name
            UpdateSubtaskProgress(intProcessCount, intProcessCountExpected)

            WriteTextToFile(strOutputFolderPathCurrentDB, objItem.Name, _
                            CleanSqlScript(objItem.Script(objScriptOptions), udtSchemaExportOptions))

            intProcessCount += 1

            CheckPauseStatus()
            If mAbortProcessing Then
                UpdateProgress("Aborted processing")
                Exit Function
            End If
        Next

        Return intProcessCount

    End Function

    Private Function ScriptDBObjects(ByRef objSqlServer As Microsoft.SqlServer.Management.Smo.Server, ByVal udtSchemaExportOptions As udtSchemaExportOptionsType, ByVal strDatabaseListToProcess() As String, ByVal strTableNamesForDataExport() As String) As Boolean

        Dim blnSuccess As Boolean
        Dim blnDBNotFoundReturn As Boolean

        Dim intIndex As Integer
        Dim intDatabasesProcessed As Integer

        Dim strCurrentDB As String
        Dim htProcessedDBList As Hashtable

        Try
            htProcessedDBList = New Hashtable

            ' Process each database in strDatabaseListToProcess
            intDatabasesProcessed = 0
            ResetProgress("Exporting DB objects to: " & SharedVBNetRoutines.VBNetRoutines.CompactPathString(udtSchemaExportOptions.OutputFolderPath), strDatabaseListToProcess.Length)

            For intIndex = 0 To strDatabaseListToProcess.Length - 1
                strCurrentDB = strDatabaseListToProcess(intIndex)
                blnDBNotFoundReturn = True

                If Not strCurrentDB Is Nothing AndAlso strCurrentDB.Length > 0 Then
                    If htProcessedDBList.ContainsKey(strCurrentDB) Then
                        ' DB has already been processed
                        blnDBNotFoundReturn = False
                    Else
                        htProcessedDBList.Add(strCurrentDB, 1)

                        If objSqlServer.Databases.Contains(strCurrentDB) Then

                            UpdateProgress("Exporting objects from database " & strCurrentDB)

                            blnSuccess = ExportDBObjectsUsingSMO(objSqlServer, strCurrentDB, strTableNamesForDataExport, udtSchemaExportOptions, blnDBNotFoundReturn)

                            If Not blnDBNotFoundReturn Then
                                If Not blnSuccess Then Exit For
                                intDatabasesProcessed += 1
                                If Not mAbortProcessing Then
                                    SetSubtaskProgressComplete()
                                End If
                            End If
                        Else
                            ' Database not actually present on the server; skip it
                        End If
                    End If
                End If

                mProgressStep = intIndex + 1
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

            Next intIndex

            ' Set blnSuccess to true here
            ' If an error occurred, then mErrorCode will indicate that
            blnSuccess = True

        Catch ex As Exception
            SetLocalError(eDBSchemaExportErrorCodes.GeneralError, "Error exporting DB schema objects: " & udtSchemaExportOptions.OutputFolderPath, ex)
        End Try

        Return blnSuccess
    End Function

    Private Function ScriptServerObjects(ByRef objSqlServer As Microsoft.SqlServer.Management.Smo.Server, ByVal udtSchemaExportOptions As udtSchemaExportOptionsType) As Boolean
        Const PROGRESS_STEP_COUNT As Integer = 2

        ' Export the Server Settings and Sql Server Agent jobs

        Dim objScriptOptions As Microsoft.SqlServer.Management.Smo.ScriptingOptions

        Dim strOutputFolderPathCurrentServer As String = String.Empty
        Dim blnSuccess As Boolean

        objScriptOptions = GetDefaultScriptOptions()

        Try
            ' Construct the path to the output folder
            strOutputFolderPathCurrentServer = System.IO.Path.Combine(udtSchemaExportOptions.OutputFolderPath, udtSchemaExportOptions.ServerOutputFolderNamePrefix & objSqlServer.Name)

            ' Create the folder if it doesn't exist
            If Not System.IO.Directory.Exists(strOutputFolderPathCurrentServer) Then
                System.IO.Directory.CreateDirectory(strOutputFolderPathCurrentServer)
            End If

        Catch ex As Exception
            SetLocalError(eDBSchemaExportErrorCodes.DatabaseConnectionError, "Error validating or creating folder " & strOutputFolderPathCurrentServer)
            Return False
        End Try

        Try
            ResetProgress("Exporting Server objects to: " & SharedVBNetRoutines.VBNetRoutines.CompactPathString(udtSchemaExportOptions.OutputFolderPath), PROGRESS_STEP_COUNT)
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


    Public Function ScriptServerAndDBObjects(ByVal udtSchemaExportOptions As udtSchemaExportOptionsType, ByVal strDatabaseListToProcess() As String, ByVal strTableNamesForDataExport() As String) As Boolean

        Dim blnSuccess As Boolean = False

        InitializeLocalVariables(False)

        Try
            blnSuccess = False
            If udtSchemaExportOptions.ConnectionInfo.ServerName Is Nothing OrElse udtSchemaExportOptions.ConnectionInfo.ServerName.Length = 0 Then
                SetLocalError(eDBSchemaExportErrorCodes.ConfigurationError, "Server name is not defined")
            ElseIf strDatabaseListToProcess Is Nothing OrElse strDatabaseListToProcess.Length = 0 Then
                If udtSchemaExportOptions.ExportServerSettingsLoginsAndJobs Then
                    ' No databases are defined, but we are exporting server settings; this is OK
                    blnSuccess = True
                Else
                    SetLocalError(eDBSchemaExportErrorCodes.ConfigurationError, "Database list to process is empty")
                End If
            Else
                If strDatabaseListToProcess.Length > 1 Then
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
            If Not System.IO.Directory.Exists(udtSchemaExportOptions.OutputFolderPath) Then
                SetLocalError(eDBSchemaExportErrorCodes.OutputFolderAccessError, "Output folder not found: " & udtSchemaExportOptions.OutputFolderPath)
                blnSuccess = False
            End If
        End If

        If blnSuccess Then
            ResetProgress("Exporting schema to: " & SharedVBNetRoutines.VBNetRoutines.CompactPathString(udtSchemaExportOptions.OutputFolderPath), 1)
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

            If blnSuccess AndAlso Not strDatabaseListToProcess Is Nothing AndAlso strDatabaseListToProcess.Length > 0 Then
                blnSuccess = ScriptDBObjects(mSqlServer, udtSchemaExportOptions, strDatabaseListToProcess, strTableNamesForDataExport)
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

    Private Function SqlServer2005OrNewer(ByVal objDatabase As Microsoft.SqlServer.Management.Smo.Database) As Boolean
        Return SqlServer2005OrNewer(objDatabase.Parent)
    End Function

    Private Function SqlServer2005OrNewer(ByVal objServer As Microsoft.SqlServer.Management.Smo.Server) As Boolean
        If objServer.Information.Version.Major >= 9 Then
            Return True
        Else
            Return False
        End If
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


    Private Function WriteTextToFile(ByVal strOutputFolderPath As String, ByVal strObjectName As String, ByRef objStringCollection As System.Collections.Specialized.StringCollection) As Boolean
        ' Calls WriteTextToFile with blnAutoAddGoStatements = True
        Return WriteTextToFile(strOutputFolderPath, strObjectName, objStringCollection, True)
    End Function

    Private Function WriteTextToFile(ByVal strOutputFolderPath As String, ByVal strObjectName As String, ByRef objStringCollection As System.Collections.Specialized.StringCollection, ByVal blnAutoAddGoStatements As Boolean) As Boolean
        Return WriteTextToFile(strOutputFolderPath, strObjectName, objStringCollection, blnAutoAddGoStatements, ".sql")
    End Function

    Private Function WriteTextToFile(ByVal strOutputFolderPath As String, ByVal strObjectName As String, ByRef objStringCollection As System.Collections.Specialized.StringCollection, ByVal blnAutoAddGoStatements As Boolean, ByVal strFileExtension As String) As Boolean

        Dim intIndex As Integer

        Dim strOutFilePath As String = "??"
        Dim swOutFile As System.IO.StreamWriter

        Try
            ' Make sure strObjectName doesn't contain any invalid characters
            strObjectName = CleanNameForOS(strObjectName)

            strOutFilePath = System.IO.Path.Combine(strOutputFolderPath, strObjectName & strFileExtension)
            swOutFile = New System.IO.StreamWriter(strOutFilePath, False)

            For intIndex = 0 To objStringCollection.Count - 1
                swOutFile.WriteLine(objStringCollection.Item(intIndex))

                If blnAutoAddGoStatements Then
                    swOutFile.WriteLine("GO")
                End If
            Next intIndex
            swOutFile.Close()

        Catch ex As Exception
            SetLocalError(eDBSchemaExportErrorCodes.OutputFolderAccessError, "Error saving file " & strOutFilePath)
            Return False
        End Try

        Return True

    End Function

End Class