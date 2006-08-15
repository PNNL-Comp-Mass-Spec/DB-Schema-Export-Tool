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
' Last updated August 15, 2006

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

    Public Const DEFAULT_OUTPUT_FOLDER_NAME_PREFIX As String = "DBSchema__"

    ' Note: this value defines the maximum number of data rows that will be exported 
    ' from tables that are auto-added to the table list for data export
    Public Const DATA_ROW_COUNT_WARNING_THRESHOLD As Integer = 1000

    Private Const COMMENT_START_TEXT As String = "/****** "
    Private Const COMMENT_END_TEXT As String = " ******/"
    Private Const COMMENT_END_TEXT_SHORT As String = "*/"

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
        Tables = 0
        Views = 1
        StoredProcedures = 2
        UserDefinedFunctions = 3
        UserDefinedDataTypes = 4
        UserDefinedTypes = 5
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

        Public SaveDataAsInsertIntoStatements As Boolean
        Public DatabaseTypeForInsertInto As eTargetDatabaseTypeConstants
        Public AutoSelectTableNamesForDataExport As Boolean

        Public ExportTables As Boolean
        Public ExportViews As Boolean
        Public ExportStoredProcedures As Boolean
        Public ExportUserDefinedFunctions As Boolean
        Public ExportUserDefinedDataTypes As Boolean
        Public ExportUserDefinedTypes As Boolean                               ' Only supported in Sql Server 2005 or newer (Server Version >= 9)

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
#End Region

#Region "Classwide Variables"
    Public Event NewMessage(ByVal strMessage As String, ByVal eMessageType As eMessageTypeConstants)
    Public Event DBExportStarting(ByVal strDatabaseName As String)
    Public Event PauseStatusChange()

    Private mSqlServer As Microsoft.SqlServer.Management.Smo.Server
    Private mSqlServerOptionsCurrent As udtServerConnectionSingleType

    Private mColumnCharNonStandardRegEx As System.Text.RegularExpressions.Regex

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

    Private Function CleanSqlScript(ByRef objStringCollection As System.Collections.Specialized.StringCollection, ByRef udtSchemaExportOptions As udtSchemaExportOptionsType) As System.Collections.Specialized.StringCollection

        Dim intIndex As Integer
        Dim intIndexStart As Integer
        Dim intIndexEnd As Integer
        Dim intCrLfIndex As Integer

        Dim strText As String

        Dim chWhiteSpaceChars() As Char = New Char() {" "c, ControlChars.Tab}

        Try
            If Not udtSchemaExportOptions.IncludeTimestampInScriptFileHeader Then
                ' Look for and remove the timestamp from the first line of the Sql script
                If Not objStringCollection Is Nothing AndAlso objStringCollection.Count > 0 Then
                    ' Look for and remove the text  "Script Date: 08/14/2006 20:14:31" prior to the first "******/"
                    ' Do not look past the first carriage return of each entry in objStringCollection

                    For intIndex = 0 To objStringCollection.Count - 1
                        strText = objStringCollection(intIndex)

                        ' Find the first CrLf after the first non-blank line in strText
                        intIndexStart = 0
                        Do
                            intCrLfIndex = strText.IndexOf(ControlChars.NewLine, intIndexStart)
                            If intCrLfIndex = intIndexStart Then
                                intIndexStart += 2
                            Else
                                Exit Do
                            End If
                        Loop While intCrLfIndex >= 0 And intCrLfIndex < intIndexStart

                        If intCrLfIndex < 0 Then intCrLfIndex = strText.Length - 1

                        intIndexStart = strText.IndexOf("Script Date: ", 0, intCrLfIndex + 1)
                        intIndexEnd = strText.IndexOf(COMMENT_END_TEXT_SHORT, 0, intCrLfIndex + 1)

                        If intIndexStart > 0 And intIndexEnd > 0 Then
                            strText = strText.Substring(0, intIndexStart).TrimEnd(chWhiteSpaceChars) & COMMENT_END_TEXT & _
                                      strText.Substring(intIndexEnd + COMMENT_END_TEXT_SHORT.Length)

                            objStringCollection(intIndex) = String.Copy(strText)
                        End If
                    Next
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

        Dim strOutputFolderPathCurrentDB As String = String.Empty
        Dim intProcessCount As Integer
        Dim intProcessCountExpected As Integer

        Dim intMaximumDataRowsToExport() As Integer
        ReDim intMaximumDataRowsToExport(-1)

        Dim blnSuccess As Boolean

        Try
            blnDBNotFoundReturn = False

            objScriptOptions = New Microsoft.SqlServer.Management.Smo.ScriptingOptions
            With objScriptOptions
                .Default = True
                .DriAll = True
                .IncludeHeaders = True          ' If True, then includes a line of the form: /****** Object:  Table [dbo].[T_Analysis_Description]    Script Date: 08/14/2006 12:14:31 ******/
                .IncludeDatabaseContext = False
                .IncludeIfNotExists = False     ' If True, then the entire SP is placed inside an nvarchar variable
                .Indexes = True
                .NoCommandTerminator = False
                .Permissions = True
                .SchemaQualify = True           ' If True, then adds extra [dbo]. prefixes
                '.ScriptDrops = True            ' If True, the script only contains Drop commands, not Create commands
                .Statistics = True
                .Triggers = True
                .ToFileOnly = False
                .WithDependencies = False       ' Scripting speed will be much slower if this is set to true
            End With

            objDatabase = objSqlServer.Databases(strDatabaseName)
            blnDBNotFoundReturn = False
        Catch ex As Exception
            SetLocalError(eDBSchemaExportErrorCodes.DatabaseConnectionError, "Error connecting to database " & strDatabaseName)
            blnDBNotFoundReturn = True
            Return False
        End Try

        Try
            ' Validate the strings in udtSchemaExportOptions
            With udtSchemaExportOptions
                If .OutputFolderPath Is Nothing Then
                    .OutputFolderPath = String.Empty
                End If

                If .OutputFolderNamePrefix Is Nothing Then
                    .OutputFolderNamePrefix = DEFAULT_OUTPUT_FOLDER_NAME_PREFIX
                End If
            End With

            ' Construct the path to the output folder
            If udtSchemaExportOptions.CreateFolderForEachDB Then
                strOutputFolderPathCurrentDB = System.IO.Path.Combine(udtSchemaExportOptions.OutputFolderPath, udtSchemaExportOptions.OutputFolderNamePrefix & objDatabase.Name)
            Else
                strOutputFolderPathCurrentDB = String.Copy(udtSchemaExportOptions.OutputFolderPath)
            End If

            ' Create the folder if it doesn't exist
            If Not System.IO.Directory.Exists(strOutputFolderPathCurrentDB) Then
                System.IO.Directory.CreateDirectory(strOutputFolderPathCurrentDB)
            End If

            If udtSchemaExportOptions.AutoSelectTableNamesForDataExport Then
                blnSuccess = AutoSelectTableNamesForDataExport(objDatabase, strTableNamesForDataExport, intMaximumDataRowsToExport, udtSchemaExportOptions)
                If Not blnSuccess Then
                    Return False
                End If
            End If

        Catch ex As Exception
            SetLocalError(eDBSchemaExportErrorCodes.DatabaseConnectionError, "Error validating or creating folder " & strOutputFolderPathCurrentDB)
            Return False
        End Try

        Try
            ResetSubtaskProgress("Counting number of objects to export")

            ' Preview the number of objects to export
            intProcessCount = ExportDBObjectsWork(objDatabase, objScriptOptions, udtSchemaExportOptions, strOutputFolderPathCurrentDB, True, 0)
            intProcessCountExpected = intProcessCount

            If Not strTableNamesForDataExport Is Nothing Then
                intProcessCountExpected += strTableNamesForDataExport.Length
            End If

            If intProcessCount > 0 Then
                intProcessCount = ExportDBObjectsWork(objDatabase, objScriptOptions, udtSchemaExportOptions, strOutputFolderPathCurrentDB, False, intProcessCountExpected)
            End If

            ' Export data from tables specified by strTableNamesForDataExport; maximum row counts are specified by intMaximumDataRowsToExport
            blnSuccess = ExportDBTableData(objDatabase, strTableNamesForDataExport, intMaximumDataRowsToExport, udtSchemaExportOptions, strOutputFolderPathCurrentDB, intProcessCount, intProcessCountExpected)

        Catch ex As Exception
            SetLocalError(eDBSchemaExportErrorCodes.DatabaseConnectionError, "Error scripting objects in database " & strDatabaseName)
            blnSuccess = False
        End Try

        Return blnSuccess
    End Function

    Private Function ExportDBObjectsWork(ByRef objDatabase As Microsoft.SqlServer.Management.Smo.Database, ByRef objScriptOptions As Microsoft.SqlServer.Management.Smo.ScriptingOptions, ByRef udtSchemaExportOptions As udtSchemaExportOptionsType, ByVal strOutputFolderPathCurrentDB As String, ByVal blnCountObjectsOnly As Boolean, ByVal intProcessCountExpected As Integer) As Integer
        Dim objTable As Microsoft.SqlServer.Management.Smo.Table

        Dim objSMOObject() As Microsoft.SqlServer.Management.Smo.SqlSmoObject
        ReDim objSMOObject(0)

        Dim objURNList() As Microsoft.SqlServer.Management.Smo.Urn
        ReDim objURNList(0)

        Dim dsObjects As DataSet
        Dim objRow As DataRow
        Dim strObjectSchema As String
        Dim strObjectName As String

        Dim objScripter As Microsoft.SqlServer.Management.Smo.Scripter
        objScripter = New Microsoft.SqlServer.Management.Smo.Scripter(mSqlServer)
        objScripter.Options = objScriptOptions

        Dim intProcessCount As Integer
        Dim intItemCount As Integer
        Dim intObjectIterator As Integer

        Dim strSql As String

        intProcessCount = 0

        ' Do not include a Try block in this Function; let the calling function handle errors

        If udtSchemaExportOptions.ExportTables Then
            If blnCountObjectsOnly Then
                ' Note: objDatabase.Tables includes system tables, so intProcessCount will be 
                '       an overestimate if udtSchemaExportOptions.IncludeSystemObjects = False
                intProcessCount += objDatabase.Tables.Count
            Else
                For Each objTable In objDatabase.Tables
                    If udtSchemaExportOptions.IncludeSystemObjects OrElse Not objTable.IsSystemObject Then
                        mSubtaskProgressStepDescription = objTable.Name
                        UpdateSubtaskProgress(intProcessCount, intProcessCountExpected)

                        objSMOObject(0) = objTable
                        WriteTextToFile(strOutputFolderPathCurrentDB, objTable.Name, CleanSqlScript(objScripter.Script(objSMOObject), udtSchemaExportOptions), True)
                    End If

                    intProcessCount += 1
                    CheckPauseStatus()
                    If mAbortProcessing Then
                        UpdateProgress("Aborted processing", mProgressPercentComplete)
                        Exit Function
                    End If
                Next objTable
            End If
        End If

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

        ''        If blnCountObjectsOnly Then
        ''            intProcessCount += dsObjects.Tables(0).Rows.Count
        ''        Else
        ''            For Each objRow In dsObjects.Tables(0).Rows
        ''                strObjectName = objRow.Item(0).ToString
        ''                mSubtaskProgressStepDescription = strObjectName
        ''                UpdateSubtaskProgress(intProcessCount, intProcessCountExpected)

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
        ''                    WriteTextToFile(strOutputFolderPathCurrentDB, strObjectName, CleanSqlScript(objScripter.Script(objSMOObject), udtSchemaExportOptions), True)
        ''                End If

        ''                intProcessCount += 1
        ''                CheckPauseStatus()
        ''                If mAbortProcessing Then
        ''                    UpdateProgress("Aborted processing", mProgressPercentComplete)
        ''                    Exit Function
        ''                End If
        ''            Next objRow
        ''        End If
        ''    End If
        ''Next intObjectIterator


        ' Option 4) Query the INFORMATION_SCHEMA views

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

                If blnCountObjectsOnly Then
                    intProcessCount += dsObjects.Tables(0).Rows.Count
                Else
                    For Each objRow In dsObjects.Tables(0).Rows
                        ' The first column is the schema
                        ' The second column is the name
                        strObjectSchema = objRow.Item(0).ToString
                        strObjectName = objRow.Item(1).ToString
                        mSubtaskProgressStepDescription = strObjectName
                        UpdateSubtaskProgress(intProcessCount, intProcessCountExpected)

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
                            WriteTextToFile(strOutputFolderPathCurrentDB, strObjectName, CleanSqlScript(objScripter.Script(objSMOObject), udtSchemaExportOptions), True)
                        End If

                        intProcessCount += 1
                        CheckPauseStatus()
                        If mAbortProcessing Then
                            UpdateProgress("Aborted processing", mProgressPercentComplete)
                            Exit Function
                        End If
                    Next objRow
                End If
            End If
        Next intObjectIterator

        If udtSchemaExportOptions.ExportUserDefinedDataTypes Then
            If blnCountObjectsOnly Then
                intProcessCount += objDatabase.UserDefinedDataTypes.Count
            Else
                intItemCount = ScriptObjects(objDatabase.UserDefinedDataTypes, objScriptOptions, udtSchemaExportOptions, strOutputFolderPathCurrentDB, intProcessCountExpected)
                intProcessCount += intItemCount
            End If
        End If

        If udtSchemaExportOptions.ExportUserDefinedTypes AndAlso mSqlServer.Information.Version.Major >= 9 Then
            If blnCountObjectsOnly Then
                intProcessCount += objDatabase.UserDefinedTypes.Count
            Else
                intItemCount = ScriptObjects(objDatabase.UserDefinedTypes, objScriptOptions, udtSchemaExportOptions, strOutputFolderPathCurrentDB, intProcessCountExpected)
                intProcessCount += intItemCount
            End If
        End If

        Return intProcessCount

    End Function

    Private Function ExportDBTableData(ByRef objDatabase As Microsoft.SqlServer.Management.Smo.Database, ByRef strTableNamesForDataExport() As String, ByRef intMaximumDataRowsToExport() As Integer, ByRef udtSchemaExportOptions As udtSchemaExportOptionsType, ByVal strOutputFolderPathCurrentDB As String, ByVal intProcessCount As Integer, ByVal intProcessCountExpected As Integer) As Boolean

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
        Dim blnDataFound As Boolean
        Dim blnSuccess As Boolean

        Try
            If Not strTableNamesForDataExport Is Nothing AndAlso strTableNamesForDataExport.Length > 0 Then
                objCurrentRow = New System.Text.StringBuilder

                For intTableIndex = 0 To strTableNamesForDataExport.Length - 1

                    mSubtaskProgressStepDescription = "Exporting data from " & strTableNamesForDataExport(intTableIndex)
                    UpdateSubtaskProgress(intProcessCount, intProcessCountExpected)

                    If objDatabase.Tables.Contains(strTableNamesForDataExport(intTableIndex)) Then
                        objTable = objDatabase.Tables(strTableNamesForDataExport(intTableIndex))

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
                        strSql &= " * FROM [" & objDatabase.Tables(strTableNamesForDataExport(intTableIndex)).Name & "]"

                        ' Read method #1: Populate a DataSet
                        dsCurrentTable = objDatabase.ExecuteWithResults(strSql)

                        objTableRows = New System.Collections.Specialized.StringCollection()

                        strHeader = COMMENT_START_TEXT & "Object:  Table [" & objTable.Name & "]"
                        If udtSchemaExportOptions.IncludeTimestampInScriptFileHeader Then
                            strHeader &= "    Script Date: " & GetTimeStamp()
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

                        WriteTextToFile(strOutputFolderPathCurrentDB, objTable.Name & "_Data", objTableRows, False)
                    End If

                    intProcessCount += 1
                    CheckPauseStatus()
                    If mAbortProcessing Then
                        UpdateProgress("Aborted processing", mProgressPercentComplete)
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

    Public Function GetSqlServerDatabases(ByRef strDatabaseList() As String) As Boolean
        Dim intindex As Integer
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

                    intindex = 0
                    For intindex = 0 To objDatabases.Count - 1
                        strDatabaseList(intindex) = objDatabases(intindex).Name

                        mProgressStep = intindex + 1
                        UpdateProgress(mProgressStep, strDatabaseList.Length)
                        If mAbortProcessing Then
                            UpdateProgress("Aborted processing", mProgressPercentComplete)
                            Exit For
                        End If
                    Next intindex

                    Array.Sort(strDatabaseList)
                End If

                If Not mAbortProcessing Then
                    UpdateProgress("Done", mProgressPercentComplete)
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

                    intTargetIndex = 0
                    For intIndex = 0 To objTables.Count - 1
                        If blnIncludeSystemObjects OrElse Not objTables(intIndex).IsSystemObject Then
                            strTableList(intTargetIndex) = objTables(intIndex).Name
                            If blnIncludeTableRowCounts Then
                                lngRowCounts(intTargetIndex) = objTables(intIndex).RowCount
                            End If
                            intTargetIndex += 1

                            mProgressStep = intIndex + 1
                            UpdateProgress(mProgressStep, strTableList.Length)
                            If mAbortProcessing Then
                                UpdateProgress("Aborted processing", mProgressPercentComplete)
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
                    UpdateProgress("Done", mProgressPercentComplete)
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
            .OutputFolderNamePrefix = DEFAULT_OUTPUT_FOLDER_NAME_PREFIX

            .CreateFolderForEachDB = True           ' This will be forced to true if more than one DB is to be scripted
            .IncludeSystemObjects = False

            .IncludeTimestampInScriptFileHeader = False

            .SaveDataAsInsertIntoStatements = True
            .DatabaseTypeForInsertInto = eTargetDatabaseTypeConstants.SqlServer
            .AutoSelectTableNamesForDataExport = True

            .ExportTables = True
            .ExportViews = True
            .ExportStoredProcedures = True
            .ExportUserDefinedFunctions = True
            .ExportUserDefinedDataTypes = True
            .ExportUserDefinedTypes = True

            ResetSqlServerConnection(.ConnectionInfo)
        End With

    End Sub

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

    Public Function ScriptDBObjectsStart(ByVal udtSchemaExportOptions As udtSchemaExportOptionsType, ByVal strDatabaseListToProcess() As String, ByVal strTableNamesForDataExport() As String) As Boolean

        Dim blnSuccess As Boolean
        Dim blnDBNotFoundReturn As Boolean

        Dim intIndex As Integer
        Dim intDatabasesProcessed As Integer

        Dim strCurrentDB As String
        Dim htProcessedDBList As Hashtable

        InitializeLocalVariables(False)

        Try
            blnSuccess = False
            If udtSchemaExportOptions.ConnectionInfo.ServerName Is Nothing OrElse udtSchemaExportOptions.ConnectionInfo.ServerName.Length = 0 Then
                SetLocalError(eDBSchemaExportErrorCodes.ConfigurationError, "Server name is not defined")
            ElseIf strDatabaseListToProcess Is Nothing OrElse strDatabaseListToProcess.Length = 0 Then
                SetLocalError(eDBSchemaExportErrorCodes.ConfigurationError, "Database list to process is empty")
            Else
                If strDatabaseListToProcess.Length > 1 Then
                    ' Force CreateFolderForEachDB to true
                    udtSchemaExportOptions.CreateFolderForEachDB = True
                End If

                blnSuccess = True
            End If

        Catch ex As Exception
            SetLocalError(eDBSchemaExportErrorCodes.DatabaseConnectionError, "Error validated the Schema Export Options", ex)
        End Try

        If Not blnSuccess Then
            Return False
        End If

        Try
            ' Confirm that the output folder exists
            If Not System.IO.Directory.Exists(udtSchemaExportOptions.OutputFolderPath) Then
                SetLocalError(eDBSchemaExportErrorCodes.OutputFolderAccessError, "Output folder not found: " & udtSchemaExportOptions.OutputFolderPath)
            End If

            ResetProgress("Exporting DB objects to: " & SharedVBNetRoutines.VBNetRoutines.CompactPathString(udtSchemaExportOptions.OutputFolderPath), strDatabaseListToProcess.Length)
            ResetSubtaskProgress("Initializing")

            htProcessedDBList = New Hashtable

            blnSuccess = ConnectToServer(udtSchemaExportOptions.ConnectionInfo)

            If blnSuccess Then
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

                            If mSqlServer.Databases.Contains(strCurrentDB) Then

                                UpdateProgress("Exporting objects from database " & strCurrentDB, mProgressPercentComplete)

                                blnSuccess = ExportDBObjectsUsingSMO(mSqlServer, strCurrentDB, strTableNamesForDataExport, udtSchemaExportOptions, blnDBNotFoundReturn)

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
                    UpdateProgress(mProgressStep, strDatabaseListToProcess.Length)
                    CheckPauseStatus()
                    If mAbortProcessing Then
                        UpdateProgress("Aborted processing", mProgressPercentComplete)
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
            End If

            If Not mAbortProcessing Then
                SetProgressComplete()
            End If

        Catch ex As Exception
            SetLocalError(eDBSchemaExportErrorCodes.GeneralError, "Error exporting DB schema objects: " & udtSchemaExportOptions.OutputFolderPath, ex)
        Finally
            RaiseEvent ProgressComplete()
        End Try

        Return blnSuccess
    End Function

    Private Function ScriptObjects(ByRef objSchemaCollection As Microsoft.SqlServer.Management.Smo.SchemaCollectionBase, ByRef objScriptOptions As Microsoft.SqlServer.Management.Smo.ScriptingOptions, ByRef udtSchemaExportOptions As udtSchemaExportOptionsType, ByVal strOutputFolderPathCurrentDB As String, ByVal intProcessCountExpected As Integer) As Integer
        ' Returns the number of objects scripted

        Dim objItem As Microsoft.SqlServer.Management.Smo.Schema
        Dim intProcessCount As Integer

        intProcessCount = 0
        For Each objItem In objSchemaCollection
            mSubtaskProgressStepDescription = objItem.Name
            UpdateSubtaskProgress(intProcessCount, intProcessCountExpected)

            WriteTextToFile(strOutputFolderPathCurrentDB, objItem.Name, CleanSqlScript(objItem.Script(objScriptOptions), udtSchemaExportOptions), True)

            intProcessCount += 1

            CheckPauseStatus()
            If mAbortProcessing Then
                UpdateProgress("Aborted processing", mProgressPercentComplete)
                Exit Function
            End If
        Next

        Return intProcessCount
    End Function

    Private Function WriteTextToFile(ByVal strOutputFolderPath As String, ByVal strObjectName As String, ByRef objStringCollection As System.Collections.Specialized.StringCollection, ByVal blnAutoAddGoStatements As Boolean) As Boolean

        Dim intIndex As Integer

        Dim strOutFilePath As String = "??"
        Dim swOutFile As System.IO.StreamWriter

        Try
            strOutFilePath = System.IO.Path.Combine(strOutputFolderPath, strObjectName & ".sql")
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

End Class