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

    ''' <summary>
    ''' Constructor
    ''' </summary>
    Public Sub New()
        MyBase.mFileDate = "July 2, 2014"
        InitializeLocalVariables()
    End Sub

#Region "Constants and Enums"

    ' Error codes specialized for this class
    Protected Enum eDBSchemaExportTool As Integer
        NoError = 0
        UnspecifiedError = -1
    End Enum

    Protected Enum eDifferenceReasonType
        Unchanged = 0
        NewFile = 1
        Changed = 2
    End Enum

    Protected Enum eRepoManagerType
        Svn = 0
        Hg = 1
        Git = 2
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

    Public Event SubtaskProgressChanged(ByVal taskDescription As String, ByVal percentComplete As Single)      ' PercentComplete ranges from 0 to 100, but can contain decimal percentage values

    Protected mLocalErrorCode As eDBSchemaExportTool

    Protected WithEvents mDBSchemaExporter As clsExportDBSchema

    'Runs specified program
    Private WithEvents m_ProgRunner As PRISM.Processes.clsProgRunner

#End Region

#Region "Properties"

    Public Property AutoSelectTableDataToExport As Boolean

    Public Property TableDataToExportFile As String

    Public Property CreateFolderForEachDB() As Boolean

    Public Property DatabaseSubfolderPrefix() As String

    Public Property Sync As Boolean

    Public Property SyncFolderPath As String

    Public Property GitUpdate As Boolean

    Public Property HgUpdate() As Boolean

    Public Property SvnUpdate As Boolean

    Public Property CommitUpdates() As Boolean


#End Region

    Private Function CheckPlural(ByVal value As Integer, ByVal textIfOne As String, ByVal textIfSeveral As String) As String
        If value = 1 Then
            Return textIfOne
        Else
            Return textIfSeveral
        End If
    End Function

    ''' <summary>
    ''' Export database schema to the specified folder
    ''' </summary>
    ''' <param name="strOutputFolderPath">Output folder path</param>
    ''' <param name="serverName">Server name</param>
    ''' <param name="dctDatabaseNamesAndOutputPaths">Dictionary where keys are database names and values will be updated to have the output folder path used</param>
    ''' <returns>True if success, false if a problem</returns>
    ''' <remarks>
    ''' If CreateFolderForEachDB is true, or if dctDatabaseNamesAndOutputPaths contains more than one entry,
    ''' then each database will be scripted to a subfolder below the output folder
    ''' </remarks>
    Public Function ExportSchema(
      ByVal strOutputFolderPath As String,
      ByVal serverName As String,
      ByRef dctDatabaseNamesAndOutputPaths As Dictionary(Of String, String)) As Boolean

        Return ExportSchema(strOutputFolderPath, serverName, dctDatabaseNamesAndOutputPaths, True, "", "")

    End Function

    ''' <summary>
    ''' Export database schema to the specified folder
    ''' </summary>
    ''' <param name="strOutputFolderPath">Output folder path</param>
    ''' <param name="serverName">Server name</param>
    ''' <param name="dctDatabaseNamesAndOutputPaths">Dictionary where keys are database names and values will be updated to have the output folder path used</param>
    ''' <param name="useIntegratedAuthentication">True for integrated authentication, false to use loginUsername and loginPassword</param>
    ''' <param name="loginUsername">Sql server login to use when useIntegratedAuthentication is false</param>
    ''' <param name="loginPassword">Sql server password to use when useIntegratedAuthentication is false</param>
    ''' <returns>True if success, false if a problem</returns>
    ''' <remarks>
    ''' If CreateFolderForEachDB is true, or if dctDatabaseNamesAndOutputPaths contains more than one entry,
    ''' then each database will be scripted to a subfolder below the output folder
    ''' </remarks>
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

            ' Populate a dictionary with the database names (properly capitalized) and the output folder path used for each
            Dim dctdatabaseNameLookup = New Dictionary(Of String, String)(StringComparer.CurrentCultureIgnoreCase)

            For Each exportedDatabase In mDBSchemaExporter.SchemaOutputFolders
                dctdatabaseNameLookup.Add(exportedDatabase.Key, exportedDatabase.Value)
            Next

            ' Add any other databases in lstDatabaseList that are missing (as would be the case if it doesn't exist on the server)
            For Each databaseName In lstDatabaseList
                If Not dctdatabaseNameLookup.ContainsKey(databaseName) Then
                    dctdatabaseNameLookup.Add(databaseName, String.Empty)
                End If
            Next

            ' Now update dctDatabaseNamesAndOutputPaths to match dctdatabaseNameLookup (which has properly capitalized database names)
            dctDatabaseNamesAndOutputPaths = dctdatabaseNameLookup

            Return blnSuccess

        Catch ex As Exception
            HandleException("Error in ExportSchema configuring mDBSchemaExporter", ex)
            Return False
        End Try

    End Function

    ''' <summary>
    ''' Compare the contents of the two files
    ''' </summary>
    ''' <param name="fiBase"></param>
    ''' <param name="fiComparison"></param>
    ''' <param name="eDifferenceReason">Output parameter: reason for the difference, or eDifferenceReasonType.Unchanged if identical</param>
    ''' <returns>True if the files differ (i.e. if they do not match)</returns>
    ''' <remarks>Files that begin with DBDefinition are treated specially in that the database Size values are ignored when looking for differences</remarks>
    Private Function FilesDiffer(
      ByVal fiBase As FileInfo,
      ByVal fiComparison As FileInfo,
      ByRef eDifferenceReason As eDifferenceReasonType) As Boolean

        Try
            eDifferenceReason = eDifferenceReasonType.Unchanged

            If Not fiBase.Exists Then Return False

            If Not fiComparison.Exists Then
                eDifferenceReason = eDifferenceReasonType.NewFile
                Return True
            End If

            Dim dbDefinitionFile = False

            If fiBase.Name.StartsWith(clsExportDBSchema.DB_DEFINITION_FILE_PREFIX) Then
                ' DB Definition file; don't worry if file lengths differ
                dbDefinitionFile = True
            Else
                If fiBase.Length <> fiComparison.Length Then
                    eDifferenceReason = eDifferenceReasonType.Changed
                    Return True
                End If
            End If


            ' Perform a line-by-line comparison

            Using srBaseFile = New StreamReader(New FileStream(fiBase.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

                Using srComparisonFile = New StreamReader(New FileStream(fiComparison.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

                    While srBaseFile.Peek > -1
                        Dim strLineIn = srBaseFile.ReadLine()

                        If srComparisonFile.Peek > -1 Then
                            Dim strComparisonLine = srComparisonFile.ReadLine()

                            Dim linesMatch = (String.Compare(strLineIn, strComparisonLine) = 0)

                            If linesMatch Then Continue While

                            If dbDefinitionFile AndAlso strLineIn.StartsWith("( NAME =") AndAlso strComparisonLine.StartsWith("( NAME =") Then
                                Dim lstSourceCols = strLineIn.Split(","c).ToList()
                                Dim lstComparisonCols = strComparisonLine.Split(","c).ToList()

                                If lstSourceCols.Count = lstComparisonCols.Count Then
                                    linesMatch = True
                                    For dataColumnIndex = 0 To lstSourceCols.Count - 1
                                        Dim sourceValue = lstSourceCols(dataColumnIndex).Trim()
                                        Dim comparisonValue = lstComparisonCols(dataColumnIndex).Trim()

                                        If sourceValue.StartsWith("SIZE") AndAlso comparisonValue.StartsWith("SIZE") Then
                                            ' Don't worry if these values differ
                                        ElseIf String.Compare(sourceValue, comparisonValue) <> 0 Then
                                            linesMatch = False
                                            Exit For
                                        End If
                                    Next
                                End If

                            End If

                            If Not linesMatch Then
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

    Public Overrides Function GetErrorMessage() As String
        ' Returns "" if no error

        Dim strErrorMessage As String

        If MyBase.ErrorCode = eProcessFoldersErrorCodes.LocalizedError Or
     MyBase.ErrorCode = eProcessFoldersErrorCodes.NoError Then
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

        Me.GitUpdate = False
        Me.HgUpdate = False
        Me.SvnUpdate = False

        Me.CommitUpdates = False

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
                strParameterFilePath = Path.Combine(Path.GetDirectoryName(Reflection.Assembly.GetExecutingAssembly().Location), Path.GetFileName(strParameterFilePath))
                If Not File.Exists(strParameterFilePath) Then
                    MyBase.SetBaseClassErrorCode(eProcessFoldersErrorCodes.ParameterFileNotFound)
                    Return False
                End If
            End If

            If objSettingsFile.LoadSettings(strParameterFilePath) Then
                If Not objSettingsFile.SectionPresent(OPTIONS_SECTION) Then
                    ShowErrorMessage("The node '<section name=""" & OPTIONS_SECTION & """> was not found in the parameter file: " & strParameterFilePath)
                    MyBase.SetBaseClassErrorCode(eProcessFoldersErrorCodes.InvalidParameterFile)
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


    Private Function ParseGitStatus(
     ByVal diTargetFolder As DirectoryInfo,
     ByVal standardOutput As String,
     ByRef intModifiedFileCount As Integer,
     ByRef lstNewFiles As List(Of String)) As Boolean

        ' Example output for Git with verbose output
        ' 	# On branch master
        ' 	# Your branch is behind 'origin/master' by 1 commit, and can be fast-forwarded.
        ' 	#   (use "git pull" to update your local branch)
        ' 	#
        '	# Changes not staged for commit:
        '	#   (use "git add <file>..." to update what will be committed)
        '	#   (use "git checkout -- <file>..." to discard changes in working directory)
        '	#
        '	#       modified:   PNNLOmics/Algorithms/Alignment/LcmsWarp/LcmsWarp.cs
        '	#
        '	# Untracked files:
        '	#   (use "git add <file>..." to include in what will be committed)
        '	#
        '	#       MyNewFile.txt
        '	no changes added to commit (use "git add" and/or "git commit -a")

        ' Example output with Git for short output (-s)
        '  M PNNLOmics/Algorithms/Alignment/LcmsWarp/LcmsWarp.cs
        ' ?? MyNewFile.txt

        Dim lstNewOrModifiedStatusSymbols = New List(Of Char) From {"M"c, "A"c, "R"c}

        If lstNewFiles Is Nothing Then
            lstNewFiles = New List(Of String)
        Else
            lstNewFiles.Clear()
        End If

        Dim blnParsingUntrackedFiles = False

        Using srReader = New StringReader(standardOutput)
            While srReader.Peek > -1
                Dim statusLine = srReader.ReadLine()

                If String.IsNullOrWhiteSpace(statusLine) OrElse statusLine.Length < 4 Then Continue While

                If statusLine.StartsWith("fatal: Not a git repository") Then
                    ShowErrorMessage("Folder is not tracked by Git: " & diTargetFolder.FullName)
                    Return False
                End If

                Dim fileIndexStatus As Char = statusLine.Chars(0)
                Dim fileWorktreeStatus As Char = statusLine.Chars(1)
                Dim filePath = statusLine.Substring(3)

                If fileIndexStatus = "?"c Then
                    lstNewFiles.Add(filePath)
                ElseIf lstNewOrModifiedStatusSymbols.Contains(fileIndexStatus) OrElse lstNewOrModifiedStatusSymbols.Contains(fileWorktreeStatus) Then
                    intModifiedFileCount += 1
                End If

            End While
        End Using

        Return True

    End Function

    Private Function ParseSvnHgStatus(
      ByVal diTargetFolder As DirectoryInfo,
      ByVal standardOutput As String,
      ByVal eRepoManager As eRepoManagerType,
      ByRef intModifiedFileCount As Integer,
      ByRef lstNewFiles As List(Of String)) As Boolean

        ' Example output for Svn where M is modified and ? is new
        '	    M       F:\My Documents\Projects\DataMining\Database_Schema\DMS\DMS5\UpdateAnalysisJobStateNameCached.sql
        '	    ?       F:\My Documents\Projects\DataMining\Database_Schema\DMS\DMS5\UpdateAnalysisJobToolNameCached.sql
        '	    M       F:\My Documents\Projects\DataMining\Database_Schema\DMS\DMS5\V_Analysis_Job_List_Report_2.sql
        '	    M       F:\My Documents\Projects\DataMining\Database_Schema\DMS\DMS5\V_GetPipelineJobParameters.sql

        ' Example output for Hg where M is modified and ? is new
        '	    M F:\My Documents\Projects\DataMining\Database_Schema\DMS\DMS5\UpdateAnalysisJobStateNameCached.sql
        '	    ? F:\My Documents\Projects\DataMining\Database_Schema\DMS\DMS5\UpdateAnalysisJobToolNameCached.sql
        '	    M F:\My Documents\Projects\DataMining\Database_Schema\DMS\DMS5\V_Analysis_Job_List_Report_2.sql
        '	    M F:\My Documents\Projects\DataMining\Database_Schema\DMS\DMS5\V_GetPipelineJobParameters.sql


        Dim lstNewOrModifiedStatusSymbols = New List(Of Char) From {"M"c, "A"c, "R"c}
        Dim minimumLineLength As Integer

        If lstNewFiles Is Nothing Then
            lstNewFiles = New List(Of String)
        Else
            lstNewFiles.Clear()
        End If

        If eRepoManager = eRepoManagerType.Svn Then
            minimumLineLength = 8
        Else
            minimumLineLength = 3
        End If

        Using srReader = New StringReader(standardOutput)
            While srReader.Peek > -1
                Dim statusLine = srReader.ReadLine()

                If String.IsNullOrWhiteSpace(statusLine) OrElse statusLine.Length < minimumLineLength Then Continue While

                If statusLine.StartsWith("svn: warning") AndAlso statusLine.Contains("is not a working copy") Then
                    ShowErrorMessage("Folder is not tracked by SVN: " & diTargetFolder.FullName)
                    Return False
                ElseIf statusLine.StartsWith("abort: no repository found in ") Then
                    ShowErrorMessage("Folder is not tracked by Hg: " & diTargetFolder.FullName)
                    Return False
                End If

                Dim fileModStatus As Char = statusLine.Chars(0)
                Dim filePropertyStatus As Char = " "c
                Dim filePath As String

                If eRepoManager = eRepoManagerType.Svn Then
                    filePropertyStatus = statusLine.Chars(1)
                    filePath = statusLine.Substring(8).Trim()
                Else
                    filePath = statusLine.Substring(2).Trim()
                End If

                If lstNewOrModifiedStatusSymbols.Contains(fileModStatus) Then
                    intModifiedFileCount += 1

                ElseIf filePropertyStatus = "M"c Then
                    intModifiedFileCount += 1

                ElseIf fileModStatus = "?"c Then
                    lstNewFiles.Add(filePath)
                End If

            End While
        End Using

        Return True

    End Function

    Public Overloads Overrides Function ProcessFolder(ByVal strInputFolderPath As String,
      ByVal strOutputFolderAlternatePath As String,
      ByVal strParameterFilePath As String,
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

    Public Shadows Function ProcessAndRecurseFolders(
      ByVal strInputFolderPath As String,
      ByVal strOutputFolderAlternatePath As String,
      ByVal strParameterFilePath As String,
      ByVal intRecurseFoldersMaxLevels As Integer) As Boolean
        ' Returns True if success, False if failure

        Return ProcessFolder(strInputFolderPath, strOutputFolderAlternatePath, strParameterFilePath, True)

    End Function

    ''' <summary>
    ''' Script the objects for one or more databases
    ''' </summary>
    ''' <param name="outputFolderPath">Output folder path</param>
    ''' <param name="serverName">Server name</param>
    ''' <param name="databaseList">Database names to script</param>
    ''' <returns>True if success, false if a problem</returns>
    Public Function ProcessDatabase(ByVal outputFolderPath As String, ByVal serverName As String, ByVal databaseList As IEnumerable(Of String)) As Boolean
        Dim blnSuccess As Boolean

        Try
            ' Keys in this dictionary are database names
            ' Values are the output folder path used (values will be defined by ExportSchema then used by SyncSchemaFiles)

            Dim dctDatabaseNamesAndOutputPaths = New Dictionary(Of String, String)
            For Each databaseName In databaseList
                dctDatabaseNamesAndOutputPaths.Add(databaseName, String.Empty)
            Next

            blnSuccess = ExportSchema(outputFolderPath, serverName, dctDatabaseNamesAndOutputPaths)

            If blnSuccess AndAlso Sync Then
                blnSuccess = SyncSchemaFiles(dctDatabaseNamesAndOutputPaths, SyncFolderPath)
            End If

            Return blnSuccess

        Catch ex As Exception
            HandleException("Error in ProcessDatabase", ex)
            Return False
        End Try

    End Function

    ''' <summary>
    ''' Script the objects for one database
    ''' </summary>
    ''' <param name="outputFolderPath">Output folder path</param>
    ''' <param name="serverName">Server name</param>
    ''' <param name="databaseName">Database name to script</param>
    ''' <returns>True if success, false if a problem</returns>
    Public Function ProcessDatabase(ByVal outputFolderPath As String, ByVal serverName As String, ByVal databaseName As String) As Boolean

        Dim databaseList As New List(Of String) From {databaseName}

        Return ProcessDatabase(outputFolderPath, serverName, databaseList)

    End Function

    Private Function RunCommand(ByVal exePath As String, ByVal cmdArgs As String, ByRef standardOutput As String, ByVal maxRuntimeSeconds As Integer) As Boolean

        standardOutput = String.Empty

        Try

            m_ProgRunner = New PRISM.Processes.clsProgRunner
            With m_ProgRunner
                .Arguments = cmdArgs
                .CreateNoWindow = True
                .MonitoringInterval = 100
                .Name = IO.Path.GetFileNameWithoutExtension(exePath)
                .Program = exePath
                .Repeat = False
                .RepeatHoldOffTime = 0
                .WorkDir = GetAppFolderPath()
                .CacheStandardOutput = True
                .EchoOutputToConsole = True
                .WriteConsoleOutputToFile = False
                .ConsoleOutputFilePath = String.Empty
            End With

            Dim dtStartTime = DateTime.UtcNow
            Dim dtLastStatus = DateTime.UtcNow
            Dim executionAborted As Boolean = False

            ' Start the program executing
            m_ProgRunner.StartAndMonitorProgram()

            ' Wait for it to exit
            If maxRuntimeSeconds < 10 Then maxRuntimeSeconds = 10

            ' Loop until program is complete, or until maxRuntimeSeconds seconds elapses
            While (m_ProgRunner.State <> PRISM.Processes.clsProgRunner.States.NotMonitoring)
                Threading.Thread.Sleep(100)

                Dim elapsedSeconds = DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds

                If elapsedSeconds > maxRuntimeSeconds Then
                    ShowErrorMessage("Program execution has surpassed " & maxRuntimeSeconds & " seconds; aborting " & exePath)
                    m_ProgRunner.StopMonitoringProgram(Kill:=True)
                    executionAborted = True
                End If

                If m_ProgRunner.State = PRISM.Processes.clsProgRunner.States.StartingProcess AndAlso DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds > 30 AndAlso DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds < 90 Then
                    ' It has taken over 30 seconds for the thread to start
                    ' Try re-joining
                    m_ProgRunner.JoinThreadNow()
                End If

                If DateTime.UtcNow.Subtract(dtLastStatus).TotalSeconds > 15 Then
                    dtLastStatus = DateTime.UtcNow
                    Console.WriteLine("Waiting for " & Path.GetFileName(exePath) & ", " & elapsedSeconds.ToString("0") & " seconds elapsed")
                End If

            End While

            If executionAborted Then
                Console.WriteLine("ProgRunner was aborted for " & exePath)
                Return True
            Else
                standardOutput = m_ProgRunner.CachedConsoleOutput
                Return True
            End If

        Catch ex As Exception
            HandleException("Error in RunCommand", ex)
            Return False
        End Try

    End Function

    Private Function SyncSchemaFiles(ByVal dctDatabaseNamesAndOutputPaths As Dictionary(Of String, String), ByVal folderPathForSync As String) As Boolean
        Try
            ResetProgress("Synchronizing with " & folderPathForSync)

            Dim intDBsProcessed As Integer = 0

            For Each dbEntry In dctDatabaseNamesAndOutputPaths

                Dim databaseName = dbEntry.Key
                Dim schemaOutputFolder = dbEntry.Value

                If String.IsNullOrWhiteSpace(schemaOutputFolder) Then
                    ShowErrorMessage("Schema output folder was not reported for " & databaseName & "; unable to synchronize")
                    Continue For
                End If

                Dim percentComplete As Single = intDBsProcessed / CSng(dctDatabaseNamesAndOutputPaths.Count) * 100

                UpdateProgress("Synchronizing database " & databaseName, percentComplete)

                Dim diSourceFolder = New DirectoryInfo(schemaOutputFolder)

                Dim targetFolderPath = String.Copy(folderPathForSync)
                If dctDatabaseNamesAndOutputPaths.Count > 1 OrElse CreateFolderForEachDB Then
                    targetFolderPath = Path.Combine(targetFolderPath, databaseName)
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
                Dim intFilesCopied As Integer = 0

                Dim lstfilesToCopy = diSourceFolder.GetFiles()

                For Each fiFile As FileInfo In lstfilesToCopy

                    Dim fileNameLcase = fiFile.Name.ToLower()

                    If fileNameLcase.StartsWith("x_") OrElse fileNameLcase.StartsWith("t_tmp_") Then
                        ShowMessage("Skipping " & databaseName & " object " & fiFile.Name)
                        Continue For
                    End If

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
                        intFilesCopied += 1

                    End If

                    intFilesProcessed += 1
                Next

                If Me.SvnUpdate Then
                    UpdateRepoChanges(diTargetFolder, eRepoManagerType.Svn)
                End If

                If Me.HgUpdate Then
                    UpdateRepoChanges(diTargetFolder, eRepoManagerType.Hg)
                End If

                If Me.GitUpdate Then
                    UpdateRepoChanges(diTargetFolder, eRepoManagerType.Git)
                End If

                intDBsProcessed += 1
            Next

            Return True

        Catch ex As Exception
            HandleException("Error in SyncSchemaFiles", ex)
            Return False
        End Try

    End Function

    Protected Function UpdateRepoChanges(ByVal diTargetFolder As DirectoryInfo, ByVal eRepoManager As eRepoManagerType) As Boolean

        Const SVN_EXE_PATH = "C:\Program Files\TortoiseSVN\bin\svn.exe"
        Const HG_EXE_PATH = "C:\Program Files\TortoiseHg\hg.exe"
        Const GIT_EXE_PATH = ""

        Dim fiRepoExe As FileInfo = Nothing
        Dim strToolName As String = "Unknown"

        Try

            Select Case eRepoManager
                Case eRepoManagerType.Svn
                    fiRepoExe = New FileInfo(SVN_EXE_PATH)
                    strToolName = "SVN"

                Case eRepoManagerType.Hg
                    fiRepoExe = New FileInfo(HG_EXE_PATH)
                    strToolName = "Hg"

                Case eRepoManagerType.Git
                    If String.IsNullOrWhiteSpace(GIT_EXE_PATH) Then
                        Throw New NotSupportedException("Not yet supported")
                    End If

                    fiRepoExe = New FileInfo(GIT_EXE_PATH)
                    strToolName = "Git"

            End Select

            If Not fiRepoExe.Exists Then
                ShowErrorMessage("Repo exe not found at " & fiRepoExe.FullName)
                Return False
            End If

            Console.WriteLine()
            UpdateProgress("Looking for new / modified files tracked by " & strToolName & " at " & diTargetFolder.FullName)

            Dim standardOutput = String.Empty
            Dim maxRuntimeSeconds As Integer = 900
            Dim blnSuccess As Boolean

            Dim cmdArgs As String = " status """ & diTargetFolder.FullName & """"
            If eRepoManager = eRepoManagerType.Git Then cmdArgs &= " -s -u"

            blnSuccess = RunCommand(fiRepoExe.FullName, cmdArgs, standardOutput, maxRuntimeSeconds)


            If Not blnSuccess Then Return False

            ' Look for modified or new files in standardOutput

            Dim intModifiedFileCount As Integer = 0
            Dim lstNewFiles As New List(Of String)

            If eRepoManager = eRepoManagerType.Svn Or eRepoManager = eRepoManagerType.Hg Then
                blnSuccess = ParseSvnHgStatus(diTargetFolder, standardOutput, eRepoManager, intModifiedFileCount, lstNewFiles)
            Else
                ' Git 
                blnSuccess = ParseGitStatus(diTargetFolder, standardOutput, intModifiedFileCount, lstNewFiles)
            End If

            If Not blnSuccess Then Return False

            If intModifiedFileCount > 0 OrElse lstNewFiles.Count > 0 Then

                If intModifiedFileCount > 0 Then
                    UpdateProgress("Found " & intModifiedFileCount & " modified " & CheckPlural(intModifiedFileCount, "file", "files"))
                End If

                If lstNewFiles.Count > 0 Then
                    UpdateProgress("Adding " & lstNewFiles.Count & " new " & CheckPlural(lstNewFiles.Count, "file", "files") & " to " & strToolName)

                    ' Add each of the new files
                    For Each newFilePath In lstNewFiles
                        cmdArgs = " add """ & newFilePath & """"
                        maxRuntimeSeconds = 30

                        blnSuccess = RunCommand(fiRepoExe.FullName, cmdArgs, standardOutput, maxRuntimeSeconds)

                        If Not blnSuccess Then
                            ShowErrorMessage("Aborting " & strToolName & " commit due to error reported by " & fiRepoExe.Name & ControlChars.NewLine & standardOutput)
                            Return False
                        End If

                    Next
                End If

                Dim commitMessage = DateTime.Now.ToString("yyyy-MM-dd") & " auto-commit"

                If Not Me.CommitUpdates Then
                    ShowMessage("Use /Commit to commit changes with commit message: " & commitMessage)
                Else
                    UpdateProgress("Commiting changes to " & strToolName & ": " & commitMessage)

                    ' Commit the changes
                    cmdArgs = " commit """ & diTargetFolder.FullName & """ --message """ & commitMessage & """"
                    maxRuntimeSeconds = 120

                    blnSuccess = RunCommand(fiRepoExe.FullName, cmdArgs, standardOutput, maxRuntimeSeconds)

                    If Not blnSuccess Then
                        ShowErrorMessage("Commit error" & ControlChars.NewLine & standardOutput)
                        Return False
                    End If

                    If eRepoManager = eRepoManagerType.Hg Or eRepoManager = eRepoManagerType.Git Then
                        ' Push the changes to the master

                        cmdArgs = " push"
                        maxRuntimeSeconds = 300

                        blnSuccess = RunCommand(fiRepoExe.FullName, cmdArgs, standardOutput, maxRuntimeSeconds)

                    End If
                End If

            End If

            Return True

        Catch ex As Exception
            HandleException("Error in UpdateRepoChanges for tool " & strToolName, ex)
            Return False
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
