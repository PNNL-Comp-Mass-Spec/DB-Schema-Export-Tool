Option Strict On

' -------------------------------------------------------------------------------
' Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA) in 2006

' E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov
' Website: https://omics.pnl.gov/ or https://www.pnnl.gov/sysbio/ or https://panomics.pnnl.gov/
' -------------------------------------------------------------------------------
'
' Licensed under the 2-Clause BSD License; you may not use this file except
' in compliance with the License.  You may obtain a copy of the License at
' https://opensource.org/licenses/BSD-2-Clause
'
' Copyright 2018 Battelle Memorial Institute

Imports System.Text.RegularExpressions
Imports PRISM
Imports ShFolderBrowser.FolderBrowser

''' <summary>
''' This program uses class clsExportDBSchema to export the objects from the
''' database(s) on the selected server
''' </summary>
Public Class frmMain

    ''' <summary>
    ''' Constructor
    ''' </summary>
    Public Sub New()
        ' This call is required by the Windows Form Designer.
        InitializeComponent()

        ' Add any initialization after the InitializeComponent() call.
        InitializeControls()
    End Sub

#Region "Constants and Enums"
    Public Const XML_SETTINGS_FILE_NAME As String = "DB_Schema_Export_Tool_Settings.xml"
    Public Const XML_SECTION_DATABASE_SETTINGS As String = "DBSchemaExportDatabaseSettings"
    Public Const XML_SECTION_PROGRAM_OPTIONS As String = "DBSchemaExportOptions"

    Private Const THREAD_WAIT_MSEC As Integer = 150

    Private Const ROW_COUNT_SEPARATOR As String = ControlChars.Tab & " ("

    Private Enum eTableNameSortModeConstants
        Name = 0
        RowCount = 1
    End Enum
#End Region

#Region "Structures"
#End Region

#Region "Classwide Variables"
    Private mXmlSettingsFilePath As String

    Private mSchemaExportOptions As clsSchemaExportOptions
    Private mDatabaseListToProcess As List(Of String)
    Private mTableNamesForDataExport As List(Of String)

    ' Keys are table names; values are row counts, though row counts will be 0 if mCachedTableListIncludesRowCounts = False
    Private mCachedTableList As Dictionary(Of String, Int64)

    Private mCachedTableListIncludesRowCounts As Boolean

    Private mTableNamesToAutoSelect As List(Of String)

    ' Note: Must contain valid RegEx statements (tested case-insensitive)
    Private mTableNameAutoSelectRegEx As List(Of String)

    Private mDefaultDMSDatabaseList As List(Of String)
    Private mDefaultMTSDatabaseList As List(Of String)

    Private mWorking As Boolean

    Private mThread As Threading.Thread

    Private WithEvents mDBSchemaExporter As clsExportDBSchema
    Private mSchemaExportSuccess As Boolean

#End Region

#Region "Delegates"
    Private Delegate Sub AppendNewMessageHandler(message As String, isError As Boolean)
    Private Delegate Sub UpdatePauseUnpauseCaptionHandler(ePauseStatus As clsExportDBSchema.ePauseStatusConstants)

    Private Delegate Sub ProgressUpdateHandler(taskDescription As String, percentComplete As Single)
    Private Delegate Sub ProgressCompleteHandler()

    Private Delegate Sub SubTaskProgressUpdateHandler(taskDescription As String, percentComplete As Single)
    Private Delegate Sub SubTaskProgressCompleteHandler()

    Private Delegate Sub HandleDBExportStartingEventHandler(databaseName As String)

#End Region

    Private Sub AppendNewMessage(message As String, isError As Boolean)
        If isError And Not message.StartsWith("Error", StringComparison.OrdinalIgnoreCase) Then
            lblMessage.Text = "Error: " & message
        Else
            lblMessage.Text = message
        End If

        Application.DoEvents()
    End Sub

    Private Sub ConfirmAbortRequest()
        Dim ePauseStatusSaved As clsExportDBSchema.ePauseStatusConstants
        Dim eResponse As DialogResult

        If Not mDBSchemaExporter Is Nothing Then
            ePauseStatusSaved = mDBSchemaExporter.PauseStatus

            mDBSchemaExporter.RequestPause()
            Application.DoEvents()

            eResponse = MessageBox.Show("Are you sure you want to abort processing?", "Abort", MessageBoxButtons.YesNoCancel, MessageBoxIcon.Question, MessageBoxDefaultButton.Button1)
            If eResponse = DialogResult.Yes Then
                mDBSchemaExporter.AbortProcessingNow()

                ' Note that AbortProcessingNow should have called RequestUnpause, but we'll call it here just in case
                mDBSchemaExporter.RequestUnpause()
            Else
                If ePauseStatusSaved = clsExportDBSchema.ePauseStatusConstants.Unpaused OrElse
                   ePauseStatusSaved = clsExportDBSchema.ePauseStatusConstants.UnpauseRequested Then
                    mDBSchemaExporter.RequestUnpause()
                End If
            End If
            Application.DoEvents()

        End If

    End Sub

    Private Sub EnableDisableControls()

        Try
            txtUsername.Enabled = Not chkUseIntegratedAuthentication.Checked
            txtPassword.Enabled = Not chkUseIntegratedAuthentication.Checked

            cmdGo.Visible = Not mWorking
            cmdExit.Visible = Not mWorking
            fraConnectionSettings.Enabled = Not mWorking
            fraOutputOptions.Enabled = Not mWorking

            mnuEditStart.Enabled = Not mWorking
            mnuEditResetOptions.Enabled = Not mWorking

        Catch ex As Exception
            MessageBox.Show("Error in EnableDisableControls: " & ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
        End Try
    End Sub

    Private Function GetAppFolderPath() As String
        Return GetAppFolderPath(True)
    End Function

    Private Function GetAppFolderPath(returnParentIfFolderNamedDebug As Boolean) As String
        Const DEBUG_FOLDER_NAME = "\debug"

        ' Could use Application.StartupPath, but .GetExecutingAssembly is better
        Dim strPath As String

        strPath = IO.Path.GetDirectoryName(Reflection.Assembly.GetExecutingAssembly().Location)
        If returnParentIfFolderNamedDebug Then
            If strPath.ToLower.EndsWith(DEBUG_FOLDER_NAME) Then
                strPath = strPath.Substring(0, strPath.Length - DEBUG_FOLDER_NAME.Length)
            End If
        End If

        Return strPath
    End Function

    Private Function GetSelectedDatabases() As List(Of String)
        Return GetSelectedListboxItems(lstDatabasesToProcess)
    End Function

    Private Function GetSelectedTableNamesForDataExport(warnIfRowCountOverThreshold As Boolean) As List(Of String)
        Dim lstTableNames As List(Of String)

        lstTableNames = GetSelectedListboxItems(lstTableNamesToExportData)

        StripRowCountsFromTableNames(lstTableNames)

        If lstTableNames.Count = 0 OrElse Not mCachedTableListIncludesRowCounts OrElse Not warnIfRowCountOverThreshold Then
            Return lstTableNames
        End If

        Dim lstValidTableNames = New List(Of String)(lstTableNames.Count)

        ' See if any of the tables in lstTableNames has more than clsExportDBSchema.DATA_ROW_COUNT_WARNING_THRESHOLD rows
        For Each tableName In lstTableNames
            Dim keepTable = True

            Dim tableRowCount As Int64
            If mCachedTableList.TryGetValue(tableName, tableRowCount) Then

                If tableRowCount >= clsExportDBSchema.DATA_ROW_COUNT_WARNING_THRESHOLD Then
                    Dim eResponse = MessageBox.Show("Warning, table " & tableName & " has " &
                      tableRowCount.ToString & " rows.  Are you sure you want to export data from it?",
                      "Row Count Over " & clsExportDBSchema.DATA_ROW_COUNT_WARNING_THRESHOLD.ToString,
                      MessageBoxButtons.YesNoCancel, MessageBoxIcon.Question, MessageBoxDefaultButton.Button2)

                    If eResponse = DialogResult.No Then
                        keepTable = False
                    ElseIf eResponse = DialogResult.Cancel Then
                        Exit For
                    End If
                End If

            Else
                ' Table not found; keep it anyway
            End If

            If keepTable Then
                lstValidTableNames.Add(tableName)
            End If
        Next

        Return lstValidTableNames

    End Function

    Private Function GetSelectedListboxItems(ByRef objListbox As ListBox) As List(Of String)
        Dim lstItems As List(Of String)
        lstItems = New List(Of String)(objListbox.SelectedItems.Count + 1)

        Try

            lstItems.AddRange(From objItem As Object In objListbox.SelectedItems Select CStr(objItem))

        Catch ex As Exception
            If lstItems Is Nothing Then
                lstItems = New List(Of String)
            End If
        End Try

        Return lstItems

    End Function

    Private Function GetSettingsFilePath() As String
        Return IO.Path.Combine(GetAppFolderPath(), XML_SETTINGS_FILE_NAME)
    End Function

    Private Sub HandleDBExportStartingEvent(databaseName As String)
        Try
            lblProgress.Text = "Exporting schema from " & databaseName

            If mnuEditPauseAfterEachDatabase.Checked Then
                mDBSchemaExporter.RequestPause()
            End If
        Catch ex As Exception
            MessageBox.Show("Error in HandleDBExportStartingEvent: " & ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
        End Try
    End Sub

    Private Sub InitializeProgressBars()
        lblProgress.Text = String.Empty
        With pbarProgress
            .Minimum = 0
            .Maximum = 100
            .Value = 0
        End With

        lblSubtaskProgress.Text = String.Empty
        With pbarSubtaskProgress
            .Minimum = 0
            .Maximum = 100
            .Value = 0
        End With
    End Sub

    Private Sub IniFileLoadOptions()
        ' Prompts the user to select a file to load the options from

        Dim strFilePath As String

        Dim objOpenFile As New OpenFileDialog

        strFilePath = String.Copy(mXmlSettingsFilePath)

        With objOpenFile
            .AddExtension = True
            .CheckFileExists = True
            .CheckPathExists = True
            .DefaultExt = ".xml"
            .DereferenceLinks = True
            .Multiselect = False
            .ValidateNames = True

            .Filter = "Settings files (*.xml)|*.xml|All files (*.*)|*.*"

            .FilterIndex = 1

            If strFilePath.Length > 0 Then
                Try
                    .InitialDirectory = IO.Directory.GetParent(strFilePath).ToString
                Catch
                    .InitialDirectory = GetAppFolderPath()
                End Try
            Else
                .InitialDirectory = GetAppFolderPath()
            End If

            .FileName = String.Empty
            .Title = "Specify file to load options from"

            .ShowDialog()
            If .FileName.Length > 0 Then
                mXmlSettingsFilePath = .FileName

                IniFileLoadOptions(mXmlSettingsFilePath, True, True)
            End If
        End With

    End Sub

    Private Sub IniFileLoadOptions(strFilePath As String, resetToDefaultsPriorToLoad As Boolean, connectToServer As Boolean)
        ' Loads options from the given file

        Dim objXmlFile As New XmlSettingsFileAccessor

        Dim strServerNameSaved As String

        Try
            If resetToDefaultsPriorToLoad Then
                ResetToDefaults(False)
            End If

            ' Sleep for 100 msec, just to be safe
            Threading.Thread.Sleep(100)

            ' Read the settings from the XML file
            With objXmlFile
                ' Pass True to .LoadSettings() to turn off case sensitive matching
                .LoadSettings(strFilePath, False)

                Try
                    Me.Width = .GetParam(XML_SECTION_PROGRAM_OPTIONS, "WindowWidth", Me.Width)
                    Me.Height = .GetParam(XML_SECTION_PROGRAM_OPTIONS, "WindowHeight", Me.Height)

                    strServerNameSaved = txtServerName.Text
                    txtServerName.Text = .GetParam(XML_SECTION_DATABASE_SETTINGS, "ServerName", txtServerName.Text)

                    chkUseIntegratedAuthentication.Checked = .GetParam(XML_SECTION_DATABASE_SETTINGS, "UseIntegratedAuthentication", chkUseIntegratedAuthentication.Checked)
                    txtUsername.Text = .GetParam(XML_SECTION_DATABASE_SETTINGS, "Username", txtUsername.Text)
                    txtPassword.Text = .GetParam(XML_SECTION_DATABASE_SETTINGS, "Password", txtPassword.Text)

                    txtOutputFolderPath.Text = .GetParam(XML_SECTION_PROGRAM_OPTIONS, "OutputFolderPath", txtOutputFolderPath.Text)

                    mnuEditScriptObjectsThreaded.Checked = .GetParam(XML_SECTION_PROGRAM_OPTIONS, "ScriptObjectsThreaded", mnuEditScriptObjectsThreaded.Checked)
                    mnuEditPauseAfterEachDatabase.Checked = .GetParam(XML_SECTION_PROGRAM_OPTIONS, "PauseAfterEachDatabase", mnuEditPauseAfterEachDatabase.Checked)
                    mnuEditIncludeTimestampInScriptFileHeader.Checked = .GetParam(XML_SECTION_PROGRAM_OPTIONS, "IncludeTimestampInScriptFileHeader", mnuEditIncludeTimestampInScriptFileHeader.Checked)

                    chkCreateFolderForEachDB.Checked = .GetParam(XML_SECTION_PROGRAM_OPTIONS, "CreateFolderForEachDB", chkCreateFolderForEachDB.Checked)
                    txtOutputFolderNamePrefix.Text = .GetParam(XML_SECTION_PROGRAM_OPTIONS, "OutputFolderNamePrefix", txtOutputFolderNamePrefix.Text)

                    chkExportServerSettingsLoginsAndJobs.Checked = .GetParam(XML_SECTION_PROGRAM_OPTIONS, "ExportServerSettingsLoginsAndJobs", chkExportServerSettingsLoginsAndJobs.Checked)
                    txtServerOutputFolderNamePrefix.Text = .GetParam(XML_SECTION_PROGRAM_OPTIONS, "ServerOutputFolderNamePrefix", txtServerOutputFolderNamePrefix.Text)

                    mnuEditIncludeTableRowCounts.Checked = .GetParam(XML_SECTION_PROGRAM_OPTIONS, "IncludeTableRowCounts", mnuEditIncludeTableRowCounts.Checked)
                    mnuEditAutoSelectDefaultTableNames.Checked = .GetParam(XML_SECTION_PROGRAM_OPTIONS, "AutoSelectDefaultTableNames", mnuEditAutoSelectDefaultTableNames.Checked)
                    mnuEditSaveDataAsInsertIntoStatements.Checked = .GetParam(XML_SECTION_PROGRAM_OPTIONS, "SaveDataAsInsertIntoStatements", mnuEditSaveDataAsInsertIntoStatements.Checked)
                    mnuEditWarnOnHighTableRowCount.Checked = .GetParam(XML_SECTION_PROGRAM_OPTIONS, "WarnOnHighTableRowCount", mnuEditWarnOnHighTableRowCount.Checked)

                    If lstDatabasesToProcess.Items.Count = 0 OrElse
                       Not strServerNameSaved Is Nothing AndAlso strServerNameSaved.ToLower <> txtServerName.Text.ToLower Then
                        If connectToServer Then
                            UpdateDatabaseList()
                        End If
                    End If

                Catch ex As Exception
                    MessageBox.Show("Invalid parameter in settings file: " & IO.Path.GetFileName(strFilePath), "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
                End Try
            End With

        Catch ex As Exception
            MessageBox.Show("Error loading settings from file: " & strFilePath, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
        End Try

    End Sub

    Private Sub IniFileSaveOptions()
        ' Prompts the user to select a file to load the options from

        Dim strFilePath As String

        Dim objSaveFile As New SaveFileDialog

        strFilePath = String.Copy(mXmlSettingsFilePath)

        With objSaveFile
            .AddExtension = True
            .CheckFileExists = False
            .CheckPathExists = True
            .DefaultExt = ".xml"
            .DereferenceLinks = True
            .OverwritePrompt = False
            .ValidateNames = True

            .Filter = "Settings files (*.xml)|*.xml|All files (*.*)|*.*"

            .FilterIndex = 1

            If strFilePath.Length > 0 Then
                Try
                    .InitialDirectory = IO.Directory.GetParent(strFilePath).ToString
                Catch
                    .InitialDirectory = GetAppFolderPath()
                End Try
            Else
                .InitialDirectory = GetAppFolderPath()
            End If

            If IO.File.Exists(strFilePath) Then
                .FileName = IO.Path.GetFileName(strFilePath)
            Else
                .FileName = XML_SETTINGS_FILE_NAME
            End If

            .Title = "Specify file to save options to"

            .ShowDialog()
            If .FileName.Length > 0 Then
                mXmlSettingsFilePath = .FileName

                IniFileSaveOptions(mXmlSettingsFilePath, False)
            End If
        End With

    End Sub

    Private Sub IniFileSaveOptions(strFilePath As String, Optional ByVal saveWindowDimensionsOnly As Boolean = False)
        Dim objXmlFile As New XmlSettingsFileAccessor

        Try
            With objXmlFile
                ' Pass True to .LoadSettings() here so that newly made Xml files will have the correct capitalization
                .LoadSettings(strFilePath, True)

                Try
                    .SetParam(XML_SECTION_PROGRAM_OPTIONS, "WindowWidth", Me.Width)
                    .SetParam(XML_SECTION_PROGRAM_OPTIONS, "WindowHeight", Me.Height - 20)

                    If Not saveWindowDimensionsOnly Then

                        .SetParam(XML_SECTION_DATABASE_SETTINGS, "ServerName", txtServerName.Text)

                        .SetParam(XML_SECTION_DATABASE_SETTINGS, "UseIntegratedAuthentication", chkUseIntegratedAuthentication.Checked)
                        .SetParam(XML_SECTION_DATABASE_SETTINGS, "Username", txtUsername.Text)
                        .SetParam(XML_SECTION_DATABASE_SETTINGS, "Password", txtPassword.Text)

                        .SetParam(XML_SECTION_PROGRAM_OPTIONS, "OutputFolderPath", txtOutputFolderPath.Text)

                        .SetParam(XML_SECTION_PROGRAM_OPTIONS, "ScriptObjectsThreaded", mnuEditScriptObjectsThreaded.Checked)
                        .SetParam(XML_SECTION_PROGRAM_OPTIONS, "PauseAfterEachDatabase", mnuEditPauseAfterEachDatabase.Checked)
                        .SetParam(XML_SECTION_PROGRAM_OPTIONS, "IncludeTimestampInScriptFileHeader", mnuEditIncludeTimestampInScriptFileHeader.Checked)

                        .SetParam(XML_SECTION_PROGRAM_OPTIONS, "CreateFolderForEachDB", chkCreateFolderForEachDB.Checked)
                        .SetParam(XML_SECTION_PROGRAM_OPTIONS, "OutputFolderNamePrefix", txtOutputFolderNamePrefix.Text)

                        .SetParam(XML_SECTION_PROGRAM_OPTIONS, "ExportServerSettingsLoginsAndJobs", chkExportServerSettingsLoginsAndJobs.Checked)
                        .SetParam(XML_SECTION_PROGRAM_OPTIONS, "ServerOutputFolderNamePrefix", txtServerOutputFolderNamePrefix.Text)

                        .SetParam(XML_SECTION_PROGRAM_OPTIONS, "IncludeTableRowCounts", mnuEditIncludeTableRowCounts.Checked)
                        .SetParam(XML_SECTION_PROGRAM_OPTIONS, "AutoSelectDefaultTableNames", mnuEditAutoSelectDefaultTableNames.Checked)
                        .SetParam(XML_SECTION_PROGRAM_OPTIONS, "SaveDataAsInsertIntoStatements", mnuEditSaveDataAsInsertIntoStatements.Checked)
                        .SetParam(XML_SECTION_PROGRAM_OPTIONS, "WarnOnHighTableRowCount", mnuEditWarnOnHighTableRowCount.Checked)

                    End If

                Catch ex As Exception
                    MessageBox.Show("Error storing parameter in settings file: " & IO.Path.GetFileName(strFilePath), "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
                End Try

                .SaveSettings()
            End With
        Catch ex As Exception
            MessageBox.Show("Error saving settings to file: " & strFilePath, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
        End Try

        ''Dim objProperty As Configuration.SettingsProperty
        ''Dim objProvider As Configuration.SettingsProvider
        ''Dim objAttributes As Configuration.SettingsAttributeDictionary

        ''Try
        ''    objProvider = New Configuration.LocalFileSettingsProvider
        ''    objAttributes = New Configuration.SettingsAttributeDictionary
        ''    objAttributes.Add("", "")

        ''    objProperty = New Configuration.SettingsProperty("ServerName", Type.GetType("String"), objProvider, False, "", Configuration.SettingsSerializeAs.String, objAttributes, False, False)
        ''    My.Settings.Properties.Add(objProperty)

        ''    My.Settings.Item("ServerName") = txtServerName.Text

        ''    My.Settings.Save()
        ''Catch ex As Exception

        ''End Try
    End Sub

    Private Sub PopulateTableNamesToExport(enableAutoSelectDefaultTableNames As Boolean)

        Try
            If mCachedTableList.Count <= 0 Then
                lstTableNamesToExportData.Items.Clear()
                Exit Sub
            End If

            Dim eSortOrder As eTableNameSortModeConstants
            If cboTableNamesToExportSortOrder.SelectedIndex >= 0 Then
                eSortOrder = CType(cboTableNamesToExportSortOrder.SelectedIndex, eTableNameSortModeConstants)
            Else
                eSortOrder = eTableNameSortModeConstants.Name
            End If

            Dim lstSortedTables As List(Of KeyValuePair(Of String, Int64))

            If eSortOrder = eTableNameSortModeConstants.RowCount Then
                ' Sort on RowCount
                lstSortedTables = (From item In mCachedTableList Select item Order By item.Value).ToList()

            Else
                ' Sort on Table Name
                lstSortedTables = (From item In mCachedTableList Select item Order By item.Key).ToList()
            End If

            ' Assure that the auto-select table names are not nothing
            If mTableNamesToAutoSelect Is Nothing Then
                mTableNamesToAutoSelect = New List(Of String)
            End If

            If mTableNameAutoSelectRegEx Is Nothing Then
                mTableNameAutoSelectRegEx = New List(Of String)
            End If

            ' Initialize objRegExArray (we'll fill it below if autoHiglightRows = True)
            Const objRegExOptions As RegexOptions =
              RegexOptions.Compiled Or
              RegexOptions.IgnoreCase Or
              RegexOptions.Singleline

            Dim lstRegExSpecs = New List(Of Regex)
            Dim autoHiglightRows As Boolean

            If mnuEditAutoSelectDefaultTableNames.Checked And enableAutoSelectDefaultTableNames Then
                autoHiglightRows = True

                For Each regexItem In mTableNameAutoSelectRegEx
                    lstRegExSpecs.Add(New Regex(regexItem, objRegExOptions))
                Next
            Else
                autoHiglightRows = False
            End If

            ' Cache the currently selected names so that we can re-highlight them below
            Dim lstSelectedTableNamesSaved = New SortedSet(Of String)
            For Each objItem In lstTableNamesToExportData.SelectedItems
                lstSelectedTableNamesSaved.Add(StripRowCountFromTableName(CStr(objItem)))
            Next

            lstTableNamesToExportData.Items.Clear()

            For Each tableItem In lstSortedTables

                ' tableItem.Key is Table Name
                ' tableItem.Value is the number of rows in the table (if mCachedTableListIncludesRowCounts = True)

                Dim strItem As String

                If mCachedTableListIncludesRowCounts Then
                    strItem = tableItem.Key & ROW_COUNT_SEPARATOR & ValueToTextEstimate(tableItem.Value)
                    If tableItem.Value = 1 Then
                        strItem &= " row)"
                    Else
                        strItem &= " rows)"
                    End If
                Else
                    strItem = String.Copy(tableItem.Key)
                End If

                Dim intItemIndex = lstTableNamesToExportData.Items.Add(strItem)

                Dim highlightCurrentRow = False
                If lstSelectedTableNamesSaved.Contains(tableItem.Key) Then
                    ' User had previously highlighted this table name; re-highlight it
                    highlightCurrentRow = True
                ElseIf autoHiglightRows Then
                    ' Test strTableName against the RegEx values from mTableNameAutoSelectRegEx()
                    For Each regexMatcher In lstRegExSpecs
                        If regexMatcher.Match(tableItem.Key).Success Then
                            highlightCurrentRow = True
                            Exit For
                        End If
                    Next

                    If Not highlightCurrentRow Then
                        ' No match: test strTableName against the names in mTableNamesToAutoSelect
                        If mTableNamesToAutoSelect.Contains(tableItem.Key, StringComparer.CurrentCultureIgnoreCase) Then
                            highlightCurrentRow = True
                        End If
                    End If
                End If

                If highlightCurrentRow Then
                    ' Highlight this table name
                    lstTableNamesToExportData.SetSelected(intItemIndex, True)
                End If
            Next

        Catch ex As Exception
            MessageBox.Show("Error in PopulateTableNamesToExport: " & ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
        End Try

    End Sub

    Private Sub InitializeControls()

        mCachedTableList = New Dictionary(Of String, Int64)

        mDefaultDMSDatabaseList = New List(Of String)
        mDefaultMTSDatabaseList = New List(Of String)

        PopulateComboBoxes()

        SetToolTips()

        ResetToDefaults(False)

        Try
            mXmlSettingsFilePath = GetSettingsFilePath()

            If Not IO.File.Exists(mXmlSettingsFilePath) Then
                IniFileSaveOptions(mXmlSettingsFilePath)
            End If
        Catch ex As Exception
            ' Ignore errors here
        End Try

        IniFileLoadOptions(mXmlSettingsFilePath, False, False)

        EnableDisableControls()

    End Sub

    Private Sub PopulateComboBoxes()
        Dim index As Integer

        Try
            With cboTableNamesToExportSortOrder
                With .Items
                    .Clear()
                    .Insert(eTableNameSortModeConstants.Name, "Sort by Name")
                    .Insert(eTableNameSortModeConstants.RowCount, "Sort by Row Count")
                End With
                .SelectedIndex = eTableNameSortModeConstants.RowCount
            End With

            With lstObjectTypesToScript
                With .Items
                    .Clear()
                    .Insert(clsExportDBSchema.eSchemaObjectTypeConstants.SchemasAndRoles, "Schemas and Roles")
                    .Insert(clsExportDBSchema.eSchemaObjectTypeConstants.Tables, "Tables")
                    .Insert(clsExportDBSchema.eSchemaObjectTypeConstants.Views, "Views")
                    .Insert(clsExportDBSchema.eSchemaObjectTypeConstants.StoredProcedures, "Stored Procedures")
                    .Insert(clsExportDBSchema.eSchemaObjectTypeConstants.UserDefinedFunctions, "User Defined Functions")
                    .Insert(clsExportDBSchema.eSchemaObjectTypeConstants.UserDefinedDataTypes, "User Defined Data Types")
                    .Insert(clsExportDBSchema.eSchemaObjectTypeConstants.UserDefinedTypes, "User Defined Types")
                    .Insert(clsExportDBSchema.eSchemaObjectTypeConstants.Synonyms, "Synonyms")
                End With

                ' Auto-select all of the options
                For index = 0 To .Items.Count - 1
                    .SetSelected(index, True)
                Next index
            End With
        Catch ex As Exception
            MessageBox.Show("Error in PopulateComboBoxes: " & ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
        End Try

    End Sub

    Private Sub ProgressUpdate(taskDescription As String, percentComplete As Single)
        lblProgress.Text = taskDescription
        UpdateProgressBar(pbarProgress, percentComplete)
    End Sub

    Private Sub ProgressComplete()
        pbarProgress.Value = pbarProgress.Maximum
    End Sub

    Private Sub ScriptDBSchemaObjects()

        Dim message As String
        Dim index As Integer

        If mWorking Then Exit Sub

        Try
            ' Validate txtOutputFolderPath.Text
            If txtOutputFolderPath.TextLength = 0 Then
                txtOutputFolderPath.Text = GetAppFolderPath()
            End If

            If Not IO.Directory.Exists(txtOutputFolderPath.Text) Then
                message = "Output folder not found: " & txtOutputFolderPath.Text
                MessageBox.Show(message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
                Exit Sub
            End If

            mSchemaExportOptions = clsExportDBSchema.GetDefaultSchemaExportOptions()

            With mSchemaExportOptions
                .OutputFolderPath = txtOutputFolderPath.Text
                .OutputFolderNamePrefix = txtOutputFolderNamePrefix.Text
                .CreateFolderForEachDB = chkCreateFolderForEachDB.Checked
                .IncludeSystemObjects = mnuEditIncludeSystemObjects.Checked
                .IncludeTimestampInScriptFileHeader = mnuEditIncludeTimestampInScriptFileHeader.Checked

                .ExportServerSettingsLoginsAndJobs = chkExportServerSettingsLoginsAndJobs.Checked
                .ServerOutputFolderNamePrefix = txtServerOutputFolderNamePrefix.Text

                .SaveDataAsInsertIntoStatements = mnuEditSaveDataAsInsertIntoStatements.Checked
                .DatabaseTypeForInsertInto = clsSchemaExportOptions.eTargetDatabaseTypeConstants.SqlServer       ' Reserved for future expansion
                .AutoSelectTableNamesForDataExport = mnuEditAutoSelectDefaultTableNames.Checked

                ' Note: mDBSchemaExporter & mTableNameAutoSelectRegEx will be passed to mDBSchemaExporter below

                For index = 0 To lstObjectTypesToScript.Items.Count - 1
                    Dim selected = lstObjectTypesToScript.GetSelected(index)

                    Select Case CType(index, clsExportDBSchema.eSchemaObjectTypeConstants)
                        Case clsExportDBSchema.eSchemaObjectTypeConstants.SchemasAndRoles
                            .ExportDBSchemasAndRoles = selected
                        Case clsExportDBSchema.eSchemaObjectTypeConstants.Tables
                            .ExportTables = selected
                        Case clsExportDBSchema.eSchemaObjectTypeConstants.Views
                            .ExportViews = selected
                        Case clsExportDBSchema.eSchemaObjectTypeConstants.StoredProcedures
                            .ExportStoredProcedures = selected
                        Case clsExportDBSchema.eSchemaObjectTypeConstants.UserDefinedFunctions
                            .ExportUserDefinedFunctions = selected
                        Case clsExportDBSchema.eSchemaObjectTypeConstants.UserDefinedDataTypes
                            .ExportUserDefinedDataTypes = selected
                        Case clsExportDBSchema.eSchemaObjectTypeConstants.UserDefinedTypes
                            .ExportUserDefinedTypes = selected
                        Case clsExportDBSchema.eSchemaObjectTypeConstants.Synonyms
                            .ExportSynonyms = selected
                    End Select
                Next index

                With .ConnectionInfo
                    .ServerName = txtServerName.Text
                    .UserName = txtUsername.Text
                    .Password = txtPassword.Text
                    .UseIntegratedAuthentication = chkUseIntegratedAuthentication.Checked
                End With
            End With

        Catch ex As Exception
            MessageBox.Show("Error initializing mSchemaExportOptions in ScriptDBSchemaObjects: " & ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
            Exit Sub
        End Try

        Try
            ' Populate mDatabaseListToProcess and mTableNamesForDataExport
            mDatabaseListToProcess = GetSelectedDatabases()
            mTableNamesForDataExport = GetSelectedTableNamesForDataExport(mnuEditWarnOnHighTableRowCount.Checked)

            If mDatabaseListToProcess.Count = 0 And Not mSchemaExportOptions.ExportServerSettingsLoginsAndJobs Then
                MessageBox.Show("No databases or tables were selected; unable to continue", "Nothing To Do", MessageBoxButtons.OK, MessageBoxIcon.Information)
                Exit Sub
            End If

        Catch ex As Exception
            MessageBox.Show("Error determining list of databases (and tables) to process: " & ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
            Exit Sub
        End Try

        Try
            If mDBSchemaExporter Is Nothing Then
                mDBSchemaExporter = New clsExportDBSchema()
            End If

            If Not mTableNamesToAutoSelect Is Nothing Then
                mDBSchemaExporter.TableNamesToAutoSelect = mTableNamesToAutoSelect
            End If

            If Not mTableNameAutoSelectRegEx Is Nothing Then
                mDBSchemaExporter.TableNameAutoSelectRegEx = mTableNameAutoSelectRegEx
            End If
        Catch ex As Exception
            MessageBox.Show("Error instantiating mDBSchemaExporter and updating the data export auto-select lists: " & ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
            Exit Sub
        End Try

        Try
            mWorking = True
            EnableDisableControls()

            UpdatePauseUnpauseCaption(clsExportDBSchema.ePauseStatusConstants.Unpaused)
            Application.DoEvents()

            If Not mnuEditScriptObjectsThreaded.Checked Then
                ScriptDBSchemaObjectsThread()
            Else
                ' Use the following to call the SP on a separate thread
                mThread = New Threading.Thread(AddressOf ScriptDBSchemaObjectsThread)
                mThread.Start()
                Threading.Thread.Sleep(THREAD_WAIT_MSEC)

                Do While mThread.ThreadState = Threading.ThreadState.Running OrElse mThread.ThreadState = Threading.ThreadState.AbortRequested OrElse mThread.ThreadState = Threading.ThreadState.WaitSleepJoin OrElse mThread.ThreadState = Threading.ThreadState.Suspended OrElse mThread.ThreadState = Threading.ThreadState.SuspendRequested
                    Try
                        If mThread.Join(THREAD_WAIT_MSEC) Then
                            ' The Join succeeded, meaning the thread has finished running
                            Exit Do
                            'ElseIf mRequestCancel = True Then
                            '    mThread.Abort()
                        End If

                    Catch ex As Exception
                        ' Error joining thread; this can happen if the thread is trying to abort, so I believe we can ignore the error
                        ' Sleep another THREAD_WAIT_MSEC msec, then exit the Do Loop
                        Threading.Thread.Sleep(THREAD_WAIT_MSEC)
                        Exit Do
                    End Try
                    Application.DoEvents()
                Loop
            End If

            Application.DoEvents()
            If Not mSchemaExportSuccess OrElse mDBSchemaExporter.ErrorCode <> 0 Then
                message = "Error exporting the schema objects (ErrorCode=" & mDBSchemaExporter.ErrorCode.ToString & "): " & ControlChars.NewLine & mDBSchemaExporter.StatusMessage
                MessageBox.Show(message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
            End If
        Catch ex As Exception
            MessageBox.Show("Error calling ScriptDBSchemaObjectsThread in ScriptDBSchemaObjects: " & ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
        Finally
            mWorking = False
            EnableDisableControls()
            UpdatePauseUnpauseCaption(clsExportDBSchema.ePauseStatusConstants.Unpaused)
            Try
                If Not mThread Is Nothing AndAlso mThread.ThreadState <> Threading.ThreadState.Stopped Then
                    mThread.Abort()
                End If
            Catch ex As Exception
                ' Ignore errors here
            End Try
        End Try

        Try
            lblSubtaskProgress.Text = "Schema export complete"
        Catch ex As Exception
            MessageBox.Show("Error finalizing results in ScriptDBSchemaObjects: " & ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
        End Try

    End Sub

    Private Sub ScriptDBSchemaObjectsThread()
        ' Note: Populate mDatabaseListToProcess and mTableNamesForDataExport prior to calling this sub

        Try
            mSchemaExportSuccess = mDBSchemaExporter.ScriptServerAndDBObjects(mSchemaExportOptions, mDatabaseListToProcess, mTableNamesForDataExport)
        Catch ex As Exception
            MessageBox.Show("Error in ScriptDBSchemaObjectsThread: " & ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
        End Try
    End Sub

    Private Sub SelectDefaultDBs(lstDefaultDBs As List(Of String))

        Dim index As Integer
        Dim strCurrentDB As String

        If Not lstDefaultDBs Is Nothing Then

            Dim lstSortedDBs = (From item In lstDefaultDBs Select item.ToLower()).ToList()

            lstSortedDBs.Sort()

            lstDatabasesToProcess.ClearSelected()

            For index = 0 To lstDatabasesToProcess.Items.Count - 1
                strCurrentDB = lstDatabasesToProcess.Items(index).ToString.ToLower()

                If lstSortedDBs.BinarySearch(strCurrentDB) >= 0 Then
                    lstDatabasesToProcess.SetSelected(index, True)
                End If

            Next index
        End If

    End Sub

    Private Sub ResetToDefaults(confirm As Boolean)
        Dim eResponse As DialogResult

        Try
            If confirm Then
                eResponse = MessageBox.Show("Are you sure you want to reset all settings to their default values?", "Reset to Defaults", MessageBoxButtons.YesNoCancel, MessageBoxIcon.Question, MessageBoxDefaultButton.Button1)
                If eResponse <> DialogResult.Yes Then
                    Exit Sub
                End If
            End If

            Me.Width = 600
            Me.Height = 600

            txtServerName.Text = clsExportDBSchema.SQL_SERVER_NAME_DEFAULT

            chkUseIntegratedAuthentication.Checked = True
            txtUsername.Text = clsExportDBSchema.SQL_SERVER_USERNAME_DEFAULT
            txtPassword.Text = clsExportDBSchema.SQL_SERVER_PASSWORD_DEFAULT

            mnuEditScriptObjectsThreaded.Checked = False
            mnuEditIncludeSystemObjects.Checked = False
            mnuEditPauseAfterEachDatabase.Checked = False
            mnuEditIncludeTimestampInScriptFileHeader.Checked = False

            mnuEditIncludeTableRowCounts.Checked = True
            mnuEditAutoSelectDefaultTableNames.Checked = True
            mnuEditSaveDataAsInsertIntoStatements.Checked = True
            mnuEditWarnOnHighTableRowCount.Checked = True

            chkCreateFolderForEachDB.Checked = True
            txtOutputFolderNamePrefix.Text = clsExportDBSchema.DEFAULT_DB_OUTPUT_FOLDER_NAME_PREFIX

            chkExportServerSettingsLoginsAndJobs.Checked = False
            txtServerOutputFolderNamePrefix.Text = clsExportDBSchema.DEFAULT_SERVER_OUTPUT_FOLDER_NAME_PREFIX

            mTableNamesToAutoSelect = clsDBSchemaExportTool.GetTableNamesToAutoExportData
            mTableNameAutoSelectRegEx = clsDBSchemaExportTool.GetTableRegExToAutoExportData()

            mDefaultDMSDatabaseList.Clear()
            mDefaultDMSDatabaseList.Add("DMS5")
            mDefaultDMSDatabaseList.Add("DMS_Capture")
            mDefaultDMSDatabaseList.Add("DMS_Data_Package")
            mDefaultDMSDatabaseList.Add("DMS_Pipeline")
            mDefaultDMSDatabaseList.Add("Ontology_Lookup")
            mDefaultDMSDatabaseList.Add("Protein_Sequences")
            mDefaultDMSDatabaseList.Add("DMSHistoricLog1")

            mDefaultMTSDatabaseList.Clear()
            mDefaultMTSDatabaseList.Add("MT_Template_01")
            mDefaultMTSDatabaseList.Add("PT_Template_01")
            mDefaultMTSDatabaseList.Add("MT_Main")
            mDefaultMTSDatabaseList.Add("MTS_Master")
            mDefaultMTSDatabaseList.Add("Prism_IFC")
            mDefaultMTSDatabaseList.Add("Prism_RPT")
            mDefaultMTSDatabaseList.Add("PAST_BB")
            mDefaultMTSDatabaseList.Add("Master_Sequences")
            mDefaultMTSDatabaseList.Add("MT_Historic_Log")
            mDefaultMTSDatabaseList.Add("MT_HistoricLog")

            EnableDisableControls()
        Catch ex As Exception
            MessageBox.Show("Error in ResetToDefaults: " & ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
        End Try

    End Sub

    Private Sub SelectAllListboxItems(objListBox As ListBox)
        Dim index As Integer

        Try
            For index = 0 To objListBox.Items.Count - 1
                objListBox.SetSelected(index, True)
            Next
        Catch ex As Exception
            ' Ignore errors here
        End Try
    End Sub

    Private Sub SelectOutputFolder()
        ' Prompts the user to select the output folder to create the scripted objects in

        Try
            Dim objFolderBrowser = New FolderBrowser()
            Dim success As Boolean

            If txtOutputFolderPath.TextLength > 0 Then
                success = objFolderBrowser.BrowseForFolder(txtOutputFolderPath.Text)
            Else
                success = objFolderBrowser.BrowseForFolder(GetAppFolderPath())
            End If

            If success Then
                txtOutputFolderPath.Text = objFolderBrowser.FolderPath
            End If
        Catch ex As Exception
            MessageBox.Show("Error in SelectOutputFolder: " & ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
        End Try
    End Sub

    Private Sub SetToolTips()


        Try

            Using objToolTipControl = New ToolTip()
                objToolTipControl.SetToolTip(chkCreateFolderForEachDB, "This will be automatically enabled if multiple databases are chosen above")
                objToolTipControl.SetToolTip(txtOutputFolderNamePrefix, "The output folder for each database will be named with this prefix followed by the database name")
                objToolTipControl.SetToolTip(txtServerOutputFolderNamePrefix, "Server settings will be saved in a folder with this prefix followed by the server name")
            End Using

        Catch ex As Exception
            MessageBox.Show("Error in SetToolTips: " & ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
        End Try

    End Sub

    Private Sub ShowAboutBox()
        Dim message As String

        message = String.Empty

        message &= "Program written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA) in August 2006" & ControlChars.NewLine
        message &= "Copyright 2006, Battelle Memorial Institute.  All Rights Reserved." & ControlChars.NewLine & ControlChars.NewLine

        message &= "This is version " & Application.ProductVersion & " (" & PROGRAM_DATE & "). " & ControlChars.NewLine & ControlChars.NewLine

        message &= "E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov" & ControlChars.NewLine
        message &= "Website: https://omics.pnl.gov/ or https://panomics.pnnl.gov/" & ControlChars.NewLine & ControlChars.NewLine

        message &= "Licensed under the 2-Clause BSD License; you may not use this file except "
        message &= "in compliance with the License.  You may obtain a copy of the License at https://opensource.org/licenses/BSD-2-Clause"
        message &= ControlChars.NewLine & ControlChars.NewLine

        MessageBox.Show(message, "About", MessageBoxButtons.OK, MessageBoxIcon.Information)
    End Sub

    Private Sub StripRowCountsFromTableNames(ByRef lstTableNames As List(Of String))
        Dim index As Integer

        If lstTableNames Is Nothing Then Exit Sub
        For index = 0 To lstTableNames.Count - 1
            lstTableNames(index) = StripRowCountFromTableName(lstTableNames(index))
        Next index

    End Sub

    Private Function StripRowCountFromTableName(strTableName As String) As String
        Dim intCharIndex As Integer

        If strTableName Is Nothing Then
            Return String.Empty
        Else
            intCharIndex = strTableName.IndexOf(ROW_COUNT_SEPARATOR, StringComparison.Ordinal)
            If intCharIndex > 0 Then
                Return strTableName.Substring(0, intCharIndex)
            Else
                Return strTableName
            End If
        End If

    End Function

    Private Sub SubTaskProgressUpdate(taskDescription As String, percentComplete As Single)
        lblSubtaskProgress.Text = taskDescription
        UpdateProgressBar(pbarSubtaskProgress, percentComplete)

        If mDBSchemaExporter.ProgressStepCount > 0 Then
            Dim percentCompleteOverall As Single
            percentCompleteOverall = mDBSchemaExporter.ProgressStep / CSng(mDBSchemaExporter.ProgressStepCount) * 100.0! + 1.0! / mDBSchemaExporter.ProgressStepCount * percentComplete
            UpdateProgressBar(pbarProgress, percentCompleteOverall)
        End If
    End Sub

    Private Sub SubTaskProgressComplete()
        pbarSubtaskProgress.Value = pbarProgress.Maximum
    End Sub

    Private Sub UpdateDatabaseList()

        If mWorking Then Exit Sub

        Try
            mWorking = True
            EnableDisableControls()

            UpdatePauseUnpauseCaption(clsExportDBSchema.ePauseStatusConstants.Unpaused)
            Application.DoEvents()

            If VerifyOrUpdateServerConnection(True) Then
                ' Cache the currently selected names so that we can re-highlight them below

                Dim lstSelectedDatabaseNamesSaved = New SortedSet(Of String)
                For Each objItem In lstDatabasesToProcess.SelectedItems
                    lstSelectedDatabaseNamesSaved.Add(CStr(objItem))
                Next

                lstDatabasesToProcess.Items.Clear()

                Dim lstDatabaseList = mDBSchemaExporter.GetSqlServerDatabases()

                If lstDatabaseList.Count > 0 Then
                    For Each databaseName In lstDatabaseList

                        If String.IsNullOrWhiteSpace(databaseName) Then Continue For

                        Dim intItemIndex = lstDatabasesToProcess.Items.Add(databaseName)

                        If lstSelectedDatabaseNamesSaved.Contains(databaseName) Then
                            ' Highlight this table name
                            lstDatabasesToProcess.SetSelected(intItemIndex, True)
                        End If
                    Next
                End If
            End If
        Catch ex As Exception
            MessageBox.Show("Error in UpdateDatabaseList: " & ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
        Finally
            mWorking = False
            EnableDisableControls()
        End Try
    End Sub

    Private Sub UpdatePauseUnpauseCaption(ePauseStatus As clsExportDBSchema.ePauseStatusConstants)
        Try
            Select Case ePauseStatus
                Case clsExportDBSchema.ePauseStatusConstants.Paused
                    cmdPauseUnpause.Text = "Un&pause"
                Case clsExportDBSchema.ePauseStatusConstants.PauseRequested
                    cmdPauseUnpause.Text = "&Pausing"
                Case clsExportDBSchema.ePauseStatusConstants.Unpaused
                    cmdPauseUnpause.Text = "&Pause"
                Case clsExportDBSchema.ePauseStatusConstants.UnpauseRequested
                    cmdPauseUnpause.Text = "Un&pausing"
                Case Else
                    cmdPauseUnpause.Text = "Un&pause"
            End Select
        Catch ex As Exception
            ' Ignore errors here
        End Try
    End Sub

    Private Sub UpdateProgressBar(pbar As ProgressBar, sngPercentComplete As Single)
        Dim intPercentComplete = CInt(sngPercentComplete)

        If intPercentComplete < pbar.Minimum Then intPercentComplete = pbar.Minimum
        If intPercentComplete > pbar.Maximum Then intPercentComplete = pbar.Maximum
        pbar.Value = intPercentComplete

    End Sub

    Private Sub UpdateTableNamesInSelectedDB()

        Dim databaseName As String = String.Empty

        If mWorking Then Exit Sub

        Try
            If lstDatabasesToProcess.Items.Count = 0 Then
                MessageBox.Show("The database list is currently empty; unable to continue", "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
                Exit Try
            ElseIf lstDatabasesToProcess.SelectedIndex < 0 Then
                ' Auto-select the first database
                lstDatabasesToProcess.SelectedIndex = 0
            End If
            databaseName = CStr(lstDatabasesToProcess.Items(lstDatabasesToProcess.SelectedIndex))

        Catch ex As Exception
            MessageBox.Show("Error determining selected database name in UpdateTableNamesInSelectedDB: " & ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
        End Try

        If databaseName Is Nothing OrElse databaseName.Length = 0 Then
            Exit Sub
        End If

        Try
            mWorking = True
            EnableDisableControls()

            UpdatePauseUnpauseCaption(clsExportDBSchema.ePauseStatusConstants.Unpaused)
            Application.DoEvents()

            If VerifyOrUpdateServerConnection(True) Then

                Dim includeTableRowCounts = mnuEditIncludeTableRowCounts.Checked

                mCachedTableList = mDBSchemaExporter.GetSqlServerDatabaseTableNames(databaseName, includeTableRowCounts, mnuEditIncludeSystemObjects.Checked)

                If mCachedTableList.Count > 0 Then
                    mCachedTableListIncludesRowCounts = includeTableRowCounts

                    PopulateTableNamesToExport(True)
                End If
            End If
        Catch ex As Exception
            MessageBox.Show("Error in UpdateTableNamesInSelectedDB: " & ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
        Finally
            mWorking = False
            EnableDisableControls()
        End Try

    End Sub

    Private Function ValueToTextEstimate(lngValue As Long) As String
        ' Converts lngValue to a text-based value
        ' For data between 10000 and 1 million, rounds to the nearest thousand
        ' For data over 1 million, displays as x.x million

        Dim strValue As String
        Dim lngValueAbs As Long
        Dim dblDivisor As Double

        lngValueAbs = Math.Abs(lngValue)
        If lngValueAbs < 100 Then
            strValue = lngValue.ToString
        ElseIf lngValue >= 1000000 Then
            strValue = Math.Round(lngValue / 1000000.0, 1) & " million"
        Else
            dblDivisor = 10 ^ (Math.Floor(Math.Log10(lngValueAbs)) - 1)
            strValue = (Math.Round(lngValue / dblDivisor, 0) * dblDivisor).ToString("#,###,##0")
        End If

        Return strValue
    End Function

    Private Function VerifyOrUpdateServerConnection(informUserOnFailure As Boolean) As Boolean
        Dim connected As Boolean

        Try
            Dim message As String
            connected = False

            If txtServerName.TextLength = 0 Then
                message = "Please enter the server name"
                If informUserOnFailure Then
                    MessageBox.Show(message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
                End If
            Else
                Dim connectionInfo = New clsServerConnectionInfo(txtServerName.Text, chkUseIntegratedAuthentication.Checked)

                If Not chkUseIntegratedAuthentication.Checked Then
                    connectionInfo.UserName = txtUsername.Text
                    connectionInfo.Password = txtPassword.Text
                End If

                If mDBSchemaExporter Is Nothing Then
                    mDBSchemaExporter = New clsExportDBSchema(connectionInfo)
                    connected = mDBSchemaExporter.ConnectedToServer
                Else
                    connected = mDBSchemaExporter.ConnectToServer(connectionInfo)
                End If

                If Not connected AndAlso informUserOnFailure Then
                    message = "Error connecting to server " & connectionInfo.ServerName
                    If mDBSchemaExporter.StatusMessage.Length > 0 Then
                        message &= "; " & mDBSchemaExporter.StatusMessage
                    End If
                    MessageBox.Show(message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
                End If
            End If

        Catch ex As Exception
            MessageBox.Show("Error in VerifyOrUpdateServerConnection: " & ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
            connected = False
        End Try

        Return connected
    End Function

    Private Sub frmMain_Load(eventSender As Object, eventArgs As EventArgs) Handles MyBase.Load
        ' Note that InitializeControls() is called in Sub New()

    End Sub

#Region "Control Handlers"

    Private Sub cboTableNamesToExportSortOrder_SelectedIndexChanged(sender As Object, e As EventArgs) Handles cboTableNamesToExportSortOrder.SelectedIndexChanged
        PopulateTableNamesToExport(False)
    End Sub

    Private Sub cmdPauseUnpause_Click(sender As Object, e As EventArgs) Handles cmdPauseUnpause.Click
        If Not mDBSchemaExporter Is Nothing Then
            mDBSchemaExporter.TogglePause()
            If mDBSchemaExporter.PauseStatus = clsExportDBSchema.ePauseStatusConstants.UnpauseRequested OrElse
               mDBSchemaExporter.PauseStatus = clsExportDBSchema.ePauseStatusConstants.Unpaused Then
            End If
        End If
    End Sub

    Private Sub cmdRefreshDBList_Click(sender As Object, e As EventArgs) Handles cmdRefreshDBList.Click
        UpdateDatabaseList()
    End Sub

    Private Sub cmdUpdateTableNames_Click(sender As Object, e As EventArgs) Handles cmdUpdateTableNames.Click
        UpdateTableNamesInSelectedDB()
    End Sub

    Private Sub chkUseIntegratedAuthentication_CheckedChanged(sender As Object, e As EventArgs) Handles chkUseIntegratedAuthentication.CheckedChanged
        EnableDisableControls()
    End Sub

    Private Sub cmdAbort_Click(sender As Object, e As EventArgs) Handles cmdAbort.Click
        ConfirmAbortRequest()
    End Sub

    Private Sub cmdGo_Click(eventSender As Object, eventArgs As EventArgs) Handles cmdGo.Click
        ScriptDBSchemaObjects()
    End Sub

    Private Sub cmdSelectDefaultDMSDBs_Click(sender As Object, e As EventArgs) Handles cmdSelectDefaultDMSDBs.Click
        SelectDefaultDBs(mDefaultDMSDatabaseList)
    End Sub

    Private Sub cmdSelectDefaultMTSDBs_Click(sender As Object, e As EventArgs) Handles cmdSelectDefaultMTSDBs.Click
        SelectDefaultDBs(mDefaultMTSDatabaseList)
    End Sub

    Private Sub cmdExit_Click(eventSender As Object, eventArgs As EventArgs) Handles cmdExit.Click
        Me.Close()
    End Sub

    Private Sub lstDatabasesToProcess_KeyPress(sender As Object, e As KeyPressEventArgs) Handles lstDatabasesToProcess.KeyPress
        ' Always mark e as handled to allow Ctrl+A to be used to select all the entries in the listbox
        e.Handled = True
    End Sub

    Private Sub lstDatabasesToProcess_KeyDown(sender As Object, e As KeyEventArgs) Handles lstDatabasesToProcess.KeyDown
        If e.Control Then
            If e.KeyCode = Keys.A Then
                ' Ctrl+A - Select All
                SelectAllListboxItems(lstDatabasesToProcess)
                e.Handled = True
            End If
        End If
    End Sub

    Private Sub lstObjectTypesToScript_KeyPress(sender As Object, e As KeyPressEventArgs) Handles lstObjectTypesToScript.KeyPress
        ' Always mark e as handled to allow Ctrl+A to be used to select all the entries in the listbox
        e.Handled = True
    End Sub

    Private Sub lstObjectTypesToScript_KeyDown(sender As Object, e As KeyEventArgs) Handles lstObjectTypesToScript.KeyDown
        If e.Control Then
            If e.KeyCode = Keys.A Then
                ' Ctrl+A - Select All
                SelectAllListboxItems(lstObjectTypesToScript)
                e.Handled = True
            End If
        End If
    End Sub

    Private Sub lstTableNamesToExportData_KeyPress(sender As Object, e As KeyPressEventArgs) Handles lstTableNamesToExportData.KeyPress
        ' Always mark e as handled to allow Ctrl+A to be used to select all the entries in the listbox
        e.Handled = True
    End Sub

    Private Sub lstTableNamesToExportData_KeyDown(sender As Object, e As KeyEventArgs) Handles lstTableNamesToExportData.KeyDown
        If e.Control Then
            If e.KeyCode = Keys.A Then
                ' Ctrl+A - Select All
                SelectAllListboxItems(lstTableNamesToExportData)
                e.Handled = True
            End If
        End If
    End Sub

#End Region

#Region "Form Handlers"
    Private Sub frmMain_Closing(sender As Object, e As ComponentModel.CancelEventArgs) Handles MyBase.Closing
        If Not mDBSchemaExporter Is Nothing Then
            mDBSchemaExporter.AbortProcessingNow()
        End If
    End Sub
#End Region

#Region "Menu Handlers"
    Private Sub mnuFileSelectOutputFolder_Click(sender As Object, e As EventArgs) Handles mnuFileSelectOutputFolder.Click
        SelectOutputFolder()
    End Sub

    Private Sub mnuFileSaveOptions_Click(sender As Object, e As EventArgs) Handles mnuFileSaveOptions.Click
        IniFileSaveOptions()
    End Sub

    Private Sub mnuFileLoadOptions_Click(sender As Object, e As EventArgs) Handles mnuFileLoadOptions.Click
        IniFileLoadOptions()
    End Sub

    Private Sub mnuFileExit_Click(sender As Object, e As EventArgs) Handles mnuFileExit.Click
        Me.Close()
    End Sub

    Private Sub mnuEditStart_Click(sender As Object, e As EventArgs) Handles mnuEditStart.Click
        ScriptDBSchemaObjects()
    End Sub

    Private Sub mnuEditIncludeSystemObjects_Click(sender As Object, e As EventArgs) Handles mnuEditIncludeSystemObjects.Click
        mnuEditIncludeSystemObjects.Checked = Not mnuEditIncludeSystemObjects.Checked
    End Sub

    Private Sub mnuEditScriptObjectsThreaded_Click(sender As Object, e As EventArgs) Handles mnuEditScriptObjectsThreaded.Click
        mnuEditScriptObjectsThreaded.Checked = Not mnuEditScriptObjectsThreaded.Checked
    End Sub

    Private Sub mnuEditPauseAfterEachDatabase_Click_1(sender As Object, e As EventArgs) Handles mnuEditPauseAfterEachDatabase.Click
        mnuEditPauseAfterEachDatabase.Checked = Not mnuEditPauseAfterEachDatabase.Checked
    End Sub

    Private Sub mnuEditIncludeTimestampInScriptFileHeader_Click(sender As Object, e As EventArgs) Handles mnuEditIncludeTimestampInScriptFileHeader.Click
        mnuEditIncludeTimestampInScriptFileHeader.Checked = Not mnuEditIncludeTimestampInScriptFileHeader.Checked
    End Sub

    Private Sub mnuEditIncludeTableRowCounts_Click(sender As Object, e As EventArgs) Handles mnuEditIncludeTableRowCounts.Click
        mnuEditIncludeTableRowCounts.Checked = Not mnuEditIncludeTableRowCounts.Checked
    End Sub

    Private Sub mnuEditAutoSelectDefaultTableNames_Click(sender As Object, e As EventArgs) Handles mnuEditAutoSelectDefaultTableNames.Click
        mnuEditAutoSelectDefaultTableNames.Checked = Not mnuEditAutoSelectDefaultTableNames.Checked
    End Sub

    Private Sub mnuEditSaveDataAsInsertIntoStatements_Click(sender As Object, e As EventArgs) Handles mnuEditSaveDataAsInsertIntoStatements.Click
        mnuEditSaveDataAsInsertIntoStatements.Checked = Not mnuEditSaveDataAsInsertIntoStatements.Checked
    End Sub

    Private Sub mnuEditWarnOnHighTableRowCount_Click(sender As Object, e As EventArgs) Handles mnuEditWarnOnHighTableRowCount.Click
        mnuEditWarnOnHighTableRowCount.Checked = Not mnuEditWarnOnHighTableRowCount.Checked
    End Sub

    Private Sub mnuEditResetOptions_Click(sender As Object, e As EventArgs) Handles mnuEditResetOptions.Click
        ResetToDefaults(True)
    End Sub

    Private Sub mnuHelpAbout_Click(sender As Object, e As EventArgs) Handles mnuHelpAbout.Click
        ShowAboutBox()
    End Sub
#End Region

#Region "DB Schema Export Automation Events"

    Private Sub mDBSchemaExporter_ErrorMessage(message As String, ex As Exception) Handles mDBSchemaExporter.ErrorEvent
        If Me.InvokeRequired Then
            Me.Invoke(New AppendNewMessageHandler(AddressOf AppendNewMessage), New Object() {message, True})
        Else
            AppendNewMessage(message, True)
        End If
        Application.DoEvents()
    End Sub

    Private Sub mDBSchemaExporter_StatusMessage(message As String) Handles mDBSchemaExporter.StatusEvent
        If Me.InvokeRequired Then
            Me.Invoke(New AppendNewMessageHandler(AddressOf AppendNewMessage), New Object() {message, False})
        Else
            AppendNewMessage(message, False)
        End If
        Application.DoEvents()
    End Sub

    Private Sub mDBSchemaExporter_PauseStatusChange() Handles mDBSchemaExporter.PauseStatusChange
        If Me.InvokeRequired Then
            Me.Invoke(New UpdatePauseUnpauseCaptionHandler(AddressOf UpdatePauseUnpauseCaption), New Object() {mDBSchemaExporter.PauseStatus})
        Else
            UpdatePauseUnpauseCaption(mDBSchemaExporter.PauseStatus)
        End If
        Application.DoEvents()
    End Sub

    Private Sub mDBSchemaExporter_ProgressChanged(taskDescription As String, percentComplete As Single) Handles mDBSchemaExporter.ProgressUpdate
        If Me.InvokeRequired Then
            Me.Invoke(New ProgressUpdateHandler(AddressOf ProgressUpdate), New Object() {taskDescription, percentComplete})
        Else
            ProgressUpdate(taskDescription, percentComplete)
        End If
        Application.DoEvents()
    End Sub

    Private Sub mDBSchemaExporter_ProgressComplete() Handles mDBSchemaExporter.ProgressComplete
        If Me.InvokeRequired Then
            Me.Invoke(New ProgressCompleteHandler(AddressOf ProgressComplete), New Object() {})
        Else
            ProgressComplete()
        End If
        Application.DoEvents()
    End Sub

    Private Sub mDBSchemaExporter_ProgressReset() Handles mDBSchemaExporter.ProgressReset
        If Me.InvokeRequired Then
            Me.Invoke(New ProgressUpdateHandler(AddressOf ProgressUpdate), New Object() {mDBSchemaExporter.ProgressStepDescription, 0})
        Else
            ProgressUpdate(mDBSchemaExporter.ProgressStepDescription, 0)
        End If
    End Sub

    Private Sub mDBSchemaExporter_DBExportStarting(databaseName As String) Handles mDBSchemaExporter.DBExportStarting
        If Me.InvokeRequired Then
            Me.Invoke(New HandleDBExportStartingEventHandler(AddressOf HandleDBExportStartingEvent), New Object() {databaseName})
        Else
            HandleDBExportStartingEvent(databaseName)
        End If
        Application.DoEvents()
    End Sub

    Private Sub mDBSchemaExporter_SubtaskProgressChanged(taskDescription As String, percentComplete As Single) Handles mDBSchemaExporter.SubtaskProgressChanged
        If Me.InvokeRequired Then
            Me.Invoke(New SubTaskProgressUpdateHandler(AddressOf SubTaskProgressUpdate), New Object() {taskDescription, percentComplete})
        Else
            SubTaskProgressUpdate(taskDescription, percentComplete)
        End If
        Application.DoEvents()
    End Sub

    Private Sub mDBSchemaExporter_SubtaskProgressComplete() Handles mDBSchemaExporter.SubtaskProgressComplete
        If Me.InvokeRequired Then
            Me.Invoke(New SubTaskProgressCompleteHandler(AddressOf SubTaskProgressComplete), New Object() {})
        Else
            SubTaskProgressComplete()
        End If
        Application.DoEvents()
    End Sub

    Private Sub mDBSchemaExporter_SubtaskProgressReset() Handles mDBSchemaExporter.SubtaskProgressReset
        If Me.InvokeRequired Then
            Me.Invoke(New SubTaskProgressUpdateHandler(AddressOf SubTaskProgressUpdate), New Object() {mDBSchemaExporter.SubtaskProgressStepDescription, 0})
        Else
            SubTaskProgressUpdate(mDBSchemaExporter.SubtaskProgressStepDescription, 0)
        End If
        Application.DoEvents()
    End Sub
#End Region

End Class