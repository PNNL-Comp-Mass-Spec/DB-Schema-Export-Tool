Option Strict On

' This program uses class clsExportDBSchema to export the objects from the
' database(s) on the selected server
'
' -------------------------------------------------------------------------------
' Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA)
' Program started August 11, 2006
' Copyright 2006, Battelle Memorial Institute.  All Rights Reserved.

' E-mail: matthew.monroe@pnl.gov or matt@alchemistmatt.com
' Website: http://ncrr.pnl.gov/ or http://www.sysbio.org/resources/staff/
' -------------------------------------------------------------------------------

Public Class frmMain

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

    Protected Enum eTableNameSortModeConstants
        Name = 0
        RowCount = 1
    End Enum
#End Region

#Region "Structures"
#End Region

#Region "Classwide Variables"
    Private mXmlSettingsFilePath As String

    Private mSchemaExportOptions As clsExportDBSchema.udtSchemaExportOptionsType
    Private mDatabaseListToProcess As String()
    Private mTableNamesForDataExport As String()

    ' Note: mCachedTableList is sorted alphabetically to allow for binary searching
    Protected mCachedTableListCount As Integer
    Protected mCachedTableList() As String

    ' Note: mCachedTableListRowCounts is sorted parallel with mCachedTableList
    Protected mCachedTableListRowCounts() As Long
    Protected mCachedTableListIncludesRowCounts As Boolean

    Protected mTableNamesToAutoSelect() As String

    ' Note: Must contain valid RegEx statements (tested case-insensitive)
    Protected mTableNameAutoSelectRegEx() As String

    Protected mDefaultDMSDatabaseList() As String
    Protected mDefaultMTSDatabaseList() As String

    Private mWorking As Boolean

    Private mThread As Threading.Thread

    Private WithEvents mDBSchemaExporter As clsExportDBSchema
    Private mSchemaExportSuccess As Boolean

#End Region

#Region "Delegates"
    Private Delegate Sub AppendNewMessageHandler(ByVal strMessage As String, ByVal eMessageType As clsExportDBSchema.eMessageTypeConstants)
    Private Delegate Sub UpdatePauseUnpauseCaptionHandler(ByVal ePauseStatus As clsExportDBSchema.ePauseStatusConstants)

    Private Delegate Sub ProgressUpdateHandler(ByVal taskDescription As String, ByVal percentComplete As Single)
    Private Delegate Sub ProgressCompleteHandler()

    Private Delegate Sub SubTaskProgressUpdateHandler(ByVal taskDescription As String, ByVal percentComplete As Single)
    Private Delegate Sub SubTaskProgressCompleteHandler()

    Private Delegate Sub HandleDBExportStartingEventHandler(ByVal strDatabaseName As String)

#End Region

    Private Sub AppendNewMessage(ByVal strMessage As String, ByVal eMessageType As clsExportDBSchema.eMessageTypeConstants)
        lblMessage.Text = strMessage
        Application.DoEvents()
    End Sub

    Private Sub ConfirmAbortRequest()
        Dim ePauseStatusSaved As clsExportDBSchema.ePauseStatusConstants
        Dim eResponse As System.Windows.Forms.DialogResult

        If Not mDBSchemaExporter Is Nothing Then
            ePauseStatusSaved = mDBSchemaExporter.PauseStatus

            mDBSchemaExporter.RequestPause()
            Application.DoEvents()

            eResponse = MessageBox.Show("Are you sure you want to abort processing?", "Abort", MessageBoxButtons.YesNoCancel, MessageBoxIcon.Question, MessageBoxDefaultButton.Button1)
            If eResponse = System.Windows.Forms.DialogResult.Yes Then
                mDBSchemaExporter.AbortProcessingNow()

                ' Note that AbortProcessingNow should have called RequestUnpause, but we'll call it here just in case
                mDBSchemaExporter.RequestUnpause()
            Else
                If ePauseStatusSaved = clsExportDBSchema.ePauseStatusConstants.Unpaused OrElse _
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
            System.Windows.Forms.MessageBox.Show("Error in EnableDisableControls: " & ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
        End Try
    End Sub

    Private Function GetAppFolderPath() As String
        Return GetAppFolderPath(True)
    End Function

    Private Function GetAppFolderPath(ByVal blnReturnParentIfFolderNamedDebug As Boolean) As String
        Const DEBUG_FOLDER_NAME As String = "\debug"

        ' Could use Application.StartupPath, but .GetExecutingAssembly is better
        Dim strPath As String

        strPath = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location)
        If blnReturnParentIfFolderNamedDebug Then
            If strPath.ToLower.EndsWith(DEBUG_FOLDER_NAME) Then
                strPath = strPath.Substring(0, strPath.Length - DEBUG_FOLDER_NAME.Length)
            End If
        End If

        Return strPath
    End Function

    Private Function GetSelectedDatabases() As String()
        Return GetSelectedListboxItems(lstDatabasesToProcess)
    End Function

    Private Function GetSelectedTableNamesForDataExport(ByVal blnWarnIfRowCountOverThreshold As Boolean) As String()
        Dim strTableNames() As String

        Dim intValidTableNameCount As Integer
        Dim strValidTableNames() As String

        Dim intIndex As Integer
        Dim intIndexMatch As Integer
        Dim eResponse As System.Windows.Forms.DialogResult

        Dim blnKeepTable As Boolean

        strTableNames = GetSelectedListboxItems(lstTableNamesToExportData)

        StripRowCountsFromTableNames(strTableNames)

        If strTableNames.Length > 0 AndAlso mCachedTableListIncludesRowCounts AndAlso blnWarnIfRowCountOverThreshold Then
            intValidTableNameCount = 0
            ReDim strValidTableNames(strTableNames.Length - 1)

            ' See if any of the tables in strTableNames() has more than clsExportDBSchema.DATA_ROW_COUNT_WARNING_THRESHOLD rows
            For intIndex = 0 To strTableNames.Length - 1
                blnKeepTable = True

                intIndexMatch = Array.BinarySearch(mCachedTableList, strTableNames(intIndex))
                If intIndexMatch >= 0 Then
                    If mCachedTableListRowCounts(intIndexMatch) >= clsExportDBSchema.DATA_ROW_COUNT_WARNING_THRESHOLD Then
                        eResponse = System.Windows.Forms.MessageBox.Show("Warning, table " & strTableNames(intIndex) & " has " & _
                                       mCachedTableListRowCounts(intIndexMatch).ToString & " rows.  Are you sure you want to export data from it?", _
                                       "Row Count Over " & clsExportDBSchema.DATA_ROW_COUNT_WARNING_THRESHOLD.ToString, _
                                       MessageBoxButtons.YesNoCancel, MessageBoxIcon.Question, MessageBoxDefaultButton.Button2)

                        If eResponse = Windows.Forms.DialogResult.No Then
                            blnKeepTable = False
                        ElseIf eResponse = Windows.Forms.DialogResult.Cancel Then
                            Exit For
                        End If
                    End If
                Else
                    ' Table not found; keep it anyway
                End If

                If blnKeepTable Then
                    strValidTableNames(intValidTableNameCount) = strTableNames(intIndex)
                    intValidTableNameCount += 1
                End If
            Next intIndex

            If intValidTableNameCount < strValidTableNames.Length Then
                ReDim strTableNames(intValidTableNameCount - 1)
                For intIndex = 0 To intValidTableNameCount - 1
                    strTableNames(intIndex) = String.Copy(strValidTableNames(intIndex))
                Next intIndex
            End If
        End If

        Return strTableNames
    End Function

    Private Function GetSelectedListboxItems(ByRef objListbox As System.Windows.Forms.ListBox) As String()
        Dim objItem As Object
        Dim strItemList() As String
        Dim intIndex As Integer

        Try
            ReDim strItemList(objListbox.SelectedItems.Count - 1)

            If strItemList.Length > 0 Then
                intIndex = 0
                For Each objItem In objListbox.SelectedItems
                    strItemList(intIndex) = CStr(objItem)
                    intIndex += 1
                Next objItem
            End If

        Catch ex As Exception
            ReDim strItemList(-1)
        End Try

        Return strItemList
    End Function

    Private Function GetSettingsFilePath() As String
        Return System.IO.Path.Combine(GetAppFolderPath(), XML_SETTINGS_FILE_NAME)
    End Function

    Private Sub HandleDBExportStartingEvent(ByVal strDatabaseName As String)
        Try
            lblProgress.Text = "Exporting schema from " & strDatabaseName

            If mnuEditPauseAfterEachDatabase.Checked Then
                mDBSchemaExporter.RequestPause()
            End If
        Catch ex As Exception
            System.Windows.Forms.MessageBox.Show("Error in HandleDBExportStartingEvent: " & ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
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

        Dim objOpenFile As New System.Windows.Forms.OpenFileDialog

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
                    .InitialDirectory = System.IO.Directory.GetParent(strFilePath).ToString
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

    Private Sub IniFileLoadOptions(ByVal strFilePath As String, ByVal blnResetToDefaultsPriorToLoad As Boolean, ByVal blnConnectToServer As Boolean)
        ' Loads options from the given file

        Dim objXmlFile As New PRISM.Files.XmlSettingsFileAccessor

        Dim strServerNameSaved As String

        Try
            If blnResetToDefaultsPriorToLoad Then
                ResetToDefaults(False)
            End If

            ' Sleep for 100 msec, just to be safe
            System.Threading.Thread.Sleep(100)

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

                    If lstDatabasesToProcess.Items.Count = 0 OrElse _
                       Not strServerNameSaved Is Nothing AndAlso strServerNameSaved.ToLower <> txtServerName.Text.ToLower Then
                        If blnConnectToServer Then
                            UpdateDatabaseList()
                        End If
                    End If

                Catch ex As Exception
                    System.Windows.Forms.MessageBox.Show("Invalid parameter in settings file: " & System.IO.Path.GetFileName(strFilePath), "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
                End Try
            End With

            objXmlFile = Nothing

        Catch ex As Exception
            System.Windows.Forms.MessageBox.Show("Error loading settings from file: " & strFilePath, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
        End Try

    End Sub

    Private Sub IniFileSaveOptions()
        ' Prompts the user to select a file to load the options from

        Dim strFilePath As String

        Dim objSaveFile As New System.Windows.Forms.SaveFileDialog

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
                    .InitialDirectory = System.IO.Directory.GetParent(strFilePath).ToString
                Catch
                    .InitialDirectory = GetAppFolderPath()
                End Try
            Else
                .InitialDirectory = GetAppFolderPath()
            End If

            If System.IO.File.Exists(strFilePath) Then
                .FileName = System.IO.Path.GetFileName(strFilePath)
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

    Private Sub IniFileSaveOptions(ByVal strFilePath As String, Optional ByVal blnSaveWindowDimensionsOnly As Boolean = False)
        Dim objXmlFile As New PRISM.Files.XmlSettingsFileAccessor

        Try
            With objXmlFile
                ' Pass True to .LoadSettings() here so that newly made Xml files will have the correct capitalization
                .LoadSettings(strFilePath, True)

                Try
                    .SetParam(XML_SECTION_PROGRAM_OPTIONS, "WindowWidth", Me.Width)
                    .SetParam(XML_SECTION_PROGRAM_OPTIONS, "WindowHeight", Me.Height - 20)

                    If Not blnSaveWindowDimensionsOnly Then

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
                    System.Windows.Forms.MessageBox.Show("Error storing parameter in settings file: " & System.IO.Path.GetFileName(strFilePath), "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
                End Try

                .SaveSettings()
            End With
        Catch ex As Exception
            System.Windows.Forms.MessageBox.Show("Error saving settings to file: " & strFilePath, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
        End Try

        ''Dim objProperty As System.Configuration.SettingsProperty
        ''Dim objProvider As System.Configuration.SettingsProvider
        ''Dim objAttributes As System.Configuration.SettingsAttributeDictionary

        ''Try
        ''    objProvider = New System.Configuration.LocalFileSettingsProvider
        ''    objAttributes = New System.Configuration.SettingsAttributeDictionary
        ''    objAttributes.Add("", "")

        ''    objProperty = New System.Configuration.SettingsProperty("ServerName", System.Type.GetType("System.String"), objProvider, False, "", Configuration.SettingsSerializeAs.String, objAttributes, False, False)
        ''    My.Settings.Properties.Add(objProperty)

        ''    My.Settings.Item("ServerName") = txtServerName.Text

        ''    My.Settings.Save()
        ''Catch ex As Exception

        ''End Try
    End Sub

    Private Sub PopulateTableNamesToExport(ByVal blnEnableAutoSelectDefaultTableNames As Boolean)

        Dim strTableName As String
        Dim strItem As String

        Dim intIndex As Integer
        Dim intItemIndex As Integer
        Dim intCompareIndex As Integer

        Dim htSelectedTableNamesSaved As Hashtable
        Dim objItem As Object

        Dim objRegExOptions As System.Text.RegularExpressions.RegexOptions
        Dim objRegExArray() As System.Text.RegularExpressions.Regex

        Dim intPointerArray() As Integer
        Dim lngValues() As Long
        Dim strNames() As String
        Dim eSortOrder As eTableNameSortModeConstants

        Dim blnAutoHiglightRows As Boolean
        Dim blnHighlightCurrentRow As Boolean

        Try
            If mCachedTableListCount <= 0 Then
                lstTableNamesToExportData.Items.Clear()
                Exit Sub
            End If

            If cboTableNamesToExportSortOrder.SelectedIndex >= 0 Then
                eSortOrder = CType(cboTableNamesToExportSortOrder.SelectedIndex, eTableNameSortModeConstants)
            Else
                eSortOrder = eTableNameSortModeConstants.Name
            End If

            ' Populate intPointerArray
            ReDim intPointerArray(mCachedTableListCount - 1)
            For intIndex = 0 To mCachedTableListCount - 1
                intPointerArray(intIndex) = intIndex
            Next intIndex

            If eSortOrder = eTableNameSortModeConstants.RowCount Then
                ' Sort on RowCount
                ReDim lngValues(mCachedTableListCount - 1)
                Array.Copy(mCachedTableListRowCounts, lngValues, mCachedTableListCount)
                Array.Sort(lngValues, intPointerArray)
            Else
                ' Sort on Table Name
                ReDim strNames(mCachedTableListCount - 1)
                Array.Copy(mCachedTableList, strNames, mCachedTableListCount)
                Array.Sort(strNames, intPointerArray)
            End If

            ' Assure that the auto-select table names are not nothing
            If mTableNamesToAutoSelect Is Nothing Then
                ReDim mTableNamesToAutoSelect(-1)
            End If

            If mTableNameAutoSelectRegEx Is Nothing Then
                ReDim mTableNameAutoSelectRegEx(-1)
            End If

            ' Initialize objRegExArray (we'll fill it below if blnAutoHiglightRows = True)
            objRegExOptions = System.Text.RegularExpressions.RegexOptions.Compiled Or _
                              System.Text.RegularExpressions.RegexOptions.IgnoreCase Or _
                              System.Text.RegularExpressions.RegexOptions.Singleline

            ReDim objRegExArray(mTableNameAutoSelectRegEx.Length - 1)

            If mnuEditAutoSelectDefaultTableNames.Checked And blnEnableAutoSelectDefaultTableNames Then
                blnAutoHiglightRows = True

                For intIndex = 0 To mTableNameAutoSelectRegEx.Length - 1
                    objRegExArray(intIndex) = New System.Text.RegularExpressions.Regex(mTableNameAutoSelectRegEx(intIndex), objRegExOptions)
                Next intIndex
            Else
                blnAutoHiglightRows = False
            End If

            ' Cache the currently selected names so that we can re-highlight them below
            htSelectedTableNamesSaved = New Hashtable
            For Each objItem In lstTableNamesToExportData.SelectedItems
                htSelectedTableNamesSaved.Add(StripRowCountFromTableName(CStr(objItem)), "")
            Next

            lstTableNamesToExportData.Items.Clear()

            For intIndex = 0 To mCachedTableListCount - 1
                strTableName = String.Copy(mCachedTableList(intPointerArray(intIndex)))

                If mCachedTableListIncludesRowCounts Then
                    strItem = strTableName & ROW_COUNT_SEPARATOR & ValueToTextEstimate(mCachedTableListRowCounts(intPointerArray(intIndex)))
                    If mCachedTableListRowCounts(intPointerArray(intIndex)) = 1 Then
                        strItem &= " row)"
                    Else
                        strItem &= " rows)"
                    End If
                Else
                    strItem = String.Copy(strTableName)
                End If
                intItemIndex = lstTableNamesToExportData.Items.Add(strItem)

                blnHighlightCurrentRow = False
                If htSelectedTableNamesSaved.Contains(strTableName) Then
                    ' User had previously highlighted this table name; re-highlight it
                    blnHighlightCurrentRow = True
                ElseIf blnAutoHiglightRows Then
                    ' Test strTableName against the RegEx values from mTableNameAutoSelectRegEx()
                    For intCompareIndex = 0 To mTableNameAutoSelectRegEx.Length - 1
                        If objRegExArray(intCompareIndex).Match(strTableName).Success Then
                            blnHighlightCurrentRow = True
                            Exit For
                        End If
                    Next intCompareIndex

                    If Not blnHighlightCurrentRow Then
                        ' No match: test strTableName against the names in mTableNamesToAutoSelect()
                        For intCompareIndex = 0 To mTableNamesToAutoSelect.Length - 1
                            If strTableName.ToLower = mTableNamesToAutoSelect(intCompareIndex).ToLower Then
                                blnHighlightCurrentRow = True
                                Exit For
                            End If
                        Next intCompareIndex
                    End If
                End If

                If blnHighlightCurrentRow Then
                    ' Highlight this table name
                    lstTableNamesToExportData.SetSelected(intItemIndex, True)
                End If
            Next intIndex

        Catch ex As Exception
            System.Windows.Forms.MessageBox.Show("Error in PopulateTableNamesToExport: " & ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
        End Try

    End Sub

    Private Sub InitializeControls()

        mCachedTableListCount = 0
        ReDim mCachedTableList(-1)
        ReDim mCachedTableListRowCounts(-1)

        ReDim mDefaultDMSDatabaseList(-1)
        ReDim mDefaultMTSDatabaseList(-1)

        PopulateComboBoxes()

        SetToolTips()

        ResetToDefaults(False)

        Try
            mXmlSettingsFilePath = GetSettingsFilePath()

            If Not System.IO.File.Exists(mXmlSettingsFilePath) Then
                IniFileSaveOptions(mXmlSettingsFilePath)
            End If
        Catch ex As Exception
            ' Ignore errors here
        End Try

        IniFileLoadOptions(mXmlSettingsFilePath, False, False)

        EnableDisableControls()

    End Sub

    Private Sub PopulateComboBoxes()
        Dim intIndex As Integer

        Try
            With cboTableNamesToExportSortOrder
                With .Items
                    .Clear()
                    .Insert(eTableNameSortModeConstants.Name, "Sort by Name")
                    .Insert(eTableNameSortModeConstants.RowCount, "Sort by Row Count")
                End With
                .SelectedIndex = eTableNameSortModeConstants.Name
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
                End With

                For intIndex = 0 To .Items.Count - 1
                    .SetSelected(intIndex, True)
                Next intIndex
            End With
        Catch ex As Exception
            System.Windows.Forms.MessageBox.Show("Error in PopulateComboBoxes: " & ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
        End Try

    End Sub

    Private Sub ProgressUpdate(ByVal taskDescription As String, ByVal percentComplete As Single)
        lblProgress.Text = taskDescription
        pbarProgress.Value = percentComplete
    End Sub

    Private Sub ProgressComplete()
        pbarProgress.Value = pbarProgress.Maximum
    End Sub

    Private Sub ScriptDBSchemaObjects()

        Dim strMessage As String
        Dim intIndex As Integer
        Dim blnSelected As Boolean

        If mWorking Then Exit Sub

        Try
            ' Validate txtOutputFolderPath.Text
            If txtOutputFolderPath.TextLength = 0 Then
                txtOutputFolderPath.Text = GetAppFolderPath()
            End If

            If Not System.IO.Directory.Exists(txtOutputFolderPath.Text) Then
                strMessage = "Output folder not found: " & txtOutputFolderPath.Text
                System.Windows.Forms.MessageBox.Show(strMessage, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
                Exit Sub
            End If

            clsExportDBSchema.InitializeSchemaExportOptions(mSchemaExportOptions)
            With mSchemaExportOptions
                .OutputFolderPath = txtOutputFolderPath.Text
                .OutputFolderNamePrefix = txtOutputFolderNamePrefix.Text
                .CreateFolderForEachDB = chkCreateFolderForEachDB.Checked
                .IncludeSystemObjects = mnuEditIncludeSystemObjects.Checked
                .IncludeTimestampInScriptFileHeader = mnuEditIncludeTimestampInScriptFileHeader.Checked

                .ExportServerSettingsLoginsAndJobs = chkExportServerSettingsLoginsAndJobs.Checked
                .ServerOutputFolderNamePrefix = txtServerOutputFolderNamePrefix.Text

                .SaveDataAsInsertIntoStatements = mnuEditSaveDataAsInsertIntoStatements.Checked
                .DatabaseTypeForInsertInto = clsExportDBSchema.eTargetDatabaseTypeConstants.SqlServer       ' Reserved for future expansion
                .AutoSelectTableNamesForDataExport = mnuEditAutoSelectDefaultTableNames.Checked

                ' Note: mDBSchemaExporter & mTableNameAutoSelectRegEx will be passed to mDBSchemaExporter below

                For intIndex = 0 To lstObjectTypesToScript.Items.Count - 1
                    blnSelected = lstObjectTypesToScript.GetSelected(intIndex)

                    Select Case CType(intIndex, clsExportDBSchema.eSchemaObjectTypeConstants)
                        Case clsExportDBSchema.eSchemaObjectTypeConstants.SchemasAndRoles
                            .ExportDBSchemasAndRoles = blnSelected
                        Case clsExportDBSchema.eSchemaObjectTypeConstants.Tables
                            .ExportTables = blnSelected
                        Case clsExportDBSchema.eSchemaObjectTypeConstants.Views
                            .ExportViews = blnSelected
                        Case clsExportDBSchema.eSchemaObjectTypeConstants.StoredProcedures
                            .ExportStoredProcedures = blnSelected
                        Case clsExportDBSchema.eSchemaObjectTypeConstants.UserDefinedFunctions
                            .ExportUserDefinedFunctions = blnSelected
                        Case clsExportDBSchema.eSchemaObjectTypeConstants.UserDefinedDataTypes
                            .ExportUserDefinedDataTypes = blnSelected
                        Case clsExportDBSchema.eSchemaObjectTypeConstants.UserDefinedTypes
                            .ExportUserDefinedTypes = blnSelected
                    End Select
                Next intIndex

                With .ConnectionInfo
                    .ServerName = txtServerName.Text
                    .UserName = txtUsername.Text
                    .Password = txtPassword.Text
                    .UseIntegratedAuthentication = chkUseIntegratedAuthentication.Checked
                End With
            End With

        Catch ex As Exception
            System.Windows.Forms.MessageBox.Show("Error initializing mSchemaExportOptions in ScriptDBSchemaObjects: " & ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
            Exit Sub
        End Try

        Try
            ' Populate mDatabaseListToProcess and mTableNamesForDataExport
            mDatabaseListToProcess = GetSelectedDatabases()
            mTableNamesForDataExport = GetSelectedTableNamesForDataExport(mnuEditWarnOnHighTableRowCount.Checked)

            If mDatabaseListToProcess.Length = 0 And Not mSchemaExportOptions.ExportServerSettingsLoginsAndJobs Then
                System.Windows.Forms.MessageBox.Show("No databases or tables were selected; unable to continue", "Nothing To Do", MessageBoxButtons.OK, MessageBoxIcon.Information)
                Exit Sub
            End If

        Catch ex As Exception
            System.Windows.Forms.MessageBox.Show("Error determining list of databases (and tables) to process: " & ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
            Exit Sub
        End Try

        Try
            If mDBSchemaExporter Is Nothing Then
                mDBSchemaExporter = New clsExportDBSchema
            End If

            If Not mTableNamesToAutoSelect Is Nothing Then
                mDBSchemaExporter.TableNamesToAutoSelect = mTableNamesToAutoSelect
            End If

            If Not mTableNameAutoSelectRegEx Is Nothing Then
                mDBSchemaExporter.TableNameAutoSelectRegEx = mTableNameAutoSelectRegEx
            End If
        Catch ex As Exception
            System.Windows.Forms.MessageBox.Show("Error instantiating mDBSchemaExporter and updating the data export auto-select lists: " & ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
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
                System.Threading.Thread.Sleep(THREAD_WAIT_MSEC)

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
                        System.Threading.Thread.Sleep(THREAD_WAIT_MSEC)
                        Exit Do
                    End Try
                    Application.DoEvents()
                Loop
            End If

            Application.DoEvents()
            If Not mSchemaExportSuccess OrElse mDBSchemaExporter.ErrorCode <> 0 Then
                strMessage = "Error exporting the schema objects (ErrorCode=" & mDBSchemaExporter.ErrorCode.ToString & "): " & ControlChars.NewLine & mDBSchemaExporter.StatusMessage
                System.Windows.Forms.MessageBox.Show(strMessage, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
            End If
        Catch ex As Exception
            System.Windows.Forms.MessageBox.Show("Error calling ScriptDBSchemaObjectsThread in ScriptDBSchemaObjects: " & ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
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
            System.Windows.Forms.MessageBox.Show("Error finalizing results in ScriptDBSchemaObjects: " & ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
        End Try

    End Sub

    Private Sub ScriptDBSchemaObjectsThread()
        ' Note: Populate mDatabaseListToProcess and mTableNamesForDataExport prior to calling this sub

        Try
            mSchemaExportSuccess = mDBSchemaExporter.ScriptServerAndDBObjects(mSchemaExportOptions, mDatabaseListToProcess, mTableNamesForDataExport)
        Catch ex As Exception
            System.Windows.Forms.MessageBox.Show("Error in ScriptDBSchemaObjectsThread: " & ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
        End Try
    End Sub

    Private Sub SelectDefaultDBs(ByVal strDBList() As String)

        Dim intIndex As Integer
        Dim strCurrentDB As String

        If Not strDBList Is Nothing Then

            ' Update the entries in strDBList to be lower case, then sort alphabetically
            For intIndex = 0 To strDBList.Length - 1
                strDBList(intIndex) = strDBList(intIndex).ToLower
            Next intIndex

            Array.Sort(strDBList)

            lstDatabasesToProcess.ClearSelected()

            For intIndex = 0 To lstDatabasesToProcess.Items.Count - 1
                strCurrentDB = lstDatabasesToProcess.Items(intIndex).ToString.ToLower

                If Array.BinarySearch(strDBList, strCurrentDB) >= 0 Then
                    lstDatabasesToProcess.SetSelected(intIndex, True)
                End If
            Next intIndex
        End If

    End Sub

    Private Sub ResetToDefaults(ByVal blnConfirm As Boolean)
        Dim eResponse As System.Windows.Forms.DialogResult

        Try
            If blnConfirm Then
                eResponse = System.Windows.Forms.MessageBox.Show("Are you sure you want to reset all settings to their default values?", "Reset to Defaults", MessageBoxButtons.YesNoCancel, MessageBoxIcon.Question, MessageBoxDefaultButton.Button1)
                If eResponse <> Windows.Forms.DialogResult.Yes Then
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

            clsExportDBSchema.InitializeAutoSelectTableNames(mTableNamesToAutoSelect)
            clsExportDBSchema.InitializeAutoSelectTableRegEx(mTableNameAutoSelectRegEx)

            ReDim mDefaultDMSDatabaseList(2)
            mDefaultDMSDatabaseList(0) = "DMS5"
            mDefaultDMSDatabaseList(1) = "Protein_Sequences"
            mDefaultDMSDatabaseList(2) = "DMSHistoricLog1"

            ReDim mDefaultMTSDatabaseList(9)
            mDefaultMTSDatabaseList(0) = "MT_Template_01"
            mDefaultMTSDatabaseList(1) = "PT_Template_01"
            mDefaultMTSDatabaseList(2) = "MT_Main"
            mDefaultMTSDatabaseList(3) = "MTS_Master"
            mDefaultMTSDatabaseList(4) = "Prism_IFC"
            mDefaultMTSDatabaseList(5) = "Prism_RPT"
            mDefaultMTSDatabaseList(6) = "PAST_BB"
            mDefaultMTSDatabaseList(7) = "Master_Sequences"
            mDefaultMTSDatabaseList(8) = "MT_Historic_Log"
            mDefaultMTSDatabaseList(9) = "MT_HistoricLog"

            EnableDisableControls()
        Catch ex As Exception
            System.Windows.Forms.MessageBox.Show("Error in ResetToDefaults: " & ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
        End Try

    End Sub

    Private Sub SelectAllListboxItems(ByVal objListBox As ListBox)
        Dim intIndex As Integer

        Try
            For intIndex = 0 To objListBox.Items.Count - 1
                objListBox.SetSelected(intIndex, True)
            Next
        Catch ex As Exception
            ' Ignore errors here
        End Try
    End Sub

    Private Sub SelectOutputFolder()
        ' Prompts the user to select the output folder to create the scripted objects in 

        Dim objFolderBrowser As PRISM.Files.FolderBrowser
        Dim blnSuccess As Boolean

        Try
            objFolderBrowser = New PRISM.Files.FolderBrowser

            If txtOutputFolderPath.TextLength > 0 Then
                blnSuccess = objFolderBrowser.BrowseForFolder(txtOutputFolderPath.Text)
            Else
                blnSuccess = objFolderBrowser.BrowseForFolder(GetAppFolderPath())
            End If

            If blnSuccess Then
                txtOutputFolderPath.Text = objFolderBrowser.FolderPath
            End If
        Catch ex As Exception
            System.Windows.Forms.MessageBox.Show("Error in SelectOutputFolder: " & ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
        End Try
    End Sub

    Private Sub SetToolTips()
        Dim objToolTipControl As New System.Windows.Forms.ToolTip

        Try
            With objToolTipControl
                .SetToolTip(chkCreateFolderForEachDB, "This will be automatically enabled if multiple databases are chosen above")
                .SetToolTip(txtOutputFolderNamePrefix, "The output folder for each database will be named with this prefix followed by the database name")
                .SetToolTip(txtServerOutputFolderNamePrefix, "Server settings will be saved in a folder with this prefix followed by the server name")
            End With
        Catch ex As Exception
            System.Windows.Forms.MessageBox.Show("Error in SetToolTips: " & ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
        Finally
            objToolTipControl = Nothing
        End Try

    End Sub

    Private Sub ShowAboutBox()
        Dim strMessage As String

        strMessage = String.Empty

        strMessage &= "Program written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA) in August 2006" & ControlChars.NewLine
        strMessage &= "Copyright 2006, Battelle Memorial Institute.  All Rights Reserved." & ControlChars.NewLine & ControlChars.NewLine

        strMessage &= "This is version " & System.Windows.Forms.Application.ProductVersion & " (" & PROGRAM_DATE & "). " & ControlChars.NewLine & ControlChars.NewLine

        strMessage &= "E-mail: matthew.monroe@pnl.gov or matt@alchemistmatt.com" & ControlChars.NewLine
        strMessage &= "Website: http://ncrr.pnl.gov/ or http://www.sysbio.org/resources/staff/" & ControlChars.NewLine & ControlChars.NewLine

        strMessage &= "Licensed under the Apache License, Version 2.0; you may not use this file except in compliance with the License.  "
        strMessage &= "You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0" & ControlChars.NewLine & ControlChars.NewLine

        strMessage &= "Notice: This computer software was prepared by Battelle Memorial Institute, "
        strMessage &= "hereinafter the Contractor, under Contract No. DE-AC05-76RL0 1830 with the "
        strMessage &= "Department of Energy (DOE).  All rights in the computer software are reserved "
        strMessage &= "by DOE on behalf of the United States Government and the Contractor as "
        strMessage &= "provided in the Contract.  NEITHER THE   VERNMENT NOR THE CONTRACTOR MAKES ANY "
        strMessage &= "WARRANTY, EXPRESS OR IMPLIED, OR ASSUMES ANY LIABILITY FOR THE USE OF THIS "
        strMessage &= "SOFTWARE.  This notice including this sentence must appear on any copies of "
        strMessage &= "this computer software." & ControlChars.NewLine

        Windows.Forms.MessageBox.Show(strMessage, "About", MessageBoxButtons.OK, MessageBoxIcon.Information)
    End Sub

    Private Sub StripRowCountsFromTableNames(ByRef strTableNames() As String)
        Dim intIndex As Integer

        If strTableNames Is Nothing Then Exit Sub
        For intIndex = 0 To strTableNames.Length - 1
            strTableNames(intIndex) = StripRowCountFromTableName(strTableNames(intIndex))
        Next intIndex

    End Sub

    Private Function StripRowCountFromTableName(ByVal strTableName As String) As String
        Dim intCharIndex As Integer

        If strTableName Is Nothing Then
            Return String.Empty
        Else
            intCharIndex = strTableName.IndexOf(ROW_COUNT_SEPARATOR)
            If intCharIndex > 0 Then
                Return strTableName.Substring(0, intCharIndex)
            Else
                Return strTableName
            End If
        End If

    End Function

    Private Sub SubTaskProgressUpdate(ByVal taskDescription As String, ByVal percentComplete As Single)
        lblSubtaskProgress.Text = taskDescription
        pbarSubtaskProgress.Value = percentComplete

        If mDBSchemaExporter.ProgressStepCount > 0 Then
            pbarProgress.Value = mDBSchemaExporter.ProgressStep / CSng(mDBSchemaExporter.ProgressStepCount) * 100.0! + 1.0! / mDBSchemaExporter.ProgressStepCount * percentComplete
        End If
    End Sub

    Private Sub SubTaskProgressComplete()
        pbarSubtaskProgress.Value = pbarProgress.Maximum
    End Sub

    Private Sub UpdateDatabaseList()
        Dim strDatabaseList() As String = New String() {}
        Dim intIndex As Integer
        Dim intItemIndex As Integer

        Dim htSelectedDatabaseNamesSaved As Hashtable
        Dim objItem As Object

        If mWorking Then Exit Sub

        Try
            mWorking = True
            EnableDisableControls()

            UpdatePauseUnpauseCaption(clsExportDBSchema.ePauseStatusConstants.Unpaused)
            Application.DoEvents()

            If VerifyOrUpdateServerConnection(True) Then
                ' Cache the currently selected names so that we can re-highlight them below
                htSelectedDatabaseNamesSaved = New Hashtable
                For Each objItem In lstDatabasesToProcess.SelectedItems
                    htSelectedDatabaseNamesSaved.Add(CStr(objItem), "")
                Next

                lstDatabasesToProcess.Items.Clear()

                If mDBSchemaExporter.GetSqlServerDatabases(strDatabaseList) Then
                    For intIndex = 0 To strDatabaseList.Length - 1
                        If strDatabaseList(intIndex) Is Nothing Then Exit For

                        intItemIndex = lstDatabasesToProcess.Items.Add(strDatabaseList(intIndex))

                        If htSelectedDatabaseNamesSaved.Contains(strDatabaseList(intIndex)) Then
                            ' Highlight this table name
                            lstDatabasesToProcess.SetSelected(intItemIndex, True)
                        End If
                    Next intIndex
                End If
            End If
        Catch ex As Exception
            System.Windows.Forms.MessageBox.Show("Error in UpdateDatabaseList: " & ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
        Finally
            mWorking = False
            EnableDisableControls()
        End Try
    End Sub

    Private Sub UpdatePauseUnpauseCaption(ByVal ePauseStatus As clsExportDBSchema.ePauseStatusConstants)
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

    Private Sub UpdateTableNamesInSelectedDB()

        Dim strDatabaseName As String = String.Empty
        Dim strTableList() As String = New String() {}
        Dim lngRowCounts() As Long = New Long() {}
        Dim blnIncludeTableRowCounts As Boolean

        Dim intIndex As Integer
        If mWorking Then Exit Sub

        Try
            If lstDatabasesToProcess.Items.Count = 0 Then
                System.Windows.Forms.MessageBox.Show("The database list is currently empty; unable to continue", "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
                Exit Try
            ElseIf lstDatabasesToProcess.SelectedIndex < 0 Then
                ' Auto-select the first database
                lstDatabasesToProcess.SelectedIndex = 0
            End If
            strDatabaseName = CStr(lstDatabasesToProcess.Items(lstDatabasesToProcess.SelectedIndex))

        Catch ex As Exception
            System.Windows.Forms.MessageBox.Show("Error determining selected database name in UpdateTableNamesInSelectedDB: " & ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
        End Try

        If strDatabaseName Is Nothing OrElse strDatabaseName.Length = 0 Then
            Exit Sub
        End If

        Try
            mWorking = True
            EnableDisableControls()

            UpdatePauseUnpauseCaption(clsExportDBSchema.ePauseStatusConstants.Unpaused)
            Application.DoEvents()

            If VerifyOrUpdateServerConnection(True) Then

                blnIncludeTableRowCounts = mnuEditIncludeTableRowCounts.Checked
                If mDBSchemaExporter.GetSqlServerDatabaseTableNames(strDatabaseName, strTableList, lngRowCounts, blnIncludeTableRowCounts, mnuEditIncludeSystemObjects.Checked) Then
                    mCachedTableListCount = 0
                    mCachedTableListIncludesRowCounts = blnIncludeTableRowCounts
                    ReDim mCachedTableList(strTableList.Length - 1)
                    ReDim mCachedTableListRowCounts(strTableList.Length - 1)

                    For intIndex = 0 To strTableList.Length - 1
                        If strTableList(mCachedTableListCount) Is Nothing Then Exit For
                        mCachedTableList(mCachedTableListCount) = strTableList(mCachedTableListCount)
                        mCachedTableListRowCounts(mCachedTableListCount) = lngRowCounts(mCachedTableListCount)
                        mCachedTableListCount += 1
                    Next intIndex

                    If mCachedTableListCount < mCachedTableList.Length Then
                        ReDim Preserve mCachedTableList(mCachedTableListCount - 1)
                        ReDim Preserve mCachedTableListRowCounts(mCachedTableListCount - 1)
                    End If

                    Array.Sort(mCachedTableList, mCachedTableListRowCounts)

                    PopulateTableNamesToExport(True)
                End If
            End If
        Catch ex As Exception
            System.Windows.Forms.MessageBox.Show("Error in UpdateTableNamesInSelectedDB: " & ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
        Finally
            mWorking = False
            EnableDisableControls()
        End Try

    End Sub

    Private Function ValueToTextEstimate(ByVal lngValue As Long) As String
        ' Converts lngValue to a text-based value
        ' For data between 10000 and 1 million, rounds to the nearest thousand
        ' For data over 1 million, displays as x.x million

        Dim strValue As String = String.Empty
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

    Private Function VerifyOrUpdateServerConnection(ByVal blnInformUserOnFailure As Boolean) As Boolean
        Dim blnConnected As Boolean
        Dim udtConnectionInfo As clsExportDBSchema.udtServerConnectionInfoType

        Dim strMessage As String

        Try
            strMessage = String.Empty
            blnConnected = False

            If txtServerName.TextLength = 0 Then
                strMessage = "Please enter the server name"
                If blnInformUserOnFailure Then
                    System.Windows.Forms.MessageBox.Show(strMessage, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
                End If
            Else
                With udtConnectionInfo
                    .ServerName = txtServerName.Text
                    .UseIntegratedAuthentication = chkUseIntegratedAuthentication.Checked
                    .UserName = txtUsername.Text
                    .Password = txtPassword.Text
                End With

                If mDBSchemaExporter Is Nothing Then
                    mDBSchemaExporter = New clsExportDBSchema(udtConnectionInfo)
                    blnConnected = mDBSchemaExporter.ConnectedToServer
                Else
                    blnConnected = mDBSchemaExporter.ConnectToServer(udtConnectionInfo)
                End If

                If Not blnConnected AndAlso blnInformUserOnFailure Then
                    strMessage = "Error connecting to server " & udtConnectionInfo.ServerName
                    If mDBSchemaExporter.StatusMessage.Length > 0 Then
                        strMessage &= "; " & mDBSchemaExporter.StatusMessage
                    End If
                    System.Windows.Forms.MessageBox.Show(strMessage, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
                End If
            End If

        Catch ex As Exception
            System.Windows.Forms.MessageBox.Show("Error in VerifyOrUpdateServerConnection: " & ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation)
            blnConnected = False
        End Try

        Return blnConnected
    End Function

    Private Sub frmMain_Load(ByVal eventSender As System.Object, ByVal eventArgs As System.EventArgs) Handles MyBase.Load
        ' Note that InitializeControls() is called in Sub New()

    End Sub

#Region "Control Handlers"

    Private Sub cboTableNamesToExportSortOrder_SelectedIndexChanged(ByVal sender As System.Object, ByVal e As System.EventArgs) Handles cboTableNamesToExportSortOrder.SelectedIndexChanged
        PopulateTableNamesToExport(False)
    End Sub

    Private Sub cmdPauseUnpause_Click(ByVal sender As System.Object, ByVal e As System.EventArgs) Handles cmdPauseUnpause.Click
        If Not mDBSchemaExporter Is Nothing Then
            mDBSchemaExporter.TogglePause()
            If mDBSchemaExporter.PauseStatus = clsExportDBSchema.ePauseStatusConstants.UnpauseRequested OrElse _
                mDBSchemaExporter.PauseStatus = clsExportDBSchema.ePauseStatusConstants.Unpaused Then
            End If
        End If
    End Sub

    Private Sub cmdRefreshDBList_Click(ByVal sender As System.Object, ByVal e As System.EventArgs) Handles cmdRefreshDBList.Click
        UpdateDatabaseList()
    End Sub

    Private Sub cmdUpdateTableNames_Click(ByVal sender As System.Object, ByVal e As System.EventArgs) Handles cmdUpdateTableNames.Click
        UpdateTableNamesInSelectedDB()
    End Sub

    Private Sub chkUseIntegratedAuthentication_CheckedChanged(ByVal sender As System.Object, ByVal e As System.EventArgs) Handles chkUseIntegratedAuthentication.CheckedChanged
        EnableDisableControls()
    End Sub

    Private Sub cmdAbort_Click(ByVal sender As System.Object, ByVal e As System.EventArgs) Handles cmdAbort.Click
        ConfirmAbortRequest()
    End Sub

    Private Sub cmdGo_Click(ByVal eventSender As System.Object, ByVal eventArgs As System.EventArgs) Handles cmdGo.Click
        ScriptDBSchemaObjects()
    End Sub

    Private Sub cmdSelectDefaultDMSDBs_Click(ByVal sender As System.Object, ByVal e As System.EventArgs) Handles cmdSelectDefaultDMSDBs.Click
        SelectDefaultDBs(mDefaultDMSDatabaseList)
    End Sub

    Private Sub cmdSelectDefaultMTSDBs_Click(ByVal sender As System.Object, ByVal e As System.EventArgs) Handles cmdSelectDefaultMTSDBs.Click
        SelectDefaultDBs(mDefaultMTSDatabaseList)
    End Sub

    Private Sub cmdExit_Click(ByVal eventSender As System.Object, ByVal eventArgs As System.EventArgs) Handles cmdExit.Click
        Me.Close()
    End Sub

    Private Sub lstDatabasesToProcess_KeyPress(ByVal sender As Object, ByVal e As System.Windows.Forms.KeyPressEventArgs) Handles lstDatabasesToProcess.KeyPress
        ' Always mark e as handled to allow Ctrl+A to be used to select all the entries in the listbox
        e.Handled = True
    End Sub

    Private Sub lstDatabasesToProcess_KeyDown(ByVal sender As Object, ByVal e As System.Windows.Forms.KeyEventArgs) Handles lstDatabasesToProcess.KeyDown
        If e.Control Then
            If e.KeyCode = Keys.A Then
                ' Ctrl+A - Select All
                SelectAllListboxItems(lstDatabasesToProcess)
                e.Handled = True
            End If
        End If
    End Sub

    Private Sub lstObjectTypesToScript_KeyPress(ByVal sender As Object, ByVal e As System.Windows.Forms.KeyPressEventArgs) Handles lstObjectTypesToScript.KeyPress
        ' Always mark e as handled to allow Ctrl+A to be used to select all the entries in the listbox
        e.Handled = True
    End Sub

    Private Sub lstObjectTypesToScript_KeyDown(ByVal sender As Object, ByVal e As System.Windows.Forms.KeyEventArgs) Handles lstObjectTypesToScript.KeyDown
        If e.Control Then
            If e.KeyCode = Keys.A Then
                ' Ctrl+A - Select All
                SelectAllListboxItems(lstObjectTypesToScript)
                e.Handled = True
            End If
        End If
    End Sub

    Private Sub lstTableNamesToExportData_KeyPress(ByVal sender As Object, ByVal e As System.Windows.Forms.KeyPressEventArgs) Handles lstTableNamesToExportData.KeyPress
        ' Always mark e as handled to allow Ctrl+A to be used to select all the entries in the listbox
        e.Handled = True
    End Sub

    Private Sub lstTableNamesToExportData_KeyDown(ByVal sender As Object, ByVal e As System.Windows.Forms.KeyEventArgs) Handles lstTableNamesToExportData.KeyDown
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
    Private Sub frmMain_Closing(ByVal sender As Object, ByVal e As System.ComponentModel.CancelEventArgs) Handles MyBase.Closing
        If Not mDBSchemaExporter Is Nothing Then
            mDBSchemaExporter.AbortProcessingNow()
        End If
    End Sub
#End Region

#Region "Menu Handlers"
    Private Sub mnuFileSelectOutputFolder_Click(ByVal sender As System.Object, ByVal e As System.EventArgs) Handles mnuFileSelectOutputFolder.Click
        SelectOutputFolder()
    End Sub

    Private Sub mnuFileSaveOptions_Click(ByVal sender As System.Object, ByVal e As System.EventArgs) Handles mnuFileSaveOptions.Click
        IniFileSaveOptions()
    End Sub

    Private Sub mnuFileLoadOptions_Click(ByVal sender As System.Object, ByVal e As System.EventArgs) Handles mnuFileLoadOptions.Click
        IniFileLoadOptions()
    End Sub

    Private Sub mnuFileExit_Click(ByVal sender As System.Object, ByVal e As System.EventArgs) Handles mnuFileExit.Click
        Me.Close()
    End Sub

    Private Sub mnuEditStart_Click(ByVal sender As System.Object, ByVal e As System.EventArgs) Handles mnuEditStart.Click
        ScriptDBSchemaObjects()
    End Sub

    Private Sub mnuEditIncludeSystemObjects_Click(ByVal sender As System.Object, ByVal e As System.EventArgs) Handles mnuEditIncludeSystemObjects.Click
        mnuEditIncludeSystemObjects.Checked = Not mnuEditIncludeSystemObjects.Checked
    End Sub

    Private Sub mnuEditScriptObjectsThreaded_Click(ByVal sender As System.Object, ByVal e As System.EventArgs) Handles mnuEditScriptObjectsThreaded.Click
        mnuEditScriptObjectsThreaded.Checked = Not mnuEditScriptObjectsThreaded.Checked
    End Sub

    Private Sub mnuEditPauseAfterEachDatabase_Click_1(ByVal sender As System.Object, ByVal e As System.EventArgs) Handles mnuEditPauseAfterEachDatabase.Click
        mnuEditPauseAfterEachDatabase.Checked = Not mnuEditPauseAfterEachDatabase.Checked
    End Sub

    Private Sub mnuEditIncludeTimestampInScriptFileHeader_Click(ByVal sender As System.Object, ByVal e As System.EventArgs) Handles mnuEditIncludeTimestampInScriptFileHeader.Click
        mnuEditIncludeTimestampInScriptFileHeader.Checked = Not mnuEditIncludeTimestampInScriptFileHeader.Checked
    End Sub

    Private Sub mnuEditIncludeTableRowCounts_Click(ByVal sender As System.Object, ByVal e As System.EventArgs) Handles mnuEditIncludeTableRowCounts.Click
        mnuEditIncludeTableRowCounts.Checked = Not mnuEditIncludeTableRowCounts.Checked
    End Sub

    Private Sub mnuEditAutoSelectDefaultTableNames_Click(ByVal sender As System.Object, ByVal e As System.EventArgs) Handles mnuEditAutoSelectDefaultTableNames.Click
        mnuEditAutoSelectDefaultTableNames.Checked = Not mnuEditAutoSelectDefaultTableNames.Checked
    End Sub

    Private Sub mnuEditSaveDataAsInsertIntoStatements_Click(ByVal sender As System.Object, ByVal e As System.EventArgs) Handles mnuEditSaveDataAsInsertIntoStatements.Click
        mnuEditSaveDataAsInsertIntoStatements.Checked = Not mnuEditSaveDataAsInsertIntoStatements.Checked
    End Sub

    Private Sub mnuEditWarnOnHighTableRowCount_Click(ByVal sender As System.Object, ByVal e As System.EventArgs) Handles mnuEditWarnOnHighTableRowCount.Click
        mnuEditWarnOnHighTableRowCount.Checked = Not mnuEditWarnOnHighTableRowCount.Checked
    End Sub

    Private Sub mnuEditResetOptions_Click(ByVal sender As System.Object, ByVal e As System.EventArgs) Handles mnuEditResetOptions.Click
        ResetToDefaults(True)
    End Sub

    Private Sub mnuHelpAbout_Click(ByVal sender As System.Object, ByVal e As System.EventArgs) Handles mnuHelpAbout.Click
        ShowAboutBox()
    End Sub
#End Region

#Region "DB Schema Export Automation Events"
    Private Sub mDBSchemaExporter_NewMessage(ByVal strMessage As String, ByVal eMessageType As clsExportDBSchema.eMessageTypeConstants) Handles mDBSchemaExporter.NewMessage
        If Me.InvokeRequired Then
            Me.Invoke(New AppendNewMessageHandler(AddressOf AppendNewMessage), New Object() {strMessage, eMessageType})
        Else
            AppendNewMessage(strMessage, eMessageType)
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

    Private Sub mDBSchemaExporter_ProgressChanged(ByVal taskDescription As String, ByVal percentComplete As Single) Handles mDBSchemaExporter.ProgressChanged
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

    Private Sub mDBSchemaExporter_DBExportStarting(ByVal strDatabaseName As String) Handles mDBSchemaExporter.DBExportStarting
        If Me.InvokeRequired Then
            Me.Invoke(New HandleDBExportStartingEventHandler(AddressOf HandleDBExportStartingEvent), New Object() {strDatabaseName})
        Else
            HandleDBExportStartingEvent(strDatabaseName)
        End If
        Application.DoEvents()
    End Sub

    Private Sub mDBSchemaExporter_SubtaskProgressChanged(ByVal taskDescription As String, ByVal percentComplete As Single) Handles mDBSchemaExporter.SubtaskProgressChanged
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