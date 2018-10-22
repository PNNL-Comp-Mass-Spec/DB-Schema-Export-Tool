<Global.Microsoft.VisualBasic.CompilerServices.DesignerGenerated()> _
Partial Class frmMain
    Inherits System.Windows.Forms.Form

    'Form overrides dispose to clean up the component list.
    <System.Diagnostics.DebuggerNonUserCode()> _
    Protected Overrides Sub Dispose(ByVal disposing As Boolean)
        If disposing AndAlso components IsNot Nothing Then
            components.Dispose()
        End If
        MyBase.Dispose(disposing)
    End Sub

    'Required by the Windows Form Designer
    Private components As System.ComponentModel.IContainer

    'NOTE: The following procedure is required by the Windows Form Designer
    'It can be modified using the Windows Form Designer.  
    'Do not modify it using the code editor.
    <System.Diagnostics.DebuggerStepThrough()> _
    Private Sub InitializeComponent()
        Me.components = New System.ComponentModel.Container()
        Me.mnuEditSep1 = New System.Windows.Forms.MenuItem()
        Me.mnuEditStart = New System.Windows.Forms.MenuItem()
        Me.mnuEditScriptObjectsThreaded = New System.Windows.Forms.MenuItem()
        Me.fraConnectionSettings = New System.Windows.Forms.GroupBox()
        Me.lblServerName = New System.Windows.Forms.Label()
        Me.chkUseIntegratedAuthentication = New System.Windows.Forms.CheckBox()
        Me.txtPassword = New System.Windows.Forms.TextBox()
        Me.txtUsername = New System.Windows.Forms.TextBox()
        Me.txtServerName = New System.Windows.Forms.TextBox()
        Me.lblPassword = New System.Windows.Forms.Label()
        Me.lblUsername = New System.Windows.Forms.Label()
        Me.mnuEditSep4 = New System.Windows.Forms.MenuItem()
        Me.mnuEdit = New System.Windows.Forms.MenuItem()
        Me.mnuEditIncludeSystemObjects = New System.Windows.Forms.MenuItem()
        Me.mnuEditPauseAfterEachDatabase = New System.Windows.Forms.MenuItem()
        Me.mnuEditIncludeTimestampInScriptFileHeader = New System.Windows.Forms.MenuItem()
        Me.mnuEditSep2 = New System.Windows.Forms.MenuItem()
        Me.mnuEditIncludeTableRowCounts = New System.Windows.Forms.MenuItem()
        Me.mnuEditAutoSelectDefaultTableNames = New System.Windows.Forms.MenuItem()
        Me.mnuEditSep3 = New System.Windows.Forms.MenuItem()
        Me.mnuEditWarnOnHighTableRowCount = New System.Windows.Forms.MenuItem()
        Me.mnuEditSaveDataAsInsertIntoStatements = New System.Windows.Forms.MenuItem()
        Me.mnuEditResetOptions = New System.Windows.Forms.MenuItem()
        Me.lblProgress = New System.Windows.Forms.Label()
        Me.mnuFileLoadOptions = New System.Windows.Forms.MenuItem()
        Me.mnuFileSaveOptions = New System.Windows.Forms.MenuItem()
        Me.mnuFileExit = New System.Windows.Forms.MenuItem()
        Me.mnuFileSep2 = New System.Windows.Forms.MenuItem()
        Me.cmdUpdateTableNames = New System.Windows.Forms.Button()
        Me.chkCreateDirectoryForEachDB = New System.Windows.Forms.CheckBox()
        Me.pbarProgress = New System.Windows.Forms.ProgressBar()
        Me.cmdRefreshDBList = New System.Windows.Forms.Button()
        Me.mnuHelp = New System.Windows.Forms.MenuItem()
        Me.mnuHelpAbout = New System.Windows.Forms.MenuItem()
        Me.fraOutputOptions = New System.Windows.Forms.GroupBox()
        Me.lblTableDataToExport = New System.Windows.Forms.Label()
        Me.lblServerOutputDirectoryNamePrefix = New System.Windows.Forms.Label()
        Me.txtServerOutputDirectoryNamePrefix = New System.Windows.Forms.TextBox()
        Me.chkExportServerSettingsLoginsAndJobs = New System.Windows.Forms.CheckBox()
        Me.lblSelectDefaultDBs = New System.Windows.Forms.Label()
        Me.cmdSelectDefaultDMSDBs = New System.Windows.Forms.Button()
        Me.cmdSelectDefaultMTSDBs = New System.Windows.Forms.Button()
        Me.lblOutputDirectoryNamePrefix = New System.Windows.Forms.Label()
        Me.txtOutputDirectoryNamePrefix = New System.Windows.Forms.TextBox()
        Me.cboTableNamesToExportSortOrder = New System.Windows.Forms.ComboBox()
        Me.lstTableNamesToExportData = New System.Windows.Forms.ListBox()
        Me.txtOutputDirectoryPath = New System.Windows.Forms.TextBox()
        Me.lblOutputDirectoryPath = New System.Windows.Forms.Label()
        Me.lstDatabasesToProcess = New System.Windows.Forms.ListBox()
        Me.mnuFileSep1 = New System.Windows.Forms.MenuItem()
        Me.lblSubtaskProgress = New System.Windows.Forms.Label()
        Me.fraStatus = New System.Windows.Forms.GroupBox()
        Me.pbarSubtaskProgress = New System.Windows.Forms.ProgressBar()
        Me.lblMessage = New System.Windows.Forms.Label()
        Me.MainMenuControl = New System.Windows.Forms.MainMenu(Me.components)
        Me.mnuFile = New System.Windows.Forms.MenuItem()
        Me.mnuFileSelectOutputDirectory = New System.Windows.Forms.MenuItem()
        Me.fraControls = New System.Windows.Forms.GroupBox()
        Me.cmdGo = New System.Windows.Forms.Button()
        Me.cmdExit = New System.Windows.Forms.Button()
        Me.cmdAbort = New System.Windows.Forms.Button()
        Me.cmdPauseUnpause = New System.Windows.Forms.Button()
        Me.lstObjectTypesToScript = New System.Windows.Forms.ListBox()
        Me.fraObjectTypesToScript = New System.Windows.Forms.GroupBox()
        Me.fraConnectionSettings.SuspendLayout()
        Me.fraOutputOptions.SuspendLayout()
        Me.fraStatus.SuspendLayout()
        Me.fraControls.SuspendLayout()
        Me.fraObjectTypesToScript.SuspendLayout()
        Me.SuspendLayout()
        '
        'mnuEditSep1
        '
        Me.mnuEditSep1.Index = 2
        Me.mnuEditSep1.Text = "-"
        '
        'mnuEditStart
        '
        Me.mnuEditStart.Index = 0
        Me.mnuEditStart.Shortcut = System.Windows.Forms.Shortcut.CtrlG
        Me.mnuEditStart.Text = "&Export DB Schema"
        '
        'mnuEditScriptObjectsThreaded
        '
        Me.mnuEditScriptObjectsThreaded.Index = 3
        Me.mnuEditScriptObjectsThreaded.Text = "Script Objects in Separate &Thread"
        '
        'fraConnectionSettings
        '
        Me.fraConnectionSettings.BackColor = System.Drawing.SystemColors.Control
        Me.fraConnectionSettings.Controls.Add(Me.lblServerName)
        Me.fraConnectionSettings.Controls.Add(Me.chkUseIntegratedAuthentication)
        Me.fraConnectionSettings.Controls.Add(Me.txtPassword)
        Me.fraConnectionSettings.Controls.Add(Me.txtUsername)
        Me.fraConnectionSettings.Controls.Add(Me.txtServerName)
        Me.fraConnectionSettings.Controls.Add(Me.lblPassword)
        Me.fraConnectionSettings.Controls.Add(Me.lblUsername)
        Me.fraConnectionSettings.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.fraConnectionSettings.ForeColor = System.Drawing.SystemColors.ControlText
        Me.fraConnectionSettings.Location = New System.Drawing.Point(16, 15)
        Me.fraConnectionSettings.Margin = New System.Windows.Forms.Padding(4)
        Me.fraConnectionSettings.Name = "fraConnectionSettings"
        Me.fraConnectionSettings.Padding = New System.Windows.Forms.Padding(4)
        Me.fraConnectionSettings.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.fraConnectionSettings.Size = New System.Drawing.Size(356, 155)
        Me.fraConnectionSettings.TabIndex = 0
        Me.fraConnectionSettings.TabStop = False
        Me.fraConnectionSettings.Text = "Connection Settings"
        '
        'lblServerName
        '
        Me.lblServerName.BackColor = System.Drawing.SystemColors.Control
        Me.lblServerName.Cursor = System.Windows.Forms.Cursors.Default
        Me.lblServerName.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.lblServerName.ForeColor = System.Drawing.SystemColors.ControlText
        Me.lblServerName.Location = New System.Drawing.Point(13, 20)
        Me.lblServerName.Margin = New System.Windows.Forms.Padding(4, 0, 4, 0)
        Me.lblServerName.Name = "lblServerName"
        Me.lblServerName.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.lblServerName.Size = New System.Drawing.Size(171, 20)
        Me.lblServerName.TabIndex = 0
        Me.lblServerName.Text = "Server Name"
        '
        'chkUseIntegratedAuthentication
        '
        Me.chkUseIntegratedAuthentication.BackColor = System.Drawing.SystemColors.Control
        Me.chkUseIntegratedAuthentication.Checked = True
        Me.chkUseIntegratedAuthentication.CheckState = System.Windows.Forms.CheckState.Checked
        Me.chkUseIntegratedAuthentication.Cursor = System.Windows.Forms.Cursors.Default
        Me.chkUseIntegratedAuthentication.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.chkUseIntegratedAuthentication.ForeColor = System.Drawing.SystemColors.ControlText
        Me.chkUseIntegratedAuthentication.Location = New System.Drawing.Point(11, 49)
        Me.chkUseIntegratedAuthentication.Margin = New System.Windows.Forms.Padding(4)
        Me.chkUseIntegratedAuthentication.Name = "chkUseIntegratedAuthentication"
        Me.chkUseIntegratedAuthentication.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.chkUseIntegratedAuthentication.Size = New System.Drawing.Size(288, 20)
        Me.chkUseIntegratedAuthentication.TabIndex = 2
        Me.chkUseIntegratedAuthentication.Text = "Use integrated authentication"
        Me.chkUseIntegratedAuthentication.UseVisualStyleBackColor = False
        '
        'txtPassword
        '
        Me.txtPassword.AcceptsReturn = True
        Me.txtPassword.BackColor = System.Drawing.SystemColors.Window
        Me.txtPassword.Cursor = System.Windows.Forms.Cursors.IBeam
        Me.txtPassword.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.txtPassword.ForeColor = System.Drawing.SystemColors.WindowText
        Me.txtPassword.Location = New System.Drawing.Point(192, 101)
        Me.txtPassword.Margin = New System.Windows.Forms.Padding(4)
        Me.txtPassword.MaxLength = 0
        Me.txtPassword.Name = "txtPassword"
        Me.txtPassword.PasswordChar = Global.Microsoft.VisualBasic.ChrW(42)
        Me.txtPassword.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.txtPassword.Size = New System.Drawing.Size(152, 23)
        Me.txtPassword.TabIndex = 6
        Me.txtPassword.Text = "mt4fun"
        '
        'txtUsername
        '
        Me.txtUsername.AcceptsReturn = True
        Me.txtUsername.BackColor = System.Drawing.SystemColors.Window
        Me.txtUsername.Cursor = System.Windows.Forms.Cursors.IBeam
        Me.txtUsername.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.txtUsername.ForeColor = System.Drawing.SystemColors.WindowText
        Me.txtUsername.Location = New System.Drawing.Point(192, 71)
        Me.txtUsername.Margin = New System.Windows.Forms.Padding(4)
        Me.txtUsername.MaxLength = 0
        Me.txtUsername.Name = "txtUsername"
        Me.txtUsername.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.txtUsername.Size = New System.Drawing.Size(152, 23)
        Me.txtUsername.TabIndex = 4
        Me.txtUsername.Text = "mtuser"
        '
        'txtServerName
        '
        Me.txtServerName.AcceptsReturn = True
        Me.txtServerName.BackColor = System.Drawing.SystemColors.Window
        Me.txtServerName.Cursor = System.Windows.Forms.Cursors.IBeam
        Me.txtServerName.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.txtServerName.ForeColor = System.Drawing.SystemColors.WindowText
        Me.txtServerName.Location = New System.Drawing.Point(192, 17)
        Me.txtServerName.Margin = New System.Windows.Forms.Padding(4)
        Me.txtServerName.MaxLength = 0
        Me.txtServerName.Name = "txtServerName"
        Me.txtServerName.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.txtServerName.Size = New System.Drawing.Size(152, 23)
        Me.txtServerName.TabIndex = 1
        Me.txtServerName.Text = "Pogo"
        '
        'lblPassword
        '
        Me.lblPassword.BackColor = System.Drawing.SystemColors.Control
        Me.lblPassword.Cursor = System.Windows.Forms.Cursors.Default
        Me.lblPassword.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.lblPassword.ForeColor = System.Drawing.SystemColors.ControlText
        Me.lblPassword.Location = New System.Drawing.Point(11, 101)
        Me.lblPassword.Margin = New System.Windows.Forms.Padding(4, 0, 4, 0)
        Me.lblPassword.Name = "lblPassword"
        Me.lblPassword.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.lblPassword.Size = New System.Drawing.Size(96, 20)
        Me.lblPassword.TabIndex = 5
        Me.lblPassword.Text = "Password"
        '
        'lblUsername
        '
        Me.lblUsername.BackColor = System.Drawing.SystemColors.Control
        Me.lblUsername.Cursor = System.Windows.Forms.Cursors.Default
        Me.lblUsername.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.lblUsername.ForeColor = System.Drawing.SystemColors.ControlText
        Me.lblUsername.Location = New System.Drawing.Point(11, 76)
        Me.lblUsername.Margin = New System.Windows.Forms.Padding(4, 0, 4, 0)
        Me.lblUsername.Name = "lblUsername"
        Me.lblUsername.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.lblUsername.Size = New System.Drawing.Size(171, 20)
        Me.lblUsername.TabIndex = 3
        Me.lblUsername.Text = "SQL Server Username"
        '
        'mnuEditSep4
        '
        Me.mnuEditSep4.Index = 12
        Me.mnuEditSep4.Text = "-"
        '
        'mnuEdit
        '
        Me.mnuEdit.Index = 1
        Me.mnuEdit.MenuItems.AddRange(New System.Windows.Forms.MenuItem() {Me.mnuEditStart, Me.mnuEditIncludeSystemObjects, Me.mnuEditSep1, Me.mnuEditScriptObjectsThreaded, Me.mnuEditPauseAfterEachDatabase, Me.mnuEditIncludeTimestampInScriptFileHeader, Me.mnuEditSep2, Me.mnuEditIncludeTableRowCounts, Me.mnuEditAutoSelectDefaultTableNames, Me.mnuEditSep3, Me.mnuEditWarnOnHighTableRowCount, Me.mnuEditSaveDataAsInsertIntoStatements, Me.mnuEditSep4, Me.mnuEditResetOptions})
        Me.mnuEdit.Text = "&Edit"
        '
        'mnuEditIncludeSystemObjects
        '
        Me.mnuEditIncludeSystemObjects.Index = 1
        Me.mnuEditIncludeSystemObjects.Text = "Include System &Objects"
        '
        'mnuEditPauseAfterEachDatabase
        '
        Me.mnuEditPauseAfterEachDatabase.Index = 4
        Me.mnuEditPauseAfterEachDatabase.Text = "&Pause after each Database"
        '
        'mnuEditIncludeTimestampInScriptFileHeader
        '
        Me.mnuEditIncludeTimestampInScriptFileHeader.Index = 5
        Me.mnuEditIncludeTimestampInScriptFileHeader.Text = "Include Timestamp in Script File Header"
        '
        'mnuEditSep2
        '
        Me.mnuEditSep2.Index = 6
        Me.mnuEditSep2.Text = "-"
        '
        'mnuEditIncludeTableRowCounts
        '
        Me.mnuEditIncludeTableRowCounts.Checked = True
        Me.mnuEditIncludeTableRowCounts.Index = 7
        Me.mnuEditIncludeTableRowCounts.Text = "Obtain Table Row &Counts"
        '
        'mnuEditAutoSelectDefaultTableNames
        '
        Me.mnuEditAutoSelectDefaultTableNames.Checked = True
        Me.mnuEditAutoSelectDefaultTableNames.Index = 8
        Me.mnuEditAutoSelectDefaultTableNames.Text = "&Auto-select Default Table Names"
        '
        'mnuEditSep3
        '
        Me.mnuEditSep3.Index = 9
        Me.mnuEditSep3.Text = "-"
        '
        'mnuEditWarnOnHighTableRowCount
        '
        Me.mnuEditWarnOnHighTableRowCount.Checked = True
        Me.mnuEditWarnOnHighTableRowCount.Index = 10
        Me.mnuEditWarnOnHighTableRowCount.Text = "&Warn If High Table Row Count"
        '
        'mnuEditSaveDataAsInsertIntoStatements
        '
        Me.mnuEditSaveDataAsInsertIntoStatements.Checked = True
        Me.mnuEditSaveDataAsInsertIntoStatements.Index = 11
        Me.mnuEditSaveDataAsInsertIntoStatements.Text = "Save Data As &Insert Into Statements"
        '
        'mnuEditResetOptions
        '
        Me.mnuEditResetOptions.Index = 13
        Me.mnuEditResetOptions.Text = "&Reset Options to Defaults"
        '
        'lblProgress
        '
        Me.lblProgress.Anchor = CType(((System.Windows.Forms.AnchorStyles.Top Or System.Windows.Forms.AnchorStyles.Left) _
            Or System.Windows.Forms.AnchorStyles.Right), System.Windows.Forms.AnchorStyles)
        Me.lblProgress.BackColor = System.Drawing.SystemColors.Control
        Me.lblProgress.Cursor = System.Windows.Forms.Cursors.Default
        Me.lblProgress.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.lblProgress.ForeColor = System.Drawing.SystemColors.ControlText
        Me.lblProgress.Location = New System.Drawing.Point(325, 15)
        Me.lblProgress.Margin = New System.Windows.Forms.Padding(4, 0, 4, 0)
        Me.lblProgress.Name = "lblProgress"
        Me.lblProgress.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.lblProgress.Size = New System.Drawing.Size(417, 30)
        Me.lblProgress.TabIndex = 1
        '
        'mnuFileLoadOptions
        '
        Me.mnuFileLoadOptions.Index = 2
        Me.mnuFileLoadOptions.Shortcut = System.Windows.Forms.Shortcut.CtrlL
        Me.mnuFileLoadOptions.Text = "&Load Options ..."
        '
        'mnuFileSaveOptions
        '
        Me.mnuFileSaveOptions.Index = 3
        Me.mnuFileSaveOptions.Shortcut = System.Windows.Forms.Shortcut.CtrlS
        Me.mnuFileSaveOptions.Text = "&Save Options ..."
        '
        'mnuFileExit
        '
        Me.mnuFileExit.Index = 5
        Me.mnuFileExit.Text = "E&xit"
        '
        'mnuFileSep2
        '
        Me.mnuFileSep2.Index = 4
        Me.mnuFileSep2.Text = "-"
        '
        'cmdUpdateTableNames
        '
        Me.cmdUpdateTableNames.BackColor = System.Drawing.SystemColors.Control
        Me.cmdUpdateTableNames.Cursor = System.Windows.Forms.Cursors.Default
        Me.cmdUpdateTableNames.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.cmdUpdateTableNames.ForeColor = System.Drawing.SystemColors.ControlText
        Me.cmdUpdateTableNames.Location = New System.Drawing.Point(549, 70)
        Me.cmdUpdateTableNames.Margin = New System.Windows.Forms.Padding(4)
        Me.cmdUpdateTableNames.Name = "cmdUpdateTableNames"
        Me.cmdUpdateTableNames.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.cmdUpdateTableNames.Size = New System.Drawing.Size(195, 31)
        Me.cmdUpdateTableNames.TabIndex = 4
        Me.cmdUpdateTableNames.Text = "Refresh &Table Names"
        Me.cmdUpdateTableNames.UseVisualStyleBackColor = False
        '
        'chkCreateDirectoryForEachDB
        '
        Me.chkCreateDirectoryForEachDB.Anchor = CType((System.Windows.Forms.AnchorStyles.Bottom Or System.Windows.Forms.AnchorStyles.Left), System.Windows.Forms.AnchorStyles)
        Me.chkCreateDirectoryForEachDB.BackColor = System.Drawing.SystemColors.Control
        Me.chkCreateDirectoryForEachDB.Checked = True
        Me.chkCreateDirectoryForEachDB.CheckState = System.Windows.Forms.CheckState.Checked
        Me.chkCreateDirectoryForEachDB.Cursor = System.Windows.Forms.Cursors.Default
        Me.chkCreateDirectoryForEachDB.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.chkCreateDirectoryForEachDB.ForeColor = System.Drawing.SystemColors.ControlText
        Me.chkCreateDirectoryForEachDB.Location = New System.Drawing.Point(15, 377)
        Me.chkCreateDirectoryForEachDB.Margin = New System.Windows.Forms.Padding(4)
        Me.chkCreateDirectoryForEachDB.Name = "chkCreateDirectoryForEachDB"
        Me.chkCreateDirectoryForEachDB.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.chkCreateDirectoryForEachDB.Size = New System.Drawing.Size(223, 25)
        Me.chkCreateDirectoryForEachDB.TabIndex = 7
        Me.chkCreateDirectoryForEachDB.Text = "Create directory for each DB"
        Me.chkCreateDirectoryForEachDB.UseVisualStyleBackColor = False
        '
        'pbarProgress
        '
        Me.pbarProgress.Location = New System.Drawing.Point(11, 17)
        Me.pbarProgress.Margin = New System.Windows.Forms.Padding(4)
        Me.pbarProgress.Name = "pbarProgress"
        Me.pbarProgress.Size = New System.Drawing.Size(300, 25)
        Me.pbarProgress.Style = System.Windows.Forms.ProgressBarStyle.Continuous
        Me.pbarProgress.TabIndex = 0
        '
        'cmdRefreshDBList
        '
        Me.cmdRefreshDBList.BackColor = System.Drawing.SystemColors.Control
        Me.cmdRefreshDBList.Cursor = System.Windows.Forms.Cursors.Default
        Me.cmdRefreshDBList.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.cmdRefreshDBList.ForeColor = System.Drawing.SystemColors.ControlText
        Me.cmdRefreshDBList.Location = New System.Drawing.Point(8, 73)
        Me.cmdRefreshDBList.Margin = New System.Windows.Forms.Padding(4)
        Me.cmdRefreshDBList.Name = "cmdRefreshDBList"
        Me.cmdRefreshDBList.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.cmdRefreshDBList.Size = New System.Drawing.Size(195, 31)
        Me.cmdRefreshDBList.TabIndex = 2
        Me.cmdRefreshDBList.Text = "Refresh &Database List"
        Me.cmdRefreshDBList.UseVisualStyleBackColor = False
        '
        'mnuHelp
        '
        Me.mnuHelp.Index = 2
        Me.mnuHelp.MenuItems.AddRange(New System.Windows.Forms.MenuItem() {Me.mnuHelpAbout})
        Me.mnuHelp.Text = "&Help"
        '
        'mnuHelpAbout
        '
        Me.mnuHelpAbout.Index = 0
        Me.mnuHelpAbout.Text = "&About"
        '
        'fraOutputOptions
        '
        Me.fraOutputOptions.Anchor = CType((((System.Windows.Forms.AnchorStyles.Top Or System.Windows.Forms.AnchorStyles.Bottom) _
            Or System.Windows.Forms.AnchorStyles.Left) _
            Or System.Windows.Forms.AnchorStyles.Right), System.Windows.Forms.AnchorStyles)
        Me.fraOutputOptions.BackColor = System.Drawing.SystemColors.Control
        Me.fraOutputOptions.Controls.Add(Me.lblTableDataToExport)
        Me.fraOutputOptions.Controls.Add(Me.lblServerOutputDirectoryNamePrefix)
        Me.fraOutputOptions.Controls.Add(Me.txtServerOutputDirectoryNamePrefix)
        Me.fraOutputOptions.Controls.Add(Me.chkExportServerSettingsLoginsAndJobs)
        Me.fraOutputOptions.Controls.Add(Me.lblSelectDefaultDBs)
        Me.fraOutputOptions.Controls.Add(Me.cmdSelectDefaultDMSDBs)
        Me.fraOutputOptions.Controls.Add(Me.cmdSelectDefaultMTSDBs)
        Me.fraOutputOptions.Controls.Add(Me.lblOutputDirectoryNamePrefix)
        Me.fraOutputOptions.Controls.Add(Me.txtOutputDirectoryNamePrefix)
        Me.fraOutputOptions.Controls.Add(Me.cboTableNamesToExportSortOrder)
        Me.fraOutputOptions.Controls.Add(Me.cmdRefreshDBList)
        Me.fraOutputOptions.Controls.Add(Me.chkCreateDirectoryForEachDB)
        Me.fraOutputOptions.Controls.Add(Me.cmdUpdateTableNames)
        Me.fraOutputOptions.Controls.Add(Me.lstTableNamesToExportData)
        Me.fraOutputOptions.Controls.Add(Me.txtOutputDirectoryPath)
        Me.fraOutputOptions.Controls.Add(Me.lblOutputDirectoryPath)
        Me.fraOutputOptions.Controls.Add(Me.lstDatabasesToProcess)
        Me.fraOutputOptions.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.fraOutputOptions.ForeColor = System.Drawing.SystemColors.ControlText
        Me.fraOutputOptions.Location = New System.Drawing.Point(16, 177)
        Me.fraOutputOptions.Margin = New System.Windows.Forms.Padding(4)
        Me.fraOutputOptions.Name = "fraOutputOptions"
        Me.fraOutputOptions.Padding = New System.Windows.Forms.Padding(4)
        Me.fraOutputOptions.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.fraOutputOptions.Size = New System.Drawing.Size(759, 438)
        Me.fraOutputOptions.TabIndex = 3
        Me.fraOutputOptions.TabStop = False
        Me.fraOutputOptions.Text = "Output Options"
        '
        'lblTableDataToExport
        '
        Me.lblTableDataToExport.BackColor = System.Drawing.SystemColors.Control
        Me.lblTableDataToExport.Cursor = System.Windows.Forms.Cursors.Default
        Me.lblTableDataToExport.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.lblTableDataToExport.ForeColor = System.Drawing.SystemColors.ControlText
        Me.lblTableDataToExport.Location = New System.Drawing.Point(350, 77)
        Me.lblTableDataToExport.Margin = New System.Windows.Forms.Padding(4, 0, 4, 0)
        Me.lblTableDataToExport.Name = "lblTableDataToExport"
        Me.lblTableDataToExport.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.lblTableDataToExport.Size = New System.Drawing.Size(171, 20)
        Me.lblTableDataToExport.TabIndex = 7
        Me.lblTableDataToExport.Text = "Table Data to Export"
        '
        'lblServerOutputDirectoryNamePrefix
        '
        Me.lblServerOutputDirectoryNamePrefix.Anchor = CType((System.Windows.Forms.AnchorStyles.Bottom Or System.Windows.Forms.AnchorStyles.Left), System.Windows.Forms.AnchorStyles)
        Me.lblServerOutputDirectoryNamePrefix.BackColor = System.Drawing.SystemColors.Control
        Me.lblServerOutputDirectoryNamePrefix.Cursor = System.Windows.Forms.Cursors.Default
        Me.lblServerOutputDirectoryNamePrefix.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.lblServerOutputDirectoryNamePrefix.ForeColor = System.Drawing.SystemColors.ControlText
        Me.lblServerOutputDirectoryNamePrefix.Location = New System.Drawing.Point(349, 404)
        Me.lblServerOutputDirectoryNamePrefix.Margin = New System.Windows.Forms.Padding(4, 0, 4, 0)
        Me.lblServerOutputDirectoryNamePrefix.Name = "lblServerOutputDirectoryNamePrefix"
        Me.lblServerOutputDirectoryNamePrefix.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.lblServerOutputDirectoryNamePrefix.Size = New System.Drawing.Size(153, 20)
        Me.lblServerOutputDirectoryNamePrefix.TabIndex = 17
        Me.lblServerOutputDirectoryNamePrefix.Text = "Output Directory Prefix"
        '
        'txtServerOutputDirectoryNamePrefix
        '
        Me.txtServerOutputDirectoryNamePrefix.AcceptsReturn = True
        Me.txtServerOutputDirectoryNamePrefix.Anchor = CType((System.Windows.Forms.AnchorStyles.Bottom Or System.Windows.Forms.AnchorStyles.Left), System.Windows.Forms.AnchorStyles)
        Me.txtServerOutputDirectoryNamePrefix.BackColor = System.Drawing.SystemColors.Window
        Me.txtServerOutputDirectoryNamePrefix.Cursor = System.Windows.Forms.Cursors.IBeam
        Me.txtServerOutputDirectoryNamePrefix.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.txtServerOutputDirectoryNamePrefix.ForeColor = System.Drawing.SystemColors.WindowText
        Me.txtServerOutputDirectoryNamePrefix.Location = New System.Drawing.Point(516, 400)
        Me.txtServerOutputDirectoryNamePrefix.Margin = New System.Windows.Forms.Padding(4)
        Me.txtServerOutputDirectoryNamePrefix.MaxLength = 0
        Me.txtServerOutputDirectoryNamePrefix.Name = "txtServerOutputDirectoryNamePrefix"
        Me.txtServerOutputDirectoryNamePrefix.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.txtServerOutputDirectoryNamePrefix.Size = New System.Drawing.Size(155, 23)
        Me.txtServerOutputDirectoryNamePrefix.TabIndex = 18
        Me.txtServerOutputDirectoryNamePrefix.Text = "ServerSchema__"
        '
        'chkExportServerSettingsLoginsAndJobs
        '
        Me.chkExportServerSettingsLoginsAndJobs.Anchor = CType((System.Windows.Forms.AnchorStyles.Bottom Or System.Windows.Forms.AnchorStyles.Left), System.Windows.Forms.AnchorStyles)
        Me.chkExportServerSettingsLoginsAndJobs.BackColor = System.Drawing.SystemColors.Control
        Me.chkExportServerSettingsLoginsAndJobs.Checked = True
        Me.chkExportServerSettingsLoginsAndJobs.CheckState = System.Windows.Forms.CheckState.Checked
        Me.chkExportServerSettingsLoginsAndJobs.Cursor = System.Windows.Forms.Cursors.Default
        Me.chkExportServerSettingsLoginsAndJobs.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.chkExportServerSettingsLoginsAndJobs.ForeColor = System.Drawing.SystemColors.ControlText
        Me.chkExportServerSettingsLoginsAndJobs.Location = New System.Drawing.Point(352, 377)
        Me.chkExportServerSettingsLoginsAndJobs.Margin = New System.Windows.Forms.Padding(4)
        Me.chkExportServerSettingsLoginsAndJobs.Name = "chkExportServerSettingsLoginsAndJobs"
        Me.chkExportServerSettingsLoginsAndJobs.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.chkExportServerSettingsLoginsAndJobs.Size = New System.Drawing.Size(311, 25)
        Me.chkExportServerSettingsLoginsAndJobs.TabIndex = 16
        Me.chkExportServerSettingsLoginsAndJobs.Text = "Export server settings, logins, and jobs"
        Me.chkExportServerSettingsLoginsAndJobs.UseVisualStyleBackColor = False
        '
        'lblSelectDefaultDBs
        '
        Me.lblSelectDefaultDBs.Anchor = CType((System.Windows.Forms.AnchorStyles.Bottom Or System.Windows.Forms.AnchorStyles.Left), System.Windows.Forms.AnchorStyles)
        Me.lblSelectDefaultDBs.BackColor = System.Drawing.SystemColors.Control
        Me.lblSelectDefaultDBs.Cursor = System.Windows.Forms.Cursors.Default
        Me.lblSelectDefaultDBs.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.lblSelectDefaultDBs.ForeColor = System.Drawing.SystemColors.ControlText
        Me.lblSelectDefaultDBs.Location = New System.Drawing.Point(13, 342)
        Me.lblSelectDefaultDBs.Margin = New System.Windows.Forms.Padding(4, 0, 4, 0)
        Me.lblSelectDefaultDBs.Name = "lblSelectDefaultDBs"
        Me.lblSelectDefaultDBs.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.lblSelectDefaultDBs.Size = New System.Drawing.Size(108, 23)
        Me.lblSelectDefaultDBs.TabIndex = 15
        Me.lblSelectDefaultDBs.Text = "Select default:"
        '
        'cmdSelectDefaultDMSDBs
        '
        Me.cmdSelectDefaultDMSDBs.Anchor = CType((System.Windows.Forms.AnchorStyles.Bottom Or System.Windows.Forms.AnchorStyles.Left), System.Windows.Forms.AnchorStyles)
        Me.cmdSelectDefaultDMSDBs.BackColor = System.Drawing.SystemColors.Control
        Me.cmdSelectDefaultDMSDBs.Cursor = System.Windows.Forms.Cursors.Default
        Me.cmdSelectDefaultDMSDBs.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.cmdSelectDefaultDMSDBs.ForeColor = System.Drawing.SystemColors.ControlText
        Me.cmdSelectDefaultDMSDBs.Location = New System.Drawing.Point(124, 338)
        Me.cmdSelectDefaultDMSDBs.Margin = New System.Windows.Forms.Padding(4)
        Me.cmdSelectDefaultDMSDBs.Name = "cmdSelectDefaultDMSDBs"
        Me.cmdSelectDefaultDMSDBs.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.cmdSelectDefaultDMSDBs.Size = New System.Drawing.Size(100, 31)
        Me.cmdSelectDefaultDMSDBs.TabIndex = 14
        Me.cmdSelectDefaultDMSDBs.Text = "DMS DBs"
        Me.cmdSelectDefaultDMSDBs.UseVisualStyleBackColor = False
        '
        'cmdSelectDefaultMTSDBs
        '
        Me.cmdSelectDefaultMTSDBs.Anchor = CType((System.Windows.Forms.AnchorStyles.Bottom Or System.Windows.Forms.AnchorStyles.Left), System.Windows.Forms.AnchorStyles)
        Me.cmdSelectDefaultMTSDBs.BackColor = System.Drawing.SystemColors.Control
        Me.cmdSelectDefaultMTSDBs.Cursor = System.Windows.Forms.Cursors.Default
        Me.cmdSelectDefaultMTSDBs.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.cmdSelectDefaultMTSDBs.ForeColor = System.Drawing.SystemColors.ControlText
        Me.cmdSelectDefaultMTSDBs.Location = New System.Drawing.Point(235, 338)
        Me.cmdSelectDefaultMTSDBs.Margin = New System.Windows.Forms.Padding(4)
        Me.cmdSelectDefaultMTSDBs.Name = "cmdSelectDefaultMTSDBs"
        Me.cmdSelectDefaultMTSDBs.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.cmdSelectDefaultMTSDBs.Size = New System.Drawing.Size(100, 31)
        Me.cmdSelectDefaultMTSDBs.TabIndex = 13
        Me.cmdSelectDefaultMTSDBs.Text = "MTS DBs"
        Me.cmdSelectDefaultMTSDBs.UseVisualStyleBackColor = False
        '
        'lblOutputDirectoryNamePrefix
        '
        Me.lblOutputDirectoryNamePrefix.Anchor = CType((System.Windows.Forms.AnchorStyles.Bottom Or System.Windows.Forms.AnchorStyles.Left), System.Windows.Forms.AnchorStyles)
        Me.lblOutputDirectoryNamePrefix.BackColor = System.Drawing.SystemColors.Control
        Me.lblOutputDirectoryNamePrefix.Cursor = System.Windows.Forms.Cursors.Default
        Me.lblOutputDirectoryNamePrefix.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.lblOutputDirectoryNamePrefix.ForeColor = System.Drawing.SystemColors.ControlText
        Me.lblOutputDirectoryNamePrefix.Location = New System.Drawing.Point(12, 407)
        Me.lblOutputDirectoryNamePrefix.Margin = New System.Windows.Forms.Padding(4, 0, 4, 0)
        Me.lblOutputDirectoryNamePrefix.Name = "lblOutputDirectoryNamePrefix"
        Me.lblOutputDirectoryNamePrefix.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.lblOutputDirectoryNamePrefix.Size = New System.Drawing.Size(153, 20)
        Me.lblOutputDirectoryNamePrefix.TabIndex = 8
        Me.lblOutputDirectoryNamePrefix.Text = "Output Directory Prefix"
        '
        'txtOutputDirectoryNamePrefix
        '
        Me.txtOutputDirectoryNamePrefix.AcceptsReturn = True
        Me.txtOutputDirectoryNamePrefix.Anchor = CType((System.Windows.Forms.AnchorStyles.Bottom Or System.Windows.Forms.AnchorStyles.Left), System.Windows.Forms.AnchorStyles)
        Me.txtOutputDirectoryNamePrefix.BackColor = System.Drawing.SystemColors.Window
        Me.txtOutputDirectoryNamePrefix.Cursor = System.Windows.Forms.Cursors.IBeam
        Me.txtOutputDirectoryNamePrefix.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.txtOutputDirectoryNamePrefix.ForeColor = System.Drawing.SystemColors.WindowText
        Me.txtOutputDirectoryNamePrefix.Location = New System.Drawing.Point(179, 404)
        Me.txtOutputDirectoryNamePrefix.Margin = New System.Windows.Forms.Padding(4)
        Me.txtOutputDirectoryNamePrefix.MaxLength = 0
        Me.txtOutputDirectoryNamePrefix.Name = "txtOutputDirectoryNamePrefix"
        Me.txtOutputDirectoryNamePrefix.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.txtOutputDirectoryNamePrefix.Size = New System.Drawing.Size(155, 23)
        Me.txtOutputDirectoryNamePrefix.TabIndex = 9
        Me.txtOutputDirectoryNamePrefix.Text = "DBSchema__"
        '
        'cboTableNamesToExportSortOrder
        '
        Me.cboTableNamesToExportSortOrder.Anchor = CType((System.Windows.Forms.AnchorStyles.Bottom Or System.Windows.Forms.AnchorStyles.Left), System.Windows.Forms.AnchorStyles)
        Me.cboTableNamesToExportSortOrder.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList
        Me.cboTableNamesToExportSortOrder.FormattingEnabled = True
        Me.cboTableNamesToExportSortOrder.Location = New System.Drawing.Point(353, 338)
        Me.cboTableNamesToExportSortOrder.Margin = New System.Windows.Forms.Padding(4)
        Me.cboTableNamesToExportSortOrder.Name = "cboTableNamesToExportSortOrder"
        Me.cboTableNamesToExportSortOrder.Size = New System.Drawing.Size(211, 24)
        Me.cboTableNamesToExportSortOrder.TabIndex = 6
        '
        'lstTableNamesToExportData
        '
        Me.lstTableNamesToExportData.Anchor = CType((((System.Windows.Forms.AnchorStyles.Top Or System.Windows.Forms.AnchorStyles.Bottom) _
            Or System.Windows.Forms.AnchorStyles.Left) _
            Or System.Windows.Forms.AnchorStyles.Right), System.Windows.Forms.AnchorStyles)
        Me.lstTableNamesToExportData.ItemHeight = 16
        Me.lstTableNamesToExportData.Location = New System.Drawing.Point(352, 112)
        Me.lstTableNamesToExportData.Margin = New System.Windows.Forms.Padding(4)
        Me.lstTableNamesToExportData.Name = "lstTableNamesToExportData"
        Me.lstTableNamesToExportData.SelectionMode = System.Windows.Forms.SelectionMode.MultiExtended
        Me.lstTableNamesToExportData.Size = New System.Drawing.Size(394, 196)
        Me.lstTableNamesToExportData.TabIndex = 5
        '
        'txtOutputDirectoryPath
        '
        Me.txtOutputDirectoryPath.AcceptsReturn = True
        Me.txtOutputDirectoryPath.Anchor = CType(((System.Windows.Forms.AnchorStyles.Top Or System.Windows.Forms.AnchorStyles.Left) _
            Or System.Windows.Forms.AnchorStyles.Right), System.Windows.Forms.AnchorStyles)
        Me.txtOutputDirectoryPath.BackColor = System.Drawing.SystemColors.Window
        Me.txtOutputDirectoryPath.Cursor = System.Windows.Forms.Cursors.IBeam
        Me.txtOutputDirectoryPath.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.txtOutputDirectoryPath.ForeColor = System.Drawing.SystemColors.WindowText
        Me.txtOutputDirectoryPath.Location = New System.Drawing.Point(11, 39)
        Me.txtOutputDirectoryPath.Margin = New System.Windows.Forms.Padding(4)
        Me.txtOutputDirectoryPath.MaxLength = 0
        Me.txtOutputDirectoryPath.Name = "txtOutputDirectoryPath"
        Me.txtOutputDirectoryPath.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.txtOutputDirectoryPath.Size = New System.Drawing.Size(735, 23)
        Me.txtOutputDirectoryPath.TabIndex = 1
        Me.txtOutputDirectoryPath.Text = "C:\Temp\"
        '
        'lblOutputDirectoryPath
        '
        Me.lblOutputDirectoryPath.BackColor = System.Drawing.SystemColors.Control
        Me.lblOutputDirectoryPath.Cursor = System.Windows.Forms.Cursors.Default
        Me.lblOutputDirectoryPath.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.lblOutputDirectoryPath.ForeColor = System.Drawing.SystemColors.ControlText
        Me.lblOutputDirectoryPath.Location = New System.Drawing.Point(11, 20)
        Me.lblOutputDirectoryPath.Margin = New System.Windows.Forms.Padding(4, 0, 4, 0)
        Me.lblOutputDirectoryPath.Name = "lblOutputDirectoryPath"
        Me.lblOutputDirectoryPath.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.lblOutputDirectoryPath.Size = New System.Drawing.Size(183, 21)
        Me.lblOutputDirectoryPath.TabIndex = 0
        Me.lblOutputDirectoryPath.Text = "Output Directory Path"
        '
        'lstDatabasesToProcess
        '
        Me.lstDatabasesToProcess.Anchor = CType(((System.Windows.Forms.AnchorStyles.Top Or System.Windows.Forms.AnchorStyles.Bottom) _
            Or System.Windows.Forms.AnchorStyles.Left), System.Windows.Forms.AnchorStyles)
        Me.lstDatabasesToProcess.ItemHeight = 16
        Me.lstDatabasesToProcess.Location = New System.Drawing.Point(12, 112)
        Me.lstDatabasesToProcess.Margin = New System.Windows.Forms.Padding(4)
        Me.lstDatabasesToProcess.Name = "lstDatabasesToProcess"
        Me.lstDatabasesToProcess.SelectionMode = System.Windows.Forms.SelectionMode.MultiExtended
        Me.lstDatabasesToProcess.Size = New System.Drawing.Size(321, 196)
        Me.lstDatabasesToProcess.TabIndex = 3
        '
        'mnuFileSep1
        '
        Me.mnuFileSep1.Index = 1
        Me.mnuFileSep1.Text = "-"
        '
        'lblSubtaskProgress
        '
        Me.lblSubtaskProgress.Anchor = CType(((System.Windows.Forms.AnchorStyles.Top Or System.Windows.Forms.AnchorStyles.Left) _
            Or System.Windows.Forms.AnchorStyles.Right), System.Windows.Forms.AnchorStyles)
        Me.lblSubtaskProgress.BackColor = System.Drawing.SystemColors.Control
        Me.lblSubtaskProgress.Cursor = System.Windows.Forms.Cursors.Default
        Me.lblSubtaskProgress.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.lblSubtaskProgress.ForeColor = System.Drawing.SystemColors.ControlText
        Me.lblSubtaskProgress.Location = New System.Drawing.Point(325, 54)
        Me.lblSubtaskProgress.Margin = New System.Windows.Forms.Padding(4, 0, 4, 0)
        Me.lblSubtaskProgress.Name = "lblSubtaskProgress"
        Me.lblSubtaskProgress.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.lblSubtaskProgress.Size = New System.Drawing.Size(417, 30)
        Me.lblSubtaskProgress.TabIndex = 3
        '
        'fraStatus
        '
        Me.fraStatus.Anchor = CType(((System.Windows.Forms.AnchorStyles.Bottom Or System.Windows.Forms.AnchorStyles.Left) _
            Or System.Windows.Forms.AnchorStyles.Right), System.Windows.Forms.AnchorStyles)
        Me.fraStatus.BackColor = System.Drawing.SystemColors.Control
        Me.fraStatus.Controls.Add(Me.lblProgress)
        Me.fraStatus.Controls.Add(Me.pbarProgress)
        Me.fraStatus.Controls.Add(Me.lblSubtaskProgress)
        Me.fraStatus.Controls.Add(Me.pbarSubtaskProgress)
        Me.fraStatus.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.fraStatus.ForeColor = System.Drawing.SystemColors.ControlText
        Me.fraStatus.Location = New System.Drawing.Point(16, 617)
        Me.fraStatus.Margin = New System.Windows.Forms.Padding(4)
        Me.fraStatus.Name = "fraStatus"
        Me.fraStatus.Padding = New System.Windows.Forms.Padding(4)
        Me.fraStatus.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.fraStatus.Size = New System.Drawing.Size(758, 96)
        Me.fraStatus.TabIndex = 4
        Me.fraStatus.TabStop = False
        '
        'pbarSubtaskProgress
        '
        Me.pbarSubtaskProgress.Location = New System.Drawing.Point(11, 57)
        Me.pbarSubtaskProgress.Margin = New System.Windows.Forms.Padding(4)
        Me.pbarSubtaskProgress.Name = "pbarSubtaskProgress"
        Me.pbarSubtaskProgress.Size = New System.Drawing.Size(300, 25)
        Me.pbarSubtaskProgress.Style = System.Windows.Forms.ProgressBarStyle.Continuous
        Me.pbarSubtaskProgress.TabIndex = 2
        '
        'lblMessage
        '
        Me.lblMessage.Anchor = CType(((System.Windows.Forms.AnchorStyles.Top Or System.Windows.Forms.AnchorStyles.Left) _
            Or System.Windows.Forms.AnchorStyles.Right), System.Windows.Forms.AnchorStyles)
        Me.lblMessage.BackColor = System.Drawing.SystemColors.Control
        Me.lblMessage.Cursor = System.Windows.Forms.Cursors.Default
        Me.lblMessage.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.lblMessage.ForeColor = System.Drawing.SystemColors.ControlText
        Me.lblMessage.Location = New System.Drawing.Point(8, 105)
        Me.lblMessage.Margin = New System.Windows.Forms.Padding(4, 0, 4, 0)
        Me.lblMessage.Name = "lblMessage"
        Me.lblMessage.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.lblMessage.Size = New System.Drawing.Size(112, 21)
        Me.lblMessage.TabIndex = 2
        Me.lblMessage.Visible = False
        '
        'MainMenuControl
        '
        Me.MainMenuControl.MenuItems.AddRange(New System.Windows.Forms.MenuItem() {Me.mnuFile, Me.mnuEdit, Me.mnuHelp})
        '
        'mnuFile
        '
        Me.mnuFile.Index = 0
        Me.mnuFile.MenuItems.AddRange(New System.Windows.Forms.MenuItem() {Me.mnuFileSelectOutputDirectory, Me.mnuFileSep1, Me.mnuFileLoadOptions, Me.mnuFileSaveOptions, Me.mnuFileSep2, Me.mnuFileExit})
        Me.mnuFile.Text = "&File"
        '
        'mnuFileSelectOutputDirectory
        '
        Me.mnuFileSelectOutputDirectory.Index = 0
        Me.mnuFileSelectOutputDirectory.Shortcut = System.Windows.Forms.Shortcut.CtrlO
        Me.mnuFileSelectOutputDirectory.Text = "Select &Output Directory ..."
        '
        'fraControls
        '
        Me.fraControls.Controls.Add(Me.lblMessage)
        Me.fraControls.Controls.Add(Me.cmdGo)
        Me.fraControls.Controls.Add(Me.cmdExit)
        Me.fraControls.Controls.Add(Me.cmdAbort)
        Me.fraControls.Controls.Add(Me.cmdPauseUnpause)
        Me.fraControls.Font = New System.Drawing.Font("Arial", 8.25!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.fraControls.ForeColor = System.Drawing.SystemColors.ControlText
        Me.fraControls.Location = New System.Drawing.Point(640, 15)
        Me.fraControls.Margin = New System.Windows.Forms.Padding(4)
        Me.fraControls.Name = "fraControls"
        Me.fraControls.Padding = New System.Windows.Forms.Padding(4)
        Me.fraControls.Size = New System.Drawing.Size(128, 155)
        Me.fraControls.TabIndex = 2
        Me.fraControls.TabStop = False
        Me.fraControls.Text = "Controls"
        '
        'cmdGo
        '
        Me.cmdGo.BackColor = System.Drawing.SystemColors.Control
        Me.cmdGo.Cursor = System.Windows.Forms.Cursors.Default
        Me.cmdGo.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.cmdGo.ForeColor = System.Drawing.SystemColors.ControlText
        Me.cmdGo.Location = New System.Drawing.Point(8, 20)
        Me.cmdGo.Margin = New System.Windows.Forms.Padding(4)
        Me.cmdGo.Name = "cmdGo"
        Me.cmdGo.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.cmdGo.Size = New System.Drawing.Size(108, 31)
        Me.cmdGo.TabIndex = 0
        Me.cmdGo.Text = "&Go"
        Me.cmdGo.UseVisualStyleBackColor = False
        '
        'cmdExit
        '
        Me.cmdExit.BackColor = System.Drawing.SystemColors.Control
        Me.cmdExit.Cursor = System.Windows.Forms.Cursors.Default
        Me.cmdExit.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.cmdExit.ForeColor = System.Drawing.SystemColors.ControlText
        Me.cmdExit.Location = New System.Drawing.Point(8, 59)
        Me.cmdExit.Margin = New System.Windows.Forms.Padding(4)
        Me.cmdExit.Name = "cmdExit"
        Me.cmdExit.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.cmdExit.Size = New System.Drawing.Size(108, 31)
        Me.cmdExit.TabIndex = 1
        Me.cmdExit.Text = "E&xit"
        Me.cmdExit.UseVisualStyleBackColor = False
        '
        'cmdAbort
        '
        Me.cmdAbort.BackColor = System.Drawing.SystemColors.Control
        Me.cmdAbort.Cursor = System.Windows.Forms.Cursors.Default
        Me.cmdAbort.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.cmdAbort.ForeColor = System.Drawing.SystemColors.ControlText
        Me.cmdAbort.Location = New System.Drawing.Point(8, 59)
        Me.cmdAbort.Margin = New System.Windows.Forms.Padding(4)
        Me.cmdAbort.Name = "cmdAbort"
        Me.cmdAbort.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.cmdAbort.Size = New System.Drawing.Size(108, 31)
        Me.cmdAbort.TabIndex = 5
        Me.cmdAbort.Text = "&Abort"
        Me.cmdAbort.UseVisualStyleBackColor = False
        '
        'cmdPauseUnpause
        '
        Me.cmdPauseUnpause.BackColor = System.Drawing.SystemColors.Control
        Me.cmdPauseUnpause.Cursor = System.Windows.Forms.Cursors.Default
        Me.cmdPauseUnpause.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.cmdPauseUnpause.ForeColor = System.Drawing.SystemColors.ControlText
        Me.cmdPauseUnpause.Location = New System.Drawing.Point(8, 20)
        Me.cmdPauseUnpause.Margin = New System.Windows.Forms.Padding(4)
        Me.cmdPauseUnpause.Name = "cmdPauseUnpause"
        Me.cmdPauseUnpause.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.cmdPauseUnpause.Size = New System.Drawing.Size(108, 31)
        Me.cmdPauseUnpause.TabIndex = 4
        Me.cmdPauseUnpause.Text = "&Pause"
        Me.cmdPauseUnpause.UseVisualStyleBackColor = False
        '
        'lstObjectTypesToScript
        '
        Me.lstObjectTypesToScript.Anchor = CType(((System.Windows.Forms.AnchorStyles.Top Or System.Windows.Forms.AnchorStyles.Bottom) _
            Or System.Windows.Forms.AnchorStyles.Left), System.Windows.Forms.AnchorStyles)
        Me.lstObjectTypesToScript.ItemHeight = 16
        Me.lstObjectTypesToScript.Location = New System.Drawing.Point(8, 23)
        Me.lstObjectTypesToScript.Margin = New System.Windows.Forms.Padding(4)
        Me.lstObjectTypesToScript.Name = "lstObjectTypesToScript"
        Me.lstObjectTypesToScript.SelectionMode = System.Windows.Forms.SelectionMode.MultiExtended
        Me.lstObjectTypesToScript.Size = New System.Drawing.Size(223, 116)
        Me.lstObjectTypesToScript.TabIndex = 0
        '
        'fraObjectTypesToScript
        '
        Me.fraObjectTypesToScript.Controls.Add(Me.lstObjectTypesToScript)
        Me.fraObjectTypesToScript.Font = New System.Drawing.Font("Arial", 8.25!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.fraObjectTypesToScript.ForeColor = System.Drawing.SystemColors.ControlText
        Me.fraObjectTypesToScript.Location = New System.Drawing.Point(392, 15)
        Me.fraObjectTypesToScript.Margin = New System.Windows.Forms.Padding(4)
        Me.fraObjectTypesToScript.Name = "fraObjectTypesToScript"
        Me.fraObjectTypesToScript.Padding = New System.Windows.Forms.Padding(4)
        Me.fraObjectTypesToScript.Size = New System.Drawing.Size(240, 155)
        Me.fraObjectTypesToScript.TabIndex = 1
        Me.fraObjectTypesToScript.TabStop = False
        Me.fraObjectTypesToScript.Text = "Objects to Script"
        '
        'frmMain
        '
        Me.AutoScaleDimensions = New System.Drawing.SizeF(8.0!, 16.0!)
        Me.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font
        Me.ClientSize = New System.Drawing.Size(791, 721)
        Me.Controls.Add(Me.fraObjectTypesToScript)
        Me.Controls.Add(Me.fraControls)
        Me.Controls.Add(Me.fraConnectionSettings)
        Me.Controls.Add(Me.fraOutputOptions)
        Me.Controls.Add(Me.fraStatus)
        Me.Margin = New System.Windows.Forms.Padding(4)
        Me.Menu = Me.MainMenuControl
        Me.Name = "frmMain"
        Me.Text = "DB Schema Export Tool"
        Me.fraConnectionSettings.ResumeLayout(False)
        Me.fraConnectionSettings.PerformLayout()
        Me.fraOutputOptions.ResumeLayout(False)
        Me.fraOutputOptions.PerformLayout()
        Me.fraStatus.ResumeLayout(False)
        Me.fraControls.ResumeLayout(False)
        Me.fraObjectTypesToScript.ResumeLayout(False)
        Me.ResumeLayout(False)

    End Sub
    Friend WithEvents mnuEditSep1 As System.Windows.Forms.MenuItem
    Friend WithEvents mnuEditStart As System.Windows.Forms.MenuItem
    Friend WithEvents mnuEditScriptObjectsThreaded As System.Windows.Forms.MenuItem
    Friend WithEvents fraConnectionSettings As System.Windows.Forms.GroupBox
    Friend WithEvents lblServerName As System.Windows.Forms.Label
    Friend WithEvents chkUseIntegratedAuthentication As System.Windows.Forms.CheckBox
    Friend WithEvents txtPassword As System.Windows.Forms.TextBox
    Friend WithEvents txtUsername As System.Windows.Forms.TextBox
    Friend WithEvents txtServerName As System.Windows.Forms.TextBox
    Friend WithEvents lblPassword As System.Windows.Forms.Label
    Friend WithEvents lblUsername As System.Windows.Forms.Label
    Friend WithEvents mnuEditSep4 As System.Windows.Forms.MenuItem
    Friend WithEvents mnuEdit As System.Windows.Forms.MenuItem
    Friend WithEvents mnuEditResetOptions As System.Windows.Forms.MenuItem
    Friend WithEvents lblProgress As System.Windows.Forms.Label
    Friend WithEvents mnuFileLoadOptions As System.Windows.Forms.MenuItem
    Friend WithEvents mnuFileSaveOptions As System.Windows.Forms.MenuItem
    Friend WithEvents mnuFileExit As System.Windows.Forms.MenuItem
    Friend WithEvents mnuFileSep2 As System.Windows.Forms.MenuItem
    Friend WithEvents cmdUpdateTableNames As System.Windows.Forms.Button
    Friend WithEvents chkCreateDirectoryForEachDB As System.Windows.Forms.CheckBox
    Friend WithEvents pbarProgress As System.Windows.Forms.ProgressBar
    Friend WithEvents cmdRefreshDBList As System.Windows.Forms.Button
    Friend WithEvents mnuHelp As System.Windows.Forms.MenuItem
    Friend WithEvents mnuHelpAbout As System.Windows.Forms.MenuItem
    Friend WithEvents fraOutputOptions As System.Windows.Forms.GroupBox
    Friend WithEvents txtOutputDirectoryPath As System.Windows.Forms.TextBox
    Friend WithEvents lblOutputDirectoryPath As System.Windows.Forms.Label
    Friend WithEvents lstDatabasesToProcess As System.Windows.Forms.ListBox
    Friend WithEvents mnuFileSep1 As System.Windows.Forms.MenuItem
    Friend WithEvents lblSubtaskProgress As System.Windows.Forms.Label
    Friend WithEvents fraStatus As System.Windows.Forms.GroupBox
    Friend WithEvents pbarSubtaskProgress As System.Windows.Forms.ProgressBar
    Friend WithEvents MainMenuControl As System.Windows.Forms.MainMenu
    Friend WithEvents mnuFile As System.Windows.Forms.MenuItem
    Friend WithEvents mnuFileSelectOutputDirectory As System.Windows.Forms.MenuItem
    Friend WithEvents mnuEditPauseAfterEachDatabase As System.Windows.Forms.MenuItem
    Friend WithEvents fraControls As System.Windows.Forms.GroupBox
    Friend WithEvents lblMessage As System.Windows.Forms.Label
    Friend WithEvents cmdGo As System.Windows.Forms.Button
    Friend WithEvents cmdExit As System.Windows.Forms.Button
    Friend WithEvents cmdAbort As System.Windows.Forms.Button
    Friend WithEvents cmdPauseUnpause As System.Windows.Forms.Button
    Friend WithEvents cboTableNamesToExportSortOrder As System.Windows.Forms.ComboBox
    Friend WithEvents mnuEditSep2 As System.Windows.Forms.MenuItem
    Friend WithEvents mnuEditIncludeTableRowCounts As System.Windows.Forms.MenuItem
    Friend WithEvents mnuEditAutoSelectDefaultTableNames As System.Windows.Forms.MenuItem
    Friend WithEvents lstTableNamesToExportData As System.Windows.Forms.ListBox
    Friend WithEvents mnuEditIncludeSystemObjects As System.Windows.Forms.MenuItem
    Friend WithEvents lstObjectTypesToScript As System.Windows.Forms.ListBox
    Friend WithEvents fraObjectTypesToScript As System.Windows.Forms.GroupBox
    Friend WithEvents mnuEditSaveDataAsInsertIntoStatements As System.Windows.Forms.MenuItem
    Friend WithEvents mnuEditWarnOnHighTableRowCount As System.Windows.Forms.MenuItem
    Friend WithEvents lblOutputDirectoryNamePrefix As System.Windows.Forms.Label
    Friend WithEvents txtOutputDirectoryNamePrefix As System.Windows.Forms.TextBox
    Friend WithEvents mnuEditSep3 As System.Windows.Forms.MenuItem
    Friend WithEvents mnuEditIncludeTimestampInScriptFileHeader As System.Windows.Forms.MenuItem
    Friend WithEvents lblSelectDefaultDBs As System.Windows.Forms.Label
    Friend WithEvents cmdSelectDefaultDMSDBs As System.Windows.Forms.Button
    Friend WithEvents cmdSelectDefaultMTSDBs As System.Windows.Forms.Button
    Friend WithEvents chkExportServerSettingsLoginsAndJobs As System.Windows.Forms.CheckBox
    Friend WithEvents lblServerOutputDirectoryNamePrefix As System.Windows.Forms.Label
    Friend WithEvents txtServerOutputDirectoryNamePrefix As System.Windows.Forms.TextBox
    Friend WithEvents lblTableDataToExport As Label
End Class
