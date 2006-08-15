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
        Me.components = New System.ComponentModel.Container
        Me.mnuEditSep1 = New System.Windows.Forms.MenuItem
        Me.mnuEditStart = New System.Windows.Forms.MenuItem
        Me.mnuEditScriptObjectsThreaded = New System.Windows.Forms.MenuItem
        Me.fraConnectionSettings = New System.Windows.Forms.GroupBox
        Me.lblServerName = New System.Windows.Forms.Label
        Me.chkUseIntegratedAuthentication = New System.Windows.Forms.CheckBox
        Me.txtPassword = New System.Windows.Forms.TextBox
        Me.txtUsername = New System.Windows.Forms.TextBox
        Me.txtServerName = New System.Windows.Forms.TextBox
        Me.lblPassword = New System.Windows.Forms.Label
        Me.lblUsername = New System.Windows.Forms.Label
        Me.mnuEditSep4 = New System.Windows.Forms.MenuItem
        Me.mnuEdit = New System.Windows.Forms.MenuItem
        Me.mnuEditIncludeSystemObjects = New System.Windows.Forms.MenuItem
        Me.mnuEditPauseAfterEachDatabase = New System.Windows.Forms.MenuItem
        Me.mnuEditIncludeTimestampInScriptFileHeader = New System.Windows.Forms.MenuItem
        Me.mnuEditSep2 = New System.Windows.Forms.MenuItem
        Me.mnuEditIncludeTableRowCounts = New System.Windows.Forms.MenuItem
        Me.mnuEditAutoSelectDefaultTableNames = New System.Windows.Forms.MenuItem
        Me.mnuEditSep3 = New System.Windows.Forms.MenuItem
        Me.mnuEditWarnOnHighTableRowCount = New System.Windows.Forms.MenuItem
        Me.mnuEditSaveDataAsInsertIntoStatements = New System.Windows.Forms.MenuItem
        Me.mnuEditResetOptions = New System.Windows.Forms.MenuItem
        Me.lblProgress = New System.Windows.Forms.Label
        Me.mnuFileLoadOptions = New System.Windows.Forms.MenuItem
        Me.mnuFileSaveOptions = New System.Windows.Forms.MenuItem
        Me.mnuFileExit = New System.Windows.Forms.MenuItem
        Me.mnuFileSep2 = New System.Windows.Forms.MenuItem
        Me.cmdUpdateTableNames = New System.Windows.Forms.Button
        Me.chkCreateFolderForEachDB = New System.Windows.Forms.CheckBox
        Me.pbarProgress = New SmoothProgressBar.SmoothProgressBar
        Me.cmdRefreshDBList = New System.Windows.Forms.Button
        Me.mnuHelp = New System.Windows.Forms.MenuItem
        Me.mnuHelpAbout = New System.Windows.Forms.MenuItem
        Me.fraOutputOptions = New System.Windows.Forms.GroupBox
        Me.lblSelectDefaultDBs = New System.Windows.Forms.Label
        Me.cmdSelectDefaultDMSDBs = New System.Windows.Forms.Button
        Me.cmdSelectDefaultMTSDBs = New System.Windows.Forms.Button
        Me.lblOutputFolderNamePrefix = New System.Windows.Forms.Label
        Me.txtOutputFolderNamePrefix = New System.Windows.Forms.TextBox
        Me.cboTableNamesToExportSortOrder = New System.Windows.Forms.ComboBox
        Me.lstTableNamesToExportData = New System.Windows.Forms.ListBox
        Me.txtOutputFolderPath = New System.Windows.Forms.TextBox
        Me.lblOutputFolderPath = New System.Windows.Forms.Label
        Me.lstDatabasesToProcess = New System.Windows.Forms.ListBox
        Me.mnuFileSep1 = New System.Windows.Forms.MenuItem
        Me.lblSubtaskProgress = New System.Windows.Forms.Label
        Me.fraStatus = New System.Windows.Forms.GroupBox
        Me.pbarSubtaskProgress = New SmoothProgressBar.SmoothProgressBar
        Me.lblMessage = New System.Windows.Forms.Label
        Me.MainMenuControl = New System.Windows.Forms.MainMenu(Me.components)
        Me.mnuFile = New System.Windows.Forms.MenuItem
        Me.mnuFileSelectOutputFolder = New System.Windows.Forms.MenuItem
        Me.fraControls = New System.Windows.Forms.GroupBox
        Me.cmdGo = New System.Windows.Forms.Button
        Me.cmdExit = New System.Windows.Forms.Button
        Me.cmdAbort = New System.Windows.Forms.Button
        Me.cmdPauseUnpause = New System.Windows.Forms.Button
        Me.lstObjectTypesToScript = New System.Windows.Forms.ListBox
        Me.fraObjectTypesToScript = New System.Windows.Forms.GroupBox
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
        Me.fraConnectionSettings.Location = New System.Drawing.Point(12, 12)
        Me.fraConnectionSettings.Name = "fraConnectionSettings"
        Me.fraConnectionSettings.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.fraConnectionSettings.Size = New System.Drawing.Size(267, 116)
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
        Me.lblServerName.Location = New System.Drawing.Point(10, 16)
        Me.lblServerName.Name = "lblServerName"
        Me.lblServerName.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.lblServerName.Size = New System.Drawing.Size(128, 16)
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
        Me.chkUseIntegratedAuthentication.Location = New System.Drawing.Point(8, 40)
        Me.chkUseIntegratedAuthentication.Name = "chkUseIntegratedAuthentication"
        Me.chkUseIntegratedAuthentication.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.chkUseIntegratedAuthentication.Size = New System.Drawing.Size(216, 16)
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
        Me.txtPassword.Location = New System.Drawing.Point(144, 82)
        Me.txtPassword.MaxLength = 0
        Me.txtPassword.Name = "txtPassword"
        Me.txtPassword.PasswordChar = Global.Microsoft.VisualBasic.ChrW(42)
        Me.txtPassword.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.txtPassword.Size = New System.Drawing.Size(115, 20)
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
        Me.txtUsername.Location = New System.Drawing.Point(144, 58)
        Me.txtUsername.MaxLength = 0
        Me.txtUsername.Name = "txtUsername"
        Me.txtUsername.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.txtUsername.Size = New System.Drawing.Size(115, 20)
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
        Me.txtServerName.Location = New System.Drawing.Point(144, 14)
        Me.txtServerName.MaxLength = 0
        Me.txtServerName.Name = "txtServerName"
        Me.txtServerName.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.txtServerName.Size = New System.Drawing.Size(115, 20)
        Me.txtServerName.TabIndex = 1
        Me.txtServerName.Text = "Pogo"
        '
        'lblPassword
        '
        Me.lblPassword.BackColor = System.Drawing.SystemColors.Control
        Me.lblPassword.Cursor = System.Windows.Forms.Cursors.Default
        Me.lblPassword.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.lblPassword.ForeColor = System.Drawing.SystemColors.ControlText
        Me.lblPassword.Location = New System.Drawing.Point(8, 82)
        Me.lblPassword.Name = "lblPassword"
        Me.lblPassword.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.lblPassword.Size = New System.Drawing.Size(72, 16)
        Me.lblPassword.TabIndex = 5
        Me.lblPassword.Text = "Password"
        '
        'lblUsername
        '
        Me.lblUsername.BackColor = System.Drawing.SystemColors.Control
        Me.lblUsername.Cursor = System.Windows.Forms.Cursors.Default
        Me.lblUsername.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.lblUsername.ForeColor = System.Drawing.SystemColors.ControlText
        Me.lblUsername.Location = New System.Drawing.Point(8, 62)
        Me.lblUsername.Name = "lblUsername"
        Me.lblUsername.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.lblUsername.Size = New System.Drawing.Size(128, 16)
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
        Me.lblProgress.Location = New System.Drawing.Point(244, 12)
        Me.lblProgress.Name = "lblProgress"
        Me.lblProgress.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.lblProgress.Size = New System.Drawing.Size(311, 24)
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
        Me.cmdUpdateTableNames.Location = New System.Drawing.Point(264, 59)
        Me.cmdUpdateTableNames.Name = "cmdUpdateTableNames"
        Me.cmdUpdateTableNames.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.cmdUpdateTableNames.Size = New System.Drawing.Size(146, 25)
        Me.cmdUpdateTableNames.TabIndex = 4
        Me.cmdUpdateTableNames.Text = "Refresh &Table Names"
        Me.cmdUpdateTableNames.UseVisualStyleBackColor = False
        '
        'chkCreateFolderForEachDB
        '
        Me.chkCreateFolderForEachDB.Anchor = CType((System.Windows.Forms.AnchorStyles.Bottom Or System.Windows.Forms.AnchorStyles.Left), System.Windows.Forms.AnchorStyles)
        Me.chkCreateFolderForEachDB.BackColor = System.Drawing.SystemColors.Control
        Me.chkCreateFolderForEachDB.Checked = True
        Me.chkCreateFolderForEachDB.CheckState = System.Windows.Forms.CheckState.Checked
        Me.chkCreateFolderForEachDB.Cursor = System.Windows.Forms.Cursors.Default
        Me.chkCreateFolderForEachDB.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.chkCreateFolderForEachDB.ForeColor = System.Drawing.SystemColors.ControlText
        Me.chkCreateFolderForEachDB.Location = New System.Drawing.Point(6, 281)
        Me.chkCreateFolderForEachDB.Name = "chkCreateFolderForEachDB"
        Me.chkCreateFolderForEachDB.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.chkCreateFolderForEachDB.Size = New System.Drawing.Size(167, 16)
        Me.chkCreateFolderForEachDB.TabIndex = 7
        Me.chkCreateFolderForEachDB.Text = "Create folder for each DB"
        Me.chkCreateFolderForEachDB.UseVisualStyleBackColor = False
        '
        'pbarProgress
        '
        Me.pbarProgress.Location = New System.Drawing.Point(8, 14)
        Me.pbarProgress.Maximum = 100
        Me.pbarProgress.Minimum = 0
        Me.pbarProgress.Name = "pbarProgress"
        Me.pbarProgress.Size = New System.Drawing.Size(225, 20)
        Me.pbarProgress.TabIndex = 0
        Me.pbarProgress.Value = 0
        '
        'cmdRefreshDBList
        '
        Me.cmdRefreshDBList.BackColor = System.Drawing.SystemColors.Control
        Me.cmdRefreshDBList.Cursor = System.Windows.Forms.Cursors.Default
        Me.cmdRefreshDBList.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.cmdRefreshDBList.ForeColor = System.Drawing.SystemColors.ControlText
        Me.cmdRefreshDBList.Location = New System.Drawing.Point(6, 59)
        Me.cmdRefreshDBList.Name = "cmdRefreshDBList"
        Me.cmdRefreshDBList.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.cmdRefreshDBList.Size = New System.Drawing.Size(146, 25)
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
        Me.fraOutputOptions.Controls.Add(Me.lblSelectDefaultDBs)
        Me.fraOutputOptions.Controls.Add(Me.cmdSelectDefaultDMSDBs)
        Me.fraOutputOptions.Controls.Add(Me.cmdSelectDefaultMTSDBs)
        Me.fraOutputOptions.Controls.Add(Me.lblOutputFolderNamePrefix)
        Me.fraOutputOptions.Controls.Add(Me.txtOutputFolderNamePrefix)
        Me.fraOutputOptions.Controls.Add(Me.cboTableNamesToExportSortOrder)
        Me.fraOutputOptions.Controls.Add(Me.cmdRefreshDBList)
        Me.fraOutputOptions.Controls.Add(Me.chkCreateFolderForEachDB)
        Me.fraOutputOptions.Controls.Add(Me.cmdUpdateTableNames)
        Me.fraOutputOptions.Controls.Add(Me.lstTableNamesToExportData)
        Me.fraOutputOptions.Controls.Add(Me.txtOutputFolderPath)
        Me.fraOutputOptions.Controls.Add(Me.lblOutputFolderPath)
        Me.fraOutputOptions.Controls.Add(Me.lstDatabasesToProcess)
        Me.fraOutputOptions.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.fraOutputOptions.ForeColor = System.Drawing.SystemColors.ControlText
        Me.fraOutputOptions.Location = New System.Drawing.Point(12, 134)
        Me.fraOutputOptions.Name = "fraOutputOptions"
        Me.fraOutputOptions.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.fraOutputOptions.Size = New System.Drawing.Size(568, 325)
        Me.fraOutputOptions.TabIndex = 3
        Me.fraOutputOptions.TabStop = False
        Me.fraOutputOptions.Text = "Output Options"
        '
        'lblSelectDefaultDBs
        '
        Me.lblSelectDefaultDBs.BackColor = System.Drawing.SystemColors.Control
        Me.lblSelectDefaultDBs.Cursor = System.Windows.Forms.Cursors.Default
        Me.lblSelectDefaultDBs.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.lblSelectDefaultDBs.ForeColor = System.Drawing.SystemColors.ControlText
        Me.lblSelectDefaultDBs.Location = New System.Drawing.Point(11, 258)
        Me.lblSelectDefaultDBs.Name = "lblSelectDefaultDBs"
        Me.lblSelectDefaultDBs.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.lblSelectDefaultDBs.Size = New System.Drawing.Size(81, 19)
        Me.lblSelectDefaultDBs.TabIndex = 15
        Me.lblSelectDefaultDBs.Text = "Select default:"
        '
        'cmdSelectDefaultDMSDBs
        '
        Me.cmdSelectDefaultDMSDBs.BackColor = System.Drawing.SystemColors.Control
        Me.cmdSelectDefaultDMSDBs.Cursor = System.Windows.Forms.Cursors.Default
        Me.cmdSelectDefaultDMSDBs.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.cmdSelectDefaultDMSDBs.ForeColor = System.Drawing.SystemColors.ControlText
        Me.cmdSelectDefaultDMSDBs.Location = New System.Drawing.Point(92, 253)
        Me.cmdSelectDefaultDMSDBs.Name = "cmdSelectDefaultDMSDBs"
        Me.cmdSelectDefaultDMSDBs.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.cmdSelectDefaultDMSDBs.Size = New System.Drawing.Size(75, 25)
        Me.cmdSelectDefaultDMSDBs.TabIndex = 14
        Me.cmdSelectDefaultDMSDBs.Text = "DMS DBs"
        Me.cmdSelectDefaultDMSDBs.UseVisualStyleBackColor = False
        '
        'cmdSelectDefaultMTSDBs
        '
        Me.cmdSelectDefaultMTSDBs.BackColor = System.Drawing.SystemColors.Control
        Me.cmdSelectDefaultMTSDBs.Cursor = System.Windows.Forms.Cursors.Default
        Me.cmdSelectDefaultMTSDBs.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.cmdSelectDefaultMTSDBs.ForeColor = System.Drawing.SystemColors.ControlText
        Me.cmdSelectDefaultMTSDBs.Location = New System.Drawing.Point(175, 253)
        Me.cmdSelectDefaultMTSDBs.Name = "cmdSelectDefaultMTSDBs"
        Me.cmdSelectDefaultMTSDBs.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.cmdSelectDefaultMTSDBs.Size = New System.Drawing.Size(75, 25)
        Me.cmdSelectDefaultMTSDBs.TabIndex = 13
        Me.cmdSelectDefaultMTSDBs.Text = "MTS DBs"
        Me.cmdSelectDefaultMTSDBs.UseVisualStyleBackColor = False
        '
        'lblOutputFolderNamePrefix
        '
        Me.lblOutputFolderNamePrefix.BackColor = System.Drawing.SystemColors.Control
        Me.lblOutputFolderNamePrefix.Cursor = System.Windows.Forms.Cursors.Default
        Me.lblOutputFolderNamePrefix.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.lblOutputFolderNamePrefix.ForeColor = System.Drawing.SystemColors.ControlText
        Me.lblOutputFolderNamePrefix.Location = New System.Drawing.Point(6, 303)
        Me.lblOutputFolderNamePrefix.Name = "lblOutputFolderNamePrefix"
        Me.lblOutputFolderNamePrefix.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.lblOutputFolderNamePrefix.Size = New System.Drawing.Size(115, 16)
        Me.lblOutputFolderNamePrefix.TabIndex = 8
        Me.lblOutputFolderNamePrefix.Text = "Output Folder Prefix"
        '
        'txtOutputFolderNamePrefix
        '
        Me.txtOutputFolderNamePrefix.AcceptsReturn = True
        Me.txtOutputFolderNamePrefix.BackColor = System.Drawing.SystemColors.Window
        Me.txtOutputFolderNamePrefix.Cursor = System.Windows.Forms.Cursors.IBeam
        Me.txtOutputFolderNamePrefix.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.txtOutputFolderNamePrefix.ForeColor = System.Drawing.SystemColors.WindowText
        Me.txtOutputFolderNamePrefix.Location = New System.Drawing.Point(131, 300)
        Me.txtOutputFolderNamePrefix.MaxLength = 0
        Me.txtOutputFolderNamePrefix.Name = "txtOutputFolderNamePrefix"
        Me.txtOutputFolderNamePrefix.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.txtOutputFolderNamePrefix.Size = New System.Drawing.Size(117, 20)
        Me.txtOutputFolderNamePrefix.TabIndex = 9
        Me.txtOutputFolderNamePrefix.Text = "DBSchema__"
        '
        'cboTableNamesToExportSortOrder
        '
        Me.cboTableNamesToExportSortOrder.Anchor = CType((System.Windows.Forms.AnchorStyles.Bottom Or System.Windows.Forms.AnchorStyles.Left), System.Windows.Forms.AnchorStyles)
        Me.cboTableNamesToExportSortOrder.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList
        Me.cboTableNamesToExportSortOrder.FormattingEnabled = True
        Me.cboTableNamesToExportSortOrder.Location = New System.Drawing.Point(264, 283)
        Me.cboTableNamesToExportSortOrder.Name = "cboTableNamesToExportSortOrder"
        Me.cboTableNamesToExportSortOrder.Size = New System.Drawing.Size(159, 22)
        Me.cboTableNamesToExportSortOrder.TabIndex = 6
        '
        'lstTableNamesToExportData
        '
        Me.lstTableNamesToExportData.Anchor = CType((((System.Windows.Forms.AnchorStyles.Top Or System.Windows.Forms.AnchorStyles.Bottom) _
                    Or System.Windows.Forms.AnchorStyles.Left) _
                    Or System.Windows.Forms.AnchorStyles.Right), System.Windows.Forms.AnchorStyles)
        Me.lstTableNamesToExportData.ItemHeight = 14
        Me.lstTableNamesToExportData.Location = New System.Drawing.Point(264, 91)
        Me.lstTableNamesToExportData.Name = "lstTableNamesToExportData"
        Me.lstTableNamesToExportData.SelectionMode = System.Windows.Forms.SelectionMode.MultiExtended
        Me.lstTableNamesToExportData.Size = New System.Drawing.Size(295, 186)
        Me.lstTableNamesToExportData.TabIndex = 5
        '
        'txtOutputFolderPath
        '
        Me.txtOutputFolderPath.AcceptsReturn = True
        Me.txtOutputFolderPath.Anchor = CType(((System.Windows.Forms.AnchorStyles.Top Or System.Windows.Forms.AnchorStyles.Left) _
                    Or System.Windows.Forms.AnchorStyles.Right), System.Windows.Forms.AnchorStyles)
        Me.txtOutputFolderPath.BackColor = System.Drawing.SystemColors.Window
        Me.txtOutputFolderPath.Cursor = System.Windows.Forms.Cursors.IBeam
        Me.txtOutputFolderPath.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.txtOutputFolderPath.ForeColor = System.Drawing.SystemColors.WindowText
        Me.txtOutputFolderPath.Location = New System.Drawing.Point(8, 32)
        Me.txtOutputFolderPath.MaxLength = 0
        Me.txtOutputFolderPath.Name = "txtOutputFolderPath"
        Me.txtOutputFolderPath.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.txtOutputFolderPath.Size = New System.Drawing.Size(551, 20)
        Me.txtOutputFolderPath.TabIndex = 1
        Me.txtOutputFolderPath.Text = "C:\Temp\"
        '
        'lblOutputFolderPath
        '
        Me.lblOutputFolderPath.BackColor = System.Drawing.SystemColors.Control
        Me.lblOutputFolderPath.Cursor = System.Windows.Forms.Cursors.Default
        Me.lblOutputFolderPath.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.lblOutputFolderPath.ForeColor = System.Drawing.SystemColors.ControlText
        Me.lblOutputFolderPath.Location = New System.Drawing.Point(8, 16)
        Me.lblOutputFolderPath.Name = "lblOutputFolderPath"
        Me.lblOutputFolderPath.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.lblOutputFolderPath.Size = New System.Drawing.Size(137, 17)
        Me.lblOutputFolderPath.TabIndex = 0
        Me.lblOutputFolderPath.Text = "Output Folder Path"
        '
        'lstDatabasesToProcess
        '
        Me.lstDatabasesToProcess.Anchor = CType(((System.Windows.Forms.AnchorStyles.Top Or System.Windows.Forms.AnchorStyles.Bottom) _
                    Or System.Windows.Forms.AnchorStyles.Left), System.Windows.Forms.AnchorStyles)
        Me.lstDatabasesToProcess.ItemHeight = 14
        Me.lstDatabasesToProcess.Location = New System.Drawing.Point(9, 91)
        Me.lstDatabasesToProcess.Name = "lstDatabasesToProcess"
        Me.lstDatabasesToProcess.SelectionMode = System.Windows.Forms.SelectionMode.MultiExtended
        Me.lstDatabasesToProcess.Size = New System.Drawing.Size(242, 158)
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
        Me.lblSubtaskProgress.Location = New System.Drawing.Point(244, 44)
        Me.lblSubtaskProgress.Name = "lblSubtaskProgress"
        Me.lblSubtaskProgress.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.lblSubtaskProgress.Size = New System.Drawing.Size(311, 24)
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
        Me.fraStatus.Location = New System.Drawing.Point(12, 460)
        Me.fraStatus.Name = "fraStatus"
        Me.fraStatus.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.fraStatus.Size = New System.Drawing.Size(567, 78)
        Me.fraStatus.TabIndex = 4
        Me.fraStatus.TabStop = False
        '
        'pbarSubtaskProgress
        '
        Me.pbarSubtaskProgress.Location = New System.Drawing.Point(8, 46)
        Me.pbarSubtaskProgress.Maximum = 100
        Me.pbarSubtaskProgress.Minimum = 0
        Me.pbarSubtaskProgress.Name = "pbarSubtaskProgress"
        Me.pbarSubtaskProgress.Size = New System.Drawing.Size(225, 20)
        Me.pbarSubtaskProgress.TabIndex = 2
        Me.pbarSubtaskProgress.Value = 0
        '
        'lblMessage
        '
        Me.lblMessage.Anchor = CType(((System.Windows.Forms.AnchorStyles.Top Or System.Windows.Forms.AnchorStyles.Left) _
                    Or System.Windows.Forms.AnchorStyles.Right), System.Windows.Forms.AnchorStyles)
        Me.lblMessage.BackColor = System.Drawing.SystemColors.Control
        Me.lblMessage.Cursor = System.Windows.Forms.Cursors.Default
        Me.lblMessage.Font = New System.Drawing.Font("Arial", 8.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.lblMessage.ForeColor = System.Drawing.SystemColors.ControlText
        Me.lblMessage.Location = New System.Drawing.Point(6, 85)
        Me.lblMessage.Name = "lblMessage"
        Me.lblMessage.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.lblMessage.Size = New System.Drawing.Size(84, 17)
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
        Me.mnuFile.MenuItems.AddRange(New System.Windows.Forms.MenuItem() {Me.mnuFileSelectOutputFolder, Me.mnuFileSep1, Me.mnuFileLoadOptions, Me.mnuFileSaveOptions, Me.mnuFileSep2, Me.mnuFileExit})
        Me.mnuFile.Text = "&File"
        '
        'mnuFileSelectOutputFolder
        '
        Me.mnuFileSelectOutputFolder.Index = 0
        Me.mnuFileSelectOutputFolder.Shortcut = System.Windows.Forms.Shortcut.CtrlO
        Me.mnuFileSelectOutputFolder.Text = "Select &Output Folder ..."
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
        Me.fraControls.Location = New System.Drawing.Point(480, 12)
        Me.fraControls.Name = "fraControls"
        Me.fraControls.Size = New System.Drawing.Size(96, 115)
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
        Me.cmdGo.Location = New System.Drawing.Point(6, 16)
        Me.cmdGo.Name = "cmdGo"
        Me.cmdGo.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.cmdGo.Size = New System.Drawing.Size(81, 25)
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
        Me.cmdExit.Location = New System.Drawing.Point(6, 48)
        Me.cmdExit.Name = "cmdExit"
        Me.cmdExit.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.cmdExit.Size = New System.Drawing.Size(81, 25)
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
        Me.cmdAbort.Location = New System.Drawing.Point(6, 48)
        Me.cmdAbort.Name = "cmdAbort"
        Me.cmdAbort.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.cmdAbort.Size = New System.Drawing.Size(81, 25)
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
        Me.cmdPauseUnpause.Location = New System.Drawing.Point(6, 16)
        Me.cmdPauseUnpause.Name = "cmdPauseUnpause"
        Me.cmdPauseUnpause.RightToLeft = System.Windows.Forms.RightToLeft.No
        Me.cmdPauseUnpause.Size = New System.Drawing.Size(81, 25)
        Me.cmdPauseUnpause.TabIndex = 4
        Me.cmdPauseUnpause.Text = "&Pause"
        Me.cmdPauseUnpause.UseVisualStyleBackColor = False
        '
        'lstObjectTypesToScript
        '
        Me.lstObjectTypesToScript.Anchor = CType(((System.Windows.Forms.AnchorStyles.Top Or System.Windows.Forms.AnchorStyles.Bottom) _
                    Or System.Windows.Forms.AnchorStyles.Left), System.Windows.Forms.AnchorStyles)
        Me.lstObjectTypesToScript.ItemHeight = 14
        Me.lstObjectTypesToScript.Location = New System.Drawing.Point(6, 19)
        Me.lstObjectTypesToScript.Name = "lstObjectTypesToScript"
        Me.lstObjectTypesToScript.SelectionMode = System.Windows.Forms.SelectionMode.MultiExtended
        Me.lstObjectTypesToScript.Size = New System.Drawing.Size(168, 88)
        Me.lstObjectTypesToScript.TabIndex = 0
        '
        'fraObjectTypesToScript
        '
        Me.fraObjectTypesToScript.Controls.Add(Me.lstObjectTypesToScript)
        Me.fraObjectTypesToScript.Font = New System.Drawing.Font("Arial", 8.25!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.fraObjectTypesToScript.ForeColor = System.Drawing.SystemColors.ControlText
        Me.fraObjectTypesToScript.Location = New System.Drawing.Point(294, 12)
        Me.fraObjectTypesToScript.Name = "fraObjectTypesToScript"
        Me.fraObjectTypesToScript.Size = New System.Drawing.Size(180, 115)
        Me.fraObjectTypesToScript.TabIndex = 1
        Me.fraObjectTypesToScript.TabStop = False
        Me.fraObjectTypesToScript.Text = "Objects to Script"
        '
        'frmMain
        '
        Me.AutoScaleDimensions = New System.Drawing.SizeF(6.0!, 13.0!)
        Me.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font
        Me.ClientSize = New System.Drawing.Size(592, 545)
        Me.Controls.Add(Me.fraObjectTypesToScript)
        Me.Controls.Add(Me.fraControls)
        Me.Controls.Add(Me.fraConnectionSettings)
        Me.Controls.Add(Me.fraOutputOptions)
        Me.Controls.Add(Me.fraStatus)
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
    Friend WithEvents chkCreateFolderForEachDB As System.Windows.Forms.CheckBox
    Friend WithEvents pbarProgress As SmoothProgressBar.SmoothProgressBar
    Friend WithEvents cmdRefreshDBList As System.Windows.Forms.Button
    Friend WithEvents mnuHelp As System.Windows.Forms.MenuItem
    Friend WithEvents mnuHelpAbout As System.Windows.Forms.MenuItem
    Friend WithEvents fraOutputOptions As System.Windows.Forms.GroupBox
    Friend WithEvents txtOutputFolderPath As System.Windows.Forms.TextBox
    Friend WithEvents lblOutputFolderPath As System.Windows.Forms.Label
    Friend WithEvents lstDatabasesToProcess As System.Windows.Forms.ListBox
    Friend WithEvents mnuFileSep1 As System.Windows.Forms.MenuItem
    Friend WithEvents lblSubtaskProgress As System.Windows.Forms.Label
    Friend WithEvents fraStatus As System.Windows.Forms.GroupBox
    Friend WithEvents pbarSubtaskProgress As SmoothProgressBar.SmoothProgressBar
    Friend WithEvents MainMenuControl As System.Windows.Forms.MainMenu
    Friend WithEvents mnuFile As System.Windows.Forms.MenuItem
    Friend WithEvents mnuFileSelectOutputFolder As System.Windows.Forms.MenuItem
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
    Friend WithEvents lblOutputFolderNamePrefix As System.Windows.Forms.Label
    Friend WithEvents txtOutputFolderNamePrefix As System.Windows.Forms.TextBox
    Friend WithEvents mnuEditSep3 As System.Windows.Forms.MenuItem
    Friend WithEvents mnuEditIncludeTimestampInScriptFileHeader As System.Windows.Forms.MenuItem
    Friend WithEvents lblSelectDefaultDBs As System.Windows.Forms.Label
    Friend WithEvents cmdSelectDefaultDMSDBs As System.Windows.Forms.Button
    Friend WithEvents cmdSelectDefaultMTSDBs As System.Windows.Forms.Button
End Class
