namespace DB_Schema_Export_Tool
{
    partial class frmMain
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.components = new System.ComponentModel.Container();
            this.lstObjectTypesToScript = new System.Windows.Forms.ListBox();
            this.mnuHelp = new System.Windows.Forms.MenuItem();
            this.mnuHelpAbout = new System.Windows.Forms.MenuItem();
            this.lblTableDataToExport = new System.Windows.Forms.Label();
            this.lblServerOutputDirectoryNamePrefix = new System.Windows.Forms.Label();
            this.lblProgress = new System.Windows.Forms.Label();
            this.txtServerOutputDirectoryNamePrefix = new System.Windows.Forms.TextBox();
            this.chkExportServerSettingsLoginsAndJobs = new System.Windows.Forms.CheckBox();
            this.lblSelectDefaultDBs = new System.Windows.Forms.Label();
            this.cmdSelectDefaultDMSDBs = new System.Windows.Forms.Button();
            this.cmdSelectDefaultMTSDBs = new System.Windows.Forms.Button();
            this.lblOutputDirectoryNamePrefix = new System.Windows.Forms.Label();
            this.txtOutputDirectoryNamePrefix = new System.Windows.Forms.TextBox();
            this.cboTableNamesToExportSortOrder = new System.Windows.Forms.ComboBox();
            this.cmdRefreshDBList = new System.Windows.Forms.Button();
            this.lstTableNamesToExportData = new System.Windows.Forms.ListBox();
            this.lblOutputDirectoryPath = new System.Windows.Forms.Label();
            this.lstDatabasesToProcess = new System.Windows.Forms.ListBox();
            this.mnuFileSep1 = new System.Windows.Forms.MenuItem();
            this.fraStatus = new System.Windows.Forms.GroupBox();
            this.pbarProgress = new System.Windows.Forms.ProgressBar();
            this.lblSubtaskProgress = new System.Windows.Forms.Label();
            this.pbarSubtaskProgress = new System.Windows.Forms.ProgressBar();
            this.lblMessage = new System.Windows.Forms.Label();
            this.MainMenuControl = new System.Windows.Forms.MainMenu(this.components);
            this.mnuFile = new System.Windows.Forms.MenuItem();
            this.mnuFileSelectOutputDirectory = new System.Windows.Forms.MenuItem();
            this.mnuFileLoadOptions = new System.Windows.Forms.MenuItem();
            this.mnuFileSaveOptions = new System.Windows.Forms.MenuItem();
            this.mnuFileSep2 = new System.Windows.Forms.MenuItem();
            this.mnuFileExit = new System.Windows.Forms.MenuItem();
            this.mnuEdit = new System.Windows.Forms.MenuItem();
            this.mnuEditStart = new System.Windows.Forms.MenuItem();
            this.mnuEditIncludeSystemObjects = new System.Windows.Forms.MenuItem();
            this.mnuEditSep1 = new System.Windows.Forms.MenuItem();
            this.mnuEditScriptObjectsThreaded = new System.Windows.Forms.MenuItem();
            this.mnuEditPauseAfterEachDatabase = new System.Windows.Forms.MenuItem();
            this.mnuEditIncludeTimestampInScriptFileHeader = new System.Windows.Forms.MenuItem();
            this.mnuEditSep2 = new System.Windows.Forms.MenuItem();
            this.mnuEditIncludeTableRowCounts = new System.Windows.Forms.MenuItem();
            this.mnuEditAutoSelectDefaultTableNames = new System.Windows.Forms.MenuItem();
            this.mnuEditSep3 = new System.Windows.Forms.MenuItem();
            this.mnuEditWarnOnHighTableRowCount = new System.Windows.Forms.MenuItem();
            this.mnuEditSaveDataAsInsertIntoStatements = new System.Windows.Forms.MenuItem();
            this.mnuEditSep4 = new System.Windows.Forms.MenuItem();
            this.mnuEditResetOptions = new System.Windows.Forms.MenuItem();
            this.fraControls = new System.Windows.Forms.GroupBox();
            this.cmdGo = new System.Windows.Forms.Button();
            this.cmdExit = new System.Windows.Forms.Button();
            this.cmdAbort = new System.Windows.Forms.Button();
            this.cmdPauseUnpause = new System.Windows.Forms.Button();
            this.txtOutputDirectoryPath = new System.Windows.Forms.TextBox();
            this.fraObjectTypesToScript = new System.Windows.Forms.GroupBox();
            this.fraOutputOptions = new System.Windows.Forms.GroupBox();
            this.chkCreateDirectoryForEachDB = new System.Windows.Forms.CheckBox();
            this.cmdUpdateTableNames = new System.Windows.Forms.Button();
            this.fraConnectionSettings = new System.Windows.Forms.GroupBox();
            this.chkPostgreSQL = new System.Windows.Forms.CheckBox();
            this.lblServerName = new System.Windows.Forms.Label();
            this.chkUseIntegratedAuthentication = new System.Windows.Forms.CheckBox();
            this.txtPassword = new System.Windows.Forms.TextBox();
            this.txtUsername = new System.Windows.Forms.TextBox();
            this.txtServerName = new System.Windows.Forms.TextBox();
            this.lblPassword = new System.Windows.Forms.Label();
            this.lblUsername = new System.Windows.Forms.Label();
            this.fraStatus.SuspendLayout();
            this.fraControls.SuspendLayout();
            this.fraObjectTypesToScript.SuspendLayout();
            this.fraOutputOptions.SuspendLayout();
            this.fraConnectionSettings.SuspendLayout();
            this.SuspendLayout();
            // 
            // lstObjectTypesToScript
            // 
            this.lstObjectTypesToScript.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left)));
            this.lstObjectTypesToScript.ItemHeight = 16;
            this.lstObjectTypesToScript.Location = new System.Drawing.Point(8, 23);
            this.lstObjectTypesToScript.Margin = new System.Windows.Forms.Padding(4);
            this.lstObjectTypesToScript.Name = "lstObjectTypesToScript";
            this.lstObjectTypesToScript.SelectionMode = System.Windows.Forms.SelectionMode.MultiExtended;
            this.lstObjectTypesToScript.Size = new System.Drawing.Size(223, 116);
            this.lstObjectTypesToScript.TabIndex = 0;
            this.lstObjectTypesToScript.KeyDown += new System.Windows.Forms.KeyEventHandler(this.lstObjectTypesToScript_KeyDown);
            this.lstObjectTypesToScript.KeyPress += new System.Windows.Forms.KeyPressEventHandler(this.lstObjectTypesToScript_KeyPress);
            // 
            // mnuHelp
            // 
            this.mnuHelp.Index = 2;
            this.mnuHelp.MenuItems.AddRange(new System.Windows.Forms.MenuItem[] {
            this.mnuHelpAbout});
            this.mnuHelp.Text = "&Help";
            // 
            // mnuHelpAbout
            // 
            this.mnuHelpAbout.Index = 0;
            this.mnuHelpAbout.Text = "&About";
            this.mnuHelpAbout.Click += new System.EventHandler(this.mnuHelpAbout_Click);
            // 
            // lblTableDataToExport
            // 
            this.lblTableDataToExport.BackColor = System.Drawing.SystemColors.Control;
            this.lblTableDataToExport.Cursor = System.Windows.Forms.Cursors.Default;
            this.lblTableDataToExport.Font = new System.Drawing.Font("Arial", 8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblTableDataToExport.ForeColor = System.Drawing.SystemColors.ControlText;
            this.lblTableDataToExport.Location = new System.Drawing.Point(350, 77);
            this.lblTableDataToExport.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.lblTableDataToExport.Name = "lblTableDataToExport";
            this.lblTableDataToExport.RightToLeft = System.Windows.Forms.RightToLeft.No;
            this.lblTableDataToExport.Size = new System.Drawing.Size(171, 20);
            this.lblTableDataToExport.TabIndex = 7;
            this.lblTableDataToExport.Text = "Table Data to Export";
            // 
            // lblServerOutputDirectoryNamePrefix
            // 
            this.lblServerOutputDirectoryNamePrefix.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left)));
            this.lblServerOutputDirectoryNamePrefix.BackColor = System.Drawing.SystemColors.Control;
            this.lblServerOutputDirectoryNamePrefix.Cursor = System.Windows.Forms.Cursors.Default;
            this.lblServerOutputDirectoryNamePrefix.Font = new System.Drawing.Font("Arial", 8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblServerOutputDirectoryNamePrefix.ForeColor = System.Drawing.SystemColors.ControlText;
            this.lblServerOutputDirectoryNamePrefix.Location = new System.Drawing.Point(349, 278);
            this.lblServerOutputDirectoryNamePrefix.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.lblServerOutputDirectoryNamePrefix.Name = "lblServerOutputDirectoryNamePrefix";
            this.lblServerOutputDirectoryNamePrefix.RightToLeft = System.Windows.Forms.RightToLeft.No;
            this.lblServerOutputDirectoryNamePrefix.Size = new System.Drawing.Size(153, 20);
            this.lblServerOutputDirectoryNamePrefix.TabIndex = 17;
            this.lblServerOutputDirectoryNamePrefix.Text = "Output Directory Prefix";
            // 
            // lblProgress
            // 
            this.lblProgress.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.lblProgress.BackColor = System.Drawing.SystemColors.Control;
            this.lblProgress.Cursor = System.Windows.Forms.Cursors.Default;
            this.lblProgress.Font = new System.Drawing.Font("Arial", 8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblProgress.ForeColor = System.Drawing.SystemColors.ControlText;
            this.lblProgress.Location = new System.Drawing.Point(325, 15);
            this.lblProgress.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.lblProgress.Name = "lblProgress";
            this.lblProgress.RightToLeft = System.Windows.Forms.RightToLeft.No;
            this.lblProgress.Size = new System.Drawing.Size(417, 30);
            this.lblProgress.TabIndex = 1;
            // 
            // txtServerOutputDirectoryNamePrefix
            // 
            this.txtServerOutputDirectoryNamePrefix.AcceptsReturn = true;
            this.txtServerOutputDirectoryNamePrefix.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left)));
            this.txtServerOutputDirectoryNamePrefix.BackColor = System.Drawing.SystemColors.Window;
            this.txtServerOutputDirectoryNamePrefix.Cursor = System.Windows.Forms.Cursors.IBeam;
            this.txtServerOutputDirectoryNamePrefix.Font = new System.Drawing.Font("Arial", 8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.txtServerOutputDirectoryNamePrefix.ForeColor = System.Drawing.SystemColors.WindowText;
            this.txtServerOutputDirectoryNamePrefix.Location = new System.Drawing.Point(516, 274);
            this.txtServerOutputDirectoryNamePrefix.Margin = new System.Windows.Forms.Padding(4);
            this.txtServerOutputDirectoryNamePrefix.MaxLength = 0;
            this.txtServerOutputDirectoryNamePrefix.Name = "txtServerOutputDirectoryNamePrefix";
            this.txtServerOutputDirectoryNamePrefix.RightToLeft = System.Windows.Forms.RightToLeft.No;
            this.txtServerOutputDirectoryNamePrefix.Size = new System.Drawing.Size(155, 23);
            this.txtServerOutputDirectoryNamePrefix.TabIndex = 18;
            this.txtServerOutputDirectoryNamePrefix.Text = "ServerSchema__";
            // 
            // chkExportServerSettingsLoginsAndJobs
            // 
            this.chkExportServerSettingsLoginsAndJobs.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left)));
            this.chkExportServerSettingsLoginsAndJobs.BackColor = System.Drawing.SystemColors.Control;
            this.chkExportServerSettingsLoginsAndJobs.Checked = true;
            this.chkExportServerSettingsLoginsAndJobs.CheckState = System.Windows.Forms.CheckState.Checked;
            this.chkExportServerSettingsLoginsAndJobs.Cursor = System.Windows.Forms.Cursors.Default;
            this.chkExportServerSettingsLoginsAndJobs.Font = new System.Drawing.Font("Arial", 8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.chkExportServerSettingsLoginsAndJobs.ForeColor = System.Drawing.SystemColors.ControlText;
            this.chkExportServerSettingsLoginsAndJobs.Location = new System.Drawing.Point(352, 251);
            this.chkExportServerSettingsLoginsAndJobs.Margin = new System.Windows.Forms.Padding(4);
            this.chkExportServerSettingsLoginsAndJobs.Name = "chkExportServerSettingsLoginsAndJobs";
            this.chkExportServerSettingsLoginsAndJobs.RightToLeft = System.Windows.Forms.RightToLeft.No;
            this.chkExportServerSettingsLoginsAndJobs.Size = new System.Drawing.Size(311, 25);
            this.chkExportServerSettingsLoginsAndJobs.TabIndex = 16;
            this.chkExportServerSettingsLoginsAndJobs.Text = "Export server settings, logins, and jobs";
            this.chkExportServerSettingsLoginsAndJobs.UseVisualStyleBackColor = false;
            // 
            // lblSelectDefaultDBs
            // 
            this.lblSelectDefaultDBs.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left)));
            this.lblSelectDefaultDBs.BackColor = System.Drawing.SystemColors.Control;
            this.lblSelectDefaultDBs.Cursor = System.Windows.Forms.Cursors.Default;
            this.lblSelectDefaultDBs.Font = new System.Drawing.Font("Arial", 8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblSelectDefaultDBs.ForeColor = System.Drawing.SystemColors.ControlText;
            this.lblSelectDefaultDBs.Location = new System.Drawing.Point(13, 216);
            this.lblSelectDefaultDBs.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.lblSelectDefaultDBs.Name = "lblSelectDefaultDBs";
            this.lblSelectDefaultDBs.RightToLeft = System.Windows.Forms.RightToLeft.No;
            this.lblSelectDefaultDBs.Size = new System.Drawing.Size(108, 23);
            this.lblSelectDefaultDBs.TabIndex = 15;
            this.lblSelectDefaultDBs.Text = "Select default:";
            // 
            // cmdSelectDefaultDMSDBs
            // 
            this.cmdSelectDefaultDMSDBs.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left)));
            this.cmdSelectDefaultDMSDBs.BackColor = System.Drawing.SystemColors.Control;
            this.cmdSelectDefaultDMSDBs.Cursor = System.Windows.Forms.Cursors.Default;
            this.cmdSelectDefaultDMSDBs.Font = new System.Drawing.Font("Arial", 8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.cmdSelectDefaultDMSDBs.ForeColor = System.Drawing.SystemColors.ControlText;
            this.cmdSelectDefaultDMSDBs.Location = new System.Drawing.Point(124, 212);
            this.cmdSelectDefaultDMSDBs.Margin = new System.Windows.Forms.Padding(4);
            this.cmdSelectDefaultDMSDBs.Name = "cmdSelectDefaultDMSDBs";
            this.cmdSelectDefaultDMSDBs.RightToLeft = System.Windows.Forms.RightToLeft.No;
            this.cmdSelectDefaultDMSDBs.Size = new System.Drawing.Size(100, 31);
            this.cmdSelectDefaultDMSDBs.TabIndex = 14;
            this.cmdSelectDefaultDMSDBs.Text = "DMS DBs";
            this.cmdSelectDefaultDMSDBs.UseVisualStyleBackColor = false;
            this.cmdSelectDefaultDMSDBs.Click += new System.EventHandler(this.cmdSelectDefaultDMSDBs_Click);
            // 
            // cmdSelectDefaultMTSDBs
            // 
            this.cmdSelectDefaultMTSDBs.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left)));
            this.cmdSelectDefaultMTSDBs.BackColor = System.Drawing.SystemColors.Control;
            this.cmdSelectDefaultMTSDBs.Cursor = System.Windows.Forms.Cursors.Default;
            this.cmdSelectDefaultMTSDBs.Font = new System.Drawing.Font("Arial", 8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.cmdSelectDefaultMTSDBs.ForeColor = System.Drawing.SystemColors.ControlText;
            this.cmdSelectDefaultMTSDBs.Location = new System.Drawing.Point(235, 212);
            this.cmdSelectDefaultMTSDBs.Margin = new System.Windows.Forms.Padding(4);
            this.cmdSelectDefaultMTSDBs.Name = "cmdSelectDefaultMTSDBs";
            this.cmdSelectDefaultMTSDBs.RightToLeft = System.Windows.Forms.RightToLeft.No;
            this.cmdSelectDefaultMTSDBs.Size = new System.Drawing.Size(100, 31);
            this.cmdSelectDefaultMTSDBs.TabIndex = 13;
            this.cmdSelectDefaultMTSDBs.Text = "MTS DBs";
            this.cmdSelectDefaultMTSDBs.UseVisualStyleBackColor = false;
            this.cmdSelectDefaultMTSDBs.Click += new System.EventHandler(this.cmdSelectDefaultMTSDBs_Click);
            // 
            // lblOutputDirectoryNamePrefix
            // 
            this.lblOutputDirectoryNamePrefix.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left)));
            this.lblOutputDirectoryNamePrefix.BackColor = System.Drawing.SystemColors.Control;
            this.lblOutputDirectoryNamePrefix.Cursor = System.Windows.Forms.Cursors.Default;
            this.lblOutputDirectoryNamePrefix.Font = new System.Drawing.Font("Arial", 8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblOutputDirectoryNamePrefix.ForeColor = System.Drawing.SystemColors.ControlText;
            this.lblOutputDirectoryNamePrefix.Location = new System.Drawing.Point(12, 281);
            this.lblOutputDirectoryNamePrefix.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.lblOutputDirectoryNamePrefix.Name = "lblOutputDirectoryNamePrefix";
            this.lblOutputDirectoryNamePrefix.RightToLeft = System.Windows.Forms.RightToLeft.No;
            this.lblOutputDirectoryNamePrefix.Size = new System.Drawing.Size(153, 20);
            this.lblOutputDirectoryNamePrefix.TabIndex = 8;
            this.lblOutputDirectoryNamePrefix.Text = "Output Directory Prefix";
            // 
            // txtOutputDirectoryNamePrefix
            // 
            this.txtOutputDirectoryNamePrefix.AcceptsReturn = true;
            this.txtOutputDirectoryNamePrefix.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left)));
            this.txtOutputDirectoryNamePrefix.BackColor = System.Drawing.SystemColors.Window;
            this.txtOutputDirectoryNamePrefix.Cursor = System.Windows.Forms.Cursors.IBeam;
            this.txtOutputDirectoryNamePrefix.Font = new System.Drawing.Font("Arial", 8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.txtOutputDirectoryNamePrefix.ForeColor = System.Drawing.SystemColors.WindowText;
            this.txtOutputDirectoryNamePrefix.Location = new System.Drawing.Point(179, 278);
            this.txtOutputDirectoryNamePrefix.Margin = new System.Windows.Forms.Padding(4);
            this.txtOutputDirectoryNamePrefix.MaxLength = 0;
            this.txtOutputDirectoryNamePrefix.Name = "txtOutputDirectoryNamePrefix";
            this.txtOutputDirectoryNamePrefix.RightToLeft = System.Windows.Forms.RightToLeft.No;
            this.txtOutputDirectoryNamePrefix.Size = new System.Drawing.Size(155, 23);
            this.txtOutputDirectoryNamePrefix.TabIndex = 9;
            this.txtOutputDirectoryNamePrefix.Text = "DBSchema__";
            // 
            // cboTableNamesToExportSortOrder
            // 
            this.cboTableNamesToExportSortOrder.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left)));
            this.cboTableNamesToExportSortOrder.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.cboTableNamesToExportSortOrder.FormattingEnabled = true;
            this.cboTableNamesToExportSortOrder.Location = new System.Drawing.Point(353, 212);
            this.cboTableNamesToExportSortOrder.Margin = new System.Windows.Forms.Padding(4);
            this.cboTableNamesToExportSortOrder.Name = "cboTableNamesToExportSortOrder";
            this.cboTableNamesToExportSortOrder.Size = new System.Drawing.Size(211, 24);
            this.cboTableNamesToExportSortOrder.TabIndex = 6;
            this.cboTableNamesToExportSortOrder.SelectedIndexChanged += new System.EventHandler(this.cboTableNamesToExportSortOrder_SelectedIndexChanged);
            // 
            // cmdRefreshDBList
            // 
            this.cmdRefreshDBList.BackColor = System.Drawing.SystemColors.Control;
            this.cmdRefreshDBList.Cursor = System.Windows.Forms.Cursors.Default;
            this.cmdRefreshDBList.Font = new System.Drawing.Font("Arial", 8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.cmdRefreshDBList.ForeColor = System.Drawing.SystemColors.ControlText;
            this.cmdRefreshDBList.Location = new System.Drawing.Point(8, 73);
            this.cmdRefreshDBList.Margin = new System.Windows.Forms.Padding(4);
            this.cmdRefreshDBList.Name = "cmdRefreshDBList";
            this.cmdRefreshDBList.RightToLeft = System.Windows.Forms.RightToLeft.No;
            this.cmdRefreshDBList.Size = new System.Drawing.Size(195, 31);
            this.cmdRefreshDBList.TabIndex = 2;
            this.cmdRefreshDBList.Text = "Refresh &Database List";
            this.cmdRefreshDBList.UseVisualStyleBackColor = false;
            this.cmdRefreshDBList.Click += new System.EventHandler(this.cmdRefreshDBList_Click);
            // 
            // lstTableNamesToExportData
            // 
            this.lstTableNamesToExportData.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.lstTableNamesToExportData.ItemHeight = 16;
            this.lstTableNamesToExportData.Location = new System.Drawing.Point(352, 112);
            this.lstTableNamesToExportData.Margin = new System.Windows.Forms.Padding(4);
            this.lstTableNamesToExportData.Name = "lstTableNamesToExportData";
            this.lstTableNamesToExportData.SelectionMode = System.Windows.Forms.SelectionMode.MultiExtended;
            this.lstTableNamesToExportData.Size = new System.Drawing.Size(394, 84);
            this.lstTableNamesToExportData.TabIndex = 5;
            this.lstTableNamesToExportData.KeyDown += new System.Windows.Forms.KeyEventHandler(this.lstTableNamesToExportData_KeyDown);
            this.lstTableNamesToExportData.KeyPress += new System.Windows.Forms.KeyPressEventHandler(this.lstTableNamesToExportData_KeyPress);
            // 
            // lblOutputDirectoryPath
            // 
            this.lblOutputDirectoryPath.BackColor = System.Drawing.SystemColors.Control;
            this.lblOutputDirectoryPath.Cursor = System.Windows.Forms.Cursors.Default;
            this.lblOutputDirectoryPath.Font = new System.Drawing.Font("Arial", 8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblOutputDirectoryPath.ForeColor = System.Drawing.SystemColors.ControlText;
            this.lblOutputDirectoryPath.Location = new System.Drawing.Point(11, 20);
            this.lblOutputDirectoryPath.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.lblOutputDirectoryPath.Name = "lblOutputDirectoryPath";
            this.lblOutputDirectoryPath.RightToLeft = System.Windows.Forms.RightToLeft.No;
            this.lblOutputDirectoryPath.Size = new System.Drawing.Size(183, 21);
            this.lblOutputDirectoryPath.TabIndex = 0;
            this.lblOutputDirectoryPath.Text = "Output Directory Path";
            // 
            // lstDatabasesToProcess
            // 
            this.lstDatabasesToProcess.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left)));
            this.lstDatabasesToProcess.ItemHeight = 16;
            this.lstDatabasesToProcess.Location = new System.Drawing.Point(12, 112);
            this.lstDatabasesToProcess.Margin = new System.Windows.Forms.Padding(4);
            this.lstDatabasesToProcess.Name = "lstDatabasesToProcess";
            this.lstDatabasesToProcess.SelectionMode = System.Windows.Forms.SelectionMode.MultiExtended;
            this.lstDatabasesToProcess.Size = new System.Drawing.Size(321, 84);
            this.lstDatabasesToProcess.TabIndex = 3;
            this.lstDatabasesToProcess.KeyDown += new System.Windows.Forms.KeyEventHandler(this.lstDatabasesToProcess_KeyDown);
            this.lstDatabasesToProcess.KeyPress += new System.Windows.Forms.KeyPressEventHandler(this.lstDatabasesToProcess_KeyPress);
            // 
            // mnuFileSep1
            // 
            this.mnuFileSep1.Index = 1;
            this.mnuFileSep1.Text = "-";
            // 
            // fraStatus
            // 
            this.fraStatus.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.fraStatus.BackColor = System.Drawing.SystemColors.Control;
            this.fraStatus.Controls.Add(this.lblProgress);
            this.fraStatus.Controls.Add(this.pbarProgress);
            this.fraStatus.Controls.Add(this.lblSubtaskProgress);
            this.fraStatus.Controls.Add(this.pbarSubtaskProgress);
            this.fraStatus.Font = new System.Drawing.Font("Arial", 8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.fraStatus.ForeColor = System.Drawing.SystemColors.ControlText;
            this.fraStatus.Location = new System.Drawing.Point(21, 491);
            this.fraStatus.Margin = new System.Windows.Forms.Padding(4);
            this.fraStatus.Name = "fraStatus";
            this.fraStatus.Padding = new System.Windows.Forms.Padding(4);
            this.fraStatus.RightToLeft = System.Windows.Forms.RightToLeft.No;
            this.fraStatus.Size = new System.Drawing.Size(758, 96);
            this.fraStatus.TabIndex = 9;
            this.fraStatus.TabStop = false;
            // 
            // pbarProgress
            // 
            this.pbarProgress.Location = new System.Drawing.Point(11, 17);
            this.pbarProgress.Margin = new System.Windows.Forms.Padding(4);
            this.pbarProgress.Name = "pbarProgress";
            this.pbarProgress.Size = new System.Drawing.Size(300, 25);
            this.pbarProgress.Style = System.Windows.Forms.ProgressBarStyle.Continuous;
            this.pbarProgress.TabIndex = 0;
            // 
            // lblSubtaskProgress
            // 
            this.lblSubtaskProgress.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.lblSubtaskProgress.BackColor = System.Drawing.SystemColors.Control;
            this.lblSubtaskProgress.Cursor = System.Windows.Forms.Cursors.Default;
            this.lblSubtaskProgress.Font = new System.Drawing.Font("Arial", 8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblSubtaskProgress.ForeColor = System.Drawing.SystemColors.ControlText;
            this.lblSubtaskProgress.Location = new System.Drawing.Point(325, 54);
            this.lblSubtaskProgress.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.lblSubtaskProgress.Name = "lblSubtaskProgress";
            this.lblSubtaskProgress.RightToLeft = System.Windows.Forms.RightToLeft.No;
            this.lblSubtaskProgress.Size = new System.Drawing.Size(417, 30);
            this.lblSubtaskProgress.TabIndex = 3;
            // 
            // pbarSubtaskProgress
            // 
            this.pbarSubtaskProgress.Location = new System.Drawing.Point(11, 57);
            this.pbarSubtaskProgress.Margin = new System.Windows.Forms.Padding(4);
            this.pbarSubtaskProgress.Name = "pbarSubtaskProgress";
            this.pbarSubtaskProgress.Size = new System.Drawing.Size(300, 25);
            this.pbarSubtaskProgress.Style = System.Windows.Forms.ProgressBarStyle.Continuous;
            this.pbarSubtaskProgress.TabIndex = 2;
            // 
            // lblMessage
            // 
            this.lblMessage.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.lblMessage.BackColor = System.Drawing.SystemColors.Control;
            this.lblMessage.Cursor = System.Windows.Forms.Cursors.Default;
            this.lblMessage.Font = new System.Drawing.Font("Arial", 8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblMessage.ForeColor = System.Drawing.SystemColors.ControlText;
            this.lblMessage.Location = new System.Drawing.Point(8, 105);
            this.lblMessage.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.lblMessage.Name = "lblMessage";
            this.lblMessage.RightToLeft = System.Windows.Forms.RightToLeft.No;
            this.lblMessage.Size = new System.Drawing.Size(112, 21);
            this.lblMessage.TabIndex = 2;
            this.lblMessage.Visible = false;
            // 
            // MainMenuControl
            // 
            this.MainMenuControl.MenuItems.AddRange(new System.Windows.Forms.MenuItem[] {
            this.mnuFile,
            this.mnuEdit,
            this.mnuHelp});
            // 
            // mnuFile
            // 
            this.mnuFile.Index = 0;
            this.mnuFile.MenuItems.AddRange(new System.Windows.Forms.MenuItem[] {
            this.mnuFileSelectOutputDirectory,
            this.mnuFileSep1,
            this.mnuFileLoadOptions,
            this.mnuFileSaveOptions,
            this.mnuFileSep2,
            this.mnuFileExit});
            this.mnuFile.Text = "&File";
            // 
            // mnuFileSelectOutputDirectory
            // 
            this.mnuFileSelectOutputDirectory.Index = 0;
            this.mnuFileSelectOutputDirectory.Shortcut = System.Windows.Forms.Shortcut.CtrlO;
            this.mnuFileSelectOutputDirectory.Text = "Select &Output Directory ...";
            this.mnuFileSelectOutputDirectory.Click += new System.EventHandler(this.mnuFileSelectOutputDirectory_Click);
            // 
            // mnuFileLoadOptions
            // 
            this.mnuFileLoadOptions.Index = 2;
            this.mnuFileLoadOptions.Shortcut = System.Windows.Forms.Shortcut.CtrlL;
            this.mnuFileLoadOptions.Text = "&Load Options ...";
            this.mnuFileLoadOptions.Click += new System.EventHandler(this.mnuFileLoadOptions_Click);
            // 
            // mnuFileSaveOptions
            // 
            this.mnuFileSaveOptions.Index = 3;
            this.mnuFileSaveOptions.Shortcut = System.Windows.Forms.Shortcut.CtrlS;
            this.mnuFileSaveOptions.Text = "&Save Options ...";
            this.mnuFileSaveOptions.Click += new System.EventHandler(this.mnuFileSaveOptions_Click);
            // 
            // mnuFileSep2
            // 
            this.mnuFileSep2.Index = 4;
            this.mnuFileSep2.Text = "-";
            // 
            // mnuFileExit
            // 
            this.mnuFileExit.Index = 5;
            this.mnuFileExit.Text = "E&xit";
            this.mnuFileExit.Click += new System.EventHandler(this.mnuFileExit_Click);
            // 
            // mnuEdit
            // 
            this.mnuEdit.Index = 1;
            this.mnuEdit.MenuItems.AddRange(new System.Windows.Forms.MenuItem[] {
            this.mnuEditStart,
            this.mnuEditIncludeSystemObjects,
            this.mnuEditSep1,
            this.mnuEditScriptObjectsThreaded,
            this.mnuEditPauseAfterEachDatabase,
            this.mnuEditIncludeTimestampInScriptFileHeader,
            this.mnuEditSep2,
            this.mnuEditIncludeTableRowCounts,
            this.mnuEditAutoSelectDefaultTableNames,
            this.mnuEditSep3,
            this.mnuEditWarnOnHighTableRowCount,
            this.mnuEditSaveDataAsInsertIntoStatements,
            this.mnuEditSep4,
            this.mnuEditResetOptions});
            this.mnuEdit.Text = "&Edit";
            // 
            // mnuEditStart
            // 
            this.mnuEditStart.Index = 0;
            this.mnuEditStart.Shortcut = System.Windows.Forms.Shortcut.CtrlG;
            this.mnuEditStart.Text = "&Export DB Schema";
            this.mnuEditStart.Click += new System.EventHandler(this.mnuEditStart_Click);
            // 
            // mnuEditIncludeSystemObjects
            // 
            this.mnuEditIncludeSystemObjects.Index = 1;
            this.mnuEditIncludeSystemObjects.Text = "Include System &Objects";
            this.mnuEditIncludeSystemObjects.Click += new System.EventHandler(this.mnuEditIncludeSystemObjects_Click);
            // 
            // mnuEditSep1
            // 
            this.mnuEditSep1.Index = 2;
            this.mnuEditSep1.Text = "-";
            // 
            // mnuEditScriptObjectsThreaded
            // 
            this.mnuEditScriptObjectsThreaded.Index = 3;
            this.mnuEditScriptObjectsThreaded.Text = "Script Objects in Separate &Thread";
            this.mnuEditScriptObjectsThreaded.Click += new System.EventHandler(this.mnuEditScriptObjectsThreaded_Click);
            // 
            // mnuEditPauseAfterEachDatabase
            // 
            this.mnuEditPauseAfterEachDatabase.Index = 4;
            this.mnuEditPauseAfterEachDatabase.Text = "&Pause after each Database";
            this.mnuEditPauseAfterEachDatabase.Click += new System.EventHandler(this.mnuEditPauseAfterEachDatabase_Click);
            // 
            // mnuEditIncludeTimestampInScriptFileHeader
            // 
            this.mnuEditIncludeTimestampInScriptFileHeader.Index = 5;
            this.mnuEditIncludeTimestampInScriptFileHeader.Text = "Include Timestamp in Script File Header";
            this.mnuEditIncludeTimestampInScriptFileHeader.Click += new System.EventHandler(this.mnuEditIncludeTimestampInScriptFileHeader_Click);
            // 
            // mnuEditSep2
            // 
            this.mnuEditSep2.Index = 6;
            this.mnuEditSep2.Text = "-";
            // 
            // mnuEditIncludeTableRowCounts
            // 
            this.mnuEditIncludeTableRowCounts.Checked = true;
            this.mnuEditIncludeTableRowCounts.Index = 7;
            this.mnuEditIncludeTableRowCounts.Text = "Obtain Table Row &Counts";
            this.mnuEditIncludeTableRowCounts.Click += new System.EventHandler(this.mnuEditIncludeTableRowCounts_Click);
            // 
            // mnuEditAutoSelectDefaultTableNames
            // 
            this.mnuEditAutoSelectDefaultTableNames.Checked = true;
            this.mnuEditAutoSelectDefaultTableNames.Index = 8;
            this.mnuEditAutoSelectDefaultTableNames.Text = "&Auto-select Default Table Names";
            this.mnuEditAutoSelectDefaultTableNames.Click += new System.EventHandler(this.mnuEditAutoSelectDefaultTableNames_Click);
            // 
            // mnuEditSep3
            // 
            this.mnuEditSep3.Index = 9;
            this.mnuEditSep3.Text = "-";
            // 
            // mnuEditWarnOnHighTableRowCount
            // 
            this.mnuEditWarnOnHighTableRowCount.Checked = true;
            this.mnuEditWarnOnHighTableRowCount.Index = 10;
            this.mnuEditWarnOnHighTableRowCount.Text = "&Warn If High Table Row Count";
            this.mnuEditWarnOnHighTableRowCount.Click += new System.EventHandler(this.mnuEditWarnOnHighTableRowCount_Click);
            // 
            // mnuEditSaveDataAsInsertIntoStatements
            // 
            this.mnuEditSaveDataAsInsertIntoStatements.Checked = true;
            this.mnuEditSaveDataAsInsertIntoStatements.Index = 11;
            this.mnuEditSaveDataAsInsertIntoStatements.Text = "Save Data As &Insert Into Statements";
            this.mnuEditSaveDataAsInsertIntoStatements.Click += new System.EventHandler(this.mnuEditSaveDataAsInsertIntoStatements_Click);
            // 
            // mnuEditSep4
            // 
            this.mnuEditSep4.Index = 12;
            this.mnuEditSep4.Text = "-";
            // 
            // mnuEditResetOptions
            // 
            this.mnuEditResetOptions.Index = 13;
            this.mnuEditResetOptions.Text = "&Reset Options to Defaults";
            this.mnuEditResetOptions.Click += new System.EventHandler(this.mnuEditResetOptions_Click);
            // 
            // fraControls
            // 
            this.fraControls.Controls.Add(this.lblMessage);
            this.fraControls.Controls.Add(this.cmdGo);
            this.fraControls.Controls.Add(this.cmdExit);
            this.fraControls.Controls.Add(this.cmdAbort);
            this.fraControls.Controls.Add(this.cmdPauseUnpause);
            this.fraControls.Font = new System.Drawing.Font("Arial", 8.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.fraControls.ForeColor = System.Drawing.SystemColors.ControlText;
            this.fraControls.Location = new System.Drawing.Point(645, 5);
            this.fraControls.Margin = new System.Windows.Forms.Padding(4);
            this.fraControls.Name = "fraControls";
            this.fraControls.Padding = new System.Windows.Forms.Padding(4);
            this.fraControls.Size = new System.Drawing.Size(128, 155);
            this.fraControls.TabIndex = 7;
            this.fraControls.TabStop = false;
            this.fraControls.Text = "Controls";
            // 
            // cmdGo
            // 
            this.cmdGo.BackColor = System.Drawing.SystemColors.Control;
            this.cmdGo.Cursor = System.Windows.Forms.Cursors.Default;
            this.cmdGo.Font = new System.Drawing.Font("Arial", 8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.cmdGo.ForeColor = System.Drawing.SystemColors.ControlText;
            this.cmdGo.Location = new System.Drawing.Point(8, 20);
            this.cmdGo.Margin = new System.Windows.Forms.Padding(4);
            this.cmdGo.Name = "cmdGo";
            this.cmdGo.RightToLeft = System.Windows.Forms.RightToLeft.No;
            this.cmdGo.Size = new System.Drawing.Size(108, 31);
            this.cmdGo.TabIndex = 0;
            this.cmdGo.Text = "&Go";
            this.cmdGo.UseVisualStyleBackColor = false;
            this.cmdGo.Click += new System.EventHandler(this.cmdGo_Click);
            // 
            // cmdExit
            // 
            this.cmdExit.BackColor = System.Drawing.SystemColors.Control;
            this.cmdExit.Cursor = System.Windows.Forms.Cursors.Default;
            this.cmdExit.Font = new System.Drawing.Font("Arial", 8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.cmdExit.ForeColor = System.Drawing.SystemColors.ControlText;
            this.cmdExit.Location = new System.Drawing.Point(8, 59);
            this.cmdExit.Margin = new System.Windows.Forms.Padding(4);
            this.cmdExit.Name = "cmdExit";
            this.cmdExit.RightToLeft = System.Windows.Forms.RightToLeft.No;
            this.cmdExit.Size = new System.Drawing.Size(108, 31);
            this.cmdExit.TabIndex = 1;
            this.cmdExit.Text = "E&xit";
            this.cmdExit.UseVisualStyleBackColor = false;
            this.cmdExit.Click += new System.EventHandler(this.cmdExit_Click);
            // 
            // cmdAbort
            // 
            this.cmdAbort.BackColor = System.Drawing.SystemColors.Control;
            this.cmdAbort.Cursor = System.Windows.Forms.Cursors.Default;
            this.cmdAbort.Font = new System.Drawing.Font("Arial", 8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.cmdAbort.ForeColor = System.Drawing.SystemColors.ControlText;
            this.cmdAbort.Location = new System.Drawing.Point(8, 59);
            this.cmdAbort.Margin = new System.Windows.Forms.Padding(4);
            this.cmdAbort.Name = "cmdAbort";
            this.cmdAbort.RightToLeft = System.Windows.Forms.RightToLeft.No;
            this.cmdAbort.Size = new System.Drawing.Size(108, 31);
            this.cmdAbort.TabIndex = 5;
            this.cmdAbort.Text = "&Abort";
            this.cmdAbort.UseVisualStyleBackColor = false;
            this.cmdAbort.Click += new System.EventHandler(this.cmdAbort_Click);
            // 
            // cmdPauseUnpause
            // 
            this.cmdPauseUnpause.BackColor = System.Drawing.SystemColors.Control;
            this.cmdPauseUnpause.Cursor = System.Windows.Forms.Cursors.Default;
            this.cmdPauseUnpause.Font = new System.Drawing.Font("Arial", 8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.cmdPauseUnpause.ForeColor = System.Drawing.SystemColors.ControlText;
            this.cmdPauseUnpause.Location = new System.Drawing.Point(8, 20);
            this.cmdPauseUnpause.Margin = new System.Windows.Forms.Padding(4);
            this.cmdPauseUnpause.Name = "cmdPauseUnpause";
            this.cmdPauseUnpause.RightToLeft = System.Windows.Forms.RightToLeft.No;
            this.cmdPauseUnpause.Size = new System.Drawing.Size(108, 31);
            this.cmdPauseUnpause.TabIndex = 4;
            this.cmdPauseUnpause.Text = "&Pause";
            this.cmdPauseUnpause.UseVisualStyleBackColor = false;
            this.cmdPauseUnpause.Click += new System.EventHandler(this.cmdPauseUnpause_Click);
            // 
            // txtOutputDirectoryPath
            // 
            this.txtOutputDirectoryPath.AcceptsReturn = true;
            this.txtOutputDirectoryPath.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.txtOutputDirectoryPath.BackColor = System.Drawing.SystemColors.Window;
            this.txtOutputDirectoryPath.Cursor = System.Windows.Forms.Cursors.IBeam;
            this.txtOutputDirectoryPath.Font = new System.Drawing.Font("Arial", 8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.txtOutputDirectoryPath.ForeColor = System.Drawing.SystemColors.WindowText;
            this.txtOutputDirectoryPath.Location = new System.Drawing.Point(11, 39);
            this.txtOutputDirectoryPath.Margin = new System.Windows.Forms.Padding(4);
            this.txtOutputDirectoryPath.MaxLength = 0;
            this.txtOutputDirectoryPath.Name = "txtOutputDirectoryPath";
            this.txtOutputDirectoryPath.RightToLeft = System.Windows.Forms.RightToLeft.No;
            this.txtOutputDirectoryPath.Size = new System.Drawing.Size(735, 23);
            this.txtOutputDirectoryPath.TabIndex = 1;
            this.txtOutputDirectoryPath.Text = "C:\\Temp\\";
            // 
            // fraObjectTypesToScript
            // 
            this.fraObjectTypesToScript.Controls.Add(this.lstObjectTypesToScript);
            this.fraObjectTypesToScript.Font = new System.Drawing.Font("Arial", 8.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.fraObjectTypesToScript.ForeColor = System.Drawing.SystemColors.ControlText;
            this.fraObjectTypesToScript.Location = new System.Drawing.Point(397, 5);
            this.fraObjectTypesToScript.Margin = new System.Windows.Forms.Padding(4);
            this.fraObjectTypesToScript.Name = "fraObjectTypesToScript";
            this.fraObjectTypesToScript.Padding = new System.Windows.Forms.Padding(4);
            this.fraObjectTypesToScript.Size = new System.Drawing.Size(240, 155);
            this.fraObjectTypesToScript.TabIndex = 6;
            this.fraObjectTypesToScript.TabStop = false;
            this.fraObjectTypesToScript.Text = "Objects to Script";
            // 
            // fraOutputOptions
            // 
            this.fraOutputOptions.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.fraOutputOptions.BackColor = System.Drawing.SystemColors.Control;
            this.fraOutputOptions.Controls.Add(this.lblTableDataToExport);
            this.fraOutputOptions.Controls.Add(this.lblServerOutputDirectoryNamePrefix);
            this.fraOutputOptions.Controls.Add(this.txtServerOutputDirectoryNamePrefix);
            this.fraOutputOptions.Controls.Add(this.chkExportServerSettingsLoginsAndJobs);
            this.fraOutputOptions.Controls.Add(this.lblSelectDefaultDBs);
            this.fraOutputOptions.Controls.Add(this.cmdSelectDefaultDMSDBs);
            this.fraOutputOptions.Controls.Add(this.cmdSelectDefaultMTSDBs);
            this.fraOutputOptions.Controls.Add(this.lblOutputDirectoryNamePrefix);
            this.fraOutputOptions.Controls.Add(this.txtOutputDirectoryNamePrefix);
            this.fraOutputOptions.Controls.Add(this.cboTableNamesToExportSortOrder);
            this.fraOutputOptions.Controls.Add(this.cmdRefreshDBList);
            this.fraOutputOptions.Controls.Add(this.chkCreateDirectoryForEachDB);
            this.fraOutputOptions.Controls.Add(this.cmdUpdateTableNames);
            this.fraOutputOptions.Controls.Add(this.lstTableNamesToExportData);
            this.fraOutputOptions.Controls.Add(this.txtOutputDirectoryPath);
            this.fraOutputOptions.Controls.Add(this.lblOutputDirectoryPath);
            this.fraOutputOptions.Controls.Add(this.lstDatabasesToProcess);
            this.fraOutputOptions.Font = new System.Drawing.Font("Arial", 8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.fraOutputOptions.ForeColor = System.Drawing.SystemColors.ControlText;
            this.fraOutputOptions.Location = new System.Drawing.Point(21, 171);
            this.fraOutputOptions.Margin = new System.Windows.Forms.Padding(4);
            this.fraOutputOptions.Name = "fraOutputOptions";
            this.fraOutputOptions.Padding = new System.Windows.Forms.Padding(4);
            this.fraOutputOptions.RightToLeft = System.Windows.Forms.RightToLeft.No;
            this.fraOutputOptions.Size = new System.Drawing.Size(759, 312);
            this.fraOutputOptions.TabIndex = 8;
            this.fraOutputOptions.TabStop = false;
            this.fraOutputOptions.Text = "Output Options";
            // 
            // chkCreateDirectoryForEachDB
            // 
            this.chkCreateDirectoryForEachDB.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left)));
            this.chkCreateDirectoryForEachDB.BackColor = System.Drawing.SystemColors.Control;
            this.chkCreateDirectoryForEachDB.Checked = true;
            this.chkCreateDirectoryForEachDB.CheckState = System.Windows.Forms.CheckState.Checked;
            this.chkCreateDirectoryForEachDB.Cursor = System.Windows.Forms.Cursors.Default;
            this.chkCreateDirectoryForEachDB.Font = new System.Drawing.Font("Arial", 8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.chkCreateDirectoryForEachDB.ForeColor = System.Drawing.SystemColors.ControlText;
            this.chkCreateDirectoryForEachDB.Location = new System.Drawing.Point(15, 251);
            this.chkCreateDirectoryForEachDB.Margin = new System.Windows.Forms.Padding(4);
            this.chkCreateDirectoryForEachDB.Name = "chkCreateDirectoryForEachDB";
            this.chkCreateDirectoryForEachDB.RightToLeft = System.Windows.Forms.RightToLeft.No;
            this.chkCreateDirectoryForEachDB.Size = new System.Drawing.Size(223, 25);
            this.chkCreateDirectoryForEachDB.TabIndex = 7;
            this.chkCreateDirectoryForEachDB.Text = "Create directory for each DB";
            this.chkCreateDirectoryForEachDB.UseVisualStyleBackColor = false;
            // 
            // cmdUpdateTableNames
            // 
            this.cmdUpdateTableNames.BackColor = System.Drawing.SystemColors.Control;
            this.cmdUpdateTableNames.Cursor = System.Windows.Forms.Cursors.Default;
            this.cmdUpdateTableNames.Font = new System.Drawing.Font("Arial", 8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.cmdUpdateTableNames.ForeColor = System.Drawing.SystemColors.ControlText;
            this.cmdUpdateTableNames.Location = new System.Drawing.Point(549, 70);
            this.cmdUpdateTableNames.Margin = new System.Windows.Forms.Padding(4);
            this.cmdUpdateTableNames.Name = "cmdUpdateTableNames";
            this.cmdUpdateTableNames.RightToLeft = System.Windows.Forms.RightToLeft.No;
            this.cmdUpdateTableNames.Size = new System.Drawing.Size(195, 31);
            this.cmdUpdateTableNames.TabIndex = 4;
            this.cmdUpdateTableNames.Text = "Refresh &Table Names";
            this.cmdUpdateTableNames.UseVisualStyleBackColor = false;
            this.cmdUpdateTableNames.Click += new System.EventHandler(this.cmdUpdateTableNames_Click);
            // 
            // fraConnectionSettings
            // 
            this.fraConnectionSettings.BackColor = System.Drawing.SystemColors.Control;
            this.fraConnectionSettings.Controls.Add(this.chkPostgreSQL);
            this.fraConnectionSettings.Controls.Add(this.lblServerName);
            this.fraConnectionSettings.Controls.Add(this.chkUseIntegratedAuthentication);
            this.fraConnectionSettings.Controls.Add(this.txtPassword);
            this.fraConnectionSettings.Controls.Add(this.txtUsername);
            this.fraConnectionSettings.Controls.Add(this.txtServerName);
            this.fraConnectionSettings.Controls.Add(this.lblPassword);
            this.fraConnectionSettings.Controls.Add(this.lblUsername);
            this.fraConnectionSettings.Font = new System.Drawing.Font("Arial", 8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.fraConnectionSettings.ForeColor = System.Drawing.SystemColors.ControlText;
            this.fraConnectionSettings.Location = new System.Drawing.Point(21, 5);
            this.fraConnectionSettings.Margin = new System.Windows.Forms.Padding(4);
            this.fraConnectionSettings.Name = "fraConnectionSettings";
            this.fraConnectionSettings.Padding = new System.Windows.Forms.Padding(4);
            this.fraConnectionSettings.RightToLeft = System.Windows.Forms.RightToLeft.No;
            this.fraConnectionSettings.Size = new System.Drawing.Size(356, 155);
            this.fraConnectionSettings.TabIndex = 5;
            this.fraConnectionSettings.TabStop = false;
            this.fraConnectionSettings.Text = "Connection Settings";
            // 
            // chkPostgreSQL
            // 
            this.chkPostgreSQL.BackColor = System.Drawing.SystemColors.Control;
            this.chkPostgreSQL.Cursor = System.Windows.Forms.Cursors.Default;
            this.chkPostgreSQL.Font = new System.Drawing.Font("Arial", 8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.chkPostgreSQL.ForeColor = System.Drawing.SystemColors.ControlText;
            this.chkPostgreSQL.Location = new System.Drawing.Point(12, 44);
            this.chkPostgreSQL.Margin = new System.Windows.Forms.Padding(4);
            this.chkPostgreSQL.Name = "chkPostgreSQL";
            this.chkPostgreSQL.RightToLeft = System.Windows.Forms.RightToLeft.No;
            this.chkPostgreSQL.Size = new System.Drawing.Size(182, 22);
            this.chkPostgreSQL.TabIndex = 7;
            this.chkPostgreSQL.Text = "PostgreSQL";
            this.chkPostgreSQL.UseVisualStyleBackColor = false;
            this.chkPostgreSQL.CheckedChanged += new System.EventHandler(this.chkPostgreSQL_CheckedChanged);
            // 
            // lblServerName
            // 
            this.lblServerName.BackColor = System.Drawing.SystemColors.Control;
            this.lblServerName.Cursor = System.Windows.Forms.Cursors.Default;
            this.lblServerName.Font = new System.Drawing.Font("Arial", 8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblServerName.ForeColor = System.Drawing.SystemColors.ControlText;
            this.lblServerName.Location = new System.Drawing.Point(13, 20);
            this.lblServerName.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.lblServerName.Name = "lblServerName";
            this.lblServerName.RightToLeft = System.Windows.Forms.RightToLeft.No;
            this.lblServerName.Size = new System.Drawing.Size(171, 20);
            this.lblServerName.TabIndex = 0;
            this.lblServerName.Text = "Server Name";
            // 
            // chkUseIntegratedAuthentication
            // 
            this.chkUseIntegratedAuthentication.BackColor = System.Drawing.SystemColors.Control;
            this.chkUseIntegratedAuthentication.Checked = true;
            this.chkUseIntegratedAuthentication.CheckState = System.Windows.Forms.CheckState.Checked;
            this.chkUseIntegratedAuthentication.Cursor = System.Windows.Forms.Cursors.Default;
            this.chkUseIntegratedAuthentication.Font = new System.Drawing.Font("Arial", 8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.chkUseIntegratedAuthentication.ForeColor = System.Drawing.SystemColors.ControlText;
            this.chkUseIntegratedAuthentication.Location = new System.Drawing.Point(8, 126);
            this.chkUseIntegratedAuthentication.Margin = new System.Windows.Forms.Padding(4);
            this.chkUseIntegratedAuthentication.Name = "chkUseIntegratedAuthentication";
            this.chkUseIntegratedAuthentication.RightToLeft = System.Windows.Forms.RightToLeft.No;
            this.chkUseIntegratedAuthentication.Size = new System.Drawing.Size(288, 20);
            this.chkUseIntegratedAuthentication.TabIndex = 2;
            this.chkUseIntegratedAuthentication.Text = "Integrated authentication";
            this.chkUseIntegratedAuthentication.UseVisualStyleBackColor = false;
            this.chkUseIntegratedAuthentication.CheckedChanged += new System.EventHandler(this.chkUseIntegratedAuthentication_CheckedChanged);
            // 
            // txtPassword
            // 
            this.txtPassword.AcceptsReturn = true;
            this.txtPassword.BackColor = System.Drawing.SystemColors.Window;
            this.txtPassword.Cursor = System.Windows.Forms.Cursors.IBeam;
            this.txtPassword.Font = new System.Drawing.Font("Arial", 8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.txtPassword.ForeColor = System.Drawing.SystemColors.WindowText;
            this.txtPassword.Location = new System.Drawing.Point(192, 95);
            this.txtPassword.Margin = new System.Windows.Forms.Padding(4);
            this.txtPassword.MaxLength = 0;
            this.txtPassword.Name = "txtPassword";
            this.txtPassword.PasswordChar = '*';
            this.txtPassword.RightToLeft = System.Windows.Forms.RightToLeft.No;
            this.txtPassword.Size = new System.Drawing.Size(152, 23);
            this.txtPassword.TabIndex = 6;
            this.txtPassword.Text = "mt4fun";
            // 
            // txtUsername
            // 
            this.txtUsername.AcceptsReturn = true;
            this.txtUsername.BackColor = System.Drawing.SystemColors.Window;
            this.txtUsername.Cursor = System.Windows.Forms.Cursors.IBeam;
            this.txtUsername.Font = new System.Drawing.Font("Arial", 8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.txtUsername.ForeColor = System.Drawing.SystemColors.WindowText;
            this.txtUsername.Location = new System.Drawing.Point(192, 65);
            this.txtUsername.Margin = new System.Windows.Forms.Padding(4);
            this.txtUsername.MaxLength = 0;
            this.txtUsername.Name = "txtUsername";
            this.txtUsername.RightToLeft = System.Windows.Forms.RightToLeft.No;
            this.txtUsername.Size = new System.Drawing.Size(152, 23);
            this.txtUsername.TabIndex = 4;
            this.txtUsername.Text = "mtuser";
            // 
            // txtServerName
            // 
            this.txtServerName.AcceptsReturn = true;
            this.txtServerName.BackColor = System.Drawing.SystemColors.Window;
            this.txtServerName.Cursor = System.Windows.Forms.Cursors.IBeam;
            this.txtServerName.Font = new System.Drawing.Font("Arial", 8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.txtServerName.ForeColor = System.Drawing.SystemColors.WindowText;
            this.txtServerName.Location = new System.Drawing.Point(192, 17);
            this.txtServerName.Margin = new System.Windows.Forms.Padding(4);
            this.txtServerName.MaxLength = 0;
            this.txtServerName.Name = "txtServerName";
            this.txtServerName.RightToLeft = System.Windows.Forms.RightToLeft.No;
            this.txtServerName.Size = new System.Drawing.Size(152, 23);
            this.txtServerName.TabIndex = 1;
            this.txtServerName.Text = "Pogo";
            // 
            // lblPassword
            // 
            this.lblPassword.BackColor = System.Drawing.SystemColors.Control;
            this.lblPassword.Cursor = System.Windows.Forms.Cursors.Default;
            this.lblPassword.Font = new System.Drawing.Font("Arial", 8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblPassword.ForeColor = System.Drawing.SystemColors.ControlText;
            this.lblPassword.Location = new System.Drawing.Point(11, 95);
            this.lblPassword.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.lblPassword.Name = "lblPassword";
            this.lblPassword.RightToLeft = System.Windows.Forms.RightToLeft.No;
            this.lblPassword.Size = new System.Drawing.Size(96, 20);
            this.lblPassword.TabIndex = 5;
            this.lblPassword.Text = "Password";
            // 
            // lblUsername
            // 
            this.lblUsername.BackColor = System.Drawing.SystemColors.Control;
            this.lblUsername.Cursor = System.Windows.Forms.Cursors.Default;
            this.lblUsername.Font = new System.Drawing.Font("Arial", 8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblUsername.ForeColor = System.Drawing.SystemColors.ControlText;
            this.lblUsername.Location = new System.Drawing.Point(11, 70);
            this.lblUsername.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.lblUsername.Name = "lblUsername";
            this.lblUsername.RightToLeft = System.Windows.Forms.RightToLeft.No;
            this.lblUsername.Size = new System.Drawing.Size(96, 25);
            this.lblUsername.TabIndex = 3;
            this.lblUsername.Text = "Username";
            // 
            // frmMain
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(800, 599);
            this.Controls.Add(this.fraStatus);
            this.Controls.Add(this.fraControls);
            this.Controls.Add(this.fraObjectTypesToScript);
            this.Controls.Add(this.fraOutputOptions);
            this.Controls.Add(this.fraConnectionSettings);
            this.Menu = this.MainMenuControl;
            this.Name = "frmMain";
            this.Text = "DB Schema Export Tool";
            this.FormClosing += new System.Windows.Forms.FormClosingEventHandler(this.frmMain_FormClosing);
            this.fraStatus.ResumeLayout(false);
            this.fraControls.ResumeLayout(false);
            this.fraObjectTypesToScript.ResumeLayout(false);
            this.fraOutputOptions.ResumeLayout(false);
            this.fraOutputOptions.PerformLayout();
            this.fraConnectionSettings.ResumeLayout(false);
            this.fraConnectionSettings.PerformLayout();
            this.ResumeLayout(false);

        }

        #endregion

        internal System.Windows.Forms.ListBox lstObjectTypesToScript;
        internal System.Windows.Forms.MenuItem mnuHelp;
        internal System.Windows.Forms.MenuItem mnuHelpAbout;
        internal System.Windows.Forms.Label lblTableDataToExport;
        internal System.Windows.Forms.Label lblServerOutputDirectoryNamePrefix;
        internal System.Windows.Forms.Label lblProgress;
        internal System.Windows.Forms.TextBox txtServerOutputDirectoryNamePrefix;
        internal System.Windows.Forms.CheckBox chkExportServerSettingsLoginsAndJobs;
        internal System.Windows.Forms.Label lblSelectDefaultDBs;
        internal System.Windows.Forms.Button cmdSelectDefaultDMSDBs;
        internal System.Windows.Forms.Button cmdSelectDefaultMTSDBs;
        internal System.Windows.Forms.Label lblOutputDirectoryNamePrefix;
        internal System.Windows.Forms.TextBox txtOutputDirectoryNamePrefix;
        internal System.Windows.Forms.ComboBox cboTableNamesToExportSortOrder;
        internal System.Windows.Forms.Button cmdRefreshDBList;
        internal System.Windows.Forms.ListBox lstTableNamesToExportData;
        internal System.Windows.Forms.Label lblOutputDirectoryPath;
        internal System.Windows.Forms.ListBox lstDatabasesToProcess;
        internal System.Windows.Forms.MenuItem mnuFileSep1;
        internal System.Windows.Forms.GroupBox fraStatus;
        internal System.Windows.Forms.ProgressBar pbarProgress;
        internal System.Windows.Forms.Label lblSubtaskProgress;
        internal System.Windows.Forms.ProgressBar pbarSubtaskProgress;
        internal System.Windows.Forms.Label lblMessage;
        internal System.Windows.Forms.MainMenu MainMenuControl;
        internal System.Windows.Forms.MenuItem mnuFile;
        internal System.Windows.Forms.MenuItem mnuFileSelectOutputDirectory;
        internal System.Windows.Forms.MenuItem mnuFileLoadOptions;
        internal System.Windows.Forms.MenuItem mnuFileSaveOptions;
        internal System.Windows.Forms.MenuItem mnuFileSep2;
        internal System.Windows.Forms.MenuItem mnuFileExit;
        internal System.Windows.Forms.MenuItem mnuEdit;
        internal System.Windows.Forms.MenuItem mnuEditStart;
        internal System.Windows.Forms.MenuItem mnuEditIncludeSystemObjects;
        internal System.Windows.Forms.MenuItem mnuEditSep1;
        internal System.Windows.Forms.MenuItem mnuEditScriptObjectsThreaded;
        internal System.Windows.Forms.MenuItem mnuEditPauseAfterEachDatabase;
        internal System.Windows.Forms.MenuItem mnuEditIncludeTimestampInScriptFileHeader;
        internal System.Windows.Forms.MenuItem mnuEditSep2;
        internal System.Windows.Forms.MenuItem mnuEditIncludeTableRowCounts;
        internal System.Windows.Forms.MenuItem mnuEditAutoSelectDefaultTableNames;
        internal System.Windows.Forms.MenuItem mnuEditSep3;
        internal System.Windows.Forms.MenuItem mnuEditWarnOnHighTableRowCount;
        internal System.Windows.Forms.MenuItem mnuEditSaveDataAsInsertIntoStatements;
        internal System.Windows.Forms.MenuItem mnuEditSep4;
        internal System.Windows.Forms.MenuItem mnuEditResetOptions;
        internal System.Windows.Forms.GroupBox fraControls;
        internal System.Windows.Forms.Button cmdGo;
        internal System.Windows.Forms.Button cmdExit;
        internal System.Windows.Forms.Button cmdAbort;
        internal System.Windows.Forms.Button cmdPauseUnpause;
        internal System.Windows.Forms.TextBox txtOutputDirectoryPath;
        internal System.Windows.Forms.GroupBox fraObjectTypesToScript;
        internal System.Windows.Forms.GroupBox fraOutputOptions;
        internal System.Windows.Forms.CheckBox chkCreateDirectoryForEachDB;
        internal System.Windows.Forms.Button cmdUpdateTableNames;
        internal System.Windows.Forms.GroupBox fraConnectionSettings;
        internal System.Windows.Forms.CheckBox chkPostgreSQL;
        internal System.Windows.Forms.Label lblServerName;
        internal System.Windows.Forms.CheckBox chkUseIntegratedAuthentication;
        internal System.Windows.Forms.TextBox txtPassword;
        internal System.Windows.Forms.TextBox txtUsername;
        internal System.Windows.Forms.TextBox txtServerName;
        internal System.Windows.Forms.Label lblPassword;
        internal System.Windows.Forms.Label lblUsername;
    }
}

