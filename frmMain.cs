using PRISM;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Windows.Forms;
using PRISM.FileProcessor;
using ShFolderBrowser.FolderBrowser;

namespace DB_Schema_Export_Tool
{
    public partial class frmMain : Form
    {
        public frmMain()
        {
            InitializeComponent();

            InitializeControls();
        }

        #region "Constants and Enums"

        private const string XML_SETTINGS_FILE_NAME = "DB_Schema_Export_Tool_Settings.xml";
        private const string XML_SECTION_DATABASE_SETTINGS = "DBSchemaExportDatabaseSettings";
        private const string XML_SECTION_PROGRAM_OPTIONS = "DBSchemaExportOptions";
        private const int THREAD_WAIT_MSEC = 150;
        private const string ROW_COUNT_SEPARATOR = "\t (";

        private enum TableNameSortModeConstants
        {
            Name = 0,
            RowCount = 1
        }

        private enum MessageTypeConstants
        {
            Normal = 0,
            Debug = 1,
            Warning = 2,
            Error = 3
        }

        public enum SchemaObjectTypeConstants
        {
            SchemasAndRoles = 0,
            Tables = 1,
            Views = 2,
            StoredProcedures = 3,
            UserDefinedFunctions = 4,
            UserDefinedDataTypes = 5,
            UserDefinedTypes = 6,
            Synonyms = 7,
        }

        #endregion

        #region "Classwide variables"

        string mXmlSettingsFilePath;

        SchemaExportOptions mSchemaExportOptions;
        List<string> mDatabaseListToProcess;
        List<string> mTableNamesForDataExport;

        // Keys are table names; values are row counts, though row counts will be 0 if mCachedTableListIncludesRowCounts = False
        Dictionary<string, long> mCachedTableList;

        bool mCachedTableListIncludesRowCounts;

        SortedSet<string> mTableNamesToAutoSelect;

        // Note: Must contain valid RegEx statements (tested case-insensitive)
        SortedSet<string> mTableNameAutoSelectRegEx;

        List<string> mDefaultDMSDatabaseList;
        List<string> mDefaultMTSDatabaseList;

        bool mWorking;

        Thread mThread;

        private DBSchemaExportTool mDBSchemaExporter;

        bool mSchemaExportSuccess;

        #endregion

        #region "Delegates"

        private delegate void AppendNewMessageHandler(string message, MessageTypeConstants msgType);

        private delegate void UpdatePauseUnpauseCaptionHandler(DBSchemaExporterBase.PauseStatusConstants pauseStatus);

        private delegate void ProgressUpdateHandler(string taskDescription, float percentComplete);
        private delegate void ProgressCompleteHandler();

        private delegate void HandleDBExportStartingEventHandler(string databaseName);

        #endregion

        private void AppendNewMessage(string message, MessageTypeConstants msgType)
        {
            if (string.IsNullOrWhiteSpace(message))
                return;

            if (msgType == MessageTypeConstants.Error && !message.StartsWith("Error", StringComparison.OrdinalIgnoreCase))
            {
                lblMessage.Text = "Error: " + message;
            }
            else if (msgType == MessageTypeConstants.Warning && !message.StartsWith("Warning", StringComparison.OrdinalIgnoreCase))
            {
                lblMessage.Text = "Warning: " + message;
            }
            else
            {
                lblMessage.Text = message;
            }

            Application.DoEvents();
        }

        private void ConfirmAbortRequest()
        {
            if (mDBSchemaExporter == null)
                return;

            var pauseStatusSaved = mDBSchemaExporter.PauseStatus;

            mDBSchemaExporter.RequestPause();
            Application.DoEvents();
            var eResponse = MessageBox.Show("Are you sure you want to abort processing?", "Abort", MessageBoxButtons.YesNoCancel,
                                            MessageBoxIcon.Question, MessageBoxDefaultButton.Button1);
            if (eResponse == DialogResult.Yes)
            {
                mDBSchemaExporter.AbortProcessingNow();

                // Note that AbortProcessingNow should have called RequestUnpause, but we'll call it here just in case
                mDBSchemaExporter.RequestUnpause();
            }
            else if (pauseStatusSaved == DBSchemaExporterBase.PauseStatusConstants.Unpaused
                     || pauseStatusSaved == DBSchemaExporterBase.PauseStatusConstants.UnpauseRequested)
            {
                mDBSchemaExporter.RequestUnpause();
            }

            Application.DoEvents();

        }

        private void EnableDisableControls()
        {
            try
            {
                txtUsername.Enabled = !chkUseIntegratedAuthentication.Checked || chkPostgreSQL.Checked;
                txtPassword.Enabled = !chkUseIntegratedAuthentication.Checked || chkPostgreSQL.Checked;
                chkUseIntegratedAuthentication.Enabled = !chkPostgreSQL.Checked;

                cmdGo.Visible = !mWorking;
                cmdExit.Visible = !mWorking;
                fraConnectionSettings.Enabled = !mWorking;
                fraOutputOptions.Enabled = !mWorking;

                mnuEditStart.Enabled = !mWorking;
                mnuEditResetOptions.Enabled = !mWorking;
            }
            catch (Exception ex)
            {
                MessageBox.Show("Error in EnableDisableControls: " + ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation);
            }

        }

        private string GetAppDirectoryPath()
        {
            return GetAppGetAppDirectoryPath(true);
        }

        private string GetAppGetAppDirectoryPath(bool returnParentIfDirectoryNamedDebug)
        {
            const string DEBUG_DIRECTORY_NAME = @"\debug";

            var appDirectoryPath = ProcessFilesOrDirectoriesBase.GetAppDirectoryPath();

            if (returnParentIfDirectoryNamedDebug)
            {
                if (appDirectoryPath.ToLower().EndsWith(DEBUG_DIRECTORY_NAME))
                {
                    return appDirectoryPath.Substring(0, appDirectoryPath.Length - DEBUG_DIRECTORY_NAME.Length);
                }

            }

            return appDirectoryPath;
        }

        private List<string> GetSelectedDatabases()
        {
            return GetSelectedListboxItems(lstDatabasesToProcess);
        }

        private List<string> GetSelectedTableNamesForDataExport(bool warnIfRowCountOverThreshold)
        {
            var lstTableNames = GetSelectedListboxItems(lstTableNamesToExportData);

            StripRowCountsFromTableNames(lstTableNames);

            if (lstTableNames.Count == 0 || !mCachedTableListIncludesRowCounts || !warnIfRowCountOverThreshold)
            {
                return lstTableNames;
            }

            var lstValidTableNames = new List<string>(lstTableNames.Count);

            // See if any of the tables in lstTableNames has more than DBSchemaExporterBase.DATA_ROW_COUNT_WARNING_THRESHOLD rows
            foreach (var tableName in lstTableNames)
            {
                var keepTable = true;

                if (mCachedTableList.TryGetValue(tableName, out var tableRowCount))
                {
                    if (tableRowCount >= DBSchemaExporterBase.DATA_ROW_COUNT_WARNING_THRESHOLD)
                    {
                        var msg = string.Format("Warning, table {0} has {1} rows. Are you sure you want to export data from it?",
                                                tableName, tableRowCount);
                        var caption = "Row Count Over " + DBSchemaExporterBase.DATA_ROW_COUNT_WARNING_THRESHOLD;

                        var eResponse = MessageBox.Show(msg, caption, MessageBoxButtons.YesNoCancel, MessageBoxIcon.Question,
                                                        MessageBoxDefaultButton.Button2);

                        if (eResponse == DialogResult.No)
                        {
                            keepTable = false;
                        }
                        else if (eResponse == DialogResult.Cancel)
                        {
                            break;
                        }
                    }
                }
                // ReSharper disable once RedundantIfElseBlock
                else
                {
                    // Table not found; keep it anyway
                }

                if (keepTable)
                {
                    lstValidTableNames.Add(tableName);
                }
            }

            return lstValidTableNames;
        }

        private List<string> GetSelectedListboxItems(ListBox listBox)
        {
            var lstItems = new List<string>(listBox.SelectedItems.Count);

            try
            {
                lstItems.AddRange(from object item in listBox.SelectedItems select item.ToString());
            }
            catch (Exception)
            {
                // Ignore errors here
            }

            return lstItems;
        }

        private string GetSettingsFilePath()
        {
            return Path.Combine(GetAppDirectoryPath(), XML_SETTINGS_FILE_NAME);
        }

        private void HandleDBExportStartingEvent(string databaseName)
        {
            try
            {
                lblProgress.Text = "Exporting schema from " + databaseName;

                if (mnuEditPauseAfterEachDatabase.Checked)
                {
                    mDBSchemaExporter.RequestPause();
                }

            }
            catch (Exception ex)
            {
                MessageBox.Show("Error in HandleDBExportStartingEvent: " + ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation);
            }

        }

        private void InitializeProgressBars()
        {
            lblProgress.Text = string.Empty;
            pbarProgress.Minimum = 0;
            pbarProgress.Maximum = 100;
            pbarProgress.Value = 0;
        }

        /// <summary>
        /// Prompts the user to select a file to load the options from
        /// </summary>
        private void IniFileLoadOptions()
        {
            var fileDialog = new OpenFileDialog
            {
                AddExtension = true,
                CheckFileExists = true,
                CheckPathExists = true,
                DefaultExt = ".xml",
                DereferenceLinks = true,
                Multiselect = false,
                ValidateNames = true,
                Filter = "Settings files (*.xml)|*.xml|All files (*.*)|*.*",
                FilterIndex = 1
            };

            var filePath = string.Copy(mXmlSettingsFilePath);
            if (filePath.Length > 0)
            {
                try
                {
                    fileDialog.InitialDirectory = Directory.GetParent(filePath).ToString();
                }
                catch
                {
                    fileDialog.InitialDirectory = GetAppDirectoryPath();
                }

            }
            else
            {
                fileDialog.InitialDirectory = GetAppDirectoryPath();
            }

            fileDialog.FileName = string.Empty;
            fileDialog.Title = "Specify file to load options from";

            fileDialog.ShowDialog();
            if (fileDialog.FileName.Length > 0)
            {
                mXmlSettingsFilePath = fileDialog.FileName;
                IniFileLoadOptions(mXmlSettingsFilePath, true, true);
            }

        }

        private void IniFileLoadOptions(string filePath, bool resetToDefaultsPriorToLoad, bool connectToServer)
        {
            // Loads options from the given file
            var xmlFile = new XmlSettingsFileAccessor();

            try
            {
                if (resetToDefaultsPriorToLoad)
                {
                    ResetToDefaults(false);
                }

                // Sleep for 100 msec, just to be safe
                Thread.Sleep(100);

                // Read the settings from the XML file
                // Pass True to .LoadSettings() to turn off case sensitive matching
                xmlFile.LoadSettings(filePath, false);
                try
                {
                    this.Width = xmlFile.GetParam(XML_SECTION_PROGRAM_OPTIONS, "WindowWidth", this.Width);
                    this.Height = xmlFile.GetParam(XML_SECTION_PROGRAM_OPTIONS, "WindowHeight", this.Height);

                    var strServerNameSaved = txtServerName.Text;
                    txtServerName.Text = xmlFile.GetParam(XML_SECTION_DATABASE_SETTINGS, "ServerName", txtServerName.Text);
                    chkPostgreSQL.Checked = xmlFile.GetParam(XML_SECTION_DATABASE_SETTINGS, "PostgreSQL", chkPostgreSQL.Checked);

                    chkUseIntegratedAuthentication.Checked = xmlFile.GetParam(XML_SECTION_DATABASE_SETTINGS, "UseIntegratedAuthentication", chkUseIntegratedAuthentication.Checked);
                    txtUsername.Text = xmlFile.GetParam(XML_SECTION_DATABASE_SETTINGS, "Username", txtUsername.Text);
                    txtPassword.Text = xmlFile.GetParam(XML_SECTION_DATABASE_SETTINGS, "Password", txtPassword.Text);

                    txtOutputDirectoryPath.Text = xmlFile.GetParam(XML_SECTION_PROGRAM_OPTIONS, "OutputDirectoryPath", txtOutputDirectoryPath.Text);

                    mnuEditScriptObjectsThreaded.Checked = xmlFile.GetParam(XML_SECTION_PROGRAM_OPTIONS, "ScriptObjectsThreaded", mnuEditScriptObjectsThreaded.Checked);
                    mnuEditPauseAfterEachDatabase.Checked = xmlFile.GetParam(XML_SECTION_PROGRAM_OPTIONS, "PauseAfterEachDatabase", mnuEditPauseAfterEachDatabase.Checked);
                    mnuEditIncludeTimestampInScriptFileHeader.Checked = xmlFile.GetParam(XML_SECTION_PROGRAM_OPTIONS, "IncludeTimestampInScriptFileHeader", mnuEditIncludeTimestampInScriptFileHeader.Checked);

                    chkCreateDirectoryForEachDB.Checked = xmlFile.GetParam(XML_SECTION_PROGRAM_OPTIONS, "CreateDirectoryForEachDB", chkCreateDirectoryForEachDB.Checked);
                    txtOutputDirectoryNamePrefix.Text = xmlFile.GetParam(XML_SECTION_PROGRAM_OPTIONS, "OutputDirectoryNamePrefix", txtOutputDirectoryNamePrefix.Text);

                    chkExportServerSettingsLoginsAndJobs.Checked = xmlFile.GetParam(XML_SECTION_PROGRAM_OPTIONS, "ExportServerSettingsLoginsAndJobs", chkExportServerSettingsLoginsAndJobs.Checked);
                    txtServerOutputDirectoryNamePrefix.Text = xmlFile.GetParam(XML_SECTION_PROGRAM_OPTIONS, "ServerOutputDirectoryNamePrefix", txtServerOutputDirectoryNamePrefix.Text);

                    mnuEditIncludeTableRowCounts.Checked = xmlFile.GetParam(XML_SECTION_PROGRAM_OPTIONS, "IncludeTableRowCounts", mnuEditIncludeTableRowCounts.Checked);
                    mnuEditAutoSelectDefaultTableNames.Checked = xmlFile.GetParam(XML_SECTION_PROGRAM_OPTIONS, "AutoSelectDefaultTableNames", mnuEditAutoSelectDefaultTableNames.Checked);
                    mnuEditSaveDataAsInsertIntoStatements.Checked = xmlFile.GetParam(XML_SECTION_PROGRAM_OPTIONS, "SaveDataAsInsertIntoStatements", mnuEditSaveDataAsInsertIntoStatements.Checked);
                    mnuEditWarnOnHighTableRowCount.Checked = xmlFile.GetParam(XML_SECTION_PROGRAM_OPTIONS, "WarnOnHighTableRowCount", mnuEditWarnOnHighTableRowCount.Checked);

                    if (lstDatabasesToProcess.Items.Count == 0 ||
                        strServerNameSaved != null && !strServerNameSaved.Equals(txtServerName.Text, StringComparison.OrdinalIgnoreCase))
                    {
                        if (connectToServer)
                        {
                            UpdateDatabaseList();
                        }

                    }

                }
                catch (Exception)
                {
                    var msg = "Invalid parameter in settings file: " + Path.GetFileName(filePath);
                    MessageBox.Show(msg, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation);
                }
            }
            catch (Exception)
            {
                var msg = "Error loading settings from file: " + filePath;
                MessageBox.Show(msg, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation);
            }

        }

        /// <summary>
        /// Prompts the user to select a file to load the options from
        /// </summary>
        void IniFileSaveOptions()
        {

            var fileDialog = new SaveFileDialog
            {
                AddExtension = true,
                CheckFileExists = false,
                CheckPathExists = true,
                DefaultExt = ".xml",
                DereferenceLinks = true,
                OverwritePrompt = false,
                ValidateNames = true,
                Filter = "Settings files (*.xml)|*.xml|All files (*.*)|*.*",
                FilterIndex = 1
            };

            var filePath = string.Copy(mXmlSettingsFilePath);
            if (filePath.Length > 0)
            {
                try
                {
                    fileDialog.InitialDirectory = Directory.GetParent(filePath).ToString();
                }
                catch
                {
                    fileDialog.InitialDirectory = GetAppDirectoryPath();
                }
            }
            else
            {
                fileDialog.InitialDirectory = GetAppDirectoryPath();
            }

            if (File.Exists(filePath))
            {
                fileDialog.FileName = Path.GetFileName(filePath);
            }
            else
            {
                fileDialog.FileName = XML_SETTINGS_FILE_NAME;
            }

            fileDialog.Title = "Specify file to save options to";
            fileDialog.ShowDialog();

            if (fileDialog.FileName.Length > 0)
            {
                mXmlSettingsFilePath = fileDialog.FileName;
                IniFileSaveOptions(mXmlSettingsFilePath);
            }
        }


        private void IniFileSaveOptions(string filePath, bool saveWindowDimensionsOnly = false)
        {
            var xmlFile = new XmlSettingsFileAccessor();

            try
            {
                // Pass True to .LoadSettings() here so that newly made Xml files will have the correct capitalization
                xmlFile.LoadSettings(filePath, true);
                try
                {
                    xmlFile.SetParam(XML_SECTION_PROGRAM_OPTIONS, "WindowWidth", this.Width);
                    xmlFile.SetParam(XML_SECTION_PROGRAM_OPTIONS, "WindowHeight", this.Height - 20);
                    if (!saveWindowDimensionsOnly)
                    {
                        xmlFile.SetParam(XML_SECTION_DATABASE_SETTINGS, "ServerName", txtServerName.Text);
                        xmlFile.SetParam(XML_SECTION_DATABASE_SETTINGS, "PostgreSQL", chkPostgreSQL.Checked);

                        xmlFile.SetParam(XML_SECTION_DATABASE_SETTINGS, "UseIntegratedAuthentication", chkUseIntegratedAuthentication.Checked);
                        xmlFile.SetParam(XML_SECTION_DATABASE_SETTINGS, "Username", txtUsername.Text);
                        xmlFile.SetParam(XML_SECTION_DATABASE_SETTINGS, "Password", txtPassword.Text);

                        xmlFile.SetParam(XML_SECTION_PROGRAM_OPTIONS, "OutputDirectoryPath", txtOutputDirectoryPath.Text);

                        xmlFile.SetParam(XML_SECTION_PROGRAM_OPTIONS, "ScriptObjectsThreaded", mnuEditScriptObjectsThreaded.Checked);
                        xmlFile.SetParam(XML_SECTION_PROGRAM_OPTIONS, "PauseAfterEachDatabase", mnuEditPauseAfterEachDatabase.Checked);
                        xmlFile.SetParam(XML_SECTION_PROGRAM_OPTIONS, "IncludeTimestampInScriptFileHeader", mnuEditIncludeTimestampInScriptFileHeader.Checked);

                        xmlFile.SetParam(XML_SECTION_PROGRAM_OPTIONS, "CreateDirectoryForEachDB", chkCreateDirectoryForEachDB.Checked);
                        xmlFile.SetParam(XML_SECTION_PROGRAM_OPTIONS, "OutputDirectoryNamePrefix", txtOutputDirectoryNamePrefix.Text);

                        xmlFile.SetParam(XML_SECTION_PROGRAM_OPTIONS, "ExportServerSettingsLoginsAndJobs", chkExportServerSettingsLoginsAndJobs.Checked);
                        xmlFile.SetParam(XML_SECTION_PROGRAM_OPTIONS, "ServerOutputDirectoryNamePrefix", txtServerOutputDirectoryNamePrefix.Text);

                        xmlFile.SetParam(XML_SECTION_PROGRAM_OPTIONS, "IncludeTableRowCounts", mnuEditIncludeTableRowCounts.Checked);
                        xmlFile.SetParam(XML_SECTION_PROGRAM_OPTIONS, "AutoSelectDefaultTableNames", mnuEditAutoSelectDefaultTableNames.Checked);
                        xmlFile.SetParam(XML_SECTION_PROGRAM_OPTIONS, "SaveDataAsInsertIntoStatements", mnuEditSaveDataAsInsertIntoStatements.Checked);
                        xmlFile.SetParam(XML_SECTION_PROGRAM_OPTIONS, "WarnOnHighTableRowCount", mnuEditWarnOnHighTableRowCount.Checked);
                    }
                }
                catch (Exception)
                {
                    var msg = "Error storing parameter in settings file: " + Path.GetFileName(filePath);
                    MessageBox.Show(msg, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation);
                }

                xmlFile.SaveSettings();
            }
            catch (Exception)
            {
                var msg = "Error saving settings to file: " + filePath;
                MessageBox.Show(msg, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation);
            }
        }

        private void PopulateTableNamesToExport(bool enableAutoSelectDefaultTableNames)
        {
            try
            {
                if (mCachedTableList.Count <= 0)
                {
                    lstTableNamesToExportData.Items.Clear();
                    return;
                }

                TableNameSortModeConstants sortOrder;
                if (cboTableNamesToExportSortOrder.SelectedIndex >= 0)
                {
                    sortOrder = (TableNameSortModeConstants)cboTableNamesToExportSortOrder.SelectedIndex;
                }
                else
                {
                    sortOrder = TableNameSortModeConstants.Name;
                }

                List<KeyValuePair<string, long>> lstSortedTables;
                if (sortOrder == TableNameSortModeConstants.RowCount)
                {
                    // Sort on RowCount
                    lstSortedTables = (from item in mCachedTableList orderby item.Value select item).ToList();
                }
                else
                {
                    // Sort on table name
                    lstSortedTables = (from item in mCachedTableList orderby item.Key select item).ToList();
                }

                // Assure that the auto-select table names are not nothing
                if (mTableNamesToAutoSelect == null)
                {
                    mTableNamesToAutoSelect = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);
                }

                if (mTableNameAutoSelectRegEx == null)
                {
                    mTableNameAutoSelectRegEx = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);
                }

                // Initialize lstRegExSpecs (we'll fill it below if autoHighlightRows = True)
                const RegexOptions regexOptions = RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline;

                var lstRegExSpecs = new List<Regex>();
                bool autoHighlightRows;

                if (mnuEditAutoSelectDefaultTableNames.Checked && enableAutoSelectDefaultTableNames)
                {
                    autoHighlightRows = true;

                    foreach (var regexItem in mTableNameAutoSelectRegEx)
                    {
                        lstRegExSpecs.Add(new Regex(regexItem, regexOptions));
                    }

                }
                else
                {
                    autoHighlightRows = false;
                }

                // Cache the currently selected names so that we can re-highlight them below
                var lstSelectedTableNamesSaved = new SortedSet<string>();
                foreach (var item in lstTableNamesToExportData.SelectedItems)
                {
                    lstSelectedTableNamesSaved.Add(StripRowCountFromTableName(item.ToString()));
                }

                lstTableNamesToExportData.Items.Clear();

                foreach (var tableItem in lstSortedTables)
                {
                    // tableItem.Key is Table Name
                    // tableItem.Value is the number of rows in the table (if mCachedTableListIncludesRowCounts = True)

                    var tableName = tableItem.Key;

                    string textForRow;
                    if (mCachedTableListIncludesRowCounts)
                    {
                        textForRow = tableName + ROW_COUNT_SEPARATOR + ValueToTextEstimate(tableItem.Value);
                        if (tableItem.Value == 1)
                        {
                            textForRow += " row)";
                        }
                        else
                        {
                            textForRow += " rows)";
                        }

                    }
                    else
                    {
                        textForRow = string.Copy(tableName);
                    }

                    var itemIndex = lstTableNamesToExportData.Items.Add(textForRow);

                    var highlightCurrentRow = false;
                    if (lstSelectedTableNamesSaved.Contains(tableName))
                    {
                        // User had previously highlighted this table name; re-highlight it
                        highlightCurrentRow = true;
                    }
                    else if (autoHighlightRows)
                    {
                        // Test strTableName against the RegEx values from mTableNameAutoSelectRegEx()
                        foreach (var regexMatcher in lstRegExSpecs)
                        {
                            if (regexMatcher.Match(tableName).Success)
                            {
                                highlightCurrentRow = true;
                                break;
                            }

                        }

                        if (!highlightCurrentRow)
                        {
                            // No match: test tableName against the names in mTableNamesToAutoSelect
                            if (mTableNamesToAutoSelect.Contains(tableName))
                            {
                                highlightCurrentRow = true;
                            }

                        }

                    }

                    if (highlightCurrentRow)
                    {
                        // Highlight this table name
                        lstTableNamesToExportData.SetSelected(itemIndex, true);
                    }

                }

            }
            catch (Exception ex)
            {
                MessageBox.Show("Error in PopulateTableNamesToExport: " + ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation);
            }

        }

        private void InitializeControls()
        {
            mCachedTableList = new Dictionary<string, long>();

            mDefaultDMSDatabaseList = new List<string>();
            mDefaultMTSDatabaseList = new List<string>();

            InitializeProgressBars();

            PopulateComboBoxes();

            SetToolTips();

            ResetToDefaults(false);

            try
            {
                mXmlSettingsFilePath = GetSettingsFilePath();

                if (!File.Exists(mXmlSettingsFilePath))
                {
                    IniFileSaveOptions(mXmlSettingsFilePath);
                }

            }
            catch (Exception)
            {
                // Ignore errors here
            }

            IniFileLoadOptions(mXmlSettingsFilePath, false, false);

            EnableDisableControls();
        }

        private void InitializeDBSchemaExporter()
        {
            if (mSchemaExportOptions.PostgreSQL)
            {
                mDBSchemaExporter = new DBSchemaExportTool(mSchemaExportOptions);
            }
            else
            {
                mDBSchemaExporter = new DBSchemaExportTool(mSchemaExportOptions);
            }

            mDBSchemaExporter.DBExportStarting += mDBSchemaExporter_DBExportStarting;
            mDBSchemaExporter.StatusEvent += mDBSchemaExporter_StatusMessage;
            mDBSchemaExporter.DebugEvent += mDBSchemaExporter_DebugMessage;
            mDBSchemaExporter.WarningEvent += mDBSchemaExporter_WarningMessage;
            mDBSchemaExporter.ErrorEvent += mDBSchemaExporter_ErrorMessage;
            mDBSchemaExporter.ProgressUpdate += mDBSchemaExporter_ProgressUpdate;
            mDBSchemaExporter.ProgressComplete += mDBSchemaExporter_ProgressComplete;

            mDBSchemaExporter.DBExportStarting += mDBSchemaExporter_DBExportStarting;
            mDBSchemaExporter.PauseStatusChange += mDBSchemaExporter_PauseStatusChange;
        }

        private void PopulateComboBoxes()
        {
            try
            {
                cboTableNamesToExportSortOrder.Items.Clear();
                cboTableNamesToExportSortOrder.Items.Insert((int)TableNameSortModeConstants.Name, "Sort by Name");
                cboTableNamesToExportSortOrder.Items.Insert((int)TableNameSortModeConstants.RowCount, "Sort by Row Count");
                cboTableNamesToExportSortOrder.SelectedIndex = (int)TableNameSortModeConstants.RowCount;

                lstObjectTypesToScript.Items.Clear();
                lstObjectTypesToScript.Items.Insert((int)SchemaObjectTypeConstants.SchemasAndRoles, "Schemas and Roles");
                lstObjectTypesToScript.Items.Insert((int)SchemaObjectTypeConstants.Tables, "Tables");
                lstObjectTypesToScript.Items.Insert((int)SchemaObjectTypeConstants.Views, "Views");
                lstObjectTypesToScript.Items.Insert((int)SchemaObjectTypeConstants.StoredProcedures, "Stored Procedures");
                lstObjectTypesToScript.Items.Insert((int)SchemaObjectTypeConstants.UserDefinedFunctions, "User Defined Functions");
                lstObjectTypesToScript.Items.Insert((int)SchemaObjectTypeConstants.UserDefinedDataTypes, "User Defined Data Types");
                lstObjectTypesToScript.Items.Insert((int)SchemaObjectTypeConstants.UserDefinedTypes, "User Defined Types");
                lstObjectTypesToScript.Items.Insert((int)SchemaObjectTypeConstants.Synonyms, "Synonyms");

                // Auto-select all of the options
                int index;
                for (index = 0; index < lstObjectTypesToScript.Items.Count; index++)
                {
                    lstObjectTypesToScript.SetSelected(index, true);
                }

            }
            catch (Exception ex)
            {
                MessageBox.Show("Error in PopulateComboBoxes: " + ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation);
            }

        }

        private void ProgressUpdate(string taskDescription, float percentComplete)
        {
            lblProgress.Text = taskDescription;
            UpdateProgressBar(pbarProgress, percentComplete);
        }

        private void ProgressComplete()
        {
            pbarProgress.Value = pbarProgress.Maximum;
        }

        private void ScriptDBSchemaObjects()
        {
            string message;
            if (mWorking)
            {
                return;
            }

            try
            {
                // Validate txtOutputDirectoryPath.Text
                if (txtOutputDirectoryPath.TextLength == 0)
                {
                    txtOutputDirectoryPath.Text = GetAppDirectoryPath();
                }

                if (!Directory.Exists(txtOutputDirectoryPath.Text))
                {
                    message = "Output directory not found: " + txtOutputDirectoryPath.Text;
                    MessageBox.Show(message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation);
                    return;
                }

                UpdateSchemaExportOptions();

            }
            catch (Exception ex)
            {
                MessageBox.Show("Error initializing mSchemaExportOptions in ScriptDBSchemaObjects: " + ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation);
                return;
            }

            try
            {
                // Populate mDatabaseListToProcess and mTableNamesForDataExport
                mDatabaseListToProcess = GetSelectedDatabases();
                mTableNamesForDataExport = GetSelectedTableNamesForDataExport(mnuEditWarnOnHighTableRowCount.Checked);

                if (mDatabaseListToProcess.Count == 0 && !mSchemaExportOptions.ExportServerSettingsLoginsAndJobs)
                {
                    MessageBox.Show("No databases or tables were selected; unable to continue", "Nothing To Do", MessageBoxButtons.OK, MessageBoxIcon.Information);
                    return;
                }

            }
            catch (Exception ex)
            {
                MessageBox.Show("Error determining list of databases (and tables) to process: " + ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation);
                return;
            }

            try
            {
                if (mDBSchemaExporter == null)
                {
                    InitializeDBSchemaExporter();
                }

                if (mTableNamesToAutoSelect != null)
                {
                    mDBSchemaExporter.StoreTableNamesToAutoSelect(mTableNamesToAutoSelect);
                }

                if (mTableNameAutoSelectRegEx != null)
                {
                    mDBSchemaExporter.StoreTableNameAutoSelectRegEx(mTableNameAutoSelectRegEx);
                }

            }
            catch (Exception ex)
            {
                MessageBox.Show("Error instantiating mDBSchemaExporter and updating the data export auto-select lists: " + ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation);
                return;
            }

            try
            {
                mWorking = true;
                EnableDisableControls();

                UpdatePauseUnpauseCaption(DBSchemaExporterBase.PauseStatusConstants.Unpaused);
                Application.DoEvents();

                if (!mnuEditScriptObjectsThreaded.Checked)
                {
                    ScriptDBSchemaObjectsThread();
                }
                else
                {
                    // Use the following to call the SP on a separate thread
                    mThread = new Thread(ScriptDBSchemaObjectsThread);
                    mThread.Start();

                    Thread.Sleep(THREAD_WAIT_MSEC);
                    while (mThread.ThreadState == ThreadState.Running ||
                           mThread.ThreadState == ThreadState.AbortRequested ||
                           mThread.ThreadState == ThreadState.WaitSleepJoin ||
                           mThread.ThreadState == ThreadState.Suspended ||
                           mThread.ThreadState == ThreadState.SuspendRequested)
                    {
                        try
                        {
                            if (mThread.Join(THREAD_WAIT_MSEC))
                            {
                                // The Join succeeded, meaning the thread has finished running
                                break;
                            }
                            // else if (mRequestCancel)
                            //    mThread.Abort();

                        }
                        catch (Exception)
                        {
                            // Error joining thread; this can happen if the thread is trying to abort, so I believe we can ignore the error
                            // Sleep another THREAD_WAIT_MSEC msec, then exit the while loop
                            Thread.Sleep(THREAD_WAIT_MSEC);
                            break;
                        }

                        Application.DoEvents();
                    }

                }

                Application.DoEvents();
                if (!mSchemaExportSuccess || mDBSchemaExporter.ErrorCode != DBSchemaExporterBase.DBSchemaExportErrorCodes.NoError)
                {
                    message = string.Format("Error exporting the schema objects (ErrorCode={0}):\n{1}",
                                            mDBSchemaExporter.ErrorCode, mDBSchemaExporter.StatusMessage);
                    MessageBox.Show(message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation);
                }

            }
            catch (Exception ex)
            {
                MessageBox.Show("Error calling ScriptDBSchemaObjectsThread in ScriptDBSchemaObjects: " + ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation);
            }
            finally
            {
                mWorking = false;
                EnableDisableControls();
                UpdatePauseUnpauseCaption(DBSchemaExporterBase.PauseStatusConstants.Unpaused);
                try
                {
                    if (mThread != null && mThread.ThreadState != ThreadState.Stopped)
                    {
                        mThread.Abort();
                    }

                }
                catch (Exception)
                {
                    // Ignore errors here
                }

            }

            try
            {
                lblProgress.Text = "Schema export complete";
            }
            catch (Exception ex)
            {
                MessageBox.Show("Error finalizing results in ScriptDBSchemaObjects: " + ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation);
            }

        }

        private void ScriptDBSchemaObjectsThread()
        {
            // Note: Populate mDatabaseListToProcess and mTableNamesForDataExport prior to calling this sub
            try
            {
                mSchemaExportSuccess = mDBSchemaExporter.ScriptServerAndDBObjects(mDatabaseListToProcess, mTableNamesForDataExport);
            }
            catch (Exception ex)
            {
                MessageBox.Show("Error in ScriptDBSchemaObjectsThread: " + ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation);
            }

        }

        private void SelectDefaultDBs(IReadOnlyCollection<string> lstDefaultDBs)
        {
            if (lstDefaultDBs != null)
            {
                var lstSortedDBs = (from item in lstDefaultDBs select item.ToLower()).ToList();

                lstSortedDBs.Sort();

                lstDatabasesToProcess.ClearSelected();

                for (var index = 0; index < lstDatabasesToProcess.Items.Count; index++)
                {
                    var currentDatabase = lstDatabasesToProcess.Items[index].ToString().ToLower();
                    if (lstSortedDBs.BinarySearch(currentDatabase) >= 0)
                    {
                        lstDatabasesToProcess.SetSelected(index, true);
                    }

                }

            }

        }


        private void ResetToDefaults(bool confirm)
        {
            try
            {
                if (confirm)
                {
                    var eResponse = MessageBox.Show("Are you sure you want to reset all settings to their default values?", "Reset to Defaults", MessageBoxButtons.YesNoCancel, MessageBoxIcon.Question, MessageBoxDefaultButton.Button1);
                    if (eResponse != DialogResult.Yes)
                    {
                        return;
                    }
                }

                this.Width = 600;
                this.Height = 600;

                txtServerName.Text = DBSchemaExporterSQLServer.SQL_SERVER_NAME_DEFAULT;

                chkUseIntegratedAuthentication.Checked = true;
                chkPostgreSQL.Checked = false;
                txtUsername.Text = DBSchemaExporterSQLServer.SQL_SERVER_USERNAME_DEFAULT;
                txtPassword.Text = DBSchemaExporterSQLServer.SQL_SERVER_PASSWORD_DEFAULT;

                mnuEditScriptObjectsThreaded.Checked = false;
                mnuEditIncludeSystemObjects.Checked = false;
                mnuEditPauseAfterEachDatabase.Checked = false;
                mnuEditIncludeTimestampInScriptFileHeader.Checked = false;

                mnuEditIncludeTableRowCounts.Checked = true;
                mnuEditAutoSelectDefaultTableNames.Checked = true;
                mnuEditSaveDataAsInsertIntoStatements.Checked = true;
                mnuEditWarnOnHighTableRowCount.Checked = true;

                chkCreateDirectoryForEachDB.Checked = true;
                txtOutputDirectoryNamePrefix.Text = SchemaExportOptions.DEFAULT_DB_OUTPUT_DIRECTORY_NAME_PREFIX;

                chkExportServerSettingsLoginsAndJobs.Checked = false;
                txtServerOutputDirectoryNamePrefix.Text = SchemaExportOptions.DEFAULT_SERVER_OUTPUT_DIRECTORY_NAME_PREFIX;

                mTableNamesToAutoSelect = DBSchemaExportTool.GetTableNamesToAutoExportData(chkPostgreSQL.Checked);
                mTableNameAutoSelectRegEx = DBSchemaExportTool.GetTableRegExToAutoExportData();

                mDefaultDMSDatabaseList.Clear();

                if (chkPostgreSQL.Checked)
                {
                    mDefaultDMSDatabaseList.Add("dms");
                    mDefaultDMSDatabaseList.Add("mts");
                }
                else
                {
                    mDefaultDMSDatabaseList.Add("DMS5");
                    mDefaultDMSDatabaseList.Add("DMS_Capture");
                    mDefaultDMSDatabaseList.Add("DMS_Data_Package");
                    mDefaultDMSDatabaseList.Add("DMS_Pipeline");
                    mDefaultDMSDatabaseList.Add("Ontology_Lookup");
                    mDefaultDMSDatabaseList.Add("Protein_Sequences");
                    mDefaultDMSDatabaseList.Add("DMSHistoricLog1");

                    mDefaultMTSDatabaseList.Clear();
                    mDefaultMTSDatabaseList.Add("MT_Template_01");
                    mDefaultMTSDatabaseList.Add("PT_Template_01");
                    mDefaultMTSDatabaseList.Add("MT_Main");
                    mDefaultMTSDatabaseList.Add("MTS_Master");
                    mDefaultMTSDatabaseList.Add("Prism_IFC");
                    mDefaultMTSDatabaseList.Add("Prism_RPT");
                    mDefaultMTSDatabaseList.Add("PAST_BB");
                    mDefaultMTSDatabaseList.Add("Master_Sequences");
                    mDefaultMTSDatabaseList.Add("MT_Historic_Log");
                    mDefaultMTSDatabaseList.Add("MT_HistoricLog");
                }

                EnableDisableControls();
            }
            catch (Exception ex)
            {
                MessageBox.Show("Error in ResetToDefaults: " + ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation);
            }

        }

        private void SelectAllListboxItems(ListBox listBox)
        {
            try
            {
                for (var index = 0; index < listBox.Items.Count; index++)
                {
                    listBox.SetSelected(index, true);
                }

            }
            catch (Exception)
            {
                // Ignore errors here
            }

        }

        private void SelectOutputDirectory()
        {
            // Prompts the user to select the output directory to create the scripted objects in
            try
            {
                var folderBrowser = new FolderBrowser();
                bool success;

                if ((txtOutputDirectoryPath.TextLength > 0))
                {
                    success = folderBrowser.BrowseForFolder(txtOutputDirectoryPath.Text);
                }
                else
                {
                    success = folderBrowser.BrowseForFolder(GetAppDirectoryPath());
                }

                if (success)
                {
                    txtOutputDirectoryPath.Text = folderBrowser.FolderPath;
                }

            }
            catch (Exception ex)
            {
                MessageBox.Show(("Error in SelectOutputDirectory: " + ex.Message), "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation);
            }

        }
        private void SetToolTips()
        {
            try
            {
                var toolTipControl = new ToolTip();
                toolTipControl.SetToolTip(chkCreateDirectoryForEachDB, "This will be automatically enabled if multiple databases are chosen above");
                toolTipControl.SetToolTip(txtOutputDirectoryNamePrefix, "The output directory for each database will be named with this prefix followed by the database name");
                toolTipControl.SetToolTip(txtServerOutputDirectoryNamePrefix, "Server settings will be saved in a directory with this prefix followed by the server name");
            }
            catch (Exception ex)
            {
                MessageBox.Show(("Error in SetToolTips: " + ex.Message), "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation);
            }

        }

        private void ShowAboutBox()
        {
            var message = new StringBuilder();
            message.AppendLine("Program written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA) in August 2006");
            message.AppendLine("Copyright 2006, Battelle Memorial Institute.  All Rights Reserved.");
            message.AppendLine();
            message.AppendLine("This is version " + Application.ProductVersion + " (" + SchemaExportOptions.PROGRAM_DATE + "). ");
            message.AppendLine();

            message.AppendLine("E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov");
            message.AppendLine("Website: https://omics.pnl.gov/ or https://panomics.pnnl.gov/");
            message.AppendLine();
            message.AppendLine("Licensed under the 2-Clause BSD License; you may not use this file except " +
                               "in compliance with the License.  You may obtain a copy of the License " +
                               "at https://opensource.org/licenses/BSD-2-Clause");
            message.AppendLine();

            MessageBox.Show(message.ToString(), "About", MessageBoxButtons.OK, MessageBoxIcon.Information);
        }

        private void StripRowCountsFromTableNames(IList<string> tableNames)
        {
            int index;
            if (tableNames == null)
            {
                return;
            }

            for (index = 0; index < tableNames.Count; index++)
            {
                tableNames[index] = StripRowCountFromTableName(tableNames[index]);
            }

        }

        private string StripRowCountFromTableName(string tableName)
        {
            if (string.IsNullOrWhiteSpace(tableName))
            {
                return string.Empty;
            }

            var intCharIndex = tableName.IndexOf(ROW_COUNT_SEPARATOR, StringComparison.Ordinal);
            if (intCharIndex > 0)
            {
                return tableName.Substring(0, intCharIndex);
            }

            return tableName;
        }


        private void UpdateDatabaseList()
        {
            if (mWorking)
            {
                return;
            }

            try
            {
                mWorking = true;
                EnableDisableControls();

                UpdatePauseUnpauseCaption(DBSchemaExporterBase.PauseStatusConstants.Unpaused);
                Application.DoEvents();

                var connectionDatabase = chkPostgreSQL.Checked ? DBSchemaExporterPostgreSQL.POSTGRES_DATABASE : string.Empty;

                if (!VerifyOrUpdateServerConnection(connectionDatabase, true))
                    return;

                // Cache the currently selected names so that we can re-highlight them below
                var selectedDatabaseNamesSaved = new SortedSet<string>();
                foreach (var item in lstDatabasesToProcess.SelectedItems)
                {
                    selectedDatabaseNamesSaved.Add(item.ToString());
                }

                lstDatabasesToProcess.Items.Clear();

                var databaseList = mDBSchemaExporter.GetServerDatabases();

                foreach (var databaseName in databaseList)
                {
                    if (string.IsNullOrWhiteSpace(databaseName))
                    {
                        continue;
                    }

                    var itemIndex = lstDatabasesToProcess.Items.Add(databaseName);

                    if (selectedDatabaseNamesSaved.Contains(databaseName))
                    {
                        // Highlight this table name
                        lstDatabasesToProcess.SetSelected(itemIndex, true);
                    }
                }

                AppendNewMessage(string.Format("Found {0} databases on {1}", lstDatabasesToProcess.Items.Count, txtServerName.Text), MessageTypeConstants.Normal);

            }
            catch (Exception ex)
            {
                MessageBox.Show("Error in UpdateDatabaseList: " + ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation);
            }
            finally
            {
                mWorking = false;
                EnableDisableControls();
            }

        }

        private void UpdatePauseUnpauseCaption(DBSchemaExporterBase.PauseStatusConstants ePauseStatus)
        {
            try
            {
                switch (ePauseStatus)
                {
                    case DBSchemaExporterBase.PauseStatusConstants.Paused:
                        cmdPauseUnpause.Text = "Un&pause";
                        break;
                    case DBSchemaExporterBase.PauseStatusConstants.PauseRequested:
                        cmdPauseUnpause.Text = "&Pausing";
                        break;
                    case DBSchemaExporterBase.PauseStatusConstants.Unpaused:
                        cmdPauseUnpause.Text = "&Pause";
                        break;
                    case DBSchemaExporterBase.PauseStatusConstants.UnpauseRequested:
                        cmdPauseUnpause.Text = "Un&pausing";
                        break;
                    default:
                        cmdPauseUnpause.Text = "Un&pause";
                        break;
                }
            }
            catch (Exception)
            {
                // Ignore errors here
            }

        }

        private void UpdateProgressBar(ProgressBar targetBar, float percentComplete)
        {
            var roundedValue = (int)percentComplete;
            if (roundedValue < targetBar.Minimum)
            {
                roundedValue = targetBar.Minimum;
            }

            if (roundedValue > targetBar.Maximum)
            {
                roundedValue = targetBar.Maximum;
            }

            targetBar.Value = roundedValue;
        }

        private void UpdateSchemaExportOptions()
        {
            if (mSchemaExportOptions == null)
            {
                mSchemaExportOptions = new SchemaExportOptions();
            }

            mSchemaExportOptions.OutputDirectoryPath = txtOutputDirectoryPath.Text;
            mSchemaExportOptions.DatabaseSubdirectoryPrefix = txtOutputDirectoryNamePrefix.Text;
            mSchemaExportOptions.CreateDirectoryForEachDB = chkCreateDirectoryForEachDB.Checked;
            mSchemaExportOptions.ServerOutputDirectoryNamePrefix = txtServerOutputDirectoryNamePrefix.Text;

            mSchemaExportOptions.ScriptingOptions.IncludeSystemObjects = mnuEditIncludeSystemObjects.Checked;
            mSchemaExportOptions.ScriptingOptions.IncludeTimestampInScriptFileHeader = mnuEditIncludeTimestampInScriptFileHeader.Checked;
            mSchemaExportOptions.ScriptingOptions.ExportServerSettingsLoginsAndJobs = chkExportServerSettingsLoginsAndJobs.Checked;
            mSchemaExportOptions.ScriptingOptions.SaveDataAsInsertIntoStatements = mnuEditSaveDataAsInsertIntoStatements.Checked;
            mSchemaExportOptions.ScriptingOptions.DatabaseTypeForInsertInto = DatabaseScriptingOptions.TargetDatabaseTypeConstants.SqlServer;
            mSchemaExportOptions.ScriptingOptions.AutoSelectTableNamesForDataExport = mnuEditAutoSelectDefaultTableNames.Checked;

            // Note: mDBSchemaExporter & mTableNameAutoSelectRegEx will be passed to mDBSchemaExporter below
            for (var index = 0; index < lstObjectTypesToScript.Items.Count; index++)
            {
                var selected = lstObjectTypesToScript.GetSelected(index);
                switch ((SchemaObjectTypeConstants)index)
                {
                    case SchemaObjectTypeConstants.SchemasAndRoles:
                        mSchemaExportOptions.ScriptingOptions.ExportDBSchemasAndRoles = selected;
                        break;
                    case SchemaObjectTypeConstants.Tables:
                        mSchemaExportOptions.ScriptingOptions.ExportTables = selected;
                        break;
                    case SchemaObjectTypeConstants.Views:
                        mSchemaExportOptions.ScriptingOptions.ExportViews = selected;
                        break;
                    case SchemaObjectTypeConstants.StoredProcedures:
                        mSchemaExportOptions.ScriptingOptions.ExportStoredProcedures = selected;
                        break;
                    case SchemaObjectTypeConstants.UserDefinedFunctions:
                        mSchemaExportOptions.ScriptingOptions.ExportUserDefinedFunctions = selected;
                        break;
                    case SchemaObjectTypeConstants.UserDefinedDataTypes:
                        mSchemaExportOptions.ScriptingOptions.ExportUserDefinedDataTypes = selected;
                        break;
                    case SchemaObjectTypeConstants.UserDefinedTypes:
                        mSchemaExportOptions.ScriptingOptions.ExportUserDefinedTypes = selected;
                        break;
                    case SchemaObjectTypeConstants.Synonyms:
                        mSchemaExportOptions.ScriptingOptions.ExportSynonyms = selected;
                        break;
                }
            }

            mSchemaExportOptions.ServerName = txtServerName.Text;
            mSchemaExportOptions.PostgreSQL = chkPostgreSQL.Checked;

            if (chkUseIntegratedAuthentication.Checked && !mSchemaExportOptions.PostgreSQL)
            {
                mSchemaExportOptions.DBUser = string.Empty;
                mSchemaExportOptions.DBUserPassword = string.Empty;
            }
            else
            {
                mSchemaExportOptions.DBUser = txtUsername.Text;
                mSchemaExportOptions.DBUserPassword = txtPassword.Text;
            }

        }

        private void UpdateTableNamesInSelectedDB()
        {
            string databaseName;

            if (mWorking)
            {
                return;
            }

            try
            {
                if (lstDatabasesToProcess.Items.Count == 0)
                {
                    MessageBox.Show("The database list is currently empty; unable to continue", "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation);
                    return;
                }

                if (lstDatabasesToProcess.SelectedIndex < 0)
                {
                    // Auto-select the first database
                    lstDatabasesToProcess.SelectedIndex = 0;
                }

                databaseName = lstDatabasesToProcess.Items[lstDatabasesToProcess.SelectedIndex].ToString();
            }
            catch (Exception ex)
            {
                MessageBox.Show("Error determining selected database name in UpdateTableNamesInSelectedDB: " + ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation);
                return;
            }

            if (string.IsNullOrWhiteSpace(databaseName))
            {
                return;
            }

            try
            {
                mWorking = true;
                EnableDisableControls();
                UpdatePauseUnpauseCaption(DBSchemaExporterBase.PauseStatusConstants.Unpaused);
                Application.DoEvents();

                if (!VerifyOrUpdateServerConnection(databaseName, true))
                    return;

                var includeTableRowCounts = mnuEditIncludeTableRowCounts.Checked;

                mCachedTableList = mDBSchemaExporter.GetDatabaseTableNames(databaseName, includeTableRowCounts, mnuEditIncludeSystemObjects.Checked);

                if (mCachedTableList.Count > 0)
                {
                    mCachedTableListIncludesRowCounts = includeTableRowCounts;
                    PopulateTableNamesToExport(true);
                }

            }
            catch (Exception ex)
            {
                MessageBox.Show("Error in UpdateTableNamesInSelectedDB: " + ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation);
            }
            finally
            {
                mWorking = false;
                EnableDisableControls();
            }

        }

        private string ValueToTextEstimate(long value)
        {
            // Converts value to a text-based value
            // For data between 10000 and 1 million, rounds to the nearest thousand
            // For data over 1 million, displays as x.x million

            var lngValueAbs = Math.Abs(value);
            if (lngValueAbs < 100)
            {
                return value.ToString();
            }

            if (value >= 1000000)
            {
                return Math.Round(value / 1000000.0, 1) + " million";
            }

            var divisor = Math.Pow(10, Math.Floor(Math.Log10(lngValueAbs)) - 1);

            return string.Format("{0:#,###,##0}", Math.Round(value / divisor, 0) * divisor);
        }

        private bool VerifyOrUpdateServerConnection(string databaseName, bool informUserOnFailure)
        {
            try
            {

                if (txtServerName.TextLength == 0)
                {
                    var message = "Please enter the server name";
                    if (informUserOnFailure)
                    {
                        MessageBox.Show(message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation);
                    }

                    return false;

                }

                UpdateSchemaExportOptions();
                if (mDBSchemaExporter == null)
                {
                    InitializeDBSchemaExporter();
                }

                var connected = mDBSchemaExporter.ConnectToServer(databaseName);

                if (!connected && informUserOnFailure)
                {
                    var message = "Error connecting to server " + mSchemaExportOptions.ServerName;
                    if (mDBSchemaExporter.StatusMessage.Length > 0)
                    {
                        MessageBox.Show(message + "; " + mDBSchemaExporter.StatusMessage, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation);
                    }
                    else
                    {
                        MessageBox.Show(message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation);
                    }
                }

                return connected;
            }
            catch (Exception ex)
            {
                MessageBox.Show("Error in VerifyOrUpdateServerConnection: " + ex.Message, "Error", MessageBoxButtons.OK, MessageBoxIcon.Exclamation);
                return false;
            }

        }

        #region "Control Handlers"

        private void cboTableNamesToExportSortOrder_SelectedIndexChanged(object sender, EventArgs e)
        {
            PopulateTableNamesToExport(false);
        }

        private void cmdPauseUnpause_Click(object sender, EventArgs e)
        {
            if (mDBSchemaExporter != null)
            {
                mDBSchemaExporter.TogglePause();
                if (mDBSchemaExporter.PauseStatus == DBSchemaExporterBase.PauseStatusConstants.UnpauseRequested
                    || mDBSchemaExporter.PauseStatus == DBSchemaExporterBase.PauseStatusConstants.Unpaused)
                {

                }

            }

        }

        private void chkPostgreSQL_CheckedChanged(object sender, EventArgs e)
        {
            EnableDisableControls();
        }

        private void cmdRefreshDBList_Click(object sender, EventArgs e)
        {
            UpdateDatabaseList();
        }

        private void cmdUpdateTableNames_Click(object sender, EventArgs e)
        {
            UpdateTableNamesInSelectedDB();
        }

        private void chkUseIntegratedAuthentication_CheckedChanged(object sender, EventArgs e)
        {
            EnableDisableControls();
        }

        private void cmdAbort_Click(object sender, EventArgs e)
        {
            ConfirmAbortRequest();
        }

        private void cmdGo_Click(object eventSender, EventArgs eventArgs)
        {
            ScriptDBSchemaObjects();
        }

        private void cmdSelectDefaultDMSDBs_Click(object sender, EventArgs e)
        {
            SelectDefaultDBs(mDefaultDMSDatabaseList);
        }

        private void cmdSelectDefaultMTSDBs_Click(object sender, EventArgs e)
        {
            SelectDefaultDBs(mDefaultMTSDatabaseList);
        }

        private void cmdExit_Click(object eventSender, EventArgs eventArgs)
        {
            this.Close();
        }

        private void lstDatabasesToProcess_KeyPress(object sender, KeyPressEventArgs e)
        {
            // Always mark e as handled to allow Ctrl+A to be used to select all the entries in the listbox
            e.Handled = true;
        }

        private void lstDatabasesToProcess_KeyDown(object sender, KeyEventArgs e)
        {
            if (e.Control)
            {
                if (e.KeyCode == Keys.A)
                {
                    // Ctrl+A - Select All
                    SelectAllListboxItems(lstDatabasesToProcess);
                    e.Handled = true;
                }

            }

        }

        private void lstObjectTypesToScript_KeyPress(object sender, KeyPressEventArgs e)
        {
            // Always mark e as handled to allow Ctrl+A to be used to select all the entries in the listbox
            e.Handled = true;
        }

        private void lstObjectTypesToScript_KeyDown(object sender, KeyEventArgs e)
        {
            if (e.Control)
            {
                if (e.KeyCode == Keys.A)
                {
                    // Ctrl+A - Select All
                    SelectAllListboxItems(lstObjectTypesToScript);
                    e.Handled = true;
                }

            }

        }

        private void lstTableNamesToExportData_KeyPress(object sender, KeyPressEventArgs e)
        {
            // Always mark e as handled to allow Ctrl+A to be used to select all the entries in the listbox
            e.Handled = true;
        }

        private void lstTableNamesToExportData_KeyDown(object sender, KeyEventArgs e)
        {
            if (e.Control)
            {
                if (e.KeyCode == Keys.A)
                {
                    // Ctrl+A - Select All
                    SelectAllListboxItems(lstTableNamesToExportData);
                    e.Handled = true;
                }

            }

        }

        #endregion

        #region "Form Handlers"

        private void frmMain_FormClosing(object sender, FormClosingEventArgs e)
        {
            mDBSchemaExporter?.AbortProcessingNow();
        }

        #endregion

        #region "Menu Handlers"

        private void mnuFileSelectOutputDirectory_Click(object sender, EventArgs e)
        {
            SelectOutputDirectory();
        }

        private void mnuFileSaveOptions_Click(object sender, EventArgs e)
        {
            IniFileSaveOptions();
        }

        private void mnuFileLoadOptions_Click(object sender, EventArgs e)
        {
            IniFileLoadOptions();
        }

        private void mnuFileExit_Click(object sender, EventArgs e)
        {
            this.Close();
        }

        private void mnuEditStart_Click(object sender, EventArgs e)
        {
            ScriptDBSchemaObjects();
        }

        private void mnuEditIncludeSystemObjects_Click(object sender, EventArgs e)
        {
            mnuEditIncludeSystemObjects.Checked = !mnuEditIncludeSystemObjects.Checked;
        }

        private void mnuEditScriptObjectsThreaded_Click(object sender, EventArgs e)
        {
            mnuEditScriptObjectsThreaded.Checked = !mnuEditScriptObjectsThreaded.Checked;
        }

        private void mnuEditPauseAfterEachDatabase_Click(object sender, EventArgs e)
        {
            mnuEditPauseAfterEachDatabase.Checked = !mnuEditPauseAfterEachDatabase.Checked;
        }

        private void mnuEditIncludeTimestampInScriptFileHeader_Click(object sender, EventArgs e)
        {
            mnuEditIncludeTimestampInScriptFileHeader.Checked = !mnuEditIncludeTimestampInScriptFileHeader.Checked;
        }

        private void mnuEditIncludeTableRowCounts_Click(object sender, EventArgs e)
        {
            mnuEditIncludeTableRowCounts.Checked = !mnuEditIncludeTableRowCounts.Checked;
        }

        private void mnuEditAutoSelectDefaultTableNames_Click(object sender, EventArgs e)
        {
            mnuEditAutoSelectDefaultTableNames.Checked = !mnuEditAutoSelectDefaultTableNames.Checked;
        }

        private void mnuEditSaveDataAsInsertIntoStatements_Click(object sender, EventArgs e)
        {
            mnuEditSaveDataAsInsertIntoStatements.Checked = !mnuEditSaveDataAsInsertIntoStatements.Checked;
        }

        private void mnuEditWarnOnHighTableRowCount_Click(object sender, EventArgs e)
        {
            mnuEditWarnOnHighTableRowCount.Checked = !mnuEditWarnOnHighTableRowCount.Checked;
        }

        private void mnuEditResetOptions_Click(object sender, EventArgs e)
        {
            ResetToDefaults(true);
        }

        private void mnuHelpAbout_Click(object sender, EventArgs e)
        {
            ShowAboutBox();
        }

        #endregion

        #region "DB Schema Export Events"

        private void mDBSchemaExporter_DebugMessage(string message)
        {
            if (this.InvokeRequired)
            {
                this.Invoke(new AppendNewMessageHandler(AppendNewMessage), message, true);
            }
            else
            {
                AppendNewMessage(message, MessageTypeConstants.Debug);
            }

            Application.DoEvents();
        }

        private void mDBSchemaExporter_ErrorMessage(string message, Exception ex)
        {
            if (this.InvokeRequired)
            {
                this.Invoke(new AppendNewMessageHandler(AppendNewMessage), message, true);
            }
            else
            {
                if (message != null && ex != null && message.IndexOf(ex.Message, StringComparison.OrdinalIgnoreCase) < 0)
                {
                    AppendNewMessage(message + ": " + ex.Message, MessageTypeConstants.Error);
                }
                else
                {
                    AppendNewMessage(message, MessageTypeConstants.Error);
                }
            }

            Application.DoEvents();
        }

        private void mDBSchemaExporter_StatusMessage(string message)
        {
            if (this.InvokeRequired)
            {
                this.Invoke(new AppendNewMessageHandler(AppendNewMessage), message, false);
            }
            else
            {
                AppendNewMessage(message, MessageTypeConstants.Normal);
            }

            Application.DoEvents();
        }

        private void mDBSchemaExporter_WarningMessage(string message)
        {
            if (this.InvokeRequired)
            {
                this.Invoke(new AppendNewMessageHandler(AppendNewMessage), message, false);
            }
            else
            {
                AppendNewMessage(message, MessageTypeConstants.Warning);
            }

            Application.DoEvents();
        }

        private void mDBSchemaExporter_PauseStatusChange()
        {
            if (this.InvokeRequired)
            {
                this.Invoke(new UpdatePauseUnpauseCaptionHandler(UpdatePauseUnpauseCaption), mDBSchemaExporter.PauseStatus);
            }
            else
            {
                UpdatePauseUnpauseCaption(mDBSchemaExporter.PauseStatus);
            }

            Application.DoEvents();
        }

        private void mDBSchemaExporter_ProgressUpdate(string taskDescription, float percentComplete)
        {
            if (this.InvokeRequired)
            {
                this.Invoke(new ProgressUpdateHandler(ProgressUpdate), taskDescription, percentComplete);
            }
            else
            {
                ProgressUpdate(taskDescription, percentComplete);
            }

            Application.DoEvents();
        }

        private void mDBSchemaExporter_ProgressComplete()
        {
            if (this.InvokeRequired)
            {
                this.Invoke(new ProgressCompleteHandler(ProgressComplete), new object[0]);
            }
            else
            {
                ProgressComplete();
            }

            Application.DoEvents();
        }

        private void mDBSchemaExporter_DBExportStarting(string databaseName)
        {
            if (this.InvokeRequired)
            {
                this.Invoke(new HandleDBExportStartingEventHandler(HandleDBExportStartingEvent), databaseName);
            }
            else
            {
                HandleDBExportStartingEvent(databaseName);
            }

            Application.DoEvents();
        }

        #endregion

    }
}
