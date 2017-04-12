Public Class clsSchemaExportOptions

    ' Note: Currenly only SqlServer is supported
    Public Enum eTargetDatabaseTypeConstants
        SqlServer = 0
        MySql = 1
        Postgres = 2
        SqlLite = 3
    End Enum

    Public Property OutputFolderPath As String
    Public Property OutputFolderNamePrefix As String
    Public Property CreateFolderForEachDB As Boolean
    Public Property IncludeSystemObjects As Boolean
    Public Property IncludeTimestampInScriptFileHeader As Boolean

    Public Property ExportServerSettingsLoginsAndJobs As Boolean
    Public Property ServerOutputFolderNamePrefix As String

    Public Property SaveDataAsInsertIntoStatements As Boolean
    Public Property DatabaseTypeForInsertInto As eTargetDatabaseTypeConstants
    Public Property AutoSelectTableNamesForDataExport As Boolean

    Public Property ExportDBSchemasAndRoles As Boolean
    Public Property ExportTables As Boolean
    Public Property ExportViews As Boolean
    Public Property ExportStoredProcedures As Boolean
    Public Property ExportUserDefinedFunctions As Boolean
    Public Property ExportUserDefinedDataTypes As Boolean
    Public Property ExportUserDefinedTypes As Boolean                               ' Only supported in Sql Server 2005 or newer; see SqlServer2005OrNewer
    Public Property ExportSynonyms As Boolean

    Public Property ConnectionInfo As clsServerConnectionInfo

    Public Sub New()
        ConnectionInfo = New clsServerConnectionInfo("", True)
    End Sub

End Class
