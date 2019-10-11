Option Strict On

Public Class clsServerConnectionInfo
    Public Property ServerName As String
    Public Property UserName As String
    Public Property Password As String
    Public Property UseIntegratedAuthentication As Boolean

    Public Sub New(server As String, useIntegrated As Boolean)
        Reset()
        ServerName = server
        UseIntegratedAuthentication = useIntegrated
    End Sub

    Public Sub Reset()
        ServerName = String.Empty
        UserName = String.Empty
        Password = String.Empty
        UseIntegratedAuthentication = True
    End Sub
End Class
