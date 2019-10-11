Option Strict On

Public Class clsWorkingParams
    Public Property ProcessCount As Integer
    Public Property ProcessCountExpected As Integer
    Public Property OutputDirectoryPathCurrentDB As String
    Public Property CountObjectsOnly As Boolean

    ''' <summary>
    ''' Constructor
    ''' </summary>
    Public Sub New()
        Reset()
    End Sub

    Public Sub Reset()
        ProcessCount = 0
        ProcessCountExpected = 0
        OutputDirectoryPathCurrentDB = String.Empty
        CountObjectsOnly = True
    End Sub
End Class
