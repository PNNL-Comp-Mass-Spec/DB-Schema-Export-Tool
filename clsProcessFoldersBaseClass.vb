Option Strict On

Imports System.IO
Imports System.Reflection

''' <summary>
''' This class can be used as a base class for classes that process a folder or folders
''' Note that this class contains simple error codes that
''' can be set from any derived classes.  The derived classes can also set their own local error codes
''' </summary>
''' <remarks>
''' Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA)
''' Copyright 2005, Battelle Memorial Institute.  All Rights Reserved.
''' Started April 26, 2005
''' </remarks>
Public MustInherit Class clsProcessFoldersBaseClass
	Inherits clsProcessFilesOrFoldersBase

	''' <summary>
	''' Constructor
	''' </summary>
	''' <remarks></remarks>
	Public Sub New()
		mFileDate = "October 17, 2013"
		mErrorCode = eProcessFoldersErrorCodes.NoError	
	End Sub

#Region "Constants and Enums"
	Public Enum eProcessFoldersErrorCodes
		NoError = 0
		InvalidInputFolderPath = 1
		InvalidOutputFolderPath = 2
		ParameterFileNotFound = 4
		InvalidParameterFile = 8
		FilePathError = 16
		LocalizedError = 32
		UnspecifiedError = -1
	End Enum

	'' Copy the following to any derived classes
	''Public Enum eDerivedClassErrorCodes
	''    NoError = 0
	''    UnspecifiedError = -1
	''End Enum
#End Region

#Region "Classwide Variables"
	''Private mLocalErrorCode As eDerivedClassErrorCodes

	''Public ReadOnly Property LocalErrorCode() As eDerivedClassErrorCodes
	''    Get
	''        Return mLocalErrorCode
	''    End Get
	''End Property

	Private mErrorCode As eProcessFoldersErrorCodes

#End Region

#Region "Interface Functions"

	Public ReadOnly Property ErrorCode() As eProcessFoldersErrorCodes
		Get
			Return mErrorCode
		End Get
	End Property

#End Region

	Protected Overrides Sub CleanupPaths(ByRef strInputFileOrFolderPath As String, ByRef strOutputFolderPath As String)
		CleanupFolderPaths(strInputFileOrFolderPath, strOutputFolderPath)
	End Sub

	Protected Function CleanupFolderPaths(ByRef strInputFolderPath As String, ByRef strOutputFolderPath As String) As Boolean
		' Validates that strInputFolderPath and strOutputFolderPath contain valid folder paths
		' Will ignore strOutputFolderPath if it is Nothing or empty; will create strOutputFolderPath if it does not exist
		'
		' Returns True if success, False if failure

		Dim ioFolder As DirectoryInfo
		Dim blnSuccess As Boolean

		Try
			' Make sure strInputFolderPath points to a valid folder
			ioFolder = New DirectoryInfo(strInputFolderPath)

			If Not ioFolder.Exists() Then
				If ShowMessages Then
					ShowErrorMessage("Input folder not found: " & strInputFolderPath)
				Else
					LogMessage("Input folder not found: " & strInputFolderPath, eMessageTypeConstants.ErrorMsg)
				End If
				mErrorCode = eProcessFoldersErrorCodes.InvalidInputFolderPath
				blnSuccess = False
			Else
				If String.IsNullOrWhiteSpace(strOutputFolderPath) Then
					' Define strOutputFolderPath based on strInputFolderPath
					strOutputFolderPath = ioFolder.FullName
				End If

				' Make sure strOutputFolderPath points to a folder
				ioFolder = New DirectoryInfo(strOutputFolderPath)

				If Not ioFolder.Exists() Then
					' strOutputFolderPath points to a non-existent folder; attempt to create it
					ioFolder.Create()
				End If

				mOutputFolderPath = String.Copy(ioFolder.FullName)

				blnSuccess = True
			End If

		Catch ex As Exception
			HandleException("Error cleaning up the folder paths", ex)
			Return False
		End Try

		Return blnSuccess
	End Function

	Protected Function GetBaseClassErrorMessage() As String
		' Returns String.Empty if no error

		Dim strErrorMessage As String

		Select Case ErrorCode
			Case eProcessFoldersErrorCodes.NoError
				strErrorMessage = String.Empty
			Case eProcessFoldersErrorCodes.InvalidInputFolderPath
				strErrorMessage = "Invalid input folder path"
			Case eProcessFoldersErrorCodes.InvalidOutputFolderPath
				strErrorMessage = "Invalid output folder path"
			Case eProcessFoldersErrorCodes.ParameterFileNotFound
				strErrorMessage = "Parameter file not found"
			Case eProcessFoldersErrorCodes.InvalidParameterFile
				strErrorMessage = "Invalid parameter file"
			Case eProcessFoldersErrorCodes.FilePathError
				strErrorMessage = "General file path error"
			Case eProcessFoldersErrorCodes.LocalizedError
				strErrorMessage = "Localized error"
			Case eProcessFoldersErrorCodes.UnspecifiedError
				strErrorMessage = "Unspecified error"
			Case Else
				' This shouldn't happen
				strErrorMessage = "Unknown error state"
		End Select

		Return strErrorMessage

	End Function

    Public Function ProcessFoldersWildcard(strInputFolderPath As String) As Boolean
        Return ProcessFoldersWildcard(strInputFolderPath, String.Empty, String.Empty)
    End Function

    Public Function ProcessFoldersWildcard(strInputFolderPath As String, strOutputFolderAlternatePath As String) As Boolean
        Return ProcessFoldersWildcard(strInputFolderPath, strOutputFolderAlternatePath, String.Empty)
    End Function

    Public Function ProcessFoldersWildcard(strInputFolderPath As String, strOutputFolderAlternatePath As String, strParameterFilePath As String) As Boolean
        Return ProcessFoldersWildcard(strInputFolderPath, strOutputFolderAlternatePath, strParameterFilePath, True)
    End Function

    Public Function ProcessFoldersWildcard(strInputFolderPath As String, strOutputFolderAlternatePath As String, strParameterFilePath As String, blnResetErrorCode As Boolean) As Boolean
        ' Returns True if success, False if failure

        Dim blnSuccess As Boolean
        Dim intMatchCount As Integer

        Dim strCleanPath As String
        Dim strInputFolderToUse As String
        Dim strFolderNameMatchPattern As String

        Dim ioFolderMatch As DirectoryInfo
        Dim ioInputFolderInfo As DirectoryInfo

        mAbortProcessing = False
        blnSuccess = True
        Try
            ' Possibly reset the error code
            If blnResetErrorCode Then mErrorCode = eProcessFoldersErrorCodes.NoError

            If Not String.IsNullOrWhiteSpace(strOutputFolderAlternatePath) Then
                ' Update the cached output folder path
                mOutputFolderPath = String.Copy(strOutputFolderAlternatePath)
            End If

            ' See if strInputFolderPath contains a wildcard (* or ?)
            If Not strInputFolderPath Is Nothing AndAlso (strInputFolderPath.Contains("*") Or strInputFolderPath.Contains("?")) Then
                ' Copy the path into strCleanPath and replace any * or ? characters with _
                strCleanPath = strInputFolderPath.Replace("*", "_")
                strCleanPath = strCleanPath.Replace("?", "_")

                ioInputFolderInfo = New DirectoryInfo(strCleanPath)
                If ioInputFolderInfo.Parent.Exists Then
                    strInputFolderToUse = ioInputFolderInfo.Parent.FullName
                Else
                    ' Use the current working directory
                    strInputFolderToUse = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location)
                End If

                ' Remove any directory information from strInputFolderPath
                strFolderNameMatchPattern = Path.GetFileName(strInputFolderPath)

                ' Process any matching folder in this folder
                Try
                    ioInputFolderInfo = New DirectoryInfo(strInputFolderToUse)
                Catch ex As Exception
                    HandleException("Error in ProcessFoldersWildcard", ex)
                    mErrorCode = eProcessFoldersErrorCodes.InvalidInputFolderPath
                    Return False
                End Try

                intMatchCount = 0
                For Each ioFolderMatch In ioInputFolderInfo.GetDirectories(strFolderNameMatchPattern)

                    blnSuccess = ProcessFolder(ioFolderMatch.FullName, strOutputFolderAlternatePath, strParameterFilePath, True)

                    If Not blnSuccess Or mAbortProcessing Then Exit For
                    intMatchCount += 1

                    If intMatchCount Mod 1 = 0 Then Console.Write(".")

                Next ioFolderMatch

                If intMatchCount = 0 Then
                    If mErrorCode = eProcessFoldersErrorCodes.NoError Then
                        If ShowMessages Then
                            ShowErrorMessage("No match was found for the input folder path:" & strInputFolderPath)
                        Else
                            LogMessage("No match was found for the input folder path:" & strInputFolderPath, eMessageTypeConstants.ErrorMsg)
                        End If
                    End If
                Else
                    Console.WriteLine()
                End If

            Else
                blnSuccess = ProcessFolder(strInputFolderPath, strOutputFolderAlternatePath, strParameterFilePath, blnResetErrorCode)
            End If

        Catch ex As Exception
            HandleException("Error in ProcessFoldersWildcard", ex)
            Return False
        End Try

        Return blnSuccess

    End Function

    Public Function ProcessFolder(strInputFolderPath As String) As Boolean
        Return ProcessFolder(strInputFolderPath, String.Empty, String.Empty, True)
    End Function

    Public Function ProcessFolder(strInputFolderPath As String, strOutputFolderAlternatePath As String, strParameterFilePath As String) As Boolean
        Return ProcessFolder(strInputFolderPath, strOutputFolderAlternatePath, strParameterFilePath, True)
    End Function

    Public MustOverride Function ProcessFolder(strInputFolderPath As String, strOutputFolderAlternatePath As String, strParameterFilePath As String, blnResetErrorCode As Boolean) As Boolean

    Public Function ProcessAndRecurseFolders(strInputFolderPath As String) As Boolean
        Return ProcessAndRecurseFolders(strInputFolderPath, String.Empty)
    End Function

    Public Function ProcessAndRecurseFolders(strInputFolderPath As String, intRecurseFoldersMaxLevels As Integer) As Boolean
        Return ProcessAndRecurseFolders(strInputFolderPath, String.Empty, String.Empty, intRecurseFoldersMaxLevels)
    End Function

    Public Function ProcessAndRecurseFolders(strInputFolderPath As String, strOutputFolderAlternatePath As String) As Boolean
        Return ProcessAndRecurseFolders(strInputFolderPath, strOutputFolderAlternatePath, String.Empty)
    End Function

    Public Function ProcessAndRecurseFolders(strInputFolderPath As String, strOutputFolderAlternatePath As String, strParameterFilePath As String) As Boolean
        Return ProcessAndRecurseFolders(strInputFolderPath, strOutputFolderAlternatePath, strParameterFilePath, 0)
    End Function

    Public Function ProcessAndRecurseFolders(strInputFolderPath As String, strOutputFolderAlternatePath As String, strParameterFilePath As String, intRecurseFoldersMaxLevels As Integer) As Boolean
        ' Calls ProcessFolders for all matching folders in strInputFolderPath 
        ' If intRecurseFoldersMaxLevels is <=0 then we recurse infinitely

        Dim strCleanPath As String
        Dim strInputFolderToUse As String
        Dim strFolderNameMatchPattern As String

        Dim ioFolderInfo As DirectoryInfo

        Dim blnSuccess As Boolean
        Dim intFolderProcessCount, intFolderProcessFailCount As Integer

        ' Examine strInputFolderPath to see if it contains a * or ?
        Try
            If Not strInputFolderPath Is Nothing AndAlso (strInputFolderPath.Contains("*") Or strInputFolderPath.Contains("?")) Then
                ' Copy the path into strCleanPath and replace any * or ? characters with _
                strCleanPath = strInputFolderPath.Replace("*", "_")
                strCleanPath = strCleanPath.Replace("?", "_")

                ioFolderInfo = New DirectoryInfo(strCleanPath)
                If ioFolderInfo.Parent.Exists Then
                    strInputFolderToUse = ioFolderInfo.Parent.FullName
                Else
                    ' Use the current working directory
                    strInputFolderToUse = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location)
                End If

                ' Remove any directory information from strInputFolderPath
                strFolderNameMatchPattern = Path.GetFileName(strInputFolderPath)

            Else
                ioFolderInfo = New DirectoryInfo(strInputFolderPath)
                If ioFolderInfo.Exists Then
                    strInputFolderToUse = ioFolderInfo.FullName
                Else
                    ' Use the current working directory
                    strInputFolderToUse = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location)
                End If
                strFolderNameMatchPattern = "*"
            End If

            If Not String.IsNullOrWhiteSpace(strInputFolderToUse) Then

                ' Validate the output folder path
                If Not String.IsNullOrWhiteSpace(strOutputFolderAlternatePath) Then
                    Try
                        ioFolderInfo = New DirectoryInfo(strOutputFolderAlternatePath)
                        If Not ioFolderInfo.Exists Then ioFolderInfo.Create()
                    Catch ex As Exception
                        HandleException("Error in ProcessAndRecurseFolders", ex)
                        mErrorCode = eProcessFoldersErrorCodes.InvalidOutputFolderPath
                        Return False
                    End Try
                End If

                ' Initialize some parameters
                mAbortProcessing = False
                intFolderProcessCount = 0
                intFolderProcessFailCount = 0

                ' Call RecurseFoldersWork
                blnSuccess = RecurseFoldersWork(strInputFolderToUse, strFolderNameMatchPattern, strParameterFilePath, strOutputFolderAlternatePath, intFolderProcessCount, intFolderProcessFailCount, 1, intRecurseFoldersMaxLevels)

            Else
                mErrorCode = eProcessFoldersErrorCodes.InvalidInputFolderPath
                Return False
            End If

        Catch ex As Exception
            HandleException("Error in ProcessAndRecurseFolders", ex)
            Return False
        End Try

        Return blnSuccess

    End Function

    Private Function RecurseFoldersWork(strInputFolderPath As String, strFolderNameMatchPattern As String, strParameterFilePath As String, strOutputFolderAlternatePath As String, ByRef intFolderProcessCount As Integer, ByRef intFolderProcessFailCount As Integer, intRecursionLevel As Integer, intRecurseFoldersMaxLevels As Integer) As Boolean
        ' If intRecurseFoldersMaxLevels is <=0 then we recurse infinitely

        Dim ioInputFolderInfo As DirectoryInfo
        Dim ioSubFolderInfo As DirectoryInfo
        Dim ioFolderMatch As DirectoryInfo

        Dim strOutputFolderPathToUse As String
        Dim blnSuccess As Boolean

        Try
            ioInputFolderInfo = New DirectoryInfo(strInputFolderPath)
        Catch ex As Exception
            ' Input folder path error
            HandleException("Error in RecurseFoldersWork", ex)
            mErrorCode = eProcessFoldersErrorCodes.InvalidInputFolderPath
            Return False
        End Try

        Try
            If Not String.IsNullOrWhiteSpace(strOutputFolderAlternatePath) Then
                strOutputFolderAlternatePath = Path.Combine(strOutputFolderAlternatePath, ioInputFolderInfo.Name)
                strOutputFolderPathToUse = String.Copy(strOutputFolderAlternatePath)
            Else
                strOutputFolderPathToUse = String.Empty
            End If
        Catch ex As Exception
            ' Output file path error
            HandleException("Error in RecurseFoldersWork", ex)
            mErrorCode = eProcessFoldersErrorCodes.InvalidOutputFolderPath
            Return False
        End Try

        Try
            ShowMessage("Examining " & strInputFolderPath)

            If intRecursionLevel = 1 And strFolderNameMatchPattern = "*" Then
                ' Need to process the current folder
                blnSuccess = ProcessFolder(ioInputFolderInfo.FullName, strOutputFolderPathToUse, strParameterFilePath, True)
                If Not blnSuccess Then
                    intFolderProcessFailCount += 1
                Else
                    intFolderProcessCount += 1
                End If
            End If

            ' Process any matching folder in this folder
            blnSuccess = True
            For Each ioFolderMatch In ioInputFolderInfo.GetDirectories(strFolderNameMatchPattern)
                If mAbortProcessing Then Exit For

                If strOutputFolderPathToUse.Length > 0 Then
                    blnSuccess = ProcessFolder(ioFolderMatch.FullName, Path.Combine(strOutputFolderPathToUse, ioFolderMatch.Name), strParameterFilePath, True)
                Else
                    blnSuccess = ProcessFolder(ioFolderMatch.FullName, String.Empty, strParameterFilePath, True)
                End If

                If Not blnSuccess Then
                    intFolderProcessFailCount += 1
                    blnSuccess = True
                Else
                    intFolderProcessCount += 1
                End If

            Next ioFolderMatch

        Catch ex As Exception
            HandleException("Error in RecurseFoldersWork", ex)
            mErrorCode = eProcessFoldersErrorCodes.InvalidInputFolderPath
            Return False
        End Try

        If Not mAbortProcessing Then
            ' If intRecurseFoldersMaxLevels is <=0 then we recurse infinitely
            '  otherwise, compare intRecursionLevel to intRecurseFoldersMaxLevels
            If intRecurseFoldersMaxLevels <= 0 OrElse intRecursionLevel <= intRecurseFoldersMaxLevels Then
                ' Call this function for each of the subfolders of ioInputFolderInfo
                For Each ioSubFolderInfo In ioInputFolderInfo.GetDirectories()
                    blnSuccess = RecurseFoldersWork(ioSubFolderInfo.FullName, strFolderNameMatchPattern, strParameterFilePath, strOutputFolderAlternatePath, intFolderProcessCount, intFolderProcessFailCount, intRecursionLevel + 1, intRecurseFoldersMaxLevels)
                    If Not blnSuccess Then Exit For
                Next ioSubFolderInfo
            End If
        End If

        Return blnSuccess

    End Function

    Protected Sub SetBaseClassErrorCode(eNewErrorCode As eProcessFoldersErrorCodes)
        mErrorCode = eNewErrorCode
    End Sub

    '' The following functions should be placed in any derived class
    '' Cannot define as MustOverride since it contains a customized enumerated type (eDerivedClassErrorCodes) in the function declaration

    ''Private Sub SetLocalErrorCode(eNewErrorCode As eDerivedClassErrorCodes)
    ''    SetLocalErrorCode(eNewErrorCode, False)
    ''End Sub

    ''Private Sub SetLocalErrorCode(eNewErrorCode As eDerivedClassErrorCodes, blnLeaveExistingErrorCodeUnchanged As Boolean)
    ''    If blnLeaveExistingErrorCodeUnchanged AndAlso mLocalErrorCode <> eDerivedClassErrorCodes.NoError Then
    ''        ' An error code is already defined; do not change it
    ''    Else
    ''        mLocalErrorCode = eNewErrorCode

	''        If eNewErrorCode = eDerivedClassErrorCodes.NoError Then
	''            If MyBase.ErrorCode = clsProcessFoldersBaseClass.eProcessFoldersErrorCodes.LocalizedError Then
	''                MyBase.SetBaseClassErrorCode(clsProcessFoldersBaseClass.eProcessFoldersErrorCodes.NoError)
	''            End If
	''        Else
	''            MyBase.SetBaseClassErrorCode(clsProcessFoldersBaseClass.eProcessFoldersErrorCodes.LocalizedError)
	''        End If
	''    End If

	''End Sub

End Class
