' Stop Provisa from the Start Menu ("Stop Provisa"). The counterpart to launch-gui.vbs: the same
' hidden PowerShell host, running the lifecycle script's stop command instead of the wizard.
'
' Waits for the stop to finish and then says so. Provisa's servers have no window of their own -
' they are background processes started by provisa-native.ps1 - so without an acknowledgement the
' shortcut would look like it did nothing, and the obvious next move is to click it again.
Dim fso, sh, psPath, rc
Set fso = CreateObject("Scripting.FileSystemObject")
Set sh  = CreateObject("WScript.Shell")
psPath = fso.GetParentFolderName(WScript.ScriptFullName) & "\provisa-native.ps1"
rc = sh.Run("powershell.exe -ExecutionPolicy Bypass -WindowStyle Hidden -File """ & psPath & """ stop", 0, True)
If rc = 0 Then
  MsgBox "Provisa has been shut down.", 64, "Provisa"
Else
  MsgBox "Provisa could not be shut down (exit code " & rc & ")." & vbCrLf & _
         "Run 'provisa stop' from " & fso.GetParentFolderName(WScript.ScriptFullName) & " to see why.", 16, "Provisa"
End If
