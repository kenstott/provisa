Dim fso, sh, psPath
Set fso = CreateObject("Scripting.FileSystemObject")
Set sh  = CreateObject("WScript.Shell")
psPath = fso.GetParentFolderName(WScript.ScriptFullName) & "\first-launch-gui.ps1"
sh.Run "powershell.exe -ExecutionPolicy Bypass -WindowStyle Normal -File """ & psPath & """", 1, False
