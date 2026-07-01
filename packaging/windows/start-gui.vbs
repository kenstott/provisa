Dim fso, sh, psPath
Set fso = CreateObject("Scripting.FileSystemObject")
Set sh  = CreateObject("WScript.Shell")
psPath = fso.GetParentFolderName(WScript.ScriptFullName) & "\start-gui.ps1"
sh.Run "powershell.exe -ExecutionPolicy Bypass -WindowStyle Hidden -File """ & psPath & """", 0, False
