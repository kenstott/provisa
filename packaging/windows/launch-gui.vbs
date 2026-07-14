' Launch the setup wizard (a WinForms GUI that shows its own window even though this PowerShell host
' is hidden). On first run it collects the deployment choices, then hands off to first-launch; once
' set up it just starts the app. No console window is shown either way.
Dim fso, sh, psPath
Set fso = CreateObject("Scripting.FileSystemObject")
Set sh  = CreateObject("WScript.Shell")
psPath = fso.GetParentFolderName(WScript.ScriptFullName) & "\setup-wizard.ps1"
sh.Run "powershell.exe -ExecutionPolicy Bypass -WindowStyle Hidden -File """ & psPath & """", 0, False
