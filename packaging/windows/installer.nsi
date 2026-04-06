; Provisa Windows NSIS Installer
; Build: makensis /DVERSION=<tag> installer.nsi

!ifndef VERSION
  !define VERSION "dev"
!endif

Name "Provisa ${VERSION}"
OutFile "dist\Provisa-Setup.exe"
InstallDir "$LOCALAPPDATA\Programs\Provisa"
RequestExecutionLevel user

!include "MUI2.nsh"

!insertmacro MUI_PAGE_WELCOME
!insertmacro MUI_PAGE_INSTFILES
!insertmacro MUI_PAGE_FINISH

!insertmacro MUI_UNPAGE_CONFIRM
!insertmacro MUI_UNPAGE_INSTFILES

!insertmacro MUI_LANGUAGE "English"

; ── Install ───────────────────────────────────────────────────────────────────
Section "Provisa" SecMain
  SetOutPath "$INSTDIR"
  File /r "build\*"

  ; Add/Remove Programs entry (per-user hive — no admin required)
  WriteRegStr HKCU \
    "Software\Microsoft\Windows\CurrentVersion\Uninstall\Provisa" \
    "DisplayName" "Provisa ${VERSION}"
  WriteRegStr HKCU \
    "Software\Microsoft\Windows\CurrentVersion\Uninstall\Provisa" \
    "UninstallString" "$INSTDIR\Uninstall.exe"
  WriteRegStr HKCU \
    "Software\Microsoft\Windows\CurrentVersion\Uninstall\Provisa" \
    "DisplayVersion" "${VERSION}"
  WriteRegStr HKCU \
    "Software\Microsoft\Windows\CurrentVersion\Uninstall\Provisa" \
    "Publisher" "Provisa"

  ; Start Menu shortcut — runs first-launch on click
  CreateDirectory "$SMPROGRAMS\Provisa"
  CreateShortcut "$SMPROGRAMS\Provisa\Provisa First Launch.lnk" \
    "powershell.exe" \
    "-ExecutionPolicy Bypass -File `"$INSTDIR\first-launch.ps1`""

  ; Add $INSTDIR to user PATH (HKCU — no admin required)
  ReadRegStr $0 HKCU "Environment" "Path"
  WriteRegExpandStr HKCU "Environment" "Path" "$0;$INSTDIR"
  SendMessage ${HWND_BROADCAST} ${WM_WININICHANGE} 0 "STR:Environment" /TIMEOUT=5000

  WriteUninstaller "$INSTDIR\Uninstall.exe"
SectionEnd

; ── Uninstall ─────────────────────────────────────────────────────────────────
Section "Uninstall"
  ; Stop and remove Provisa VM before deleting files
  nsExec::Exec 'VBoxManage controlvm Provisa acpipowerbutton'
  Sleep 3000
  nsExec::Exec 'VBoxManage unregistervm Provisa --delete'

  RMDir /r "$INSTDIR"

  Delete "$SMPROGRAMS\Provisa\Provisa First Launch.lnk"
  RMDir  "$SMPROGRAMS\Provisa"

  DeleteRegKey HKCU \
    "Software\Microsoft\Windows\CurrentVersion\Uninstall\Provisa"

  ; Note: $INSTDIR is left in user PATH on uninstall (cosmetic; safe to leave)
  ; Note: VirtualBox itself is not uninstalled — may be used by other apps
SectionEnd
