; Provisa Windows NSIS Installer
; Build: makensis /DVERSION=<tag> installer.nsi

!ifndef VERSION
  !define VERSION "dev"
!endif

Name "Provisa ${VERSION}"
OutFile "dist\Provisa-Setup.exe"
InstallDir "$PROGRAMFILES64\Provisa"
RequestExecutionLevel admin

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

  ; Add/Remove Programs entry
  WriteRegStr HKLM \
    "Software\Microsoft\Windows\CurrentVersion\Uninstall\Provisa" \
    "DisplayName" "Provisa ${VERSION}"
  WriteRegStr HKLM \
    "Software\Microsoft\Windows\CurrentVersion\Uninstall\Provisa" \
    "UninstallString" "$INSTDIR\Uninstall.exe"
  WriteRegStr HKLM \
    "Software\Microsoft\Windows\CurrentVersion\Uninstall\Provisa" \
    "DisplayVersion" "${VERSION}"
  WriteRegStr HKLM \
    "Software\Microsoft\Windows\CurrentVersion\Uninstall\Provisa" \
    "Publisher" "Provisa"

  ; Start Menu shortcut — runs first-launch on click
  CreateDirectory "$SMPROGRAMS\Provisa"
  CreateShortcut "$SMPROGRAMS\Provisa\Provisa First Launch.lnk" \
    "powershell.exe" \
    "-ExecutionPolicy Bypass -File `"$INSTDIR\first-launch.ps1`""

  ; Add $INSTDIR to system PATH
  ReadRegStr $0 HKLM \
    "SYSTEM\CurrentControlSet\Control\Session Manager\Environment" "Path"
  WriteRegExpandStr HKLM \
    "SYSTEM\CurrentControlSet\Control\Session Manager\Environment" \
    "Path" "$0;$INSTDIR"
  SendMessage ${HWND_BROADCAST} ${WM_WININICHANGE} 0 "STR:Environment" /TIMEOUT=5000

  WriteUninstaller "$INSTDIR\Uninstall.exe"
SectionEnd

; ── Uninstall ─────────────────────────────────────────────────────────────────
Section "Uninstall"
  RMDir /r "$INSTDIR"

  Delete "$SMPROGRAMS\Provisa\Provisa First Launch.lnk"
  RMDir  "$SMPROGRAMS\Provisa"

  DeleteRegKey HKLM \
    "Software\Microsoft\Windows\CurrentVersion\Uninstall\Provisa"

  ; Note: $INSTDIR is left in PATH on uninstall (cosmetic; safe to leave)
SectionEnd
