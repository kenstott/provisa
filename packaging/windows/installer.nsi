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
    "-ExecutionPolicy Bypass -File `"$INSTDIR\first-launch.ps1`"" \
    "$INSTDIR\first-launch.ps1" 0

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

  ; Remove $INSTDIR from system PATH
  ReadRegStr $0 HKLM \
    "SYSTEM\CurrentControlSet\Control\Session Manager\Environment" "Path"
  ${StrRep} $1 "$0" ";$INSTDIR" ""
  WriteRegExpandStr HKLM \
    "SYSTEM\CurrentControlSet\Control\Session Manager\Environment" \
    "Path" "$1"
  SendMessage ${HWND_BROADCAST} ${WM_WININICHANGE} 0 "STR:Environment" /TIMEOUT=5000
SectionEnd

; ── StrRep function (needed for PATH removal) ─────────────────────────────────
!macro _StrRep OUTPUT NEEDLE SEARCH REPLACE
  Push "${REPLACE}"
  Push "${SEARCH}"
  Push "${NEEDLE}"
  Call StrRep
  Pop "${OUTPUT}"
!macroend
!define StrRep "!insertmacro _StrRep"

Function StrRep
  Exch $R0  ; needle
  Exch
  Exch $R1  ; search
  Exch 2
  Exch $R2  ; replace
  Push $R3
  Push $R4
  Push $R5
  Push $R6
  Push $R7
  Push $R8
  Push $R9

  StrLen $R3 $R1
  StrLen $R4 $R2
  StrCpy $R5 ""
  StrCpy $R6 $R0

  loop:
    StrLen $R7 $R6
    IntCmp $R7 $R3 next next done
    StrCpy $R8 $R6 $R3
    StrCmp $R8 $R1 found
    StrCpy $R5 "$R5$R8" "" 1
    StrCpy $R6 $R6 "" 1
    Goto loop
  found:
    StrCpy $R5 "$R5$R2"
    StrCpy $R6 $R6 "" $R3
    Goto loop
  done:
    StrCpy $R5 "$R5$R6"

  Pop $R9
  Pop $R8
  Pop $R7
  Pop $R6
  Pop $R5
  Pop $R4
  Pop $R3
  StrCpy $R0 $R5
  Pop $R2
  Pop $R1
  Exch $R0
FunctionEnd
