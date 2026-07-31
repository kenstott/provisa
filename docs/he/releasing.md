# שחרור גרסאות (Releasing)

שחרורי גרסאות מופעלים על ידי דחיפת תגית git. שם התגית קובע את הערוץ (channel).

## מוסכמות תגיות

| פורמט תגית | ערוץ | סוג GitHub Release |
|-----------|---------|-------------------|
| `v1.2.3-alpha.1` | alpha | Pre-release |
| `v1.2.3-beta.1` | beta | Pre-release |
| `v1.2.3-rc.1` | rc | Pre-release |
| `v1.2.3` | stable | Latest release |

## יצירת שחרור

```bash
# Alpha
git tag v1.2.3-alpha.1 && git push origin v1.2.3-alpha.1

# Beta
git tag v1.2.3-beta.1 && git push origin v1.2.3-beta.1

# Release candidate
git tag v1.2.3-rc.1 && git push origin v1.2.3-rc.1

# Stable
git tag v1.2.3 && git push origin v1.2.3
```

זרימת ה-CI (`build-dmg.yml`, בשם "Build Provisa Packages") מופעלת על כל תגית `v*` ומריצה jobs אלה, רובם במקביל:

1. **פתירת מטא-דאטת שחרור** — מזהה ערוץ מסיומת התגית, גוזר גרסת PEP 440 ושמות assets
2. **הורדה/אריזה של תוספי Trino** — מושך מחברי Trino של Calcite ואורז tarball
3. **משיכת image-י Docker ליבה/obs/הדגמה** — שומר tarballs של image שירות (arm64, בתוספת amd64 ליבה עבור שכבת ה-container של Windows)
4. **בניית DMG-ים ל-macOS Core/Obs/Demo** — רץ על `macos-14` (Apple Silicon), air-gapped
5. **בניית AppImage ל-Linux** — ליבה, air-gapped
6. **בניית מתקין Core ל-Windows** — ילידי, Python מוטמע, ללא Docker
7. **בניית מתקין שכבת-Container ל-Windows** — WSL2 + Trino, מושך images לפי דרישה (ללא VirtualBox/OVA)
8. **בניית דרייבר JDBC** — Maven shaded JAR
9. **בנייה ובדיקת לקוח Python**, ואז **פרסום ל-PyPI**
10. **פרסום GitHub Release** — מעלה את כל ה-assets, מגדיר דגל pre-release עבור alpha/beta/rc

## Assets של שחרור

כל שחרור מפרסם את ה-assets הבאים, כולם מצורפים ל-GitHub Release (ה-wheel הולך גם ל-PyPI):

| Asset | פלטפורמה / שימוש |
|-------|----------------|
| `Provisa-<tag>-macOS.dmg` | macOS Core (Apple Silicon, air-gapped) |
| `Provisa-Runtime-<tag>-macOS.dmg` | runtime‏ Python ילידי ל-macOS (מותקן לצד Core) |
| `Provisa-Obs-<tag>-macOS.dmg` | תוסף Observability ל-macOS |
| `Provisa-Demo-<tag>-macOS.dmg` | תוסף Demo ל-macOS (דורש Obs) |
| `Provisa-<tag>-linux-x86_64.AppImage` | ליבת Linux x86_64 (air-gapped) |
| `Provisa-<tag>-windows-x64.exe` | מתקין ילידי Windows x64 (Python מוטמע, ללא Docker) |
| `Provisa-Container-<tag>-windows-x64.exe` | שדרוג שכבת-container ל-Windows x64 (WSL2 + Trino) |
| `provisa-jdbc-<tag>.jar` | דרייבר JDBC — Tableau, PowerBI, DBeaver |
| `provisa_client-<pep440>-py3-none-any.whl` | לקוח Python (גם PyPI) |
| `provisa-core-images-<tag>.tar.gz` | tarballs‏ image של שירותי ליבה (arm64, air-gapped) |
| `provisa-core-images-amd64-<tag>.zip` | image-י שירותי ליבה (amd64, שכבת-container של Windows / air-gap) |
| `provisa-obs-images-<tag>.tar.gz` | image-י מחסנית Observability (אופציונלי) |
| `provisa-demo-images-<tag>.tar.gz` | image-י חבילת נתוני הדגמה (אופציונלי) |
| `provisa-trino-plugins-<tag>.tar.gz` | מחברי מנוע תיאום (SharePoint, Splunk, File) |

גרסת לקוח ה-Python מומרת אוטומטית לפורמט PEP 440:
`v0.1.0-alpha.1` → `0.1.0a1`, `v0.1.0-beta.1` → `0.1.0b1`, `v0.1.0-rc.1` → `0.1.0rc1`.

## הגדרת פרסום PyPI (חד-פעמי)

1. העתיקו את טוקן ה-API שלכם מ-`~/.pypirc` (הערך `pypi-...` עבור `pypi.org`)
2. הוסיפו אותו כ-secret של המאגר בשם `PYPI_API_TOKEN` תחת **Settings → Secrets → Actions**

ה-job‏ `publish-pypi` יפרסם אז אוטומטית על כל תגית.

## Secrets נדרשים במאגר

הגדירו אלה תחת **Settings → Secrets → Actions**:

| Secret | נדרש עבור | תיאור |
|--------|-------------|-------------|
| `PYPI_API_TOKEN` | פרסום PyPI | טוקן API מ-`~/.pypirc` (מתחיל ב-`pypi-`) |
| `APPLE_CERT_P12_BASE64` | build-ים חתומים | קובץ תעודת `.p12` מקודד Base64 (ראו למטה) |
| `APPLE_CERT_P12_PASSWORD` | build-ים חתומים | סיסמה שהוגדרה בעת ייצוא ה-`.p12` מ-Keychain Access |
| `APPLE_DEVELOPER_ID` | build-ים חתומים | שם תעודה מלא: `Developer ID Application: Your Name (TEAMID)` |
| `APPLE_NOTARYTOOL_APPLE_ID` | build-ים מאושרי-notarization | אימייל Apple ID |
| `APPLE_NOTARYTOOL_PASSWORD` | build-ים מאושרי-notarization | סיסמה ספציפית-לאפליקציה מ-appleid.apple.com (לא סיסמת ההתחברות שלכם) |
| `APPLE_NOTARYTOOL_TEAM_ID` | build-ים מאושרי-notarization | מזהה Apple Team בן 10 תווים |

build-ים ללא secrets אלה מצליחים אך מייצרים DMG לא-חתום/לא-מאושר-notarization (משתמשים יראו אזהרת Gatekeeper).

## ייצוא תעודת ה-.p12

1. פתחו את **Keychain Access** → login keychain → **My Certificates**
2. מצאו את **Developer ID Application: Your Name (TEAMID)** — הרחיבו אותה כדי לאשר שהמפתח הפרטי מקונן מתחת
3. בחרו הן את התעודה והן את המפתח הפרטי שלה → קליק ימני → **Export 2 Items** → שמרו כ-`.p12` → הגדירו סיסמה חזקה
4. קודדו ל-Base64 והעתיקו ללוח:
   ```bash
   base64 -i YourCert.p12 | pbcopy
   ```
5. הדביקו כערך של `APPLE_CERT_P12_BASE64`; הגדירו את `APPLE_CERT_P12_PASSWORD` לסיסמה משלב 3

## מציאת שם התעודה שלכם

```bash
security find-identity -v -p codesigning | grep "Developer ID Application"
```

העתיקו את המחרוזת המלאה במרכאות — זהו הערך עבור `APPLE_DEVELOPER_ID`.

## מחיקת תגית שגויה

```bash
git tag -d v1.2.3-alpha.1
git push origin :refs/tags/v1.2.3-alpha.1
```

אז מחקו את ה-GitHub Release המתאים ב-UI לפני תיוג מחדש.
