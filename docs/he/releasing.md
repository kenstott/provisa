# שחרור גרסאות (Releasing)

שחרורי גרסאות מופעלים על ידי דחיפת תגית git. שם התגית קובע את הערוץ (channel).

## מוסכמות תגיות {: #tag-conventions }

| פורמט תגית | ערוץ | סוג GitHub Release |
| ----------- | --------- | ------------------- |
| `v1.2.3-alpha.1` | alpha | Pre-release |
| `v1.2.3-beta.1` | beta | Pre-release |
| `v1.2.3-rc.1` | rc | Pre-release |
| `v1.2.3` | stable | Latest release |

## יצירת שחרור {: #creating-a-release }

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

## Assets של שחרור {: #release-assets }

כל שחרור מפרסם את ה-assets הבאים, כולם מצורפים ל-GitHub Release (ה-wheel הולך גם ל-PyPI):

| Asset | פלטפורמה / שימוש |
| ------- | ---------------- |
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

## הגדרת פרסום PyPI (חד-פעמי) {: #pypi-publishing-setup-one-time }

ה-jobs‏ `publish-pypi` (‏`provisa-client`) ו-`publish-provisa-pypi` (‏`provisa`) ב-`.github/workflows/build-dmg.yml` מפרסמים באמצעות trusted publishing של PyPI: טוקן ה-OIDC של GitHub של ה-job‏ (`id-token: write`) מאמת אותו, ושום טוקן API אינו נקרא. רשמו את המאגר הזה ואת `build-dmg.yml` כ-trusted publisher לכל פרויקט ב-pypi.org (**Your projects → Manage → Publishing**). הפרסומים רצים אז אוטומטית על כל תגית.

## חתימה ושרשרת אספקה {: #signing-and-supply-chain }

מה שכל artifact של שחרור נושא, כפי שזרימות העבודה של השחרור בונות אותו:

| Artifact | חתימה |
| -------- | --------- |
| `provisa` ו-`provisa-client` ב-PyPI | attestation לפי PEP 740 חתום ב-Sigstore לכל קובץ (`attestations: true`), ניתן לאימות ב-pypi.org |
| `provisa-pg-ext`, ‏`provisa-duckdb-ext` ב-PyPI | attestation לפי PEP 740 חתום ב-Sigstore לכל קובץ |
| קובץ `.jar` של דרייבר JDBC | attestation של build provenance ב-Sigstore (‏`actions/attest-build-provenance`) |
| מתקני macOS‏ `.dmg` | חתימת קוד Developer ID ואישור notarization של Apple |
| מתקני Windows‏ `.exe` | לא חתומים |
| Linux‏ `.AppImage` | לא חתום |

ההתקנה החתומה בכל מערכת הפעלה היא `pip install provisa`: חבילת PyPI עם attestation של Sigstore. סוקר שזקוק ל-artifact חתום ב-Linux או ב-Windows מתקין עם pip. בניית Windows חותמת ב-Authenticode כשמוגדרת תעודת חתימת קוד (`WINDOWS_CERT_PFX_BASE64`, ‏`WINDOWS_CERT_PFX_PASSWORD`, ‏`WINDOWS_CERT_TIMESTAMP_URL`, הנקראים על ידי `packaging/windows/build-sfx.ps1`); אף אחת אינה מוגדרת, ולכן היא נשלחת לא חתומה.

תלויות ושערי CVE:

- תלויות Python ננעלות עם גיבובים ב-`uv.lock`; ה-CI מתקין אותן עם `uv sync --frozen`.
- זרימת העבודה `CVE Audit` (‏`.github/workflows/cve-audit.yml`) מריצה `pip-audit` על הקבוצה הנעולה בכל push ל-`main`, בכל pull request ופעם בשבוע.
- בשחרור, הבניות של `provisa` ושל `provisa-client` מפיקות כל אחת SBOM בפורמט CycloneDX ומריצות `pip-audit` על מה שנשלח. דרייבר ה-JDBC נסרק עם `grype --fail-on medium`. ממצא מכשיל את ה-job, והשחרור אינו מפרסם דבר: העלאות ה-PyPI וגם ה-release ב-GitHub ממתינים ל-jobs האלה. ה-SBOMs ודוחות ה-CVE מצורפים לכל שחרור.
- מתקני ה-desktop בונים את ה-wheel של `provisa` מקוד המקור על ה-runner של השחרור ופותרים את תלויותיו עם `pip` בזמן הבנייה, לא מ-`uv.lock`; שער ה-CVE של השחרור סורק את הקבוצה הנעולה, לא את התלויות המצורפות למתקנים.
- חריג מתועד אחד מתעלמים ממנו: PYSEC-2026-4114 / CVE-2026-49265 (‏PKCE של oauthlib בצד השרת; Provisa משתמש ב-oauthlib בצד הלקוח בלבד).
- `packaging/pypi/requirements-embedded.lock` הוא פתרון עם גיבובים מקובעים של `provisa[embedded]` עבור mirrors באוויר סגור (airgap) (ראו `packaging/pypi/MIRROR-CHECKLIST.md`). הוא נוצר מחדש ידנית, לא בכל שחרור.

## Secrets נדרשים במאגר {: #required-repository-secrets }

הגדירו אלה תחת **Settings → Secrets → Actions**:

| Secret | נדרש עבור | תיאור |
| -------- | ------------- | ------------- |
| `APPLE_CERT_P12_BASE64` | build-ים חתומים | קובץ תעודת `.p12` מקודד Base64 (ראו למטה) |
| `APPLE_CERT_P12_PASSWORD` | build-ים חתומים | סיסמה שהוגדרה בעת ייצוא ה-`.p12` מ-Keychain Access |
| `APPLE_DEVELOPER_ID` | build-ים חתומים | שם תעודה מלא: `Developer ID Application: Your Name (TEAMID)` |
| `APPLE_NOTARYTOOL_APPLE_ID` | build-ים מאושרי-notarization | אימייל Apple ID |
| `APPLE_NOTARYTOOL_PASSWORD` | build-ים מאושרי-notarization | סיסמה ספציפית-לאפליקציה מ-appleid.apple.com (לא סיסמת ההתחברות שלכם) |
| `APPLE_NOTARYTOOL_TEAM_ID` | build-ים מאושרי-notarization | מזהה Apple Team בן 10 תווים |

build-ים ללא secrets אלה מצליחים אך מייצרים DMG לא-חתום/לא-מאושר-notarization (משתמשים יראו אזהרת Gatekeeper).

## ייצוא תעודת ה-.p12 {: #exporting-the-p12-certificate }

1. פתחו את **Keychain Access** → login keychain → **My Certificates**
2. מצאו את **Developer ID Application: Your Name (TEAMID)** — הרחיבו אותה כדי לאשר שהמפתח הפרטי מקונן מתחת
3. בחרו הן את התעודה והן את המפתח הפרטי שלה → קליק ימני → **Export 2 Items** → שמרו כ-`.p12` → הגדירו סיסמה חזקה
4. קודדו ל-Base64 והעתיקו ללוח:

   ```bash
   base64 -i YourCert.p12 | pbcopy
   ```

5. הדביקו כערך של `APPLE_CERT_P12_BASE64`; הגדירו את `APPLE_CERT_P12_PASSWORD` לסיסמה משלב 3

## מציאת שם התעודה שלכם {: #finding-your-certificate-name }

```bash
security find-identity -v -p codesigning | grep "Developer ID Application"
```

העתיקו את המחרוזת המלאה שבין המירכאות. המחרוזת הזו היא הערך של `APPLE_DEVELOPER_ID`.

## מחיקת תגית שגויה {: #deleting-a-bad-tag }

```bash
git tag -d v1.2.3-alpha.1
git push origin :refs/tags/v1.2.3-alpha.1
```

אז מחקו את ה-GitHub Release המתאים ב-UI לפני תיוג מחדש.
