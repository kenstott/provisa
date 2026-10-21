# מקורות Node ו-Python

לפעמים הנתונים שאתם צריכים נמצאים מאחורי קוד שהצוות שלכם כבר כתב: מודל ניקוד, לקוח לשירות פנימי, סקריפט שמסדר ייצוא. מקור Node או Python מאפשר להגיש את הקוד הזה כטבלאות. אתם מסמנים את הפונקציות שרוצים לשתף, מפנים את Provisa אל הקוד, וכל פונקציה מסומנת הופכת לטבלה שכל תפקיד קורא לפי אותם כללים כמו כל טבלה אחרת.

## למה הופכת פונקציה {: #what-a-function-becomes }

Provisa קורא את ההצהרות של הקוד עצמו: תגי JSDoc וטיפוסי TypeScript ב-Node, docstrings ורמזי טיפוס ב-Python. שלושה תגים נחשבים.

| תג | הופך ל |
|---|---|
| `@table` | טבלה. הפונקציה מחזירה רשימה או אובייקט איטרבילי, וכל איבר הוא שורה |
| `@command` | Command, שרץ לפי בקשה |
| `@stream` | טבלה חיה. הפונקציה היא גנרטור, וכל שורה נשלחת ברגע שהיא מופקת |

פונקציה בלי תג מתעלמים ממנה. תג ש-Provisa לא מכיר נדחה, וכך גם טיפוס שהוא לא יודע למפות. הדחייה מציינת את הפונקציה ומפרטת את כל הבעיות בבת אחת, כך שמתקנים הכול במעבר אחד.

טיפוס השורה קובע את העמודות. טקסט, מספרים, בוליאנים, תאריכים וחותמות זמן ממופים לטיפוסי העמודות שלהם, רשימות נשארות רשימות, ואובייקטים מקוננים הופכים לטיפוסים בעלי שם. כשטיפוס השורה רופף (`any`, `unknown` או `Record` ב-TypeScript, `Any` או `dict` ב-Python), Provisa קורא לפונקציה פעם אחת, בלי ארגומנטים, ומציע עמודות לפי השורות שחוזרות. אם לפונקציה יש פרמטר חובה אי אפשר לקרוא לה כך, ואתם קובעים את טיפוסי העמודות בעצמכם כשאתם רושמים את הטבלה.

הפרמטרים של הפונקציה הופכים לפרמטרים של הטבלה. פרמטר בלי ערך ברירת מחדל הוא חובה, כך שקריאה שמשמיטה אותו נדחית עוד לפני שהקוד שלכם רץ. פרמטר עם ערך ברירת מחדל הוא אופציונלי; השמיטו אותו וברירת המחדל שלכם חלה. Steward עדיין יכול להפוך פרמטר אופציונלי לחובה.

## מאיפה הקוד מגיע {: #where-the-code-comes-from }

הקוד מקובע, כך שמקור תמיד מריץ בדיוק את מה שנרשם:

- **חבילה**: חבילת npm ל-Node או חבילת PyPI ל-Python, בגרסה מדויקת.
- **מאגר Git**: commit מלא בן 40 תווים, עם תת-תיקייה אם הקוד לא נמצא בשורש.
- **העלאה**: קובץ `.zip`, `.tgz` או `.tar.gz` של הקוד.

קוד Node רץ על Node.js, Bun או Deno, לפי בחירתכם. קוד Python רץ על ה-Python ש-Provisa מספק. כשיש לקוד יותר ממודול Python אחד ברמה העליונה, ציינו בשדה **מודול כניסה** דרך איזה מהם נכנסים.

## התקנת תלויות {: #installing-dependencies }

קוד עם תלויות צריך lockfile, וכל התקנה עוקבת אחריו בדיוק. בלעדיו, חבילה שמצהירה על תלויות נדחית, והדחייה אומרת לכם להוסיף אחד.

| סביבת ריצה | Lockfile |
|---|---|
| Node.js | `package-lock.json` או `npm-shrinkwrap.json` |
| Bun | `bun.lock` או `bun.lockb` |
| Deno | `deno.lock` |
| Python | `uv.lock` (עם ה-`pyproject.toml` שלו), `pylock.toml`, או `requirements.txt` עם `--hash` בכל שורה |

lockfile הוא אופציונלי, אבל בלעדיו שתי התקנות של אותו קוד עלולות להיות שונות, כך שבדיקה שעוברת היום עלולה להיכשל מחר בגלל גרסה חדשה של תלות. להתקנה זהה בכל פעם, הקוד חייב לכלול lockfile, או שאתם יוצרים אחד ומספקים אותו כאן. אימג' שנבנה כבר מקובע. ברגע שאתם בונים את האימג' של המקור ([בניית אימג'](#running-on-a-remote-host)), התלויות שלו נמצאות בתוכו, ואם האימג' הזה הוא מה שעובר את הבדיקות ואת ה-CI/CD שלכם, האימג' שבדקתם הוא זה שרץ. ל-lockfile יש משמעות במצב dev ובכל בנייה חדשה של האימג'. קוד שכולל lockfile משלו מותקן ממנו: מאגר Git או העלאה שיש בהם אחד, או חבילת npm שהמחבר שלה פרסם `npm-shrinkwrap.json` ‏(npm לעולם אינו מפרסם את `package-lock.json`). קוד בלי lockfile מותקן עם תלויות שאינן מקובעות: מאגר Git או העלאה בלי lockfile, רוב חבילות npm, וחבילות Python מ-PyPI, שב-wheels שלהן אין מקום לאחד. ספקו lockfile לצד קוד כזה כדי שכל התקנה תהיה זהה. יש קוד שאינו צריך אחד, כי אופן ההפצה שלו כבר מקבע כל תלות.

אף חבילה לא מורשית להריץ סקריפטי התקנה משלה: npm מתקין עם `--ignore-scripts`, ו-Python מתקין רק wheels ולעולם לא בונה מהפצת קוד מקור.

החבילות מגיעות ממאגרי החבילות של הארגון שלכם, אם יש לכם כאלה. מנהל מזין פעם אחת את מאגרי ה-npm, אינדקסי ה-PyPI ושרתי ה-Git שלכם תחת **Admin → מאגרי חבילות**, וכל מקור Node ו-Python בארגון מתקין דרכם. מאגר ה-npm הציבורי וה-PyPI הציבורי לא צריכים רשומה. ראו [מאגרי חבילות](package-registries.md).

התקנה נשמרת במטמון לפי גיבוב התוכן של הקוד, כך שאותו קוד מותקן פעם אחת בלבד.

## ארגז החול {: #the-sandbox }

הקוד שלכם רץ בתהליך worker משלו, בתוך ארגז חול. הוא יכול לקרוא את הקוד המותקן שלו ושום דבר אחר בדיסק. הוא מגיע רק לשרתים שברשימת היציאה המותרת של הפריסה, דרך פרוקסי ש-Provisa מריץ, ולעולם לא לפורטים של Provisa עצמו. שרת שלא יכול לספק את ארגז החול מסרב להפעיל את ה-worker ומציין מה חסר, במקום להריץ את הקוד בלי הגבלה. ב-Windows מקורות קוד נדחים.

## קריאת טבלה {: #reading-a-table }

טבלה של מקור קוד מונחתת, ולעולם לא נקראת במקומה. Provisa קורא לפונקציה עם הארגומנטים של הקריאה, בודק כל שורה מול העמודות, ושומר את השורות תחת הארגומנטים האלה למשך ה-TTL של המטמון של הטבלה. אחר כך חלים על השורות המונחתות נראות, מסכות וחוקי שורה, כמו על כל טבלה.

ארגומנטים מעבירים כמו כל פרמטר של טבלה: כתנאי `_nf_` ב-SQL וכארגומנטים של שדה ב-GraphQL.

## מפתחות באצווה: הפרמטר `loader` {: #batched-keys }

‏join יכול להגיע לטבלת קוד: השורות של טבלה אחרת מספקות את הפרמטרים שלה, כמו לקוחות שמצורפים לציון הנטישה שלהם. כברירת מחדל Provisa קורא לפונקציה פעם אחת לכל צירוף נפרד של ארגומנטים שהשורות המצורפות נותנות, ושולח את כל הקריאות האלה ל-worker בבקשה אחת.

פונקציה שמקבלת פרמטר בשם `loader` נקראת במקום זאת פעם אחת לכל אצווה, לפי הדפוס ש-DataLoader של GraphQL הפך למוכר. ‏`loader` הוא רשימה של אובייקטי JSON, מפתח אחד בכל אחד, שמחזיקים את ערכי השורות המצורפות עבור שאר הפרמטרים של הפונקציה. Provisa מסיר מפתחות כפולים, כך שכל צירוף נפרד של ערכים מופיע פעם אחת. הרשימה דלילה: שדה בלי ערך מושמט מהמפתח שלו במקום להישלח כ-null, כך שערך ברירת המחדל של הפונקציה עצמה חל, כמו בקריאה יחידה.

הפונקציה מחזירה שורות רגילות שנושאות את עמודות המפתח שלהן, ו-Provisa מצרף אותן חזרה לשורות המצורפות לפי העמודות האלה, אף פעם לא לפי מיקום. השורות יכולות לחזור בכל סדר, ומפתח יכול לתת אפס שורות או הרבה.

השתמשו ב-`loader` כשקריאה אחת על מפתחות רבים עדיפה על קריאות רבות על מפתח אחד:

- הרצת מודל על אצווה שלמה במעבר וקטורי אחד.
- איגוד בקשות ל-API או למודל שפה שמקבל קלטים רבים בבת אחת.
- הכנה פעם אחת לכל אצווה, כמו טעינת מודל או פתיחת חיבור, במקום פעם אחת לכל מפתח.

פונקציית ה-Python הזו נותנת ציון לכל לקוח באצווה במעבר אחד:

```python
from dataclasses import dataclass
from typing import TypedDict


class CustomerKey(TypedDict, total=False):
    customer_id: str
    plan: str
    region: str


@dataclass
class Score:
    customer_id: str
    plan: str
    region: str
    score: float


def churn_score(
    customer_id: str, plan: str = "basic", region: str = "US", loader: list[CustomerKey] | None = None
) -> list[Score]:
    """Churn score of each customer in the batch.

    @table
    """
    keys = loader or [{"customer_id": customer_id, "plan": plan, "region": region}]
    rows = [{"plan": "basic", "region": "US", **key} for key in keys]
    scores = MODEL.predict(features(rows))  # one vectorized call for the whole batch
    return [Score(r["customer_id"], r["plan"], r["region"], s) for r, s in zip(rows, scores)]
```

כשהיא מצורפת לטבלת לקוחות, היא נקראת עם `loader=[{"customer_id": "637", "plan": "pro", "region": "EU"}, {"customer_id": "638"}, ...]`. המפתח השני נושא רק את `customer_id`, ולכן התוכנית והאזור שלו מקבלים את ערכי ברירת המחדל. כל שורה שמוחזרת נושאת `customer_id`, ‏`plan` ו-`region`, ו-Provisa מתאים אותה ללקוח שלה לפיהם.

אותה פונקציה ב-TypeScript:

```typescript
export interface CustomerKey {
  customer_id: string;
  plan?: string;
  region?: string;
}

export interface Score {
  customer_id: string;
  plan: string;
  region: string;
  score: number;
}

/** Churn score of each customer in the batch.
 * @table
 */
export async function churnScore(
  customer_id: string,
  plan = "basic",
  region = "US",
  loader?: CustomerKey[],
): Promise<Score[]> {
  const keys = loader ?? [{ customer_id, plan, region }];
  const rows = keys.map((key) => ({ plan: "basic", region: "US", ...key }));
  const scores = await model.predict(rows); // one call for the whole batch
  return rows.map((row, i) => ({ ...row, score: scores[i] }) as Score);
}
```

## דוגמה {: #an-example }

מודול ה-Python הזה מעריך עד כמה סביר שחשבון יעזוב:

```python
from dataclasses import dataclass
from datetime import datetime


@dataclass
class RiskRow:
    account_id: str
    score: float
    computed_at: datetime
    reasons: list[str]


def churn_risk(account_id: str, as_of: datetime | None = None) -> list[RiskRow]:
    """Churn risk of one account.

    @table
    """
    ...
```

רשמו אותו כמקור `python` מחבילת ה-PyPI שלו, מ-commit ב-Git או מהעלאה. הגילוי מציע אז את `churn_risk` כטבלה עם ארבע עמודות (`account_id`, `score`, `computed_at`, `reasons`), פרמטר חובה `account_id` ופרמטר אופציונלי, `as_of`. אחרי ש-Steward רושם את הטבלה, קראו אותה ב-SQL:

```sql
SELECT score, reasons
FROM churn_risk
WHERE _nf_account_id = 'A-1042'
```

אותה פונקציה ב-TypeScript נראית כך:

```typescript
export interface RiskRow {
  account_id: string;
  score: number;
  computed_at: Date;
  reasons: string[];
}

/** Churn risk of one account.
 * @table
 */
export async function churnRisk(accountId: string, asOf?: Date): Promise<RiskRow[]> {
  // ...
}
```
