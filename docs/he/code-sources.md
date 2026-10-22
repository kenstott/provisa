# Resolvers של קוד

לוגיקה עסקית ב-Python או ב-Node.

אם כתבתם פעם שרת GraphQL, אתם מכירים את הדפוס הזה. Resolver של קוד הוא פונקציה שעונה עבור סוג אובייקט אחד: ב-GraphQL, פונקציה המסומנת ב-`@table` *היא* ה-resolver של סוג הטבלה שלה. Provisa קוראת לה עם הארגומנטים של השאילתה ומפעילה ממשל על מה שהיא מחזירה.

מה שונה לעומת resolver שכותבים ידנית:

- **תחת ממשל.** נראות, מסכות וחוקי שורה חלים על השורות שהפונקציה מחזירה, כמו בכל טבלה.
- **באצוות.** פונקציה שמקבלת פרמטר `loader` מקבלת את כל המפתחות של join בקריאה אחת, דפוס ה-DataLoader.
- **דחיפת מסננים.** מסנן על עמודה מגיע לפונקציה כפרמטר כשאתם מבקשים זאת.
- **בכל מקום.** אותה פונקציה עונה ל-GraphQL, ל-SQL (דרך HTTP, pgwire ו-Arrow Flight), ל-Cypher (דרך HTTP ו-Bolt), ל-gRPC, ל-REST, ל-JSON:API ול-MCP.

Resolvers של קוד הם אחת משלוש הדרכים לכתוב לוגיקה עסקית ב-Provisa. ראו [לוגיקה עסקית ב-Provisa](business-logic.md).

## resolver ראשון {: #an-example }

```python
from dataclasses import dataclass


@dataclass
class Risk:
    score: float
    reasons: list[str]


def churn_risk(account_id: str) -> list[Risk]:
    """Churn risk of one account.

    @table
    """
    return [Risk(score=0.82, reasons=["no logins in 30 days"])]
```

כדי להגיש אותו:

1. הוסיפו מקור `python` והפנו אותו לקוד: חבילת PyPI, commit ב-Git, העלאה או תיקייה (ראו [מאיפה מגיע הקוד](#where-the-code-comes-from)).
2. שמרו אותו. Provisa מתקינה את הקוד, קוראת אותו ומציעה את `churn_risk` כטבלה עם העמודות `score` ו-`reasons` ופרמטר חובה אחד, `account_id`.
3. Steward רושם את הטבלה בתחום, כאן `accounts`, ומעניק אליה גישה כמו לכל טבלה אחרת.

אחר כך קראו אותה. ב-GraphQL הפרמטר הוא ארגומנט של שדה:

```graphql
{
  churnRisk(account_id: "A-1042") {
    score
    reasons
  }
}
```

ב-SQL הוא תנאי `_nf_`:

```sql
SELECT score, reasons
FROM accounts.churn_risk
WHERE _nf_account_id = 'A-1042'
```

Node עובד באותה דרך, עם תגי JSDoc וטיפוסי TypeScript: ראו [Node](#node).

## בנייה {: #build }

### למה הופכת פונקציה {: #what-a-function-becomes }

Provisa קוראת את ההצהרות של הקוד עצמו: docstring ורמזי טיפוס ב-Python, תגי JSDoc וטיפוסי TypeScript ב-Node.

| תג | הופך ל |
|---|---|
| `@table` | טבלה. הפונקציה מחזירה רשימה או iterable, וכל איבר הוא שורה. |
| `@command` | פקודה, שמורצת לפי בקשה. |
| `@stream` | טבלה חיה. הפונקציה היא generator, וכל שורה נשלחת ברגע שהיא מופקת. |

- פונקציה ללא תג מתעלמים ממנה.
- תג ש-Provisa לא מכירה נדחה, וכך גם טיפוס שהיא לא יודעת למפות. הדחייה מפרטת את כל הבעיות בבת אחת, כדי שתתקנו אותן במעבר אחד.

טיפוס השורה קובע את העמודות:

- טקסט, מספרים, בוליאנים, תאריכים וחותמות זמן ממופים לטיפוסי העמודה שלהם. רשימות נשארות רשימות, ואובייקטים מקוננים הופכים לטיפוסים בעלי שם.
- ב-Python טיפוס שורה הוא dataclass, `TypedDict`, `NamedTuple`, מודל Pydantic או כל מחלקה עם שדות מוערים. השורות של מודל Pydantic נשלחות כפי ש-`model_dump(by_alias=True)` מחזיר אותן, כך ששדה עם alias הופך לעמודה בשם ה-alias.
- ב-TypeScript טיפוס שורה הוא interface, כינוי טיפוס או מחלקה.
- התיעוד של השדה עצמו הופך לתיאור העמודה שלו כשאתם רושמים את הטבלה: `Field(description=...)` של Pydantic, או הערת תיעוד על מאפיין TypeScript.
- לטיפוס שורה רופף (`Any` או `dict` ב-Python, `any`, `unknown` או `Record` ב-TypeScript) אין עמודות לקרוא. Provisa קוראת לפונקציה פעם אחת בלי ארגומנטים ומציעה עמודות לפי השורות שחוזרות. אם לפונקציה יש פרמטר חובה, אי אפשר לקרוא לה כך, ואתם מזינים את טיפוסי העמודות בעצמכם כשאתם רושמים את הטבלה.

הפרמטרים של הפונקציה הופכים לפרמטרים של הטבלה:

- פרמטר ללא ערך ברירת מחדל הוא חובה. קריאה שמשמיטה אותו נדחית לפני שהקוד רץ.
- פרמטר עם ערך ברירת מחדל הוא רשות. השמיטו אותו וברירת המחדל שלכם חלה.
- Steward עדיין יכול להפוך פרמטר רשות לחובה.

### דחיפת מסננים {: #pushdown }

מסנן על אחת העמודות המוחזרות יכול להגיע לפונקציה כארגומנט, כך שהיא תביא רק את השורות שהתבקשו. הצהירו איזה פרמטר מקבל אותו באחת משלוש דרכים:

- **לפי שם.** פרמטר ששמו זהה בדיוק לעמודה מוחזרת מקבל שוויון על העמודה הזו. פרמטר בשם `<op>_<column>` מקבל את האופרטור הזה, כמו `gte_score` או `in_region`.
- **לפי תג.** שורה `@<op>_<column> <parameter>` (או `@<column> <parameter>`) ב-docstring או ב-JSDoc, או `/* @<op>_<column> */` בתוך השורה לפני פרמטר TypeScript.
- **לפי `Annotated`.** ב-Python, המחרוזת `"<op>_<column>"` (או `"<column>"`) על רמז הטיפוס של הפרמטר.

האופרטורים:

| `op` | מקבל |
|---|---|
| `ne`, `gt`, `gte`, `lt`, `lte` | ערך מטיפוס העמודה |
| `in` | רשימה מטיפוס העמודה |
| `like` | טקסט, על עמודת טקסט |

- טווח על עמודה ממלא את שני הפרמטרים שלה: `score BETWEEN 0.5 AND 0.9` נותן `gte_score` ו-`lte_score`.
- שמות עמודות מושווים בדיוק כפי שהצהרתם עליהם בטיפוס השורה. שם שתואם רק אחרי ש-Provisa משנה את שם העמודה עבור קורא אינו התאמה.
- הצהרה שהטיפוסים של הפונקציה לא יכולים לקבל נדחית.
- המסנן עדיין מוחל על השורות שהפונקציה מחזירה, כך שפונקציה שמתעלמת מערך שקיבלה או מרחיבה אותו לא יכולה להחזיר שורות שגויות.

### מפתחות באצוות: הפרמטר `loader` {: #batched-keys }

join יכול להגיע לטבלת קוד, כשהשורות של טבלה אחרת מספקות את הפרמטרים שלה: למשל לקוחות שמחוברים לציון הנטישה שלהם. כברירת מחדל Provisa קוראת לפונקציה פעם אחת לכל קבוצה נפרדת של ארגומנטים ששורות ה-join נותנות, ושולחת את כל הקריאות האלה ל-worker בבקשה אחת.

פונקציה שמקבלת פרמטר בשם `loader` נקראת במקום זאת פעם אחת לכל אצווה, הדפוס ש-DataLoader של GraphQL הפך למוכר.

- **מה היא מקבלת.** `loader` הוא hash: dict (אובייקט ב-TypeScript) מ-hash יציב של כל מפתח אל המפתח עצמו. מפתח הוא אובייקט JSON שמחזיק את הערכים של שורות ה-join עבור שאר הפרמטרים של הפונקציה.
- **ללא כפילויות.** כל צירוף נפרד של ערכים מופיע פעם אחת.
- **דליל.** שדה בלי ערך נשמט מהמפתח, כך שברירת המחדל של הפונקציה עצמה חלה, כמו בקריאה בודדת.
- **מה היא מחזירה.** hash מכל מפתח שקיבלה אל השורות של אותו מפתח: שורות רגילות של הטבלה, אף אחת או הרבה. Provisa מוסרת את השורות של כל מפתח לכל שורת join ששיתפה את המפתח.
- **התאמה לפי hash.** הפונקציה ממלאת בערכי ברירת מחדל את השדות החסרים של מפתח דליל, כך שהערכים שהיא רואה שונים מאלה של שורת ה-join. Provisa מחשבת את ה-hash, והפונקציה רק מחזירה את המפתחות שקיבלה.
- **טיפוס כרגיל.** הפונקציה עדיין מצהירה רק על טיפוס השורה שלה, רשימת שורות.
- **נדרשות ברירות מחדל.** לפרמטרים שלה לצד `loader` נדרשות ברירות מחדל, כי קריאת loader מעבירה רק את `loader`.
- **גודל אצווה.** הגבילו אותו, כמו `maxBatchSize` של DataLoader, עם `loader N` באחד משלושה מקומות: שורה `@loader 100` לצד `@table`, הערה `/* @loader 100 */` לפני הפרמטר `loader` ב-TypeScript, או מחרוזת `Annotated` ב-Python, `loader: Annotated[dict[str, CustomerKey] | None, "loader 100"] = None`. Provisa מחלקת אז את המפתחות לאצוות בגודל הזה וממזגת את התשובות לפי hash. בלי גודל, אצווה אחת מחזיקה את כל המפתחות.
- **נדחה.** גודל על פונקציה בלי פרמטר `loader`, גודל שאינו מספר שלם חיובי, ושני גדלים על פונקציה אחת.

השתמשו ב-`loader` כשקריאה אחת על מפתחות רבים עדיפה על קריאות רבות על מפתח אחד:

- הרצת מודל על אצווה שלמה במעבר וקטורי אחד.
- איגוד בקשות ל-API או למודל שפה שמקבל קלטים רבים בבת אחת.
- הכנה פעם אחת לכל אצווה, כמו טעינת מודל או פתיחת חיבור.

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
    score: float


def churn_score(
    customer_id: str | None = None,
    plan: str = "basic",
    region: str = "US",
    loader: dict[str, CustomerKey] | None = None,
) -> list[Score]:
    """Churn score of each customer in the batch.

    @table
    """
    keys = loader or {"": {"customer_id": customer_id, "plan": plan, "region": region}}
    rows = {h: {"plan": "basic", "region": "US", **key} for h, key in keys.items()}
    scores = MODEL.predict(features(list(rows.values())))  # one vectorized call for the batch
    out = {h: [Score(r["customer_id"], s)] for (h, r), s in zip(rows.items(), scores)}
    return out if loader else out[""]  # a loader call returns the hash of key to rows
```

כשהיא מחוברת לטבלת לקוחות, קוראים לה עם `loader` כמו `{"9f2c...": {"customer_id": "637", "plan": "pro", "region": "EU"}, "41ab...": {"customer_id": "638"}, ...}`. המפתח השני נושא רק `customer_id`, ולכן התוכנית והאזור שלו מקבלים את ברירות המחדל. הפונקציה מחזירה `{"9f2c...": [...], "41ab...": [...], ...}`.

### מאיפה מגיע הקוד {: #where-the-code-comes-from }

- **חבילה.** חבילת npm עבור Node או חבילת PyPI עבור Python, בגרסה מדויקת.
- **מאגר Git.** commit מלא בן 40 תווים, עם תת-תיקייה אם הקוד לא נמצא בשורש.
- **העלאה.** קובץ `.zip`, `.tgz` או `.tar.gz` של הקוד.
- **תיקייה במחשב שלכם.** **בחירת תיקייה** מכווצת את התיקייה בדפדפן, בלי `node_modules`, `.venv`, `__pycache__` ו-`.git`, ומעלה אותה כארכיון של המקור. בדפדפן שיכול לשמור את התיקייה (Chrome, Edge), **שמירה** מכווצת ושולחת אותה שוב; דפדפן אחר מבקש מכם את התיקייה מחדש.
- **תיקייה בשרת.** בהתקנה של ארגון יחיד, תיקייה מתחת לאלה שהמפעיל מתיר. היא נקראת במקומה, כך ששינוי נקרא בשאילתה הבאה. שרת של ארגונים רבים דוחה אותה.

כל מקור קוד מלבד התיקייה בשרת מקובע, כך שהמקור מריץ בדיוק את מה שנשמר.

- **מודול כניסה.** כשלקוד Python יש יותר ממודול עליון אחד, ציינו את זה שנכנסים דרכו.
- **פתיחה ב-VS Code.** תיקייה בשרת נפתחת ב-VS Code המותקן אצלכם; מקור Git נפתח ב-commit שלו ב-vscode.dev. לחבילה או להעלאה אין קישור כזה.

### התקנת תלויות {: #installing-dependencies }

קוד עם תלויות צריך lockfile, וכל התקנה עוקבת אחריו בדיוק. בלעדיו, חבילה שמצהירה על תלויות נדחית, והדחייה מבקשת להוסיף אחד.

| סביבת ריצה | Lockfile |
|---|---|
| Node.js | `package-lock.json` או `npm-shrinkwrap.json` |
| Bun | `bun.lock` או `bun.lockb` |
| Deno | `deno.lock` |
| Python | `uv.lock` (עם `pyproject.toml` שלו), `pylock.toml`, או `requirements.txt` עם `--hash` בכל שורה |

- **מצורף או מסופק.** קוד שמביא lockfile משלו מותקן ממנו: מאגר Git, העלאה או תיקייה שיש בהם אחד, או חבילת npm שהמחבר שלה פרסם `npm-shrinkwrap.json` (npm לעולם לא מפרסם `package-lock.json`). לרוב חבילות npm ולחבילות Python מ-PyPI אין אחד, אז ספקו lockfile לצידן.
- **למה זה חשוב.** בלעדיו, שתי התקנות של אותו קוד יכולות להיות שונות, ובדיקה שעוברת היום יכולה להיכשל מחר בגרסת תלות חדשה.
- **אימג' בנוי כבר קבוע.** ברגע ש[בונים את האימג' של המקור](#running-on-a-remote-host), התלויות שלו בתוכו. Lockfile חשוב למצב dev ולכל בנייה חדשה.
- **בלי סקריפטי התקנה.** npm מתקין עם `--ignore-scripts`, ו-Python מתקין רק wheels, ואף פעם לא בונה מהפצת מקור.
- **המאגרים שלכם.** החבילות מגיעות ממאגרי ה-npm, מאינדקסי PyPI ומשרתי Git של הארגון שלכם, שמוזנים פעם אחת תחת **Admin → Package registries**. מאגר npm הציבורי ו-PyPI לא צריכים רשומה. ראו [Package Registries](package-registries.md).
- **במטמון.** התקנה נשמרת במטמון לפי hash התוכן של הקוד, כך שאותו קוד מותקן פעם אחת.

### Node {: #node }

- קוד Node רץ על Node.js, Bun או Deno, לפי בחירתכם.
- התגים נכתבים ב-JSDoc, והטיפוסים מגיעים מ-TypeScript.

ה-resolver הראשון ב-TypeScript:

```typescript
export interface Risk {
  score: number;
  reasons: string[];
}

/** Churn risk of one account.
 * @table
 */
export async function churnRisk(account_id: string): Promise<Risk[]> {
  return [{ score: 0.82, reasons: ["no logins in 30 days"] }];
}
```

ה-`churnScore` באצוות ב-TypeScript:

```typescript
export interface CustomerKey {
  customer_id: string;
  plan?: string;
  region?: string;
}

export interface Score {
  customer_id: string;
  score: number;
}

/** Churn score of each customer in the batch.
 * @table
 */
export async function churnScore(
  customer_id?: string,
  plan = "basic",
  region = "US",
  loader?: Record<string, CustomerKey>,
): Promise<Score[]> {
  const keys = loader ?? { "": { customer_id, plan, region } };
  const rows = Object.entries(keys).map(([h, key]) => [h, { plan: "basic", region: "US", ...key }] as const);
  const scores = await model.predict(rows.map(([, row]) => row)); // one call for the whole batch
  const out = Object.fromEntries(
    rows.map(([h, row], i) => [h, [{ customer_id: row.customer_id, score: scores[i] }]]),
  );
  return loader ? out : out[""]; // a loader call returns the hash of key to rows
}
```

## פריסה {: #deployment }

### מצב dev ו-workers {: #workers }

כברירת מחדל מקור רץ על שרת Provisa; זה מצב dev.

- הקוד שלכם רץ בתהליך worker משלו, אחד לכל מקור וגרסת קוד בכל תהליך Provisa, שמופעל בקריאה הראשונה שלו.
- worker שקורס מכשיל את הקריאות שבתהליך, ונוקב בשם המקור והפונקציה. הקריאה הבאה מפעילה worker חדש.
- קריאה מבוטלת כשמועד היעד של הבקשה שלה חולף: ה-worker מתבקש לעצור אותה, והבקשה הממתינה משתחררת מיד.

### קריאת טבלה {: #reading-a-table }

האופן שבו קריאה מגיעה לפונקציה תלוי במה עוד היא קוראת:

- **קריאה של טבלת קוד אחת בלבד** הולכת ישירות לפונקציה. שום דבר לא נשמר. Provisa קוראת לה עם הארגומנטים של הקריאה והמסננים שנדחפו, ומחילה את שאר המשפט על השורות שהיא מחזירה.
- **קריאה שמחברת טבלאות אחרות** שייכת למנוע הפדרציה. הטבלה של מקור במצב dev נשמרת עבורה: השורות נשמרות לפי אותם ארגומנטים למשך ה-TTL של מטמון הטבלה.
- **הטבלה של מקור בנוי** ב-join כזה נקראת בזמן אמת מהשרת המרוחק שלה, שהמנוע מצרף כקטלוג Postgres. היא נשמרת במקום זאת כשהמקור שלה משוכפל או מוגבל ברצפה, כשהטבלה נשמרת לתמיד, כשהיא מצהירה על TTL שמירה גדול מ-0, או במנוע שלא יכול לצרף שרת.

בכל מקרה, נראות, מסכות וחוקי שורה חלים על השורות, כמו בכל טבלה. מעבירים ארגומנטים כמו כל פרמטר של טבלה: תנאי `_nf_` ב-SQL, ארגומנטים של שדות ב-GraphQL.

### הרצה על שרת מרוחק {: #running-on-a-remote-host }

כשהקוד צריך מכונה שאינה השרת, כמו מחשב עם GPU או שרת בתוך רשת אחרת, בנו את האימג' שלו והריצו אותו שם. המקור עובר אז למצב בנוי, והטבלאות, ההרשאות והקוראים שלו נשארים אותו דבר.

**בניית האימג'.**

- שמרו קודם את המקור במצב dev, כדי שהקוד שלו ייקרא ויקובע.
- תחת **Remote host** בטופס שלו, בחרו את הארכיטקטורה (Linux x86-64 או ARM64) ולאן האימג' הולך: אחד ממאגרי הקונטיינרים של הארגון שלכם (**Admin → Container registries**), או הורדה.
- Provisa מרכיבה את האימג' בעצמה, על בסיס מינימלי, בלי daemon של Docker: הקוד ב-hash המקובע שלו, התלויות שלו מותקנות לפלטפורמה הזו מה-lockfile שלו דרך מאגרי החבילות שלכם, סביבת הריצה המקובעת ותוכנית השרת המרוחק. שום דבר באימג' לא רץ בזמן שהוא נבנה.
- התג של האימג' נושא את hash הקוד ואת גרסת Provisa, והתוויות שלו נוקבות בסביבת הריצה, ב-hash הקוד ובייצואים.

**הרצה.** הטופס מציג שירות compose עבור האימג'. טענו קודם אימג' שהורד עם `docker load -i <file>`. הקונטיינר מאזין על 7443 ושומר את הזהות שלו על ה-volume שלו. את פורט השרת בוחרים עם `PROVISA_HOST_PORT`; השירות לא יעלה בלעדיו.

```yaml
services:
  provisa-code-rates:
    image: ghcr.io/example/provisa-code-rates:3f9a1c2e7b40-0.1.0
    restart: unless-stopped
    ports:
      - "${PROVISA_HOST_PORT:?set PROVISA_HOST_PORT to a free port on this host}:7443"
    volumes:
      - provisa-code-rates-state:/var/lib/provisa
volumes:
  provisa-code-rates-state: {}
```

בהפעלה הראשונה הקונטיינר מדפיס ביומן שלו שורה שמתחילה ב-`pairing code:`.

**חיבור.**

- בטופס, הזינו את השרת שדרכו Provisa מגיעה לקונטיינר, את הפורט שפרסמתם ואת קוד הצימוד. ברשת Docker משותפת או ב-Kubernetes, הזינו את שם הקונטיינר או ה-Service ואת 7443, בלי לפרסם דבר.
- Provisa מצמדת את עצמה לקונטיינר דרך TLS הדדי. הקוד משמש פעם אחת; מאז כל צד מקבל רק את השני.
- Provisa בודקת שהקונטיינר מריץ את הקוד של המקור ומציע את הייצואים שלו, ואז מעבירה את המקור למצב בנוי.
- כל תהליך Provisa מחזיק חיבור משלו לשרת ומתחבר שוב אם השרת מפסיק לענות. כל עוד הוא לא עונה, קריאות של הטבלאות של המקור נכשלות ונוקבות בשם השרת.

**אם הקוד משתנה.**

- קונטיינר שחוזר עם קוד אחר נדחה, תוך ציון ה-hash שהמקור מקובע אליו וזה שהשרת מדווח, עד ש-Steward בודק את הקוד ומקבע אותו מחדש מהטופס.
- כדי לשנות את הקוד עצמו, החזירו את המקור ל-dev, שמרו את הקוד החדש ובנו שוב.

### השירותים של Provisa {: #provisas-services }

הקוד שלכם יכול לייבא מודול ש-Provisa מספקת תמיד בשרת: `provisa_services` ב-Python, `@provisa/services` ב-Node.js, Bun ו-Deno. אין מה להתקין, והוא לא נמצא באף מאגר חבילות. מחוץ ל-Provisa הוא לא קיים, ולכן הבדיקות שלכם מדמות אותו.

```python
import provisa_services as ps

region = ps.settings.get("region")
rates = ps.cache.get("rates") or fetch_rates()
ps.cache.set("rates", rates, ttl=3600)
```

```typescript
import { settings, cache } from "@provisa/services";

const region = await settings.get("region");
const rates = (await cache.get("rates")) ?? (await fetchRates());
await cache.set("rates", rates, 3600);
```

| קריאה | מה היא עושה |
|---|---|
| `secrets.get(name)` | אחד מהסודות שהמקור הצהיר עליהם, נקרא עכשיו מהכספת. ראו [סודות](#secrets). |
| `settings.get(name, default)`, `settings.all()` | ההגדרות של המקור, מ-**Settings** בטופס שלו, ש-Steward יכול לשנות בלי לשנות את הקוד. הגדרה חסרה בלי ברירת מחדל זורקת שגיאה. |
| `cache.get(key, default)`, `cache.set(key, value, ttl)`, `cache.delete(key)` | המטמון של המקור עצמו, משותף לכל ה-workers שלו. ערך הוא כל מה ש-JSON יכול להחזיק, עד 1 MB, שנשמר `ttl` שניות או עד שמוחלף. |
| `vectors.upsert(items, model)`, `vectors.search(query, k, model)`, `vectors.embed(texts, model)`, `vectors.delete(ids)` | אוסף הווקטורים של המקור עצמו, לנתוני הייחוס שלו. לפריט יש `id` ו-`text` או `vector`; טקסט מוטמע על ידי אחד ממודלי הווקטורים של הארגון שלכם. הוא לא יכול לחפש בטבלאות שתחת ממשל, שהנתונים שלהן מגיעים לקוד רק כפרמטרים. |
| `http.get(url)`, `http.post(url, ...)` | בקשה ש-Provisa מבצעת עבור הקוד, דרך ה-proxy היוצא ורשויות האישורים של הפריסה. השרת חייב להיות ברשימת היציאה המותרת, וכל בקשה נרשמת במסלול הביקורת. |
| `llm.chat(model, messages)`, `llm.embeddings(model, input)`, `llm.base_url()` (`llm.baseUrl()` ב-JavaScript) | שירות ה-LLM של Provisa. ראו [קריאה למודל](#calling-a-model). |
| `log(message, level, fields)`, `metric(name, value, kind, attributes)` | שורת יומן, ונקודת מדד בשם `code_source.<name>`, שמצורפות לבקשה שהקוד שלכם משרת. |

- קריאה שהשירות דוחה זורקת `ServiceError` שנוקבת בשם השירות ואומרת למה.
- השירותים עונים לקוד שרץ על שרת Provisa. על שרת מרוחק כל קריאה זורקת שגיאה.

#### סודות {: #secrets }

מפתח ל-API בתשלום שייך לכספת של הארגון שלכם, לא לקוד.

- תחת **Secrets** בטופס של המקור, בחרו את סודות הארגון שהקוד רשאי לקרוא. בקובץ תצורה, פרטו אותם ב-mapping של המקור:

    ```yaml
    mapping:
      secrets: [openrouter]
    ```

- שם לבדו הוא סוד הארגון בשם הזה. כדי לקרוא במקום זאת משתנה סביבה של הפריסה, תנו את ההפניה: `secrets: {openrouter: "${env:OPENROUTER_API_KEY}"}`.
- הרשימה מפנה לכספת ולעולם לא מעתיקה ערך, כך שתצורה שנוקבת בסוד שאיש עוד לא שמר עדיין נטענת.

הקוד שלכם קורא סוד עם `secrets.get(name)`, או ישירות מ-`$PROVISA_SERVICES/secrets/<name>`:

```python
import os, urllib.request
key = urllib.request.urlopen(f"{os.environ['PROVISA_SERVICES']}/secrets/openrouter").read().decode()
```

```typescript
const key = await (await fetch(`${process.env.PROVISA_SERVICES}/secrets/openrouter`)).text();
```

- `PROVISA_SERVICES` הוא משתנה הסביבה היחיד שהקוד שלכם מקבל. הבקשה עוברת דרך אותו proxy יציאה כמו שאר התעבורה שלו, וה-proxy עונה עליה בעצמו; שום חלק אחר של Provisa לא נגיש בדרך הזו.
- הכתובת נושאת אסימון שרק ה-worker של המקור הזה מחזיק. היא קוראת רק את השמות שהמקור מפרט, ומפסיקה לעבוד כשה-worker נעצר.
- כל קריאה מפענחת את הסוד בכספת באותו רגע, כך שסוד שהוחלף נכנס לתוקף בקריאה הבאה.
- שם שהמקור לא מפרט נדחה, וכך גם סוד שאי אפשר לפענח. הדחייה נוקבת בשם הסוד, לעולם לא בערך שלו.
- כל קריאה נרשמת במסלול הביקורת של הארגון עם המקור, השם והשעה. אף ערך לא נכתב לעולם ליומן.
- השרת שה-API נמצא עליו עדיין חייב להיות ברשימת היציאה המותרת.

#### קריאה למודל {: #calling-a-model }

הקוד שלכם יכול לקרוא למודל שפה דרך Provisa במקום להחזיק מפתח של ספק. `$PROVISA_SERVICES/llm/v1` הוא נקודת קצה תואמת OpenAI, כך שכל SDK של OpenAI עובד כשכתובת הבסיס שלו מופנית לשם. הכתובת היא האישור, ולכן המפתח שה-SDK מבקש יכול להיות כל טקסט.

```python
import os
from openai import OpenAI

client = OpenAI(base_url=f"{os.environ['PROVISA_SERVICES']}/llm/v1", api_key="unused")
reply = client.chat.completions.create(
    model="provisa:sql_generation",
    messages=[{"role": "user", "content": "Summarise this account's last quarter."}],
)
```

```typescript
import OpenAI from "openai";

const client = new OpenAI({ baseURL: `${process.env.PROVISA_SERVICES}/llm/v1`, apiKey: "unused" });
const reply = await client.chat.completions.create({
  model: "provisa:sql_generation",
  messages: [{ role: "user", content: "Summarise this account's last quarter." }],
});
```

- **לפי תפקיד.** כתבו מודל כ-`provisa:<role>` ותקבלו את המודל שהארגון שלכם מקצה לתפקיד הזה תחת **Admin → AI Models**; הוא עוקב אחרי הבחירה הזו כשמנהל משנה אותה. אפשר גם לנקוב במודל ישירות כשמוקצה לו תפקיד.
- **Embeddings.** `$PROVISA_SERVICES/llm/v1/embeddings` מקבל כל אחד ממודלי הווקטורים של הארגון שלכם.
- כל מודל אחר נדחה.
- הקריאה יוצאת דרך הספקים, נקודות הקצה והמפתחות של הארגון שלכם, ומה שהיא שולחת עובר קודם דרך כללי ההשחרה של Provisa.
- סטרימינג לא מוצע. שלחו `stream: false`, שזו ברירת המחדל.

## אמצעי הגנה {: #safeguards }

### ה-sandbox {: #the-sandbox }

- הקוד שלכם רץ בתהליך worker משלו, בתוך sandbox. הוא יכול לקרוא את הקוד המותקן שלו ושום דבר אחר בדיסק.
- הוא מגיע רק לשרתים שברשימת היציאה המותרת של הפריסה, דרך proxy ש-Provisa מריצה, ולעולם לא לפורטים של Provisa עצמה.
- שרת שלא יכול לספק את ה-sandbox מסרב להפעיל את ה-worker ונוקב במה שחסר; הקוד לעולם לא רץ בלי הגבלה. ב-Windows, מקורות קוד נדחים.
- על שרת מרוחק, מדיניות הרשת של הקונטיינר היא גבול היציאה של הקוד. רשימת היציאה המותרת של השרת חלה על מצב dev.

### דחיות {: #refusals }

כל אחד מהמקרים האלה נדחה, לפני שהקוד רץ כשהדבר אפשרי:

- תג לא מוכר או טיפוס ש-Provisa לא יודעת למפות, כל הבעיות של פונקציה בבת אחת.
- קריאה שמשמיטה פרמטר חובה.
- הצהרת דחיפת מסננים או `loader` שהטיפוסים של הפונקציה לא יכולים לקבל.
- חבילה עם תלויות ובלי lockfile.
- תיקייה בשרת מחוץ לאלה המותרות, או על שרת של ארגונים רבים.
- שרת מרוחק שמריץ קוד שונה מזה שהמקור מקובע אליו.
- סוד שהמקור לא מפרט, מודל שאינו מותר, קריאת שירות שהשירות לא עונה עליה.

### מדידה ותקרות {: #metering }

- כל קריאה למודל נרשמת עבור המקור, עם המודל, האסימונים שלו והעלות שלו לפי הרשומה של המודל ב-**Polly model prices**, ונכתבת למסלול הביקורת של הארגון.
- כדי להגביל כמה מקור מוציא, הגדירו **LLM monthly cap** בטופס שלו, בדולר. בקובץ תצורה זה נכנס ל-mapping:

    ```yaml
    mapping:
      llm: {cap_usd: 20}
    ```

- מקור עם תקרה יכול לקרוא רק למודלים שיש להם מחיר שם.
- ברגע שההוצאה שלו לחודש (UTC) מגיעה לתקרה, הקריאות שלו נדחות עד שהחודש מתחלף. קריאות שכבר רצות כשמגיעים לתקרה עדיין מסתיימות, כך שהחודש יכול להסתיים מעט מעל התקרה.

### ממשל על התוצאות {: #governance }

- השורות של הפונקציה נבדקות מול העמודות של הטבלה.
- נראות, מסכות וחוקי שורה חלים אז עבור כל קורא, כמו בכל טבלה.
- הקוד שלכם רואה את הנתונים של טבלה תחת ממשל רק כפרמטרים שקריאה מעבירה לו.

### מעקב {: #tracing }

- כל קריאה לקוד שלכם נעקבת כחלק מהבקשה שהפעילה אותה: הפונקציה, הסוג שלה, כמה ארגומנטים קיבלה (לעולם לא הערכים שלהם), השורות והאצוות שהחזירה, משך הזמן שלה וה-CPU של ה-worker.
- כל קריאת שירות נעקבת באותו אופן.
- ברמת הפירוט הרגילה הנתונים האלה נמצאים על ה-span של הבקשה; ברמת debug כל קריאה היא span נפרד.
- אם הקוד שלכם משתמש ב-OpenTelemetry, ה-spans שלו מצטרפים לאותו trace.
