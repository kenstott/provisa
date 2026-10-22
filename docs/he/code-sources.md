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

טיפוס השורה קובע את העמודות. טקסט, מספרים, בוליאנים, תאריכים וחותמות זמן ממופים לטיפוסי העמודות שלהם, רשימות נשארות רשימות, ואובייקטים מקוננים הופכים לטיפוסים בעלי שם. ב-TypeScript טיפוס שורה הוא interface, כינוי טיפוס (type alias) או מחלקה. ב-Python הוא dataclass, `TypedDict`, `NamedTuple`, מודל Pydantic או כל מחלקה עם שדות מוערים (annotated). השורות של מודל Pydantic נשלחות כפי ש-`model_dump(by_alias=True)` מחזיר אותן, כך ששדה עם כינוי הופך לעמודה ששמה הוא הכינוי. התיעוד של השדה עצמו הופך לתיאור העמודה שלו כשאתם רושמים את הטבלה: הערת תיעוד על property ב-TypeScript, או `Field(description=...)` של Pydantic. כשטיפוס השורה רופף (`any`, `unknown` או `Record` ב-TypeScript, `Any` או `dict` ב-Python), Provisa קורא לפונקציה פעם אחת, בלי ארגומנטים, ומציע עמודות לפי השורות שחוזרות. אם לפונקציה יש פרמטר חובה אי אפשר לקרוא לה כך, ואתם קובעים את טיפוסי העמודות בעצמכם כשאתם רושמים את הטבלה.

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

## סודות {: #secrets }

קוד שקורא ל-API בתשלום צריך את המפתח שלו, והמפתח שייך לכספת של הארגון שלכם, לא לקוד. תחת **Secrets** בטופס של המקור, בחרו את סודות הארגון שהקוד רשאי לקרוא. בקובץ תצורה, פרטו אותם ב-mapping של המקור:

```yaml
mapping:
  secrets: [openrouter]
```

שם לבדו הוא סוד הארגון בשם הזה. כדי לקרוא במקום זאת משתנה סביבה של הפריסה, תנו את ההפניה: `secrets: {openrouter: "${env:OPENROUTER_API_KEY}"}`. הרשימה היא הפניה לכספת, אף פעם לא העתק של ערך, ולכן תצורה שמזכירה סוד שאיש עוד לא שמר נטענת בכל זאת.

הקוד שלכם מקבל משתנה סביבה אחד, `PROVISA_SERVICES`, וקורא סוד מ-`$PROVISA_SERVICES/secrets/<name>` כשהוא צריך אותו. ב-Python:

```python
import os, urllib.request
key = urllib.request.urlopen(f"{os.environ['PROVISA_SERVICES']}/secrets/openrouter").read().decode()
```

ב-TypeScript:

```typescript
const key = await (await fetch(`${process.env.PROVISA_SERVICES}/secrets/openrouter`)).text();
```

הבקשה עוברת דרך אותו proxy יוצא כמו שאר התעבורה של הקוד שלכם, וה-proxy עונה עליה בעצמו. שום דבר אחר של Provisa אינו נגיש בדרך הזו. הכתובת נושאת אסימון שרק ה-worker של המקור הזה מחזיק; הוא קורא רק את השמות שהמקור מפרט, והוא מפסיק לעבוד כשה-worker נעצר. כל קריאה פותרת את הסוד בכספת באותו רגע, כך שסוד שהוחלף תקף מהקריאה הבאה.

שם שהמקור אינו מפרט נדחה, וכך גם סוד שאי אפשר לפתור. הדחייה מציינת את הסוד, אף פעם לא את הערך שלו. כל קריאה נרשמת ביומן הביקורת של הארגון עם המקור, השם והשעה, ואף ערך אינו נכתב ללוג לעולם. המארח של ה-API עדיין חייב להופיע ברשימת היציאה המותרת.

## קריאה למודל {: #calling-a-model }

הקוד שלכם יכול לקרוא למודל שפה דרך Provisa במקום להחזיק מפתח של ספק. ‏`$PROVISA_SERVICES/llm/v1` הוא endpoint תואם OpenAI, כך שכל SDK של OpenAI עובד כשמכוונים אליו את כתובת הבסיס שלו. הכתובת היא האישור, ולכן המפתח שה-SDK מבקש יכול להיות כל טקסט. ב-Python:

```python
import os
from openai import OpenAI

client = OpenAI(base_url=f"{os.environ['PROVISA_SERVICES']}/llm/v1", api_key="unused")
reply = client.chat.completions.create(
    model="provisa:sql_generation",
    messages=[{"role": "user", "content": "Summarise this account's last quarter."}],
)
```

ב-TypeScript:

```typescript
import OpenAI from "openai";

const client = new OpenAI({ baseURL: `${process.env.PROVISA_SERVICES}/llm/v1`, apiKey: "unused" });
const reply = await client.chat.completions.create({
  model: "provisa:sql_generation",
  messages: [{ role: "user", content: "Summarise this account's last quarter." }],
});
```

ציינו מודל לפי התפקיד שלו, בצורה `provisa:<role>`: תקבלו את המודל שהארגון שלכם מקצה לתפקיד הזה תחת **Admin → AI Models**, והוא עוקב אחרי הבחירה הזו כשמנהל משנה אותה. אפשר גם לציין מודל ישירות כשהוא מוקצה לתפקיד. ל-embeddings, ‏`$PROVISA_SERVICES/llm/v1/embeddings` מקבל כל אחד ממודלי הווקטורים של הארגון שלכם. כל מודל אחר נדחה בשמו. הקריאה יוצאת דרך הספקים, ה-endpoints והמפתחות של הארגון שלכם, ומה שהיא שולחת עובר קודם דרך כללי ההשחרה של Provisa. סטרימינג אינו מוצע: שלחו `stream: false`, שהוא ברירת המחדל.

כל קריאה נרשמת עבור המקור, עם המודל, האסימונים שלו והעלות לפי הרשומה של המודל ב-**Polly model prices**, ונכתבת ליומן הביקורת של הארגון. כדי להגביל את מה שמקור מוציא, הגדירו **LLM monthly cap** בטופס שלו, ב-USD. בקובץ תצורה הוא נכנס ל-mapping:

```yaml
mapping:
  llm: {cap_usd: 20}
```

מקור עם תקרה יכול לקרוא רק למודלים שיש להם שם מחיר. ברגע שההוצאה שלו בחודש (UTC) מגיעה לתקרה, הקריאות שלו נדחות עד שהחודש מתחלף. קריאות שכבר רצות כשהתקרה מושגת מסתיימות בכל זאת, כך שהחודש יכול להסתיים מעט מעל התקרה.

## השירותים של Provisa {: #provisas-services }

הקוד שלכם יכול לייבא מודול אחד ש-Provisa מספק תמיד, בכל מקום שבו הוא רץ: `provisa_services` ב-Python, ו-`@provisa/services` ב-Node.js, ב-Bun וב-Deno. אין צורך להתקין דבר, והוא אינו נמצא בשום מאגר חבילות. מחוץ ל-Provisa המודול אינו קיים, ולכן בדיקות של הפונקציות שלכם מדמות אותו.

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
| `secrets.get(name)` | אחד הסודות שהמקור הצהיר עליהם, נקרא עכשיו מהכספת. ראו [סודות](#secrets). |
| `settings.get(name, default)` | אחת ההגדרות של המקור, מ-**Settings** בטופס שלו, ש-steward יכול לשנות בלי לשנות את הקוד. אם היא חסרה ואין ברירת מחדל, נזרקת שגיאה. |
| `cache.get(key, default)`, `cache.set(key, value, ttl)`, `cache.delete(key)` | המטמון של המקור עצמו, משותף לכל ה-workers שלו. ערך הוא כל מה ש-JSON יכול להחזיק, עד 1 MB, נשמר `ttl` שניות או עד שמוחלף. |
| `vectors.upsert(items, model)`, `vectors.search(query, k, model)`, `vectors.embed(texts, model)`, `vectors.delete(ids)` | אוסף הווקטורים של המקור עצמו, לנתוני הייחוס שלו. לפריט יש `id` ו-`text` או `vector`; טקסט מוטמע על ידי אחד ממודלי הווקטורים של הארגון שלכם. הוא אינו יכול לחפש בטבלאות המנוהלות שלכם: הנתונים שלהן מגיעים לקוד רק כפרמטרים. |
| `http.get(url)`, `http.post(url, ...)` | בקשה ש-Provisa מבצע עבור הקוד, דרך ה-proxy היוצא ורשויות האישורים של הפריסה. המארח חייב להופיע ברשימת היציאה המותרת, וכל בקשה נרשמת ביומן הביקורת. |
| `llm.chat(model, messages)`, `llm.embeddings(model, input)`, `llm.base_url()` (`llm.baseUrl()` ב-JavaScript) | שירות ה-LLM של Provisa. ראו [קריאה למודל](#calling-a-model). |
| `dq.dataset(name)`, `dq.product(id)` | איכות הנתונים של מערך נתונים (`domain.table` או מזהה הטבלה שלו) או של מוצר נתונים, כפי שתחום המטא מציג אותה לתפקיד של הבקשה שהקוד שלכם משרת. עבור מערך נתונים: טבלאות הבדיקה הסורקות אותו, כל אחת עם התיאור, החוזה, מוצר הנתונים שאליו היא משויכת וההרצה האחרונה שלה (התוצאה, הזמן והבדיקות שנכשלו), וכן הפרופילים שלו עם ההרצה האחרונה של כל אחד. עבור מוצר: ההבטחות, הציפיות, בדיקות השותפים והבדיקות היתומות שלו. |
| `log(message, level, fields)`, `metric(name, value, kind, attributes)` | שורת לוג ונקודת מדד בשם `code_source.<name>`, מצורפות לבקשה שהקוד שלכם משרת. |

קריאה שהשירות דוחה זורקת `ServiceError` שמציין את השירות ואת הסיבה.

השירותים עונים לקוד שרץ על שרת Provisa. על מארח מרוחק הם עדיין אינם נגישים, וכל קריאה זורקת שגיאה.

כל קריאה לקוד שלכם נעקבת כחלק מהבקשה שהפעילה אותה, עם הפונקציה, הסוג שלה, כמה ארגומנטים קיבלה (אף פעם לא הערכים שלהם), השורות והאצוות שהחזירה, משך הזמן שלה וה-CPU של ה-worker. כל קריאה לשירות נעקבת באותו אופן. ברמת פירוט רגילה העובדות האלה נמצאות על ה-span של הבקשה; ברמת debug כל קריאה היא span משלה. אם הקוד שלכם משתמש ב-OpenTelemetry, ה-spans שלו מצטרפים לאותו trace.

## הרצה על מארח מרוחק {: #running-on-a-remote-host }

מקור רץ כברירת מחדל על שרת Provisa; זהו מצב dev. כשהקוד שלו צריך מכונה שהשרת אינו, כמו מכונת GPU או מארח בתוך רשת אחרת, בנו את האימג' שלו והריצו אותו שם. המקור רץ אז במצב built, והטבלאות, ההרשאות והקוראים שלו נשארים אותו דבר.

**בניית האימג'.** שמרו קודם את המקור במצב dev, כדי שהקוד שלו ייקרא ויקובע. תחת **Remote host** בטופס שלו, בחרו את הארכיטקטורה (Linux x86-64 או ARM64) ולאן האימג' הולך: לאחד ממאגרי הקונטיינרים של הארגון שלכם, שהוזנו תחת **Admin → Container registries**, או להורדה. ‏Provisa מרכיב את האימג' בעצמו. הוא מכיל את הקוד ב-hash המקובע שלו, את התלויות שלו מותקנות לפלטפורמה הזו מה-lockfile של הקוד דרך מאגרי החבילות שלכם, את סביבת הריצה המקובעת ואת תוכנית המארח המרוחק, על אימג' בסיס מינימלי. שום דבר באימג' לא רץ בזמן הבנייה, ואף daemon של Docker אינו מעורב. התג של האימג' נושא את ה-hash של הקוד ואת הגרסה של Provisa, והתוויות שלו מציינות את סביבת הריצה, את ה-hash של הקוד ואת הייצואים.

**הרצה.** הטופס מציג שירות compose לאימג'. אימג' שהורד נטען קודם עם `docker load -i <file>`. הקונטיינר מאזין בפנים בפורט 7443 ושומר את הזהות שלו על ה-volume. את פורט המארח בוחרים עם `PROVISA_HOST_PORT`; בלעדיו השירות לא יעלה:

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

בהפעלה הראשונה הקונטיינר כותב ללוג שלו שורה שמתחילה ב-`pairing code:`.

**חיבור.** בחזרה בטופס, הזינו את המארח שבו Provisa מגיע לקונטיינר, את הפורט שפרסמתם ואת קוד הצימוד. ברשת Docker משותפת או ב-Kubernetes, הזינו את שם הקונטיינר או ה-Service ואת 7443, בלי לפרסם דבר. ‏Provisa מצמד את עצמו לקונטיינר ב-TLS הדדי. הקוד משמש פעם אחת, ומאז כל צד מקבל רק את השני. ‏Provisa בודק אז שהקונטיינר מריץ את הקוד של המקור ומציע את הייצואים שלו, ומעביר את המקור למצב built. כל תהליך של Provisa שומר חיבור משלו למארח, ומתחבר מחדש אם המארח מפסיק לענות. כל עוד המארח לא עונה, קריאות של הטבלאות של המקור נכשלות ומציינות את המארח.

**אם הקוד משתנה.** קונטיינר שחוזר עם קוד אחר נדחה, עם ה-hash שהמקור מקובע אליו וה-hash שהמארח מדווח, עד ש-steward בודק את הקוד ומקבע אותו מחדש מהטופס. כדי לשנות את הקוד עצמו, החזירו את המקור ל-dev, שמרו את הקוד החדש ובנו שוב.

בקונטיינר, מדיניות הרשת שלו היא גבול היציאה של הקוד; רשימת היציאה המותרת של השרת חלה על מצב dev.

## קריאת טבלה {: #reading-a-table }

טבלה של מקור קוד מונחתת, ולעולם לא נקראת במקומה. Provisa קורא לפונקציה עם הארגומנטים של הקריאה, בודק כל שורה מול העמודות, ושומר את השורות תחת הארגומנטים האלה למשך ה-TTL של המטמון של הטבלה. אחר כך חלים על השורות המונחתות נראות, מסכות וחוקי שורה, כמו על כל טבלה.

ארגומנטים מעבירים כמו כל פרמטר של טבלה: כתנאי `_nf_` ב-SQL וכארגומנטים של שדה ב-GraphQL.

## מפתחות באצווה: הפרמטר `loader` {: #batched-keys }

‏join יכול להגיע לטבלת קוד: השורות של טבלה אחרת מספקות את הפרמטרים שלה, כמו לקוחות שמצורפים לציון הנטישה שלהם. כברירת מחדל Provisa קורא לפונקציה פעם אחת לכל צירוף נפרד של ארגומנטים שהשורות המצורפות נותנות, ושולח את כל הקריאות האלה ל-worker בבקשה אחת.

פונקציה שמקבלת פרמטר בשם `loader` נקראת במקום זאת פעם אחת לכל אצווה, לפי הדפוס ש-DataLoader של GraphQL הפך למוכר. ‏`loader` הוא hash: מילון (אובייקט ב-TypeScript) מ-hash יציב של כל מפתח אל המפתח עצמו, אובייקט JSON שמחזיק את ערכי השורות המצורפות עבור שאר הפרמטרים של הפונקציה. Provisa מסיר מפתחות כפולים, כך שכל צירוף נפרד של ערכים מופיע פעם אחת. מפתח הוא דליל: שדה בלי ערך מושמט במקום להישלח כ-null, כך שערך ברירת המחדל של הפונקציה עצמה חל, כמו בקריאה יחידה.

הפונקציה מחזירה את אותה צורה: hash מכל מפתח שקיבלה אל השורות של אותו מפתח, שורות רגילות של הטבלה. Provisa נותן את השורות של כל מפתח לכל שורה מצורפת שחלקה את המפתח, ומפתח יכול להצביע על אפס שורות או על הרבה. התאמה לפי ה-hash, ולא לפי ערכי השורות, שומרת על התאמה של מפתח דליל: הפונקציה ממלאת את השדות החסרים שלו בערכי ברירת מחדל, ולכן הערכים שהיא רואה שונים מאלה של השורה המצורפת. ‏Provisa מחשב את ה-hash; הפונקציה רק מחזירה את המפתחות שקיבלה. הפונקציה עדיין מצהירה רק על סוג השורה שלה, רשימת שורות, והשורות של כל מפתח הן מהסוג הזה. הפרמטרים שלה לצד `loader` צריכים ערכי ברירת מחדל, כי קריאת loader מעבירה רק את `loader`.

פונקציה יכולה להגביל את גודל האצווה שלה, כמו `maxBatchSize` של DataLoader, באמצעות הצהרת `loader N` באחד משלושה מקומות: שורת `@loader 100` לצד `@table` ב-docstring או ב-JSDoc, הערת `/* @loader 100 */` ממש לפני הפרמטר `loader` ב-TypeScript, או מחרוזת `Annotated` על הפרמטר ב-Python, `loader: Annotated[dict[str, CustomerKey] | None, "loader 100"] = None`. ‏Provisa מחלק אז את המפתחות לאצוות בגודל הזה, קורא לפונקציה פעם אחת לכל אצווה, ומאחד את התשובות לפי ה-hash. בלי גודל, אצווה אחת מחזיקה את כל המפתחות. הגילוי דוחה בשם גודל בפונקציה בלי פרמטר `loader`, גודל שאינו מספר שלם חיובי, ושני גדלים בפונקציה אחת.

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

כשהיא מצורפת לטבלת לקוחות, היא נקראת עם `loader` כמו `{"9f2c...": {"customer_id": "637", "plan": "pro", "region": "EU"}, "41ab...": {"customer_id": "638"}, ...}`. המפתח השני נושא רק את `customer_id`, ולכן התוכנית והאזור שלו מקבלים את ערכי ברירת המחדל. הפונקציה מחזירה `{"9f2c...": [...], "41ab...": [...], ...}`, ו-Provisa נותן כל רשימת שורות לכל שורת לקוח שחלקה את המפתח הזה.

אותה פונקציה ב-TypeScript:

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
