# Sources Node et Python

Il arrive que les données dont vous avez besoin se trouvent derrière du code que votre équipe a déjà écrit : un modèle de scoring, un client pour un service interne, un script qui nettoie un export. Une source Node ou Python vous permet de servir ce code sous forme de tables. Vous marquez les fonctions à partager, vous indiquez à Provisa où se trouve le code, et chaque fonction marquée devient une table que chaque rôle lit selon les mêmes règles que n'importe quelle autre table.

## Ce que devient une fonction {: #what-a-function-becomes }

Provisa lit les déclarations de votre code lui-même : balises JSDoc et types TypeScript pour Node, docstrings et annotations de type pour Python. Trois balises comptent.

| Balise | Devient |
|---|---|
| `@table` | Une table. La fonction renvoie une liste ou un itérable, et chaque élément est une ligne |
| `@command` | Une command, exécutée à la demande |
| `@stream` | Une table en direct. La fonction est un générateur, et chaque ligne est envoyée dès qu'elle est produite |

Une fonction sans balise est ignorée. Une balise que Provisa ne connaît pas est refusée, tout comme un type qu'il ne sait pas convertir. Le refus nomme la fonction et liste tous les problèmes d'un coup, pour que vous les corrigiez en une seule passe.

Le type de ligne donne les colonnes. Texte, nombres, booléens, dates et horodatages correspondent à leurs types de colonne, les listes restent des listes, et les objets imbriqués deviennent des types nommés. Quand le type d'une ligne est lâche (`any`, `unknown` ou un `Record` en TypeScript, `Any` ou un `dict` en Python), Provisa appelle la fonction une fois, sans arguments, et propose des colonnes d'après les lignes renvoyées. Si la fonction a un paramètre obligatoire, elle ne peut pas être appelée ainsi, et vous typez les colonnes vous-même en enregistrant la table.

Les paramètres de la fonction deviennent les paramètres de la table. Un paramètre sans valeur par défaut est obligatoire : une lecture qui l'omet est refusée avant que votre code ne s'exécute. Un paramètre avec valeur par défaut est facultatif ; omettez-le et votre valeur par défaut s'applique. Un intendant peut tout de même rendre obligatoire un paramètre facultatif.

## D'où vient le code {: #where-the-code-comes-from }

Le code est épinglé, si bien qu'une source exécute toujours exactement ce qui a été enregistré :

- **Un paquet** : un paquet npm pour Node ou un paquet PyPI pour Python, dans une version exacte.
- **Un dépôt Git** : un commit complet de 40 caractères, avec un sous-répertoire si le code n'est pas à la racine.
- **Un téléversement** : un `.zip`, `.tgz` ou `.tar.gz` du code.

Le code Node s'exécute sur Node.js, Bun ou Deno, selon votre choix. Le code Python s'exécute sur le Python fourni avec Provisa. Quand le code a plus d'un module Python de premier niveau, indiquez dans **Module d'entrée** celui par lequel il est chargé.

## Installer les dépendances {: #installing-dependencies }

Un code qui a des dépendances a besoin d'un lockfile, et chaque installation le suit à la lettre. Sans lockfile, un paquet qui déclare des dépendances est refusé, et le refus vous dit d'en ajouter un.

| Runtime | Lockfile |
|---|---|
| Node.js | `package-lock.json` ou `npm-shrinkwrap.json` |
| Bun | `bun.lock` ou `bun.lockb` |
| Deno | `deno.lock` |
| Python | `uv.lock` (avec son `pyproject.toml`), `pylock.toml`, ou un `requirements.txt` avec `--hash` sur chaque ligne |

Un lockfile est facultatif, mais sans lui deux installations du même code peuvent différer, si bien qu'un test qui passe aujourd'hui peut échouer demain sur une nouvelle version d'une dépendance. Pour une installation identique à chaque fois, le code doit contenir un lockfile, ou vous en créez un et le fournissez ici. Une image construite est déjà figée. Une fois l'image de la source construite ([Construire l'image](#running-on-a-remote-host)), ses dépendances sont à l'intérieur, et si c'est cette image qui passe par vos tests et votre CI/CD, l'image testée est celle qui s'exécute. Un lockfile compte pour le mode dev et pour chaque nouvelle construction de l'image. Le code qui contient son propre lockfile s'installe à partir de celui-ci : un dépôt Git ou un téléversement qui en a un, ou un paquet npm dont l'auteur a publié `npm-shrinkwrap.json` (npm ne publie jamais `package-lock.json`). Le code sans lockfile s'installe avec des dépendances non figées : un dépôt Git ou un téléversement sans lockfile, la plupart des paquets npm et les paquets Python de PyPI, dont les wheels n'ont pas de place pour en contenir un. Fournissez un lockfile à côté de ce code pour que chaque installation soit identique. Certains codes n'en ont pas besoin, car leur mode de distribution fixe déjà chaque dépendance.

Aucun paquet ne peut exécuter ses propres scripts d'installation : npm installe avec `--ignore-scripts`, et Python n'installe que des wheels, sans jamais construire à partir d'une distribution source.

Les paquets proviennent des registres de votre organisation quand elle en a. Un administrateur saisit une fois vos registres npm, index PyPI et hôtes Git dans **Admin → Registres de paquets**, et chaque source Node et Python de l'organisation installe par leur biais. Le registre npm public et le PyPI public n'ont besoin d'aucune entrée. Voir [Registres de paquets](package-registries.md).

Une installation est mise en cache selon le hash de contenu du code : le même code ne s'installe qu'une fois.

## Le bac à sable {: #the-sandbox }

Votre code s'exécute dans son propre processus worker, dans un bac à sable. Il peut lire son propre code installé et rien d'autre sur le disque. Il ne joint que les hôtes de la liste d'autorisation de sortie du déploiement, par un proxy que Provisa exécute, et jamais les ports de Provisa lui-même. Un hôte qui ne peut pas fournir le bac à sable refuse de démarrer le worker et nomme ce qui manque, plutôt que d'exécuter le code sans confinement. Sous Windows, les sources de code sont refusées.

## Lire une table {: #reading-a-table }

La table d'une source de code est atterrie, jamais lue sur place. Provisa appelle la fonction avec les arguments de la lecture, vérifie chaque ligne par rapport aux colonnes, et conserve les lignes sous ces arguments pendant le TTL de cache de la table. La visibilité, les masques et les règles de ligne s'appliquent ensuite aux lignes atterries, comme pour toute table.

Vous passez les arguments comme n'importe quel paramètre de table : en conditions `_nf_` en SQL et en arguments de champ en GraphQL.

## Clés par lots : le paramètre `loader` {: #batched-keys }

Une jointure peut atteindre une table de code : les lignes d'une autre table fournissent ses paramètres, comme des clients joints à leur score d'attrition. Par défaut, Provisa appelle la fonction une fois pour chaque ensemble distinct d'arguments que donnent les lignes jointes, et envoie tous ces appels au worker en une seule requête.

Une fonction qui prend un paramètre nommé `loader` est appelée à la place une fois par lot, le motif que DataLoader de GraphQL a rendu familier. `loader` est un hash : un dict (un objet en TypeScript) qui associe un hash stable de chaque clé à la clé elle-même, un objet JSON contenant les valeurs des lignes jointes pour les autres paramètres de la fonction. Provisa dédoublonne les clés, si bien que chaque combinaison distincte de valeurs n'apparaît qu'une fois. Une clé est creuse : un champ sans valeur est omis au lieu d'être envoyé à null, de sorte que la valeur par défaut de la fonction s'applique, comme lors d'un appel unique.

La fonction renvoie la même forme : un hash qui associe chaque clé reçue aux lignes de cette clé, des lignes ordinaires de la table. Provisa donne les lignes de chaque clé à toutes les lignes jointes qui partageaient cette clé, et une clé peut correspondre à zéro ou plusieurs lignes. Rapprocher par le hash, et non par les valeurs des lignes, garde une clé creuse rapprochée : la fonction complète ses champs manquants avec des valeurs par défaut, donc les valeurs qu'elle voit diffèrent de celles de la ligne jointe. Provisa calcule le hash ; la fonction ne fait que renvoyer les clés qu'elle a reçues. La fonction déclare toujours son seul type de ligne, une liste de lignes, et les lignes de chaque clé sont de ce type. Ses paramètres à côté de `loader` ont besoin de valeurs par défaut, car un appel avec loader ne passe que `loader`.

Une fonction peut plafonner la taille de ses lots, comme le fait `maxBatchSize` de DataLoader, en déclarant `loader N` à l'un de trois endroits : une ligne `@loader 100` à côté de `@table` dans sa docstring ou son JSDoc, un commentaire `/* @loader 100 */` juste avant le paramètre `loader` en TypeScript, ou une chaîne `Annotated` sur le paramètre en Python, `loader: Annotated[dict[str, CustomerKey] | None, "loader 100"] = None`. Provisa découpe alors les clés en lots de cette taille, appelle la fonction une fois par lot et fusionne les réponses par hash. Sans taille, un seul lot contient toutes les clés. La découverte refuse, en le nommant, une taille sur une fonction sans paramètre `loader`, une taille qui n'est pas un entier positif et deux tailles sur une même fonction.

Utilisez `loader` quand un appel sur de nombreuses clés fait mieux que de nombreux appels sur une seule :

- Évaluer un modèle sur un lot entier en une seule passe vectorisée.
- Regrouper les requêtes vers une API ou un modèle de langage qui accepte de nombreuses entrées à la fois.
- Préparer une fois par lot, comme charger un modèle ou ouvrir une connexion, plutôt qu'une fois par clé.

Cette fonction Python évalue chaque client d'un lot en une seule passe :

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

Jointe à une table de clients, elle est appelée avec un `loader` tel que `{"9f2c...": {"customer_id": "637", "plan": "pro", "region": "EU"}, "41ab...": {"customer_id": "638"}, ...}`. La deuxième clé ne porte que `customer_id`, donc son plan et sa région prennent les valeurs par défaut. La fonction renvoie `{"9f2c...": [...], "41ab...": [...], ...}`, et Provisa donne chaque liste de lignes à chaque ligne de client qui partageait cette clé.

La même fonction en TypeScript :

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

## Un exemple {: #an-example }

Ce module Python estime la probabilité qu'un compte parte :

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

Enregistrez-le comme source `python` à partir de son paquet PyPI, de son commit Git ou d'un téléversement. La découverte propose alors `churn_risk` comme table à quatre colonnes (`account_id`, `score`, `computed_at`, `reasons`), avec un paramètre obligatoire `account_id` et un paramètre facultatif, `as_of`. Une fois qu'un intendant a enregistré la table, lisez-la en SQL :

```sql
SELECT score, reasons
FROM churn_risk
WHERE _nf_account_id = 'A-1042'
```

La même fonction en TypeScript se lit ainsi :

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
