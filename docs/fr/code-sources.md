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
