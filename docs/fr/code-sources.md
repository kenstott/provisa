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

Le type de ligne donne les colonnes. Texte, nombres, booléens, dates et horodatages correspondent à leurs types de colonne, les listes restent des listes, et les objets imbriqués deviennent des types nommés. En TypeScript, un type de ligne est une interface, un alias de type ou une classe. En Python, c'est une dataclass, un `TypedDict`, un `NamedTuple`, un modèle Pydantic ou toute classe à champs annotés. Les lignes d'un modèle Pydantic sont envoyées telles que `model_dump(by_alias=True)` les donne, de sorte qu'un champ doté d'un alias devient une colonne nommée d'après cet alias. La documentation propre d'un champ devient la description de sa colonne lorsque vous enregistrez la table : un commentaire de documentation sur une propriété TypeScript, ou un `Field(description=...)` Pydantic. Quand le type d'une ligne est lâche (`any`, `unknown` ou un `Record` en TypeScript, `Any` ou un `dict` en Python), Provisa appelle la fonction une fois, sans arguments, et propose des colonnes d'après les lignes renvoyées. Si la fonction a un paramètre obligatoire, elle ne peut pas être appelée ainsi, et vous typez les colonnes vous-même en enregistrant la table.

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

## Secrets {: #secrets }

Un code qui appelle une API payante a besoin de sa clé, et la clé a sa place dans le coffre de votre organisation, pas dans le code. Sous **Secrets** dans le formulaire de la source, choisissez les secrets de l'organisation que le code peut lire. Dans un fichier de configuration, listez-les dans le mapping de la source :

```yaml
mapping:
  secrets: [openrouter]
```

Un nom seul désigne le secret de l'organisation portant ce nom. Pour lire plutôt une variable d'environnement du déploiement, donnez la référence : `secrets: {openrouter: "${env:OPENROUTER_API_KEY}"}`. La liste est une référence au coffre, jamais une copie d'une valeur : une configuration qui nomme un secret que personne n'a encore enregistré se charge quand même.

Votre code reçoit une variable d'environnement, `PROVISA_SERVICES`, et lit un secret depuis `$PROVISA_SERVICES/secrets/<name>` quand il en a besoin. En Python :

```python
import os, urllib.request
key = urllib.request.urlopen(f"{os.environ['PROVISA_SERVICES']}/secrets/openrouter").read().decode()
```

En TypeScript :

```typescript
const key = await (await fetch(`${process.env.PROVISA_SERVICES}/secrets/openrouter`)).text();
```

La requête passe par le même proxy de sortie que le reste du trafic de votre code, et le proxy y répond lui-même. Rien d'autre de Provisa n'est joignable par ce chemin. L'adresse porte un jeton que seul le worker de cette source détient ; il ne lit que les noms que la source liste, et il cesse de fonctionner quand le worker s'arrête. Chaque lecture résout le secret dans le coffre à ce moment-là : un secret renouvelé prend effet à la lecture suivante.

Un nom que la source ne liste pas est refusé, de même qu'un secret impossible à résoudre. Le refus nomme le secret, jamais sa valeur. Chaque lecture est consignée dans la piste d'audit de l'organisation avec la source, le nom et l'heure, et aucune valeur n'est jamais écrite dans un journal. L'hôte de l'API doit tout de même figurer dans la liste des sorties autorisées.

## Appeler un modèle {: #calling-a-model }

Votre code peut appeler un modèle de langage via Provisa au lieu de détenir une clé de fournisseur. `$PROVISA_SERVICES/llm/v1` est un endpoint compatible OpenAI : n'importe quel SDK OpenAI fonctionne si vous y pointez son URL de base. L'adresse sert d'identifiant, donc la clé que demande le SDK peut être n'importe quel texte. En Python :

```python
import os
from openai import OpenAI

client = OpenAI(base_url=f"{os.environ['PROVISA_SERVICES']}/llm/v1", api_key="unused")
reply = client.chat.completions.create(
    model="provisa:sql_generation",
    messages=[{"role": "user", "content": "Summarise this account's last quarter."}],
)
```

En TypeScript :

```typescript
import OpenAI from "openai";

const client = new OpenAI({ baseURL: `${process.env.PROVISA_SERVICES}/llm/v1`, apiKey: "unused" });
const reply = await client.chat.completions.create({
  model: "provisa:sql_generation",
  messages: [{ role: "user", content: "Summarise this account's last quarter." }],
});
```

Nommez un modèle par son rôle, sous la forme `provisa:<role>` : vous obtenez le modèle que votre organisation attribue à ce rôle sous **Admin → AI Models**, et il suit ce choix quand un administrateur le modifie. Vous pouvez aussi nommer un modèle directement lorsqu'il est attribué à un rôle. Pour les embeddings, `$PROVISA_SERVICES/llm/v1/embeddings` accepte n'importe lequel des modèles vectoriels de votre organisation. Tout autre modèle est refusé par son nom. L'appel sort par les fournisseurs, endpoints et clés propres à votre organisation, et ce qu'il envoie passe d'abord par les règles de masquage de Provisa. Le streaming n'est pas proposé : envoyez `stream: false`, la valeur par défaut.

Chaque appel est enregistré pour la source, avec le modèle, ses tokens et son coût selon l'entrée du modèle dans **Polly model prices**, et écrit dans la piste d'audit de l'organisation. Pour limiter ce que dépense une source, définissez **LLM monthly cap** dans son formulaire, en USD. Dans un fichier de configuration, il va dans le mapping :

```yaml
mapping:
  llm: {cap_usd: 20}
```

Une source plafonnée ne peut appeler que des modèles qui y ont un prix. Dès que ses dépenses du mois (UTC) atteignent le plafond, ses appels sont refusés jusqu'au changement de mois. Les appels déjà en cours au moment où le plafond est atteint se terminent quand même, si bien que le mois peut finir légèrement au-dessus du plafond.

## Les services de Provisa {: #provisas-services }

Votre code peut importer un module que Provisa fournit toujours, où qu'il s'exécute : `provisa_services` en Python, et `@provisa/services` en Node.js, Bun et Deno. Rien à installer, et il ne figure dans aucun registre de paquets. Hors de Provisa, le module n'existe pas : les tests de vos fonctions le simulent.

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

| Appel | Ce qu'il fait |
|---|---|
| `secrets.get(name)` | L'un des secrets déclarés de la source, lu maintenant dans le coffre. Voir [Secrets](#secrets). |
| `settings.get(name, default)` | L'un des paramètres de la source, issu de **Settings** dans son formulaire, qu'un steward peut modifier sans changer le code. Absent et sans valeur par défaut, il lève une erreur. |
| `cache.get(key, default)`, `cache.set(key, value, ttl)`, `cache.delete(key)` | Le cache propre à la source, partagé par tous ses workers. Une valeur est tout ce que JSON peut contenir, jusqu'à 1 Mo, conservée `ttl` secondes ou jusqu'à son remplacement. |
| `vectors.upsert(items, model)`, `vectors.search(query, k, model)`, `vectors.embed(texts, model)`, `vectors.delete(ids)` | La collection vectorielle propre à la source, pour ses propres données de référence. Un élément a un `id` et un `text` ou un `vector` ; un texte est vectorisé par l'un des modèles vectoriels de votre organisation. Elle ne peut pas chercher dans vos tables gouvernées : leurs données n'atteignent le code que comme paramètres. |
| `http.get(url)`, `http.post(url, ...)` | Une requête que Provisa fait pour le code, via le proxy sortant et les autorités de certification du déploiement. L'hôte doit figurer dans la liste des sorties autorisées, et chaque requête est consignée dans la piste d'audit. |
| `llm.chat(model, messages)`, `llm.embeddings(model, input)`, `llm.base_url()` (`llm.baseUrl()` en JavaScript) | Le service LLM de Provisa. Voir [Appeler un modèle](#calling-a-model). |
| `dq.dataset(name)`, `dq.product(id)` | La qualité des données d'un jeu de données (`domain.table` ou son id de table) ou d'un produit de données, telle que le méta-domaine la montre au rôle de la requête que votre code sert. Pour un jeu de données : les tables de contrôle qui l'analysent, chacune avec sa description, son contrat, le produit auquel elle est affectée et sa dernière exécution (son résultat, son heure et ses contrôles en échec), ainsi que ses profils avec la dernière exécution de chacun. Pour un produit : ses promesses, attentes, contrôles partenaires et orphelins. |
| `log(message, level, fields)`, `metric(name, value, kind, attributes)` | Une ligne de journal, et un point de métrique nommé `code_source.<name>`, rattachés à la requête que votre code sert. |

Un appel que le service refuse lève une `ServiceError` qui nomme le service et en donne la raison.

Les services répondent au code qui s'exécute sur le serveur Provisa. Sur un hôte distant, ils ne sont pas encore joignables, et chaque appel lève une erreur.

Chaque appel à votre code est tracé comme partie de la requête qui l'a déclenché, avec la fonction, son type, le nombre d'arguments reçus (jamais leurs valeurs), les lignes et lots renvoyés, sa durée et le CPU du worker. Chaque appel de service est tracé de la même façon. Au niveau de détail de trace normal, ces informations figurent sur le span de la requête ; au niveau debug, chaque appel est un span à part. Si votre code utilise OpenTelemetry, ses spans rejoignent la même trace.

## Exécuter sur un hôte distant {: #running-on-a-remote-host }

Une source s'exécute par défaut sur le serveur Provisa ; c'est le mode dev. Quand son code a besoin d'une machine que le serveur n'est pas, comme une machine à GPU ou un hôte dans un autre réseau, construisez son image et exécutez-la là-bas. La source passe alors en mode built, et ses tables, ses droits et ses lecteurs restent les mêmes.

**Construire l'image.** Enregistrez d'abord la source en mode dev, pour que son code soit lu et épinglé. Sous **Remote host** dans son formulaire, choisissez l'architecture (Linux x86-64 ou ARM64) et la destination de l'image : l'un des registres de conteneurs de votre organisation, saisis sous **Admin → Container registries**, ou un téléchargement. Provisa assemble l'image lui-même. Elle contient le code à son hash épinglé, ses dépendances installées pour cette plateforme depuis le lockfile du code via vos registres de paquets, le runtime épinglé et le programme d'hôte distant, sur une image de base minimale. Rien dans l'image ne s'exécute pendant sa construction, et aucun daemon Docker n'intervient. Le tag de l'image porte le hash du code et la version de Provisa, et ses labels nomment le runtime, le hash du code et les exports.

**L'exécuter.** Le formulaire affiche un service compose pour l'image. Une image téléchargée se charge d'abord avec `docker load -i <file>`. Le conteneur écoute sur le port 7443 en interne et conserve son identité sur le volume. Vous choisissez le port de l'hôte avec `PROVISA_HOST_PORT` ; sans lui, le service ne démarre pas :

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

À son premier démarrage, le conteneur écrit dans son journal une ligne commençant par `pairing code:`.

**La connecter.** De retour dans le formulaire, saisissez l'hôte par lequel Provisa joint le conteneur, le port que vous avez publié et le code d'appairage. Sur un réseau Docker partagé ou dans Kubernetes, saisissez le nom du conteneur ou du Service et 7443, sans rien publier. Provisa s'appaire avec le conteneur en TLS mutuel. Le code sert une fois, puis chaque côté n'accepte plus que l'autre. Provisa vérifie ensuite que le conteneur exécute le code de la source et propose ses exports, et fait passer la source en mode built. Chaque processus Provisa garde sa propre connexion à l'hôte et se reconnecte si l'hôte ne répond plus. Tant que l'hôte ne répond pas, les lectures des tables de la source échouent en nommant l'hôte.

**Si le code change.** Un conteneur qui revient avec un autre code est refusé, en nommant le hash auquel la source est épinglée et celui que l'hôte annonce, jusqu'à ce qu'un steward examine le code et le réépingle depuis le formulaire. Pour changer le code lui-même, repassez la source en dev, enregistrez le nouveau code et reconstruisez.

Dans le conteneur, sa politique réseau est la frontière de sortie du code ; la liste des sorties autorisées du serveur s'applique au mode dev.

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
