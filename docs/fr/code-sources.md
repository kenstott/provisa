# Resolvers de code

Logique métier en Python ou Node.

Si vous avez déjà écrit un serveur GraphQL, vous connaissez ce modèle. Un resolver de code est une fonction qui répond pour un type d'objet : en GraphQL, une fonction marquée `@table` *est* le resolver du type de sa table. Provisa l'appelle avec les arguments de la requête et gouverne ce qu'elle renvoie.

Ce qui change par rapport à un resolver écrit à la main :

- **Gouverné.** La visibilité, les masques et les règles de ligne s'appliquent aux lignes que renvoie votre fonction, comme pour toute table.
- **Par lots.** Une fonction qui prend un paramètre `loader` reçoit toutes les clés d'une jointure en un seul appel, le modèle DataLoader.
- **Délégué.** Un filtre sur une colonne atteint votre fonction comme paramètre quand vous le demandez.
- **Servi partout.** La même fonction répond à GraphQL, SQL (sur HTTP, pgwire et Arrow Flight), Cypher (sur HTTP et Bolt), gRPC, REST, JSON:API et MCP.


## Un premier resolver {: #an-example }

```python
from dataclasses import dataclass


@dataclass
class Region:
    code: str
    name: str


def regions() -> list[Region]:
    """Sales regions.

    @table
    """
    return [
        Region(code="emea", name="Europe, Middle East and Africa"),
        Region(code="amer", name="Americas"),
        Region(code="apac", name="Asia Pacific"),
    ]
```

Pour le servir :

1. Ajoutez une source `python` et pointez-la vers le code : un paquet PyPI, un commit Git, un téléversement ou un dossier (voir [D'où vient le code](#where-the-code-comes-from)).
2. Enregistrez-la. Provisa installe le code, le lit et propose `regions` comme table avec les colonnes `code` et `name`.
3. Un intendant enregistre la table dans un domaine, ici `sales`, et l'accorde comme n'importe quelle autre.

Lisez-la ensuite en GraphQL :

```graphql
{
  regions {
    code
    name
  }
}
```

Ou en SQL :

```sql
SELECT code, name
FROM sales.regions
```

Node fonctionne de la même façon, avec des balises JSDoc et des types TypeScript : voir [Node](#node).

## Construire {: #build }

### Ce que devient une fonction {: #what-a-function-becomes }

Provisa lit les déclarations de votre code lui-même : docstrings et annotations de type en Python, balises JSDoc et types TypeScript en Node.

| Balise | Devient |
|---|---|
| `@table` | Une table. La fonction renvoie une liste ou un itérable, et chaque élément est une ligne. |
| `@command` | Une commande, exécutée à la demande. |
| `@stream` | Une table en direct. La fonction est un générateur, et chaque ligne est envoyée dès qu'elle est produite. |

- Une fonction sans balise est ignorée.
- Une balise que Provisa ne connaît pas est refusée, tout comme un type qu'il ne sait pas mapper. Le refus liste tous les problèmes d'un coup, pour que vous les corrigiez en une passe.

Le type de ligne donne les colonnes :

- Le texte, les nombres, les booléens, les dates et les horodatages correspondent à leurs types de colonne. Les listes restent des listes, et les objets imbriqués deviennent des types nommés.
- En Python, un type de ligne est une dataclass, un `TypedDict`, un `NamedTuple`, un modèle Pydantic ou toute classe à champs annotés. Les lignes d'un modèle Pydantic sont envoyées telles que `model_dump(by_alias=True)` les donne, si bien qu'un champ avec un alias devient une colonne nommée par son alias.
- En TypeScript, un type de ligne est une interface, un alias de type ou une classe.
- La documentation propre d'un champ devient la description de sa colonne quand vous enregistrez la table : un `Field(description=...)` Pydantic, ou un commentaire de documentation sur une propriété TypeScript.
- Un type de ligne lâche (`Any` ou un `dict` en Python, `any`, `unknown` ou un `Record` en TypeScript) n'a pas de colonnes à lire. Provisa appelle la fonction une fois sans argument et propose des colonnes à partir des lignes renvoyées. Si la fonction a un paramètre obligatoire, elle ne peut pas être appelée ainsi, et vous indiquez vous-même les types des colonnes en enregistrant la table.

### Paramètres {: #parameters }

Les paramètres d'une fonction deviennent les paramètres de sa table. Celle-ci note un compte :

```python
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

En GraphQL, le paramètre est un argument de champ :

```graphql
{
  churnRisk(account_id: "A-1042") {
    score
    reasons
  }
}
```

En SQL, c'est une condition `_nf_` :

```sql
SELECT score, reasons
FROM accounts.churn_risk
WHERE _nf_account_id = 'A-1042'
```

- Un paramètre sans valeur par défaut est obligatoire. Une lecture qui l'omet est refusée avant que votre code ne s'exécute.
- Un paramètre avec une valeur par défaut est facultatif. Omettez-le et votre valeur par défaut s'applique.
- Un intendant peut tout de même rendre obligatoire un paramètre facultatif.

### Ce qui parvient à votre fonction {: #what-reaches-your-function }

Un appel reçoit les arguments de la lecture et rien d'autre de la requête. La valeur de chaque paramètre vient d'un argument de champ, d'une condition `_nf_`, d'un filtre qui lui est délégué ou des clés d'une jointure. Votre fonction ne voit jamais qui demande : l'identité et les rôles de l'appelant restent chez Provisa, qui gouverne les lignes que votre fonction renvoie.

La délégation des filtres repose là-dessus. Un filtre de la requête devient la valeur d'un paramètre, si bien que votre fonction peut ne récupérer que les lignes demandées.

### Délégation des filtres {: #pushdown }

Un filtre sur l'une des colonnes renvoyées peut atteindre votre fonction comme argument, pour qu'elle ne récupère que les lignes demandées. Déclarez quel paramètre le reçoit de l'une de trois façons :

- **Par nom.** Un paramètre nommé exactement comme une colonne renvoyée reçoit une égalité sur cette colonne. Un paramètre nommé `<op>_<column>` reçoit cet opérateur, comme `gte_score` ou `in_region`.
- **Par balise.** Une ligne `@<op>_<column> <parameter>` (ou `@<column> <parameter>`) dans la docstring ou le JSDoc, ou un `/* @<op>_<column> */` en ligne avant un paramètre TypeScript.
- **Par `Annotated`.** En Python, la chaîne `"<op>_<column>"` (ou `"<column>"`) sur l'annotation de type du paramètre.

Les opérateurs :

| `op` | Reçoit |
|---|---|
| `ne`, `gt`, `gte`, `lt`, `lte` | Une valeur du type de la colonne |
| `in` | Une liste du type de la colonne |
| `like` | Du texte, sur une colonne de texte |

- Un intervalle sur une colonne remplit ses deux paramètres : `score BETWEEN 0.5 AND 0.9` donne `gte_score` et `lte_score`.
- Les noms de colonne sont comparés exactement comme vous les avez déclarés dans le type de ligne. Un nom qui ne correspond qu'une fois que Provisa renomme la colonne pour un lecteur n'est pas une correspondance.
- Une déclaration que les types de votre fonction ne peuvent pas accepter est refusée.
- Le filtre est quand même appliqué aux lignes que renvoie votre fonction, si bien qu'une fonction qui ignore ou assouplit une valeur reçue ne peut pas renvoyer de mauvaises lignes.

### Clés par lots : le paramètre `loader` {: #batched-keys }

Une jointure peut atteindre une table de code, les lignes d'une autre table fournissant ses paramètres : par exemple des clients joints à leur score d'attrition. Par défaut, Provisa appelle la fonction une fois pour chaque ensemble distinct d'arguments que donnent les lignes jointes, et envoie tous ces appels au worker en une seule requête.

Une fonction qui prend un paramètre nommé `loader` est appelée à la place une fois par lot, le modèle que le DataLoader de GraphQL a popularisé.

- **Ce qu'elle reçoit.** `loader` est un hash : un dict (un objet en TypeScript) d'un hash stable de chaque clé vers la clé elle-même. Une clé est un objet JSON contenant les valeurs des lignes jointes pour les autres paramètres de la fonction.
- **Dédupliqué.** Chaque combinaison distincte de valeurs apparaît une fois.
- **Clairsemé.** Un champ sans valeur est omis de la clé, pour que la valeur par défaut de la fonction s'applique, comme lors d'un appel unique.
- **Ce qu'elle renvoie.** Un hash de chaque clé reçue vers les lignes de cette clé : des lignes ordinaires de la table, aucune ou plusieurs. Provisa donne les lignes de chaque clé à chaque ligne jointe qui partageait cette clé.
- **Associé par hash.** La fonction remplit avec des valeurs par défaut les champs manquants d'une clé clairsemée, si bien que les valeurs qu'elle voit diffèrent de celles de la ligne jointe. Provisa calcule le hash, et la fonction ne fait que renvoyer les clés reçues.
- **Typée comme d'habitude.** La fonction déclare toujours son seul type de ligne, une liste de lignes.
- **Valeurs par défaut requises.** Ses paramètres à côté de `loader` ont besoin de valeurs par défaut, car un appel avec loader ne passe que `loader`.
- **Taille de lot.** Limitez-la, comme le `maxBatchSize` de DataLoader, avec `loader N` à l'un de trois endroits : une ligne `@loader 100` à côté de `@table`, un commentaire `/* @loader 100 */` avant le paramètre `loader` en TypeScript, ou une chaîne `Annotated` en Python, `loader: Annotated[dict[str, CustomerKey] | None, "loader 100"] = None`. Provisa répartit alors les clés en lots de cette taille et fusionne les réponses par hash. Sans taille, un lot contient toutes les clés.
- **Refusé.** Une taille sur une fonction sans paramètre `loader`, une taille qui n'est pas un entier positif, et deux tailles sur une même fonction.

Utilisez `loader` quand un appel sur de nombreuses clés vaut mieux que de nombreux appels sur une seule :

- Évaluer un modèle sur un lot entier en une passe vectorisée.
- Regrouper des requêtes vers une API ou un modèle de langage qui accepte de nombreuses entrées à la fois.
- Préparer une fois par lot, par exemple charger un modèle ou ouvrir une connexion.

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

Jointe à une table de clients, elle est appelée avec un `loader` tel que `{"9f2c...": {"customer_id": "637", "plan": "pro", "region": "EU"}, "41ab...": {"customer_id": "638"}, ...}`. La deuxième clé ne porte que `customer_id`, donc son plan et sa région prennent les valeurs par défaut. La fonction renvoie `{"9f2c...": [...], "41ab...": [...], ...}`.

### D'où vient le code {: #where-the-code-comes-from }

- **Un paquet.** Un paquet npm pour Node ou un paquet PyPI pour Python, dans une version exacte.
- **Un dépôt Git.** Un commit complet de 40 caractères, avec un sous-répertoire si le code n'est pas à la racine.
- **Un téléversement.** Un `.zip`, `.tgz` ou `.tar.gz` du code.
- **Un dossier sur votre machine.** **Choisir un dossier** compresse le dossier dans votre navigateur, en laissant de côté `node_modules`, `.venv`, `__pycache__` et `.git`, et le téléverse comme archive de la source. Dans un navigateur qui peut conserver le dossier (Chrome, Edge), **Enregistrer** le compresse et l'envoie à nouveau ; un autre navigateur vous redemande le dossier.
- **Un répertoire sur le serveur.** Sur une installation à une seule organisation, un répertoire sous ceux que l'opérateur autorise. Il est lu sur place, donc une modification est lue par la requête suivante. Un serveur à plusieurs organisations le refuse.

Toute origine sauf le répertoire du serveur est figée, si bien que la source exécute exactement ce qui a été enregistré.

- **Module d'entrée.** Quand le code Python a plus d'un module de premier niveau, nommez celui par lequel entrer.
- **Ouvrir dans VS Code.** Un répertoire du serveur s'ouvre dans votre VS Code installé ; une origine Git s'ouvre à son commit sur vscode.dev. Un paquet ou un téléversement n'a pas ce lien.

### Installer les dépendances {: #installing-dependencies }

Du code avec des dépendances a besoin d'un lockfile, et chaque installation le suit exactement. Sans lockfile, un paquet qui déclare des dépendances est refusé, et le refus vous demande d'en ajouter un.

| Environnement | Lockfile |
|---|---|
| Node.js | `package-lock.json` ou `npm-shrinkwrap.json` |
| Bun | `bun.lock` ou `bun.lockb` |
| Deno | `deno.lock` |
| Python | `uv.lock` (avec son `pyproject.toml`), `pylock.toml`, ou un `requirements.txt` avec `--hash` sur chaque ligne |

- **Fourni ou donné.** Du code qui apporte son propre lockfile s'installe à partir de celui-ci : un dépôt Git, un téléversement ou un dossier qui en contient un, ou un paquet npm dont l'auteur a publié `npm-shrinkwrap.json` (npm ne publie jamais `package-lock.json`). La plupart des paquets npm, et les paquets Python de PyPI, n'en apportent aucun, alors donnez-en un à côté.
- **Pourquoi c'est important.** Sans lockfile, deux installations du même code peuvent différer, et un test qui passe aujourd'hui peut échouer demain sur une nouvelle version de dépendance.
- **Une image construite est déjà figée.** Dès que vous [construisez l'image de la source](#running-on-a-remote-host), ses dépendances sont à l'intérieur. Un lockfile compte pour le mode dev et pour chaque nouvelle construction.
- **Aucun script d'installation.** npm installe avec `--ignore-scripts`, et Python n'installe que des wheels, sans jamais construire depuis une distribution source.
- **Vos registres.** Les paquets proviennent des propres registres npm, index PyPI et hôtes Git de votre organisation, saisis une fois sous **Admin → Package registries**. Le registre npm public et PyPI ne demandent aucune entrée. Voir [Package Registries](package-registries.md).
- **Mis en cache.** Une installation est mise en cache selon le hash du contenu du code, si bien que le même code s'installe une seule fois.

### Node {: #node }

- Le code Node s'exécute sur Node.js, Bun ou Deno, au choix.
- Les balises vont dans le JSDoc, et les types viennent de TypeScript.

Le premier resolver en TypeScript :

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

Le `churnScore` par lots en TypeScript :

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

## Déploiement {: #deployment }

### Mode dev et workers {: #workers }

Par défaut, une source s'exécute sur le serveur Provisa ; c'est le mode dev.

- Votre code s'exécute dans un processus worker qui lui est propre, un par source et par version de son code dans chaque processus Provisa, démarré à son premier appel.
- Un worker qui plante fait échouer les appels en cours, en nommant la source et la fonction. L'appel suivant en démarre un nouveau.
- Un appel est annulé quand le délai de sa requête expire : on demande au worker de l'arrêter, et la requête en attente est libérée aussitôt.

### Lire une table {: #reading-a-table }

La façon dont une lecture atteint votre fonction dépend de ce qu'elle lit d'autre :

- **Une lecture d'une seule table de code** va directement à votre fonction. Rien n'est déposé. Provisa l'appelle avec les arguments de la lecture et ses filtres délégués, et applique le reste de l'instruction aux lignes qu'elle renvoie.
- **Une lecture qui joint d'autres tables** revient au moteur de fédération. La table d'une source en mode dev est déposée pour elle : les lignes sont conservées sous ces arguments pendant le TTL de cache de la table.
- **La table d'une source construite** dans une telle jointure est lue en direct depuis son hôte distant, que le moteur attache comme catalogue Postgres. Elle est déposée à la place quand sa source est répliquée ou plancher, quand la table est conservée pour de bon, quand elle déclare un TTL de dépôt supérieur à 0, ou sur un moteur qui ne peut pas attacher d'hôte.

Dans tous les cas, la visibilité, les masques et les règles de ligne s'appliquent aux lignes, comme pour toute table. Vous passez les arguments comme pour tout paramètre de table : conditions `_nf_` en SQL, arguments de champ en GraphQL.

### Exécuter sur un hôte distant {: #running-on-a-remote-host }

Quand votre code a besoin d'une machine autre que le serveur, comme une machine à GPU ou un hôte dans un autre réseau, construisez son image et exécutez-la là-bas. La source passe alors en mode construit, et ses tables, ses droits et ses lecteurs restent les mêmes.

**Construire l'image.**

- Enregistrez d'abord la source en mode dev, pour que son code soit lu et figé.
- Sous **Remote host** dans son formulaire, choisissez l'architecture (Linux x86-64 ou ARM64) et la destination de l'image : l'un des registres de conteneurs de votre organisation (**Admin → Container registries**), ou un téléchargement.
- Provisa assemble l'image lui-même, sur une base minimale, sans démon Docker : le code à son hash figé, ses dépendances installées pour cette plateforme depuis son lockfile via vos registres de paquets, l'environnement figé et le programme d'hôte distant. Rien dans l'image ne s'exécute pendant sa construction.
- Le tag de l'image porte le hash du code et la version de Provisa, et ses labels nomment l'environnement, le hash du code et les exports.

**L'exécuter.** Le formulaire affiche un service compose pour l'image. Chargez d'abord une image téléchargée avec `docker load -i <file>`. Le conteneur écoute sur 7443 et conserve son identité sur son volume. Vous choisissez le port de l'hôte avec `PROVISA_HOST_PORT` ; le service ne démarre pas sans lui.

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

**Le connecter.**

- Dans le formulaire, saisissez l'hôte par lequel Provisa atteint le conteneur, le port que vous avez publié et le code d'appairage. Sur un réseau Docker partagé ou dans Kubernetes, saisissez le nom du conteneur ou du Service et 7443, sans rien publier.
- Provisa s'appaire avec le conteneur en TLS mutuel. Le code sert une fois ; ensuite chaque côté n'accepte que l'autre.
- Provisa vérifie que le conteneur exécute le code de la source et propose ses exports, puis passe la source en mode construit.
- Chaque processus Provisa garde sa propre connexion à l'hôte et se reconnecte si l'hôte cesse de répondre. Tant qu'il ne répond pas, les lectures des tables de la source échouent et nomment l'hôte.

**Si le code change.**

- Un conteneur qui revient avec un autre code est refusé jusqu'à ce qu'un intendant examine le code et le refige depuis le formulaire.
- Pour changer le code lui-même, repassez la source en mode dev, enregistrez le nouveau code et reconstruisez.

### Les services de Provisa {: #provisas-services }

Votre code peut importer un module que Provisa fournit toujours sur le serveur : `provisa_services` en Python, `@provisa/services` en Node.js, Bun et Deno. Rien à installer, et il ne figure dans aucun registre de paquets. Hors de Provisa il n'existe pas, donc vos tests le simulent.

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
| `secrets.get(name)` | L'un des secrets déclarés de la source, lu maintenant dans le coffre-fort. Voir [Secrets](#secrets). |
| `settings.get(name, default)`, `settings.all()` | Les réglages de la source, issus de **Settings** dans son formulaire, qu'un intendant peut changer sans changer le code. Un réglage absent sans valeur par défaut lève une erreur. |
| `cache.get(key, default)`, `cache.set(key, value, ttl)`, `cache.delete(key)` | Le cache propre de la source, partagé par tous ses workers. Une valeur est tout ce que JSON peut contenir, jusqu'à 1 Mo, conservée `ttl` secondes ou jusqu'à son remplacement. |
| `vectors.upsert(items, model)`, `vectors.search(query, k, model)`, `vectors.embed(texts, model)`, `vectors.delete(ids)` | La collection de vecteurs propre de la source, pour ses propres données de référence. Un élément a un `id` et un `text` ou un `vector` ; un texte est vectorisé par l'un des modèles vectoriels de votre organisation. Elle ne peut pas chercher dans vos tables gouvernées, dont les données n'atteignent le code que comme paramètres. |
| `http.get(url)`, `http.post(url, ...)` | Une requête que Provisa fait pour le code, via le proxy sortant et les autorités de certification du déploiement. L'hôte doit figurer sur la liste d'autorisation en sortie, et chaque requête est consignée dans la piste d'audit. |
| `llm.chat(model, messages)`, `llm.embeddings(model, input)`, `llm.base_url()` (`llm.baseUrl()` en JavaScript) | Le service LLM de Provisa. Voir [Appeler un modèle](#calling-a-model). |
| `log(message, level, fields)`, `metric(name, value, kind, attributes)` | Une ligne de journal, et un point de métrique nommé `code_source.<name>`, rattachés à la requête que sert votre code. |

- Un appel que le service refuse lève une `ServiceError` qui nomme le service et dit pourquoi.
- Les services répondent au code qui s'exécute sur le serveur Provisa. Sur un hôte distant, tout appel lève une erreur.

#### Secrets {: #secrets }

La clé d'une API payante a sa place dans le coffre-fort de votre organisation, pas dans le code.

- Sous **Secrets** dans le formulaire de la source, choisissez les secrets de l'organisation que le code peut lire. Dans un fichier de configuration, listez-les dans le mapping de la source :

    ```yaml
    mapping:
      secrets: [openrouter]
    ```

- Un nom seul désigne le secret de l'organisation de ce nom. Pour lire plutôt une variable d'environnement du déploiement, donnez la référence : `secrets: {openrouter: "${env:OPENROUTER_API_KEY}"}`.
- La liste renvoie au coffre-fort et ne copie jamais de valeur, si bien qu'une configuration qui nomme un secret que personne n'a encore stocké se charge quand même.

Votre code lit un secret avec `secrets.get(name)`, ou directement depuis `$PROVISA_SERVICES/secrets/<name>` :

```python
import os, urllib.request
key = urllib.request.urlopen(f"{os.environ['PROVISA_SERVICES']}/secrets/openrouter").read().decode()
```

```typescript
const key = await (await fetch(`${process.env.PROVISA_SERVICES}/secrets/openrouter`)).text();
```

- `PROVISA_SERVICES` est la seule variable d'environnement que reçoit votre code. La requête passe par le même proxy de sortie que le reste de son trafic, et le proxy y répond lui-même ; rien d'autre de Provisa n'est joignable ainsi.
- L'adresse porte un jeton que seul le worker de cette source détient. Elle ne lit que les noms que la source liste, et cesse de fonctionner quand le worker s'arrête.
- Chaque lecture résout le secret dans le coffre-fort à cet instant, si bien qu'un secret renouvelé prend effet à la lecture suivante.
- Un nom que la source ne liste pas est refusé, tout comme un secret qui ne peut pas être résolu. Le refus nomme le secret, jamais sa valeur.
- Chaque lecture est consignée dans la piste d'audit de l'organisation avec la source, le nom et l'heure. Aucune valeur n'est jamais écrite dans un journal.
- L'hôte de l'API doit tout de même figurer sur la liste d'autorisation en sortie.

#### Appeler un modèle {: #calling-a-model }

Votre code peut appeler un modèle de langage via Provisa au lieu de détenir une clé de fournisseur. `$PROVISA_SERVICES/llm/v1` est un endpoint compatible OpenAI, donc tout SDK OpenAI fonctionne avec son URL de base pointée là. L'adresse est l'identifiant, donc la clé que demande le SDK peut être n'importe quel texte.

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

- **Par rôle.** Nommez un modèle `provisa:<role>` et vous obtenez celui que votre organisation attribue à ce rôle sous **Admin → AI Models** ; il suit ce choix quand un administrateur le modifie. Vous pouvez aussi nommer un modèle directement quand un rôle lui est attribué.
- **Embeddings.** `$PROVISA_SERVICES/llm/v1/embeddings` accepte n'importe lequel des modèles vectoriels de votre organisation.
- Tout autre modèle est refusé.
- L'appel sort par les propres fournisseurs, endpoints et clés de votre organisation, et ce qu'il envoie passe d'abord par les règles de masquage de Provisa.
- Le streaming n'est pas proposé. Envoyez `stream: false`, la valeur par défaut.

## Garde-fous {: #safeguards }

### Le sandbox {: #the-sandbox }

- Votre code s'exécute dans son propre processus worker, dans un sandbox. Il peut lire son propre code installé et rien d'autre sur le disque.
- Il n'atteint que les hôtes de la liste d'autorisation en sortie du déploiement, via un proxy que Provisa exécute, et jamais les ports propres de Provisa.
- Un hôte qui ne peut pas fournir le sandbox refuse de démarrer le worker et nomme ce qui manque ; le code ne s'exécute jamais sans confinement. Sous Windows, les sources de code sont refusées.
- Sur un hôte distant, la politique réseau du conteneur est la limite de sortie du code. La liste d'autorisation en sortie du serveur s'applique au mode dev.

### Refus {: #refusals }

Chacun de ces cas est refusé, avant l'exécution de votre code quand c'est possible :

- Une balise inconnue ou un type que Provisa ne sait pas mapper, tous les problèmes d'une fonction d'un coup.
- Une lecture qui omet un paramètre obligatoire.
- Une déclaration de délégation ou de `loader` que les types de la fonction ne peuvent pas accepter.
- Un paquet avec des dépendances et sans lockfile.
- Un répertoire du serveur hors de ceux autorisés, ou sur un serveur à plusieurs organisations.
- Un hôte distant qui exécute un autre code que celui auquel la source est figée.
- Un secret que la source ne liste pas, un modèle non autorisé, un appel de service auquel le service ne répond pas.

### Mesure et plafonds {: #metering }

- Chaque appel de modèle est consigné pour la source, avec le modèle, ses tokens et son coût selon l'entrée du modèle dans **Polly model prices**, et écrit dans la piste d'audit de l'organisation.
- Pour limiter ce que dépense une source, définissez **LLM monthly cap** dans son formulaire, en USD. Dans un fichier de configuration, il va dans le mapping :

    ```yaml
    mapping:
      llm: {cap_usd: 20}
    ```

- Une source plafonnée ne peut appeler que des modèles qui y ont un prix.
- Dès que ses dépenses du mois (UTC) atteignent le plafond, ses appels sont refusés jusqu'au changement de mois. Les appels déjà en cours quand le plafond est atteint se terminent quand même, si bien que le mois peut finir légèrement au-dessus du plafond.

### Gouvernance des résultats {: #governance }

- Les lignes de votre fonction sont vérifiées par rapport aux colonnes de la table.
- La visibilité, les masques et les règles de ligne s'appliquent ensuite pour chaque lecteur, comme pour toute table.
- Votre code ne voit les données d'une table gouvernée que sous forme des paramètres qu'une lecture lui donne.

### Traçage {: #tracing }

- Chaque appel à votre code est tracé comme partie de la requête qui l'a déclenché : la fonction, son type, le nombre d'arguments reçus (jamais leurs valeurs), les lignes et les lots renvoyés, sa durée et le CPU du worker.
- Chaque appel de service est tracé de la même façon.
- Au niveau de trace normal, ces informations sont sur le span de la requête ; au niveau débogage, chaque appel est un span à part.
- Si votre code utilise OpenTelemetry, ses spans rejoignent la même trace.
