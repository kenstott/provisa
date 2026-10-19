# Importer depuis Hasura {: #importing-from-hasura }

Provisa peut convertir des métadonnées Hasura existantes en un `config.yaml` Provisa, en préservant les tables suivies, les relations, les autorisations et les schémas distants.

## Import interactif (Admin → Import Hasura Config) {: #interactive-import-admin-import-hasura-config }

La surface d'administration exécute les mêmes convertisseurs : un import ne nécessite ni accès shell ni allers-retours de fichier de configuration. Requiert la capacité `org_settings` ; l'import atterrit dans l'organisation dans laquelle agit la session.

1. **Téléverser.** Choisissez un répertoire de métadonnées Hasura v2 zippé, un projet DDN zippé, un export de métadonnées consolidé (`.yaml`/`.json`, y compris l'enveloppe `{resource_version, metadata}` que retourne l'API de métadonnées), ou un unique fichier `.hml`. Laissez le format sur *Detect automatically* sauf si le téléversement est ambigu.
2. **Associer les domaines** (optionnel). Chaque paire associe un schéma v2 ou un sous-graphe DDN à un domaine Provisa ; tout ce qui n'est pas associé conserve son nom d'origine.
3. **Convertir et prévisualiser.** Le serveur convertit et renvoie les décomptes, le rapport de conversion (voir [Le rapport](#the-report)) et la configuration générée. Rien n'est écrit à cette étape.
4. **Réviser et modifier.** La configuration est modifiable sur place : détails de connexion, noms de domaines, noms de rôles. Ce que vous appliquez est ce qui est affiché.
5. **Appliquer.** *Appliquer à cette organisation* charge la configuration et reconstruit les schémas de l'organisation. Un import fusionne toujours avec ce que possède déjà l'organisation.

Endpoints : `POST /admin/import/hasura/preview` et `POST /admin/import/hasura/apply`.

---

## Hasura v2

### Exporter les métadonnées {: #export-metadata }

Les deux dispositions d'export sont converties.

**Le répertoire de métadonnées de la CLI.** `hasura metadata export` écrit un répertoire. `databases/databases.yaml` liste chaque source avec son type et sa connexion, et renvoie à ses tables et à ses fonctions par des index `!include` :

```bash
hasura metadata export
```

```yaml
# metadata/databases/databases.yaml
- name: default
  kind: postgres
  configuration:
    connection_info:
      database_url:
        from_env: HASURA_GRAPHQL_DATABASE_URL
  tables: "!include default/tables/tables.yaml"
  functions: "!include default/functions/functions.yaml"
```

La disposition à plat (`tables.yaml` et `actions.yaml` à la racine) est convertie aussi.

**Le document unique de l'API de métadonnées.** L'API de métadonnées renvoie tout l'export sous forme d'un seul objet, avec la `configuration` de chaque source que la disposition en répertoire n'a nulle part où mettre :

```bash
curl -X POST http://localhost:8080/v1/metadata \
  -H "X-Hasura-Admin-Secret: <secret>" \
  -d '{"type":"export_metadata","args":{}}' \
  > metadata.json
```

L'objet nu comme l'enveloppe `{resource_version, metadata}` sont acceptés. L'import d'administration prend cette forme (voir [Import interactif](#interactive-import-admin-import-hasura-config)) ; le convertisseur en ligne de commande ne lit que des répertoires.

### Convertir {: #convert }

Le convertisseur en ligne de commande lit un **répertoire** de métadonnées Hasura (la disposition produite par `hasura metadata export`, ou la disposition à plat `tables.yaml` / `actions.yaml`) et écrit une configuration Provisa :

```bash
python -m provisa.hasura_v2 ./metadata -o config.yaml
```

Omettez `-o` pour écrire la configuration sur stdout. Lorsqu'il reste quelque chose à refaire à la main, le rapport s'affiche sur stderr.

Options :

| Option | Rôle |
| ------ | --------- |
| `-o`, `--output` | Chemin du YAML de sortie (par défaut : stdout) |
| `--source-overrides` | Fichier YAML de surcharges de connexion par source (hôte, port, identifiants), indexé par nom de source |
| `--domain-map` | Correspondances schéma-vers-domaine sous forme de paires `SCHEMA=DOMAIN` |
| `--auth-env-file` | Fichier `.env` de configuration d'authentification ; convertit JWT/JWK, le secret d'administration et la table de claims |
| `--dry-run` | Analyser et valider sans écrire la sortie |

### Le rapport {: #the-report }

Chaque conversion range ce qu'elle trouve en trois parties. Lisez-les dans cet ordre.

1. **Repris tel quel.** L'objet a une contrepartie directe dans Provisa et se convertit sans changer de sens : sources, tables, relations approuvées, autorisations de select, cron triggers.
2. **Transposé vers une meilleure fonctionnalité de Provisa.** Provisa a une fonctionnalité qui fait mieux le travail que celle de Hasura, donc l'objet devient cette fonctionnalité. Chaque élément dit pourquoi. Les event triggers, les actions, les remote schemas, les noms de champs racines supplémentaires et les rôles hérités de plusieurs rôles sont ici.
3. **À refaire à la main.** L'objet n'a pas d'équivalent dans Provisa, ou il manque à l'export ce dont la conversion a besoin. Chaque élément dit quoi faire. Ce sont les avertissements.

L'aperçu renvoie les trois parties sous forme de listes. Le convertisseur en ligne de commande les affiche sur stderr sous les titres `Carried as`, `Mapped to` et `Redo by hand`, chacun avec un décompte. Une conversion sans rien dans la troisième partie est terminée.

### Ce qui est converti {: #what-gets-converted }

| Concept Hasura | Partie | Résultat dans Provisa |
| --------------- | ---- | -------------- |
| Source (`kind`, `connection_info`) | repris | `sources[]`. Une connexion écrite dans l'export est convertie en hôte, port, base de données et identifiants. Une connexion lue depuis une variable d'environnement n'est pas devinée (voir [Connexions et domaines](#connections-and-domains-on-the-import-tab)). |
| Table suivie | repris | Une table enregistrée. Son nom personnalisé (ou son champ racine `select`) devient l'alias de la table. |
| Noms de colonnes personnalisés (`column_config`, ou l'ancien `custom_column_names`) | repris | `alias` de la colonne. |
| Noms de champs racines au-delà de `select` (`select_by_pk`, `select_aggregate`, `insert`, ...) | transposé | Signalés par leur nom. Provisa sert une table comme un seul champ : une lecture par clé est un filtre sur lui, un agrégat son champ `_aggregate`, une écriture sa mutation. Vérifiez que les requêtes de l'app ont cette forme. |
| Relationship objet et tableau | repris | Une relation approuvée (`many-to-one` pour l'objet, `one-to-many` pour le tableau) dont le champ GraphQL est le nom de la relationship Hasura. Les cas qui diffèrent sont détaillés ci-dessous. |
| Autorisation de select | repris | Autorisations de colonne (`visible_to`) et une règle de lignes. Un terme à variable de session (`X-Hasura-User-Id`) devient `current_setting('provisa.hasura_user_id')`, que la requête lie au moment de l'interrogation aux claims de l'identité ou aux attributs du membre. Une variable de session Hasura garde son propre nom ; `provisa.user_id` reste l'identité connectée. |
| Règle de lignes à travers une relationship | repris | Une sous-requête `EXISTS` sur la table liée. |
| Autorisation d'insert, update et delete | repris | Autorisations d'écriture sur la table : une par rôle et par opération. `columns` deviennent les colonnes de l'autorisation, `filter` son `rows`, `check` son `check` et `set` ses presets. |
| Rôle hérité d'un rôle | repris | Ce rôle comme `parent_role_id`. |
| Rôle hérité de plusieurs rôles | transposé | Un rôle détenant l'union de leurs autorisations, écrite explicitement : un rôle Provisa a un seul parent. |
| Computed field | à refaire | Une colonne d'une vue sur la table. |
| Fonction suivie | repris | Une commande ; voir ci-dessous. Sinon, à refaire. |
| Action | transposé | Une commande qui appelle le handler de l'action. |
| Cron trigger | repris | Un trigger planifié. |
| Event trigger | transposé | Un trigger sur les modifications de la table. |
| Remote schema | transposé | Une source `graphql_remote` dont les champs racines atterrissent comme tables gouvernées. |

Cas des relationships :

- **Collision.** Lorsque la table a déjà un champ du nom de la relationship, la relationship prend le nom que lui donne Provisa, et le rapport avertit. Renommez-la dans la page Relationships si l'app a besoin d'un autre nom.
- **Le même join deux fois.** Une seconde relationship qui déclare un join déjà déclaré fusionne avec la première. Le rapport l'indique parmi les éléments repris.
- **Plusieurs paires de colonnes.** Une relation Provisa joint sur une paire de colonnes. Une relationship qui joint sur plusieurs est signalée par un avertissement et n'est pas créée, mais les règles de lignes qui la traversent sont reprises.
- **Colonne de clé étrangère seule.** Une relationship objet déclarée par `foreign_key_constraint_on: artist_id` ne nomme aucune cible dans l'export. Le convertisseur la résout par la relationship tableau inverse et avertit lorsqu'il n'y en a pas.

Détails des autorisations d'écriture :

- Un `filter` ou `check` vide signifie toutes les lignes, écrit `TRUE`.
- Une valeur de `set` qui est une variable de session (`X-Hasura-User-Id`) devient un preset `session` de la variable de session correspondante (`X-Hasura-User-Id` devient `hasura_user_id`). Toute autre valeur devient un preset `literal`. Une colonne à preset ne figure pas aussi dans les colonnes de l'autorisation.
- L'admin implicite de Hasura détient toutes les autorisations. Il devient le rôle `org_admin`, qui détient une autorisation d'insert, d'update et de delete sur toutes les colonnes et toutes les lignes, et qui est nommé sur chaque colonne importée.
- Chaque rôle ayant une autorisation d'insert, d'update ou de delete reçoit aussi le droit `write`.

Détails des commandes :

- **Fonction suivie.** L'export nomme la fonction et les rôles qui l'appellent, pas ses arguments ni ce qu'elle renvoie. L'aperçu lit les deux dans le catalogue de la base de données et enregistre une commande dont il a trouvé la signature. Lorsque la source n'est pas atteinte, n'est pas PostgreSQL, ou que la fonction est absente, surchargée ou renvoie des lignes d'une table que l'import n'enregistre pas, la fonction passe à refaire. Le convertisseur en ligne de commande n'atteint jamais de base de données, donc toute fonction suivie qu'il trouve passe à refaire.
- **Action.** La commande appelle le handler de l'action par un `POST`. Un `{{VAR}}` dans l'URL du handler s'écrit `${env:VAR}` et se résout quand la commande est appelée. La commande est visible pour les rôles que nomment les autorisations de l'action, ou pour `org_admin` seul lorsqu'elles n'en nomment aucun. Une action asynchrone est signalée par un avertissement : sa commande attend la réponse du handler. Un type de retour dont l'export ne porte pas les champs est signalé par un avertissement.
- **Event trigger.** Le trigger se déclenche à chaque modification de la table, une fois par lot de modifications, et envoie la table et les lignes modifiées. Il ne se déclenche pas par ligne avec anciennes et nouvelles valeurs. Un trigger Hasura sur moins des trois opérations est signalé par un avertissement, car le trigger Provisa se déclenche sur toutes. Les nouvelles tentatives ne sont pas reprises : un trigger Hasura avec `num_retries` est signalé, car un trigger Provisa appelle une fois. Réessayez dans le handler si un appel manqué compte.
- **Cron trigger.** L'expression cron, l'URL, le payload et l'indicateur enabled sont repris. Les nouvelles tentatives ne sont pas reprises et sont signalées de la même façon.

Un document consolidé contient aussi des sections pour lesquelles le convertisseur n'a pas de cible : `query_collections`, `allowlist`, `rest_endpoints`, `network` et `opentelemetry`. Lorsqu'une est présente, le rapport la liste parmi ce qu'il faut refaire. Le lecteur de répertoire n'ouvre jamais leurs fichiers.

### Connexions et domaines dans l'onglet d'import {: #connections-and-domains-on-the-import-tab }

Une connexion que l'export écrit est convertie telle qu'écrite. Lorsque l'export lit une URL de base de données depuis une variable d'environnement (`database_url: {from_env: HASURA_GRAPHQL_DATABASE_URL}`), la valeur de la variable n'est pas dans l'export, donc la conversion n'en devine aucune. La source est enregistrée avec des champs de connexion vides, et le rapport demande de la refaire : saisissez l'hôte, le port, la base de données, l'utilisateur et le mot de passe de la source dans les paramètres de sources de l'import, puis convertissez à nouveau. Seuls les champs que vous modifiez voyagent, comme surcharges de source. Les lignes de domaine couvrent chaque schéma, sous-graphe et remote schema que porte le téléversement ; chacune est un sélecteur sur les domaines existants de l'organisation qui accepte aussi un nom saisi, marqué « nouveau domaine » lorsqu'il ne correspond à aucun. Appliquer fusionne avec ce que l'organisation possède déjà.

### Les types proviennent de la source à l'aperçu {: #types-come-from-the-source-at-preview }

Un export Hasura nomme les colonnes sans types, et une table suivie sans autorisation ne nomme aucune colonne. L'aperçu s'exécute avec les connexions de source fournies, donc il lit le `information_schema.columns` de chaque source SQL accessible : chaque colonne non typée reçoit le type de la source mappé vers le vocabulaire IR, et une table sans colonnes prend toutes les colonnes que la source possède, visibles pour `org_admin` seul, puisque Hasura ne les exposait à aucun autre rôle. Une source que l'aperçu ne peut pas atteindre est signalée par un avertissement `[sources]` et ses colonnes restent non typées, à finaliser avant l'application.

### Limitations {: #limitations }

- **Les computed fields** sont à refaire à la main : l'export nomme la fonction de base de données mais ne la définit pas. Ajoutez-la comme colonne d'une vue sur la table, ou calculez-la dans l'app.
- **Les remote schemas** atterrissent comme tables à partir des SDL d'autorisations de rôle. Un remote schema sans autorisations n'amène rien, car l'export ne porte aucun autre énoncé de sa forme.
- **Les fonctions suivies** ont besoin de la base de données à l'aperçu pour lire leur signature ; hors ligne, elles sont à refaire dans la page Commands.
- **Les nouvelles tentatives des event et cron triggers** ne sont pas reprises.

---

## Hasura DDN (v3)

### Localiser le projet HML {: #locate-the-hml-project }

Le convertisseur DDN lit directement le **répertoire** du projet DDN contenant les fichiers `.hml`, sans étape de build de supergraphe. Le premier composant de répertoire sous la racine du projet est pris comme nom de sous-graphe ; les fichiers sous `globals/` sont assignés au sous-graphe `globals`.

### Convertir {: #convert_1 }

```bash
python -m provisa.ddn ./my-ddn-project -o config.yaml
```

Omettez `-o` pour écrire la configuration sur stdout.

Options :

| Option | Rôle |
| ------ | --------- |
| `-o`, `--output` | Chemin YAML de sortie (par défaut : stdout) |
| `--source-overrides` | Fichier YAML avec des surcharges de connexion par source |
| `--domain-map` | Correspondances sous-graphe-vers-domaine sous forme de paires `SUBGRAPH=DOMAIN` |
| `--aggregates-output` | Chemin de sortie pour le fichier annexe des expressions d'agrégation (par défaut : `<output>-aggregates.yaml`) |
| `--dry-run` | Analyse et valide sans écrire de sortie |

Les métadonnées `AggregateExpression` sont préservées dans un fichier annexe `*-aggregates.yaml`.

### Ce qui est converti {: #what-gets-converted_1 }

| Concept DDN | Équivalent Provisa |
| ------------ | ------------------- |
| Modèle de sous-graphe | `tables[]` sous une source |
| Relation | `relationships[]` |
| Règle d'autorisation | Filtre RLS |
| Commande | Mutation webhook ou vue |
| Connecteur | Entrée de source avec détails de connexion |

### Limitations

- **Connecteurs Lambda** (fonctions TypeScript/Python) nécessitent une configuration manuelle de webhook
- **Plugins de cycle de vie** n'ont pas d'équivalent direct
- **Modes d'authentification DDN** se mappent aux fournisseurs d'authentification Provisa mais les chemins de revendication JWT peuvent nécessiter des ajustements

---

## Après l'import {: #after-import }

1. Examinez le `config.yaml` généré et le rapport. Commencez par les éléments de `Redo by hand`
2. Vérifiez les identifiants de connexion. Une connexion que l'export lisait depuis une variable d'environnement arrive vide ; le rapport la nomme
3. Démarrez Provisa et confirmez que les tables apparaissent dans l'Explorer
4. Exécutez vos requêtes GraphQL existantes ; le schéma est compatible avec les motifs courants
5. Soumettez les requêtes pour approbation via l'API Admin ou l'UI avant d'activer la gouvernance en production
