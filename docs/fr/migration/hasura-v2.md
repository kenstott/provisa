# Migration de Hasura v2 vers Provisa {: #migrating-from-hasura-v2-to-provisa }

## Prérequis {: #prerequisites }

1. Une instance Hasura v2 (v2.x) en fonctionnement, avec les métadonnées exportées.
2. Exportez les métadonnées dans l'une ou l'autre disposition :

   **Le répertoire de métadonnées de la CLI :**

   ```bash
   hasura metadata export --endpoint http://localhost:8080
   ```

   Cela crée un répertoire `metadata/`. `databases/databases.yaml` liste chaque source avec son type
   et sa connexion et renvoie à ses tables et à ses fonctions par des index `!include` ;
   `actions.yaml`, `cron_triggers.yaml`, `inherited_roles.yaml`, `remote_schemas.yaml` et les
   autres fichiers se trouvent à côté. L'ancienne disposition à plat (`tables.yaml` à la racine)
   est convertie aussi.

   **Le document unique de l'API de métadonnées :**

   ```bash
   curl -X POST http://localhost:8080/v1/metadata \
     -H "X-Hasura-Admin-Secret: <secret>" \
     -d '{"type":"export_metadata","args":{}}' > metadata.json
   ```

   L'objet nu comme l'enveloppe `{resource_version, metadata}` fonctionnent. L'import d'administration
   (Admin, Import Hasura Config) prend cette forme ; le convertisseur en ligne de commande ci-dessous
   lit des répertoires.
3. Python 3.11+ avec le paquet `provisa` installé.

## Utilisation de la CLI {: #cli-usage }

```bash
python -m provisa.hasura_v2 <metadata-dir> -o provisa.yaml
```

### Arguments

| Argument | Obligatoire | Description |
| ---------- | ---------- | ------------- |
| `metadata_dir` | Oui | Chemin vers le répertoire de métadonnées Hasura v2 exporté |

### Options

| Option | Valeur par défaut | Description |
| -------- | --------- | ------------- |
| `-o, --output FILE` | stdout | Chemin du fichier YAML de sortie |
| `--source-overrides FILE` | Aucune | Fichier YAML avec des surcharges de connexion par source |
| `--domain-map KEY=VAL ...` | Aucune | Correspondances schéma → domaine (par exemple, `public=core hr=people`) |
| `--auth-env-file FILE` | Aucune | Chemin vers un fichier `.env` contenant la configuration d'authentification JWT/admin-secret |
| `--dry-run` | désactivé | Analyse et valide sans écrire de sortie |

### Fichier de surcharges de source {: #source-overrides-file }

Un fichier YAML indexé par nom de source, avec les propriétés de connexion à surcharger :

```yaml
default:
  host: prod-db.example.com
  port: 5432
  database: myapp
  username: provisa_user
  password: "${env:PROD_DB_PASSWORD}"
```

### Fichier d'environnement d'authentification {: #auth-env-file }

Un fichier de type `.env` contenant la configuration d'authentification Hasura à
convertir. Le convertisseur effectue les correspondances suivantes :

- JWT avec `jwk_url` -> Provisa `provider: oauth`.
- JWT `claims_map` -> Provisa `role_mapping[]`.
- Admin secret -> Provisa `superuser`.
- Authentification par webhook -> un avertissement est émis (aucun équivalent Provisa).

## Le rapport {: #the-report }

La conversion se termine par un rapport en trois parties :

1. **Repris tel quel.** L'objet a une contrepartie dans Provisa et se convertit sans changer de sens.
2. **Transposé vers une meilleure fonctionnalité de Provisa.** Chaque élément dit pourquoi la fonctionnalité de Provisa fait mieux le travail.
3. **À refaire à la main.** Chaque élément dit quoi faire. Ce sont les avertissements.

Le convertisseur en ligne de commande affiche le rapport sur stderr sous les titres `Carried as`, `Mapped to` et `Redo by hand`, chacun avec un décompte, lorsqu'il reste quelque chose à refaire. L'import d'administration renvoie les trois listes avec l'aperçu.

## Matrice de parité des fonctionnalités {: #feature-parity-matrix }

| Fonctionnalité Hasura v2 | Partie | Équivalent Provisa | Notes |
| --- | --- | --- | --- |
| **Sources** (postgres, mysql, mssql, bigquery, citus) | repris | `sources[]` | Type converti : pg/postgres -> postgresql, mssql -> sqlserver. Une URL de connexion écrite est décomposée en hôte/port/base de données/utilisateur/mot de passe. Taille du pool conservée. Une URL lue depuis une variable d'environnement n'est pas devinée (voir ci-dessous). |
| **Tables** (tables suivies) | repris | `tables[]` | Schéma et nom de table conservés. `source_id` renvoie à la source. |
| **Noms de table personnalisés** (`custom_name`, `custom_root_fields.select`) | repris | `tables[].alias` | Première valeur non nulle parmi `select`, `select_by_pk`, `custom_name`. |
| **Noms de colonnes personnalisés** (`column_config`, ancien `custom_column_names`) | repris | `columns[].alias` | Le nom personnalisé de chaque colonne devient son alias. |
| **Autres noms de champs racines** (`select_by_pk`, `select_aggregate`, `insert`, ...) | transposé | Le champ unique de la table | Signalés par leur nom. Une lecture par clé est un filtre sur la table, un agrégat son champ `_aggregate`, une écriture sa mutation. |
| **Autorisations de select** (colonnes, filtre) | repris | `columns[].visible_to[]`, `rls_rules[]` | Les listes de colonnes deviennent `visible_to` ; `*` est pris en charge. Les filtres deviennent du SQL ; les variables de session deviennent `current_setting('provisa.<name>')`. |
| **Filtres à travers des relationships** | repris | Sous-requêtes `EXISTS` dans `rls_rules[]` | La règle suit la relationship jusqu'à sa table. |
| **Autorisations d'insert, update et delete** | repris | `tables[].write_grants[]` | Une autorisation par rôle et par opération : colonnes issues de `columns`, `rows` de `filter`, `check` de `check`, presets de `set`. Les rôles reçoivent le droit `write`. |
| **Rôle admin** | repris | `org_admin` | Détient toutes les autorisations sur toutes les colonnes et lignes, et est nommé sur chaque colonne importée. |
| **Relationships objet** | repris | `relationships[]` avec `cardinality: many-to-one` | Approuvées. Le champ GraphQL est le nom de la relationship Hasura. |
| **Relationships tableau** | repris | `relationships[]` avec `cardinality: one-to-many` | Approuvées. Le champ GraphQL est le nom de la relationship Hasura. |
| **Rôles hérités** (un rôle) | repris | `roles[].parent_role_id` | Le rôle devient le parent. |
| **Rôles hérités** (plusieurs rôles) | transposé | Un rôle détenant l'union de leurs autorisations | Les rôles Provisa ont un seul parent, donc l'union est écrite explicitement sur le rôle. |
| **Computed fields** | à refaire | Une colonne de vue | L'export nomme la fonction, pas son corps. Le rapport donne le `SELECT` à ajouter. |
| **Fonctions suivies** | repris, sinon à refaire | Commandes `functions[]` | L'aperçu lit la signature dans la base de données. Sans elle, ajoutez la fonction dans la page Commands. |
| **Actions** | transposé | Commandes appelant le handler | `{{VAR}}` dans l'URL du handler s'écrit `${env:VAR}` et se résout quand la commande est appelée. |
| **Cron triggers** | repris | `scheduled_triggers[]` | Expression cron, URL, payload et indicateur enabled. Les nouvelles tentatives ne sont pas reprises (avertissement). |
| **Event triggers** | transposé | `scheduled_triggers[]` sur la modification de la table | Se déclenche par lot de modifications, à toute modification. Les nouvelles tentatives ne sont pas reprises (avertissement). |
| **Remote schemas** | transposé | `sources[]` (`graphql_remote`) atterri en tables | Nom, URL, en-têtes et authentification conservés ; les champs racines deviennent des tables gouvernées. |
| **Tables enum** | repris | Table créée | L'indicateur `is_enum` n'est pas repris (aucun équivalent Provisa). |
| **Allow lists, query collections, endpoints REST** | à refaire | Aucun | Signalés lorsqu'ils sont présents dans un export consolidé ; non convertis. |

### Relationships {: #relationships }

- Le champ GraphQL de chaque relationship est le nom Hasura. Lorsque la table a déjà un champ de ce nom, la relationship prend le nom que lui donne Provisa et le rapport avertit.
- Deux relationships qui déclarent le même join fusionnent en une seule. Le rapport indique laquelle a fusionné dans laquelle.
- Une relationship qui joint sur plusieurs paires de colonnes est signalée par un avertissement et n'est pas créée, car une relation Provisa joint sur une seule paire. Les règles de lignes qui la traversent sont tout de même converties.
- Une relationship objet déclarée par la seule colonne de clé étrangère est résolue par la relationship tableau inverse ; sans elle, un avertissement est émis.

### Autorisations d'écriture {: #write-grants }

Une autorisation indique ce qu'un rôle peut écrire dans une table par une opération. Un `filter` ou `check` vide signifie toutes les lignes (`TRUE`). Une valeur de `set` `X-Hasura-User-Id` devient un preset `session` de `hasura_user_id` ; toute autre valeur devient un preset `literal`. Une colonne à preset ne figure pas aussi dans les colonnes de l'autorisation.

### Actions, fonctions suivies et triggers {: #actions-tracked-functions-and-triggers }

- Une **action** devient une commande qui appelle son handler, visible pour les rôles que nomment ses autorisations (pour `org_admin` seul lorsqu'elles n'en nomment aucun). Une action asynchrone est signalée par un avertissement : la commande attend la réponse du handler.
- Une **fonction suivie** devient une commande lorsque l'aperçu peut lire ses arguments et sa table de retour depuis une source PostgreSQL. Hors ligne, et pour toute fonction absente, surchargée ou qui renvoie une table que l'import n'enregistre pas, elle passe à refaire.
- Un **event trigger** se déclenche à chaque modification de sa table, une fois par lot, et envoie la table et les lignes modifiées plutôt qu'un événement par ligne avec anciennes et nouvelles valeurs. Un trigger qui n'écoutait pas les trois opérations est signalé par un avertissement.

## Étapes après la conversion {: #post-conversion-steps }

1. **Examinez le rapport.** Commencez par `Redo by hand`.
2. **Configurez les connexions des sources.** Une connexion que l'export écrit est convertie telle qu'écrite. Une connexion lue depuis une variable d'environnement arrive vide. Utilisez `--source-overrides` ou modifiez directement la sortie.
3. **Vérifiez les affectations de domaines.** Sans `--domain-map`, toutes les tables vont dans `default`.
   Affectez des schémas à des domaines avec `--domain-map public=core analytics=reporting`.
4. **Contrôlez les règles de lignes.** Les filtres sont convertis en SQL. Examinez les expressions booléennes
   complexes (`_and`/`_or`/`_exists` imbriqués) et les filtres à travers des relationships.
5. **Refaites ce qui est listé.** Les computed fields, les fonctions suivies que l'aperçu n'a pas pu lire,
   les nouvelles tentatives des triggers et l'authentification par webhook n'ont pas de conversion automatique.
6. **Configurez l'authentification.** Si votre instance Hasura utilise l'authentification JWT/webhook, créez un fichier
   d'environnement d'authentification et relancez avec `--auth-env-file`.
7. **Envoyez les variables de session.** Configurez le fournisseur d'identité pour que le jeton porte chaque variable de session que le rapport liste sous « mappé vers » (par exemple `x-hasura-user-id`, au niveau supérieur ou dans l'objet de claims Hasura), ou enregistrez-la par membre comme attribut.
8. **Testez.** Démarrez le serveur Provisa et vérifiez les requêtes sur vos sources de données.

## Problèmes courants et dépannage {: #common-issues-and-troubleshooting }

### La connexion vient d'une variable d'environnement {: #connection-comes-from-an-environment-variable }

Si le `database_url` de la source est une référence à une variable d'environnement (`{"from_env": "PG_URL"}`),
sa valeur n'est pas dans l'export. Le convertisseur n'en devine aucune : la source a un hôte,
une base de données, un utilisateur et un mot de passe vides, et le rapport la liste dans `Redo by hand`.
Saisissez la connexion dans les paramètres de sources de l'import (ou avec `--source-overrides`) et convertissez
à nouveau.

### Colonnes génériques {: #wildcard-columns }

Lorsqu'une autorisation accorde `columns: "*"`, le convertisseur crée une seule entrée de colonne
générique. Après la conversion, vous pouvez souhaiter la remplacer par des listes de colonnes
explicites en inspectant le schéma réel de la base de données.

### Fidélité des event triggers {: #event-trigger-fidelity }

Un event trigger devient un trigger sur les modifications de sa table, qui appelle le même webhook. Il se déclenche
par lot de modifications, à toute modification, que celle-ci soit passée par Provisa ou directement par la base de données, et envoie la table et les
lignes modifiées. Les anciennes et nouvelles valeurs par ligne, le filtrage par opération et les nouvelles tentatives de Hasura ne sont pas repris ;
le rapport avertit de chacun qui s'applique.
Faites agir le handler sur les lignes qui le concernent, et réessayez dans le handler si un appel manqué compte.

### Rôles manquants {: #missing-roles }

Les rôles sont collectés à partir des autorisations de table, des autorisations d'action et des rôles hérités. Si un rôle existe dans Hasura
mais n'apparaît dans aucun d'eux, il n'apparaîtra pas dans la sortie.

### Champs racines personnalisés {: #custom-root-fields }

Les noms de champs racines `select` et `select_by_pk` donnent l'alias de la table. Les autres champs racines personnalisés
(`select_aggregate`, `insert`, `update`, `delete`) sont signalés par leur nom : Provisa sert
une table comme un seul champ, avec les agrégats et les écritures comme son champ `_aggregate` et ses mutations.

## Exemple {: #example }

Convertir un projet Hasura v2 typique avec deux schémas mappés vers des domaines :

```bash
# Export metadata from Hasura
hasura metadata export --endpoint http://localhost:8080

# Convert with domain mapping and source overrides
python -m provisa.hasura_v2 metadata/ \
  -o provisa.yaml \
  --domain-map public=core hr=people \
  --source-overrides overrides.yaml \
  --auth-env-file auth.env

# Dry run first to check for warnings
python -m provisa.hasura_v2 metadata/ --dry-run
```

Structure de la sortie (abrégée) :

```yaml
sources:
  - id: default
    type: postgresql
    host: prod-db.example.com
    port: 5432
    database: myapp
domains:
  - id: core
  - id: people
tables:
  - source_id: default
    domain_id: core
    schema_name: public
    table_name: users
    columns:
      - name: id
        visible_to: [org_admin, user]
      - name: email
        visible_to: [org_admin, user]
    write_grants:
      - role: user
        operation: update
        columns: [email]
        rows: "id = current_setting('provisa.hasura_user_id')"
        check: "TRUE"
      - role: org_admin
        operation: insert
        columns: [id, email]
        check: "TRUE"
      - role: org_admin
        operation: update
        columns: [id, email]
        rows: "TRUE"
        check: "TRUE"
      - role: org_admin
        operation: delete
        rows: "TRUE"
roles:
  - id: user
    capabilities: [query_development, write]
    domain_access: ["*"]
rls_rules:
  - table_id: users
    role_id: user
    filter: "id = current_setting('provisa.hasura_user_id')"
relationships:
  - id: orders.user
    source_table_id: orders
    target_table_id: users
    source_column: user_id
    target_column: id
    cardinality: many-to-one
    graphql_alias: user
```
