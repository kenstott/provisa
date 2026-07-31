# Migration de Hasura DDN (v3) vers Provisa

## Prérequis

1. Un projet Hasura DDN avec des fichiers HML (extension `.hml`).
   Les projets DDN ont généralement une structure de répertoires du type :
   ```
   my-ddn-project/
     app/
       subgraph1/
         models/
           MyModel.hml
         commands/
           MyCommand.hml
       subgraph2/
         ...
     globals/
       ...
   ```
2. Python 3.11+ avec le paquet `provisa` installé.

## Utilisation de la CLI

```bash
python -m provisa.ddn <hml-dir> -o provisa.yaml
```

### Arguments

| Argument | Obligatoire | Description |
|----------|----------|-------------|
| `hml_dir` | Oui | Chemin vers le répertoire du projet DDN HML (analysé récursivement pour les fichiers `.hml`) |

### Options

| Option | Valeur par défaut | Description |
|--------|---------|-------------|
| `-o, --output FILE` | stdout | Chemin du fichier YAML de sortie |
| `--source-overrides FILE` | Aucune | Fichier YAML avec des surcharges de connexion par source |
| `--domain-map KEY=VAL ...` | Aucune | Correspondances sous-graphe → domaine (par exemple, `app=core analytics=reporting`) |
| `--dry-run` | désactivé | Analyse et valide sans écrire de sortie |

### Fichier de surcharges de source

Un fichier YAML indexé par nom de connecteur (après assainissement de l'ID : espaces, points,
barres obliques deviennent des underscores) avec les propriétés de connexion :

```yaml
my_pg_connector:
  host: prod-db.example.com
  port: 5432
  database: chinook
  username: provisa_user
  password: "${env:PROD_DB_PASSWORD}"
```

## Matrice de parité des fonctionnalités

| Type DDN | Équivalent Provisa | Remarques |
|---|---|---|
| **DataConnectorLink** | `sources[]` | Le type de source est déduit de l'URL du connecteur (postgres, mysql, mssql, mongo, clickhouse, snowflake, bigquery). Les détails de connexion utilisent des valeurs d'espace réservé par défaut ; utilisez `--source-overrides` pour définir les valeurs réelles. |
| **ObjectType** | Définitions de colonnes sur `tables[]` | Les champs deviennent des colonnes. `dataConnectorTypeMapping.fieldMapping` résout les noms de champs GraphQL vers les noms de colonnes physiques. |
| **Model** | `tables[]` | Chaque Model produit une table. `source_id` provient du connecteur, `table_name` de la collection. `graphql_type_name` devient `alias`. Le sous-graphe (et donc `domain_id`) est dérivé du répertoire du fichier : le premier composant de répertoire sous la racine du projet. |
| **Relationship** | `relationships[]` | Type objet -> `many-to-one`, type tableau -> `one-to-many`. La correspondance des champs est résolue via une recherche de colonne physique. |
| **TypePermissions** | `columns[].visible_to[]` | `allowedFields` détermine quels rôles peuvent voir chaque colonne. |
| **ModelPermissions** | `rls_rules[]` | Les prédicats de filtre sont convertis en clauses SQL WHERE. Prend en charge `_eq`, `_neq`, `_gt`, `_lt`, `_gte`, `_lte`, `_in`, `_nin`, `_like`, `_is_null`, `_and`, `_or`, `_not`. Les références aux variables de session sont préservées sous la forme `${x-hasura-...}`. |
| **Command** | `functions[]` | Les fonctions et les procédures sont toutes deux prises en charge. Les arguments, le type de retour et le nom du champ racine GraphQL sont préservés. `domain_id` est défini à partir du sous-graphe. |
| **AggregateExpression** | Fichier annexe `provisa-aggregates.yaml` | Les fonctions count, count_distinct et les fonctions d'agrégation par champ sont préservées dans un fichier annexe et converties en configuration d'agrégation Provisa. |
| **BooleanExpressionType** | Ignoré (silencieusement) | Utilisé en interne par DDN pour le filtrage ; aucun équivalent Provisa direct n'est nécessaire. |
| **AuthConfig** | Ignoré (silencieusement) | La configuration d'authentification DDN n'est pas transposée ; configurez l'authentification Provisa séparément. |
| **ScalarType** | Ignoré | Un avertissement avec le décompte est émis. |
| **GraphqlConfig** | Ignoré | Un avertissement avec le décompte est émis. |
| **CompatibilityConfig** | Ignoré | Un avertissement avec le décompte est émis. |
| **Autres types non reconnus** | Ignorés | Un avertissement avec le décompte par type est émis. |

## Concept clé : résolution des champs GraphQL vers les colonnes physiques

DDN sépare le schéma GraphQL (noms de champs) du schéma physique de la base de données (noms de
colonnes) via `dataConnectorTypeMapping` sur les ObjectTypes. Le convertisseur :

1. Lit les entrées `fieldMapping` des correspondances de types de chaque ObjectType.
2. Construit une table de correspondance : `{graphql_field_name -> physical_column_name}`.
3. Pour les champs sans correspondance explicite, suppose que le nom du champ est égal au nom de
   la colonne.
4. Utilise cette table de correspondance lors de la construction des colonnes, des relations et
   des expressions de filtre RLS.

Cela signifie que le fichier `provisa.yaml` de sortie utilise les **noms de colonnes physiques**
pour `columns[].name` et définit `columns[].alias` sur le nom de champ GraphQL lorsqu'ils
diffèrent.

## Étapes après la conversion

1. **Passez en revue le YAML de sortie.** Vérifiez les sources, les tables et les correspondances
   de colonnes.
2. **Configurez les connexions aux sources.** Les connecteurs ne fournissent qu'un indice d'URL
   pour la détection du type. L'hôte, le port, la base de données et les identifiants réels
   doivent être fournis via `--source-overrides` ou en modifiant la sortie.
3. **Vérifiez les assignations de domaine.** Les noms de sous-graphes sont dérivés de la structure
   de répertoires (le premier composant de répertoire sous la racine du projet). Sans
   `--domain-map`, chaque nom de sous-graphe devient directement un ID de domaine. Utilisez
   `--domain-map` pour les renommer.
4. **Vérifiez les règles RLS.** Les prédicats de filtre DDN sont convertis en approximations SQL.
   La logique booléenne imbriquée (`_and`/`_or`/`_not`) est prise en charge, mais les filtres
   complexes traversant des relations peuvent nécessiter une revue manuelle.
5. **Passez en revue la configuration d'agrégation.** Les expressions d'agrégation sont écrites
   dans un fichier annexe `provisa-aggregates.yaml` et converties en configuration d'agrégation
   Provisa.
6. **Passez en revue les avertissements.** Le convertisseur affiche un résumé sur stderr listant
   les types DDN ignorés et tout Model référençant des ObjectTypes inconnus.
7. **Testez.** Démarrez le serveur Provisa et vérifiez les requêtes par rapport à vos sources de
   données.

## Problèmes courants et dépannage

### Échec de la détection du type de source

L'URL du connecteur est utilisée de façon heuristique (recherche de mots-clés comme « postgres »,
« mysql », « mongo »). Si l'URL ne contient pas de mot-clé reconnaissable, la source utilise par
défaut `postgresql`. Remplacez avec `--source-overrides`.

### ObjectType manquant pour un Model

Si un Model référence un nom d'ObjectType introuvable dans un fichier `.hml`, la table est ignorée
et un avertissement est émis. Assurez-vous que tous les fichiers HML sont inclus dans le
répertoire analysé.

### Découverte des sous-graphes

Les sous-graphes sont dérivés de la structure de répertoires : le premier composant de répertoire
sous la racine du projet est pris comme nom de sous-graphe. Le champ `subgraph` à l'intérieur des
documents HML n'est pas utilisé. Les fichiers sous un répertoire `globals/` sont assignés au
sous-graphe `globals` et exclus de la découverte de domaine.

### Résolution de la source d'une relation

Les relations référencent un `source_type` (nom d'ObjectType) et un `target_model` (nom de
Model). Si aucun Model n'utilise l'ObjectType donné, la relation est ignorée silencieusement.

### Alias de colonnes partout

Si votre projet DDN utilise `fieldMapping` de façon extensive, attendez-vous à ce que la plupart
des colonnes aient un `alias` dans la sortie. C'est le comportement attendu -- `name` est la
colonne physique, `alias` est le nom GraphQL utilisé par votre application.

### Expressions d'agrégation

Les expressions d'agrégation sont préservées dans un fichier annexe `provisa-aggregates.yaml`
écrit à côté de la sortie et converties en configuration d'agrégation Provisa. Elles ne sont pas
stockées dans le `description` de la table.

## Exemple : conversion d'un projet DDN Chinook

```bash
# Convert the DDN project
python -m provisa.ddn ./chinook-ddn/ \
  -o provisa.yaml \
  --domain-map app=music \
  --source-overrides overrides.yaml

# Dry run to check warnings first
python -m provisa.ddn ./chinook-ddn/ --dry-run
```

Structure de sortie :

```yaml
sources:
  - id: chinook_pg
    type: postgresql
    host: prod-db.example.com
    port: 5432
    database: chinook
    ...
domains:
  - id: music
tables:
  - source_id: chinook_pg
    domain_id: music
    schema_name: public
    table_name: Album
    columns:
      - name: AlbumId
        visible_to: [admin, user]
      - name: Title
        visible_to: [admin, user]
      - name: ArtistId
        visible_to: [admin, user]
    alias: Albums
  - source_id: chinook_pg
    domain_id: music
    schema_name: public
    table_name: Artist
    columns:
      - name: artist_id
        visible_to: [admin, user]
        alias: ArtistId
      - name: artist_name
        visible_to: [admin, user]
        alias: Name
    alias: Artists
roles:
  - id: admin
    capabilities: [read]
    domain_access: ["*"]
  - id: user
    capabilities: [read]
    domain_access: ["*"]
relationships:
  - id: chinook_pg.public.Album.Artist
    source_table_id: chinook_pg.public.Album
    target_table_id: chinook_pg.public.Artist
    source_column: ArtistId
    target_column: artist_id
    cardinality: many-to-one
functions:
  - name: GetTopTracks
    source_id: chinook_pg
    schema_name: public
    function_name: get_top_tracks
    returns: Track
    domain_id: music
    description: "DDN function"
```
