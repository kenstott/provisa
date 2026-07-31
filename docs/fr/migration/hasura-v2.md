# Migration de Hasura v2 vers Provisa

## Prérequis

1. Une instance Hasura v2 (v2.x) en cours d'exécution, avec les métadonnées exportées.
2. Exporter les métadonnées avec la CLI Hasura :

   ```bash
   hasura metadata export --endpoint http://localhost:8080
   ```

   Cela crée un répertoire `metadata/` contenant `sources.yaml`, `actions.yaml`,
   `cron_triggers.yaml`, `inherited_roles.yaml`, `remote_schemas.yaml`, etc.
3. Python 3.11+ avec le paquet `provisa` installé.

## Utilisation de la CLI

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

### Fichier de surcharges de source

Un fichier YAML indexé par nom de source, avec les propriétés de connexion à surcharger :

```yaml
default:
  host: prod-db.example.com
  port: 5432
  database: myapp
  username: provisa_user
  password: "${env:PROD_DB_PASSWORD}"
```

### Fichier d'environnement d'authentification

Un fichier de type `.env` contenant la configuration d'authentification Hasura à
convertir. Le convertisseur effectue les correspondances suivantes :

- JWT avec `jwk_url` -> Provisa `provider: oauth`.
- JWT `claims_map` -> Provisa `role_mapping[]`.
- Admin secret -> Provisa `superuser`.
- Authentification par webhook -> un avertissement est émis (aucun équivalent Provisa).

## Matrice de parité des fonctionnalités

| Fonctionnalité Hasura v2 | Équivalent Provisa | Remarques |
| --- | --- | --- |
| **Sources** (postgres, mysql, mssql, bigquery, citus) | `sources[]` | Type mappé : pg/postgres -> postgresql, mssql -> sqlserver. L'URL de connexion est analysée en host/port/database/username/password. Les paramètres de pool sont conservés. |
| **Tables** (tables suivies) | `tables[]` | Le schéma et le nom de la table sont conservés. `source_id` établit le lien avec la source. |
| **Noms de table personnalisés** (`custom_name`, `custom_root_fields.select`) | `tables[].alias` | Première valeur non nulle parmi `select`, `select_by_pk`, `custom_name`. |
| **Noms de colonne personnalisés** | `columns[].alias` | Mappe le dictionnaire `custom_column_names` vers des alias de colonne. |
| **Autorisations de sélection** (colonnes, filtre) | `columns[].visible_to[]`, `rls_rules[]` | Les listes de colonnes deviennent `visible_to`. Les colonnes génériques (`*`) sont prises en charge. Les filtres sont convertis en SQL via `bool_expr_to_sql`. |
| **Autorisations d'insertion/mise à jour** (colonnes) | `columns[].writable_by[]` | Les listes de colonnes deviennent `writable_by`. Les rôles obtiennent la capacité `write`. |
| **Autorisations de suppression** | Mise à niveau de la capacité du rôle | Le rôle obtient la capacité `write`. Aucune correspondance de suppression par table. |
| **Relations d'objet** | `relationships[]` avec `cardinality: many-to-one` | La correspondance des colonnes est conservée. |
| **Relations de tableau** | `relationships[]` avec `cardinality: one-to-many` | La correspondance des colonnes est conservée. |
| **Champs calculés** | `functions[]` | Mappés vers une Function dont `returns` pointe vers l'ID de la table parente. |
| **Fonctions suivies** | `functions[]` | `exposed_as` prend la valeur mutation par défaut. Le schéma est conservé. |
| **Actions** (gestionnaire de procédure stockée) | `functions[]` | Converties en une configuration de Function lorsqu'elles s'appuient sur une procédure stockée. |
| **Actions** (gestionnaire de webhook) | Non converties | Un avertissement est émis, incluant l'URL du gestionnaire. |
| **Déclencheurs cron** | Non convertis | Un avertissement est émis. (Des déclencheurs planifiés existent au runtime, mais le convertisseur ne les mappe pas.) |
| **Déclencheurs d'événements** | Non convertis | Un avertissement est émis. (Des déclencheurs d'événements existent au runtime, mais le convertisseur ne les mappe pas.) |
| **Rôles hérités** | `roles[].parent_role_id` | Le premier rôle de `role_set` devient le rôle parent. Tous les rôles enfants sont créés. |
| **Schémas distants** | `sources[]` (`graphql_remote`) | Enregistrés comme source `graphql_remote`. Le nom, l'URL, les en-têtes et la configuration d'authentification sont conservés. |
| **Tables enum** | Table créée | L'indicateur `is_enum` n'est pas reporté (aucun équivalent Provisa). |
| **Listes d'autorisation** | Ignorées | Absentes du modèle de métadonnées. |

## Étapes après la conversion

1. **Vérifier le YAML de sortie.** Vérifiez que les sources, les tables et les rôles sont corrects.
2. **Configurer les connexions de source.** Le convertisseur analyse les URL de connexion,
   mais retombe sur `localhost` en cas d'échec d'analyse. Utilisez `--source-overrides` ou modifiez directement la sortie.
3. **Vérifier les assignations de domaine.** Sans `--domain-map`, toutes les tables se retrouvent dans `default`.
   Assignez les schémas aux domaines avec `--domain-map public=core analytics=reporting`.
4. **Vérifier les règles RLS.** Les filtres sont convertis en approximations SQL. Les
   expressions booléennes complexes (`_and`/`_or`/`_exists` imbriqués) doivent être révisées manuellement.
5. **Examiner les avertissements.** Le convertisseur affiche sur stderr un résumé des avertissements
   pour les fonctionnalités qu'il ne mappe pas (déclencheurs d'événements, déclencheurs cron, actions basées sur des webhooks).
6. **Configurer l'authentification.** Si votre instance Hasura utilise une authentification JWT/webhook, créez un
   fichier d'environnement d'authentification et relancez avec `--auth-env-file`.
7. **Tester.** Démarrez le serveur Provisa et vérifiez les requêtes par rapport à vos sources de données.

## Problèmes courants et dépannage

### L'URL de connexion n'est pas analysée

Si le `database_url` de la source est une référence à une variable d'environnement (`{"from_env": "PG_URL"}`),
le convertisseur ne peut pas la résoudre au moment de la conversion. La source aura des valeurs
d'espace réservé (`host: localhost`, `database: default`). Corrigez cela avec `--source-overrides`.

### Colonnes génériques

Lorsqu'une autorisation accorde `columns: "*"`, le convertisseur crée une seule entrée de colonne
générique. Après la conversion, vous pouvez souhaiter la remplacer par des listes de colonnes
explicites en inspectant le schéma réel de la base de données.

### Fidélité des déclencheurs d'événements

Les déclencheurs d'événements sont convertis avec `operations` et `webhook_url`, mais les garanties de
livraison spécifiques à Hasura (exactement une fois, redélivrance) n'ont pas d'équivalents directs
dans Provisa. Examinez la section `event_triggers` et configurez votre infrastructure de webhooks en conséquence.

### Rôles manquants

Les rôles sont collectés uniquement à partir des entrées d'autorisation. Si un rôle existe dans
Hasura mais n'a aucune autorisation sur une table ou une action, il n'apparaîtra pas dans la sortie.

### Champs racine personnalisés

Seuls les champs racine `select` et `select_by_pk` sont utilisés pour l'alias de la table. Les autres
champs racine personnalisés (`select_aggregate`, `insert`, `update`, `delete`) ne sont pas mappés.

## Exemple

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

Structure de sortie :

```yaml
sources:
  - id: default
    type: postgresql
    host: prod-db.example.com
    port: 5432
    database: myapp
    ...
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
        visible_to: [user, admin]
      - name: email
        visible_to: [admin]
        writable_by: [admin]
    alias: Users
roles:
  - id: admin
    capabilities: [read, write]
    domain_access: ["*"]
  - id: user
    capabilities: [read]
    domain_access: ["*"]
rls_rules:
  - table_id: default.public.users
    role_id: user
    filter: "id = x-hasura-user-id"
relationships:
  - id: default.public.orders.user
    source_table_id: default.public.orders
    target_table_id: default.public.users
    source_column: user_id
    target_column: id
    cardinality: many-to-one
```
