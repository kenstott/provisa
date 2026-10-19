# Référence de l'API GraphQL admin {: #admin-graphql-api-reference }

L'API GraphQL admin est le plan de configuration de Provisa. C'est l'API que l'application web admin appelle pour chaque opération de gestion — créer des sources, enregistrer des tables, définir des relations, configurer des règles RLS, et tout ce qui façonne le modèle.

**Point de montage :** `POST /admin/graphql`

Ce n'est pas la même API que le plan de données à `/data/graphql`. Le plan de données sert les requêtes des utilisateurs finaux sur les domaines enregistrés et est décrit par le SDL à `/data/sdl`. L'API admin configure à quoi ce schéma ressemble et qui peut voir quoi.

---

## Comment l'interface communique avec cette API {: #how-the-ui-talks-to-this-api }

L'application web admin utilise Apollo Client, pointé vers `${API_BASE}/admin/graphql`.

Chaque requête porte un jeton bearer (récupéré à nouveau auprès du fournisseur d'authentification à chaque appel), un en-tête `X-Org-Id` en mode multilocataire, et un en-tête `X-Env` lors du service d'un environnement de branche.

Le schéma est assemblé à partir de deux classes `@strawberry.type` — `Query` depuis `schema_query.py` et `Mutation` depuis `schema_mutation.py` — et enveloppé dans une `ModelCommitExtension` qui enregistre chaque mutation contre la branche d'environnement courante.

---

## Autorisation {: #authorization }

**Mode dev :** lorsqu'aucune authentification n'est configurée et que chaque requête arrive comme un principal anonyme, toutes les vérifications de capacité sont ignorées. Cela garde une installation locale fonctionnelle sans configuration d'authentification.

**Portes de capacité :** les déploiements en production imposent des capacités nommées. Le droit spécifique requis par chaque champ est noté en ligne. Appeler une mutation sans la capacité requise déclenche une `PermissionError`. Le rôle d'administrateur de plateforme contourne toutes les vérifications de capacité.

**Portes de domaine :** plusieurs mutations vérifient également le domaine auquel l'objet appartient. Un appelant limité à `sales` ne peut pas enregistrer une table dans `finance`, mettre en file d'attente une règle RLS pour celle-ci, ou créer une relation dont la table source se trouve dans un domaine qu'il ne détient pas. Les vues sont davantage contraintes : chaque table que le SQL de la vue lit doit se trouver dans les domaines de l'appelant, car le SQL en écriture libre donnerait sinon à un membre un accès à des données hors de sa portée.

**Héritage de rôle :** les capacités d'un rôle parent sont héritées par les rôles enfants. `createRole` et `deleteRole` refusent les cycles et empêchent de supprimer un rôle qui a des héritiers.

---

## Type de retour commun {: #common-return-type }

La plupart des mutations renvoient `MutationResult`.

```graphql
type MutationResult {
  success: Boolean!
  message: String!
  code: String          # stable i18n key, e.g. "schema.source_created"
  params: JSON          # key/value pairs for client-side localization
}
```

Lorsqu'une mutation échoue, `success` vaut `false` et `message` porte la raison en anglais. `code` est un identifiant stable que l'interface utilise pour afficher un message localisé.

---

## Requêtes {: #queries }

### Sources

#### `sources → [SourceType!]!`

Toutes les sources de données enregistrées.

```graphql
query {
  sources {
    id type host port database username dialect
    cacheEnabled cacheTtl replicate
    loadProtected offPeakWindow offPeakTz
    gqlNamingConvention path allowedDomains
    description mappingJson federationHintsJson
    changeSignal passwordRef
    cdc {
      bootstrapServers topicPrefix consumerGroupId
      schemaRegistryUrl schemaRegistryAuth schemaRegistryUsername schemaRegistryPassword
      schemaRegistryToken schemaRegistryClientCert schemaRegistryClientKey schemaRegistryCa
    }
  }
}
```

`passwordRef` est une référence `${secret:NAME}` dans le coffre-fort de l'organisation — jamais l'identifiant en clair.

#### `source(id: String!) → SourceType`

Une seule source par ID. Renvoie `null` si introuvable.

#### `availableSchemas(sourceId: String!) → [String!]!`

Schémas visibles dans une source, filtrés pour exclure ceux internes à Provisa. Utilise d'abord l'introspection native ; retombe sur le catalogue du moteur lorsque le type de source n'a pas de pool direct.

#### `availableTables(sourceId: String!, schemaName: String = "public") → [AvailableTableType!]!`

Tables dans un schéma d'une source, avec leurs commentaires. Pour les sources OpenAPI, renvoie les opérations GET dont la réponse est un tableau ou un wrapper de pagination. Pour les sources GraphQL, renvoie les champs de requête renvoyant une liste. Pour gRPC, renvoie les RPC en streaming serveur.

#### `availableColumns(sourceId: String!, schemaName: String!, tableName: String!) → [String!]!`

Noms de colonnes pour une table dans le catalogue du moteur. Pour les sources govdata, utilise un résolveur séparé.

#### `availableColumnsMetadata(sourceId: String!, schemaName: String!, tableName: String!) → [AvailableColumnType!]!`

Noms de colonnes avec types de données, commentaires, types de filtre natifs, et indicateurs de clé primaire. Pour les sources OpenAPI, dérive la forme du schéma de réponse et des paramètres de l'opération.

#### `availableFunctions(sourceId: String!, schemaName: String = "openapi") → [AvailableTableType!]!`

Opérations non-GET pour une source OpenAPI (POST, PUT, PATCH, DELETE). Renvoie une liste vide pour les sources non-OpenAPI.

#### `crawlSource(path, depth, pattern, recursive, simpleLinks, sameDomain, excludePattern) → CrawlResultType`

Prévisualiser ce qu'une exploration de connecteur de fichiers découvrirait — fichiers, tables et colonnes — avant qu'une source ne soit créée. Les paramètres HTTP uniquement (`simpleLinks`, `sameDomain`, `excludePattern`) sont ignorés pour les racines locales, S3, FTP et SFTP.

#### `suggestTableAlias(tableName: String!, domainId: String!, sourceId: String!) → String!`

Renvoie l'alias à utiliser lors de l'enregistrement de `tableName` dans `domainId` depuis `sourceId`. Renvoie un alias simple en snake-case en l'absence de conflit, ou un alias préfixé par la source (`sqlite_b_orders`) lorsque le nom effectif est déjà pris par une source différente dans le même domaine.

---

### Tables

#### `tables → [RegisteredTableType!]!`

Toutes les tables enregistrées, chacune avec sa liste complète de colonnes. La visibilité des colonnes dans la réponse respecte la capacité `table_registration` de l'appelant — `canDeployToDb` est conditionné à la détention de ce droit par l'appelant.

Chaque `RegisteredTableType` expose des sous-champs calculés :

- **`refreshPolicySummary → RefreshPolicySummaryType`** — la politique effective de rafraîchissement/service en texte brut, dérivée côté serveur de la même résolution du planificateur que celle utilisée par le moteur. Renvoie `null` pendant le démarrage.
- **`graphqlFieldName → String`** — le nom de champ que cette table porte dans le schéma compilé du plan de données, afin que le panneau Data Product puisse construire un exemple exécutable sans reproduire l'algorithme de nommage.
- **`dqDataset → String`** — cette table en tant que jeu de données de contrat de qualité des données, sous la forme que le vérificateur scanne.
- **`productId → String`** — le produit de données auquel cette table appartient. Une table de vérificateur DQ hérite du produit de la table dont son contrat scanne.

#### `refreshPolicyPreview(...) → RefreshPolicySummaryType`

Prévisualiser le résumé effectif de rafraîchissement/service pour des paramètres de table *provisoires* (non enregistrés), afin que le résumé en haut du formulaire se mette à jour au fur et à mesure des changements de champs sans rien persister. Même dérivation que `refreshPolicySummary` ci-dessus.

Arguments : `sourceId`, `domainId`, `schemaName`, `tableName`, `cacheTtl`, `replicate`, `loadProtected`, `offPeakWindow`, `offPeakTz`, `changeSignal`.

#### `columnDependents(tableId: String!, renamed: [String!], removed: [String!]) → [ColumnDependentsType!]!`

Artefacts qu'un renommage d'alias ou une suppression de colonne en attente casserait. Consultatif — l'interface admin l'affiche avant l'enregistrement et l'administrateur décide. Doit être appelé *avant* l'enregistrement, car les dépendants ont été écrits contre le nom exposé que la colonne porte actuellement.

---

### Relations {: #relationships }

#### `relationships → [RelationshipType!]!`

Toutes les relations définies par l'utilisateur (exclut les entrées `gql_auto__` auto-générées et les entrées synthétiques `meta:%` utilisées par le diagramme ERD).

#### `allRelationships → [RelationshipType!]!`

Identique à `relationships`, mais inclut les entrées synthétiques `meta:%`. Utilisé par le diagramme ERD, qui doit montrer chaque arête, y compris les liens implicites `HAS_TABLE` entre les tables de données et le registre de métadonnées.

Chaque `RelationshipType` expose :

- **`autoSuggested → Boolean`** — si la relation a été suggérée par l'analyse de clé étrangère (`id` commence par `fk__`).
- **`physicalName → String`** — le nom de la relation sur les plans SQL et gRPC (le paramètre `?include=`). Dérivé côté serveur ; les clients ne doivent pas translittérer l'alias GraphQL.

---

### Domaines, rôles et utilisateurs {: #domains-roles-users }

#### `domains → [DomainType!]!`

Tous les domaines de la base de données locataire de l'organisation active. La base de données locataire est isolée au niveau du schéma, de sorte que la liste des domaines d'un org-admin ne contient que les lignes de son organisation.

#### `roles → [RoleType!]!`

Rôles visibles par l'appelant. Un admin voit chaque rôle ; un non-admin ne voit que les rôles sans `org_id` ou appartenant à son organisation.

#### `resolveOwners(refs: [String!]!) → [UserSummaryType!]!`

Résout des ID de rôle ou d'utilisateur en utilisateurs individuels. Utilisé pour développer `DataProduct.ownerRole`, `Domain.steward`, et `Column.visibleTo` en une liste lisible par un humain. Les références inconnues sont renvoyées telles quelles afin que l'interface affiche l'ID brut plutôt que rien.

---

### Règles RLS {: #rls-rules }

#### `rlsRules → [RLSRuleType!]!`

Toutes les règles de sécurité au niveau des lignes. Le référentiel sous-jacent déchiffre `filterExpr` à la frontière.

---

### Produits de données {: #data-products }

#### `dataProducts → [DataProductType!]!`

Tous les produits de données. Requiert la capacité `data_product_read`.

---

### Étiquettes {: #tags }

#### `tags → [TagType!]!`

Toutes les définitions d'étiquettes, y compris leurs valeurs de paramètre autorisées par étiquette.

#### `tagAssignments → [TagAssignmentType!]!`

Toutes les affectations d'étiquettes à travers sources, tables, colonnes et relations.

---

### Vues matérialisées {: #materialized-views }

#### `mvList → [MVType!]!`

Toutes les vues matérialisées avec leur statut d'exécution : activée/désactivée, horodatage du dernier rafraîchissement, nombre de lignes, et dernière erreur.

---

### Cache

#### `cacheStats → CacheStatsType`

Statistiques de cache. Renvoie `storeType: "redis"` avec des métriques opérationnelles complètes lorsque Redis est configuré, `storeType: "memory"` pour le magasin fakeredis embarqué, et `storeType: "noop"` lorsqu'aucun cache n'est configuré.

#### `cacheTableStats → [CacheTableStatType!]!`

Nombre d'entrées en cache par table. Vide lorsqu'aucun magasin de cache n'est configuré.

#### `hotTables → [HotTableStatType!]!`

Tables dont Provisa conserve une copie, sur deux niveaux : `hot` (répliqué dans le magasin de réponses pour l'intégration en ligne des JOIN) et `warm` (déposé comme copie Iceberg). Une table est au plus dans un seul niveau.

#### `materializeStoreInfo → MaterializeStoreInfoType`

Identité du magasin de matérialisation durable : nom du moteur, référence DSN du magasin, nombre de MV, et si le magasin est local à l'instance (un fichier local comme DuckDB ou SQLite, ce qui signifie que chaque instance derrière un équilibreur de charge conserve sa propre copie).

---

### Santé système {: #system-health }

#### `systemHealth → SystemHealthType`

Statut de connexion du moteur, nombres de pools de workers, état du pool de la base de données de métadonnées, mode de cache, et vivacité de chaque écouteur de protocole (pgwire, gRPC, Arrow Flight, Bolt).

```graphql
query {
  systemHealth {
    engineConnected engineWorkerCount engineActiveWorkers
    metadataPoolSize metadataPoolFree metadataDialect
    cacheMode cacheConnected mvRefreshLoopRunning
    protocols { name status port }
  }
}
```

---

### Tâches planifiées {: #scheduled-tasks }

#### `scheduledTasks → [ScheduledTaskType!]!`

Déclencheurs planifiés depuis la config avec leur état d'exécution. Chaque entrée porte son expression cron, son `kind` (`webhook` ou `sql`), si elle est actuellement activée, l'horodatage de la dernière exécution (toujours `null` dans cette version — suivi par le planificateur), et la prochaine heure d'exécution planifiée depuis APScheduler.

---

### Qualité des données {: #data-quality }

#### `dqContractParse(checker: String!, contractText: String!) → DqContractType`

Analyser du texte de contrat brut en lignes modifiables du panneau constructeur. Appelé à chaque édition ; un échec d'analyse revient comme `error` plutôt que comme une erreur GraphQL, car un texte à moitié écrit est normal pendant que l'opérateur tape.

#### `dqCheckCatalog(checker: String!, dataset: String!) → DqCheckCatalogType`

Les vérifications que `checker` propose, limitées aux colonnes de `dataset`. Le jeu de données est la cible observée du contrat, résolue de la même manière que le scanneur la résout — de sorte que les vérifications proposées correspondent aux colonnes que le vérificateur verra réellement.

#### `dqCheckDefinition(checker: String!, check: DqCheckBuildInput!) → DqCheckDefinitionType`

Le texte d'une vérification depuis les éditeurs du panneau. Côté serveur car le dialecte a une seule implémentation ; une vérification créée par le constructeur et une écrite à la main doivent être indiscernables.

#### `dqContractBuild(checker: String!, dataset: String!, checks: [DqCheckInput!]!) → DqContractTextType`

Sérialiser les lignes de vérification modifiées en texte de contrat. L'inverse de `dqContractParse`. Côté serveur pour la même raison : le panneau ne peut pas émettre un texte que le vérificateur refuserait.

---

### Aperçus de source {: #source-previews }

#### `neo4jPreview(sourceId: String!, cypher: String!) → QueryPreviewType`

Prévisualiser une projection Cypher sur une source Neo4j : jusqu'à cinq lignes et les types de colonnes que l'enregistrement portera. Les échecs reviennent comme `error`.

#### `sparqlPreview(sourceId: String!, query: String!) → QueryPreviewType`

Prévisualiser un SELECT SPARQL sur une source SPARQL : jusqu'à cinq lignes, toutes les colonnes en texte.

---

### Kaggle

#### `kaggleTokenValid(token: String!) → Boolean!`

Vérification en direct contre l'API Kaggle. Renvoie `true` seulement lorsque le jeton s'authentifie. Alimente l'étape de contrôle du jeton dans le formulaire de source Kaggle.

#### `kaggleDatasets(token: String!, query: String = "", page: Int = 1) → [KaggleDatasetType!]!`

Rechercher dans l'ensemble du catalogue de jeux de données publics de Kaggle. Contrôlé par jeton.

---

### Calendriers {: #calendars }

#### `calendars → [CalendarType!]!`

Toutes les versions de calendrier de limite d'instantané enregistrées. Alimente le sélecteur de config de planification d'instantané et confirme quels calendriers une MV périodique peut référencer.

---

### Métriques {: #metrics }

#### `metrics → [MetricType!]!`

Toutes les définitions de métriques gouvernées. Les métriques dérivées de faits portent `fromFact`.

---

### Version du schéma {: #schema-version }

#### `schemaVersion → String!`

Hash SHA-256 de l'état actuel du schéma (domaines, ID de table, ID de relation). Le client Apollo lit cette valeur depuis l'en-tête de réponse `X-Schema-Version` et relance toutes les requêtes actives lorsqu'elle avance.

---

### Assistants IA {: #ai-helpers }

#### `generateTableDescription(tableId: String!) → String!`

Utiliser le LLM configuré pour générer une description d'une à deux phrases pour une table enregistrée. Enregistrez d'abord la table ; appeler ceci sur une table non enregistrée renvoie un message d'instruction.

#### `generateColumnDescription(tableId: String!, columnName: String!) → String!`

Utiliser le LLM configuré pour générer une description d'une phrase pour une seule colonne.

---

### Demandes de création {: #creation-requests }

#### `creationRequests → [CreationRequestType!]!`

Demandes de création en attente, visibles par les appelants qui détiennent la capacité de création correspondante. Utilisé lorsqu'un membre sans `create_relationship` ni `create_view` soumet une demande qu'un détenteur du droit doit approuver. Une demande de relation est listée pour les utilisateurs dont le droit `create_relationship` atteint le domaine de sa table source ou cible, et pour l'utilisateur qui l'a faite.

---

## Mutations

### Sources

#### `createSource(input: SourceInput!) → MutationResult`

Enregistre une nouvelle source de données. Un identifiant qui existe déjà est refusé (`schema.source_id_taken`) après les vérifications de la définition et avant que quoi que ce soit ne soit préparé ou écrit ; une source se modifie avec `updateSource`. Valide la connexion avant la persistance — une source refusée ne laisse aucune entrée dans le coffre. Stocke les identifiants dans le coffre de l'organisation et enregistre la référence ; le texte en clair n'atteint jamais la base de données. Requiert la capacité `source_registration`.

#### `updateSource(input: SourceInput!) → MutationResult`

Met à jour les détails de connexion, la description et la configuration d'une source existante, puis la provisionne à nouveau sur le moteur lié, reconstruit les schémas et réconcilie ses tables atterries, de sorte qu'une source enregistrée sous un moteur soit servie par celui vers lequel on a basculé depuis. Démonte et rattache l'endpoint pgwire des sources fichiers/SharePoint pour qu'un changement de chemin prenne effet immédiatement. Requiert `source_registration`.

#### `deleteSource(id: String!) → MutationResult`

Supprimer une source et son entrée de coffre-fort. Supprime le catalogue du moteur et reconstruit les schémas.

#### `renameSource(oldId: String!, newId: String!) → MutationResult`

Renomme l'identifiant d'une source. Un nouvel identifiant détenu par une autre source est refusé (`schema.source_id_taken`) et rien n'est écrit ; renommer une source vers son propre identifiant ne change rien.

#### `updateSourceCache(sourceId: String!, cacheEnabled: Boolean!, cacheTtl: Int) → MutationResult`

Activer ou désactiver la mise en cache des résultats de requête pour une source, et définir le TTL en secondes.

#### `updateSourceReplicate(sourceId: String!, replicate: Int) → MutationResult`

Définit quand les tables d'une source sont servies depuis leurs répliques : `null` Default, `-1` Never, `N` supérieur à 0 dès qu'une table dépasse N instructions gouvernées par intervalle, `0` Always. Une table sans valeur propre en hérite. Refusé lorsqu'il associerait `load_protected` à `-1`, ou laisserait sans `cache_ttl` une table `ttl` qu'il réplique. Requiert `source_registration`.

#### `updateSourceLoadProtection(sourceId, loadProtected, offPeakWindow, offPeakTz) → MutationResult`

Marquer une source comme protégée en charge (rafraîchissement planifié uniquement). Requiert au moins une porte — fenêtre hors pointe, cadence de TTL de cache, ou un signal de changement par sondage — sinon l'appel échoue.

#### `updateSourceNaming(sourceId: String!, gqlNamingConvention: String) → MutationResult`

Définir la convention de nommage GraphQL par source.

#### `updateSourceAllowedDomains(sourceId: String!, allowedDomains: [String!]!) → MutationResult`

Définir quels domaines peuvent utiliser une source (liste vide = sans restriction). Requiert `source_registration`.

#### `stageKaggleDataset(token, owner, ref, idPrefix) → KaggleStageResultType`

Télécharger et décompresser un jeu de données Kaggle sur le disque local. Renvoie le chemin du répertoire mis en zone de transit ; l'appelant crée ensuite une source de type `files` pointant vers celui-ci. Les paquets contenant SQLite sont rejetés dans leur intégralité. Requiert `source_registration`.

#### `refreshKaggleSource(sourceId: String!, token: String!) → MutationResult`

Récupérer à nouveau sur place le jeu de données d'une source dérivée de Kaggle. Ignore le téléchargement si Kaggle n'a rien de plus récent que ce qui est sur le disque. Requiert `source_registration`.

#### `refreshSourceStatistics(sourceId: String!) → MutationResult`

Exécuter `ANALYZE` sur toutes les tables enregistrées d'une source. Améliore les décisions d'ordre de jointure et de diffusion pour les requêtes fédérées.

---

### Tables

#### `registerTable(input: TableInput!) → MutationResult`

Enregistrer une nouvelle table (ou vue) dans un domaine. Requiert la capacité `table_registration` et l'appartenance au domaine cible. Un appelant sans `create_relationship` qui soumet une vue est mis en file d'attente comme demande de création pour qu'un titulaire des droits l'approuve.

#### `updateTable(input: TableInput!) → MutationResult`

Mettre à jour l'alias, la description, les métadonnées de colonnes, les paramètres de MV, et la config de livraison en direct d'une table existante. Requiert `table_registration`.

#### `deleteTable(id: Int!) → MutationResult`

Supprimer une table enregistrée. Recherche le domaine de la table pour la porte de domaine avant la suppression.

#### `updateTableCache(tableId: Int!, cacheTtl: Int) → MutationResult`

Remplacer le TTL de cache pour une table.

#### `updateTableReplicate(tableId: Int!, replicate: Int) → MutationResult`

Définit quand une table est servie depuis sa réplique. `null` = hériter de la valeur de la source. Mêmes valeurs et refus que `updateSourceReplicate`. Requiert `table_registration`.

#### `updateTableLoadProtection(tableId, loadProtected, offPeakWindow, offPeakTz) → MutationResult`

Remplacer la protection de charge pour une table. `null` pour `loadProtected` hérite du défaut de la source. Valide la combinaison de porte effective (table → source).

#### `updateTableNaming(tableId: Int!, gqlNamingConvention: String) → MutationResult`

Définir la convention de nommage GraphQL par table.

#### `deployViewToDb(tableId: Int!) → MutationResult`

Promouvoir une vue virtuelle Provisa en une vraie vue de base de données sur sa source native sous-jacente.

#### `forceRegen(tableId: Int!, reason: String!) → MutationResult`

Recalculer à la demande les lignes déposées d'une table, en contournant la porte de changement normale. `reason` est une annotation d'audit requise. Refusé pour les tables fédérées en direct (pas de lignes déposées).

#### `invalidateFileSource(tableId: Int!) → MutationResult`

Forcer le prochain accès d'une table de connecteur de fichiers SQLite à se resynchroniser depuis le disque.

#### `registerEntity(input: EntityInput!) → MutationResult`

Sucre syntaxique pour enregistrer une entité dimension/hub. Se réduit à une MV (bitemporelle, si historisée) et appelle `registerTable`.

#### `registerFact(input: FactInput!) → MutationResult`

Sucre syntaxique pour enregistrer un fait de schéma en étoile. Se réduit à une MV d'agrégation, crée les relations de dimension, et auto-enregistre les mesures de fait comme métriques gouvernées.

---

### Relations {: #relationships_1 }

#### `createRelationship(input: RelationshipInput!) → MutationResult`

Crée une relation. Le contrôle vérifie le domaine de la table source (pas celui de la cible). Une arête inter-domaines est stockée avec `needsReview: true`. Un appelant sans `create_relationship` est mis en file comme demande de création. Les arêtes de jonction (plusieurs-à-plusieurs) exigent des listes de clés de même longueur. Refusé nommément, avant toute écriture : un identifiant qui existe déjà (`schema.relationship_id_taken`) ; un nom de table qui ne se résout vers aucune table enregistrée ou vers plusieurs (`schema.relationship_unknown_source`, `_target`, `_junction`, `schema.relationship_ambiguous_table`) ; un alias Cypher ou un alias GraphQL déjà détenu par une autre relation de la même table source (`schema.relationship_alias_taken`, `schema.relationship_graphql_alias_taken`). Une table est nommée par son nom virtuel : son alias quand elle en a un.

#### `updateRelationship(input: RelationshipInput!) → MutationResult`

Modifie une relation existante, avec les mêmes vérifications que `createRelationship`. Un identifiant qu'aucune relation ne détient est refusé (`schema.relationship_not_found`). Une modification exige le droit lui-même ; elle n'est jamais mise en file comme demande.

#### `deleteRelationship(id: String!) → MutationResult`

Supprimer une relation par ID et reconstruire les schémas.

---

### Domaines {: #domains }

#### `createDomain(input: DomainInput!) → MutationResult`

Crée un domaine. Un identifiant qui existe déjà est refusé (`schema.domain_id_taken`). Les mots de segment réservés (`tables`, `relationships` et autres segments de chemin d'URI) sont refusés, de même que le littéral joker `*`. Requiert la capacité `org_settings`.

#### `deleteDomain(id: String!) → MutationResult`

Supprimer un domaine. Requiert `org_settings`.

#### `updateGqlNamingConvention(convention: String!) → MutationResult`

Définir la convention de nommage GraphQL globale et reconstruire les schémas pour tous les rôles. Seuls les noms de convention reconnus sont acceptés.

---

### Rôles

#### `createRole(input: RoleInput!) → MutationResult`

Crée un rôle avec des capacités, un accès aux domaines, des limites de débit facultatives et un rôle parent facultatif. Vérifie que le parent existe et que la chaîne de parents est sans cycle. Après les vérifications de la définition, un identifiant qui existe déjà est refusé (`schema.role_id_taken`). Requiert `user_management`.

#### `updateRole(input: RoleInput!) → MutationResult`

Modifie un rôle existant, avec les mêmes vérifications que `createRole`. Un identifiant qu'aucun rôle ne détient est refusé (`schema.role_not_found`). Redéfinir un rôle est un acte dans chaque domaine qu'il atteint.

#### `deleteRole(id: String!) → MutationResult`

Supprimer un rôle. Échoue si d'autres rôles en héritent — reparentez-les d'abord. Requiert `user_management`.

---

### Règles RLS {: #rls-rules_1 }

#### `upsertRlsRule(input: RLSRuleInput!) → MutationResult`

Créer ou mettre à jour une règle de sécurité au niveau des lignes. L'expression de filtre est validée au moment de l'enregistrement contre les colonnes de la table ou du domaine cible, de sorte qu'une règle que l'admin ne peut pas interroger soit refusée avec la raison plutôt que d'échouer silencieusement au moment de la requête. Requiert `masking_config`.

Les cibles sont mutuellement exclusives : définissez `tableId` pour une règle au niveau table, `domainId` pour une règle au niveau domaine, ou `actionName` pour une fonction/webhook suivi.

#### `deleteRlsRule(roleId, tableId, domainId, actionName) → MutationResult`

Supprimer une règle RLS. Requiert `masking_config`.

---

### Produits de données {: #data-products_1 }

#### `createDataProduct(input: DataProductInput!) → MutationResult`

Crée un produit de données. Un identifiant qui existe déjà est refusé (`schema.data_product_id_taken`). Requiert la capacité `data_product_rw`.

#### `updateDataProduct(input: DataProductInput!) → MutationResult`

Modifie un produit de données existant. Un identifiant qu'aucun produit ne détient est refusé (`schema.data_product_not_found`). Requiert `data_product_rw`.

#### `deleteDataProduct(id: String!) → MutationResult`

Supprimer un produit de données. Requiert `data_product_rw`.

---

### Étiquettes {: #tags_1 }

#### `createTag(input: TagInput!) → MutationResult`

Crée une définition d'étiquette. Un identifiant qui existe déjà est refusé (`schema.tag_id_taken`). Les étiquettes système et les étiquettes dérivées ne peuvent pas être redéfinies. `appliesTo` doit être un sous-ensemble non vide de `["source", "table", "column", "relationship", "command"]`.

#### `updateTag(input: TagInput!) → MutationResult`

Modifie une définition d'étiquette existante, avec les mêmes vérifications que `createTag`. Un identifiant qu'aucune étiquette ne détient est refusé (`schema.tag_not_found`).

#### `deleteTag(id: String!) → MutationResult`

Supprimer une étiquette. Refuse les étiquettes système et dérivées.

#### `assignTag(input: TagAssignmentInput!) → MutationResult`

Affecter une étiquette à une source, table, colonne, relation, ou commande. Applique les politiques de champ de l'étiquette (`reason_policy`, `expires_policy`) et — pour les étiquettes paramétrées — valide la valeur du paramètre contre la liste autorisée de l'étiquette.

#### `unassignTag(input: TagAssignmentInput!) → MutationResult`

Retirer une affectation d'étiquette.

#### `upsertTagParamValue(input: TagParamValueInput!) → MutationResult`

Ajouter ou redécrire une valeur de paramètre autorisée pour une étiquette paramétrée. La liste des valeurs autorisées est fermée : chaque affectation doit nommer une valeur de cette liste.

#### `deleteTagParamValue(tagId: String!, value: String!) → MutationResult`

Retirer une valeur autorisée. Refusé tant qu'une affectation la porte encore, car ces affectations nommeraient alors un type que la liste n'admet plus.

---

### Métriques {: #metrics_1 }

#### `createMetric(input: MetricInput!) → MutationResult`

Crée une définition de métrique gouvernée. Un nom qui existe déjà est refusé (`schema.metric_name_taken`). L'expression doit être analysable par sqlglot et contenir au moins une fonction d'agrégation. Régénère toutes les vues composées de métriques qui font référence à cette métrique. Requiert `table_registration`.

#### `updateMetric(input: MetricInput!) → MutationResult`

Remplace la définition d'une métrique existante, avec les mêmes vérifications que `createMetric`. Un nom qu'aucune métrique ne détient est refusé (`schema.metric_not_found`). L'enregistrer exige les domaines de la nouvelle expression et de la définition qu'elle remplace.

#### `deleteMetric(name: String!) → MutationResult`

Supprimer une métrique gouvernée. Reconstruit les schémas. Requiert `table_registration`.

---

### Calendriers {: #calendars_1 }

#### `createCalendar(input: CalendarInput!) → MutationResult`

Créer ou remplacer un calendrier de limite d'instantané versionné. Validé en construisant le `Calendar` en mémoire avant la persistance — échoue sur un système de base inconnu, un fuseau horaire incorrect, ou un ancrage fiscal incorrect.

#### `deleteCalendar(name: String!) → MutationResult`

Supprimer un calendrier (toutes versions). Refusé lorsqu'une vue matérialisée le référence.

---

### Vues matérialisées {: #materialized-views_1 }

#### `refreshMv(mvId: String!) → MutationResult`

Déclencher un rafraîchissement manuel d'une vue matérialisée. Coordonne à travers la flotte lorsque le mode de cohérence de la MV est `shared`.

#### `toggleMv(mvId: String!, enabled: Boolean!) → MutationResult`

Activer ou désactiver une vue matérialisée.

---

### Cache

#### `purgeCache → MutationResult`

Purger tous les résultats de requête mis en cache.

#### `purgeCacheByTable(tableId: Int!) → MutationResult`

Purger les résultats mis en cache pour une table.

---

### Tâches planifiées {: #scheduled-tasks_1 }

#### `createScheduledTask(id, name, cron, kind, webhookName, argsJson, sql) → MutationResult`

Créer un déclencheur planifié — soit un appel webhook, soit une instruction SQL — et l'enregistrer en direct dans APScheduler. `kind` vaut `"webhook"` ou `"sql"`.

#### `deleteScheduledTask(taskId: String!) → MutationResult`

Retirer un déclencheur planifié de la config et du planificateur en direct.

#### `toggleScheduledTask(taskId: String!, enabled: Boolean!) → MutationResult`

Activer ou désactiver une tâche planifiée dans le fichier de config.

---

### Qualité des données {: #data-quality_1 }

#### `dryRunDqContract(sourceId: String!, contractText: String!) → DqDryRunType`

Exécuter un contrat contre la table en direct et renvoyer les résultats sans rien déposer. Une mutation plutôt qu'une requête car cela coûte un vrai scan. Ce qu'elle prouve, c'est si l'identifiant du jeu de données résout vers la table gouvernée que l'opérateur vise.

#### `runDqCheckNow(schemaName: String!, tableName: String!) → MutationResult`

Déclencher immédiatement le job de sondage d'une table de vérificateur. Dépose les lignes de la manière normale, de sorte que les résultats persistent et que l'historique DQ montre le nouveau scan.

---

### Maintenance du schéma {: #schema-maintenance }

#### `rebuildSchemas → MutationResult`

Reconstruire le schéma en mémoire à partir de l'état de la base de données. Utile après des changements externes de base de données.

---

### Demandes de création {: #creation-requests_1 }

#### `approveCreationRequest(requestId: Int!) → MutationResult`

Approuve une demande de création : la même règle, les mêmes refus et les mêmes entrées du journal d'administration que `POST /admin/creation-requests/{id}/approve`. Le demandeur ne peut pas approuver sa propre demande et l'approbation d'un utilisateur compte une fois. L'approbation qui complète le compte de la demande crée ce qu'elle demande ; `params.status` vaut `executed` quand c'est celle-ci, sinon `pending`, et `params.waitingOn` liste les domaines qu'une demande de relation attend encore.

#### `executeCreationRequest(requestId: Int!) → MutationResult`

Exécute une demande de création qui a reçu ses approbations — relation, vue, table, source ou webhook — par le code qu'utilise la mutation directe. Refusé avec `requests.approvals_incomplete` tant que les approbations requises de la demande (une, ou deux pour une relation) ne sont pas réunies par des utilisateurs autres que le demandeur ; une demande de relation est aussi refusée avec `requests.waiting_on_domains` tant qu'un domaine qu'elle touche n'a pas d'approbateur. Les approbations se donnent par `POST /admin/creation-requests/{id}/approve`, et l'approbation qui complète le compte effectue la création, de sorte que cette mutation sert à réessayer une création qui a échoué. Une création qui échoue laisse la demande en attente et répond avec le code propre de l'échec.

#### `rejectCreationRequest(requestId: Int!, reason: String!) → MutationResult`

Rejette une demande en file avec un motif exploitable. `reason` est obligatoire. Une demande de relation peut être rejetée par tout utilisateur qui pourrait l'approuver. L'auteur d'une demande de tout autre type peut la retirer ; cela est consigné dans le journal d'administration comme un retrait.

---

### Compilation de requête {: #query-compilation }

#### `compileQuery(input: CompileQueryInput!) → [CompileQueryResult!]!`

Compiler une requête GraphQL du plan de données contre le schéma d'un rôle et renvoyer la décision de routage complète : SQL sémantique, SQL moteur, SQL direct, route, métadonnées d'application (filtres RLS appliqués, colonnes exclues, masquage appliqué), et Cypher compilé. Renvoie un résultat par champ racine de la requête.

```graphql
mutation {
  compileQuery(input: {
    role: "analyst"
    query: "{ orders { id customer_id total } }"
  }) {
    sql
    semanticSql
    engineSql
    route
    routeReason
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      maskingApplied
    }
  }
}
```

Champs de `CompileQueryInput` :

| Champ | Type | Description |
|-------|------|-------------|
| `query` | `String!` | Requête GraphQL du plan de données à compiler |
| `role` | `String!` | Rôle dont le schéma sert de référence pour la compilation |
| `variables` | `JSON` | Liaisons de variables |
| `flatSql` | `Boolean` | Renvoyer une seule chaîne SQL aplatie au lieu d'une paire sémantique/moteur |
| `flatCypher` | `Boolean` | Aplatir la sortie Cypher |
| `nodeOnlyCypher` | `Boolean` | Émettre du Cypher nœuds uniquement (pas de motifs d'arête) |

---

## Types d'entrée clés {: #key-input-types }

### `SourceInput`

| Champ | Type | Notes |
|-------|------|-------|
| `id` | `String!` | Identifiant de source |
| `type` | `String!` | Type de connecteur (ex. `postgres`, `files`, `openapi`) |
| `host` | `String` | |
| `port` | `Int` | |
| `database` | `String` | |
| `username` | `String` | |
| `password` | `String` | Texte en clair ou référence `${secret:NAME}` |
| `path` | `String` | Chemin du système de fichiers pour les sources fichier/CSV |
| `federationHintsJson` | `String` | Objet JSON pour les extras d'entrepôt (warehouse/role Snowflake, http_path Databricks) |
| `changeSignal` | `String` | `ttl` \| `probe` \| `ttl_probe` |
| `loadProtected` | `Boolean` | Rafraîchissement planifié uniquement |
| `offPeakWindow` | `String` | Fenêtre de maintenance `HH:MM-HH:MM` |
| `offPeakTz` | `String` | Fuseau horaire IANA |
| `cdc` | `SourceCdcConfigInput` | Configuration du transport Kafka CDC, y compris la manière d'atteindre son registre de schémas (voir [Authentification du registre de schémas](subscriptions.md#schema-registry-authentication)). `schemaRegistryPassword` et `schemaRegistryToken` sont renvoyés comme références de coffre, jamais comme identifiants. |

### `TableInput`

L'entrée principale d'enregistrement de table. Champs clés au-delà des bases :

| Champ | Notes |
|-------|-------|
| `materialize` | Déposer une copie dans le magasin de matérialisation |
| `mvRefreshInterval` | Secondes entre les rafraîchissements |
| `mvPersist` | `replace` \| `append` \| `upsert` |
| `mvIncremental` | Maintenance incrémentale |
| `mvBitemporalMode` | `snapshot` \| `delta` pour les tables bitemporelles |
| `mvCalendar` | Nom du calendrier d'instantané |
| `mvGrain` | Granularité d'instantané : `daily`, `weekly`, `monthly`, `annual`, ou personnalisée `3WE` / `LFR` |
| `viewSql` | SQL pour une vue dérivée |
| `viewMetrics` | Spécification déclarative de vue composée de métriques — mutuellement exclusive avec `viewSql` |
| `dqContract` | Texte de contrat de qualité des données YAML/JSON |
| `queryTemplate` | Cypher pour une table Neo4j |
| `live` | Config de livraison en direct pour push SSE/Kafka |
| `discover` | Inférer les colonnes depuis la source en direct au moment de l'enregistrement |

### `RelationshipInput`

| Champ | Notes |
|-------|-------|
| `id` | Identifiant de relation |
| `sourceTableId` | Nom de table virtuelle (alias si défini, sinon nom de table) |
| `targetTableId` | Nom de table virtuelle ; vide pour les relations calculées |
| `sourceColumn` | Colonne de jointure du côté source |
| `targetColumn` | Colonne de jointure du côté cible |
| `cardinality` | `one-to-one` \| `one-to-many` \| `many-to-one` \| `many-to-many` |
| `alias` | Étiquette d'arête Cypher (ex. `WORKS_FOR`) |
| `graphqlAlias` | Nom de champ GraphQL sur le type source |
| `viaTable` | Nom de la table de jonction pour les arêtes plusieurs-à-plusieurs |
| `recordCandidate` | Écrire aussi une ligne relationship_candidates avec le statut `accepted` |

---

## Exemple : enregistrement d'une table {: #example-registering-a-table }

```graphql
mutation {
  registerTable(input: {
    sourceId: "sales-pg"
    domainId: "sales"
    schemaName: "public"
    tableName: "orders"
    alias: "orders"
    columns: [
      { name: "id", visibleTo: ["public"], isPrimaryKey: true }
      { name: "customer_id", visibleTo: ["public"] }
      { name: "total", visibleTo: ["public"] }
    ]
  }) {
    success
    message
    code
  }
}
```

## Exemple : création d'une règle RLS {: #example-creating-an-rls-rule }

```graphql
mutation {
  upsertRlsRule(input: {
    tableId: "orders"
    roleId: "regional-analyst"
    filterExpr: "region = '{{user.region}}'"
  }) {
    success
    message
  }
}
```
