# Référence de l'API GraphQL admin

L'API GraphQL admin est le plan de configuration de Provisa. C'est l'API que l'application web admin appelle pour chaque opération de gestion — créer des sources, enregistrer des tables, définir des relations, configurer des règles RLS, et tout ce qui façonne le modèle.

**Point de montage :** `POST /admin/graphql`

Ce n'est pas la même API que le plan de données à `/data/graphql`. Le plan de données sert les requêtes des utilisateurs finaux sur les domaines enregistrés et est décrit par le SDL à `/data/sdl`. L'API admin configure à quoi ce schéma ressemble et qui peut voir quoi.

---

## Comment l'interface communique avec cette API

L'application web admin utilise Apollo Client, pointé vers `${API_BASE}/admin/graphql`. [tool-verified: `provisa-ui/src/apolloClient.ts:19`]

Chaque requête porte un jeton bearer (récupéré à nouveau auprès du fournisseur d'authentification à chaque appel), un en-tête `X-Org-Id` en mode multilocataire, et un en-tête `X-Env` lors du service d'un environnement de branche. [tool-verified: `provisa-ui/src/apolloClient.ts:24-42`]

Le schéma est assemblé à partir de deux classes `@strawberry.type` — `Query` depuis `schema_query.py` et `Mutation` depuis `schema_mutation.py` — et enveloppé dans une `ModelCommitExtension` qui enregistre chaque mutation contre la branche d'environnement courante (REQ-1524). [tool-verified: `provisa/api/admin/schema.py:44`]

---

## Autorisation

**Mode dev :** lorsqu'aucune authentification n'est configurée et que chaque requête arrive comme un principal anonyme, toutes les vérifications de capacité sont ignorées. Cela garde une installation locale fonctionnelle sans configuration d'authentification. [tool-verified: `provisa/api/admin/capabilities.py:98-99`]

**Portes de capacité :** les déploiements en production imposent des capacités nommées. Le droit spécifique requis par chaque champ est noté en ligne. Appeler une mutation sans la capacité requise déclenche une `PermissionError`. Le rôle d'administrateur de plateforme contourne toutes les vérifications de capacité (REQ-1297). [tool-verified: `provisa/api/admin/capabilities.py:80-110`]

**Portes de domaine :** plusieurs mutations vérifient également le domaine auquel l'objet appartient. Un appelant limité à `sales` ne peut pas enregistrer une table dans `finance`, mettre en file d'attente une règle RLS pour celle-ci, ou créer une relation dont la table source se trouve dans un domaine qu'il ne détient pas (REQ-1530, REQ-1531). Les vues sont davantage contraintes : chaque table que le SQL de la vue lit doit se trouver dans les domaines de l'appelant, car le SQL en écriture libre donnerait sinon à un membre un accès à des données hors de sa portée. [tool-verified: `provisa/api/admin/domain_guard.py:1-133`]

**Héritage de rôle :** les capacités d'un rôle parent sont héritées par les rôles enfants (REQ-1677). `createRole` et `deleteRole` refusent les cycles et empêchent de supprimer un rôle qui a des héritiers.

---

## Type de retour commun

La plupart des mutations renvoient `MutationResult`. [tool-verified: `provisa/api/admin/types.py:1181-1188`]

```graphql
type MutationResult {
  success: Boolean!
  message: String!
  code: String          # stable i18n key, e.g. "schema.source_created"
  params: JSON          # key/value pairs for client-side localization (REQ-1350)
}
```

Lorsqu'une mutation échoue, `success` vaut `false` et `message` porte la raison en anglais. `code` est un identifiant stable que l'interface utilise pour afficher un message localisé.

---

## Requêtes

### Sources

#### `sources → [SourceType!]!`

Toutes les sources de données enregistrées. [tool-verified: `provisa/api/admin/schema_query.py:327-331`]

```graphql
query {
  sources {
    id type host port database username dialect
    cacheEnabled cacheTtl preferMaterialized
    loadProtected offPeakWindow offPeakTz
    gqlNamingConvention path allowedDomains
    description mappingJson federationHintsJson
    changeSignal passwordRef
    cdc { bootstrapServers topicPrefix schemaRegistryUrl consumerGroupId }
  }
}
```

`passwordRef` est une référence `${secret:NAME}` dans le coffre-fort de l'organisation — jamais l'identifiant en clair. [tool-verified: `provisa/api/admin/types.py:105`]

#### `source(id: String!) → SourceType`

Une seule source par ID. Renvoie `null` si introuvable. [tool-verified: `provisa/api/admin/schema_query.py:334-339`]

#### `availableSchemas(sourceId: String!) → [String!]!`

Schémas visibles dans une source, filtrés pour exclure ceux internes à Provisa. Utilise d'abord l'introspection native ; retombe sur le catalogue du moteur lorsque le type de source n'a pas de pool direct. [tool-verified: `provisa/api/admin/schema_query.py:628-667`]

#### `availableTables(sourceId: String!, schemaName: String = "public") → [AvailableTableType!]!`

Tables dans un schéma d'une source, avec leurs commentaires. Pour les sources OpenAPI, renvoie les opérations GET dont la réponse est un tableau ou un wrapper de pagination. Pour les sources GraphQL, renvoie les champs de requête renvoyant une liste. Pour gRPC, renvoie les RPC en streaming serveur. [tool-verified: `provisa/api/admin/schema_query.py:669-731`]

#### `availableColumns(sourceId: String!, schemaName: String!, tableName: String!) → [String!]!`

Noms de colonnes pour une table dans le catalogue du moteur. Pour les sources govdata, utilise un résolveur séparé. [tool-verified: `provisa/api/admin/schema_query.py:845-866`]

#### `availableColumnsMetadata(sourceId: String!, schemaName: String!, tableName: String!) → [AvailableColumnType!]!`

Noms de colonnes avec types de données, commentaires, types de filtre natifs, et indicateurs de clé primaire. Pour les sources OpenAPI, dérive la forme du schéma de réponse et des paramètres de l'opération. [tool-verified: `provisa/api/admin/schema_query.py:869-876`]

#### `availableFunctions(sourceId: String!, schemaName: String = "openapi") → [AvailableTableType!]!`

Opérations non-GET pour une source OpenAPI (POST, PUT, PATCH, DELETE). Renvoie une liste vide pour les sources non-OpenAPI. [tool-verified: `provisa/api/admin/schema_query.py:822-843`]

#### `crawlSource(path, depth, pattern, recursive, simpleLinks, sameDomain, excludePattern) → CrawlResultType`

Prévisualiser ce qu'une exploration de connecteur de fichiers découvrirait — fichiers, tables et colonnes — avant qu'une source ne soit créée. Les paramètres HTTP uniquement (`simpleLinks`, `sameDomain`, `excludePattern`) sont ignorés pour les racines locales, S3, FTP et SFTP. (REQ-1785) [tool-verified: `provisa/api/admin/schema_query.py:733-790`]

#### `suggestTableAlias(tableName: String!, domainId: String!, sourceId: String!) → String!`

Renvoie l'alias à utiliser lors de l'enregistrement de `tableName` dans `domainId` depuis `sourceId`. Renvoie un alias simple en snake-case en l'absence de conflit, ou un alias préfixé par la source (`sqlite_b_orders`) lorsque le nom effectif est déjà pris par une source différente dans le même domaine. [tool-verified: `provisa/api/admin/schema_query.py:879-922`]

---

### Tables

#### `tables → [RegisteredTableType!]!`

Toutes les tables enregistrées, chacune avec sa liste complète de colonnes. La visibilité des colonnes dans la réponse respecte la capacité `table_registration` de l'appelant — `canDeployToDb` est conditionné à la détention de ce droit par l'appelant. (REQ-016, REQ-021, REQ-042) [tool-verified: `provisa/api/admin/schema_query.py:502-536`]

Chaque `RegisteredTableType` expose des sous-champs calculés :

- **`refreshPolicySummary → RefreshPolicySummaryType`** — la politique effective de rafraîchissement/service en texte brut, dérivée côté serveur de la même résolution du planificateur que celle utilisée par le moteur. Renvoie `null` pendant le démarrage. (REQ-1143) [tool-verified: `provisa/api/admin/types.py:319-327`]
- **`graphqlFieldName → String`** — le nom de champ que cette table porte dans le schéma compilé du plan de données, afin que le panneau Data Product puisse construire un exemple exécutable sans reproduire l'algorithme de nommage. (REQ-1634) [tool-verified: `provisa/api/admin/types.py:330-339`]
- **`dqDataset → String`** — cette table en tant que jeu de données de contrat de qualité des données, sous la forme que le vérificateur scanne. (REQ-1443) [tool-verified: `provisa/api/admin/types.py:374-387`]
- **`productId → String`** — le produit de données auquel cette table appartient. Une table de vérificateur DQ hérite du produit de la table dont son contrat scanne. (REQ-1634) [tool-verified: `provisa/api/admin/types.py:342-372`]

#### `refreshPolicyPreview(...) → RefreshPolicySummaryType`

Prévisualiser le résumé effectif de rafraîchissement/service pour des paramètres de table *provisoires* (non enregistrés), afin que le résumé en haut du formulaire se mette à jour au fur et à mesure des changements de champs sans rien persister. Même dérivation que `refreshPolicySummary` ci-dessus. (REQ-1143) [tool-verified: `provisa/api/admin/schema_query.py:947-979`]

Arguments : `sourceId`, `domainId`, `schemaName`, `tableName`, `cacheTtl`, `preferMaterialized`, `loadProtected`, `offPeakWindow`, `offPeakTz`, `changeSignal`.

#### `columnDependents(tableId: String!, renamed: [String!], removed: [String!]) → [ColumnDependentsType!]!`

Artefacts qu'un renommage d'alias ou une suppression de colonne en attente casserait. Consultatif — l'interface admin l'affiche avant l'enregistrement et l'administrateur décide. Doit être appelé *avant* l'enregistrement, car les dépendants ont été écrits contre le nom exposé que la colonne porte actuellement. (REQ-1484) [tool-verified: `provisa/api/admin/schema_query.py:1301-1331`]

---

### Relations

#### `relationships → [RelationshipType!]!`

Toutes les relations définies par l'utilisateur (exclut les entrées `gql_auto__` auto-générées et les entrées synthétiques `meta:%` utilisées par le diagramme ERD). [tool-verified: `provisa/api/admin/schema_query.py:539-569`]

#### `allRelationships → [RelationshipType!]!`

Identique à `relationships`, mais inclut les entrées synthétiques `meta:%`. Utilisé par le diagramme ERD, qui doit montrer chaque arête, y compris les liens implicites `HAS_TABLE` entre les tables de données et le registre de métadonnées. [tool-verified: `provisa/api/admin/schema_query.py:572-601`]

Chaque `RelationshipType` expose :

- **`autoSuggested → Boolean`** — si la relation a été suggérée par l'analyse de clé étrangère (`id` commence par `fk__`). [tool-verified: `provisa/api/admin/types.py:523-525`]
- **`physicalName → String`** — le nom de la relation sur les plans SQL et gRPC (le paramètre `?include=`). Dérivé côté serveur ; les clients ne doivent pas translittérer l'alias GraphQL. (REQ-471, REQ-1417) [tool-verified: `provisa/api/admin/types.py:527-536`]

---

### Domaines, rôles et utilisateurs

#### `domains → [DomainType!]!`

Tous les domaines de la base de données locataire de l'organisation active. La base de données locataire est isolée au niveau du schéma, de sorte que la liste des domaines d'un org-admin ne contient que les lignes de son organisation. (REQ-021, REQ-042, REQ-1293) [tool-verified: `provisa/api/admin/schema_query.py:342-357`]

#### `roles → [RoleType!]!`

Rôles visibles par l'appelant. Un admin voit chaque rôle ; un non-admin ne voit que les rôles sans `org_id` ou appartenant à son organisation. (REQ-042, REQ-059, REQ-060, REQ-215) [tool-verified: `provisa/api/admin/schema_query.py:603-618`]

#### `resolveOwners(refs: [String!]!) → [UserSummaryType!]!`

Résout des ID de rôle ou d'utilisateur en utilisateurs individuels. Utilisé pour développer `DataProduct.ownerRole`, `Domain.steward`, et `Column.visibleTo` en une liste lisible par un humain. Les références inconnues sont renvoyées telles quelles afin que l'interface affiche l'ID brut plutôt que rien. [tool-verified: `provisa/api/admin/schema_query.py:388-444`]

---

### Règles RLS

#### `rlsRules → [RLSRuleType!]!`

Toutes les règles de sécurité au niveau des lignes. Le référentiel sous-jacent déchiffre `filterExpr` à la frontière. (REQ-041, REQ-402, REQ-686) [tool-verified: `provisa/api/admin/schema_query.py:621-625`]

---

### Produits de données

#### `dataProducts → [DataProductType!]!`

Tous les produits de données. Requiert la capacité `data_product_read`. (REQ-1634) [tool-verified: `provisa/api/admin/schema_query.py:360-386`]

---

### Étiquettes

#### `tags → [TagType!]!`

Toutes les définitions d'étiquettes, y compris leurs valeurs de paramètre autorisées par étiquette. (REQ-1373, REQ-1467) [tool-verified: `provisa/api/admin/schema_query.py:447-475`]

#### `tagAssignments → [TagAssignmentType!]!`

Toutes les affectations d'étiquettes à travers sources, tables, colonnes et relations. (REQ-1377) [tool-verified: `provisa/api/admin/schema_query.py:478-499`]

---

### Vues matérialisées

#### `mvList → [MVType!]!`

Toutes les vues matérialisées avec leur statut d'exécution : activée/désactivée, horodatage du dernier rafraîchissement, nombre de lignes, et dernière erreur. [tool-verified: `provisa/api/admin/schema_query.py:927-944`]

---

### Cache

#### `cacheStats → CacheStatsType`

Statistiques de cache. Renvoie `storeType: "redis"` avec des métriques opérationnelles complètes lorsque Redis est configuré, `storeType: "memory"` pour le magasin fakeredis embarqué, et `storeType: "noop"` lorsqu'aucun cache n'est configuré. [tool-verified: `provisa/api/admin/schema_query.py:1106-1140`]

#### `cacheTableStats → [CacheTableStatType!]!`

Nombre d'entrées en cache par table. Vide lorsqu'aucun magasin de cache n'est configuré. [tool-verified: `provisa/api/admin/schema_query.py:1143-1148`]

#### `hotTables → [HotTableStatType!]!`

Tables dont Provisa conserve une copie, sur deux niveaux : `hot` (répliqué dans le magasin de réponses pour l'intégration en ligne des JOIN) et `warm` (déposé comme copie Iceberg). Une table est au plus dans un seul niveau (REQ-241). [tool-verified: `provisa/api/admin/schema_query.py:1151-1175`]

#### `materializeStoreInfo → MaterializeStoreInfoType`

Identité du magasin de matérialisation durable : nom du moteur, référence DSN du magasin, nombre de MV, et si le magasin est local à l'instance (un fichier local comme DuckDB ou SQLite, ce qui signifie que chaque instance derrière un équilibreur de charge conserve sa propre copie). [tool-verified: `provisa/api/admin/schema_query.py:1178-1189`]

---

### Santé système

#### `systemHealth → SystemHealthType`

Statut de connexion du moteur, nombres de pools de workers, état du pool de la base de données de métadonnées, mode de cache, et vivacité de chaque écouteur de protocole (pgwire, gRPC, Arrow Flight, Bolt). [tool-verified: `provisa/api/admin/schema_query.py:1194-1198`]

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

### Tâches planifiées

#### `scheduledTasks → [ScheduledTaskType!]!`

Déclencheurs planifiés depuis la config avec leur état d'exécution. Chaque entrée porte son expression cron, son `kind` (`webhook` ou `sql`), si elle est actuellement activée, l'horodatage de la dernière exécution (toujours `null` dans cette version — suivi par le planificateur), et la prochaine heure d'exécution planifiée depuis APScheduler. [tool-verified: `provisa/api/admin/schema_query.py:1203-1245`]

---

### Qualité des données

#### `dqContractParse(checker: String!, contractText: String!) → DqContractType`

Analyser du texte de contrat brut en lignes modifiables du panneau constructeur. Appelé à chaque édition ; un échec d'analyse revient comme `error` plutôt que comme une erreur GraphQL, car un texte à moitié écrit est normal pendant que l'opérateur tape. (REQ-1443 clause 7) [tool-verified: `provisa/api/admin/schema_query.py:1007-1022`]

#### `dqCheckCatalog(checker: String!, dataset: String!) → DqCheckCatalogType`

Les vérifications que `checker` propose, limitées aux colonnes de `dataset`. Le jeu de données est la cible observée du contrat, résolue de la même manière que le scanneur la résout — de sorte que les vérifications proposées correspondent aux colonnes que le vérificateur verra réellement. (REQ-1443 clause 7) [tool-verified: `provisa/api/admin/schema_query.py:1025-1050`]

#### `dqCheckDefinition(checker: String!, check: DqCheckBuildInput!) → DqCheckDefinitionType`

Le texte d'une vérification depuis les éditeurs du panneau. Côté serveur car le dialecte a une seule implémentation ; une vérification créée par le constructeur et une écrite à la main doivent être indiscernables. (REQ-1443 clause 7) [tool-verified: `provisa/api/admin/schema_query.py:1053-1075`]

#### `dqContractBuild(checker: String!, dataset: String!, checks: [DqCheckInput!]!) → DqContractTextType`

Sérialiser les lignes de vérification modifiées en texte de contrat. L'inverse de `dqContractParse`. Côté serveur pour la même raison : le panneau ne peut pas émettre un texte que le vérificateur refuserait. (REQ-1443 clause 7) [tool-verified: `provisa/api/admin/schema_query.py:1078-1101`]

---

### Aperçus de source

#### `neo4jPreview(sourceId: String!, cypher: String!) → QueryPreviewType`

Prévisualiser une projection Cypher sur une source Neo4j : jusqu'à cinq lignes et les types de colonnes que l'enregistrement portera. Les échecs reviennent comme `error`. (REQ-1670) [tool-verified: `provisa/api/admin/schema_query.py:984-992`]

#### `sparqlPreview(sourceId: String!, query: String!) → QueryPreviewType`

Prévisualiser un SELECT SPARQL sur une source SPARQL : jusqu'à cinq lignes, toutes les colonnes en texte. (REQ-1683) [tool-verified: `provisa/api/admin/schema_query.py:995-1002`]

---

### Kaggle

#### `kaggleTokenValid(token: String!) → Boolean!`

Vérification en direct contre l'API Kaggle. Renvoie `true` seulement lorsque le jeton s'authentifie. Alimente l'étape de contrôle du jeton dans le formulaire de source Kaggle. (REQ-1783) [tool-verified: `provisa/api/admin/schema_query.py:793-799`]

#### `kaggleDatasets(token: String!, query: String = "", page: Int = 1) → [KaggleDatasetType!]!`

Rechercher dans l'ensemble du catalogue de jeux de données publics de Kaggle. Contrôlé par jeton. (REQ-1783) [tool-verified: `provisa/api/admin/schema_query.py:802-819`]

---

### Calendriers

#### `calendars → [CalendarType!]!`

Toutes les versions de calendrier de limite d'instantané enregistrées. Alimente le sélecteur de config de planification d'instantané et confirme quels calendriers une MV périodique peut référencer. (REQ-962) [tool-verified: `provisa/api/admin/schema_query.py:218-242`]

---

### Métriques

#### `metrics → [MetricType!]!`

Toutes les définitions de métriques gouvernées. Les métriques dérivées de faits portent `fromFact`. (REQ-1317, REQ-1320) [tool-verified: `provisa/api/admin/schema_query.py:245-264`]

---

### Version du schéma

#### `schemaVersion → String!`

Hash SHA-256 de l'état actuel du schéma (domaines, ID de table, ID de relation). Le client Apollo lit cette valeur depuis l'en-tête de réponse `X-Schema-Version` et relance toutes les requêtes actives lorsqu'elle avance. [tool-verified: `provisa/api/admin/schema_query.py:291-324`]

---

### Assistants IA

#### `generateTableDescription(tableId: String!) → String!`

Utiliser le LLM configuré pour générer une description d'une à deux phrases pour une table enregistrée. Enregistrez d'abord la table ; appeler ceci sur une table non enregistrée renvoie un message d'instruction. [tool-verified: `provisa/api/admin/schema_query.py:1250-1298`]

#### `generateColumnDescription(tableId: String!, columnName: String!) → String!`

Utiliser le LLM configuré pour générer une description d'une phrase pour une seule colonne. [tool-verified: `provisa/api/admin/schema_query.py:1334-1383`]

---

### Demandes de création

#### `creationRequests → [CreationRequestType!]!`

Demandes de création en attente, visibles par les appelants détenant la capacité de création concernée. Utilisé lorsqu'un membre sans `create_relationship` ou `create_view` soumet une demande qu'un titulaire des droits doit approuver. (REQ-434, REQ-063) [tool-verified: `provisa/api/admin/schema_query.py:267-289`]

---

## Mutations

### Sources

#### `createSource(input: SourceInput!) → MutationResult`

Enregistrer une nouvelle source de données. Valide la connexion avant la persistance — une source rejetée ne laisse aucune entrée de coffre-fort derrière elle. Stocke les identifiants dans le coffre-fort de l'organisation et enregistre la référence ; le texte en clair n'atterrit jamais dans la base de données. (REQ-012, REQ-013) Requiert la capacité `source_registration`. [tool-verified: `provisa/api/admin/schema_mutation.py:616-781`]

#### `updateSource(input: SourceInput!) → MutationResult`

Mettre à jour les détails de connexion, la description et la config d'une source existante. Démonte et rattache le point de terminaison pgwire pour les sources fichier/SharePoint afin qu'un changement de chemin prenne effet immédiatement. Requiert `source_registration`. [tool-verified: `provisa/api/admin/schema_mutation.py:922-1073`]

#### `deleteSource(id: String!) → MutationResult`

Supprimer une source et son entrée de coffre-fort. Supprime le catalogue du moteur et reconstruit les schémas. [tool-verified: `provisa/api/admin/schema_mutation.py:1101-1132`]

#### `renameSource(oldId: String!, newId: String!) → MutationResult`

Renommer l'ID d'une source. [tool-verified: `provisa/api/admin/schema_mutation.py:1076-1098`]

#### `updateSourceCache(sourceId: String!, cacheEnabled: Boolean!, cacheTtl: Int) → MutationResult`

Activer ou désactiver la mise en cache des résultats de requête pour une source, et définir le TTL en secondes. [tool-verified: `provisa/api/admin/schema_mutation.py:2327-2350`]

#### `updateSourcePreferMaterialized(sourceId: String!, preferMaterialized: Boolean!) → MutationResult`

Forcer (ou libérer) la fédération matérialisée pour toutes les tables d'une source. (REQ-826) [tool-verified: `provisa/api/admin/schema_mutation.py:2379-2402`]

#### `updateSourceLoadProtection(sourceId, loadProtected, offPeakWindow, offPeakTz) → MutationResult`

Marquer une source comme protégée en charge (rafraîchissement planifié uniquement). Requiert au moins une porte — fenêtre hors pointe, cadence de TTL de cache, ou un signal de changement par sondage — sinon l'appel échoue. (REQ-1141) [tool-verified: `provisa/api/admin/schema_mutation.py:2431-2480`]

#### `updateSourceNaming(sourceId: String!, gqlNamingConvention: String) → MutationResult`

Définir la convention de nommage GraphQL par source. [tool-verified: `provisa/api/admin/schema_mutation.py:2595-2619`]

#### `updateSourceAllowedDomains(sourceId: String!, allowedDomains: [String!]!) → MutationResult`

Définir quels domaines peuvent utiliser une source (liste vide = sans restriction). Requiert `source_registration`. (REQ-1531) [tool-verified: `provisa/api/admin/schema_mutation.py:2622-2658`]

#### `stageKaggleDataset(token, owner, ref, idPrefix) → KaggleStageResultType`

Télécharger et décompresser un jeu de données Kaggle sur le disque local. Renvoie le chemin du répertoire mis en zone de transit ; l'appelant crée ensuite une source de type `files` pointant vers celui-ci. Les paquets contenant SQLite sont rejetés dans leur intégralité. Requiert `source_registration`. (REQ-1780–1782) [tool-verified: `provisa/api/admin/schema_mutation.py:784-824`]

#### `refreshKaggleSource(sourceId: String!, token: String!) → MutationResult`

Récupérer à nouveau sur place le jeu de données d'une source dérivée de Kaggle. Ignore le téléchargement si Kaggle n'a rien de plus récent que ce qui est sur le disque. Requiert `source_registration`. (REQ-1787) [tool-verified: `provisa/api/admin/schema_mutation.py:827-920`]

#### `refreshSourceStatistics(sourceId: String!) → MutationResult`

Exécuter `ANALYZE` sur toutes les tables enregistrées d'une source. Améliore les décisions d'ordre de jointure et de diffusion pour les requêtes fédérées. (REQ-276) [tool-verified: `provisa/api/admin/schema_mutation.py:2944-3008`]

---

### Tables

#### `registerTable(input: TableInput!) → MutationResult`

Enregistrer une nouvelle table (ou vue) dans un domaine. Requiert la capacité `table_registration` et l'appartenance au domaine cible. Un appelant sans `create_relationship` qui soumet une vue est mis en file d'attente comme demande de création pour qu'un titulaire des droits l'approuve. (REQ-013, REQ-016, REQ-252, REQ-434) [tool-verified: `provisa/api/admin/schema_mutation.py:1714-1718`]

#### `updateTable(input: TableInput!) → MutationResult`

Mettre à jour l'alias, la description, les métadonnées de colonnes, les paramètres de MV, et la config de livraison en direct d'une table existante. (REQ-016, REQ-020) Requiert `table_registration`. [tool-verified: `provisa/api/admin/schema_mutation.py:1858-1995`]

#### `deleteTable(id: Int!) → MutationResult`

Supprimer une table enregistrée. Recherche le domaine de la table pour la porte de domaine avant la suppression. (REQ-1531) [tool-verified: `provisa/api/admin/schema_mutation.py:1998-2029`]

#### `updateTableCache(tableId: Int!, cacheTtl: Int) → MutationResult`

Remplacer le TTL de cache pour une table. [tool-verified: `provisa/api/admin/schema_mutation.py:2353-2376`]

#### `updateTablePreferMaterialized(tableId: Int!, preferMaterialized: Boolean) → MutationResult`

Remplacer la fédération matérialisée pour une table. `null` = hériter du défaut de la source. (REQ-826) [tool-verified: `provisa/api/admin/schema_mutation.py:2405-2428`]

#### `updateTableLoadProtection(tableId, loadProtected, offPeakWindow, offPeakTz) → MutationResult`

Remplacer la protection de charge pour une table. `null` pour `loadProtected` hérite du défaut de la source. Valide la combinaison de porte effective (table → source). (REQ-1141) [tool-verified: `provisa/api/admin/schema_mutation.py:2483-2566`]

#### `updateTableNaming(tableId: Int!, gqlNamingConvention: String) → MutationResult`

Définir la convention de nommage GraphQL par table. [tool-verified: `provisa/api/admin/schema_mutation.py:2661-2685`]

#### `deployViewToDb(tableId: Int!) → MutationResult`

Promouvoir une vue virtuelle Provisa en une vraie vue de base de données sur sa source native sous-jacente. [tool-verified: `provisa/api/admin/schema_mutation.py:3058-3060`]

#### `forceRegen(tableId: Int!, reason: String!) → MutationResult`

Recalculer à la demande les lignes déposées d'une table, en contournant la porte de changement normale. `reason` est une annotation d'audit requise. Refusé pour les tables fédérées en direct (pas de lignes déposées). (REQ-968) [tool-verified: `provisa/api/admin/schema_mutation.py:2689-2782`]

#### `invalidateFileSource(tableId: Int!) → MutationResult`

Forcer le prochain accès d'une table de connecteur de fichiers SQLite à se resynchroniser depuis le disque. [tool-verified: `provisa/api/admin/schema_mutation.py:2877-2880`]

#### `registerEntity(input: EntityInput!) → MutationResult`

Sucre syntaxique pour enregistrer une entité dimension/hub. Se réduit à une MV (bitemporelle, si historisée) et appelle `registerTable`. (REQ-1164) [tool-verified: `provisa/api/admin/schema_mutation.py:1721-1725`]

#### `registerFact(input: FactInput!) → MutationResult`

Sucre syntaxique pour enregistrer un fait de schéma en étoile. Se réduit à une MV d'agrégation, crée les relations de dimension, et auto-enregistre les mesures de fait comme métriques gouvernées. (REQ-1164, REQ-1320) [tool-verified: `provisa/api/admin/schema_mutation.py:1728-1776`]

---

### Relations

#### `upsertRelationship(input: RelationshipInput!) → MutationResult`

Créer ou mettre à jour une relation. La porte vérifie le domaine de la table source (pas celui de la cible). Une arête inter-domaines est stockée avec `needsReview: true`. Un appelant sans `create_relationship` est mis en file d'attente comme demande de création. Les arêtes de jonction (plusieurs-à-plusieurs) requièrent des longueurs de liste de clés correspondantes. (REQ-019, REQ-020, REQ-366, REQ-434) [tool-verified: `provisa/api/admin/schema_mutation.py:2297-2300`]

#### `deleteRelationship(id: String!) → MutationResult`

Supprimer une relation par ID et reconstruire les schémas. [tool-verified: `provisa/api/admin/schema_mutation.py:2303-2322`]

---

### Domaines

#### `createDomain(input: DomainInput!) → MutationResult`

Créer un domaine. Les mots de segment réservés (`tables`, `relationships`, et autres segments de chemin URI) sont rejetés, tout comme le littéral générique `*`. Requiert la capacité `org_settings`. (REQ-021, REQ-1531) [tool-verified: `provisa/api/admin/schema_mutation.py:1135-1189`]

#### `deleteDomain(id: String!) → MutationResult`

Supprimer un domaine. Requiert `org_settings`. (REQ-1531) [tool-verified: `provisa/api/admin/schema_mutation.py:1192-1213`]

#### `updateGqlNamingConvention(convention: String!) → MutationResult`

Définir la convention de nommage GraphQL globale et reconstruire les schémas pour tous les rôles. Seuls les noms de convention reconnus sont acceptés. (REQ-253, REQ-416) [tool-verified: `provisa/api/admin/schema_mutation.py:2571-2592`]

---

### Rôles

#### `createRole(input: RoleInput!) → MutationResult`

Créer ou remplacer un rôle avec des capacités, un accès de domaine, des limites de débit optionnelles, et un rôle parent optionnel. Valide que le parent existe et que la chaîne parentale est exempte de cycles. Requiert `user_management`. (REQ-042, REQ-1531, REQ-1677) [tool-verified: `provisa/api/admin/schema_mutation.py:1657-1712`]

#### `deleteRole(id: String!) → MutationResult`

Supprimer un rôle. Échoue si d'autres rôles en héritent — reparentez-les d'abord. Requiert `user_management`. (REQ-1531, REQ-1677) [tool-verified: `provisa/api/admin/schema_mutation.py:2032-2063`]

---

### Règles RLS

#### `upsertRlsRule(input: RLSRuleInput!) → MutationResult`

Créer ou mettre à jour une règle de sécurité au niveau des lignes. L'expression de filtre est validée au moment de l'enregistrement contre les colonnes de la table ou du domaine cible, de sorte qu'une règle que l'admin ne peut pas interroger soit refusée avec la raison plutôt que d'échouer silencieusement au moment de la requête. Requiert `masking_config`. (REQ-041, REQ-402, REQ-1531, REQ-1676) [tool-verified: `provisa/api/admin/schema_mutation.py:2066-2136`]

Les cibles sont mutuellement exclusives : définissez `tableId` pour une règle au niveau table, `domainId` pour une règle au niveau domaine, ou `actionName` pour une fonction/webhook suivi. (REQ-1679)

#### `deleteRlsRule(roleId, tableId, domainId, actionName) → MutationResult`

Supprimer une règle RLS. Requiert `masking_config`. (REQ-1531) [tool-verified: `provisa/api/admin/schema_mutation.py:2139-2178`]

---

### Produits de données

#### `createDataProduct(input: DataProductInput!) → MutationResult`

Créer ou remplacer un produit de données. Requiert la capacité `data_product_rw`. (REQ-1634) [tool-verified: `provisa/api/admin/schema_mutation.py:1216-1259`]

#### `deleteDataProduct(id: String!) → MutationResult`

Supprimer un produit de données. Requiert `data_product_rw`. (REQ-1634) [tool-verified: `provisa/api/admin/schema_mutation.py:1262-1285`]

---

### Étiquettes

#### `upsertTag(input: TagInput!) → MutationResult`

Créer ou mettre à jour une définition d'étiquette. Les étiquettes système et les étiquettes dérivées ne peuvent pas être redéfinies. `appliesTo` doit être un sous-ensemble non vide de `["source", "table", "column", "relationship", "command"]`. (REQ-1373, REQ-1375) [tool-verified: `provisa/api/admin/schema_mutation.py:1288-1361`]

#### `deleteTag(id: String!) → MutationResult`

Supprimer une étiquette. Refuse les étiquettes système et dérivées. (REQ-1373) [tool-verified: `provisa/api/admin/schema_mutation.py:1364-1393`]

#### `assignTag(input: TagAssignmentInput!) → MutationResult`

Affecter une étiquette à une source, table, colonne, relation, ou commande. Applique les politiques de champ de l'étiquette (`reason_policy`, `expires_policy`) et — pour les étiquettes paramétrées — valide la valeur du paramètre contre la liste autorisée de l'étiquette. (REQ-1376, REQ-1377) [tool-verified: `provisa/api/admin/schema_mutation.py:1396-1527`]

#### `unassignTag(input: TagAssignmentInput!) → MutationResult`

Retirer une affectation d'étiquette. (REQ-1377) [tool-verified: `provisa/api/admin/schema_mutation.py:1530-1564`]

#### `upsertTagParamValue(input: TagParamValueInput!) → MutationResult`

Ajouter ou redécrire une valeur de paramètre autorisée pour une étiquette paramétrée. La liste des valeurs autorisées est fermée : chaque affectation doit nommer une valeur de cette liste. (REQ-1467) [tool-verified: `provisa/api/admin/schema_mutation.py:1567-1616`]

#### `deleteTagParamValue(tagId: String!, value: String!) → MutationResult`

Retirer une valeur autorisée. Refusé tant qu'une affectation la porte encore, car ces affectations nommeraient alors un type que la liste n'admet plus. (REQ-1467) [tool-verified: `provisa/api/admin/schema_mutation.py:1619-1655`]

---

### Métriques

#### `upsertMetric(input: MetricInput!) → MutationResult`

Créer ou remplacer une définition de métrique gouvernée. L'expression doit être analysable par sqlglot et contenir au moins une fonction d'agrégation. Régénère toutes les vues composées de métriques qui référencent cette métrique. Requiert `table_registration`. (REQ-1317, REQ-1318) [tool-verified: `provisa/api/admin/schema_mutation.py:1779-1831`]

#### `deleteMetric(name: String!) → MutationResult`

Supprimer une métrique gouvernée. Reconstruit les schémas. Requiert `table_registration`. (REQ-1317) [tool-verified: `provisa/api/admin/schema_mutation.py:1834-1856`]

---

### Calendriers

#### `createCalendar(input: CalendarInput!) → MutationResult`

Créer ou remplacer un calendrier de limite d'instantané versionné. Validé en construisant le `Calendar` en mémoire avant la persistance — échoue sur un système de base inconnu, un fuseau horaire incorrect, ou un ancrage fiscal incorrect. (REQ-962) [tool-verified: `provisa/api/admin/schema_mutation.py:525-579`]

#### `deleteCalendar(name: String!) → MutationResult`

Supprimer un calendrier (toutes versions). Refusé lorsqu'une vue matérialisée le référence. (REQ-962) [tool-verified: `provisa/api/admin/schema_mutation.py:582-613`]

---

### Vues matérialisées

#### `refreshMv(mvId: String!) → MutationResult`

Déclencher un rafraîchissement manuel d'une vue matérialisée. Coordonne à travers la flotte lorsque le mode de cohérence de la MV est `shared`. (REQ-133, REQ-158, REQ-879) [tool-verified: `provisa/api/admin/schema_mutation.py:2787-2813`]

#### `toggleMv(mvId: String!, enabled: Boolean!) → MutationResult`

Activer ou désactiver une vue matérialisée. [tool-verified: `provisa/api/admin/schema_mutation.py:2816-2839`]

---

### Cache

#### `purgeCache → MutationResult`

Purger tous les résultats de requête mis en cache. [tool-verified: `provisa/api/admin/schema_mutation.py:2844-2858`]

#### `purgeCacheByTable(tableId: Int!) → MutationResult`

Purger les résultats mis en cache pour une table. [tool-verified: `provisa/api/admin/schema_mutation.py:2861-2875`]

---

### Tâches planifiées

#### `createScheduledTask(id, name, cron, kind, webhookName, argsJson, sql) → MutationResult`

Créer un déclencheur planifié — soit un appel webhook, soit une instruction SQL — et l'enregistrer en direct dans APScheduler. `kind` vaut `"webhook"` ou `"sql"`. (REQ-1003, REQ-1004) [tool-verified: `provisa/api/admin/schema_mutation.py:2923-2936`]

#### `deleteScheduledTask(taskId: String!) → MutationResult`

Retirer un déclencheur planifié de la config et du planificateur en direct. (REQ-1003) [tool-verified: `provisa/api/admin/schema_mutation.py:2939-2941`]

#### `toggleScheduledTask(taskId: String!, enabled: Boolean!) → MutationResult`

Activer ou désactiver une tâche planifiée dans le fichier de config. [tool-verified: `provisa/api/admin/schema_mutation.py:2885-2920`]

---

### Qualité des données

#### `dryRunDqContract(sourceId: String!, contractText: String!) → DqDryRunType`

Exécuter un contrat contre la table en direct et renvoyer les résultats sans rien déposer. Une mutation plutôt qu'une requête car cela coûte un vrai scan. Ce qu'elle prouve, c'est si l'identifiant du jeu de données résout vers la table gouvernée que l'opérateur vise. (REQ-1443 clause 7) [tool-verified: `provisa/api/admin/schema_mutation.py:463-487`]

#### `runDqCheckNow(schemaName: String!, tableName: String!) → MutationResult`

Déclencher immédiatement le job de sondage d'une table de vérificateur. Dépose les lignes de la manière normale, de sorte que les résultats persistent et que l'historique DQ montre le nouveau scan. (REQ-1443) [tool-verified: `provisa/api/admin/schema_mutation.py:490-522`]

---

### Maintenance du schéma

#### `rebuildSchemas → MutationResult`

Reconstruire le schéma en mémoire à partir de l'état de la base de données. Utile après des changements externes de base de données. [tool-verified: `provisa/api/admin/schema_mutation.py:456-461`]

---

### Demandes de création

#### `executeCreationRequest(requestId: Int!) → MutationResult`

Un titulaire des droits exécute une demande de création en file d'attente — relation, vue, ou webhook. Requiert la capacité que la demande attend. (REQ-434, REQ-063) [tool-verified: `provisa/api/admin/schema_mutation.py:2181-2255`]

#### `rejectCreationRequest(requestId: Int!, reason: String!) → MutationResult`

Rejeter une demande en file d'attente avec une raison exploitable. `reason` est requis. (REQ-434, REQ-063) [tool-verified: `provisa/api/admin/schema_mutation.py:2258-2294`]

---

### Compilation de requête

#### `compileQuery(input: CompileQueryInput!) → [CompileQueryResult!]!`

Compiler une requête GraphQL du plan de données contre le schéma d'un rôle et renvoyer la décision de routage complète : SQL sémantique, SQL moteur, SQL direct, route, métadonnées d'application (filtres RLS appliqués, colonnes exclues, masquage appliqué), et Cypher compilé. Renvoie un résultat par champ racine de la requête. (REQ-161) [tool-verified: `provisa/api/admin/schema_mutation.py:3011-3055`]

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

## Types d'entrée clés

### `SourceInput`

[tool-verified: `provisa/api/admin/types.py:590-611`]

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
| `changeSignal` | `String` | `ttl` \| `probe` \| `ttl_probe` (REQ-929) |
| `loadProtected` | `Boolean` | Rafraîchissement planifié uniquement (REQ-1141) |
| `offPeakWindow` | `String` | Fenêtre de maintenance `HH:MM-HH:MM` |
| `offPeakTz` | `String` | Fuseau horaire IANA |
| `cdc` | `SourceCdcConfigInput` | Config de transport CDC Kafka (REQ-824) |

### `TableInput`

[tool-verified: `provisa/api/admin/types.py:745-800`]

L'entrée principale d'enregistrement de table. Champs clés au-delà des bases :

| Champ | Notes |
|-------|-------|
| `materialize` | Déposer une copie dans le magasin de matérialisation |
| `mvRefreshInterval` | Secondes entre les rafraîchissements |
| `mvPersist` | `replace` \| `append` \| `upsert` (REQ-965) |
| `mvIncremental` | Maintenance incrémentale (REQ-969) |
| `mvBitemporalMode` | `snapshot` \| `delta` pour les tables bitemporelles (REQ-1162) |
| `mvCalendar` | Nom du calendrier d'instantané (REQ-962) |
| `mvGrain` | Granularité d'instantané : `daily`, `weekly`, `monthly`, `annual`, ou personnalisée `3WE` / `LFR` (REQ-962) |
| `viewSql` | SQL pour une vue dérivée |
| `viewMetrics` | Spécification déclarative de vue composée de métriques — mutuellement exclusive avec `viewSql` (REQ-1318) |
| `dqContract` | Texte de contrat de qualité des données YAML/JSON (REQ-1443) |
| `queryTemplate` | Cypher pour une table Neo4j (REQ-1670) |
| `live` | Config de livraison en direct pour push SSE/Kafka (REQ-565, REQ-813) |
| `discover` | Inférer les colonnes depuis la source en direct au moment de l'enregistrement (REQ-252) |

### `RelationshipInput`

[tool-verified: `provisa/api/admin/types.py:804-826`]

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
| `viaTable` | Nom de la table de jonction pour les arêtes plusieurs-à-plusieurs (REQ-1586) |
| `recordCandidate` | Écrire aussi une ligne relationship_candidates avec le statut `accepted` |

---

## Exemple : enregistrement d'une table

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

## Exemple : création d'une règle RLS

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
