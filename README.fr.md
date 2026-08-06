# Provisa

**Connectez vos bases de données. Interrogez-les avec GraphQL, gRPC, SQL ou MCP — sur n'importe quelle API ou protocole — en 5 minutes.**

Provisa expose chaque surface d'API (REST, GraphQL, SQL, gRPC, MCP et plus) sur le résultat joint de vos sources. Il en est capable parce qu'il s'agit d'une **couche sémantique active** : une définition unique de votre patrimoine de données — chaque domaine, relation et politique à travers vos sources, à l'exclusion des seuls systèmes d'origine eux-mêmes — qui à la fois exploite ce patrimoine et le gouverne. La définition n'est pas une documentation qu'un moteur pourrait consulter ; elle *est* le moteur. Les domaines et relations enregistrés sont les seuls chemins de jointure légaux, et les politiques d'accès sont compilées dans chaque plan de requête. Un modèle, trois fonctions :

- **Définir** — Les domaines, colonnes et relations sont déclarés une seule fois. Cette déclaration constitue le schéma que voit chaque consommateur et le seul ensemble de chemins de jointure qu'une requête peut emprunter.
- **Appliquer** — La sécurité au niveau des lignes, le masquage des colonnes, la visibilité des colonnes et l'approbation des requêtes sont appliqués en ligne sur le chemin d'exécution. Aucune requête n'atteint les données sans passer par ces contrôles, de sorte que la couverture est totale par construction, et non par diligence.
- **Auditer** — Comme chaque requête emprunte le même chemin gouverné, qui a interrogé quoi, sous quel rôle et selon quelle politique est enregistré de manière uniforme. Les traces distribuées, les métriques et les journaux sont eux-mêmes enregistrés comme des tables interrogeables au même titre que vos données métier.

Un seul noyau gouverné sert chaque langage et chaque transport. Interrogez avec **GraphQL, Cypher ou SQL** ; consommez via **pgwire, Bolt, gRPC, REST, Arrow Flight ou JDBC**. Chaque langage de requête se réduit à une seule représentation intermédiaire dans laquelle la gouvernance est injectée une seule fois — de sorte qu'une politique ne peut pas diverger d'un langage à l'autre — et cette IR se retraduit vers le dialecte natif de chaque source en sortie. Ajouter un langage revient à ajouter un nouveau frontal sur le noyau partagé, pas un nouveau moteur.

Le patrimoine est à la fois analytique et transactionnel. Les lectures inter-sources sont réparties via la couche de fédération ; les écritures et les lectures mono-source sont routées directement vers le pilote de la source — gouvernées de manière identique, mais transactionnelles et en moins de 100 ms. Le streaming columnaire Arrow Flight est intégré.

L'ensemble du modèle est construit à partir d'une poignée de primitives — domaines, relations, rôles et politiques. Vocabulaire restreint, ce qui rend la définition facile à comprendre et simple à évaluer et à auditer : vous pouvez lire l'ensemble des politiques et savoir ce qu'il fait. Provisa est un compilateur de requêtes léger, pas un runtime qui se place sur le chemin des données. Il convertit une requête en requêtes natives, les route, puis s'efface — c'est pourquoi le patrimoine performe.

Cette conception permet deux modes d'utilisation, qui ne s'excluent pas mutuellement :

- **Comme échafaudage de modernisation** — Modélisez votre patrimoine, laissez Provisa générer le SQL natif pour chaque source, puis capturez ce SQL et adoptez-le directement dans le système cible. Provisa est la couche de transition, pas une dépendance permanente.
- **Comme infrastructure permanente d'application des politiques** — Conservez-le en place comme le chemin gouverné que chaque requête emprunte, de sorte que la définition, l'application et l'audit restent unifiés aussi longtemps que le patrimoine existe.

## Le modèle de fédération

Tout le modèle se ramène à deux contrats et deux politiques : les sources se réduisent à des tables 2D sur un seul système de types, les requêtes se réduisent à une seule IR de type SQL, l'accessibilité détermine ce qui est interrogé en direct par rapport à ce qui est matérialisé, et une stratégie de fraîcheur gouverne chaque copie matérialisée et chaque jeu de données dérivé. Forme des données en entrée, forme des requêtes en entrée, gouvernance à la jointure, requêtes natives en sortie. Le reste de cette section détaille chaque élément.

Le modèle repose sur une réduction : chaque source est exprimée comme une collection de tables bidimensionnelles sur un système de types unique et généralisé. C'est le contrat qu'une source doit respecter pour rejoindre le patrimoine, et c'est le même contrat pour toutes. Certaines sources s'y prêtent déjà — une table MySQL ou PostgreSQL *est* une relation 2D typée. Certaines s'y prêtent avec une projection : un résultat de requête GraphQL, une fois aplati, est une table. Certaines sont étrangères à cette forme — les triplestores SPARQL, Neo4j — mais restent exploitables, car l'utilisateur fournit une requête dont le jeu de résultats est tabulaire ; la requête est l'adaptateur. Quelle que soit la source, le patrimoine voit des lignes, des colonnes et des types généralisés, et rien d'autre. Intégrer un nouveau type de source consiste à satisfaire ce seul contrat, parfois avec une étape d'intervention humaine, et non à écrire une intégration sur mesure.

Cette réduction a un pendant côté requêtes. SQL — à travers tous ses dialectes et particularités — est essentiellement le langage d'analyse sur des jeux de données 2D, ce qui fait d'une forme de type SQL la cible universelle naturelle pour les requêtes. Ainsi, chaque requête, quel que soit le langage dans lequel elle arrive, est réduite à cette représentation intermédiaire dès sa première étape. Certaines se réduisent proprement — SQL lui-même, et même GraphQL ; certaines sont difficiles — la sémantique des chemins et des graphes de Cypher demande un travail réel — mais toutes sont faisables. Canaliser chaque requête vers une seule IR avant toute autre opération est ce qui permet à la gouvernance de s'appliquer en exactement un seul endroit, sur une seule forme, quel que soit le langage d'origine.

Au-dessus de ces deux formes uniformes — sources tabulaires et forme de requête unique — la fédération désigne ici à la fois la requête en direct et l'entreposage — la même étendue que couvre un moteur de requête en direct comme Trino, plus la matérialisation sur laquelle s'appuient de tels moteurs. Le concept qui les unifie est l'**accessibilité** : pour une source donnée, le moteur peut-il l'interroger sur place, ou ses données doivent-elles d'abord être matérialisées quelque part d'interrogeable ? L'accessibilité partitionne le patrimoine entre ce qui est interrogé en direct et ce qui est copié au préalable.

La plupart des bases de données disposent déjà d'une certaine notion de lien en direct — `ATTACH` de DuckDB, `postgres_fdw` de PostgreSQL, les liens externes de Databricks. La plupart des bases de données peuvent donc jouer, dans une certaine mesure, le rôle de moteur de fédération. Aucune n'est exhaustive : chacune atteint un ensemble particulier de sources et matérialise le reste, sans qu'aucun compte unique n'indique lequel est lequel. Le modèle comble cet écart en rendant l'accessibilité explicite — un ensemble défini de méthodes, par source, qui indiquent ce que le moteur peut atteindre en direct et, par élimination, ce qui doit être matérialisé.

Ce qu'il reste, c'est la fraîcheur : pour chaque source non accessible, à quel point sa copie matérialisée doit-elle être récente ? Dans la pratique, cela se ramène à un petit ensemble de stratégies — à la demande, selon un calendrier, selon un signal de changement (CDC, filigrane, instantané), ou figée. En choisir une par source constitue toute la politique de fraîcheur.

Les jeux de données analytiques — tables dérivées, agrégats, sorties d'une transformation — s'inscrivent dans la même forme. Eux aussi doivent être exprimés dans l'IR, et parce qu'ils le sont, la traçabilité n'est pas un système distinct à maintenir : le chemin de chaque système d'origine jusqu'à une sortie finale *est* l'IR qui l'a produite, lisible de bout en bout. Les construire soulève la question de la fraîcheur un cran plus loin — le jeu de données est-il actualisé selon un calendrier, seulement une fois ses conditions préalables remplies, en continu quasiment en temps réel, ou comme un instantané historique figé ? Les façons d'exprimer comment et quand construire un jeu de données forment le même petit vocabulaire énumérable, de sorte qu'un jeu de données dérivé porte une politique de construction dans exactement le même vocabulaire qu'une copie de source.

Les modèles dimensionnels en sont une application directe. Les tables de faits et de dimensions d'un schéma en étoile sont des jeux de données analytiques comme les autres — une dimension est une projection conforme et dédupliquée ; une table de faits est une jointure et un agrégat réduits à un grain — chacune portant sa propre politique de construction et de fraîcheur. Les dimensions à évolution lente ne nécessitent aucune mécanique particulière : un instantané figé constitue un historique de type 2, une reconstruction planifiée un type 1. Et parce que le schéma est défini dans l'IR plutôt que physiquement lié aux tables d'un seul entrepôt, les mêmes définitions de faits et de dimensions se retraduisent — matérialisées dans Oracle, dans Databricks, ou laissées virtuelles au-dessus d'un moteur MPP — sans remodélisation. Le modèle génère le schéma en étoile ; il ne l'enferme pas dans un moteur.

Data Vault s'inscrit de la même façon, une couche plus tôt. Ses hubs sont des jeux de données à clé métier dédupliqués, ses links sont les relations enregistrées entre eux, et ses satellites sont des jeux de données d'attributs en insertion seule, horodatés — l'historique. Un satellite n'est qu'un jeu de données dérivé selon la stratégie de fraîcheur par signal de changement : date de chargement plus hashdiff correspond à du CDC appliqué à des attributs descriptifs, et l'historique en insertion seule correspond à la stratégie d'instantané figé. Les tables point-in-time et bridge sont des jeux de données dérivés supplémentaires construits pour la performance des requêtes. Ainsi, un raw vault est un ensemble de jeux de données analytiques dans l'IR, et un schéma en étoile en est une projection — les deux sont générés, les deux sont portables entre moteurs. Ce que le modèle ne fait pas, c'est décider de la méthodologie : ce qui devient un hub, le grain d'un satellite, la stratégie de découpage. Ce sont des choix de modélisation qui restent à faire ; une fois pris, ils vivent comme de l'IR portable plutôt que comme de l'ETL soudé à un seul entrepôt.

Les deux motifs sont déclarés via **deux raccourcis de premier ordre** plutôt que des vues écrites à la main — les primitives à partir desquelles tout schéma en étoile et tout Data Vault sont construits, maintenues neutres vis-à-vis de la méthodologie :

- **`entity`** — une projection à clé, dédupliquée et éventuellement historisée d'une source. Déclarez une clé d'entité, les attributs et un mode d'historique ; Provisa la réduit en vue matérialisée, et lorsque l'historique est demandé, en **VM bitemporelle** (`scd2` → delta, `snapshot` → instantané). Une seule construction sert à la fois une **dimension** Kimball (SCD1/SCD2) et un **hub + satellite** Data Vault.
- **`fact`** — une jointure vers des clés d'entité, réduite à un grain déclaré, avec des mesures agrégées. Provisa la réduit en VM agrégée plus des relations enregistrées vers les entités. Une seule construction sert à la fois une **table de faits** en étoile et un **link** Data Vault (un fait sans mesure est un link pur d'ensemble de clés).

Comme la réduction est pure — une spécification `entity`/`fact` devient exactement les définitions de VM, de bitemporalité et de relation qu'un modélisateur écrirait autrement à la main — l'entrepôt est de l'IR de bout en bout et se retraduit entre moteurs sans remodélisation. Déclarez un entrepôt dans l'interface d'administration (un formulaire **Model** pour les entités et les faits) ou via l'API d'administration (`registerEntity` / `registerFact`) ; le modèle *génère* l'étoile Kimball ou le Data Vault, il n'en impose aucun.

### Voyage dans le temps

Le voyage dans le temps est une idée simple — conserver chaque version d'une ligne au lieu de l'écraser, afin de pouvoir demander ce qu'étaient les données à n'importe quel moment passé. Ce qui diffère, c'est l'efficacité avec laquelle chaque moteur peut le faire, ce qui explique précisément pourquoi Provisa en fait une propriété de la **définition** de la vue matérialisée plutôt que du moteur de stockage (REQ-1162). Déclarez-le une seule fois ; il fonctionne sur tout backend qui matérialise.

La règle qui préserve la portabilité est l'**ajout seul** (append-only) : une version, une fois écrite, n'est jamais mise à jour ni supprimée. Retirer une ligne en y inscrivant une date de « fin de validité » — le procédé bitemporel habituel — nécessite un UPDATE, que beaucoup de moteurs ne peuvent pas exécuter à moindre coût (voire pas du tout) sur un magasin fédéré, donc Provisa ne le fait pas. À la place, chaque actualisation **ajoute**, et « quelle version était en vigueur à l'instant T » se déduit à la lecture à partir du journal immuable. Il existe exactement deux façons d'ajouter :

- **Instantané (snapshot)** — ajoute l'intégralité du jeu de données actualisé, estampillée avec l'heure système de cette actualisation. Pas de comparaison de différences ; correct sur tout moteur ; le stockage croît d'une copie complète par actualisation.
- **Delta** — n'ajoute que ce qui a changé, plus des tombstones pour les clés supprimées. Le delta est **calculé par le moteur** (anti-jointures dans un `INSERT … SELECT`), jamais replié ligne par ligne dans Provisa. Plus compact, et nécessite une clé d'entité.

Le temps système (le moment où Provisa a enregistré une version) est géré de cette façon ; le temps de validité (le moment où un fait est vrai dans le métier) est fourni par le SELECT propre de la vue et préservé. Les moteurs qui offrent davantage — instantanés natifs Iceberg, un MERGE qui conserve moins de lignes — peuvent être ciblés pour l'efficacité derrière la même déclaration ; le chemin en ajout seul est le plancher garanti correct partout.

La lecture est transparente. Une simple requête sur une VM bitemporelle reconstruit par défaut l'état **actuel** à partir du journal d'ajout ; pour voyager dans le temps, envoyez un en-tête `X-Provisa-As-Of: <timestamp>` et toute la requête est traitée comme si le patrimoine était dans l'état où il se trouvait à ce moment — sémantique identique sur tout substrat. Activez-le pour n'importe quelle vue matérialisée dans l'interface d'administration (un contrôle **Time Travel** : off / snapshot / delta plus une clé d'entité) ou via l'API d'administration.

Accessibilité plus fraîcheur constitue un modèle général pour la fédération de données : une définition qui indique ce qui est en direct, ce qui est matérialisé, et à quel point chaque copie reste fraîche — indépendamment de la portée d'un moteur donné. Le résultat est la liberté vis-à-vis du verrouillage propriétaire. Le modèle est portable ; le patrimoine n'est pas captif du fournisseur dont la fédération atteint aujourd'hui le plus de sources.

## Fonctionnalités

### Interfaces de requête

Ce sont les langages et les API structurées dans lesquels vous écrivez des requêtes. Chacun possède sa propre syntaxe et sa propre sémantique ; la gouvernance (RLS, masquage, visibilité des colonnes, application des relations) s'applique de manière uniforme à tous, quel que soit le protocole de transport qui les achemine.

- **GraphQL** — Schémas par rôle avec visibilité au niveau des champs, filtrage, pagination par curseur et requêtes d'agrégation (`count`, `sum`, `avg`, `min`, `max`). Contraint par le schéma aux relations enregistrées — structurellement valide par construction, le chemin le plus rapide vers une requête simple correcte. Apollo APQ inclus : les requêtes sont hachées et enregistrées côté serveur ; les appels suivants n'envoient que le hash via HTTP GET, rendant les réponses cacheables par CDN sans aucun changement côté client. Les tables de référence en dessous d'un seuil de lignes configurable sont exposées comme des types énumérés.
- **SQL** — SQL complet sur des données fédérées ; non contraint et plus expressif que GraphQL. Écrivez du SQL standard — sous-requêtes corrélées comprises — et il s'exécute sur les sources sans changement. Les requêtes mono-source contournent entièrement la couche de fédération (moins de 100 ms).
- **Cypher** — Langage de requête de graphe sur le même schéma fédéré. Parcourez les relations comme des arêtes de graphe ; combinez des sources par union ; chemins de longueur variable. La gouvernance s'applique de façon identique à GraphQL et SQL.
- **API de modèle gRPC** — `.proto` généré automatiquement à partir du schéma enregistré ; RPC de requête et d'insertion typées par table, réponses en streaming. Piloté par le schéma au même sens que GraphQL — le modèle d'enregistrement est le contrat, protobuf en est l'encodage de transport. Contrairement à Arrow Flight (qui est un transport de streaming columnaire), il s'agit d'une interface de requête complète par table.
- **JSON:API** — API de requête structurée à `/data/jsonapi/{table}`, conçue exclusivement pour HTTP. Prend en charge JSON:API 1.1 : ensembles de champs partiels (`fields[table]=col1,col2`), expressions de filtre (`filter[field][op]=value`), documents composés (`include=relation`) et tri. Ce n'est pas un langage de requête généraliste — il interroge une table à la fois avec une syntaxe de filtre standardisée plutôt qu'une chaîne de requête ad hoc.
- **Explorateur de langages de requête** — Écrivez une requête GraphQL et visualisez en direct ses traductions en **SQL sémantique** et en **Cypher** dans des panneaux latéraux ; copiez l'une ou l'autre ou passez directement à l'éditeur SQL ou de graphe. Un flux de travail pratique consiste à esquisser des fragments de requête en GraphQL, puis à assembler le SQL obtenu dans des vues ou des rapports complexes.

L'explorateur affiche une requête GraphQL accompagnée de ses traductions en direct en SQL et en Cypher :

![Explorateur de langages de requête](docs/images/query-explorer.png)

Le même schéma fédéré peut être exploré comme un graphe en direct — étiquettes de domaine et de nœud, types de relation et parcours de longueur variable :

![Visualisation de graphe](docs/images/graph-view.png)

### Outils de composition de requêtes

Ces outils vous aident à écrire des requêtes dans les langages ci-dessus — ce ne sont pas des langages de requête en eux-mêmes.

- **Requête en langage naturel** — Pipeline LN→SQL/Cypher/GraphQL propulsé par Claude. Décrivez ce que vous voulez en langage courant ; le pipeline produit une requête dans le langage choisi avec une boucle de validation interactive avant l'exécution.

![Requête en langage naturel](docs/images/natural-language.png)

### Protocoles de transport

Ce sont les protocoles de connexion. SQL, GraphQL et Cypher circulent par-dessus — le choix du protocole de transport ne change ni l'interface de requête ni le comportement de gouvernance.

- **pgwire** — N'importe quel client PostgreSQL (psql, DBeaver, DataGrip, asyncpg, SQLAlchemy, `read_sql` de pandas) se connecte sur le port 5439 comme s'il s'agissait d'un serveur Postgres. N'accepte que SQL. Le pipeline de gouvernance complet s'applique. `pg_catalog` et `information_schema` sont servis depuis un catalogue en mémoire, de sorte que les explorateurs de schéma fonctionnent sans aller-retour de fédération. TLS optionnel.
- **Bolt (Neo4j)** — N'importe quel client Neo4j (Neo4j Browser, Bloom, pilotes officiels) se connecte via le protocole Bolt et exécute du Cypher contre le graphe fédéré. Chaque rôle que détient l'utilisateur apparaît comme une base de données `provisa_<role>`. Même gouvernance que tout autre transport. TLS optionnel.
- **Arrow Flight** — Streaming columnaire à haut débit sur gRPC ; accepte GraphQL ou SQL en entrée de requête. Jeux de résultats non bornés, aucune matérialisation côté serveur, aucune infrastructure séparée requise.
- **JDBC** — Intégration d'outils de BI (Tableau, Power BI, DBeaver) en mode `approved` ou `catalog`.
- **WebSocket / SSE** — Abonnements : événements de changement quasi en temps réel ; backends : natif PG, natif MongoDB, CDC, sondage. Également exposé via Kafka.

### Sources de données

- **46 types de source** — PostgreSQL, MySQL, MongoDB, Cassandra, Elasticsearch, Neo4j, triplestores SPARQL, Kafka, Google Sheets, et plus via une seule API ; les sources de graphe et RDF sont de première classe, pas des adaptateurs
- **Routage intelligent** — Les requêtes mono-source contournent la fédération (moins de 100 ms) ; les requêtes multi-sources sont routées via la couche de fédération — apportez votre propre cluster ou utilisez les workers intégrés
- **Sources d'API** — Enregistrez des points de terminaison REST, GraphQL, gRPC, WebSocket ou RSS comme des tables interrogeables ; des assistants SPARQL sont inclus ; les jointures fédérées entre sources d'API et sources relationnelles fonctionnent de manière transparente
- **Introspection de schéma distant** — Pointez vers n'importe quel point de terminaison GraphQL, OpenAPI ou gRPC ; les opérations documentées sont automatiquement exposées comme des tables interrogeables, des nœuds et des arêtes de graphe, avec la gouvernance complète appliquée par-dessus
- **Sources de fichiers** — Fichiers CSV, Parquet et SQLite comme tables interrogeables ; prend en charge les chemins locaux et le stockage d'objets distant (`s3://`, `ftp://`, `sftp://`)
- **Intégration Kafka** — Les topics comme tables en lecture seule ; les résultats de requête comme sinks Kafka
- **Déclencheurs planifiés** — Déclencheurs cron et à intervalle (APScheduler) qui activent des webhooks, des mutations ou des publications vers des sinks Kafka
- **Indices de performance de fédération** — Des indices de routage via commentaires SQL qui outrepassent les décisions de routage automatique

![Sources de données](docs/images/data-sources.png)

Les sources, fichiers et points de terminaison distants sont enregistrés comme des tables gouvernées depuis l'interface :

![Enregistrement de table](docs/images/table-registration.png)

### Sécurité et gouvernance

- **Sécurité au niveau des lignes** — Injection de clause WHERE par table et par rôle
- **Masquage des colonnes** — Masquage par colonne (regex, constante, troncature) avec contournement basé sur le rôle
- **Préréglages de colonne** — Valeurs statiques côté serveur ou de variable de session injectées lors de l'insertion/mise à jour ; non exposées dans les types d'entrée de mutation
- **Autorisations d'écriture** — Contrôle d'accès en mutation par colonne (`writable_by`)
- **Rôles hérités** — Les rôles héritent de la RLS, de la visibilité et du masquage d'un rôle parent de manière récursive
- **Fonctions et webhooks suivis** — Fonctions de base de données et webhooks sortants exposés comme des mutations GraphQL avec des formes de retour typées
- **Hook d'approbation ABAC** — Hook d'autorisation préalable à l'exécution ; transport webhook, gRPC ou unix_socket ; portée par table, par source ou globale ; politique de repli configurable
- **Authentification enfichable** — Firebase, Keycloak, OAuth 2.0, simple (pour les tests)

![Rôles de sécurité](docs/images/security-roles.png)

### Livraison et performance

- **Vues matérialisées comme transformations enregistrées** — Une VM capture la transformation qui l'a produite : sa forme de jointure ou son SQL, les signaux d'entrée par source (instantané Iceberg, filigrane RDB) à partir desquels elle a été construite, et une vérification de déterminisme à l'enregistrement. Comme la transformation est enregistrée, les requêtes (ou sous-expressions) sont réécrites de manière transparente vers une VM à jour — correspondance structurelle de motifs de jointure avec prise en charge de la correspondance partielle, de sorte qu'une VM couvrant un sous-ensemble des jointures s'applique quand même, en préservant les jointures restantes
- **Intégration des tables actives** — Les petites tables de référence fréquemment jointes sont intégrées comme des CTE de type VALUES directement dans le plan de requête, éliminant les allers-retours inter-sources pour les données de dimension
- **Mise en cache des requêtes** — Cache de résultats Redis partitionné par rôle+RLS ; cache de hash APQ inclus
- **Observabilité comme donnée** — Les traces distribuées, métriques et journaux sont collectés via OpenTelemetry, compactés dans Iceberg sur S3, et automatiquement enregistrés comme des tables interrogeables (`traces`, `metrics`, `logs`, `queries`) dans le schéma fédéré ; interrogez-les avec SQL, GraphQL ou Cypher aux côtés de vos données métier — joignez une table `customers` à la table `queries` pour voir qui a exécuté quoi et combien de temps cela a pris

### Administration et intégration

- **API d'administration** — GraphQL à `/admin/graphql` ; téléversement/téléchargement de configuration, édition des relations, approbation des requêtes
- **Visionneuse de rapports** — `/admin/reports` liste les vues de gestion intégrées du domaine ops ainsi que tout rapport personnalisé enregistré ; nécessite la capacité `observability`
- **Aperçu de table** — chaque table enregistrée dispose d'une visionneuse de données gouvernée paginée côté serveur, avec filtres poussés vers la source, regroupement multiniveau et export CSV
- **GraphQL Voyager** — Visualisation interactive du schéma par rôle sous forme de diagramme entité-relation
- **Découverte de relations par LLM** — Suggestions de candidats à clé étrangère propulsées par Claude
- **Client Python** — `pip install provisa-client` ; GraphQL/SQL → DataFrames, Arrow Flight → Tables pyarrow, dialecte SQLAlchemy, prise en charge ADBC
- **Ingestion de données** — Points de terminaison HTTP pour envoyer des données d'événements JSON vers la plateforme
- **Import Hasura v2 / DDN** — Convertit les métadonnées Hasura v2 ou le YAML de supergraphe DDN en configuration Provisa
- **Apollo Federation** — Expose Provisa comme un sous-graphe Apollo Federation v2

Schéma à portée de rôle visualisé sous forme de diagramme entité-relation (GraphQL Voyager) :

![Schema Voyager](docs/images/schema-voyager.png)

Les relations sont enregistrées, approuvées et appliquées comme les seuls chemins de JOIN légaux :

![Relations](docs/images/relationships.png)

## Modèle de sécurité

C'est ici que « sur le chemin que chaque requête emprunte déjà » cesse d'être un slogan. Provisa applique un modèle de sécurité multicouche à travers chaque langage de requête (GraphQL, SQL, Cypher) et chaque transport (REST, gRPC, Arrow Flight, JDBC, pgwire, Bolt, WebSocket). La gouvernance s'applique de manière uniforme — il n'existe aucun chemin de requête qui la contourne. La couverture est totale par construction, et non par diligence : ajoutez une source, une colonne ou une relation, et chaque couche s'y applique automatiquement, sans rien à se rappeler d'enregistrer.

Les couches s'appliquent dans l'ordre. Une requête doit franchir chaque couche avant que la suivante ne soit évaluée.

### Couche 0 — Filtrage d'introspection

Le schéma et le catalogue présentés à un rôle ne contiennent que les tables de sa liste `domain_access` et les colonnes qui satisfont les règles `visible_to` par colonne. Les objets hors de l'accès d'un rôle sont invisibles au moment de la découverte — ils ne peuvent être ni interrogés, ni autocomplétés, ni déduits comme existants. Cela s'applique au schéma GraphQL, au catalogue SQL et à l'explorateur de schéma de l'éditeur de requêtes.

### Couche 1 — Accès public

Les tables dans des domaines sans restriction `domain_access` sont visibles pour toutes les identités authentifiées sans configuration supplémentaire. Friction nulle pour les données véritablement publiques.

### Couche 2 — Accès par domaine

Chaque rôle porte une liste `domain_access` d'identifiants de domaine. Une requête qui touche une table hors de ces domaines est rejetée avant l'exécution. C'est la frontière de propriété grossière — un rôle RH ne peut pas atteindre les tables finance, quelle que soit la façon dont le SQL est écrit.

### Couche 3 — Sécurité au niveau des lignes

Une fois l'accès par domaine confirmé, des prédicats `WHERE` par table et par rôle sont injectés dans chaque `SELECT` au moment de l'exécution. Les prédicats s'évaluent sur les données brutes. Un responsable régional interrogeant une table de commandes partagée ne voit que les lignes de sa région, même sur un `SELECT *`.

### Couche 4 — Visibilité et masquage des colonnes

Les colonnes dont la liste `visible_to` exclut le rôle demandeur sont retirées de la sortie de la requête. Les colonnes soumises à une règle de masquage voient leurs valeurs remplacées — rédaction par regex, remplacement par constante ou troncature — avant que les résultats ne quittent le serveur. Le masquage s'applique dans tous les langages de requête et formats de sortie.

### Couche 5 — Garde-fou des prédicats

Les colonnes masquées sont rejetées des clauses `WHERE` et `HAVING`. Sans cela, un appelant pourrait déduire la valeur non masquée en la recherchant par dichotomie dans un filtre, même si la sortie est masquée. Le rejet est appliqué au moment de l'analyse de la requête, avant l'exécution.

### Gouvernance des relations

Les conditions JOIN en SQL doivent correspondre à une relation enregistrée et approuvée entre tables. Les jointures non approuvées sont rejetées. Chaque relation porte une raison et une description lisibles par un humain — un guide, tant pour les utilisateurs que pour les agents autonomes, expliquant pourquoi un chemin de parcours existe. Il s'agit d'une politique de gouvernance, pas d'une frontière de sécurité stricte : les couches 2 à 5 tiennent quelle que soit la structure de la jointure, de sorte qu'un contournement délibéré n'expose pas de données auxquelles le rôle n'aurait pas pu accéder via deux requêtes séparées. Les tentatives de contournement sont journalisées et auditables.

---

Ces couches se composent. Un rôle avec accès par domaine, RLS et colonnes masquées a les cinq contraintes actives simultanément. Ajouter une nouvelle source de données, colonne ou relation ne nécessite pas de mettre à jour chaque règle — chaque couche est configurée indépendamment et s'applique automatiquement à toute requête qui touche des objets gouvernés.

### macOS

1. Téléchargez [Provisa-macOS.dmg](https://provisa.dev/dl/macos) (toujours la dernière version)
2. Faites glisser **Provisa.app** vers `/Applications` et double-cliquez pour le lancer
3. Le premier lancement effectue une configuration unique (~2 min, sans connexion internet requise)
4. Ouvrez le terminal :

```bash
provisa start   # start all services
provisa open    # open the UI in your browser
```

### Linux

1. Téléchargez [Provisa-linux-x86_64.AppImage](https://provisa.dev/dl/linux) (toujours la dernière version)
2. Rendez-le exécutable et lancez-le — le premier lancement effectue une configuration unique (sans connexion internet requise) :

```bash
chmod +x Provisa-*-linux-x86_64.AppImage
./Provisa-*-linux-x86_64.AppImage
provisa start && provisa open
```

### Windows

1. Téléchargez [Provisa-windows-x64.exe](https://provisa.dev/dl/windows) (toujours la dernière version)
2. Exécutez le programme d'installation — aucun droit d'administrateur requis
3. Ouvrez **Provisa First Launch** depuis le menu Démarrer — cela effectue une configuration unique (~5 min, sans connexion internet requise)
4. Ouvrez un nouveau terminal :

```bash
provisa start
```

### Première requête

En développement local (`PROVISA_MODE=test`), aucune identification n'est requise. En production, authentifiez-vous avec un jeton Bearer — le rôle en est extrait automatiquement.

```bash
# Local dev — no auth required, role defaults to admin
curl -X POST http://localhost:8001/data/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "{ orders { id amount region } }"}'

# Ad-hoc SQL works the same way
curl -X POST http://localhost:8001/data/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT id, amount, region FROM orders"}'

# Production — authenticate with a Bearer token; role is derived from the token
curl -X POST https://provisa.example.com/data/graphql \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ orders { id amount region } }"}'
```

### JDBC (Tableau, DBeaver, Power BI)

Téléchargez [provisa-jdbc.jar](https://provisa.dev/dl/jdbc) (toujours la dernière version) et ajoutez-le au chemin des pilotes de votre outil de BI.

```text
jdbc:provisa://localhost:8815
```

Authentifiez-vous avec votre nom d'utilisateur et votre mot de passe Provisa — le serveur attribue votre rôle.

- **mode `catalog`** — schéma complet visible ; à utiliser avec des outils de catalogue (Collibra, Atlan, DBeaver)

Consultez [docs/integrations.md](docs/integrations.md) pour les étapes de configuration de Tableau et Power BI.

### Protocole de transport PostgreSQL (pgwire)

Provisa parle le protocole de transport PostgreSQL sur le port 5439. Tout client capable de se connecter à Postgres se connecte à Provisa — sans pilote, sans adaptateur, sans changement à l'outillage existant.

**Le nom d'utilisateur PostgreSQL sélectionne le rôle Provisa.** Avec `provider: none` (mode de confiance), le mot de passe est ignoré et tout nom de rôle configuré est accepté comme nom d'utilisateur — connectez-vous en tant que `analyst`, `admin`, ou n'importe quel rôle pour voir la vue gouvernée des données correspondant à ce rôle. Avec `provider: simple`, le mot de passe est validé par bcrypt. Les autres fournisseurs (`firebase`, `keycloak`, `oauth`) ne sont pas pris en charge sur pgwire.

```bash
# psql — connect as analyst role
psql -h localhost -p 5439 -U analyst

# psql — connect as admin role
psql -h localhost -p 5439 -U admin

# asyncpg (Python) — role = username, password ignored in trust mode
conn = await asyncpg.connect(host="localhost", port=5439, user="analyst", password="x")
rows = await conn.fetch("SELECT id, amount FROM orders WHERE region = 'west'")

# SQLAlchemy
engine = create_engine("postgresql+psycopg2://analyst:x@localhost:5439/provisa")

# pandas
df = pd.read_sql("SELECT * FROM orders", engine)
```

Toutes les requêtes passent par le pipeline de gouvernance complet — l'accès par domaine, la RLS, le masquage et le garde-fou des prédicats s'appliquent exactement comme pour GraphQL et REST. Les explorateurs de schéma (DBeaver, DataGrip, pgAdmin) fonctionnent immédiatement : les requêtes vers `pg_catalog` et `information_schema` sont servies depuis un catalogue en mémoire limité à l'accès par domaine du rôle, de sorte que les utilisateurs ne voient que les tables et colonnes qu'ils sont autorisés à interroger.

DataGrip explorant le schéma gouverné et son diagramme de clés étrangères via pgwire — sans pilote, sans adaptateur :

![Provisa dans DataGrip via pgwire](docs/images/pgwire-datagrip.png)

TLS s'active en configurant `PROVISA_PGWIRE_CERT` et `PROVISA_PGWIRE_KEY`. Le port est configurable via `PROVISA_PGWIRE_PORT` (par défaut `5439`).

### Bolt (protocole de transport Neo4j)

Provisa parle également le protocole **Bolt** de Neo4j, de sorte que les outils natifs de graphe se connectent directement et exécutent du Cypher contre le graphe fédéré — sans export, sans base de données de graphe distincte. Pointez **Neo4j Browser** ou **Bloom** vers Provisa et parcourez les relations à travers les sources avec la même gouvernance (accès par domaine, RLS, masquage) appliquée.

Neo4j Browser exécutant du Cypher contre Provisa — les étiquettes de nœud, les types de relation et les clés de propriété proviennent directement du schéma enregistré :

![Provisa dans Neo4j Browser via Bolt](docs/images/bolt-neo4j-browser.png)

Activez-le en configurant `PROVISA_BOLT_PORT` (la valeur par défaut de Neo4j est `7687`). TLS s'active avec `PROVISA_BOLT_CERT` et `PROVISA_BOLT_KEY`. Chaque rôle Provisa que détient l'utilisateur authentifié apparaît comme une base de données sélectionnable `provisa_<role>` (le sélecteur `provisa_admin` ci-dessus) — en choisir une restreint la session aux droits de domaine de ce rôle ; l'utilisateur ne peut jamais dépasser les rôles qu'il détient.

### Client Python

```bash
pip install provisa-client                       # core
pip install "provisa-client[pandas]"             # + DataFrame support
pip install "provisa-client[sqlalchemy]"         # + SQLAlchemy dialect
pip install "provisa-client[adbc]"               # + ADBC over Arrow Flight
```

```python
from provisa_client import ProvisaClient, connect

# GraphQL → DataFrame
client = ProvisaClient("http://localhost:8001", username="alice", password="secret")
df = client.query_df("{ orders { id amount region } }")

# SQL → DataFrame
df = client.query_df("SELECT id, amount, region FROM orders WHERE region = 'west'")

# Arrow Flight → pyarrow Table (high-throughput columnar)
table = client.flight("{ orders { id amount region } }")

# DB-API 2.0 (PEP 249) — GraphQL or SQL, detected automatically
with connect("http://localhost:8001", username="alice", password="secret") as conn:
    cur = conn.cursor()

    # GraphQL
    cur.execute("{ orders { id amount region } }")
    rows = cur.fetchall()

    # SQL (routed through governance engine — RLS and masking applied)
    cur.execute("SELECT id, amount FROM orders WHERE region = %s", ("west",))
    rows = cur.fetchall()

# SQLAlchemy dialect — provisa+http:// or provisa+https://
from sqlalchemy import create_engine, text
import pandas as pd

engine = create_engine("provisa+http://alice:secret@localhost:8001")

# pandas read_sql — GraphQL or SQL
df = pd.read_sql("{ orders { id amount region } }", engine)
df = pd.read_sql("SELECT id, amount, region FROM orders WHERE region = 'west'", engine)

# raw execute
with engine.connect() as conn:
    rows = conn.execute(text("SELECT id, amount FROM orders")).fetchall()

# role + mode URL parameters (mode=catalog for arbitrary SQL)
engine = create_engine(
    "provisa+http://alice:secret@localhost:8001?role=analyst&mode=catalog"
)

# ADBC — Arrow-native streaming via Flight
from provisa_client.adbc import adbc_connect
with adbc_connect("http://localhost:8001", user="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        table = cur.fetch_arrow_table()
```

Consultez [docs/python-client.md](docs/python-client.md) pour la référence complète.

## Documentation

| Sujet | Document |
| --- | --- |
| Démarrage rapide pour développeurs (exécution depuis les sources) | [docs/quickstart.md](docs/quickstart.md) |
| Référence complète de configuration YAML | [docs/configuration.md](docs/configuration.md) |
| Référence des points de terminaison (GraphQL, REST, Flight, gRPC) | [docs/api-reference.md](docs/api-reference.md) |
| Conception du système et carte des composants | [docs/architecture.md](docs/architecture.md) |
| Modèle de sécurité (RLS, masquage, authentification) | [docs/security.md](docs/security.md) |
| Types de source pris en charge | [docs/sources.md](docs/sources.md) |
| Abonnements SSE | [docs/subscriptions.md](docs/subscriptions.md) |
| JDBC, outils de BI, clients Arrow Flight, Apollo Federation | [docs/integrations.md](docs/integrations.md) |
| Client Python (`provisa-client`) | [docs/python-client.md](docs/python-client.md) |
| API d'administration | [docs/admin.md](docs/admin.md) |
| Déploiement (Docker Compose, Kubernetes, macOS) | [docs/deployment.md](docs/deployment.md) |
| Import Hasura v2 / DDN | [docs/import.md](docs/import.md) |
| Flux de publication (étiquettes alpha/beta/stable) | [docs/releasing.md](docs/releasing.md) |

## Dimensionnement

Provisa inclut un moteur de fédération intégré pour les requêtes multi-sources. Au premier lancement, vous choisissez un budget de RAM ; Provisa en déduit automatiquement le nombre de workers de fédération locaux.

| RAM de l'hôte | Workers | Charge de travail typique |
| --- | --- | --- |
| < 24 Go | 0 | Développement, requêtes mono-source, petites équipes |
| 24–47 Go | 1 | Petite équipe, requêtes inter-sources modérées |
| 48–95 Go | 2 | Déploiement départemental, usage mixte BI + notebook |
| 96 Go+ | 4 | Grand département, fédération concurrente intensive |

Le nombre de workers peut être modifié à tout moment en éditant `~/.provisa/config.yaml` (`federation_workers: N`) et en exécutant `provisa restart`. Réglez-le sur `0` pour fonctionner en mode coordination seule (nœud unique).

### Montée en charge au-delà d'une seule machine

**Montée en charge horizontale** — Exécutez plusieurs instances Provisa derrière un répartiteur de charge. Chaque instance est un système pleinement fonctionnel. Toutes les instances doivent pointer vers la même base de données de configuration (définissez `CONFIG_DB_HOST` sur les machines secondaires) et, éventuellement, vers une instance Redis partagée (`REDIS_URL`) pour un cache unifié. La plupart des requêtes se répartissent de manière transparente ; les jointures inter-sources très volumineuses peuvent dépasser les ressources d'une seule instance et nécessiter une machine plus puissante ou un cluster de fédération externe.

**Redis partagé** — Définissez `REDIS_URL` sur chaque instance pour pointer vers un Redis externe. Un Redis partagé signifie que les entrées de cache d'une instance sont disponibles pour toutes, améliorant les taux de succès à travers le cluster.

**Apportez votre propre cluster de fédération** — Pointez Provisa vers un cluster de fédération externe existant plutôt que les workers intégrés. Recommandé pour les déploiements à grande échelle ou dans le cloud ; voir [docs/deployment.md](docs/deployment.md) pour la configuration.

## Licence

Business Source License 1.1 (non modifiée, conformément aux engagements du Concédant de
MariaDB). Chaque version publiée passe à la Change License (GPL v2.0 ou ultérieure) au
4ᵉ anniversaire de sa publication publique ; le code actuel et récent reste sous BSL.
L'usage en production au-delà des seuils de l'Additional Use Grant (moins de 100
employés/prestataires et moins de 1 M$ de revenus de l'année précédente) requiert une
licence commerciale. Voir [LICENSE](LICENSE).

Le Concédant ne consent pas à l'utilisation de cette œuvre pour l'entraînement d'IA/ML.
Voir [NOTICE](NOTICE), [ai.txt](ai.txt) et [robots.txt](robots.txt). Pour les licences
commerciales ou d'entraînement d'IA : <kennethstott@gmail.com>
