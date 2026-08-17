# Provisa

**Connectez vos bases de données. Interrogez avec GraphQL, gRPC, SQL, ou MCP — sur n'importe quelle API ou protocole — en 5 minutes.**

Provisa sert chaque surface API (REST, GraphQL, SQL, gRPC, MCP, et plus) sur le résultat joint de vos sources. Il peut le faire parce que c'est une **couche sémantique active** : une définition unique de votre patrimoine de données — chaque domaine, relation et politique à travers vos sources, à l'exclusion des seuls systèmes d'origine eux-mêmes — qui à la fois exploite le patrimoine et le gouverne. La définition n'est pas une documentation qu'un moteur peut consulter ; elle *est* le moteur. Les domaines et relations enregistrés sont les seuls chemins de jointure légaux, et les politiques d'accès sont compilées dans chaque plan de requête. Un seul modèle, trois fonctions :

- **Définir** — Domaines, colonnes et relations sont déclarés une fois. Cette déclaration est le schéma que voit chaque consommateur et le seul ensemble de chemins de jointure qu'une requête peut emprunter.
- **Appliquer** — La sécurité au niveau des lignes, le masquage de colonnes, la visibilité des colonnes et l'approbation de requêtes sont appliqués en ligne sur le chemin d'exécution. Aucune requête n'atteint les données sans passer par eux, donc la couverture est totale par construction plutôt que par diligence.
- **Auditer** — Parce que chaque requête emprunte le même chemin gouverné, qui a interrogé quoi, sous quel rôle, et contre quelle politique est enregistré de manière uniforme. Les traces distribuées, métriques et journaux sont eux-mêmes enregistrés comme tables interrogeables aux côtés de vos données métier.

Un seul cœur gouverné sert chaque langage et transport. Interrogez avec **GraphQL, Cypher, ou SQL** ; consommez via **pgwire, Bolt, gRPC, REST, Arrow Flight, ou JDBC**. Chaque langage de requête se ramène à une seule représentation intermédiaire où la gouvernance est injectée une fois — de sorte qu'une politique ne peut pas dériver entre langages — et cette IR se re-cible vers le dialecte natif de chaque source en sortie. Ajouter un langage est un nouveau front-end sur le cœur partagé, pas un nouveau moteur.

Le patrimoine est à la fois analytique et transactionnel. Les lectures inter-sources se répartissent via la couche de fédération ; les écritures et lectures mono-source sont routées directement vers le pilote de la source — gouvernées de façon identique, mais transactionnelles et sous 100 ms. Le streaming columnaire Arrow Flight est intégré.

L'ensemble du modèle est construit à partir d'une poignée de primitives — domaines, relations, rôles et politiques. Un vocabulaire restreint, donc la définition est facile à comprendre et simple à évaluer et auditer : vous pouvez lire l'ensemble des politiques et savoir ce qu'il fait. Provisa est un compilateur de requêtes léger, pas un runtime qui siège sur le chemin de données. Il convertit une requête en requêtes natives, les route, et s'efface — c'est pourquoi le patrimoine reste performant.

Cette conception prend en charge deux façons de l'utiliser, et elles ne s'excluent pas :

- **Comme échafaudage de modernisation** — Modélisez votre patrimoine, laissez Provisa générer le SQL natif pour chaque source, puis capturez ce SQL et adoptez-le directement dans le système cible. Provisa est la couche de transition, pas une dépendance permanente.
- **Comme infrastructure permanente d'application de politique** — Gardez-le en place comme le chemin gouverné que chaque requête emprunte, de sorte que définition, application et audit restent unifiés aussi longtemps que le patrimoine existe.

## Le modèle de fédération

Le modèle entier se ramène à deux contrats et deux politiques : les sources se réduisent à des tables 2D sur un système de types unique, les requêtes se réduisent à une seule IR de type SQL, l'accessibilité décide ce qui est interrogé en direct par rapport à ce qui est matérialisé, et une stratégie de fraîcheur gouverne chaque copie matérialisée et jeu de données dérivé. Forme de données en entrée, forme de requête en entrée, gouvernance à la jointure, requêtes natives en sortie. Le reste de cette section parcourt chaque élément.

Le modèle repose sur une seule réduction : chaque source est exprimée comme une collection de tables bidimensionnelles sur un système de types unique et généralisé. C'est le contrat qu'une source doit remplir pour rejoindre le patrimoine, et c'est le même contrat pour toutes. Certaines sources correspondent déjà — une table MySQL ou PostgreSQL *est* une relation 2D typée. D'autres correspondent avec une projection : un résultat de requête GraphQL, une fois aplati, est une table. D'autres sont étrangères à cette forme — triplestores SPARQL, Neo4j — mais restent exploitables, car l'utilisateur fournit une requête dont le jeu de résultats est tabulaire ; la requête est l'adaptateur. Quelle que soit la source, le patrimoine voit des lignes, des colonnes et des types généralisés, et rien d'autre. Intégrer un nouveau type de source consiste à remplir ce contrat unique, parfois avec une étape d'intervention humaine, et non à écrire une intégration sur mesure.

Cette réduction a un jumeau côté requête. Le SQL — à travers tous ses dialectes et particularités — est essentiellement le langage d'analyse sur des jeux de données 2D, ce qui fait d'une forme de type SQL la cible universelle naturelle pour les requêtes. Donc chaque requête, quel que soit le langage dans lequel elle arrive, est ramenée à cette représentation intermédiaire comme toute première étape. Certaines se ramènent proprement — le SQL lui-même, et même GraphQL ; certaines sont difficiles — la sémantique de chemins et de graphes de Cypher demande un vrai travail — mais toutes sont réalisables. Canaliser chaque requête vers une seule IR avant que quoi que ce soit d'autre ne se produise est ce qui permet à la gouvernance de s'appliquer en exactement un seul endroit, sur une seule forme, indépendamment du langage d'origine.

Par-dessus ces deux formes uniformes — sources tabulaires et une seule forme de requête — la fédération ici signifie à la fois requête en direct et entreposage — la même étendue que couvre un moteur de requête en direct comme Trino, plus la matérialisation sur laquelle de tels moteurs s'appuient. Le concept qui les unifie est l'**accessibilité** : pour une source donnée, le moteur peut-il l'interroger sur place, ou ses données doivent-elles d'abord être matérialisées quelque part d'interrogeable ? L'accessibilité partitionne le patrimoine entre ce qui est interrogé en direct et ce qui est copié au préalable.

La plupart des bases de données portent déjà une certaine notion de lien en direct — `ATTACH` de DuckDB, `postgres_fdw` de PostgreSQL, liens externes Databricks. Donc la plupart des bases de données peuvent agir comme moteur de fédération dans une certaine mesure. Aucune n'est exhaustive : chacune atteint un ensemble particulier de sources et matérialise le reste, sans compte-rendu unique de ce qui est quoi. Le modèle comble cet écart en rendant l'accessibilité explicite — un ensemble défini de méthodes, par source, qui énoncent ce que le moteur peut atteindre en direct et, par élimination, ce qui doit être matérialisé.

Ce qui reste, c'est la fraîcheur : pour chaque source non accessible, à quel point sa copie matérialisée doit-elle être à jour ? En pratique, cela se ramène à un petit ensemble de stratégies — à la demande, sur un calendrier, sur un signal de changement (CDC, filigrane, instantané), ou figée. Choisir l'une par source constitue toute la politique de fraîcheur.

Les jeux de données analytiques — tables dérivées, agrégats, sorties d'une transformation — se replient dans la même forme. Eux aussi doivent être exprimés dans l'IR, et parce qu'ils le sont, la traçabilité (lineage) n'est pas un système séparé à maintenir : le chemin de chaque système d'origine jusqu'à une sortie finale *est* l'IR qui l'a produite, lisible de bout en bout. Construire ces jeux de données soulève la question de la fraîcheur un cran plus loin — le jeu de données se rafraîchit-il sur un calendrier, seulement une fois ses préconditions remplies, en continu comme quasi-temps réel, ou comme un instantané historique figé ? Les façons d'exprimer comment et quand construire un jeu de données sont le même petit ensemble énumérable, donc un jeu de données dérivé porte une politique de construction dans exactement le vocabulaire que porte une copie de source.

Les modèles dimensionnels en sont une application directe. Les tables de faits et de dimensions d'un schéma en étoile sont des jeux de données analytiques comme tout autre — une dimension est une projection conforme et dédupliquée ; une table de faits est une jointure et un agrégat réduits à un grain — chacun portant sa propre politique de construction et de fraîcheur. Les dimensions à évolution lente ne nécessitent aucune machinerie spéciale : un instantané figé est un historique de Type 2, une reconstruction planifiée est un Type 1. Et parce que le schéma est défini dans l'IR plutôt que physiquement lié aux tables d'un seul entrepôt, les mêmes définitions de faits et de dimensions se re-ciblent — matérialisées dans Oracle, dans Databricks, ou laissées virtuelles sur un moteur MPP — sans remodélisation. Le modèle génère le schéma en étoile ; il ne l'enferme pas dans un moteur.

Data Vault s'inscrit de la même façon, une couche plus tôt. Ses hubs sont des jeux de données de clés métier dédupliqués, ses liens sont les relations enregistrées entre eux, et ses satellites sont des jeux de données d'attributs horodatés, en ajout seul — l'enregistrement historique. Un satellite est simplement un jeu de données dérivé sur la stratégie de fraîcheur par signal de changement : date de chargement plus hashdiff est du CDC appliqué à des attributs descriptifs, et l'historique en ajout seul est la stratégie d'instantané figé. Les tables point-in-time et bridge sont d'autres jeux de données dérivés construits pour la performance de requête. Donc un vault brut est un ensemble de jeux de données analytiques dans l'IR, et un schéma en étoile en est une projection — les deux générés, les deux portables entre moteurs. Ce que le modèle ne fait pas, c'est décider de la méthodologie : ce qui devient un hub, le grain d'un satellite, la stratégie de découpage. Ceux-ci restent des choix de modélisation ; une fois faits, ils vivent comme une IR portable plutôt que comme de l'ETL soudé à un entrepôt.

Les deux motifs sont déclarés via **deux raccourcis de première classe** plutôt que des vues écrites à la main — les primitives à partir desquelles chaque schéma en étoile et Data Vault sont construits, gardées neutres vis-à-vis de la méthodologie :

- **`entity`** — une projection à clé, dédupliquée, optionnellement historisée d'une source. Déclarez une clé d'entité, les attributs, et un mode d'historique ; Provisa la ramène à une vue matérialisée, et quand l'historique est demandé, à une **MV bitemporelle** (`scd2` → delta, `snapshot` → instantané). Une seule construction sert une **dimension** Kimball (SCD1/SCD2) et un **hub + satellite** Data Vault.
- **`fact`** — une jointure vers des clés d'entité, réduite à un grain déclaré, avec des mesures agrégées. Provisa la ramène à une MV d'agrégat plus des relations enregistrées vers les entités. Une seule construction sert une **table de faits** en étoile et un **lien** Data Vault (un fait sans mesure est un lien pur d'ensemble de clés).

Parce que la réduction est pure — une spécification `entity`/`fact` devient exactement les définitions de MV, bitemporelle et de relation qu'un modélisateur écrirait sinon à la main — l'entrepôt est de l'IR de bout en bout et se re-cible entre moteurs sans remodélisation. Déclarez un entrepôt dans l'UI d'administration (un formulaire **Model** pour entités et faits) ou via l'API d'administration (`registerEntity` / `registerFact`) ; le modèle *génère* l'étoile Kimball ou le Data Vault, il n'en impose pas un.

### Voyage dans le temps

Le voyage dans le temps est une idée simple — garder chaque version d'une ligne au lieu de l'écraser, pour pouvoir demander ce qu'étaient les données à un moment passé quelconque. Ce qui diffère, c'est l'efficacité avec laquelle chaque moteur peut le faire, ce qui est exactement pourquoi Provisa en fait une propriété de la **définition** de vue matérialisée plutôt que du moteur de stockage (REQ-1162). Déclarez-le une fois ; il fonctionne sur n'importe quel backend de matérialisation.

La règle qui le garde portable est l'**ajout seul** : une version, une fois écrite, n'est jamais mise à jour ni supprimée. Retirer une ligne en écrivant une date « valide jusqu'à » — l'astuce bitemporelle habituelle — nécessite un UPDATE, que de nombreux moteurs ne peuvent pas faire à moindre coût (ou pas du tout) sur un magasin fédéré, donc Provisa ne le fait pas. À la place, chaque rafraîchissement **ajoute**, et « quelle version était en vigueur au temps T » est dérivé à la lecture depuis le journal immuable. Il y a exactement deux façons d'ajouter :

- **Instantané (Snapshot)** — ajoute le jeu de données frais entier, estampillé du temps système de ce rafraîchissement. Pas de diff ; correct sur tout moteur ; le stockage croît d'une copie complète par rafraîchissement.
- **Delta** — n'ajoute que ce qui a changé, plus des tombstones pour les clés supprimées. Le delta est **calculé par le moteur** (anti-jointures dans un `INSERT … SELECT`), jamais replié ligne par ligne dans Provisa. Plus petit, et il nécessite une clé d'entité.

Le temps système (quand Provisa a enregistré une version) est géré de cette façon ; le temps valide (quand un fait est vrai dans le métier) est fourni par le SELECT propre de la vue et préservé. Les moteurs qui offrent plus — instantanés Iceberg natifs, un MERGE qui maintient moins de lignes — peuvent être ciblés pour l'efficacité derrière la même déclaration ; le chemin en ajout seul est le plancher correct partout.

La lecture est transparente. Une requête simple contre une MV bitemporelle reconstruit l'état **actuel** depuis le journal d'ajout par défaut ; pour voyager dans le temps, envoyez un en-tête `X-Provisa-As-Of: <timestamp>` et la requête entière est répondue comme le patrimoine était à ce moment — sémantique identique sur chaque substrat. Activez-le pour n'importe quelle vue matérialisée dans l'UI d'administration (un contrôle **Time Travel** : off / snapshot / delta plus une clé d'entité) ou via l'API d'administration.

Accessibilité plus fraîcheur est un modèle général pour la fédération de données : une définition qui dit ce qui est en direct, ce qui est matérialisé, et à quel point chaque copie reste fraîche — indépendamment de la portée d'un moteur particulier. Le résultat est la liberté par rapport au verrouillage propriétaire. Le modèle est portable ; le patrimoine n'est pas captif de la fédération d'un fournisseur particulier qui atteint le plus de sources aujourd'hui.

## Fonctionnalités

### Interfaces de requête

Ce sont les langages et API structurées dans lesquels vous écrivez des requêtes. Chacun a sa propre syntaxe et sémantique ; la gouvernance (RLS, masquage, visibilité des colonnes, application des relations) s'applique uniformément à travers tous, indépendamment du protocole de transport qui les livre.

- **GraphQL** — Schémas par rôle avec visibilité au niveau des champs, filtrage, pagination par curseur, et requêtes d'agrégation (`count`, `sum`, `avg`, `min`, `max`). Contraint par schéma aux relations enregistrées — structurellement valide par construction, le chemin le plus rapide vers une requête simple correcte. Apollo APQ inclus : les requêtes sont hachées et enregistrées côté serveur ; les appels suivants n'envoient que le hash via HTTP GET, rendant les réponses cacheables par CDN sans changement client requis. Les tables de correspondance sous un seuil de lignes configurable sont exposées comme types enum.
- **SQL** — SQL complet sur données fédérées ; non contraint et plus expressif que GraphQL. Écrivez du SQL standard — sous-requêtes corrélées comprises — et il s'exécute à travers les sources sans changement. Les requêtes mono-source contournent entièrement la couche de fédération (moins de 100 ms).
- **Cypher** — Langage de requête de graphe sur le même schéma fédéré. Parcourez les relations comme des arêtes de graphe ; unissez les sources ; chemins de longueur variable. La gouvernance s'applique de façon identique à GraphQL et SQL.
- **API modèle gRPC** — `.proto` auto-généré depuis le schéma enregistré ; RPC de requête et d'insertion typés par table, réponses en streaming. Piloté par schéma au même sens que GraphQL — le modèle d'enregistrement est le contrat, protobuf est l'encodage de transport. Contrairement à Arrow Flight (qui est un transport de streaming columnaire), c'est une interface de requête complète par table.
- **JSON:API** — API de requête structurée à `/data/jsonapi/{table}`, HTTP uniquement par conception. Prend en charge JSON:API 1.1 : jeux de champs épars (`fields[table]=col1,col2`), expressions de filtre (`filter[field][op]=value`), documents composés (`include=relation`), et tri. Pas un langage de requête généraliste — interroge une table à la fois avec une syntaxe de filtre standardisée plutôt qu'une chaîne de requête ad hoc.
- **Explorateur de langage de requête** — Écrivez une requête GraphQL et voyez des traductions en direct **SQL sémantique** et **Cypher** dans des panneaux latéraux ; copiez l'une ou l'autre ou passez directement à l'éditeur SQL ou Graph. Un flux de travail pratique est d'esquisser des fragments de requête en GraphQL, puis d'assembler le SQL résultant dans des vues ou rapports complexes.

L'explorateur montre une requête GraphQL aux côtés de ses traductions SQL et Cypher en direct :

![Query Language Explorer](docs/images/query-explorer.png)

Le même schéma fédéré est explorable comme un graphe en direct — étiquettes de domaine et de nœud, types de relation, et parcours de longueur variable :

![Graph Visualization](docs/images/graph-view.png)

### Outils de composition de requête

Ces outils vous aident à écrire des requêtes dans les langages ci-dessus — ce ne sont pas des langages de requête eux-mêmes.

- **Requête en langage naturel** — Pipeline NL→SQL/Cypher/GraphQL propulsé par Claude. Décrivez ce que vous voulez en anglais courant ; le pipeline produit une requête dans le langage de votre choix avec une boucle de validation interactive avant l'exécution.

![Natural Language Query](docs/images/natural-language.png)

### Protocoles de transport

Ce sont les protocoles de connexion. SQL, GraphQL et Cypher circulent par-dessus — le choix du protocole de transport ne change pas l'interface de requête ni le comportement de gouvernance.

- **pgwire** — N'importe quel client PostgreSQL (psql, DBeaver, DataGrip, asyncpg, SQLAlchemy, pandas `read_sql`) se connecte sur le port 5439 comme s'il s'agissait d'un serveur Postgres. Accepte uniquement le SQL. Le pipeline de gouvernance complet s'applique. `pg_catalog` et `information_schema` sont répondus depuis un catalogue en mémoire pour que les navigateurs de schéma fonctionnent sans aller-retour de fédération. TLS optionnel.
- **Bolt (Neo4j)** — N'importe quel client Neo4j (Neo4j Browser, Bloom, pilotes officiels) se connecte via le protocole Bolt et exécute Cypher contre le graphe fédéré. Chaque rôle que détient l'utilisateur apparaît comme une base de données `provisa_<role>`. Même gouvernance que tout autre transport. TLS optionnel.
- **Arrow Flight** — Streaming columnaire à haut débit sur gRPC ; accepte GraphQL ou SQL comme entrée de requête. Jeux de résultats non bornés, pas de matérialisation côté serveur, pas d'infrastructure séparée requise.
- **JDBC** — Intégration d'outils BI (Tableau, Power BI, DBeaver) en mode `approved` ou `catalog`.
- **WebSocket / SSE** — Abonnements : événements de changement quasi-temps réel ; backends : PG natif, MongoDB natif, CDC, sondage. Aussi exposé via Kafka.

### Sources de données

- **52 types de source** — PostgreSQL, MySQL, MongoDB, Cassandra, Elasticsearch, Neo4j, triplestores SPARQL, Kafka, Google Sheets, et plus via une seule API ; les sources graphe et RDF sont de première classe, pas des adaptateurs
- **Routage intelligent** — Les requêtes mono-source contournent la fédération (moins de 100 ms) ; les requêtes multi-sources sont routées via la couche de fédération — apportez votre propre cluster ou utilisez les workers embarqués
- **Sources API** — Enregistrez des endpoints REST, GraphQL, gRPC, WebSocket, ou RSS comme tables interrogeables ; helpers SPARQL inclus ; les jointures fédérées entre sources API et sources relationnelles fonctionnent de manière transparente
- **Introspection de schéma distant** — Pointez vers n'importe quel endpoint GraphQL, OpenAPI, ou gRPC ; les opérations documentées sont automatiquement exposées comme tables interrogeables, nœuds et arêtes de graphe avec la gouvernance complète appliquée par-dessus
- **Sources fichier** — Fichiers CSV, Parquet, et SQLite comme tables interrogeables ; prend en charge les chemins locaux et le stockage objet distant (`s3://`, `ftp://`, `sftp://`)
- **Intégration Kafka** — Sujets (topics) comme tables en lecture seule ; résultats de requête comme sinks Kafka
- **Déclencheurs planifiés** — Déclencheurs cron et par intervalle (APScheduler) qui déclenchent des webhooks, mutations, ou publications de sink Kafka
- **Indices de performance de fédération** — Des indices de routage en commentaire SQL remplacent les décisions de routage automatiques

![Data Sources](docs/images/data-sources.png)

Sources, fichiers, et endpoints distants sont enregistrés comme tables gouvernées depuis l'UI :

![Table Registration](docs/images/table-registration.png)

### Sécurité et gouvernance

- **Sécurité au niveau des lignes** — Injection de clause WHERE par table, par rôle
- **Masquage de colonnes** — Masquage par colonne (regex, constante, troncature) avec contournement basé sur le rôle
- **Préréglages de colonnes** — Valeurs statiques ou de variable de session injectées côté serveur à l'insertion/mise à jour ; non exposées dans les types d'entrée de mutation
- **Permissions d'écriture** — Contrôle d'accès de mutation par colonne (`writable_by`)
- **Rôles hérités** — Les rôles héritent de la RLS, de la visibilité et du masquage d'un rôle parent de manière récursive
- **Fonctions et webhooks suivis** — Fonctions BD et webhooks sortants exposés comme mutations GraphQL avec des formes de retour typées
- **Hook d'approbation ABAC** — Hook d'autorisation pré-exécution ; transport webhook, gRPC, ou unix_socket ; portée par table, par source, ou globale ; politique de repli configurable
- **Auth enfichable** — Firebase, Keycloak, OAuth 2.0, simple (test)

![Security Roles](docs/images/security-roles.png)

### Livraison et performance

- **Vues matérialisées comme transformations enregistrées** — Une MV capture la transformation qui l'a produite : sa forme de jointure ou son SQL, les signaux d'entrée par source (instantané Iceberg, filigrane RDB) à partir desquels elle a été construite, et une vérification de déterminisme à l'enregistrement. Parce que la transformation est enregistrée, les requêtes (ou sous-expressions) sont réécrites de manière transparente sur une MV fraîche — correspondance structurelle de motif de jointure avec support de correspondance partielle, de sorte qu'une MV couvrant un sous-ensemble de jointures s'applique quand même, avec les jointures restantes préservées
- **Inlining de table chaude** — Les petites tables de correspondance fréquemment jointes sont incorporées comme CTE VALUES directement dans le plan de requête, éliminant les allers-retours inter-sources pour les données de dimension
- **Cache de requête** — Cache de résultats Redis partitionné par rôle+RLS ; cache de hash APQ inclus
- **Observabilité comme donnée** — Traces distribuées, métriques et journaux sont collectés via OpenTelemetry, compactés dans Iceberg sur S3, et automatiquement enregistrés comme tables interrogeables (`traces`, `metrics`, `logs`, `queries`) dans le schéma fédéré ; interrogez-les avec SQL, GraphQL, ou Cypher aux côtés de vos données métier — joignez une table `customers` à la table `queries` pour voir qui a exécuté quoi et combien de temps ça a pris

### Administration et intégration

- **API d'administration** — GraphQL à `/admin/graphql` ; upload/download de configuration, édition de relations, approbation de requête
- **Visionneuse de rapports** — `/admin/reports` liste les vues de gestion du domaine ops intégrées et tout rapport personnalisé enregistré ; nécessite la capacité `observability`
- **Aperçu de table** — chaque table enregistrée dispose d'une visionneuse de données gouvernée paginée côté serveur avec filtres poussés, regroupement multi-niveaux, et export CSV
- **GraphQL Voyager** — Visualisation de schéma interactive à portée de rôle comme diagramme entité-relation
- **Découverte de relations par LLM** — Suggestions de candidats de clé étrangère propulsées par Claude
- **Client Python** — `pip install provisa-client` ; GraphQL/SQL → DataFrames, Arrow Flight → tables pyarrow, dialecte SQLAlchemy, support ADBC
- **Ingestion de données** — Endpoints HTTP pour pousser des données d'événement JSON dans la plateforme
- **Import Hasura v2 / DDN** — Convertit les métadonnées Hasura v2 ou le YAML de supergraphe DDN en configuration Provisa
- **Apollo Federation** — Expose Provisa comme sous-graphe Apollo Federation v2

Schéma à portée de rôle visualisé comme diagramme entité-relation (GraphQL Voyager) :

![Schema Voyager](docs/images/schema-voyager.png)

Les relations sont enregistrées, approuvées, et appliquées comme les seuls chemins de JOIN légaux :

![Relationships](docs/images/relationships.png)

## Modèle de sécurité

C'est ici que « sur le chemin que chaque requête emprunte déjà » cesse d'être un slogan. Provisa applique un modèle de sécurité multi-couches à travers chaque langage de requête (GraphQL, SQL, Cypher) et chaque transport (REST, gRPC, Arrow Flight, JDBC, pgwire, Bolt, WebSocket). La gouvernance est appliquée de manière uniforme — il n'y a aucun chemin de requête qui la contourne. La couverture est totale par construction, pas par diligence : ajoutez une source, une colonne, ou une relation et chaque couche s'y applique automatiquement, sans rien à se rappeler d'enregistrer.

Les couches s'appliquent dans l'ordre. Une requête doit franchir chaque couche avant que la suivante ne soit évaluée.

### Couche 0 — Filtrage d'introspection

Le schéma et le catalogue présentés à un rôle ne contiennent que les tables de sa liste `domain_access` et les colonnes qui passent les règles `visible_to` par colonne. Les objets hors de la portée d'un rôle sont invisibles au moment de la découverte — ils ne peuvent être ni interrogés, ni autocomplétés, ni inférés comme existants. Cela s'applique au schéma GraphQL, au catalogue SQL, et au navigateur de schéma de l'éditeur de requête.

### Couche 1 — Accès public

Les tables dans des domaines sans restriction `domain_access` sont visibles à toutes les identités authentifiées sans configuration supplémentaire. Aucune friction pour les données véritablement publiques.

### Couche 2 — Accès au domaine

Chaque rôle porte une liste `domain_access` d'ID de domaine. Une requête qui touche une table hors de ces domaines est rejetée avant l'exécution. C'est la frontière de propriété grossière — un rôle RH ne peut pas atteindre les tables de finance quelle que soit la façon dont le SQL est écrit.

### Couche 3 — Sécurité au niveau des lignes

Une fois l'accès au domaine confirmé, des prédicats `WHERE` par table, par rôle sont injectés dans chaque `SELECT` au moment de l'exécution. Les prédicats s'évaluent contre les données brutes. Un responsable régional interrogeant une table de commandes partagée ne voit que les lignes de sa région même sur un `SELECT *`.

### Couche 4 — Visibilité et masquage des colonnes

Les colonnes avec une liste `visible_to` qui exclut le rôle demandeur sont retirées de la sortie de requête. Les colonnes avec une règle de masquage voient leurs valeurs remplacées — rédaction par regex, remplacement par constante, ou troncature — avant que les résultats ne quittent le serveur. Le masquage s'applique dans tous les langages de requête et formats de sortie.

### Couche 5 — Garde de prédicat

Les colonnes masquées sont rejetées des clauses `WHERE` et `HAVING`. Sans cela, un appelant pourrait inférer la valeur non masquée en la recherchant par dichotomie dans un filtre même si la sortie est masquée. Le rejet est appliqué au moment de l'analyse de la requête, avant l'exécution.

### Gouvernance des relations

Les conditions JOIN en SQL doivent correspondre à une relation enregistrée et approuvée entre tables. Les jointures non approuvées sont rejetées. Chaque relation porte une raison et une description lisibles par un humain — un guide tant pour les utilisateurs que pour les agents autonomes sur la raison d'être d'un chemin de parcours. C'est une politique de gouvernance, pas une frontière de sécurité dure : les couches 2 à 5 tiennent indépendamment de la structure de jointure, donc un contournement délibéré n'expose pas de données que le rôle ne pourrait pas atteindre via deux requêtes séparées. Les tentatives de contournement sont journalisées et auditables.

---

Ces couches se composent. Un rôle avec accès au domaine, RLS, et colonnes masquées a les cinq contraintes actives simultanément. Ajouter une nouvelle source de données, colonne, ou relation ne nécessite pas de mettre à jour chaque règle — chaque couche est configurée indépendamment et s'applique automatiquement à toute requête qui touche des objets gouvernés.

### macOS

1. Téléchargez [Provisa-macOS.dmg](https://provisa.dev/dl/macos) (toujours la dernière version)
2. Glissez **Provisa.app** vers `/Applications` et double-cliquez pour lancer
3. Le premier lancement effectue une configuration unique (~2 min, aucune connexion internet requise)
4. Ouvrez un terminal :

```bash
provisa start   # start all services
provisa open    # open the UI in your browser
```

### Linux

1. Téléchargez [Provisa-linux-x86_64.AppImage](https://provisa.dev/dl/linux) (toujours la dernière version)
2. Rendez-le exécutable et lancez-le — le premier lancement effectue une configuration unique (aucune connexion internet requise) :

```bash
chmod +x Provisa-*-linux-x86_64.AppImage
./Provisa-*-linux-x86_64.AppImage
provisa start && provisa open
```

### Windows

1. Téléchargez [Provisa-windows-x64.exe](https://provisa.dev/dl/windows) (toujours la dernière version)
2. Exécutez l'installeur — aucun droit administrateur requis
3. Ouvrez **Provisa First Launch** depuis le menu Démarrer — effectue une configuration unique (~5 min, aucune connexion internet requise)
4. Ouvrez un nouveau terminal :

```bash
provisa start
```

### Première requête

En développement local (`PROVISA_MODE=test`), aucun identifiant n'est requis. En production, authentifiez-vous avec un jeton Bearer — le rôle en est extrait automatiquement.

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

Téléchargez [provisa-jdbc.jar](https://provisa.dev/dl/jdbc) (toujours la dernière version) et ajoutez-le au chemin de pilote de votre outil BI.

```text
jdbc:provisa://localhost:8815
```

Authentifiez-vous avec votre nom d'utilisateur et mot de passe Provisa — le serveur assigne votre rôle.

- **mode `catalog`** — schéma complet visible ; à utiliser avec les outils de catalogue (Collibra, Atlan, DBeaver)

Voir [docs/integrations.md](docs/integrations.md) pour les étapes de configuration Tableau et Power BI.

### Protocole de transport PostgreSQL (pgwire)

Provisa parle le protocole de transport PostgreSQL sur le port 5439. Tout client capable de se connecter à Postgres se connecte à Provisa — pas de pilote, pas d'adaptateur, aucun changement à l'outillage existant.

**Le nom d'utilisateur PostgreSQL sélectionne le rôle Provisa.** Avec `provider: none` (mode confiance), le mot de passe est ignoré et tout nom de rôle configuré est accepté comme nom d'utilisateur — connectez-vous comme `analyst`, `admin`, ou n'importe quel rôle pour voir la vue gouvernée des données de ce rôle. Avec `provider: simple`, le mot de passe est validé par bcrypt. Les autres fournisseurs (`firebase`, `keycloak`, `oauth`) ne sont pas pris en charge sur pgwire.

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

Toutes les requêtes passent par le pipeline de gouvernance complet — accès au domaine, RLS, masquage, et garde de prédicat s'appliquent exactement comme pour GraphQL et REST. Les navigateurs de schéma (DBeaver, DataGrip, pgAdmin) fonctionnent prêts à l'emploi : les requêtes `pg_catalog` et `information_schema` sont répondues depuis un catalogue en mémoire limité à l'accès au domaine du rôle, de sorte que les utilisateurs ne voient que les tables et colonnes qu'ils sont autorisés à interroger.

DataGrip parcourant le schéma gouverné et son diagramme de clés étrangères via pgwire — pas de pilote, pas d'adaptateur :

![Provisa in DataGrip over pgwire](docs/images/pgwire-datagrip.png)

TLS est activé en définissant `PROVISA_PGWIRE_CERT` et `PROVISA_PGWIRE_KEY`. Le port est configurable via `PROVISA_PGWIRE_PORT` (défaut `5439`).

### Bolt (protocole de transport Neo4j)

Provisa parle aussi le protocole **Bolt** de Neo4j, de sorte que les outils natifs graphe se connectent directement et exécutent Cypher contre le graphe fédéré — pas d'export, pas de base de données graphe séparée. Pointez **Neo4j Browser** ou **Bloom** vers Provisa et parcourez les relations à travers les sources avec la même gouvernance (accès au domaine, RLS, masquage) appliquée.

Neo4j Browser exécutant Cypher contre Provisa — étiquettes de nœud, types de relation, et clés de propriété proviennent directement du schéma enregistré :

![Provisa in Neo4j Browser over Bolt](docs/images/bolt-neo4j-browser.png)

Activez-le en définissant `PROVISA_BOLT_PORT` (le défaut de Neo4j est `7687`). TLS est activé avec `PROVISA_BOLT_CERT` et `PROVISA_BOLT_KEY`. Chaque rôle Provisa que détient l'utilisateur authentifié apparaît comme une base de données sélectionnable `provisa_<role>` (le sélecteur `provisa_admin` ci-dessus) — en choisir une restreint la session aux droits de domaine de ce rôle ; l'utilisateur ne peut jamais dépasser les rôles qu'il détient.

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

Voir [docs/python-client.md](docs/python-client.md) pour la référence complète.

## Documentation

| Sujet | Doc |
| --- | --- |
| Démarrage rapide développeur (exécution depuis la source) | [docs/quickstart.md](docs/quickstart.md) |
| Référence complète de configuration YAML | [docs/configuration.md](docs/configuration.md) |
| Référence des endpoints (GraphQL, REST, Flight, gRPC) | [docs/api-reference.md](docs/api-reference.md) |
| Conception du système et carte des composants | [docs/architecture.md](docs/architecture.md) |
| Modèle de sécurité (RLS, masquage, auth) | [docs/security.md](docs/security.md) |
| Types de source pris en charge | [docs/sources.md](docs/sources.md) |
| Abonnements SSE | [docs/subscriptions.md](docs/subscriptions.md) |
| JDBC, outils BI, clients Arrow Flight, Apollo Federation | [docs/integrations.md](docs/integrations.md) |
| Client Python (`provisa-client`) | [docs/python-client.md](docs/python-client.md) |
| API d'administration | [docs/admin.md](docs/admin.md) |
| Déploiement (Docker Compose, Kubernetes, macOS) | [docs/deployment.md](docs/deployment.md) |
| Import Hasura v2 / DDN | [docs/import.md](docs/import.md) |
| Flux de release (tags alpha/beta/stable) | [docs/releasing.md](docs/releasing.md) |

## Dimensionnement

Provisa inclut un moteur de fédération intégré pour les requêtes multi-sources. Au premier lancement, vous choisissez un budget RAM ; Provisa dérive automatiquement le nombre de workers de fédération locaux.

| RAM hôte | Workers | Charge de travail typique |
| --- | --- | --- |
| < 24 Go | 0 | Développement, requêtes mono-source, petites équipes |
| 24–47 Go | 1 | Petite équipe, requêtes inter-sources modérées |
| 48–95 Go | 2 | Déploiement départemental, usage mixte BI + notebook |
| 96 Go+ | 4 | Grand département, fédération concurrente intensive |

Le nombre de workers peut être changé à tout moment en éditant `~/.provisa/config.yaml` (`federation_workers: N`) et en exécutant `provisa restart`. Définissez à `0` pour fonctionner en coordination seule (nœud unique).

### Monter en charge au-delà d'une seule machine

**Mise à l'échelle horizontale** — Exécutez plusieurs instances Provisa derrière un équilibreur de charge. Chaque instance est un système pleinement fonctionnel. Toutes les instances doivent pointer vers la même BD de configuration (définissez `CONFIG_DB_HOST` sur les machines secondaires) et optionnellement une instance Redis partagée (`REDIS_URL`) pour un cache unifié. La plupart des requêtes se distribuent de manière transparente ; de très grandes jointures inter-sources peuvent dépasser les ressources d'une seule instance et nécessiter une machine plus grande ou un cluster de fédération externe.

**Redis partagé** — Définissez `REDIS_URL` sur chaque instance pour pointer vers un Redis externe. Un Redis partagé signifie que les entrées de cache d'une instance sont disponibles à toutes, améliorant les taux de succès à travers le cluster.

**Apportez votre propre cluster de fédération** — Pointez Provisa vers un cluster de fédération externe existant au lieu des workers embarqués. Recommandé pour les déploiements à grande échelle ou cloud ; voir [docs/deployment.md](docs/deployment.md) pour la configuration.

## Licence

Business Source License 1.1 (non modifiée, selon les engagements de Licensor de MariaDB). Chaque
version publiée se convertit en Change License (GPL v2.0 ou ultérieure) au 4e
anniversaire de sa publication publique ; le code actuel et récent reste sous BSL.
L'usage en production au-delà des seuils de l'Additional Use Grant (moins de 100
employés/contractants et moins de 1 M$ de revenu de l'année précédente) nécessite une licence
commerciale. Voir [LICENSE](LICENSE).

Le Licensor ne consent pas à l'utilisation de ce travail pour l'entraînement IA/ML. Voir
[NOTICE](NOTICE), [ai.txt](ai.txt), et [robots.txt](robots.txt). Pour les licences commerciales
ou d'entraînement IA : <kennethstott@gmail.com>
