# Provisa

**Connectez vos bases de données. Interrogez-les en GraphQL, gRPC, SQL ou MCP — par n'importe quelle API ou protocole — en 5 minutes.**

Provisa dessert toutes les surfaces d'API (REST, GraphQL, SQL, gRPC, MCP et d'autres encore) sur le résultat joint de vos sources. Il le peut parce qu'il est une **couche sémantique active** : une définition unique de votre patrimoine de données — chaque domaine, chaque relation et chaque politique de vos sources, à l'exclusion des seuls systèmes d'origine eux-mêmes — qui à la fois exploite le patrimoine et le gouverne. Cette définition n'est pas une documentation qu'un moteur peut consulter ; elle *est* le moteur. Les domaines et relations enregistrés sont les seuls chemins de jointure licites, et les politiques d'accès sont compilées dans chaque plan de requête. Un modèle, trois rôles :

- **Définir** — Domaines, colonnes et relations sont déclarés une seule fois. Cette déclaration est le schéma que voit chaque consommateur et le seul jeu de chemins de jointure qu'une requête peut emprunter.
- **Appliquer** — Sécurité au niveau des lignes, masquage de colonnes, visibilité des colonnes et approbation des requêtes s'appliquent en ligne sur le chemin d'exécution. Aucune requête n'atteint les données sans les traverser : la couverture est totale par construction, non par diligence.
- **Auditer** — Parce que chaque demande emprunte le même chemin gouverné, qui a interrogé quoi, sous quel rôle et contre quelle politique est consigné uniformément. Traces distribuées, métriques et journaux sont eux-mêmes enregistrés comme tables interrogeables aux côtés de vos données métier.

Un noyau gouverné unique dessert chaque langage et chaque transport. Interrogez en **GraphQL, Cypher ou SQL** ; consommez via **pgwire, Bolt, gRPC, REST, Arrow Flight ou JDBC**. Chaque langage de requête s'abaisse vers une représentation intermédiaire unique où la gouvernance est injectée une seule fois — de sorte qu'une politique ne peut pas dériver d'un langage à l'autre — et cette RI se recible vers le dialecte natif de chaque source à la sortie. Ajouter un langage, c'est ajouter une nouvelle façade au noyau partagé, non un nouveau moteur.

Le patrimoine est à la fois analytique et transactionnel. Les lectures inter-sources se déploient à travers la couche de fédération ; les écritures et les lectures mono-source sont routées directement vers le pilote de la source — gouvernées à l'identique, mais transactionnelles et sous les 100 ms. Le streaming colonnaire Arrow Flight est intégré.

Le modèle tout entier est bâti sur une poignée de primitives — domaines, relations, rôles et politiques. Vocabulaire réduit, donc définition facile à comprendre et simple à évaluer et à auditer : vous pouvez lire le jeu de politiques et savoir ce qu'il fait. Provisa est un compilateur de requêtes léger, non un runtime posté sur le chemin des données. Il convertit une demande en requêtes natives, les route, puis s'efface — c'est pourquoi le patrimoine reste performant.

Cette conception permet deux façons de l'employer, qui ne s'excluent pas :

- **Comme échafaudage de modernisation** — Modélisez votre patrimoine, laissez Provisa générer le SQL natif de chaque source, puis récupérez ce SQL et adoptez-le directement dans le système cible. Provisa est la couche de transition, non une dépendance permanente.
- **Comme infrastructure permanente d'application des politiques** — Gardez-le en place comme le chemin gouverné qu'emprunte chaque requête, afin que définition, application et audit restent unifiés aussi longtemps que le patrimoine existe.

## Le modèle de fédération

Le modèle tout entier tient en deux contrats et deux politiques : les sources se réduisent à des tables 2D sur un seul système de types, les requêtes se réduisent à une RI de type SQL, l'atteignabilité décide de ce qui est interrogé en direct plutôt que matérialisé, et une stratégie de fraîcheur gouverne chaque copie matérialisée et chaque jeu de données dérivé. Forme des données en entrée, forme des requêtes en entrée, gouvernance à la jointure, requêtes natives en sortie. La suite de cette section parcourt chaque pièce.

Le modèle repose sur une réduction : chaque source est exprimée comme une collection de tables bidimensionnelles sur un système de types unique et généralisé. C'est le contrat qu'une source doit remplir pour rejoindre le patrimoine, et c'est le même pour toutes. Certaines s'y conforment déjà — une table MySQL ou PostgreSQL *est* une relation 2D typée. Certaines s'y conforment moyennant une projection : un résultat de requête GraphQL, une fois aplati, est une table. Certaines sont étrangères à cette forme — triplestores SPARQL, Neo4j — mais restent exploitables, car l'utilisateur fournit une requête dont le jeu de résultats est tabulaire ; la requête est l'adaptateur. Quelle que soit la source, le patrimoine ne voit que des lignes, des colonnes et des types généralisés, rien d'autre. Intégrer un nouveau genre de source, c'est remplir ce seul contrat, parfois avec une étape d'intervention humaine, non écrire une intégration sur mesure.

Cette réduction a sa jumelle du côté des requêtes. SQL — à travers tous ses dialectes et ses particularités — est essentiellement le langage de l'analyse sur des jeux de données 2D, ce qui fait d'une forme de type SQL la cible universelle naturelle des requêtes. Ainsi chaque demande, dans quelque langage qu'elle arrive, est abaissée vers cette représentation intermédiaire dès la toute première étape. Certaines s'abaissent proprement — SQL lui-même, et même GraphQL ; certaines sont ardues — les sémantiques de chemins et de graphes de Cypher demandent un vrai travail — mais toutes sont réalisables. Faire converger chaque demande vers une RI unique avant toute autre chose est ce qui permet à la gouvernance de s'appliquer en exactement un endroit, sur une seule forme, quel que soit le langage d'arrivée.

Par-dessus ces deux formes uniformes — sources tabulaires et forme de requête unique — la fédération signifie ici à la fois requête en direct et entreposage — la même étendue que couvre un moteur de requête en direct comme Trino, plus la matérialisation sur laquelle ces moteurs s'appuient. Le concept qui les unifie est l'**atteignabilité** : pour une source donnée, le moteur peut-il l'interroger sur place, ou faut-il d'abord matérialiser ses données quelque part d'interrogeable ? L'atteignabilité partitionne le patrimoine entre ce qui est interrogé en direct et ce qui est d'abord copié.

La plupart des bases de données portent déjà une notion de lien en direct — `ATTACH` de DuckDB, `postgres_fdw` de PostgreSQL, liens externes de Databricks. La plupart des bases peuvent donc jouer, à un certain degré, le rôle de moteur de fédération. Aucune n'est exhaustive : chacune atteint un ensemble particulier de sources et matérialise le reste, sans aucun relevé unique de ce qui relève de l'un ou de l'autre. Le modèle comble cette lacune en rendant l'atteignabilité explicite — un ensemble défini de méthodes, par source, énonçant ce que le moteur peut atteindre en direct et, par élimination, ce qu'il faut matérialiser.

Reste la fraîcheur : pour chaque source non atteignable, à quel point sa copie matérialisée doit-elle être à jour ? En pratique cela se réduit à un petit ensemble de stratégies — à la demande, sur un calendrier, sur un signal de changement (CDC, filigrane, instantané), ou figée. En choisir une par source constitue toute la politique de fraîcheur.

Les jeux de données analytiques — tables dérivées, agrégats, sorties d'une transformation — se rangent dans la même forme. Eux aussi doivent être exprimés dans la RI, et parce qu'ils le sont, la traçabilité n'est pas un système distinct à entretenir : le chemin de chaque système d'origine jusqu'à une sortie finale *est* la RI qui l'a produite, lisible de bout en bout. Les construire soulève la question de la fraîcheur à un cran de distance — le jeu de données se rafraîchit-il sur un calendrier, seulement une fois ses conditions préalables satisfaites, en continu quasi temps réel, ou comme instantané historique figé ? Les façons d'exprimer comment et quand construire un jeu de données forment le même ensemble restreint et énumérable, si bien qu'un jeu de données dérivé porte une politique de construction exactement dans le vocabulaire qu'emploie une copie de source.

Les modèles dimensionnels en sont une application directe. Les tables de faits et de dimensions d'un schéma en étoile sont des jeux de données analytiques comme les autres — une dimension est une projection conformée et dédoublonnée ; une table de faits est une jointure et une agrégation réduites à un grain — chacune portant sa propre politique de construction et de fraîcheur. Les dimensions à évolution lente n'exigent aucune machinerie particulière : un instantané figé, c'est l'historisation de type 2 ; une reconstruction planifiée, c'est le type 1. Et parce que le schéma est défini dans la RI plutôt que lié physiquement aux tables d'un seul entrepôt, les mêmes définitions de faits et de dimensions se reciblent — matérialisées dans Oracle, dans Databricks, ou laissées virtuelles sur un moteur MPP — sans remodélisation. Le modèle génère le schéma en étoile ; il ne le rive pas à un moteur.

Data Vault s'y insère de la même façon, une couche plus tôt. Ses hubs sont des jeux de données de clés métier dédoublonnées, ses links sont les relations enregistrées entre eux, et ses satellites sont des jeux de données d'attributs horodatés en insertion seule — l'enregistrement historique. Un satellite n'est qu'un jeu de données dérivé sur la stratégie de fraîcheur par signal de changement : date de chargement plus hashdiff, c'est du CDC appliqué à des attributs descriptifs, et l'historique en insertion seule, c'est la stratégie d'instantané figé. Les tables point-in-time et bridge sont d'autres jeux de données dérivés construits pour la performance des requêtes. Un raw vault est donc un ensemble de jeux de données analytiques dans la RI, et un schéma en étoile en est une projection — tous deux générés, tous deux portables d'un moteur à l'autre. Ce que le modèle ne fait pas, c'est trancher la méthodologie : ce qui devient un hub, le grain d'un satellite, la stratégie de découpage. Ce sont des choix de modélisation ; une fois faits, ils vivent comme de la RI portable plutôt que comme de l'ETL soudé à un seul entrepôt.

Les deux patrons se déclarent au moyen de **deux raccourcis de première classe** plutôt que de vues écrites à la main — les primitives dont tout schéma en étoile et tout Data Vault sont faits, gardées neutres du point de vue de la méthodologie :

- **`entity`** — une projection d'une source, clefée, dédoublonnée, facultativement historisée. Déclarez une clé d'entité, les attributs et un mode d'historisation ; Provisa l'abaisse vers une vue matérialisée, et lorsque l'historisation est demandée vers une **MV bitemporelle** (`scd2` → delta, `snapshot` → instantané). Une seule construction sert une **dimension** Kimball (SCD1/SCD2) et un **hub + satellite** Data Vault.
- **`fact`** — une jointure vers des clés d'entités, réduite à un grain déclaré, avec des mesures agrégées. Provisa l'abaisse vers une MV d'agrégat plus des relations enregistrées vers les entités. Une seule construction sert une **table de faits** en étoile et un **link** Data Vault (un fait sans mesure est un pur link d'ensemble de clés).

Parce que l'abaissement est pur — une spécification `entity`/`fact` devient exactement les définitions de MV, de bitemporalité et de relations qu'un modélisateur écrirait sinon à la main —, l'entrepôt est de la RI de bout en bout et se recible d'un moteur à l'autre sans remodélisation. Déclarez un entrepôt dans l'interface d'administration (un formulaire **Model** pour les entités et les faits) ou via l'API d'administration (`registerEntity` / `registerFact`) ; le modèle *génère* l'étoile Kimball ou le Data Vault, il n'en impose aucun.

### Voyage dans le temps

Le voyage dans le temps est une idée simple — conserver chaque version d'une ligne au lieu de l'écraser, afin de pouvoir demander ce que la donnée *était* à n'importe quel instant passé. Ce qui diffère, c'est l'efficacité avec laquelle chaque moteur sait le faire, et c'est exactement pourquoi Provisa en fait une propriété de la **définition** de la vue matérialisée plutôt que du moteur de stockage (REQ-1162). Déclarez-le une fois ; il fonctionne sur tout backend de matérialisation.

La règle qui le maintient portable est l'**insertion seule** : une version, une fois écrite, n'est jamais mise à jour ni supprimée. Retirer une ligne en réécrivant une date de fin de validité — l'astuce bitemporelle habituelle — demande un UPDATE, que beaucoup de moteurs ne savent pas faire à bon compte (voire pas du tout) sur un magasin fédéré ; Provisa s'en abstient donc. Chaque rafraîchissement **ajoute**, et « quelle version était en vigueur à l'instant T » est dérivé à la lecture depuis le journal immuable. Il y a exactement deux façons d'ajouter :

- **Instantané** — ajouter tout le jeu de données frais, estampillé du temps système de ce rafraîchissement. Aucun calcul de différence ; correct sur tout moteur ; le stockage croît d'une copie complète par rafraîchissement.
- **Delta** — n'ajouter que ce qui a changé, plus des pierres tombales pour les clés supprimées. Le delta est **calculé par le moteur** (anti-jointures au sein d'un `INSERT … SELECT`), jamais replié ligne à ligne dans Provisa. Plus compact, et il exige une clé d'entité.

Le temps système (quand Provisa a enregistré une version) est géré ainsi ; le temps de validité (quand un fait est vrai dans le métier) est fourni par le SELECT propre à la vue et préservé. Les moteurs qui offrent davantage — instantanés Iceberg natifs, un MERGE qui entretient moins de lignes — peuvent être ciblés pour l'efficacité derrière la même déclaration ; le chemin en insertion seule est le plancher correct partout.

La lecture est transparente. Une requête ordinaire contre une MV bitemporelle reconstruit par défaut l'état **courant** depuis le journal d'ajouts ; pour voyager dans le temps, envoyez un en-tête `X-Provisa-As-Of: <timestamp>` et toute la requête est répondue telle que le patrimoine était à cet instant — sémantiques identiques sur chaque substrat. Activez-le pour n'importe quelle vue matérialisée dans l'interface d'administration (un contrôle **Time Travel** : arrêt / instantané / delta, plus une clé d'entité) ou via l'API d'administration.

Atteignabilité plus fraîcheur constitue un modèle général de fédération de données : une définition qui dit ce qui est en direct, ce qui est matérialisé et à quel point chaque copie reste fraîche — indépendamment de la portée d'un moteur particulier. Le résultat est l'affranchissement du verrouillage propriétaire. Le modèle est portable ; le patrimoine n'est pas captif du fournisseur dont la fédération atteint le plus de sources aujourd'hui.

## Fonctionnalités

### Interfaces de requête

Ce sont les langages et les API structurées dans lesquels vous écrivez vos requêtes. Chacun a sa syntaxe et ses sémantiques ; la gouvernance (RLS, masquage, visibilité des colonnes, application des relations) s'applique uniformément à tous, quel que soit le protocole de transport qui les achemine.

- **GraphQL** — Schémas par rôle avec visibilité au niveau des champs, filtrage, pagination par curseur et requêtes d'agrégat (`count`, `sum`, `avg`, `min`, `max`). Contraint par le schéma aux relations enregistrées — structurellement valide par construction, le chemin le plus rapide vers une requête simple correcte. Apollo APQ inclus : les requêtes sont hachées et enregistrées côté serveur ; les appels suivants n'envoient que le hachage en HTTP GET, rendant les réponses cachables par CDN sans aucun changement client. Les tables de correspondance sous un seuil de lignes configurable sont exposées comme types énumérés.
- **SQL** — SQL complet sur les données fédérées ; sans contrainte et plus expressif que GraphQL. Écrivez du SQL standard — sous-requêtes corrélées comprises — et il s'exécute inchangé à travers les sources. Les requêtes mono-source contournent entièrement la couche de fédération (sous les 100 ms).
- **Cypher** — Langage de requête de graphe sur le même schéma fédéré. Parcourez les relations comme des arêtes de graphe ; unissez les sources ; chemins de longueur variable. La gouvernance s'applique à l'identique de GraphQL et SQL.
- **API de modèle gRPC** — `.proto` auto-généré depuis le schéma enregistré ; RPC typés de requête et d'insertion par table, réponses en flux. Piloté par le schéma au même sens que GraphQL — le modèle d'enregistrement est le contrat, protobuf est l'encodage de transport. À la différence d'Arrow Flight (qui est un transport de streaming colonnaire), il s'agit d'une interface de requête complète par table.
- **JSON:API** — API de requête structurée à `/data/jsonapi/{table}`, HTTP uniquement par conception. Prend en charge JSON:API 1.1 : jeux de champs partiels (`fields[table]=col1,col2`), expressions de filtre (`filter[field][op]=value`), documents composés (`include=relation`) et tri. Ce n'est pas un langage de requête généraliste — il interroge une table à la fois avec une syntaxe de filtre normalisée plutôt qu'une chaîne de requête ad hoc.
- **Explorateur de langages de requête** — Écrivez une requête GraphQL et voyez ses traductions **Semantic SQL** et **Cypher** en direct dans des panneaux latéraux ; copiez l'une ou l'autre ou passez directement dans l'éditeur SQL ou Graphe. Un flux de travail pratique consiste à esquisser des fragments de requête en GraphQL, puis à assembler le SQL obtenu en vues ou rapports complexes.

L'explorateur montre une requête GraphQL à côté de ses traductions SQL et Cypher en direct :

![Explorateur de langages de requête](docs/images/query-explorer.png)

Le même schéma fédéré est explorable comme un graphe vivant — étiquettes de domaines et de nœuds, types de relations et parcours de longueur variable :

![Visualisation du graphe](docs/images/graph-view.png)

### Outils de composition de requêtes

Ces outils vous aident à écrire des requêtes dans les langages ci-dessus — ce ne sont pas eux-mêmes des langages de requête.

- **Requête en langage naturel** — Pipeline LN→SQL/Cypher/GraphQL propulsé par Claude. Décrivez ce que vous voulez en français courant ; le pipeline produit une requête dans le langage de votre choix, avec une boucle de validation interactive avant exécution.

![Requête en langage naturel](docs/images/natural-language.png)

### Protocoles de transport

Ce sont les protocoles de connexion. SQL, GraphQL et Cypher circulent dessus — le choix du protocole de transport ne change ni l'interface de requête ni le comportement de gouvernance.

- **pgwire** — N'importe quel client PostgreSQL (psql, DBeaver, DataGrip, asyncpg, SQLAlchemy, `read_sql` de pandas) se connecte sur le port 5439 comme s'il s'agissait d'un serveur Postgres. N'accepte que du SQL. Le pipeline de gouvernance complet s'applique. `pg_catalog` et `information_schema` sont répondus depuis un catalogue en mémoire, si bien que les explorateurs de schéma fonctionnent sans aller-retour vers la fédération. TLS facultatif.
- **Bolt (Neo4j)** — N'importe quel client Neo4j (Neo4j Browser, Bloom, pilotes officiels) se connecte via le protocole Bolt et exécute du Cypher contre le graphe fédéré. Chaque rôle que détient l'utilisateur apparaît comme une base `provisa_<role>`. Même gouvernance que sur tout autre transport. TLS facultatif.
- **Arrow Flight** — Streaming colonnaire à haut débit sur gRPC ; accepte GraphQL ou SQL comme entrée de requête. Jeux de résultats non bornés, aucune matérialisation côté serveur, aucune infrastructure distincte requise.
- **JDBC** — Intégration aux outils de BI (Tableau, Power BI, DBeaver) en mode `approved` ou `catalog`.
- **WebSocket / SSE** — Abonnements : événements de changement en quasi temps réel ; backends : PG natif, MongoDB natif, CDC, interrogation périodique. Également exposés via Kafka.

### Sources de données

- **53 types de sources** — PostgreSQL, MySQL, MongoDB, Cassandra, Elasticsearch, Neo4j, triplestores SPARQL, Kafka, Google Sheets et d'autres encore à travers une seule API ; les sources graphe et RDF sont de première classe, non des adaptateurs
- **Routage intelligent** — Les requêtes mono-source contournent la fédération (sous les 100 ms) ; les requêtes multi-sources passent par la couche de fédération — apportez votre propre cluster ou employez les workers embarqués
- **Sources API** — Enregistrez des endpoints REST, GraphQL, gRPC, WebSocket ou RSS comme tables interrogeables ; assistants SPARQL inclus ; les jointures fédérées entre sources API et sources relationnelles fonctionnent de façon transparente
- **Introspection de schémas distants** — Pointez vers n'importe quel endpoint GraphQL, OpenAPI ou gRPC ; les opérations documentées sont automatiquement exposées comme tables interrogeables, nœuds et arêtes de graphe, avec toute la gouvernance appliquée par-dessus
- **Sources fichiers** — Fichiers CSV, Parquet et SQLite comme tables interrogeables ; prend en charge les chemins locaux et le stockage objet distant (`s3://`, `ftp://`, `sftp://`)
- **Intégration Kafka** — Topics comme tables en lecture seule ; résultats de requêtes comme receveurs Kafka
- **Déclencheurs planifiés** — Déclencheurs cron et par intervalle (APScheduler) qui appellent des webhooks, des mutations ou des publications vers un receveur Kafka
- **Indications de fédération** — Des indications de routage en commentaire SQL surchargent les décisions de routage automatiques

![Sources de données](docs/images/data-sources.png)

Sources, fichiers et endpoints distants sont enregistrés comme tables gouvernées depuis l'interface :

![Enregistrement de table](docs/images/table-registration.png)

### Sécurité et gouvernance

- **Sécurité au niveau des lignes** — Injection de clause WHERE par table et par rôle
- **Masquage de colonnes** — Masquage par colonne (expression régulière, constante, troncature) avec contournement selon le rôle
- **Valeurs prédéfinies de colonnes** — Valeurs statiques ou de variable de session injectées côté serveur à l'insertion et à la mise à jour ; non exposées dans les types d'entrée des mutations
- **Permissions d'écriture** — Contrôle d'accès aux mutations par colonne (`writable_by`)
- **Rôles hérités** — Les rôles héritent récursivement du RLS, de la visibilité et du masquage d'un rôle parent
- **Fonctions et webhooks suivis** — Fonctions de base de données et webhooks sortants exposés comme mutations GraphQL avec des formes de retour typées
- **Point d'ancrage d'approbation ABAC** — Point d'ancrage d'autorisation avant exécution ; transport webhook, gRPC ou unix_socket ; portée par table, par source ou globale ; politique de repli configurable
- **Authentification enfichable** — Firebase, Keycloak, OAuth 2.0, simple (pour les tests)

![Rôles de sécurité](docs/images/security-roles.png)

### Livraison et performance

- **Vues matérialisées comme transformations enregistrées** — Une MV capture la transformation qui l'a produite : sa forme de jointure ou son SQL, les signaux d'entrée par source (instantané Iceberg, filigrane de SGBD) à partir desquels elle a été construite, et un contrôle de déterminisme à l'enregistrement. Parce que la transformation est enregistrée, les requêtes (ou sous-expressions) sont réécrites de façon transparente sur une MV fraîche — correspondance structurelle de motif de jointure avec prise en charge des correspondances partielles, si bien qu'une MV couvrant un sous-ensemble de jointures s'applique tout de même, les jointures restantes étant préservées
- **Inlining des tables chaudes** — Les petites tables de correspondance fréquemment jointes sont insérées comme CTE VALUES directement dans le plan de requête, éliminant les allers-retours inter-sources pour les données dimensionnelles
- **Cache de requêtes** — Cache de résultats Redis partitionné par rôle et RLS ; cache de hachages APQ inclus
- **L'observabilité comme donnée** — Traces distribuées, métriques et journaux sont collectés via OpenTelemetry, compactés en Iceberg sur S3 et automatiquement enregistrés comme tables interrogeables (`traces`, `metrics`, `logs`, `queries`) dans le schéma fédéré ; interrogez-les en SQL, GraphQL ou Cypher aux côtés de vos données métier — joignez une table `customers` à la table `queries` pour voir qui a exécuté quoi et combien de temps cela a pris

### Administration et intégration

- **API d'administration** — GraphQL à `/admin/graphql` ; téléversement/téléchargement de configuration, édition des relations, approbation des requêtes
- **Visionneuse de rapports** — `/admin/reports` liste les vues de gestion intégrées du domaine ops ainsi que tout rapport personnalisé enregistré ; exige la capacité `observability`
- **Aperçu de table** — chaque table enregistrée dispose d'une visionneuse de données gouvernée, paginée côté serveur, avec filtres poussés vers la source, regroupement multi-niveaux et export CSV
- **GraphQL Voyager** — Visualisation interactive du schéma, cadrée par rôle, sous forme de diagramme entité-relation
- **Découverte de relations par LLM** — Suggestions de clés étrangères candidates propulsées par Claude
- **Client Python** — `pip install provisa-client` ; GraphQL/SQL → DataFrames, Arrow Flight → tables pyarrow, dialecte SQLAlchemy, prise en charge d'ADBC
- **Ingestion de données** — Endpoints HTTP pour pousser des données d'événements JSON dans la plateforme
- **Import Hasura v2 / DDN** — Convertit des métadonnées Hasura v2 ou un YAML de supergraphe DDN en configuration Provisa
- **Apollo Federation** — Expose Provisa comme sous-graphe Apollo Federation v2

Schéma cadré par rôle, visualisé comme diagramme entité-relation (GraphQL Voyager) :

![Voyager du schéma](docs/images/schema-voyager.png)

Les relations sont enregistrées, approuvées et appliquées comme les seuls chemins de JOIN licites :

![Relations](docs/images/relationships.png)

## Modèle de sécurité

C'est ici que « sur le chemin qu'emprunte déjà chaque requête » cesse d'être un slogan. Provisa applique un modèle de sécurité multicouche à travers chaque langage de requête (GraphQL, SQL, Cypher) et chaque transport (REST, gRPC, Arrow Flight, JDBC, pgwire, Bolt, WebSocket). La gouvernance s'applique uniformément — aucun chemin de requête ne la contourne. La couverture est totale par construction, non par diligence : ajoutez une source, une colonne ou une relation et chaque couche s'y applique automatiquement, sans rien à penser à enregistrer.

Les couches s'appliquent dans l'ordre. Une demande doit franchir chaque couche avant que la suivante soit évaluée.

### Couche 0 — Filtrage de l'introspection

Le schéma et le catalogue présentés à un rôle ne contiennent que les tables de sa liste `domain_access` et les colonnes qui passent les règles `visible_to` par colonne. Les objets hors de l'accès d'un rôle sont invisibles au moment de la découverte — ils ne peuvent être ni interrogés, ni autocomplétés, ni supposés exister. Cela vaut pour le schéma GraphQL, le catalogue SQL et l'explorateur de schéma de l'éditeur de requêtes.

### Couche 1 — Accès public

Les tables des domaines sans restriction `domain_access` sont visibles de toutes les identités authentifiées, sans configuration supplémentaire. Zéro friction pour les données réellement publiques.

### Couche 2 — Accès au domaine

Chaque rôle porte une liste `domain_access` d'identifiants de domaines. Une requête qui touche une table hors de ces domaines est rejetée avant exécution. C'est la limite grossière de propriété — un rôle RH ne peut pas atteindre les tables financières, quelle que soit la façon dont le SQL est écrit.

### Couche 3 — Sécurité au niveau des lignes

Une fois l'accès au domaine confirmé, des prédicats `WHERE` par table et par rôle sont injectés dans chaque `SELECT` au moment de l'exécution. Les prédicats s'évaluent contre les données brutes. Un responsable régional interrogeant une table de commandes partagée ne voit que les lignes de sa région, même sur un `SELECT *`.

### Couche 4 — Visibilité et masquage des colonnes

Les colonnes dont la liste `visible_to` exclut le rôle demandeur sont retirées de la sortie de requête. Les colonnes portant une règle de masquage voient leurs valeurs remplacées — caviardage par expression régulière, remplacement par une constante ou troncature — avant que les résultats quittent le serveur. Le masquage s'applique dans tous les langages de requête et tous les formats de sortie.

### Couche 5 — Garde des prédicats

Les colonnes masquées sont rejetées des clauses `WHERE` et `HAVING`. Sans cela, un appelant pourrait déduire la valeur non masquée en la cherchant par dichotomie dans un filtre, alors même que la sortie est masquée. Le rejet est appliqué à l'analyse de la requête, avant exécution.

### Gouvernance des relations

Les conditions de JOIN en SQL doivent correspondre à une relation enregistrée et approuvée entre tables. Les jointures non approuvées sont rejetées. Chaque relation porte une raison et une description lisibles — un guide, tant pour les utilisateurs que pour les agents autonomes, sur la raison d'être d'un chemin de parcours. C'est une politique de gouvernance, non une frontière de sécurité dure : les couches 2 à 5 tiennent quelle que soit la structure de jointure, si bien qu'un contournement délibéré n'expose pas de données que le rôle ne pourrait atteindre par deux requêtes séparées. Les tentatives de contournement sont journalisées et auditables.

---

Ces couches se composent. Un rôle avec accès au domaine, RLS et colonnes masquées a les cinq contraintes actives simultanément. Ajouter une nouvelle source de données, colonne ou relation n'exige pas de mettre à jour chaque règle — chaque couche se configure indépendamment et s'applique automatiquement à toute requête qui touche des objets gouvernés.

### macOS

1. Téléchargez [Provisa-macOS.dmg](https://provisa.dev/dl/macos) (toujours la dernière version)
2. Faites glisser **Provisa.app** dans `/Applications` et double-cliquez pour lancer
3. Le premier lancement effectue une configuration unique (environ 2 min, sans besoin d'Internet)
4. Ouvrez le Terminal :

```bash
provisa start   # start all services
provisa open    # open the UI in your browser
```

### Linux

1. Téléchargez [Provisa-linux-x86_64.AppImage](https://provisa.dev/dl/linux) (toujours la dernière version)
2. Rendez-le exécutable et lancez-le — le premier lancement effectue une configuration unique (sans besoin d'Internet) :

```bash
chmod +x Provisa-*-linux-x86_64.AppImage
./Provisa-*-linux-x86_64.AppImage
provisa start && provisa open
```

### Windows

1. Téléchargez [Provisa-windows-x64.exe](https://provisa.dev/dl/windows) (toujours la dernière version)
2. Lancez l'installateur — aucun droit d'administrateur requis
3. Ouvrez **Provisa First Launch** depuis le menu Démarrer — effectue une configuration unique (environ 5 min, sans besoin d'Internet)
4. Ouvrez un nouveau terminal :

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

Téléchargez [provisa-jdbc.jar](https://provisa.dev/dl/jdbc) (toujours la dernière version) et ajoutez-le au chemin des pilotes de votre outil de BI.

```text
jdbc:provisa://localhost:8815
```

Authentifiez-vous avec vos nom d'utilisateur et mot de passe Provisa — le serveur attribue votre rôle.

- **Mode `catalog`** — schéma entièrement visible ; à utiliser avec les outils de catalogue (Collibra, Atlan, DBeaver)

Voir [docs/integrations.md](docs/integrations.md) pour les étapes de configuration de Tableau et Power BI.

### Protocole de transport PostgreSQL (pgwire)

Provisa parle le protocole de transport PostgreSQL sur le port 5439. Tout client capable de se connecter à Postgres se connecte à Provisa — sans pilote, sans adaptateur, sans changement à l'outillage existant.

**Le nom d'utilisateur PostgreSQL sélectionne le rôle Provisa.** Avec `provider: none` (mode confiance), le mot de passe est ignoré et tout nom de rôle configuré est accepté comme nom d'utilisateur — connectez-vous en `analyst`, `admin` ou tout autre rôle pour voir la vue gouvernée des données propre à ce rôle. Avec `provider: simple`, le mot de passe est validé par bcrypt. Les autres fournisseurs (`firebase`, `keycloak`, `oauth`) ne sont pas pris en charge sur pgwire.

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

Toutes les requêtes traversent le pipeline de gouvernance complet — accès au domaine, RLS, masquage et garde des prédicats s'appliquent exactement comme pour GraphQL et REST. Les explorateurs de schéma (DBeaver, DataGrip, pgAdmin) fonctionnent d'emblée : les requêtes sur `pg_catalog` et `information_schema` sont répondues depuis un catalogue en mémoire cadré sur l'accès au domaine du rôle, si bien que les utilisateurs ne voient que les tables et colonnes qu'ils sont autorisés à interroger.

DataGrip parcourant le schéma gouverné et son diagramme de clés étrangères via pgwire — sans pilote, sans adaptateur :

![Provisa dans DataGrip via pgwire](docs/images/pgwire-datagrip.png)

Le TLS s'active en définissant `PROVISA_PGWIRE_CERT` et `PROVISA_PGWIRE_KEY`. Le port est configurable via `PROVISA_PGWIRE_PORT` (`5439` par défaut).

### Bolt (protocole de transport Neo4j)

Provisa parle également le protocole **Bolt** de Neo4j, si bien que les outils nativement orientés graphe se connectent directement et exécutent du Cypher contre le graphe fédéré — sans export, sans base de données graphe distincte. Pointez **Neo4j Browser** ou **Bloom** vers Provisa et parcourez les relations à travers les sources avec la même gouvernance (accès au domaine, RLS, masquage) appliquée.

Neo4j Browser exécutant du Cypher contre Provisa — étiquettes de nœuds, types de relations et clés de propriétés viennent directement du schéma enregistré :

![Provisa dans Neo4j Browser via Bolt](docs/images/bolt-neo4j-browser.png)

Activez-le en définissant `PROVISA_BOLT_PORT` (le port par défaut de Neo4j est `7687`). Le TLS s'active avec `PROVISA_BOLT_CERT` et `PROVISA_BOLT_KEY`. Chaque rôle Provisa que détient l'utilisateur authentifié apparaît comme une base `provisa_<role>` sélectionnable (le sélecteur `provisa_admin` ci-dessus) — en choisir une restreint la session aux droits de domaine de ce rôle ; l'utilisateur ne peut jamais dépasser les rôles qu'il détient.

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

| Sujet | Document |
| --- | --- |
| Démarrage rapide développeur (exécution depuis les sources) | [docs/quickstart.md](docs/quickstart.md) |
| Référence complète de la configuration YAML | [docs/configuration.md](docs/configuration.md) |
| Référence des endpoints (GraphQL, REST, Flight, gRPC) | [docs/api-reference.md](docs/api-reference.md) |
| Conception du système et carte des composants | [docs/architecture.md](docs/architecture.md) |
| Modèle de sécurité (RLS, masquage, authentification) | [docs/security.md](docs/security.md) |
| Stockage des secrets et références `${secret:NAME}` | [docs/secrets.md](docs/secrets.md) |
| Glossaire métier et curation des termes | [docs/glossary.md](docs/glossary.md) |
| Environnements (dev / staging / prod) | [docs/environments.md](docs/environments.md) |
| Types de sources pris en charge | [docs/sources.md](docs/sources.md) |
| Abonnements SSE | [docs/subscriptions.md](docs/subscriptions.md) |
| JDBC, outils de BI, clients Arrow Flight, Apollo Federation | [docs/integrations.md](docs/integrations.md) |
| Client Python (`provisa-client`) | [docs/python-client.md](docs/python-client.md) |
| API d'administration | [docs/admin.md](docs/admin.md) |
| Déploiement (Docker Compose, Kubernetes, macOS) | [docs/deployment.md](docs/deployment.md) |
| Import Hasura v2 / DDN | [docs/import.md](docs/import.md) |
| Flux de publication (étiquettes alpha/beta/stable) | [docs/releasing.md](docs/releasing.md) |

## Dimensionnement

Provisa embarque un moteur de fédération pour les requêtes multi-sources. Au premier lancement vous choisissez un budget de RAM ; Provisa en dérive automatiquement le nombre de workers de fédération locaux.

| RAM de l'hôte | Workers | Charge de travail typique |
| --- | --- | --- |
| < 24 Go | 0 | Développement, requêtes mono-source, petites équipes |
| 24–47 Go | 1 | Petite équipe, requêtes inter-sources modérées |
| 48–95 Go | 2 | Déploiement départemental, usage mixte BI + carnets |
| 96 Go et plus | 4 | Grand département, fédération concurrente intensive |

Le nombre de workers peut être changé à tout moment en modifiant `~/.provisa/config.yaml` (`federation_workers: N`) puis en exécutant `provisa restart`. Mettez-le à `0` pour n'exécuter que la coordination (nœud unique).

### Passer à l'échelle au-delà d'une seule machine

**Montée en charge horizontale** — Exécutez plusieurs instances Provisa derrière un répartiteur de charge. Chaque instance est un système pleinement fonctionnel. Toutes les instances doivent pointer vers la même base de configuration (définissez `CONFIG_DB_HOST` sur les machines secondaires) et, facultativement, vers une instance Redis partagée (`REDIS_URL`) pour un cache unifié. La plupart des requêtes se distribuent de façon transparente ; de très grandes jointures inter-sources peuvent dépasser les ressources d'une seule instance et exiger une machine plus grande ou un cluster de fédération externe.

**Redis partagé** — Définissez `REDIS_URL` sur chaque instance pour pointer vers un Redis externe. Un Redis partagé signifie que les entrées de cache d'une instance sont disponibles pour toutes, améliorant le taux de succès à l'échelle du cluster.

**Apportez votre propre cluster de fédération** — Pointez Provisa vers un cluster de fédération externe existant plutôt que vers les workers embarqués. Recommandé pour les déploiements à grande échelle ou en cloud ; voir [docs/deployment.md](docs/deployment.md) pour la configuration.

## Licence

Business Source License 1.1 (non modifiée, selon les engagements du concédant de
MariaDB). Chaque version publiée bascule vers la Change License (GPL v2.0 ou
ultérieure) au 4e anniversaire de sa publication ; le code actuel et récent reste
sous BSL. Un usage en production au-delà des seuils de l'Additional Use Grant
(moins de 100 employés/sous-traitants et moins de 1 M$ de chiffre d'affaires sur
l'année précédente) exige une licence commerciale. Voir [LICENSE](LICENSE).

Le concédant ne consent pas à l'usage de cette œuvre pour l'entraînement d'IA ou
de ML. Voir [NOTICE](NOTICE), [ai.txt](ai.txt) et [robots.txt](robots.txt). Pour
les licences commerciales ou d'entraînement d'IA : <kennethstott@gmail.com>
