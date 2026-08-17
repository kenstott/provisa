# Modèle de sécurité

Provisa applique un modèle de sécurité à plusieurs couches sur tous les langages de requête (GraphQL, SQL, Cypher) et tous les transports (REST, gRPC, Arrow Flight, JDBC, WebSocket). (REQ-001, REQ-266) La gouvernance s'applique de manière uniforme — il n'existe aucun chemin de requête qui la contourne. (REQ-002, REQ-266)

Les couches s'appliquent dans l'ordre. Une requête doit franchir chaque couche avant que la suivante ne soit évaluée.

## Modèle en couches

### Couche 0 — Filtrage de l'introspection

Le schéma et le catalogue présentés à un rôle ne contiennent que les tables de sa liste `domain_access` et les colonnes qui respectent les règles `visible_to` par colonne. (REQ-039) Les objets hors de la portée d'accès d'un rôle sont invisibles au moment de la découverte — ils ne peuvent être ni interrogés, ni autocomplétés, ni même déduits comme existants. (REQ-039) Cela s'applique au schéma GraphQL, au catalogue SQL et au navigateur de schéma de l'éditeur de requêtes. (REQ-039, REQ-363)

Voir [Visibilité du schéma](#visibilite-du-schema).

### Couche 1 — Accès public

Les tables des domaines sans restriction `domain_access` sont visibles par toutes les identités authentifiées, sans configuration supplémentaire. Aucune friction pour les données véritablement publiques.

### Couche 2 — Accès par domaine

Chaque rôle possède une liste `domain_access` d'identifiants de domaine. Une requête touchant une table hors de ces domaines est rejetée avant l'exécution. (REQ-038, REQ-039) Il s'agit de la limite de propriété à gros grain — un rôle RH ne peut pas atteindre des tables de finance, quelle que soit la manière dont le SQL est écrit. (REQ-002)

Voir [Modèle des droits](#modele-des-droits).

### Couche 3 — Sécurité au niveau des lignes

Une fois l'accès au domaine confirmé, des prédicats `WHERE` par table et par rôle sont injectés dans chaque `SELECT` au moment de l'exécution. (REQ-041, REQ-263) Les prédicats sont évalués sur les données brutes. Un responsable régional interrogeant une table de commandes partagée ne voit que les lignes de sa région, même avec un `SELECT *`. (REQ-264)

Voir [Sécurité au niveau des lignes (RLS)](#securite-au-niveau-des-lignes-rls).

### Couche 4 — Visibilité et masquage des colonnes

Les colonnes dont la liste `visible_to` exclut le rôle demandeur sont retirées du résultat de la requête. (REQ-040, REQ-263) Les colonnes soumises à une règle de masquage voient leurs valeurs remplacées — rédaction par expression régulière, remplacement par une constante ou troncature — avant que les résultats ne quittent le serveur. (REQ-263) Le masquage s'applique dans tous les langages de requête et tous les formats de sortie. (REQ-263)

Voir [Modèle des autorisations de colonne](#modele-des-autorisations-de-colonne) et [Masquage au niveau des colonnes](#masquage-au-niveau-des-colonnes).

### Couche 5 — Protection des prédicats

Les colonnes masquées sont rejetées dans les clauses `WHERE` et `HAVING`. (REQ-263) Sans cela, un appelant pourrait déduire la valeur non masquée en la recherchant par dichotomie dans un filtre, même si le résultat affiché est masqué. Le rejet est appliqué au moment de l'analyse de la requête, avant l'exécution. (REQ-531)

### Gouvernance des relations (V002)

Les conditions JOIN en SQL doivent correspondre à une relation enregistrée et approuvée entre les tables. (REQ-001) Les jointures non approuvées sont rejetées. Chaque relation porte un motif et une description lisibles par un humain — une orientation destinée aussi bien aux utilisateurs qu'aux agents autonomes sur la raison d'être d'un chemin de parcours. Il s'agit d'une politique de gouvernance, non d'une limite de sécurité stricte : les couches 2 à 5 restent effectives quelle que soit la structure de la jointure, de sorte qu'un contournement délibéré n'expose pas de données que le rôle n'aurait pas pu atteindre au moyen de deux requêtes distinctes. Les tentatives de contournement sont journalisées et auditables.

**Mécanismes de contournement** — V002 peut être contourné de deux façons. La première est une capacité : un rôle détenant `ignore_relationships` effectue des jointures sur des relations que le catalogue ne couvre pas. Parmi les rôles système préinstallés, seul `modeler` la détient — le rôle de découverte dont le travail consiste à déterminer le modèle plutôt qu'à l'appliquer. (REQ-1297) `analyst` ne la détient pas. [tool-verified: `provisa/core/db.py:84`]

La seconde est un retrait volontaire à deux conditions, toutes deux requises :

1. **Indicateur de rôle** — `relationship_guard: false` dans la définition du rôle (valeur par défaut : `true`). [tool-verified: `provisa/core/models.py:349`]
2. **Exclusion par requête** — le SQL contient le commentaire `--relationship-guard=false`. [tool-verified: `provisa/compiler/params.py:80`]

L'indicateur de rôle seul ne contourne pas V002 ; le commentaire seul ne contourne pas V002.

**Le mode haute sécurité verrouille la protection.** Sous `security.mode: high`, aucun des deux contournements ne s'applique : `ignore_relationships` est ignoré, `relationship_guard: false` est ignoré, et chaque jointure doit exister dans le catalogue des relations approuvées. (REQ-693) Il s'agit d'une redondance délibérée — un rôle de production auquel la capacité a été accordée par erreur ne peut toujours pas sortir du modèle. [tool-verified: `provisa/pgwire/_pipeline.py:377`]

**Chemin GraphQL** — V002 est systématiquement ignoré pour les requêtes GraphQL. Les relations définies en SDL sont préapprouvées par conception ; la vérification est redondante et n'est pas appliquée. [tool-verified: `provisa/api/data/endpoint.py:468`]

**Chemins SQL et Cypher** — V002 est actif par défaut. `endpoint_dev.py` et `cypher_router.py` appliquent tous deux la vérification à deux conditions avant d'appeler `validate_sql`. [tool-verified: `provisa/api/data/endpoint_dev.py:127`, `provisa/api/rest/cypher_router.py:260`]

**Chemin pgwire** — même vérification à deux conditions que pour SQL. Le commentaire `--relationship-guard=false` est retiré de la requête avant l'exécution ; il n'atteint jamais la base de données. [tool-verified: `provisa/pgwire/_pipeline.py:60`]

---

Ces couches se combinent entre elles. Un rôle disposant d'un accès par domaine, de RLS et de colonnes masquées a les cinq contraintes actives simultanément. L'ajout d'une nouvelle source de données, d'une colonne ou d'une relation ne nécessite pas la mise à jour de chaque règle — chaque couche est configurée indépendamment et s'applique automatiquement à toute requête touchant des objets gouvernés.

---

## Modèle des droits

Des capacités attribuées indépendamment, avec une hiérarchie de rôles facultative via `parent_role_id`. `admin` les accorde toutes. (REQ-042)

| Capacité | Description |
| ----------- | ------------- |
| `source_registration` | Enregistrer des sources de données |
| `table_registration` | Enregistrer des tables, des colonnes |
| `create_relationship` | Définir des relations de clé étrangère |
| `access_config` | Configurer le RLS, le masquage |
| `query_development` | Exécuter des requêtes |
| `write` | Invoquer des mutations enregistrées (contrôle à gros grain ; voir Autorisation des mutations) |
| `full_results` | Contourner les limites d'échantillonnage |
| `ignore_relationships` | Contourner la gouvernance des relations (V002). Détenue par `modeler` uniquement parmi les rôles système, et entièrement ignorée en mode haute sécurité |
| `admin` | Superutilisateur — accorde toutes les capacités |

### Héritage des rôles

Les rôles peuvent hériter des capacités et de l'accès par domaine d'un rôle parent via `parent_role_id`. (REQ-215) La hiérarchie est aplatie au démarrage — les rôles enfants fusionnent les capacités et l'accès par domaine de leur parent avec les leurs. (REQ-215)

```yaml
roles:
  - id: basic_user
    capabilities: [query_development]
    domain_access: [public]
  - id: analyst
    capabilities: [full_results]
    domain_access: [sales, analytics]
    parent_role_id: basic_user   # inherits query_development + public domain
```

## Modèle des autorisations de colonne

Chaque colonne dispose d'un modèle d'autorisations à quatre champs contrôlant l'accès en lecture, en écriture et le masquage par rôle. (REQ-042, REQ-249)

### Visibilité à trois niveaux

| Niveau | Condition | Résultat |
| ------ | ----------- | -------- |
| **Masquée (cachée)** | Le rôle n'est pas dans `visible_to` | Colonne absente du SDL GraphQL |
| **Masquée (données)** | Le rôle est dans `visible_to`, une règle de masquage existe, le rôle n'est pas dans `unmasked_to` | Colonne visible mais données masquées en SQL |
| **Non masquée** | Le rôle est dans `visible_to` ET le rôle est dans `unmasked_to` (ou aucune règle de masquage) | Accès en lecture complet |

### Autorisations d'écriture

| Champ | Vide signifie | Objectif |
| ------- | ------------ | --------- |
| `visible_to` | Tous les rôles peuvent lire | Contrôle qui voit la colonne (masquée ou non) |
| `unmasked_to` | Aucun rôle ne voit la valeur non masquée | Contrôle qui contourne le masquage |
| `writable_by` | Aucun rôle ne peut écrire | Contrôle qui peut modifier (INSERT/UPDATE) |

L'autorisation d'écriture est appliquée dans le pipeline de mutation. Un rôle absent de `writable_by` reçoit une erreur 403 lorsqu'il tente d'écrire dans une colonne restreinte. (REQ-033, REQ-034)

### Exemple

```yaml
columns:
  - name: email
    visible_to: [admin, analyst, viewer]
    writable_by: [admin]
    unmasked_to: [admin]
    mask_type: regex
    mask_pattern: "(.).*@"
    mask_replace: "$1***@"
  - name: salary
    visible_to: [admin, hr]
    writable_by: [hr]
    unmasked_to: [admin, hr]
    mask_type: constant
    mask_value: "0"
  - name: created_at
    visible_to: []           # all can read
    writable_by: []          # nobody can write (auto-set)
```

Dans cet exemple :

- `email` : admin voit `alice@example.com` et peut modifier ; analyst/viewer voient `a***@example.com`
- `salary` : admin et hr voient la valeur réelle ; hr peut modifier ; tous les autres rôles ne voient pas la colonne du tout
- `created_at` : tout le monde peut lire, personne ne peut écrire

## Autorisation des mutations

Les mutations enregistrées (GraphQL distant, OpenAPI, gRPC, Hasura) sont soumises à deux contrôles indépendants. (REQ-867, REQ-868) Un rôle ne peut invoquer une mutation que s'il possède la capacité globale `write` ET figure dans la liste `writable_by` de cette mutation. (REQ-868) Un `writable_by` vide correspond à un refus par défaut — aucun rôle ne peut l'invoquer. (REQ-867)

Les mutations sont classées comme des écritures par contrat, et non par déclaration de l'appelant. (REQ-869) Un `SELECT` qui référence une fonction de type mutation est promu en écriture et soumis au même contrôle à deux niveaux, de sorte qu'un appelant ne peut pas invoquer une mutation en la déguisant en lecture. (REQ-869) Reclassifier une mutation comme sûre en lecture nécessite la capacité `access_config` et est enregistré comme une décision de gouvernance ; il n'existe aucune exclusion par requête. (REQ-870)

## Visibilité du schéma

Les schémas GraphQL par rôle masquent le contenu non autorisé : (REQ-039)

- **Accès par domaine** : le rôle ne voit les tables que dans ses domaines `domain_access` (`"*"` = tous) (REQ-039)
- **Visibilité des colonnes** : les colonnes absentes de `visible_to` pour un rôle sont omises du SDL (REQ-039)
- Les tables/colonnes non autorisées n'apparaissent pas dans le schéma (REQ-039)

## Sécurité au niveau des lignes (RLS)

Injection de clauses SQL WHERE par table et par rôle. Appliquée après la compilation, avant l'exécution. (REQ-041, REQ-263)

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
```

Le filtre est combiné par ET (AND) dans la clause WHERE de la requête. Fonctionne aussi bien pour les requêtes que pour les mutations (UPDATE/DELETE). (REQ-035, REQ-041)

## Masquage au niveau des colonnes

Le masquage est défini une seule fois par colonne — c'est une propriété de la colonne, pas du rôle. Le champ `unmasked_to` contrôle quels rôles le contournent. (REQ-249)

| Type de masquage | Types pris en charge | Expression SQL |
| ----------- | ---------------- | ---------------- |
| `regex` | Chaîne (varchar, char, text) | `REGEXP_REPLACE(col, pattern, replace)` |
| `constant` | Tous | Valeur littérale (NULL, 0, personnalisée) |
| `truncate` | Date/Timestamp | `DATE_TRUNC(precision, col)` |

Le masquage est répercuté dans la projection SQL SELECT — la base de données renvoie des données masquées. (REQ-263) Les données non masquées ne transitent jamais sur le réseau pour les rôles masqués. (REQ-263) Les colonnes masquées sont également bloquées dans les clauses `WHERE` et `HAVING` (protection des prédicats de la couche 5) afin d'empêcher toute déduction de la valeur non masquée par filtrage. (REQ-263, REQ-531)

## Échantillonnage

Tous les rôles voient des résultats échantillonnés (valeur par défaut : 100 lignes), sauf s'ils disposent de la capacité `full_results`. (REQ-554) Contrôlé via la variable d'environnement `PROVISA_SAMPLE_SIZE`. (REQ-554)

## Journalisation d'audit

Toute requête touchant un actif de domaine est enregistrée dans le `query_audit_log`, en ajout seul. (REQ-596, REQ-613) Chaque ligne capture `tenant_id`, `user_id`, `role_id`, un hachage SHA-256 du texte de la requête, `table_ids`, `source`, `status_code`, `duration_ms` et `logged_at`. (REQ-596) Le texte de la requête n'est jamais stocké tel quel — seul son hachage l'est. (REQ-596)

Le journal est en ajout seul au niveau de la base de données : des règles PostgreSQL bloquent `DELETE` et `UPDATE`. (REQ-596, REQ-613) Deux index — `(tenant_id, logged_at)` et `(user_id, logged_at)` — prennent en charge les requêtes de conformité à portée locataire et par plage temporelle par utilisateur. (REQ-596, REQ-613)

Lorsque le chiffrement est activé, la colonne du hachage du texte de la requête est stockée chiffrée et n'est déchiffrée que lors de lectures administratives autorisées. (REQ-689)

## Limitation de débit

Les limites de débit par rôle sont configurées dans `provisa.yaml` : nombre maximal de requêtes par seconde, nombre maximal d'abonnements SSE simultanés et nombre maximal de flux Arrow Flight simultanés. (REQ-369) Les limites sont appliquées au niveau de la couche API avant la compilation ou l'exécution ; les requêtes dépassant la limite sont rejetées avec un code HTTP 429 et un en-tête `Retry-After`. (REQ-369)

Le service de requête en langage naturel (`POST /query/nl`) dispose d'une limite indépendante via `nl.rate_limit` (requêtes par minute et par rôle). Les requêtes dépassant la limite sont rejetées avant tout appel au LLM. (REQ-370)

L'état des limites de débit réside dans Redis (`cache.redis_url`) sous forme de compteur à fenêtre glissante — sans état par instance — de sorte que les limites s'appliquent sur toutes les instances Provisa horizontales. (REQ-371)

## Authentification

Fournisseurs d'authentification enfichables : (REQ-120)

| Fournisseur | Type de jeton | Cas d'usage |
| ---------- | ----------- | ---------- |
| `none` | En-tête X-Provisa-Role | Développement |
| `basic` | Comptes locaux bcrypt + JWT | Déploiements autonomes |
| `firebase` | Jeton d'identité Firebase | Production |
| `keycloak` | JWT Keycloak | Entreprise |
| `oauth` | JWT OIDC | PingFed, Okta, Azure AD, Auth0 |
| `simple` | bcrypt + JWT | Tests |

Correspondance des rôles : revendications d'identité → rôle Provisa via des règles configurables. (REQ-120) Le champ `assignments_source` contrôle l'origine des attributions de rôle : `claims` les lit dans les revendications (claims) du jeton JWT (valeur par défaut), `provisa` les lit dans le magasin d'attributions interne de Provisa. (REQ-551)

Un superutilisateur configuré dans `provisa.yaml` (nom d'utilisateur plus un mot de passe issu d'un secret d'environnement) reçoit toujours le rôle admin et toutes les capacités, quel que soit le fournisseur configuré — un chemin d'amorçage pour la configuration initiale. (REQ-125)

### Surfaces et identifiants

Chaque surface s'authentifie via le même contrat de fournisseur, si bien qu'un identifiant qui fonctionne sur l'une fonctionne sur toutes, partout où le protocole peut le transporter. (REQ-124, REQ-1263) Ce tableau est la référence unique ; les documents propres à chaque surface ne le répètent pas.

| Surface | Mot de passe | Jeton de fournisseur | Jeton d'accès personnel | Certificat client (mTLS) |
| --------- | ---------- | ---------------- | ----------------------- | --------------------------- |
| HTTP (REST, JSON:API, GraphQL) | `Authorization: Basic` | `Authorization: Bearer` | `Authorization: Bearer` | via un proxy terminateur |
| pgwire | champ mot de passe (en clair ou SCRAM) | champ mot de passe, déploiements OIDC | champ mot de passe | oui |
| Bolt | schéma `basic` | schéma `bearer` | schéma `bearer` | oui |
| Arrow Flight | — | `token` dans le handshake ou la charge utile du ticket | idem | oui |
| gRPC | — | métadonnées `authorization` | métadonnées `authorization` | oui |
| MCP | — | `Authorization: Bearer` | `Authorization: Bearer` | via un proxy terminateur |

Là où une cellule affiche `—`, le protocole ne transporte aucun champ de nom d'utilisateur auquel associer un mot de passe ; les formes à jeton le couvrent. pgwire est le cas miroir : le paquet de démarrage possède un seul champ de secret et aucun schéma, si bien que c'est *ce qu'est* le secret qui choisit la méthode — un PAT est reconnu à son préfixe, le secret est lu comme un jeton bearer lorsque le fournisseur configuré est un fournisseur de jetons, et tout le reste est un mot de passe. Le choix est fait une fois — un identifiant que le validateur retenu refuse n'est pas réessayé auprès d'un autre.

La matrice est imposée par `tests/unit/test_auth_surface_conformance.py`, qui sollicite le vrai point d'entrée de validation de chaque surface et échoue lorsqu'une nouvelle surface est ajoutée sans ligne.

### Jetons d'accès personnels

Un PAT est un secret bearer de longue durée qu'un utilisateur frappe pour un client incapable de mener une connexion interactive — un script, un outil de BI, un pilote. (REQ-1263) Il porte sa propre organisation et son propre rôle, et chaque surface le résout via le même validateur, si bien qu'aucune surface n'a besoin de savoir ce qu'est un PAT.

La forme sur le fil est `provisa_pat_` suivi de 43 caractères base64 compatibles URL. Le préfixe est ce qui achemine un secret présenté vers le magasin de jetons plutôt que vers le fournisseur d'identité, et il rend un jeton fuité repérable dans les journaux et les dépôts.

- **Stockage** — seul le SHA-256 du secret est conservé. Le secret lui-même n'est affiché qu'une seule fois, à la création, et ne peut être récupéré. La liste porte le préfixe d'affichage et les horodatages du cycle de vie, jamais un identifiant utilisable.
- **Émission et révocation** — `POST /auth/tokens`, `GET /auth/tokens`, `DELETE /auth/tokens/{token_hash}`, ainsi que la section en libre-service sur le profil de l'utilisateur dans l'interface d'administration. Frapper et révoquer un identifiant est l'acte de son détenteur.
- **Attribution** — un PAT validé se résout au compte de son propriétaire : identifiant utilisateur, courriel et nom affiché. Une ligne d'audit ou un rapport d'usage écrit sous un PAT nomme donc la personne, pas l'identifiant. Lequel des jetons de cette personne a agi est porté séparément, dans `raw_claims["token_name"]`.
- **Expiration** — un jeton peut porter une expiration ; un jeton expiré est refusé à la validation. Supprimer l'appartenance d'un utilisateur révoque ses jetons du même coup.

### SCRAM-SHA-256 sur pgwire

Sous le fournisseur `basic`, définir `auth.scram: true` fait annoncer à pgwire SASL (code d'authentification 10) avec le mécanisme `SCRAM-SHA-256`, si bien qu'un mot de passe est prouvé plutôt qu'envoyé. (REQ-1394) La liaison de canal (`SCRAM-SHA-256-PLUS`) n'est pas proposée.

SCRAM a besoin d'un vérificateur RFC 5802, qui ne peut pas être dérivé d'un hachage bcrypt. Un vérificateur est écrit chaque fois qu'un mot de passe passe en clair — inscription, connexion, changement de mot de passe, réinitialisation par un administrateur — si bien qu'un déploiement qui active SCRAM collecte les vérificateurs au fur et à mesure que ses utilisateurs s'authentifient la fois suivante, et la première connexion SCRAM de chaque utilisateur suit sa prochaine saisie de mot de passe. À un utilisateur sans vérificateur, on répond par un échange fictif indiscernable d'un vrai, de sorte que le fil ne révèle pas qui a migré.

### TLS mutuel

La vérification du certificat client déplace le premier contrôle dans le handshake TLS : un appelant sans certificat signé par l'autorité de certification du déploiement n'atteint jamais la couche des identifiants. (REQ-1228) Elle est disponible sur pgwire, Bolt, gRPC et Arrow Flight — les quatre transports qui terminent leur propre TLS.

| Variable | Signification |
| ---------- | --------- |
| `PROVISA_MTLS_CLIENT_CA` | Paquet PEM de la ou des autorités autorisées à signer les certificats client |
| `PROVISA_MTLS_MODE` | `required` (la valeur par défaut dès qu'une autorité est définie) ou `optional` |
| `PROVISA_MTLS_BIND_PRINCIPAL` | Lorsqu'il est vrai, le common name du certificat doit être égal au nom d'utilisateur avec lequel la connexion s'authentifie ensuite |

Les surcharges par protocole suivent la même nomenclature que les réglages TLS. Rien n'est déduit : un mode défini sans autorité refuse de démarrer, et un mode non reconnu refuse de démarrer plutôt que d'être lu comme le voisin le plus sûr — un déploiement qui croit exiger des certificats client sans le faire est plus mal loti qu'un déploiement qui ne démarre pas.

### Limitation des tentatives de connexion

Deviner un mot de passe est indépendant du protocole : le même compte peut être matraqué via HTTP, pgwire et Bolt. Le compteur réside donc à la couche de validation des identifiants, et non sur une surface donnée, si bien qu'un verrouillage acquis n'importe où est appliqué partout. (REQ-1393)

Elle est active par défaut — cinq échecs en cinq minutes verrouillent le sujet pendant quinze minutes — et se règle sous `auth.login_throttle`. Un sujet verrouillé est refusé avant même que l'identifiant soit examiné, et une authentification réussie efface l'historique de ce sujet.

La clé est le principal que porte le protocole. Une surface uniquement bearer ne porte aucun principal, la clé est donc un condensé de l'identifiant lui-même ; ce que cela empêche, c'est qu'un jeton compromis soit rejoué sans limite. Le magasin est propre au processus, si bien qu'un déploiement exécutant plusieurs workers d'API autorise jusqu'à `max_attempts` par worker — la limitation est un frein à la devinette, pas un quota distribué.

### Adresser une organisation sur un protocole de niveau fil

En multi-locataire, une organisation est adressée par nom d'hôte : `acme.provisa.dev` est l'organisation `acme`. En HTTP, ce nom arrive dans l'en-tête `Host`. Un client pgwire ou Bolt n'envoie pas un tel en-tête, mais il envoie bien le nom d'hôte composé dans le ClientHello TLS, et Provisa y lit l'organisation. (REQ-1234) Rien ne change côté client — se connecter à `acme.provisa.dev` suffit.

Le nom d'hôte est une demande, pas une attribution. Il atteint le même résolveur que l'en-tête `Host`, lequel refuse toute organisation dont le principal authentifié n'est ni membre ni titulaire du droit inter-organisations. Composer un nom d'hôte où vous n'avez aucune appartenance n'atteint aucune donnée. Un client connecté par adresse IP n'envoie aucun nom d'hôte et résout son organisation à partir du seul principal, ce qui est le cas de toute connexion sur un déploiement mono-organisation.

gRPC, Arrow Flight et MCP confient leurs certificats à des bibliothèques qui n'exposent aucun rappel de nom d'hôte ; ces transports nomment une organisation avec l'en-tête de métadonnées `x-provisa-org` à la place.

## Mode haute sécurité

`security.mode: high` dans `provisa.yaml` affirme une garantie : le backend Provisa ne manipule jamais de données en clair. (REQ-693) Chaque colonne qui compte est chiffrée à la source, et seul un client détenant la clé de déchiffrement peut la lire. Cette garantie a des conséquences qu'un déploiement doit anticiper.

**Ce que fait le mode :**

- **Les endpoints de données exigent la preuve d'un déchiffrement côté client.** Tout ce qui est sous `/data/` renvoie 403 sauf si l'appelant présente l'en-tête `X-Provisa-KMS-Key` — la marque d'un client JDBC ou Python configuré pour déchiffrer localement. Un navigateur ou un consommateur REST en clair ne porte pas une telle clé et est refusé. Le verrou est un refus par défaut sur tout l'arbre : une route ajoutée demain est verrouillée le jour de sa livraison, et une exemption doit être argumentée.
- **Les endpoints de métadonnées de schéma restent ouverts.** `/data/sdl`, `/data/introspection`, `/data/schema-version`, `/data/domains`, `/data/proto` et `/data/compile` ne renvoient aucune donnée de ligne, et un client doit lire le schéma — y compris quels champs sont `@encrypted` — avant même de pouvoir se connecter.
- **gRPC et Arrow Flight continuent de servir, sous la même preuve.** Ce sont les transports qu'utilisent réellement les clients qui chiffrent ; les fermer laisserait un déploiement haute sécurité sans protocole de niveau fil. Un appel de données sur l'un ou l'autre doit porter la même clé KMS en métadonnées d'appel.
- **pgwire, Bolt et MCP ne démarrent pas.** Aucun des trois n'a de handshake par connexion capable de porter un contexte de déchiffrement : un jeu de lignes pgwire et un résultat Cypher sont en clair sur le fil, et un appel d'outil MCP remet ses résultats à un modèle sous forme de texte. Un port configuré pour l'un d'eux est refusé au démarrage plutôt que servi.
- **La protection des relations ne peut pas être contournée.** `ignore_relationships` et `relationship_guard: false` sont tous deux ignorés ; voir [Gouvernance des relations](#gouvernance-des-relations-v002).

**Vérifier qu'un déploiement est dans ce mode :** le journal de démarrage le nomme, une requête `/data/sql` sans clé KMS répond 403 avec un message citant REQ-693, et les ports pgwire, Bolt et MCP n'écoutent pas.

## Hook d'approbation ABAC

Un hook de politique externe facultatif qui se déclenche avant l'exécution de la requête. (REQ-203) Lorsqu'il est configuré, Provisa fait appel à votre moteur de politique en lui transmettant l'identité de l'utilisateur, les rôles, les tables, les colonnes et l'opération. La réponse détermine si la requête se poursuit. (REQ-203)

### Portée

Le hook ne se déclenche que lorsque la requête touche une table ou une source dans sa portée — aucune surcharge pour tout le reste. (REQ-204)

| Configuration | Effet |
| -------- | -------- |
| `auth.approval_hook.scope: all` | Chaque requête déclenche le hook |
| `sources[].approval_hook: true` | Toutes les tables de cette source déclenchent le hook |
| `tables[].approval_hook: true` | Cette table déclenche le hook |

### Protocoles

Trois transports sont pris en charge : (REQ-246)

| Type | Cas d'usage | Champ de configuration |
| ------ | ---------- | ------------- |
| `webhook` | Tout service de politique compatible HTTP (OPA, personnalisé) | `url` |
| `unix_socket` | OPA ou side-car de politique sur la même machine | `socket_path` + `url` |
| `grpc` | Service de politique colocalisé à haut débit | `url` (host:port) |

Le transport gRPC utilise le contrat `provisa.auth.ApprovalService` défini dans `provisa/auth/approval.proto`. Implémentez ce service dans votre moteur de politique : (REQ-246)

```proto
service ApprovalService {
  rpc Evaluate (ApprovalRequest) returns (ApprovalResponse);
}

message ApprovalRequest {
  string user = 1;
  repeated string roles = 2;
  repeated string tables = 3;
  repeated string columns = 4;
  string operation = 5;
}

message ApprovalResponse {
  bool approved = 1;
  string reason = 2;
}
```

Le canal gRPC est persistant — un canal par instance Provisa, réutilisé pour tous les appels vers ce point de terminaison de hook. (REQ-555)

### Requête / Réponse

Les trois transports véhiculent la même charge utile : (REQ-246)

| Champ | Type | Description |
| ------- | ------ | ------------- |
| `user` | string | Identité de l'utilisateur authentifié |
| `roles` | string[] | Rôles Provisa de l'utilisateur |
| `tables` | string[] | Identifiants de table référencés dans la requête |
| `columns` | string[] | Colonnes sélectionnées dans la requête |
| `operation` | string | `"query"` ou `"mutation"` |

Les transports webhook et Unix socket échangent du JSON. La réponse doit inclure `approved` (bool) et, facultativement, `reason` (string). (REQ-246)

### Délai d'expiration et repli

```yaml
auth:
  approval_hook:
    type: grpc          # webhook | grpc | unix_socket
    url: "localhost:50051"
    timeout_ms: 500     # default 5000
    fallback: deny      # allow | deny — applied on timeout or error
    scope: ""           # "" = use per-table/per-source flags; "all" = every query
```

En cas de dépassement du délai ou d'erreur de transport, la politique `fallback` s'applique. (REQ-247) Un disjoncteur (circuit breaker) (par défaut : ouvert après 5 échecs consécutifs, semi-ouvert après 30 s) empêche les défaillances en cascade provoquées par un point de terminaison de hook lent. (REQ-556)

### Exemple de configuration

```yaml
auth:
  approval_hook:
    type: webhook
    url: "http://opa.internal:8181/v1/data/provisa/allow"
    timeout_ms: 300
    fallback: deny

sources:
  - id: analytics_pg
    approval_hook: true   # all tables on this source require hook approval

tables:
  - id: salary_data
    approval_hook: true   # this table always requires hook approval
```

## Secrets

Les identifiants utilisent la syntaxe `${env:VAR_NAME}`, résolue au moment de l'exécution. (REQ-557) Les mots de passe ne sont jamais stockés dans la base de données de configuration. (REQ-557)
