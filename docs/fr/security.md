# Modèle de sécurité

Provisa applique un modèle de sécurité multicouche à tous les langages de requête (GraphQL, SQL, Cypher) et à tous les transports (REST, gRPC, Arrow Flight, JDBC, WebSocket). (REQ-001, REQ-266) La gouvernance s'applique uniformément — aucun chemin de requête ne la contourne. (REQ-002, REQ-266)

Les couches s'appliquent dans l'ordre. Une requête doit franchir chaque couche avant que la suivante ne soit évaluée.

## Modèle en couches

### Couche 0 — Filtrage de l'introspection

Le schéma et le catalogue présentés à un rôle ne contiennent que les tables figurant dans sa liste `domain_access` et les colonnes qui passent les règles `visible_to` propres à chaque colonne. (REQ-039) Les objets hors de l'accès d'un rôle sont invisibles au moment de la découverte — ils ne peuvent être ni interrogés, ni complétés automatiquement, ni déduits comme existants. (REQ-039) Cela vaut pour le schéma GraphQL, le catalogue SQL et l'explorateur de schéma de l'éditeur de requêtes. (REQ-039, REQ-363)

Voir [Visibilité du schéma](#visibilite-du-schema).

### Couche 1 — Accès public

Les tables des domaines sans restriction `domain_access` sont visibles de toutes les identités authentifiées, sans configuration supplémentaire. Aucune friction pour des données réellement publiques.

### Couche 2 — Accès au domaine

Chaque rôle porte une liste `domain_access` d'identifiants de domaines. Une requête qui touche une table hors de ces domaines est rejetée avant exécution. (REQ-038, REQ-039) C'est la frontière grossière de propriété — un rôle RH ne peut pas atteindre les tables financières, quelle que soit la façon dont le SQL est écrit. (REQ-002)

Voir [Modèle de droits](#modele-de-droits).

### Couche 3 — Sécurité au niveau des lignes

Une fois l'accès au domaine confirmé, des prédicats `WHERE` propres à chaque table et à chaque rôle sont injectés dans chaque `SELECT` au moment de l'exécution. (REQ-041, REQ-263) Les prédicats s'évaluent contre les données brutes. Un directeur régional qui interroge une table de commandes partagée ne voit que les lignes de sa région, même sur un `SELECT *`. (REQ-264)

Voir [Sécurité au niveau des lignes (RLS)](#securite-au-niveau-des-lignes-rls).

### Couche 4 — Visibilité et masquage des colonnes

Les colonnes dont la liste `visible_to` exclut le rôle demandeur sont retirées de la sortie de la requête. (REQ-040, REQ-263) Les colonnes portant une règle de masquage voient leurs valeurs remplacées — caviardage par expression régulière, remplacement par une constante ou troncature — avant que les résultats ne quittent le serveur. (REQ-263) Le masquage s'applique dans tous les langages de requête et tous les formats de sortie. (REQ-263)

Voir [Modèle de permissions de colonnes](#modele-de-permissions-de-colonnes) et [Masquage au niveau des colonnes](#masquage-au-niveau-des-colonnes).

### Couche 5 — Garde-fou des prédicats

Les colonnes masquées sont rejetées des clauses `WHERE` et `HAVING`. (REQ-263) Sans cela, un appelant pourrait déduire la valeur non masquée en la cherchant par dichotomie dans un filtre, alors même que la sortie est masquée. Le rejet est appliqué à l'analyse de la requête, avant exécution. (REQ-531)

### Gouvernance des relations (V002)

Les conditions de JOIN en SQL doivent correspondre à une relation enregistrée et approuvée entre les tables. (REQ-001) Les jointures non approuvées sont rejetées. Chaque relation porte une raison et une description lisibles par un humain — des indications, tant pour les utilisateurs que pour les agents autonomes, sur la raison d'être d'un chemin de traversée. C'est une politique de gouvernance, non une frontière de sécurité dure : les couches 2 à 5 tiennent quelle que soit la structure de la jointure, si bien qu'un contournement délibéré n'expose aucune donnée que le rôle ne pourrait atteindre en deux requêtes séparées. Les tentatives de contournement sont journalisées et auditables.

**Mécanismes de contournement** — V002 peut être contourné de deux façons. La première est une capacité : un rôle détenant `ignore_relationships` joint sur des relations que le catalogue ne couvre pas. Parmi les rôles système amorcés, seul `modeler` la détient — le rôle de découverte, dont le travail est de déterminer le modèle plutôt que de l'appliquer. (REQ-1297) `analyst` ne la détient pas. [tool-verified: `provisa/core/db.py:84`]

La seconde est un renoncement à deux conditions, toutes deux nécessaires :

1. **Indicateur de rôle** — `relationship_guard: false` sur la définition du rôle (par défaut : `true`). [tool-verified: `provisa/core/models.py:349`]
2. **Renoncement par requête** — le SQL contient le commentaire `--relationship-guard=false`. [tool-verified: `provisa/compiler/params.py:80`]

L'indicateur de rôle seul ne contourne pas V002 ; le commentaire seul ne contourne pas V002.

**Le mode haute sécurité fige le garde-fou.** Sous `security.mode: high`, aucun des deux contournements ne s'applique : `ignore_relationships` est ignoré, `relationship_guard: false` est ignoré, et chaque jointure doit exister dans le catalogue des relations approuvées. (REQ-693) C'est une redondance délibérée — un rôle de production à qui la capacité a été accordée par erreur ne peut toujours pas s'échapper du modèle. [tool-verified: `provisa/pgwire/_pipeline.py:377`]

**Chemin GraphQL** — V002 est inconditionnellement ignoré pour les requêtes GraphQL. Les relations définies en SDL sont approuvées par construction ; la vérification est redondante et n'est pas appliquée. [tool-verified: `provisa/api/data/endpoint.py:468`]

**Chemins SQL et Cypher** — V002 est actif par défaut. `endpoint_dev.py` et `cypher_router.py` appliquent tous deux la vérification à deux conditions avant d'appeler `validate_sql`. [tool-verified: `provisa/api/data/endpoint_dev.py:127`, `provisa/api/rest/cypher_router.py:260`]

**Chemin pgwire** — même vérification à deux conditions qu'en SQL. Le commentaire `--relationship-guard=false` est retiré de la requête avant exécution ; il n'atteint pas la base de données. [tool-verified: `provisa/pgwire/_pipeline.py:60`]

---

Ces couches se composent. Un rôle disposant d'un accès au domaine, de RLS et de colonnes masquées a les cinq contraintes actives simultanément. Ajouter une nouvelle source de données, colonne ou relation n'oblige pas à mettre à jour chaque règle — chaque couche se configure indépendamment et s'applique automatiquement à toute requête qui touche des objets gouvernés.

---

## Modèle de droits

Des capacités affectées indépendamment, avec une hiérarchie de rôles facultative via `parent_role_id`. `admin` accorde tout. (REQ-042)

| Capacité | Description |
| ----------- | ------------- |
| `source_registration` | Enregistrer des sources de données |
| `table_registration` | Enregistrer des tables, des colonnes |
| `create_relationship` | Définir des relations de clés étrangères |
| `access_config` | Configurer la RLS et le masquage |
| `query_development` | Exécuter des requêtes |
| `write` | Invoquer les mutations enregistrées (barrière grossière ; voir Autorisation des mutations) |
| `full_results` | Contourner les limites d'échantillonnage |
| `ignore_relationships` | Contourner la gouvernance des relations (V002). Détenue par `modeler` seul parmi les rôles système, et entièrement ignorée en mode haute sécurité |
| `admin` | Superuser — accorde tout |

### Héritage de rôles

Les rôles peuvent hériter des capacités et de l'accès aux domaines d'un rôle parent via `parent_role_id`. (REQ-215) La hiérarchie est aplatie au démarrage — les rôles enfants fusionnent les capacités et l'accès aux domaines de leur parent avec les leurs. (REQ-215)

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

## Modèle de permissions de colonnes

Chaque colonne dispose d'un modèle de permissions à quatre champs qui régit l'accès en lecture, en écriture et le masquage, rôle par rôle. (REQ-042, REQ-249)

### Visibilité à trois niveaux

| Niveau | Condition | Résultat |
| ------ | ----------- | -------- |
| **Masquée du schéma** | Rôle absent de `visible_to` | Colonne absente du SDL GraphQL |
| **Masquée** | Rôle présent dans `visible_to`, règle de masquage définie, rôle absent d'`unmasked_to` | Colonne visible mais données masquées en SQL |
| **Non masquée** | Rôle présent dans `visible_to` ET dans `unmasked_to` (ou aucune règle de masquage) | Accès en lecture complet |

### Permissions d'écriture

| Champ | Ce que vide signifie | Rôle du champ |
| ------- | ------------ | --------- |
| `visible_to` | Tous les rôles peuvent lire | Régit qui voit la colonne (masquée ou non) |
| `unmasked_to` | Aucun rôle ne la voit en clair | Régit qui contourne le masquage |
| `writable_by` | Aucun rôle ne peut écrire | Régit qui peut muter (INSERT/UPDATE) |

La permission d'écriture est appliquée dans le pipeline de mutation. Un rôle absent de `writable_by` reçoit une erreur 403 lorsqu'il tente d'écrire dans une colonne restreinte. (REQ-033, REQ-034)

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

Dans cet exemple :

- `email` : admin voit `alice@example.com` et peut éditer ; analyst et viewer voient `a***@example.com`
- `salary` : admin et hr voient la valeur réelle ; hr peut éditer ; tous les autres rôles ne voient pas du tout la colonne
- `created_at` : tout le monde peut lire, personne ne peut écrire

## Autorisation des mutations

Les mutations enregistrées (GraphQL distant, OpenAPI, gRPC, Hasura) sont soumises à deux vérifications indépendantes. (REQ-867, REQ-868) Un rôle ne peut invoquer une mutation que s'il détient la capacité globale `write` ET figure dans la liste `writable_by` de cette mutation. (REQ-868) Un `writable_by` vide vaut refus par défaut — aucun rôle ne peut l'invoquer. (REQ-867)

Les mutations sont classées comme écritures par contrat, non par déclaration de l'appelant. (REQ-869) Un `SELECT` qui référence une fonction de type mutation est promu en écriture et soumis à la même double barrière, si bien qu'un appelant ne peut pas invoquer une mutation en la déguisant en lecture. (REQ-869) Reclasser une mutation comme sûre en lecture exige la capacité `access_config` et est consigné comme une décision de gouvernance ; il n'existe pas de renoncement par requête. (REQ-870)

## Visibilité du schéma

Les schémas GraphQL propres à chaque rôle masquent le contenu non autorisé : (REQ-039)

- **Accès au domaine** : le rôle ne voit que les tables de ses domaines `domain_access` (`"*"` = tous) (REQ-039)
- **Visibilité des colonnes** : les colonnes absentes du `visible_to` d'un rôle sont omises du SDL (REQ-039)
- Les tables et colonnes non autorisées n'apparaissent pas dans le schéma (REQ-039)

## Sécurité au niveau des lignes (RLS)

Injection de clauses WHERE SQL par table et par rôle. Appliquée après la compilation, avant l'exécution. (REQ-041, REQ-263)

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
```

Le filtre est combiné par ET à la clause WHERE de la requête. Fonctionne aussi bien pour les requêtes que pour les mutations (UPDATE/DELETE). (REQ-035, REQ-041)

## Masquage au niveau des colonnes

Le masquage est défini une fois par colonne — c'est une propriété de la colonne, non du rôle. Le champ `unmasked_to` régit quels rôles le contournent. (REQ-249)

| Type de masque | Types pris en charge | Expression SQL |
| ----------- | ---------------- | ---------------- |
| `regex` | Chaîne (varchar, char, text) | `REGEXP_REPLACE(col, pattern, replace)` |
| `constant` | Tous | Valeur littérale (NULL, 0, personnalisée) |
| `truncate` | Date/Timestamp | `DATE_TRUNC(precision, col)` |

Le masquage est poussé dans la projection SELECT du SQL — c'est la base de données qui renvoie des données masquées. (REQ-263) Les données non masquées ne traversent jamais le réseau pour les rôles masqués. (REQ-263) Les colonnes masquées sont également bloquées dans les clauses `WHERE` et `HAVING` (garde-fou des prédicats, couche 5) afin d'empêcher d'inférer la valeur non masquée par filtrage. (REQ-263, REQ-531)

## Échantillonnage

Tous les rôles voient des résultats échantillonnés (par défaut : 100 lignes) sauf s'ils détiennent la capacité `full_results`. (REQ-554) Réglable via la variable d'environnement `PROVISA_SAMPLE_SIZE`. (REQ-554)

## Journalisation d'audit

Chaque requête qui touche un actif d'un domaine est consignée dans le `query_audit_log`, en ajout seul. (REQ-596, REQ-613) Chaque ligne capture `tenant_id`, `user_id`, `role_id`, une empreinte SHA-256 du texte de la requête, `table_ids`, `source`, `status_code`, `duration_ms` et `logged_at`. (REQ-596) Le texte de la requête n'est jamais stocké tel quel — seulement son empreinte. (REQ-596)

Le journal est en ajout seul au niveau de la base de données : des règles PostgreSQL bloquent `DELETE` et `UPDATE`. (REQ-596, REQ-613) Deux index — `(tenant_id, logged_at)` et `(user_id, logged_at)` — servent les requêtes de conformité par plage de temps, à l'échelle du locataire ou de l'utilisateur. (REQ-596, REQ-613)

Lorsque le chiffrement est activé, la colonne d'empreinte du texte de requête est stockée chiffrée et déchiffrée uniquement lors des lectures administrateur autorisées. (REQ-689)

## Limitation de débit

Les limites de débit par rôle se configurent dans `provisa.yaml` : requêtes maximales par seconde, abonnements SSE concurrents maximaux et flux Arrow Flight concurrents maximaux. (REQ-369) Les limites sont appliquées à la couche API, avant compilation ou exécution ; les requêtes au-delà de la limite sont rejetées avec un HTTP 429 et un en-tête `Retry-After`. (REQ-369)

Le service de requêtes en langage naturel (`POST /query/nl`) dispose d'une limite indépendante via `nl.rate_limit` (requêtes par minute et par rôle). Les requêtes au-delà de la limite sont rejetées avant tout appel à un modèle de langage. (REQ-370)

L'état de la limitation de débit vit dans Redis (`cache.redis_url`) sous forme de compteur à fenêtre glissante — sans état par instance — de sorte que les limites tiennent sur toutes les instances Provisa réparties horizontalement. (REQ-371)

## Authentification

Fournisseurs d'authentification enfichables : (REQ-120)

| Fournisseur | Type de jeton | Cas d'usage |
| ---------- | ----------- | ---------- |
| `none` | En-tête X-Provisa-Role | Développement |
| `basic` | Comptes locaux bcrypt + JWT | Déploiements autonomes |
| `firebase` | Jeton d'identité Firebase | Production |
| `keycloak` | JWT Keycloak | Entreprise |
| `oauth` | JWT OIDC | PingFed, Okta, Azure AD, Auth0 |
| `simple` | bcrypt + JWT | Tests |

Correspondance des rôles : revendications d'identité → rôle Provisa via des règles configurables. (REQ-120) Le champ `assignments_source` régit la provenance des affectations de rôles : `claims` les lit dans les revendications du jeton JWT (par défaut), `provisa` les lit dans le magasin d'affectations interne de Provisa. (REQ-551)

Un superuser configuré dans `provisa.yaml` (nom d'utilisateur plus un mot de passe issu d'un secret d'environnement) reçoit toujours le rôle admin et toutes les capacités, quel que soit le fournisseur configuré — un chemin d'amorçage pour la configuration initiale. (REQ-125)

### Surfaces et identifiants

Chaque surface s'authentifie via le même contrat de fournisseur, si bien qu'un identifiant qui fonctionne sur l'une fonctionne sur toutes, partout où le protocole peut le porter. (REQ-124, REQ-1263) Ce tableau est la référence unique ; les documentations propres à chaque surface ne le répètent pas.

| Surface | Mot de passe | Jeton du fournisseur | Jeton d'accès personnel | Certificat client (mTLS) |
| --------- | ---------- | ---------------- | ----------------------- | --------------------------- |
| HTTP (REST, JSON:API, GraphQL) | `Authorization: Basic` | `Authorization: Bearer` | `Authorization: Bearer` | via un proxy terminant TLS |
| pgwire | champ mot de passe (en clair ou SCRAM) | champ mot de passe, déploiements OIDC | champ mot de passe | oui |
| Bolt | schéma `basic` | schéma `bearer` | schéma `bearer` | oui |
| Arrow Flight | — | `token` dans la poignée de main ou la charge utile du ticket | idem | oui |
| gRPC | — | métadonnée `authorization` | métadonnée `authorization` | oui |
| MCP | — | `Authorization: Bearer` | `Authorization: Bearer` | via un proxy terminant TLS |

Là où une cellule affiche `—`, le protocole ne porte aucun champ de nom d'utilisateur auquel adosser un mot de passe ; les formes à jeton le couvrent. pgwire est le cas inverse : le paquet de démarrage ne comporte qu'un champ secret et aucun schéma, si bien que c'est la nature du secret qui choisit la méthode — un PAT est reconnu à son préfixe, le secret est lu comme un jeton bearer lorsque le fournisseur configuré est un fournisseur à jetons, et tout le reste est un mot de passe. Le choix se fait une seule fois — un identifiant que le validateur sélectionné refuse n'est pas réessayé contre un autre.

La matrice est vérifiée par `tests/unit/test_auth_surface_conformance.py`, qui pilote le point d'entrée de validation réel de chaque surface et échoue lorsqu'une nouvelle surface est ajoutée sans sa ligne.

### Jetons d'accès personnels

Un PAT est un secret bearer de longue durée qu'un utilisateur frappe pour un client incapable de mener une connexion interactive — un script, un outil de BI, un pilote. (REQ-1263) Il porte sa propre organisation et son propre rôle, et chaque surface le résout via le même validateur : aucune surface n'a donc besoin de savoir ce qu'est un PAT.

La forme sur le réseau est `provisa_pat_` suivi de 43 caractères base64 sûrs pour les URL. C'est le préfixe qui aiguille un secret présenté vers le magasin de jetons plutôt que vers le fournisseur d'identité, et il rend un jeton fuité repérable au grep dans les journaux et les dépôts.

- **Stockage** — seul le SHA-256 du secret est conservé. Le secret lui-même n'est montré qu'une seule fois, à la création, et ne peut pas être récupéré. La liste porte le préfixe d'affichage et les horodatages de cycle de vie, jamais un identifiant utilisable.
- **Émission et révocation** — `POST /auth/tokens`, `GET /auth/tokens`, `DELETE /auth/tokens/{token_hash}`, ainsi que la section en libre-service sur le profil de l'utilisateur dans l'interface d'administration. Frapper et révoquer un identifiant est l'acte du détenteur du jeton.
- **Attribution** — un PAT validé se résout vers le compte de son propriétaire : identifiant utilisateur, adresse e-mail et nom d'affichage. Une ligne d'audit ou un rapport d'usage écrit sous un PAT nomme donc la personne, non l'identifiant. Lequel des jetons de cette personne a agi est porté à part, dans `raw_claims["token_name"]`.
- **Expiration** — un jeton peut porter une expiration ; un jeton expiré est refusé à la validation. Supprimer l'appartenance d'un utilisateur révoque ses jetons du même geste.

### SCRAM-SHA-256 sur pgwire

Sous le fournisseur `basic`, définir `auth.scram: true` fait annoncer à pgwire SASL (code d'authentification 10) avec le mécanisme `SCRAM-SHA-256`, de sorte qu'un mot de passe est prouvé plutôt qu'envoyé. (REQ-1394) La liaison de canal (`SCRAM-SHA-256-PLUS`) n'est pas proposée.

SCRAM a besoin d'un vérificateur RFC 5802, qui ne peut pas être dérivé d'une empreinte bcrypt. Un vérificateur est écrit chaque fois qu'un mot de passe transite en clair — inscription, connexion, changement de mot de passe, réinitialisation par un administrateur — si bien qu'un déploiement qui active SCRAM collecte les vérificateurs au fil des authentifications suivantes de ses utilisateurs, et que la première connexion SCRAM de chacun suit sa prochaine saisie de mot de passe. Un utilisateur encore dépourvu de vérificateur reçoit un échange factice indiscernable d'un vrai, de sorte que le réseau ne révèle pas qui a migré.

### TLS mutuel

La vérification du certificat client déplace la première vérification vers la poignée de main TLS : un appelant sans certificat signé par l'autorité de certification du déploiement n'atteint jamais la couche des identifiants. (REQ-1228) Elle est disponible sur pgwire, Bolt, gRPC et Arrow Flight — les quatre transports qui terminent leur propre TLS.

| Variable | Signification |
| ---------- | --------- |
| `PROVISA_MTLS_CLIENT_CA` | Ensemble PEM de la ou des autorités habilitées à signer les certificats clients |
| `PROVISA_MTLS_MODE` | `required` (la valeur par défaut dès qu'une autorité est définie) ou `optional` |
| `PROVISA_MTLS_BIND_PRINCIPAL` | Lorsqu'il vaut vrai, le nom commun du certificat doit être égal au nom d'utilisateur sous lequel la connexion s'authentifie ensuite |

Les surcharges par protocole suivent le même nommage que les paramètres TLS. Rien n'est déduit : un mode défini sans autorité de certification refuse de démarrer, et un mode non reconnu refuse de démarrer plutôt que d'être lu comme le voisin le plus sûr — un déploiement qui se croit exigeant en certificats clients sans l'être est plus mal loti qu'un déploiement qui ne démarre pas.

### Ralentissement des connexions

Deviner un mot de passe est indépendant du protocole : le même compte peut être martelé sur HTTP, pgwire et Bolt. Le compteur vit donc à la couche de validation des identifiants, sur aucune surface en particulier, de sorte qu'un verrouillage gagné n'importe où est appliqué partout. (REQ-1393)

Il est actif par défaut — cinq échecs en cinq minutes verrouillent le sujet pendant quinze minutes — et se règle sous `auth.login_throttle`. Un sujet verrouillé est refusé avant même que l'identifiant ne soit examiné, et une authentification réussie efface l'historique de ce sujet.

La clé est le principal que porte le protocole. Une surface acceptant uniquement des jetons ne porte aucun principal : la clé est alors une empreinte de l'identifiant lui-même, ce qui empêche qu'un jeton compromis soit rejoué sans limite. Le magasin est propre à chaque processus, de sorte qu'un déploiement exécutant plusieurs workers d'API autorise jusqu'à `max_attempts` par worker — le ralentissement est un frein au devinage, non un quota distribué.

### Adresser une organisation sur un protocole de niveau fil

En multilocataire, une organisation est adressée par nom d'hôte : `acme.provisa.dev` est l'organisation `acme`. En HTTP, ce nom arrive dans l'en-tête `Host`. Un client pgwire ou Bolt n'envoie aucun en-tête de ce genre, mais il envoie bien le nom d'hôte qu'il a composé dans le ClientHello TLS, et Provisa y lit l'organisation. (REQ-1234) Rien ne change côté client — se connecter à `acme.provisa.dev` suffit.

Le nom d'hôte est une demande, pas un octroi. Il atteint le même résolveur que l'en-tête `Host`, lequel refuse toute organisation dont le principal authentifié n'est pas membre et pour laquelle il ne détient pas le droit inter-organisations. Composer un nom d'hôte dont vous n'êtes pas membre n'atteint aucune donnée. Un client connecté par adresse IP n'envoie aucun nom d'hôte et résout son organisation à partir du seul principal, ce qui est le cas de toute connexion sur un déploiement mono-organisation.

gRPC, Arrow Flight et MCP confient leurs certificats à des bibliothèques qui n'exposent aucun rappel sur le nom d'hôte ; ces transports nomment une organisation avec l'en-tête de métadonnée `x-provisa-org` à la place.

## Mode haute sécurité

`security.mode: high` dans `provisa.yaml` affirme une garantie : le backend Provisa ne manipule jamais de données en clair. (REQ-693) Chaque colonne qui compte est chiffrée à la source, et seul un client détenant la clé de déchiffrement peut la lire. Cette garantie a des conséquences qu'un déploiement doit anticiper.

**Ce que fait le mode :**

- **Les endpoints de données exigent la preuve d'un déchiffrement côté client.** Tout ce qui est sous `/data/` renvoie 403 sauf si l'appelant présente l'en-tête `X-Provisa-KMS-Key` — la marque d'un client JDBC ou Python configuré pour déchiffrer localement. Un navigateur ou un consommateur REST en clair ne porte pas une telle clé et est refusé. La barrière est un refus par défaut sur tout l'arbre : une route ajoutée demain est sous barrière le jour de sa livraison, et une exemption doit être argumentée.
- **Les endpoints de métadonnées de schéma restent ouverts.** `/data/sdl`, `/data/introspection`, `/data/schema-version`, `/data/domains`, `/data/proto` et `/data/compile` ne renvoient aucune donnée de lignes, et un client doit lire le schéma — y compris quels champs sont `@encrypted` — avant même de pouvoir se connecter.
- **gRPC et Arrow Flight continuent de servir, sous la même preuve.** Ce sont les transports qu'utilisent réellement les clients chiffrants ; les fermer laisserait un déploiement en haute sécurité sans aucun protocole de niveau fil. Un appel de données sur l'un ou l'autre doit porter la même clé KMS en métadonnée d'appel.
- **pgwire, Bolt et MCP ne démarrent pas.** Aucun des trois ne dispose d'une poignée de main par connexion capable de porter un contexte de déchiffrement : un jeu de lignes pgwire et un résultat Cypher sont en clair sur le réseau, et un appel d'outil MCP remet ses résultats à un modèle sous forme de texte. Un port configuré pour l'un d'eux est refusé au démarrage plutôt que servi.
- **Le garde-fou des relations ne peut pas être contourné.** `ignore_relationships` et `relationship_guard: false` sont tous deux ignorés ; voir [Gouvernance des relations](#gouvernance-des-relations-v002).

**Vérifier qu'un déploiement est bien dans ce mode :** le journal de démarrage le nomme, une requête `/data/sql` sans clé KMS répond 403 avec un message nommant REQ-693, et les ports pgwire, Bolt et MCP n'écoutent pas.

## Point d'ancrage d'approbation ABAC

Un point d'ancrage de politique externe facultatif, déclenché avant l'exécution d'une requête. (REQ-203) Lorsqu'il est configuré, Provisa appelle votre moteur de politiques avec l'identité de l'utilisateur, les rôles, les tables, les colonnes et l'opération. La réponse détermine si la requête se poursuit. (REQ-203)

### Portée

Le point d'ancrage ne se déclenche que lorsque la requête touche une table ou une source dans sa portée — aucun surcoût pour tout le reste. (REQ-204)

| Configuration | Effet |
| -------- | -------- |
| `auth.approval_hook.scope: all` | Chaque requête déclenche le point d'ancrage |
| `sources[].approval_hook: true` | Toutes les tables de cette source déclenchent le point d'ancrage |
| `tables[].approval_hook: true` | Cette table déclenche le point d'ancrage |

### Protocoles

Trois transports sont pris en charge : (REQ-246)

| Type | Cas d'usage | Champ de configuration |
| ------ | ---------- | ------------- |
| `webhook` | Tout service de politiques joignable en HTTP (OPA, sur mesure) | `url` |
| `unix_socket` | OPA ou sidecar de politiques sur la même machine | `socket_path` + `url` |
| `grpc` | Service de politiques colocalisé à fort débit | `url` (host:port) |

Le transport gRPC utilise le contrat `provisa.auth.ApprovalService` défini dans `provisa/auth/approval.proto`. Implémentez ce service dans votre moteur de politiques : (REQ-246)

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

Le canal gRPC est persistant — un canal par instance Provisa, réutilisé pour tous les appels vers ce point d'ancrage. (REQ-555)

### Requête / réponse

Les trois transports portent la même charge utile : (REQ-246)

| Champ | Type | Description |
| ------- | ------ | ------------- |
| `user` | string | Identité de l'utilisateur authentifié |
| `roles` | string[] | Rôles Provisa de l'utilisateur |
| `tables` | string[] | Identifiants des tables référencées dans la requête |
| `columns` | string[] | Colonnes sélectionnées dans la requête |
| `operation` | string | `"query"` ou `"mutation"` |

Les transports webhook et socket Unix échangent du JSON. La réponse doit inclure `approved` (booléen) et, en option, `reason` (chaîne). (REQ-246)

### Délai d'attente et repli

```yaml
auth:
  approval_hook:
    type: grpc          # webhook | grpc | unix_socket
    url: "localhost:50051"
    timeout_ms: 500     # default 5000
    fallback: deny      # allow | deny — applied on timeout or error
    scope: ""           # "" = use per-table/per-source flags; "all" = every query
```

En cas de délai dépassé ou d'erreur de transport, la politique `fallback` s'applique. (REQ-247) Un disjoncteur (par défaut : ouvert après 5 échecs consécutifs, semi-ouvert après 30 s) empêche les défaillances en cascade dues à un point d'ancrage lent. (REQ-556)

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

Les identifiants utilisent la syntaxe `${env:VAR_NAME}`, résolue à l'exécution. (REQ-557) Les mots de passe ne sont jamais stockés dans la base de configuration. (REQ-557)

Pour le service de secrets complet — coffres-forts, syntaxe de référence et fournisseurs — voir [Secrets](secrets.md).
