# Modèle de sécurité {: #security-model }

Provisa applique un modèle de sécurité à plusieurs couches à tout langage de requête (GraphQL, SQL, Cypher) et à tout transport (REST, gRPC, Arrow Flight, JDBC, WebSocket). (REQ-001, REQ-266) La gouvernance s'applique de manière uniforme — aucun chemin de requête ne la contourne. (REQ-002, REQ-266)

Les couches s'appliquent dans l'ordre. Une requête doit franchir chaque couche avant que la suivante soit évaluée.

## Modèle en couches {: #layered-model }

### Couche 0 — Filtrage de l'introspection {: #layer-0-introspection-filtering }

Le schéma et le catalogue présentés à un rôle ne contiennent que les tables de sa liste `domain_access` et les colonnes qui passent les règles `visible_to` de chaque colonne. (REQ-039) Les objets hors de l'accès d'un rôle sont invisibles au moment de la découverte — ils ne peuvent être ni interrogés, ni complétés automatiquement, ni supposés exister. (REQ-039) Cela s'applique au schéma GraphQL, au catalogue SQL et au navigateur de schéma de l'éditeur de requêtes. (REQ-039, REQ-363)

Voir [Visibilité du schéma](#schema-visibility).

### Couche 1 — Accès public {: #layer-1-public-access }

Une colonne publiée avec la portée `public` est servie à chaque rôle de l'organisation, y compris les rôles dont le `domain_access` n'atteint pas le domaine de la table ; un tel rôle voit la table avec ses seules colonnes publiques. Rien n'est public tant que celui qui enregistre une table ne le dit pas. (REQ-1959) Voir [Colonnes publiques](#public-columns).

### Couche 2 — Accès aux domaines {: #layer-2-domain-access }

Chaque rôle porte une liste `domain_access` d'identifiants de domaine. Une requête qui touche une table hors de ces domaines est refusée avant exécution. (REQ-038, REQ-039) C'est la frontière grossière de propriété — un rôle RH n'atteint pas les tables financières, quelle que soit la manière dont le SQL est écrit. (REQ-002)

Voir [Modèle de droits](#rights-model).

### Couche 3 — Sécurité au niveau des lignes {: #layer-3-row-level-security }

Une fois l'accès au domaine confirmé, des prédicats `WHERE` par table et par rôle sont injectés dans chaque `SELECT` à l'exécution. (REQ-041, REQ-263) Les prédicats s'évaluent sur les données brutes. Un responsable régional qui interroge une table de commandes partagée ne voit que les lignes de sa région, même avec `SELECT *`. (REQ-264)

Voir [Sécurité au niveau des lignes (RLS)](#row-level-security-rls).

### Couche 4 — Visibilité et masquage des colonnes {: #layer-4-column-visibility-and-masking }

Les colonnes dont la liste `visible_to` exclut le rôle demandeur sont retirées de la sortie de la requête. (REQ-040, REQ-263) Les colonnes ayant une règle de masquage voient leurs valeurs remplacées — expurgation par regex, remplacement par une constante ou troncature — avant que les résultats ne quittent le serveur. (REQ-263) Le masquage s'applique dans tous les langages de requête et formats de sortie. (REQ-263)

Voir [Modèle de permissions des colonnes](#column-permission-model) et [Masquage au niveau des colonnes](#column-level-masking).

### Couche 5 — Garde des prédicats {: #layer-5-predicate-guard }

Les colonnes masquées sont refusées dans les clauses `WHERE` et `HAVING`. (REQ-263) Sans cela, un appelant pourrait déduire la valeur non masquée en la cherchant par dichotomie dans un filtre, même si la sortie est masquée. Le refus s'applique à l'analyse de la requête, avant l'exécution. (REQ-531)

### Gouvernance des relations (V002) {: #relationship-governance-v002 }

Les conditions JOIN en SQL doivent correspondre à une relation enregistrée et approuvée entre tables. (REQ-001) Les jointures non approuvées sont refusées. Chaque relation porte un motif et une description lisibles — un guide pour les utilisateurs comme pour les agents autonomes sur la raison d'être d'un chemin de parcours. C'est une politique de gouvernance, pas une frontière de sécurité stricte : les couches 2 à 5 tiennent quelle que soit la structure de la jointure, donc un contournement délibéré n'expose aucune donnée que le rôle ne pourrait atteindre par deux requêtes séparées. Les tentatives de contournement sont journalisées et auditables.

**Mécanismes de contournement** — V002 peut être contourné de deux façons. La première est une capacité : un rôle qui détient `ignore_relationships` joint des relations que le catalogue ne couvre pas. Parmi les rôles système prédéfinis, seul `modeler` la détient — le rôle de découverte dont le travail est de déterminer le modèle plutôt que de l'appliquer. (REQ-1297) `analyst` ne la détient pas.

La seconde est une exclusion à deux conditions, qui doivent être vraies toutes les deux :

1. **Indicateur du rôle** — `relationship_guard: false` dans la définition du rôle (par défaut : `true`).
2. **Exclusion par requête** — le SQL contient le commentaire `--relationship-guard=false`.

L'indicateur du rôle seul ne contourne pas V002 ; le commentaire seul ne contourne pas V002.

**Le mode haute sécurité verrouille la garde.** Avec `security.mode: high`, aucun contournement ne s'applique : `ignore_relationships` est ignoré, `relationship_guard: false` est ignoré, et chaque jointure doit exister dans le catalogue des relations approuvées. (REQ-693) C'est une redondance délibérée — un rôle de production auquel la capacité a été accordée par erreur ne peut toujours pas sortir du modèle.

**Chemin GraphQL** — V002 est toujours ignoré pour les requêtes GraphQL. Les relations définies dans le SDL sont pré-approuvées par conception ; la vérification est redondante et n'est pas appliquée.

**Chemins SQL et Cypher** — V002 est actif par défaut. `endpoint_dev.py` comme `cypher_router.py` appliquent la vérification à deux conditions avant d'appeler `validate_sql`.

**Chemin pgwire** — même vérification à deux conditions qu'en SQL. Le commentaire `--relationship-guard=false` est retiré de la requête avant exécution ; il n'atteint pas la base de données.

---

Ces couches se combinent. Un rôle avec accès aux domaines, RLS et colonnes masquées a les cinq contraintes actives simultanément. Ajouter une source de données, une colonne ou une relation n'oblige pas à mettre à jour chaque règle — chaque couche se configure indépendamment et s'applique automatiquement à toute requête qui touche des objets gouvernés.

---

## Modèle de droits {: #rights-model }

Capacités attribuées indépendamment, avec une hiérarchie de rôles facultative via `parent_role_id`. `admin` accorde tout. (REQ-042)

| Capacité | Description |
| ----------- | ------------- |
| `source_registration` | Enregistrer des sources de données |
| `table_registration` | Enregistrer des tables et des colonnes |
| `create_relationship` | Définir des relations FK |
| `access_config` | Configurer la RLS et le masquage |
| `query_development` | Exécuter des requêtes |
| `write` | Invoquer les mutations enregistrées (contrôle grossier ; voir Autorisation des mutations) |
| `full_results` | Contourner les limites d'échantillonnage |
| `ignore_relationships` | Contourner la gouvernance des relations (V002). Détenue par `modeler` seul parmi les rôles système, et entièrement ignorée en mode haute sécurité |
| `admin` | Superutilisateur — accorde tout |

### Héritage des rôles {: #role-inheritance }

Les rôles peuvent hériter d'un rôle parent via `parent_role_id`, défini dans la configuration ou sur la page Sécurité comme « Hérite de ». (REQ-215) La chaîne est repliée au démarrage : un enfant détient l'union des capacités et de l'accès aux domaines de ses ancêtres, chaque autorisation de colonne, de métrique, de fonction et de webhook qui nomme un ancêtre, et les règles RLS des ancêtres par table, l'enfant ayant la priorité — le rôle le plus proche dans la chaîne qui a une règle pour une table fournit le prédicat de cette table, et une règle enregistrée pour l'enfant remplace celle du parent pour cette table. (REQ-1677)

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
## Rôles : accordés, pseudo et méta {: #roles-granted-pseudo-and-meta }

Trois sortes de rôles fonctionnent ensemble dans le modèle d'accès de Provisa.

### Rôles accordés {: #granted-roles }

Un rôle accordé est attribué directement à un utilisateur. Il détient des droits : capacités, accès aux domaines, entrées de masquage, filtres de lignes. L'héritage (`parent_role_id`) replie au démarrage les droits d'un ancêtre dans l'enfant. Les droits ne font que s'ajouter ; il n'y a pas de soustraction. Pour restreindre un lecteur, accordez à un composé qui nomme à la fois le rôle et une condition, ou placez une entrée de masque avant l'entrée permissive. (REQ-215, REQ-1677)

### Pseudo-rôles

Un pseudo-rôle est dérivé des attributs d'un utilisateur. L'organisation déclare quels types d'attribut deviennent des rôles (`claim_roles`). Pour chaque type déclaré `t` dont la valeur `v` est fournie par les attributs de l'utilisateur, l'utilisateur détient le rôle parent `t` et son sous-rôle `t__v`. Sans valeur, il ne détient ni l'un ni l'autre.

La résolution des attributs compte. L'enregistrement propre de l'organisation sur un membre l'emporte sur ce que fournit le fournisseur d'identité : si les deux nomment le même attribut, la valeur de l'organisation est utilisée et celle du fournisseur est marquée comme remplacée.

Les pseudo-rôles ne portent aucun droit et ne sont jamais attribués à la main. Une liste d'autorisation les nomme exactement comme elle nomme un rôle accordé ; une requête les nomme dans `X-Provisa-Role` et agit sous eux. (REQ-2057)

Pensez à un JWT : ses claims sont des faits sur la personne. Les pseudo-rôles transforment des faits choisis en rôles, de sorte qu'une autorisation peut les viser sans créer un rôle par combinaison.

### Méta-rôles

Lorsqu'une requête nomme plus d'un rôle, Provisa construit un méta-rôle éphémère identifié comme `meta:<ids sorted, joined by +>`. C'est un enfant unique de chaque rôle de l'ensemble nommé. Tout le pipeline s'exécute alors avec lui comme un rôle unique ordinaire.

Les droits sont l'union de ceux de chaque membre. Les filtres de lignes sont l'OR du filtre résolu de chaque membre ; si un membre qui lit une table n'a pas de filtre, le méta-rôle n'en a pas non plus — pas de filtre signifie toutes les lignes. Un méta-rôle n'est jamais stocké, listé ni exporté. Il est reconstruit à partir de ses membres lorsque le modèle change. (REQ-1620)

L'enregistrement d'audit nomme chaque rôle agissant, pas l'identifiant du méta-rôle.

### Comment une requête nomme des rôles {: #how-a-request-names-roles }

Un client envoie un rôle, ou un ensemble séparé par des virgules, dans `X-Provisa-Role`. Chaque rôle nommé doit être détenu. Un rôle qui n'est pas attribué à l'identité est refusé nommément.

Les droits et les données ne suivent que les rôles sous lesquels agit la requête : détenir un rôle n'est pas agir sous lui. Un rôle est ce rôle ; deux ou plus agissent sous leur méta-rôle. Un rôle agissant qui apporterait avec lui les droits de chaque rôle détenu ne signifierait rien ; une requête obtient donc le moindre privilège, et l'enregistrement d'audit nomme exactement ce qui a agi.

Les pseudo-rôles nommés entrent comme qualificatifs. Un qualificatif n'a ni droits, ni accès aux domaines, ni masques, ni filtres de lignes propres, il n'est donc pas membre des droits du méta-rôle : comme membre sans masques, il démasquerait chaque colonne, et comme membre qui ne lit aucune table, il mettrait fin à tout OR de filtres de lignes. Il n'entre que dans les autorisations : une autorisation ou une entrée de masque qui nomme `region__eu` atteint une requête qui nomme ce qualificatif à côté d'un rôle accordé. (REQ-2057)

### Ce que signifie `+` {: #what-means }

`+` apparaît à deux endroits, avec un sens différent dans chacun.

**Dans une entrée d'autorisation.** `analyst+region__eu` dans une liste `visible_to` ou `to` signifie que le lecteur les détient tous — `analyst` ET `region__eu` à la fois. Une entrée qui n'est pas un composé est un rôle unique. Entre les entrées d'une liste, la règle est n'importe laquelle : un lecteur détient l'autorisation s'il détient une entrée quelconque. ET à l'intérieur, OU entre elles.

**Dans un identifiant de méta-rôle.** `meta:analyst+engineer` est l'identifiant du serveur pour le méta-rôle de l'ensemble {analyst, engineer}. Ses droits sont l'union des deux. Un client ne nomme jamais directement l'identifiant du méta-rôle ; il nomme les rôles qu'il détient, et le serveur construit le méta-rôle.

### Règles de colonne ABAC en simple RBAC {: #abac-column-rules-as-plain-rbac }

Une politique fondée sur les attributs demande : « cet utilisateur, avec ces attributs, peut-il voir cette colonne ? » RBAC demande : « quels rôles peuvent voir cette colonne ? » Les autorisations composées et les pseudo-rôles en font la même question.

« Démasquer cet e-mail uniquement pour un analyste dans l'UE » est une entrée de masque `{to: [analyst+region__eu], mask: null}`. Tout lecteur qui détient à la fois le rôle `analyst` et le pseudo-rôle `region__eu` lit la colonne non masquée. Pas de langage de politique. Pas de rôle par combinaison.

Le compromis est le calcul plutôt que l'énumération. Un petit catalogue de rôles réduit la maintenance, mais « qui peut voir cet e-mail de l'UE ? » devient une réponse calculée : les entrées composées sont évaluées contre les attributions et les claims de chaque utilisateur. Provisa peut répondre dans les deux sens parce que le modèle est une seule définition. La vérification d'accès de l'éditeur de colonnes (`POST /admin/masks/check`) exécute cette évaluation pour un ensemble de rôles choisi.

### Avantages {: #benefits }

- Une règle de colonne ABAC devient une entrée d'autorisation RBAC.
- Un petit catalogue de rôles au lieu d'un rôle par combinaison.
- Une seule définition qui coordonne les règles de chaque région.
- Le moindre privilège et un enregistrement d'audit exact par requête.

### Bonnes pratiques {: #best-practices }

- Gardez le catalogue de rôles aussi petit que l'organisation le permet.
- Héritez au sein d'un axe (ancienneté, niveau d'habilitation). Composez entre axes indépendants (rôle + région, rôle + service).
- Placez en premier les entrées de masque les plus spécifiques — une entrée antérieure l'emporte même lorsque le lecteur en détient une ultérieure.
- Vérifiez l'accès effectif avec `POST /admin/masks/check` avant de déployer une combinaison en production.

## Modèle de permissions des colonnes {: #column-permission-model }

Chaque colonne a un modèle de permissions qui contrôle l'accès en lecture, en écriture et le masquage par rôle. (REQ-042, REQ-249)

### Visibilité à trois niveaux {: #three-tier-visibility }

| Niveau | Condition | Résultat |
| ------ | ----------- | -------- |
| **Masquée à la vue** | Rôle absent de `visible_to` | Colonne absente du schéma et du catalogue |
| **Masquée** | Rôle dans `visible_to` ; la première entrée de masque qu'il détient a un `mask` | Colonne visible, valeur remplacée en sortie |
| **Non masquée** | Rôle dans `visible_to` ; la première entrée de masque qu'il détient n'a pas de `mask`, ou il n'en détient aucune | Accès complet en lecture |

### Colonnes publiques {: #public-columns }

Chaque colonne a un `scope`, défini dans le fichier du modèle, l'API d'administration ou la page Tables. Les trois portées sont ordonnées selon ceux qu'elles servent : `restricted`, puis `domain`, puis `public`. Un éditeur de table (`table_registration` dans le domaine de la table) peut restreindre la portée d'une colonne. L'élargir — `restricted` vers `domain` ou `public`, `domain` vers `public` — sert la colonne à davantage de rôles et requiert le droit que requiert son octroi : `column_grant` ou `access_config` dans le domaine de la table. Une colonne en cours d'enregistrement est comparée à `domain`, donc l'enregistrer en `public` est un élargissement. Sur une colonne portant une étiquette sensible, tout changement de portée requiert `sensitive_data`. Un changement refusé nomme la colonne et le droit. (REQ-1959, REQ-1943, REQ-1944)

| Portée | Servie à |
| ------- | ----------- |
| `domain` (par défaut) | Les rôles qui atteignent le domaine de la table, selon `visible_to`. Un `visible_to` vide correspond à chacun de ces rôles. |
| `public` | Les mêmes rôles, et aussi les rôles qui n'atteignent pas le domaine de la table. |
| `restricted` | Seulement les rôles que nomme `visible_to`. Un `visible_to` vide ne correspond à personne. |

Publier une colonne retire la condition de domaine pour cette colonne et rien d'autre :

- Un rôle hors du domaine se voit servir la table avec ses seules colonnes publiques. Les autres colonnes de la table, et les tables qui ne publient rien, restent hors de son schéma, de son catalogue et de son SQL.
- Le propre `visible_to` de la colonne décide toujours qui : une colonne publique accordée à un rôle n'est publique que pour ce rôle.
- Les masques, les filtres de lignes, les limites de lignes et la garde des relations s'appliquent comme à l'intérieur du domaine. Sans filtre de lignes pour le rôle, les colonnes publiées sont lues sans filtre ; un filtre de lignes sur la table ou son domaine s'applique toujours.
- Une relation est servie lorsque les colonnes de ses deux extrémités le sont.
- Une colonne de paramètre d'une table d'API (un argument de chemin ou de requête) n'est pas une donnée et ne publie rien.
- Le domaine `ops` exige une autorisation explicite pour chaque colonne (REQ-1133) ; rien en lui n'est servi entre domaines, quelle que soit sa portée.

La règle est la même sur chaque surface, car chacune lit le schéma compilé du rôle : GraphQL, SQL (HTTP, pgwire, Flight, JDBC), Cypher et Bolt, gRPC, REST et JSON:API, les catalogues Flight et MCP et les listes d'administration. Une portée qui n'est pas l'une des trois valeurs est refusée à l'enregistrement d'une colonne.

### Autorisations d'écriture {: #write-grants }

`visible_to` contrôle la lecture d'une colonne (vide signifie que tous les rôles peuvent lire). L'écriture est contrôlée par table avec `write_grants`, et non par colonne. Une table n'a aucune autorisation par défaut, donc aucun rôle ne peut y écrire. (REQ-2074)

Une autorisation d'écriture indique ce qu'un rôle peut écrire dans une table par une opération (`insert`, `update` ou `delete`) :

| Champ | Rôle |
| ------- | --------- |
| `columns` | Les colonnes qu'un `INSERT` peut lister ou qu'un `UPDATE` peut définir |
| `rows` | Les lignes qu'un `UPDATE` ou un `DELETE` peut toucher (un prédicat SQL) |
| `check` | Ce que doit satisfaire chaque ligne qu'écrit un `INSERT` ou que laisse un `UPDATE` (un prédicat SQL) |
| `presets` | Colonnes que l'autorisation définit elle-même, à partir d'une variable de session, d'un littéral ou de l'instant de l'écriture. L'instruction ne peut pas les fournir. |

Si vous omettez `rows` ou `check`, l'autorisation prend le filtre de lignes en lecture du rôle sur la table, de sorte qu'un rôle écrit les lignes qu'il lit. `"TRUE"` signifie toutes les lignes.

Une seule admission décide de chaque écriture sur chaque surface, dans cet ordre : le rôle détient le droit `write` ; il détient une autorisation pour l'opération sur la table ; chaque colonne qu'écrit l'instruction figure dans l'autorisation ; les presets sont ajoutés ; `rows` est combiné par AND dans le `WHERE` ; et `check` est évalué sur les nouvelles lignes avant que quoi que ce soit soit envoyé à la source. Un refus nomme la table, le rôle et l'opération. (REQ-2074)

Les instructions plus larges demandent des autorisations plus larges. `TRUNCATE` demande une autorisation `delete` sur toutes les lignes. `MERGE` demande une autorisation `insert` et une autorisation `update` sur toutes les lignes. `INSERT ... ON CONFLICT DO UPDATE` demande une autorisation `update` sur toutes les lignes, sans presets. `COPY` demande une autorisation `insert` sans presets.

GraphQL propose à chaque rôle un champ de mutation pour exactement les opérations pour lesquelles il détient des autorisations, avec les colonnes de l'autorisation. Un rôle enfant hérite des autorisations de son parent, les siennes d'abord. Un méta-rôle détient le repli des autorisations de ses membres : l'union des colonnes, le OR de `rows` et de `check`, et seulement les presets que partagent tous les membres. Dans l'interface d'administration, la section **Écritures** d'une table liste et modifie ses autorisations.

### Exemple {: #example }

```yaml
tables:
  - source_id: hr-pg
    domain_id: people
    schema: public
    table: employees
    columns:
      - name: id
        visible_to: []                # all can read
      - name: email
        visible_to: [admin, analyst, viewer]
        masks:
          - to: [admin]               # admin sees raw value (no mask)
          - to: ["*"]                 # everyone else sees it masked
            mask: {type: regex, pattern: "(.).*@", replace: "$1***@"}
      - name: salary
        visible_to: [admin, hr]
        masks:
          - to: [admin, hr]           # admin and hr see real value
          - to: ["*"]                 # all other visible roles see 0
            mask: {type: constant, value: 0}
      - name: created_at
        visible_to: []                # all can read
    write_grants:
      - role: admin
        operation: update
        columns: [email]
        rows: "TRUE"
      - role: hr
        operation: update
        columns: [salary]
      - role: hr
        operation: insert
        columns: [email, salary]
        presets:
          - {column: created_at, source: now}
```

Dans cet exemple :

- `email` : admin voit `alice@example.com` et peut le mettre à jour sur n'importe quelle ligne ; analyst et viewer voient `a***@example.com` et ne peuvent pas écrire
- `salary` : admin et hr voient la valeur réelle ; hr peut la mettre à jour sur les lignes que hr peut lire, car l'autorisation n'indique pas de `rows` ; tout autre rôle visible voit `0`
- `created_at` : tout le monde peut le lire, aucun rôle ne le fournit, et un insert de hr le définit à l'instant de l'écriture

## Autorisation des mutations {: #mutation-authorization }

Les mutations enregistrées (GraphQL distant, OpenAPI, gRPC, Hasura) sont protégées par deux vérifications indépendantes. (REQ-868) Un rôle ne peut invoquer une mutation que s'il détient la capacité globale `write` ET que la commande lui est affectée par `visible_to` (une liste vide l'affecte à tous les rôles). (REQ-868) Une commande n'a pas de liste `writable_by` ; la configuration en refuse une par son nom. [tool-verified: `provisa/security/mutation_authz.py` `command_assigned`, `admit_command` ; `provisa/core/models.py` `_one_role_list`]

Les mutations sont classées comme écritures par contrat, pas selon ce que déclare l'appelant. (REQ-869) Un `SELECT` qui fait référence à une fonction de type mutation est promu en écriture et soumis au même contrôle à deux portes, de sorte qu'un appelant ne peut pas invoquer une mutation en la déguisant en lecture. (REQ-869) Reclasser une mutation comme sûre en lecture requiert la capacité `access_config` et est consigné comme une décision de gouvernance ; il n'y a pas d'exclusion par requête. (REQ-870)

## Visibilité du schéma {: #schema-visibility }

Les schémas GraphQL par rôle masquent le contenu non autorisé : (REQ-039)

- **Accès aux domaines** : un rôle ne voit les tables que dans ses domaines `domain_access` (`"*"` = tous) (REQ-039)
- **Visibilité des colonnes** : les colonnes absentes de `visible_to` pour un rôle sont omises du SDL (REQ-039)
- Les tables/colonnes non autorisées n'apparaissent pas dans le schéma (REQ-039)

## Sécurité au niveau des lignes (RLS) {: #row-level-security-rls }

Injection de clause SQL WHERE par table et par rôle. Appliquée après la compilation, avant l'exécution. (REQ-041, REQ-263)

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
```
Le filtre est combiné par AND à la clause WHERE de la requête. Fonctionne pour les requêtes comme pour les mutations (UPDATE/DELETE). (REQ-035, REQ-041)

Un terme `current_setting('provisa.<name>')` se résout contre les variables de session que lie la requête : `user_id` depuis l'identité authentifiée, `role` depuis le rôle agissant, et chaque claim scalaire du jeton sous son nom en minuscules sans le préfixe `x-hasura-`, de sorte qu'un filtre Hasura importé sur `X-Hasura-User-Id` lit `provisa.user_id`. Les `session_vars` configurées d'un rôle sont les constantes de base ; les liaisons de la requête les recouvrent. En mode non sécurisé, un en-tête `x-provisa-session-<name>` lie `<name>`. Une variable liée nulle part se résout en NULL et ne correspond à aucune ligne. (REQ-1682)

## Masquage au niveau des colonnes {: #column-level-masking }

Le masquage est défini comme une liste ordonnée sur chaque colonne. La première entrée que détient le lecteur décide de ce qu'il voit. (REQ-2059, REQ-263)

| Type de masque | Types de colonne pris en charge | Expression SQL |
| ----------- | ----------------------- | --------------- |
| `regex` | Chaîne (varchar, char, text) | `REGEXP_REPLACE(col, pattern, replace)` |
| `constant` | Tout | Valeur littérale (`NULL`, `0`, une chaîne personnalisée) |
| `truncate` | Date/Horodatage | `DATE_TRUNC(precision, col)` |

Chaque entrée a une liste `to` qui nomme les lecteurs qu'elle vise, et un `mask` facultatif. Une entrée sans `mask` signifie non masqué pour ces lecteurs. `*` dans `to` correspond à tout lecteur.

L'expression de masque est poussée dans la projection SQL SELECT. La base de données renvoie la valeur masquée ; les données non masquées ne traversent jamais le réseau pour les rôles masqués. (REQ-263) Les colonnes masquées sont aussi bloquées dans les clauses `WHERE` et `HAVING` (garde des prédicats de la couche 5) pour empêcher de déduire des valeurs par filtrage. (REQ-263, REQ-531)

**Un masque couvre tous les noms de la colonne, pas une colonne liée.** Une colonne lue par son alias ou par son nom physique est masquée de la même façon. Un masque ne cache pas une clé qu'une relation approuvée joint à une colonne que le rôle peut voir : la jointure compare les valeurs réelles, donc la colonne liée montre ce que contient la clé masquée. Masquez aussi la colonne liée, ou n'approuvez pas la relation pour ce rôle. (REQ-263, REQ-019)

**Aucune entrée détenue signifie non masqué.** Un lecteur qui ne détient aucune entrée lit la colonne non masquée. Terminez une liste par une entrée `*` lorsque chaque lecteur doit recevoir un masque. L'éditeur de colonnes avertit lorsqu'une liste n'a pas d'entrée `*` (`unlisted_unmasked`).

**Refuser par l'ordre.** Une entrée antérieure l'emporte même lorsque le lecteur en détient aussi une ultérieure plus permissive. Pour que les prestataires voient toujours un salaire masqué quels que soient leurs autres rôles, placez l'entrée des prestataires avant l'entrée non masquée. Un rôle accordé ne perd jamais ce que son parent accorde par héritage, mais une entrée de masque placée plus tôt dans la liste s'applique en premier — c'est ainsi que fonctionne la restriction ici.

**Méta-rôles.** La chaîne d'un méta-rôle comprend les rôles de chaque membre, leurs ancêtres et tout pseudo-rôle nommé. Une entrée accordée à n'importe quel membre atteint le méta-rôle. Une entrée placée avant, détenue par un autre membre, s'applique toujours en premier.

**Vérifier l'accès.** `POST /admin/masks/check` accepte une liste de masques et un ensemble facultatif de rôles, et renvoie l'entrée qui s'applique pour ces rôles ainsi que les entrées qu'aucun lecteur ne peut atteindre. Protégé par `masking_config`.

```yaml
# email column: analyst+region__eu sees raw; everyone else sees masked
masks:
  - to: [analyst+region__eu]          # composite: analyst AND in EU region
  - to: ["*"]                         # all others
    mask: {type: regex, pattern: "(.).*@", replace: "$1***@"}

# hire_date column: hr sees raw; all other visible roles see year only
masks:
  - to: [hr]
  - to: ["*"]
    mask: {type: truncate, precision: year}
```
## Gouvernance des réponses d'actions {: #governing-action-responses }

La réponse d'une fonction suivie ou d'un webhook est gouvernée comme les lignes d'une table. (REQ-1679) Les lignes que renvoie une action sont liées comme une relation dont les colonnes sont le contrat de sortie déclaré de l'action — `output_columns` pour une fonction, `inline_return_type` pour un webhook, ou les propriétés d'objet du `return_schema` de type tableau d'une fonction — et cette relation passe par la même étape de gouvernance que toute lecture de table, sur une CTE VALUES contenant les lignes. (REQ-1679) Rien de la RLS, du masquage ou de la visibilité n'est réimplémenté pour les actions.

Trois choses s'appliquent, dans le même ordre que pour une table. (REQ-1679)

- Un filtre de lignes : la règle RLS du rôle enregistrée contre l'action par son nom (`upsertRlsRule` avec `actionName`), ou, lorsque l'action n'en a pas, la règle de domaine du domaine de l'action. Le prédicat est validé contre le contrat à l'enregistrement, comme une règle de table. (REQ-1676)
- Visibilité des colonnes : une colonne du contrat qui déclare `visible_to` est retirée pour un rôle que la liste ne nomme pas ; une colonne qui n'en déclare aucun fait partie de la forme publique de l'action et reste.
- Masques : une colonne du contrat porte `masks`, la même liste ordonnée de masques qu'une colonne de table. (REQ-2059)

L'héritage des rôles résout la chaîne pour les trois. (REQ-1677) Une réponse qui renvoie une colonne hors du contrat déclaré est refusée avec un 502 plutôt que transmise sans gouvernance. Une action qui ne déclare aucun contrat de colonnes renvoie un scalaire et n'a rien à lier ; elle est renvoyée telle quelle. L'invocation de test d'administration indique les filtres, exclusions et masques appliqués, et l'instruction gouvernée est écrite dans le journal d'audit des requêtes.

## Échantillonnage {: #sampling }

Tous les rôles voient des résultats échantillonnés (par défaut : 100 lignes) sauf s'ils ont la capacité `full_results`. (REQ-554) Contrôlé par la variable d'environnement `PROVISA_SAMPLE_SIZE`. (REQ-554)

## Journalisation d'audit {: #audit-logging }

Chaque requête qui touche un actif de domaine est consignée dans `query_audit_log`. (REQ-596, REQ-613) Le journal d'audit appartient à l'organisation, pas à la plateforme : le registre propre de la plateforme pour les organisations, les utilisateurs et les invitations ne contient aucun enregistrement d'audit. (REQ-1922) Le `query_audit_log` de chaque organisation est une table dans le schéma propre à cette organisation, dans le magasin que l'organisation désigne pour son enregistrement dans chaque région qu'elle sélectionne. Une organisation qui ne déclare aucune région conserve ce schéma dans la base de données du plan de contrôle du déploiement, à côté de son modèle.

Chaque ligne capture `tenant_id` (l'organisation), `user_id`, `role_id`, un hachage SHA-256 du texte de la requête, le texte de la requête lui-même chiffré (REQ-689), `table_ids` et `table_names`, `source`, `status_code`, `duration_ms`, la route et le nombre de lignes, ce qui a été appliqué à l'instruction, et `logged_at`. Le texte de la requête est chiffré avec la clé de l'organisation lorsque celle-ci en a défini une, sinon avec la clé du déploiement (REQ-1574). Un déploiement qui ne configure aucun fournisseur de chiffrement le stocke en clair. Le texte n'est jamais exposé dans les tables d'exploitation interrogeables, et n'est déchiffré que lors de lectures d'administration autorisées.

Le journal est en ajout seul au niveau de la base de données : les règles PostgreSQL transforment `DELETE` et `UPDATE` sur `query_audit_log` en opérations nulles. (REQ-596, REQ-613) Deux index, `(tenant_id, logged_at)` et `(user_id, logged_at)`, prennent en charge les requêtes de conformité par organisation et par utilisateur sur des plages de temps. (REQ-596, REQ-613)

Rétention : les lignes sont conservées indéfiniment. Provisa n'a aucun réglage de rétention et ne purge jamais le journal d'audit ; comme `DELETE` est une opération nulle, supprimer d'anciennes lignes suppose qu'un opérateur supprime ou archive les données de la table en dehors de Provisa.

## Limitation de débit {: #rate-limiting }

Les limites de débit par rôle se configurent dans `provisa.yaml` : nombre maximal de requêtes par seconde, d'abonnements SSE simultanés et de flux Arrow Flight simultanés. (REQ-369) Les limites s'appliquent dans la couche API avant compilation ou exécution ; les requêtes au-delà de la limite sont refusées avec HTTP 429 et un en-tête `Retry-After`. (REQ-369)

Le service de requêtes NL (`POST /query/nl`) a une limite indépendante via `nl.rate_limit` (requêtes par minute et par rôle). Les requêtes au-delà de la limite sont refusées avant tout appel à un LLM. (REQ-370)

L'état des limites de débit vit dans Redis (`cache.redis_url`) sous forme de compteur à fenêtre glissante — sans état par instance — de sorte que les limites tiennent sur toutes les instances horizontales de Provisa. (REQ-371)

## Limite de complexité des requêtes {: #query-complexity-limit }

Une limite de débit plafonne le nombre de requêtes qu'envoie un rôle. La limite de complexité plafonne ce qu'une instruction peut demander. (REQ-1174)

Chaque instruction a un score de complexité : les relations qu'elle lit, ses jointures, les colonnes qu'elle sélectionne (un `*` compte pour les colonnes qu'il représente) et les requêtes imbriquées en elle. Une relation lue depuis une source d'API distante (OpenAPI, GraphQL distant, gRPC distant) compte pour 10, car chaque lecture consomme un budget que le système distant fixe pour tous les utilisateurs de cette source.

La limite est `max_query_complexity` dans le `rate_limit` d'un rôle. Le réglage à l'échelle de l'organisation `limits.max_query_complexity` est un plafond pour chaque rôle : un rôle peut fixer une limite plus basse, et une limite plus haute n'a aucun effet. Aucun des deux n'est défini par défaut.

```yaml
roles:
  - id: analyst
    rate_limit:
      max_query_complexity: 200
```
Le score est mesuré après l'analyse d'une requête et avant sa gouvernance ou son exécution, de sorte qu'une même limite tient sur chaque interface par laquelle une instruction peut arriver : GraphQL, SQL sur HTTP, pgwire, Arrow Flight, gRPC, Cypher, JSON:API et MCP. Une instruction au-delà de la limite est refusée avec son score, la limite et ce qu'elle demandait. Les interfaces HTTP répondent 413. Le refus est consigné comme un refus de politique.

## Authentification {: #authentication }

Fournisseurs d'authentification enfichables : (REQ-120)

| Fournisseur | Identifiant | Cas d'usage |
| ---------- | ----------- | ---------- |
| `none` | En-tête X-Provisa-Role | Développement uniquement |
| `basic` | Comptes locaux (bcrypt) + JWT de session | Déploiements autonomes |
| `firebase` | Jeton d'ID Firebase | Production |
| `keycloak` | JWT Keycloak | Entreprise |
| `oauth` / `oidc` | JWT OIDC | PingFed, Okta, Azure AD, Auth0 |
| `ldap` | Nom d'utilisateur et mot de passe de l'annuaire (LDAP ou Active Directory) | Annuaires d'entreprise (REQ-1265) |
| `saml` | Assertion signée SAML 2.0, puis un JWT de session | Authentification unique d'entreprise (REQ-1265) |
| `simple` | bcrypt + JWT | Tests |

`ldap` connecte un utilisateur par recherche puis liaison : Provisa se lie en tant que compte de service, trouve l'unique entrée que désigne le nom d'utilisateur, puis se lie en tant que cette entrée avec le mot de passe présenté, de sorte que l'annuaire le vérifie. Les groupes de l'utilisateur deviennent ses rôles. Le compte et ses groupes sont relus à chaque retour d'un jeton de session, de sorte qu'un utilisateur retiré de l'annuaire, ou d'un groupe, perd cet accès avant l'expiration du jeton. `ldaps://` et StartTLS sont tous deux pris en charge.

`saml` est du SSO Web Browser initié par le SP. La signature XML de la réponse est vérifiée par rapport aux métadonnées du fournisseur d'identité, et les claims ne sont lus que dans l'élément signé. L'émetteur (Issuer), l'audience (Audience), l'URL ACS, l'expiration et `InResponseTo` sont tous vérifiés. Les réponses non sollicitées (initiées par l'IdP), les assertions chiffrées, les documents avec DOCTYPE, les assertions multiples et les signatures SHA-1 sont refusés. Une session SAML n'est pas revérifiée auprès du fournisseur d'identité : un utilisateur retiré de ce côté conserve l'accès jusqu'à l'expiration du jeton de session.

Correspondance des rôles : claims d'identité → rôle Provisa via des règles configurables. (REQ-120) Le champ `assignments_source` contrôle d'où viennent les attributions de rôles : `claims` les lit dans les claims du jeton JWT (par défaut), `provisa` les lit dans le magasin d'attributions interne de Provisa. (REQ-551) Un déploiement multi-tenant (`multitenancy: true`) accorde lui-même les rôles, par organisation, et doit donc utiliser `assignments_source: provisa` : avec `claims`, il ne démarre pas, et un changement de réglages qui produirait cette combinaison est refusé. Un déploiement mono-tenant peut utiliser l'un ou l'autre.

Un superutilisateur configuré dans `provisa.yaml` (nom d'utilisateur plus un mot de passe issu d'un secret d'environnement) reçoit toujours le rôle admin et toutes les capacités, quel que soit le fournisseur configuré — un chemin d'amorçage pour la configuration initiale. (REQ-125)

### Surfaces et identifiants {: #surfaces-and-credentials }

Chaque surface s'authentifie via le même contrat de fournisseur, de sorte qu'un identifiant qui fonctionne sur l'une fonctionne sur toutes, partout où le protocole peut le transporter. (REQ-124, REQ-1263) Ce tableau est la seule référence ; la documentation de chaque surface ne le répète pas.

| Surface | Mot de passe | Jeton du fournisseur | Jeton d'accès personnel | Certificat client (mTLS) |
| --------- | ---------- | ---------------- | ----------------------- | --------------------------- |
| HTTP (REST, JSON:API, GraphQL) | `Authorization: Basic` | `Authorization: Bearer` | `Authorization: Bearer` | via un proxy de terminaison |
| pgwire | champ de mot de passe (en clair ou SCRAM) | champ de mot de passe, déploiements OIDC | champ de mot de passe | oui |
| Bolt | schéma `basic` | schéma `bearer` | schéma `bearer` | oui |
| Arrow Flight | — | `token` dans la poignée de main ou la charge utile du ticket ; en-tête d'appel `authorization: Bearer` sur `list_flights`, `get_flight_info` et `get_schema` | idem | oui |
| gRPC | — | métadonnées `authorization` | métadonnées `authorization` | oui |
| Airport (extension DuckDB `airport`, sur option) | — | `authorization: Bearer` (l'`auth_token` du secret) | idem | — |
| MCP | — | `Authorization: Bearer` | `Authorization: Bearer` | via un proxy de terminaison |

La seule option d'authentification de l'extension DuckDB `airport` est l'`auth_token` bearer de son secret ; un client DuckDB y présente donc un jeton du fournisseur ou un jeton d'accès personnel et agit sous le rôle auquel se résout son identité. Un client qui peut définir des en-têtes d'appel peut demander un rôle détenu, ou un ensemble séparé par des virgules, dans `x-provisa-role`. (REQ-1120, REQ-1263)

Un refus indique son type sur chacune de ces surfaces. Un identifiant absent, rejeté ou inutilisable est un refus d'authentification : HTTP 401, gRPC `UNAUTHENTICATED`, Arrow Flight et Airport `Unauthenticated`. Un identifiant valide qui demande à agir sous un rôle que son identité ne détient pas est un refus d'autorisation, avec le rôle nommé : HTTP 403, gRPC `PERMISSION_DENIED`, Arrow Flight et Airport `Unauthorized`. (REQ-273, REQ-1263) Une exception se trouve dans la bibliothèque cliente Arrow, pas dans le serveur : un refus de l'appel `get_schema` de Flight parvient à un client Python comme une erreur Arrow générique portant le motif du serveur, sans son type ; `list_flights`, `get_flight_info` et `do_get` transmettent le type.

Là où une cellule indique `—`, le protocole ne porte aucun champ de nom d'utilisateur auquel associer un mot de passe ; les formes de jeton le couvrent. pgwire est le cas inverse : le paquet de démarrage a un champ secret et aucun schéma, donc ce qu'*est* le secret choisit la méthode — un PAT est reconnu à son préfixe, le secret est lu comme un jeton bearer lorsque le fournisseur configuré est un fournisseur de jetons, et tout le reste est un mot de passe. Le choix se fait une seule fois — un identifiant que refuse le validateur choisi n'est pas réessayé avec un autre.

La matrice est imposée par `tests/unit/test_auth_surface_conformance.py`, qui exerce le véritable point d'entrée de validation de chaque surface et échoue lorsqu'une nouvelle surface est ajoutée sans ligne.

### Jetons d'accès personnels {: #personal-access-tokens }

Un PAT est un secret bearer de longue durée qu'un utilisateur génère pour un client qui ne peut pas effectuer de connexion interactive — un script, un outil BI, un pilote. (REQ-1263) Il porte sa propre organisation et son propre rôle, et chaque surface le résout via le même validateur, de sorte qu'aucune surface n'a besoin de savoir ce qu'est un PAT.

La forme filaire est `provisa_pat_` suivi de 43 caractères base64 sûrs pour les URL. Le préfixe est ce qui dirige un secret présenté vers le magasin de jetons plutôt que vers le fournisseur d'identité, et il rend un jeton divulgué repérable dans les journaux et les dépôts.

- **Stockage** — seul le SHA-256 du secret est conservé. Le secret lui-même n'est affiché qu'une fois, à la création, et ne peut pas être récupéré. La liste porte le préfixe d'affichage et les horodatages du cycle de vie, jamais un identifiant fonctionnel.
- **Émission et révocation** — `POST /auth/tokens`, `GET /auth/tokens`, `DELETE /auth/tokens/{token_hash}`, plus la section en libre-service du profil propre de l'utilisateur dans l'interface d'administration. Générer et révoquer un identifiant est l'acte du détenteur du jeton.
- **Attribution** — un PAT validé se résout vers le compte de son propriétaire : identifiant utilisateur, e-mail et nom affiché. Une ligne d'audit ou un rapport d'usage écrit sous un PAT nomme donc la personne, pas l'identifiant. Lequel des jetons de cette personne a agi est porté séparément, dans `raw_claims["token_name"]`.
- **Expiration** — un jeton peut porter une expiration ; un jeton expiré est refusé à la validation. Supprimer l'appartenance d'un utilisateur révoque ses jetons avec elle.

### SCRAM-SHA-256 sur pgwire {: #scram-sha-256-on-pgwire }

Avec le fournisseur `basic`, définir `auth.scram: true` fait annoncer à pgwire SASL (code d'authentification 10) avec le mécanisme `SCRAM-SHA-256`, de sorte qu'un mot de passe est prouvé plutôt qu'envoyé. (REQ-1394) Le channel binding (`SCRAM-SHA-256-PLUS`) n'est pas proposé.

SCRAM a besoin d'un vérificateur RFC 5802, qui ne peut pas être dérivé d'un hachage bcrypt. Un vérificateur est écrit chaque fois qu'un mot de passe passe en clair — inscription, connexion, changement de mot de passe, réinitialisation par un administrateur — de sorte qu'un déploiement qui active SCRAM collecte des vérificateurs à mesure que ses utilisateurs s'authentifient, et la première connexion SCRAM de chaque utilisateur suit sa prochaine saisie de mot de passe. Un utilisateur sans vérificateur reçoit un échange factice impossible à distinguer d'un vrai, afin que le réseau ne révèle pas qui a migré.

### TLS mutuel {: #mutual-tls }

La vérification du certificat client déplace le premier contrôle dans la poignée de main TLS : un appelant sans certificat signé par la CA du déploiement n'atteint jamais la couche des identifiants. (REQ-1228) Elle est disponible sur pgwire, Bolt, gRPC et Arrow Flight — les quatre transports qui terminent leur propre TLS.

| Variable | Signification |
| ---------- | --------- |
| `PROVISA_MTLS_CLIENT_CA` | Paquet PEM des CA autorisées à signer les certificats clients |
| `PROVISA_MTLS_MODE` | `required` (par défaut dès qu'une CA est définie) ou `optional` |
| `PROVISA_MTLS_BIND_PRINCIPAL` | Si vrai, le nom commun du certificat doit égaler le nom d'utilisateur sous lequel la connexion s'authentifie ensuite |

Les surcharges par protocole suivent le même nommage que les réglages TLS. Rien n'est déduit : un mode défini sans CA refuse de démarrer, et un mode non reconnu refuse de démarrer plutôt que d'être lu comme le voisin le plus sûr — un déploiement qui croit exiger des certificats clients et ne le fait pas est en plus mauvaise posture qu'un déploiement qui ne démarre pas.

### Versions de TLS {: #tls-versions }

Chaque écouteur que Provisa termine (l'API HTTP et l'UI, pgwire, Bolt, MCP, gRPC et Arrow Flight) accepte TLS 1.2 et TLS 1.3, et refuse TLS 1.0 et 1.1. Provisa ne fixe elle-même ni version de protocole ni liste de chiffrements. Les écouteurs HTTP, pgwire et Bolt utilisent les valeurs par défaut serveur de `ssl` de Python (minimum TLS 1.2). gRPC et Arrow Flight utilisent les valeurs par défaut de gRPC (minimum TLS 1.2). Les versions ne sont pas configurables.

### Limitation des connexions {: #login-throttling }

Deviner des mots de passe ne dépend pas du protocole : le même compte peut être attaqué via HTTP, pgwire et Bolt. Le compteur vit donc dans la couche de validation des identifiants, pas sur une surface particulière, de sorte qu'un verrouillage obtenu n'importe où s'applique partout. (REQ-1393)

Elle est active par défaut — cinq échecs en cinq minutes verrouillent le sujet pendant quinze minutes — et se règle sous `auth.login_throttle`. Un sujet verrouillé est refusé avant même que l'identifiant soit examiné, et une authentification réussie efface l'historique de ce sujet.

La clé est le principal que porte le protocole. Une surface uniquement bearer ne porte pas de principal, la clé est donc un condensé de l'identifiant lui-même ; ce que cela empêche, c'est qu'un mauvais jeton soit rejoué sans limite. Le magasin est par processus, donc un déploiement qui exécute plusieurs workers d'API autorise jusqu'à `max_attempts` par worker — la limitation freine la devinette, ce n'est pas un quota distribué.

### Nommer l'organisation {: #naming-the-org }

En mode multi-tenant, chaque requête nomme l'organisation à laquelle elle est destinée. Appartenir à exactement une organisation ne la nomme pas : une requête qui n'en nomme aucune est refusée, et le refus indique quoi envoyer. (REQ-1235) Chaque surface nomme une organisation ainsi :

| Surface | Comment un client nomme l'organisation |
| ------- | -------------------------- |
| HTTP (interface, REST, GraphQL) | Le nom d'hôte propre de l'organisation, `acme.provisa.dev`, ou l'en-tête `X-Org-Provisa` sur l'hôte du plan de contrôle |
| pgwire | Le nom de la base de données (`psql -d acme`, le champ de base de données d'un outil BI), ou le nom d'hôte propre de l'organisation composé en TLS |
| Bolt | Le nom de la base de données, `<org>.provisa_<role>` (ou `<org>.provisa_ops_<role>`), ou le nom d'hôte propre de l'organisation composé en TLS |
| Arrow Flight | `"org"` dans le ticket |
| gRPC | L'en-tête de métadonnées `x-provisa-org` |
| MCP (transport HTTP) | Le nom d'hôte propre de l'organisation, ou l'en-tête `X-Org-Provisa` |
| Client Python | `org=` sur `ProvisaClient`, `connect()` et `adbc_connect()` ; `?org=` dans une URL SQLAlchemy |

Un client pgwire qui compose un nom d'hôte en TLS l'envoie dans le ClientHello TLS, et Provisa y lit l'organisation (REQ-1234). Lorsque le nom d'hôte et le nom de la base de données nomment tous deux une organisation et diffèrent, la connexion est refusée ; aucun n'est préféré. Le nom de base de données `provisa`, la valeur par défaut, ne nomme aucune organisation, et un outil BI connecté à une organisation affiche
cette organisation comme sa base de données. Sur Bolt, `SHOW DATABASES` liste un `<org>.provisa_<role>` par organisation à laquelle
l'utilisateur appartient ; un simple `provisa_<role>` est refusé en mode multi-tenant, et une session reste dans
l'organisation qu'elle a nommée en premier.

Un nom est une demande, pas une autorisation. Il atteint partout le même résolveur, qui refuse une organisation dont le principal authentifié n'est ni membre ni détenteur du droit inter-organisations.

Un jeton d'accès personnel est émis pour une organisation et la nomme lui-même. Il n'ouvre que cette organisation : une requête qui en nomme une autre est refusée, même lorsque le propriétaire du jeton appartient aux deux.

Un déploiement mono-tenant n'a pas d'organisation à nommer. Aucune surface n'y lit un nom d'hôte, un nom de base de données ou un en-tête à cet effet.

## Secrets entre organisations {: #secrets-across-organizations }

Les secrets d'une organisation ne sont lus que par cette organisation. Dans le magasin propre de Provisa, chaque secret est conservé sous son organisation. Dans un service central (Vault, AWS Secrets Manager, GCP Secret Manager, Azure Key Vault) d'un déploiement de nombreuses organisations, chaque lecture se fait à l'intérieur du préfixe de l'organisation qui lit, et une référence qui en sortirait est refusée. (REQ-1557) Les préfixes et les étapes pour déplacer les secrets existants sous eux se trouvent dans [Secrets](secrets.md#one-organizations-secrets-in-a-central-service).

## Durcissement du déploiement {: #deployment-hardening }

### Identifiants des magasins et où ils écoutent {: #store-credentials-and-where-the-stores-listen }

Les magasins fournis n'ont aucun identifiant par défaut. Le plan de contrôle PostgreSQL, Redis, MinIO, le compte administrateur de Grafana et le secret partagé du cluster Trino reçoivent chacun une valeur générée pour le déploiement :

- Les lanceurs (`provisa start`, les installeurs de bureau et de serveur) les génèrent une fois et les conservent dans `~/.provisa/store-credentials.env` (mode 600). Une valeur déjà présente dans l'environnement est utilisée telle quelle, et un nœud secondaire, qui utilise les magasins du primaire, refuse de démarrer sans ceux du primaire.
- Les modules Terraform les génèrent avec `random_password` et donnent les mêmes valeurs à chaque nœud. Ils sont conservés dans l'état Terraform, protégez donc l'état comme vous protégeriez les mots de passe.
- Le chart Helm n'en définit aucun. Il exige un Secret que vous créez, désigné par `credentials.existingSecret`, contenant `pg-password`, `redis-password`, `minio-access-key`, `minio-secret-key`, `grafana-admin-password` et `trino-shared-secret`.

Redis exige son mot de passe, et le mot de passe du rôle PostgreSQL est redéfini à chaque démarrage pour qu'un volume créé auparavant suive le mot de passe actuel du déploiement. Les magasins, le moteur de fédération et les services de télémétrie publient leurs ports sur une seule adresse, `PROVISA_STORE_BIND_IP` : `127.0.0.1` sur un nœud isolé, ou l'adresse privée du primaire lorsque des secondaires s'y connectent. Le compte administrateur de Grafana reçoit le mot de passe généré ; la pile compose laisse des visiteurs anonymes consulter les tableaux de bord, et le chart Helm n'admet aucun visiteur anonyme.

### Chiffrement du transport {: #transport-encryption }

Chaque listener que Provisa termine sert du TLS (voir [Versions de TLS](#tls-versions)). Un nœud serveur utilise le certificat de `PROVISA_TLS_CERT`/`PROVISA_TLS_KEY`, ou en génère un auto-signé. Le chart Helm sert du TLS par défaut (`tls.enabled: true`) à partir du Secret `kubernetes.io/tls` que désigne `tls.secretName`.

### Le PostgreSQL embarqué {: #the-embedded-postgresql }

Les niveaux bureau et natif exécutent PostgreSQL en mode embarqué. Sous macOS et Linux, il écoute sur un socket unix dans son répertoire de données (mode 700), que seul le compte propriétaire du répertoire atteint. Par TCP (Windows, où il écoute sur 127.0.0.1), chaque connexion doit fournir un mot de passe généré pour le répertoire de données et conservé dedans (`provisa.password`, mode 600). Il n'est jamais démarré en root.

### Le moteur de fédération (Trino) {: #the-federation-engine-trino }

- **Entre nœuds.** Un coordinateur et ses workers s'authentifient mutuellement avec le secret partagé généré du déploiement (`internal-communication.shared-secret`). Chiffrer leur trafic est une option de l'opérateur, désactivée par défaut : désactivée, il reste dans le réseau privé ou le cluster du déploiement. Activez-la là où une exigence de conformité ou de zéro confiance demande le chiffrement à l'intérieur du déploiement. Trino documente qu'elle ralentit les requêtes à fort shuffle (jointures distribuées, agrégations, fonctions de fenêtre) de 10 % à plus de 100 %. Sur un déploiement compose, c'est le paramètre `engine.trino_internal_tls` (Paramètres du déploiement → Moteur), avec le réseau du moteur et l'adresse épinglée du coordinateur à côté, et il s'applique au démarrage suivant. Sous le chart Helm, c'est la valeur `trino.internalTls`, avec `trino.coordinatorClusterIP`. Les moteurs de la plateforme hébergée sont des coordinateurs à nœud unique, sans trafic entre nœuds, et la refusent. (REQ-2072)
- **Données déversées.** L'exécution tolérante aux pannes chiffre les données d'échange qu'elle déverse avec une clé générée pour chaque requête et supprimée à la fin de celle-ci (`fault-tolerant-execution.exchange-encryption-enabled=true`, défini explicitement). Le déversement sur disque hérité n'est pas utilisé.
- **Organisations sur un moteur.** Les catalogues de chaque organisation lui appartiennent, nommés d'après l'organisation, et les instructions de chaque organisation s'exécutent dans son propre groupe de ressources.

### Sources sur un déploiement partagé {: #sources-on-a-shared-deployment }

Sur un déploiement qui héberge de nombreuses organisations, les sources d'une organisation ne peuvent atteindre que des adresses publiques : jamais le réseau privé de la plateforme, où se trouvent son plan de contrôle, son cache et son magasin d'objets. La vérification s'applique lorsqu'une source est enregistrée, par l'interface, l'API d'administration ou une application de configuration, puis de nouveau à chaque connexion et à chaque redirection HTTP. Les sources d'un bac à sable de démonstration sont en outre tenues à l'adresse à laquelle elles ont été vérifiées. L'organisation propre du déploiement, dont l'opérateur configure les sources, n'y est pas soumise.

## Chiffrement au repos et clés propres de l'organisation {: #encryption-at-rest-and-the-organizations-own-keys }

Le chiffrement sous une clé propre à l'organisation est disponible, et il reste désactivé tant que l'organisation ne l'active pas. Rien n'est chiffré sous une clé de l'organisation à moins qu'un administrateur de l'organisation n'en crée une.

**La clé de chiffrement de l'organisation** (REQ-1574) enveloppe les secrets de l'organisation, ses colonnes chiffrées et le texte des requêtes de son audit. Un administrateur de l'organisation la crée sous **Admin → Org Secrets → Org Key**, ou avec `PUT /admin/org-encryption` : sans corps, Provisa génère la clé, qui n'est jamais affichée, et un corps `key_b64` fournit 32 octets de votre propre clé. `GET /admin/org-encryption` ne signale que son empreinte. Sans clé d'organisation, les données de l'organisation sont enveloppées avec la clé du déploiement (`encryption.provider` dans `provisa.yaml`). Un déploiement qui ne configure aucun fournisseur de chiffrement stocke les colonnes chiffrées et le texte des requêtes d'audit sans chiffrement ; les secrets ne sont jamais stockés en clair (voir [Secrets](secrets.md#provisas-built-in-store)).

**Le dossier de l'organisation** (REQ-1992) contient ses fichiers téléversés et déposés, les fichiers de déversement de son moteur et ses fichiers temporaires. Un administrateur de l'organisation active son chiffrement sous **Admin → Org Secrets → File folder encryption**, ou avec `PUT /admin/org-files` (`{"encrypted": true, "key_reference": "${secret:NAME}"}`). `POST /admin/org-files/generate-key` répond une seule fois avec une nouvelle clé de 256 bits, à conserver dans le coffre. La clé reste dans le coffre propre de l'organisation (AWS Secrets Manager, GCP Secret Manager, Azure Key Vault ou HashiCorp Vault) : Provisa ne stocke que la référence, et gocryptfs garde chiffrés sur disque les fichiers du dossier sous cette clé. Cela requiert FUSE, donc un hôte ou conteneur Linux avec `/dev/fuse` ; sous macOS le paramètre est refusé.

Sur un déploiement auto-hébergé, qui exécute typiquement une organisation sur ses propres moteurs, activer les deux chiffre les secrets, fichiers, fichiers de déversement et fichiers temporaires de cette organisation sous ses clés :

- Le moteur DuckDB qu'exécute une organisation déverse dans le dossier de l'organisation.
- Les lignes qu'un filtre de lignes récupère et le code téléversé sont préparés dans le dossier de l'organisation.
- L'exécution tolérante aux pannes de Trino chiffre les données d'échange déversées avec une clé générée pour chaque requête et supprimée à la fin de celle-ci (`fault-tolerant-execution.exchange-encryption-enabled=true`). Provisa n'active pas le déversement sur disque hérité de Trino.
- Un moteur de fédération PostgreSQL écrit ses fichiers temporaires là où ce serveur PostgreSQL les conserve, sous le chiffrement de stockage propre au serveur, et non dans le dossier de l'organisation.

### Plateformes hébergées partagées et dédiées {: #shared-and-dedicated-hosted-platforms }

La plateforme hébergée partagée isole les organisations de façon logique. Un seul déploiement les sert toutes, et les données de chaque organisation lui restent propres grâce à la gouvernance, à ses propres schémas, catalogues et groupes de ressources sur le moteur, à ses propres clés, à des clés par requête pour les données déversées du moteur, et au refus de ses sources sur les adresses privées de la plateforme. Elle est recommandée pour l'expérimentation et les petites entreprises. Une plateforme hébergée dédiée est séparée physiquement : ses propres machines, réseau et magasins, au service d'un seul client.

### Python dans le navigateur {: #python-in-the-browser }

L'espace Data Science exécute Python dans le navigateur du visiteur. Chaque lecture qu'il effectue va au serveur en tant qu'utilisateur connecté, avec son rôle actif, son organisation et son environnement, et est gouvernée exactement comme la même instruction depuis tout autre client.

## Mode haute sécurité {: #high-security-mode }

`security.mode: high` dans `provisa.yaml` affirme une garantie : le backend de Provisa ne manipule jamais de données en clair. (REQ-693) Chaque colonne qui compte est chiffrée à la source, et seul un client détenant la clé de déchiffrement peut la lire. Cette garantie a des conséquences qu'un déploiement doit prévoir.

**Ce que fait le mode :**

- **Les endpoints de données exigent une preuve de déchiffrement côté client.** Tout ce qui se trouve sous `/data/` renvoie 403 sauf si l'appelant présente l'en-tête `X-Provisa-KMS-Key` — la marque d'un client JDBC ou Python configuré pour déchiffrer localement. Un navigateur ou un consommateur REST en clair ne porte pas une telle clé et est refusé. Le contrôle est un refus par défaut sur toute l'arborescence : une route ajoutée demain est protégée le jour de sa publication, et une exemption doit être justifiée.
- **Les endpoints de métadonnées de schéma restent ouverts.** `/data/sdl`, `/data/introspection`, `/data/schema-version`, `/data/domains`, `/data/proto` et `/data/compile` ne renvoient aucune donnée de ligne, et un client doit lire le schéma — y compris les champs `@encrypted` — avant de pouvoir se connecter.
- **gRPC et Arrow Flight continuent de servir, sous la même preuve.** Ce sont les transports qu'utilisent réellement les clients qui chiffrent ; les fermer laisserait un déploiement haute sécurité sans protocole filaire. Un appel de données sur l'un ou l'autre doit porter la même clé KMS en métadonnées d'appel.
- **pgwire, Bolt et MCP ne démarrent pas.** Aucun des trois n'a de poignée de main par connexion capable de porter un contexte de déchiffrement : un ensemble de lignes pgwire et un résultat Cypher circulent en clair, et un appel d'outil MCP remet ses résultats à un modèle sous forme de texte. Un port configuré pour l'un d'eux est refusé au démarrage plutôt que servi.
- **La garde des relations ne peut pas être contournée.** `ignore_relationships` et `relationship_guard: false` sont tous deux ignorés ; voir [Gouvernance des relations](#relationship-governance-v002).

**Vérifier qu'un déploiement est dans le mode :** le journal de démarrage le nomme, une requête `/data/sql` sans clé KMS répond 403 avec un message qui nomme REQ-693, et les ports pgwire, Bolt et MCP n'écoutent pas.

## Point d'ancrage d'approbation ABAC {: #abac-approval-hook }

Un point d'ancrage de politique externe facultatif qui se déclenche avant l'exécution de la requête. (REQ-203) Lorsqu'il est configuré, Provisa appelle votre moteur de politiques avec l'identité de l'utilisateur, les rôles, les tables, les colonnes et l'opération. La réponse détermine si la requête se poursuit. (REQ-203) Une commande enregistrée avec **Nécessite une approbation** est soumise au même point d'ancrage avant chaque appel ; voir [Appels de commandes](#command-calls). (REQ-1924)

### Portée {: #scoping }

Le point d'ancrage ne se déclenche que lorsque la requête touche une table ou une source incluse — aucun surcoût pour tout le reste. (REQ-204)

| Configuration | Effet |
| -------- | -------- |
| `auth.approval_hook.scope: all` | Chaque requête déclenche le point d'ancrage |
| `sources[].approval_hook: true` | Toutes les tables de cette source déclenchent le point d'ancrage |
| `tables[].approval_hook: true` | Cette table déclenche le point d'ancrage |

### Protocoles {: #protocols }

Trois transports sont pris en charge : (REQ-246)

| Type | Cas d'usage | Champ de configuration |
| ------ | ---------- | ------------- |
| `webhook` | Tout service de politique compatible HTTP (OPA, personnalisé) | `url` |
| `unix_socket` | OPA ou sidecar de politique sur la même machine | `socket_path` + `url` |
| `grpc` | Service de politique co-localisé à haut débit | `url` (host:port) |

Le transport gRPC utilise le contrat `provisa.auth.ApprovalService` défini dans `provisa/auth/approval.proto`. Implémentez ce service dans votre moteur de politiques : (REQ-246)

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
  map<string, string> session_vars = 6;
  string command = 7;         // a command call's name; empty for a query
  string arguments_json = 8;  // a command call's arguments as a JSON object
}

message ApprovalResponse {
  bool approved = 1;
  string reason = 2;
}
```
Le canal gRPC est persistant — un canal par instance de Provisa, réutilisé pour tous les appels vers cet endpoint du point d'ancrage. (REQ-555)

### Requête / réponse {: #request-response }

Les trois transports portent la même charge utile : (REQ-246)

| Champ | Type | Description |
| ------- | ------ | ------------- |
| `user` | string | Identité de l'utilisateur authentifié |
| `roles` | string[] | Rôles Provisa de l'utilisateur |
| `tables` | string[] | Identifiants des tables référencées dans la requête |
| `columns` | string[] | Colonnes sélectionnées dans la requête |
| `operation` | string | `"query"` ou `"mutation"` ; `"command"` pour un appel de commande |
| `command` | string | Le nom de la commande pour un appel de commande ; vide pour une requête |
| `arguments` | object | Les arguments avec lesquels une commande est appelée ; vide pour une requête |

Les transports webhook et socket Unix échangent du JSON, avec `command` et `arguments` comme clés. Sur gRPC, les arguments voyagent comme le texte JSON `arguments_json`. La réponse doit inclure `approved` (bool) et éventuellement `reason` (string). (REQ-246)

### Appels de commandes {: #command-calls }

Une commande dont `requires_approval` est activé (l'interrupteur **Nécessite une approbation** du formulaire de commande) passe par le point d'ancrage avant chaque appel, sur toutes les surfaces. Elle ne s'exécute que si le point d'ancrage l'approuve. La requête porte `operation: "command"`, le nom de la commande et ses arguments ; `tables` et `columns` sont vides. (REQ-1924) [tool-verified: `provisa/api/data/action_exec.py` `_require_approval` (`operation="command"`, `tables=[]`, `columns=[]`, `command=fn["name"]`, `arguments=args`)]

```json
{
  "user": "analyst",
  "roles": ["analyst"],
  "tables": [],
  "columns": [],
  "operation": "command",
  "command": "create_issue",
  "arguments": {"input": {"title": "Crash on save"}}
}
```
[inferred: the payload also carries `session_vars`; values shown are placeholders]

Deux refus, tous deux 403 :

- `functions.approval_unavailable` : aucun point d'ancrage n'est configuré. Contrairement à une requête, une commande qui a besoin d'une approbation ne passe jamais sans lui.
- `functions.approval_denied` : le point d'ancrage a répondu `approved: false`. Le motif figure dans le message.

### Délai et repli {: #timeout-and-fallback }

```yaml
auth:
  approval_hook:
    type: grpc          # webhook | grpc | unix_socket
    url: "localhost:50051"
    timeout_ms: 500     # default 5000
    fallback: deny      # allow | deny — applied on timeout or error
    scope: ""           # "" = use per-table/per-source flags; "all" = every query
```
En cas de délai dépassé ou d'erreur de transport, la politique `fallback` s'applique. (REQ-247) Un disjoncteur (par défaut : ouvert après 5 échecs consécutifs, semi-ouvert après 30 s) empêche les défaillances en cascade dues à un endpoint de point d'ancrage lent. (REQ-556)

### Exemple de configuration {: #configuration-example }

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

Les identifiants utilisent la syntaxe `${env:VAR_NAME}`, résolue à l'exécution. (REQ-557) Les mots de passe ne sont jamais stockés dans la base de données de configuration. (REQ-557)

Pour le service de secrets complet — coffres, syntaxe des références et fournisseurs — voir [Secrets](secrets.md).
