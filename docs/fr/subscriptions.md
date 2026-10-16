# Subscriptions SSE {: #sse-subscriptions }

Provisa prend en charge la diffusion en temps réel (push) via Server-Sent Events (SSE). Les clients reçoivent un flux d'événements de changement sans polling. (REQ-258)

## Sources

Les subscriptions ciblent une **table enregistrée** :

| Source | Valeurs de `strategy` disponibles |
| -------- | ------------------------- |
| Table (PostgreSQL) | `native` (LISTEN/NOTIFY), `poll` |
| Table (RDBMS non-PG avec un bloc `cdc` au niveau source) | `debezium`, `kafka`, `poll` |
| Table (vue fédérée / toute autre source) | `poll` uniquement |

### Installation automatique des triggers PostgreSQL {: #postgresql-trigger-auto-installation }

Provisa installe automatiquement des triggers `AFTER INSERT OR UPDATE OR DELETE` sur toutes les tables PostgreSQL **préapprouvées** au démarrage. (REQ-565) Ces triggers appellent `pg_notify('provisa_{table}', ...)` afin que le DML brut (pas seulement les mutations Provisa) soit capté par les subscriptions. (REQ-565)

Si l'installation du trigger échoue (par exemple en raison de privilèges insuffisants — le rôle de base de données doit être propriétaire de la table), Provisa recourt au polling par watermark pour cette table, à condition qu'un `watermark_column` soit configuré. (REQ-566) Un avertissement est journalisé. (REQ-566)

### Subscriptions sur des vues inter-sources de données {: #cross-datasource-view-subscriptions }

Pour les vues qui combinent (join) plusieurs sources de données via le moteur de fédération, ajoutez un `watermark_column` à l'enregistrement de la table. (REQ-260, REQ-283) La colonne doit exister dans le SQL de la vue (elle n'a pas besoin d'apparaître dans le schéma GraphQL) :

```sql
-- Example: federated view with derived watermark
CREATE OR REPLACE VIEW orders_with_segments AS
SELECT o.*, s.name AS segment_name,
       GREATEST(o.updated_at, s.updated_at) AS _watermark
FROM postgresql.public.orders o
JOIN mysql.crm.customer_segments s ON o.customer_id = s.customer_id;
```

Enregistrez avec `watermark_column: _watermark`. Provisa effectue le polling avec `WHERE _watermark > <last_seen>`. (REQ-260)

### Subscriptions sur des relations imbriquées {: #nested-relationship-subscriptions }

Lorsque le champ de subscription sélectionne des champs de tables jointes (via des relations enregistrées), Provisa surveille **toutes** les tables physiques concernées simultanément. (REQ-567) Un changement sur une table jointe redéclenche la requête de subscription. (REQ-567)

## Endpoint

S'abonner à une table :

```http
GET /data/subscribe/{table}
Accept: text/event-stream
```

La connexion reste ouverte et émet un événement JSON par changement : (REQ-258, REQ-568)

```text
data: {"event":"insert","table":"orders","row":{"id":43,"amount":55.00,"region":"east"}}

data: {"event":"update","table":"orders","row":{"id":42,"amount":199.00,"region":"west"}}
```

## Modes de livraison {: #delivery-modes }

La livraison est sélectionnée via `live.strategy` dans la configuration de la table : (REQ-813, REQ-814)

| `strategy` | Mécanisme | Disponible pour | Requiert |
| ------------ | ----------- | --------------- | --------- |
| `native` | `LISTEN`/`NOTIFY` PostgreSQL, Change Streams MongoDB | PG, MongoDB | Rien de plus |
| `debezium` | Topic Kafka issu du connecteur Debezium | Tables RDBMS non-PG | Bloc `cdc` au niveau source (Debezium + Kafka) |
| `kafka` | Topic delta Kafka arbitraire | Toute table alimentée par Kafka | Bloc `cdc` au niveau source |
| `poll` | Polling basé sur watermark | Toute table disposant d'un watermark | `watermark_column` |

### LISTEN/NOTIFY

Provisa émet `LISTEN <channel>` sur une connexion PG persistante. (REQ-258) Les mutations Provisa déclenchent automatiquement `NOTIFY`. (REQ-565) Les processus d'écriture externes doivent appeler `NOTIFY <channel>, '<payload>'` après chaque écriture. Aucune infrastructure supplémentaire requise.

### Polling

Provisa réexécute périodiquement la requête source, en ne sélectionnant que les lignes où `watermark_column > last_watermark`. (REQ-260) Les différences sont émises sous forme d'événements SSE. Le polling ne peut pas détecter les suppressions définitives (hard deletes) — une ligne supprimée ne laisse aucun watermark qui progresse. Pour rendre une suppression visible, utilisez une suppression logique (soft delete) (par exemple en activant un indicateur `deleted_at`) qui fait avancer la colonne watermark ; la suppression arrive alors comme un événement de mise à jour portant le marqueur de suppression logique. (REQ-260)

Configuration du polling de table (dans `provisa.yaml`) :

```yaml
tables:
  - id: federated_orders
    source_id: federated-source
    live:
      strategy: poll
      watermark_column: updated_at
      poll_interval: 30
      outputs:
        - type: sse
```

### CDC Debezium {: #debezium-cdc }

Nécessite un connecteur Debezium en cours d'exécution écrivant dans Kafka. (REQ-261) Provisa consomme le topic Kafka et transmet les événements de changement aux clients SSE connectés. (REQ-261)

Le transport CDC est configuré une seule fois par source dans un bloc `cdc` ; les topics sont dérivés sous la forme `{topic_prefix}.{schema}.{table}` et ne sont jamais répétés par table. (REQ-824) Chaque table sélectionne ensuite `strategy: debezium` :

```yaml
sources:
  - id: sales-mysql
    cdc:
      bootstrap_servers: kafka:9092
      topic_prefix: debezium
      # schema_registry_url: http://schema-registry:8081   # set for Avro; omit for JSON
    tables:
      - id: orders
        live:
          strategy: debezium
```

#### Encodages des messages {: #message-encodings }

Un topic Debezium est lu dans l'un de deux encodages, et chaque message indique lequel. (REQ-1951)

| Encodage | Ce que porte le topic | Ce dont la source a besoin |
|---|---|---|
| JSON | L'enveloppe du convertisseur JSON de Debezium, avec ou sans l'enveloppe `schema`/`payload` | Rien au-delà du bloc `cdc` |
| Avro | Le format filaire du registre de schémas Confluent : un octet nul, un identifiant de schéma sur quatre octets, une donnée Avro | `schema_registry_url`, et la manière de s'y authentifier |

Les messages Avro sont décodés avec le schéma que nomme leur identifiant, récupéré une fois dans le registre puis conservé. Un topic dont le schéma a évolué est lu correctement, car chaque message nomme le schéma qui l'a écrit. Les topics Protobuf et JSON Schema ne sont pas lus.

Un message Avro n'est jamais lu comme du JSON. Chaque façon de ne pas pouvoir en lire un est refusée nommément :

| Situation | Refus |
|---|---|
| Message Avro, la source ne nomme aucun registre | `subscribe.avro_topic_without_registry`, avec le topic |
| Registre indisponible ou sans réponse à temps | `subscribe.schema_registry_unreachable` |
| Le registre rejette les identifiants de la source | `subscribe.schema_registry_refused_credentials` |
| Identifiant de schéma que le registre ne détient pas | `subscribe.schema_id_unknown`, avec l'identifiant |
| L'identifiant nomme un schéma Protobuf ou JSON | `subscribe.schema_not_avro` |

Un registre indisponible ou qui rejette les identifiants est refusé au démarrage de l'abonnement, avant l'ouverture du flux. Une consultation du registre attend au plus 5 secondes pour se connecter et 10 secondes pour une réponse, et moins quand il reste moins de temps à la requête.

#### Authentification du registre de schémas {: #schema-registry-authentication }

`schema_registry_auth` nomme une méthode ; les champs dont cette méthode a besoin sont vérifiés à l'enregistrement de la source.

| `schema_registry_auth` | Champs obligatoires | Envoyé au registre |
|---|---|---|
| `none` (par défaut) | — | Aucun identifiant |
| `basic` | `schema_registry_username`, `schema_registry_password` | Authentification HTTP basique |
| `bearer` | `schema_registry_token` | `Authorization: Bearer <token>` (un jeton statique) |
| `mtls` | `schema_registry_client_cert`, `schema_registry_client_key` | Le certificat client, dans la poignée de main TLS |

`schema_registry_ca` est facultatif avec chaque méthode : un paquet de CA auquel faire confiance lorsque le certificat du registre n'est pas signé par une autorité publique.

Le mot de passe et le jeton suivent le contrat des secrets. Une valeur saisie dans le formulaire de la source est stockée dans le coffre de l'organisation et la source garde une référence `${secret:NAME}` ; une référence écrite dans un fichier de configuration (`${env:...}`, `${secret:...}`) est conservée telle qu'écrite. Le certificat, la clé et le paquet de CA sont des fichiers qu'ouvre le processus serveur de Provisa, nommés par chemin absolu.

```yaml
sources:
  - id: sales-mysql
    cdc:
      bootstrap_servers: kafka:9092
      topic_prefix: debezium
      schema_registry_url: https://schema-registry:8081
      schema_registry_auth: basic
      schema_registry_username: provisa
      schema_registry_password: ${secret:registry_password}
      schema_registry_ca: /etc/provisa/registry-ca.pem
```
## Quand un flux se termine sur un échec {: #when-a-stream-ends-on-a-failure }

Un flux qui s'est ouvert a déjà répondu `200`, donc un échec après ce point ne peut pas être un statut HTTP. L'abonné reçoit une dernière trame qui dit pourquoi, puis le flux se ferme. Un flux ne se ferme jamais sur un échec sans cette trame.

La trame porte ce par quoi le même échec serait répondu sur une requête ordinaire :

| Échec | Corps |
|---|---|
| Un refus avec un code (celui d'un registre de schémas, d'une région, un refus de gouvernance) | `detail`, `code`, `params` |
| Le délai de la requête | `detail: "Request timed out"` |
| Toute chose inattendue | `detail: "Internal server error"`, `type` (le nom du type de l'exception) |

Chaque corps porte aussi `subscription_id`. Pour un échec inattendu, le message et la trace se trouvent dans le journal du serveur sous cet identifiant, pas dans la trame.

L'abonnement de table (`/data/subscribe/{table}`) envoie le corps comme les données d'un événement nommé `stream_error` :

```text
event: stream_error
data: {"detail": "Topic 'debezium.app.orders' carries Avro messages, and its source names no schema registry to read them with: set the source's schema registry URL", "code": "subscribe.avro_topic_without_registry", "params": {"topic": "debezium.app.orders"}, "subscription_id": "5f0c2a9d41b7e386"}
```
L'événement ne s'appelle pas `error`, car un `EventSource` déclenche son propre événement `error` lorsqu'une connexion tombe et un écouteur ne pourrait pas distinguer les deux. Écoutez-le par son nom :

```javascript
source.addEventListener("stream_error", (e) => {
  const { detail, code, params } = JSON.parse(e.data);
  source.close();
});
```
Un abonnement GraphQL garde la forme de réponse GraphQL, avec le code et les params sous `extensions` :

```json
{"errors": [{"message": "…", "extensions": {"code": "subscribe.avro_topic_without_registry", "params": {"topic": "debezium.app.orders"}, "subscription_id": "5f0c2a9d41b7e386"}}]}
```
Un cas ne termine pas un abonnement GraphQL : quand la relecture après un seul changement échoue, cette trame est envoyée sous la même forme et le flux passe au changement suivant. Un échec de la lecture initiale, du flux de changements lui-même ou de son démarrage est la dernière trame.

## Redirection vers un sink Kafka {: #kafka-sink-redirect }

Toute subscription GraphQL peut être redirigée vers un topic Kafka au lieu d'être diffusée en flux vers le client. (REQ-812) Ajoutez l'en-tête `X-Provisa-Sink` à la requête de subscription :

```yaml
POST /data/graphql
Authorization: Bearer <token>
Content-Type: application/json
X-Provisa-Sink: kafka://broker:9092/my-topic
```

Le serveur répond immédiatement `202 Accepted` et démarre une tâche en arrière-plan qui : (REQ-812)

1. Surveille les changements de table via la même résolution de provider que SSE (LISTEN/NOTIFY → polling asyncpg → polling fédéré)
2. Réexécute la requête équivalente à chaque changement
3. Publie le résultat sous forme de message JSON dans le topic Kafka indiqué

Le sink s'exécute pendant toute la durée de vie du processus serveur. (REQ-812) Redémarrez le serveur pour l'arrêter (l'enregistrement persistant des sinks via l'API d'administration est prévu).

**Format d'URI :** `kafka://[broker:port]/topic`

- Si `broker:port` est omis, la variable d'environnement `KAFKA_BOOTSTRAP_SERVERS` est utilisée (valeur par défaut : `localhost:9092`) (REQ-812)
- `topic` est obligatoire

**Exemple (curl) :**

```bash
curl -X POST http://localhost:8000/data/graphql \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "X-Provisa-Sink: kafka://kafka:9092/orders-live" \
  -d '{"query": "subscription { orders { id status amount } }"}'
# → 202 {"status":"streaming","sink":"kafka://kafka:9092/orders-live","table":"orders"}
```

### Sink Kafka comme seconde sortie au niveau de la configuration {: #kafka-sink-as-a-config-level-second-output }

Une subscription de table basée sur le polling peut publier simultanément dans un topic Kafka via `provisa.yaml`. (REQ-282, REQ-286) La subscription SSE et le sink Kafka sont tous deux des sorties du même Live Query Engine. (REQ-282) Chaque sortie suit son watermark de manière indépendante. (REQ-286)

```yaml
tables:
  - id: active-orders
    live:
      strategy: poll
      watermark_column: updated_at
      poll_interval: 30
      outputs:
        - type: sse
        - type: kafka
          topic: provisa.active-orders
          bootstrap_servers: kafka:9092
          key_column: id
          role: analyst           # required: the rows are published governed as this role
```

Une sortie Kafka publie les lignes de la table gouvernées comme le rôle qu'elle nomme : ses règles de lignes, la visibilité des colonnes et les masques s'appliquent comme à une lecture par ce rôle. Une sortie sans `role` est refusée au chargement de la configuration. (REQ-565) [tool-verified: `provisa/core/models.py` `LiveOutputConfig`]

## Sécurité {: #security }

Tous les modes de subscription appliquent le même pipeline de sécurité que les requêtes classiques : (REQ-258, REQ-038)

- Les filtres de sécurité au niveau des lignes sont appliqués à chaque ligne émise (REQ-040)
- Les colonnes masquées apparaissent masquées dans les événements (REQ-040)
- L'autorisation de rôle est vérifiée au moment de la connexion (REQ-258)

## Exemple client {: #client-example }

```javascript
// Table subscription (LISTEN/NOTIFY)
const source = new EventSource('/data/subscribe/orders', {
  headers: { 'Authorization': 'Bearer <token>' }
});

source.onmessage = (e) => {
  const event = JSON.parse(e.data);
  console.log(event.event, event.row);
};
```

</content>
