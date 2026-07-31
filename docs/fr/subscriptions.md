# Subscriptions SSE

Provisa prend en charge la diffusion en temps réel (push) via Server-Sent Events (SSE). Les clients reçoivent un flux d'événements de changement sans polling. (REQ-258)

## Sources

Les subscriptions ciblent une **table enregistrée** :

| Source | Valeurs de `strategy` disponibles |
| -------- | ------------------------- |
| Table (PostgreSQL) | `native` (LISTEN/NOTIFY), `poll` |
| Table (RDBMS non-PG avec un bloc `cdc` au niveau source) | `debezium`, `kafka`, `poll` |
| Table (vue fédérée / toute autre source) | `poll` uniquement |

### Installation automatique des triggers PostgreSQL

Provisa installe automatiquement des triggers `AFTER INSERT OR UPDATE OR DELETE` sur toutes les tables PostgreSQL **préapprouvées** au démarrage. (REQ-565) Ces triggers appellent `pg_notify('provisa_{table}', ...)` afin que le DML brut (pas seulement les mutations Provisa) soit capté par les subscriptions. (REQ-565)

Si l'installation du trigger échoue (par exemple en raison de privilèges insuffisants — le rôle de base de données doit être propriétaire de la table), Provisa recourt au polling par watermark pour cette table, à condition qu'un `watermark_column` soit configuré. (REQ-566) Un avertissement est journalisé. (REQ-566)

### Subscriptions sur des vues inter-sources de données

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

### Subscriptions sur des relations imbriquées

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

## Modes de livraison

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

### CDC Debezium

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

## Redirection vers un sink Kafka

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

### Sink Kafka comme seconde sortie au niveau de la configuration

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
```

Voir [Kafka Sinks](sources.md) pour la référence complète de configuration des sinks.

## Sécurité

Tous les modes de subscription appliquent le même pipeline de sécurité que les requêtes classiques : (REQ-258, REQ-038)

- Les filtres de sécurité au niveau des lignes sont appliqués à chaque ligne émise (REQ-040)
- Les colonnes masquées apparaissent masquées dans les événements (REQ-040)
- L'autorisation de rôle est vérifiée au moment de la connexion (REQ-258)

## Exemple client

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
