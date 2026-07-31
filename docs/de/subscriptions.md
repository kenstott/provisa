# SSE-Subscriptions

Provisa unterstützt Echtzeit-Push über Server-Sent Events (SSE). Clients erhalten einen Strom von Änderungsereignissen ohne Polling. (REQ-258)

## Quellen

Subscriptions zielen auf eine **registrierte Tabelle**:

| Quelle | Verfügbare `strategy`-Werte |
|--------|-------------------------|
| Tabelle (PostgreSQL) | `native` (LISTEN/NOTIFY), `poll` |
| Tabelle (Nicht-PG-RDBMS mit einem `cdc`-Block auf Quellenebene) | `debezium`, `kafka`, `poll` |
| Tabelle (föderierte Sicht / jede andere Quelle) | nur `poll` |

### Automatische Trigger-Installation für PostgreSQL

Provisa installiert beim Start automatisch `AFTER INSERT OR UPDATE OR DELETE`-Trigger auf allen **vorab genehmigten** PostgreSQL-Tabellen. (REQ-565) Diese Trigger rufen `pg_notify('provisa_{table}', ...)` auf, sodass rohes DML (nicht nur Provisa-Mutationen) von Subscriptions erfasst wird. (REQ-565)

Schlägt die Trigger-Installation fehl (z. B. wegen unzureichender Berechtigung — die Datenbankrolle muss Eigentümer der Tabelle sein), fällt Provisa für diese Tabelle auf Watermark-Polling zurück, sofern eine `watermark_column` konfiguriert ist. (REQ-566) Es wird eine Warnung protokolliert. (REQ-566)

### Subscriptions auf datenquellenübergreifende Sichten

Für Sichten, die mehrere Datenquellen über die Föderations-Engine verknüpfen, fügen Sie der Tabellenregistrierung eine `watermark_column` hinzu. (REQ-260, REQ-283) Die Spalte muss im SQL der Sicht vorhanden sein (sie muss nicht im GraphQL-Schema erscheinen):

```sql
-- Example: federated view with derived watermark
CREATE OR REPLACE VIEW orders_with_segments AS
SELECT o.*, s.name AS segment_name,
       GREATEST(o.updated_at, s.updated_at) AS _watermark
FROM postgresql.public.orders o
JOIN mysql.crm.customer_segments s ON o.customer_id = s.customer_id;
```

Registrieren Sie mit `watermark_column: _watermark`. Provisa führt Polling mit `WHERE _watermark > <last_seen>` durch. (REQ-260)

### Subscriptions auf verschachtelte Beziehungen

Wenn das Subscription-Feld Felder aus verknüpften Tabellen auswählt (über registrierte Beziehungen), überwacht Provisa **alle** beteiligten physischen Tabellen gleichzeitig. (REQ-567) Eine Änderung an jeder verknüpften Tabelle löst die Subscription-Abfrage erneut aus. (REQ-567)

## Endpunkt

Eine Tabelle abonnieren:
```
GET /data/subscribe/{table}
Accept: text/event-stream
```

Die Verbindung bleibt offen und sendet pro Änderung ein JSON-Ereignis: (REQ-258, REQ-568)
```
data: {"event":"insert","table":"orders","row":{"id":43,"amount":55.00,"region":"east"}}

data: {"event":"update","table":"orders","row":{"id":42,"amount":199.00,"region":"west"}}
```

## Übertragungsmodi

Die Übertragung wird über `live.strategy` in der Tabellenkonfiguration festgelegt: (REQ-813, REQ-814)

| `strategy` | Mechanismus | Verfügbar für | Erfordert |
|------------|-----------|---------------|---------|
| `native` | PostgreSQL `LISTEN`/`NOTIFY`, MongoDB Change Streams | PG, MongoDB | Nichts Zusätzliches |
| `debezium` | Kafka-Topic vom Debezium-Connector | Nicht-PG-RDBMS-Tabellen | `cdc`-Block auf Quellenebene (Debezium + Kafka) |
| `kafka` | Beliebiges Kafka-Delta-Topic | Jede Kafka-gespeiste Tabelle | `cdc`-Block auf Quellenebene |
| `poll` | Watermark-basiertes Polling | Jede Tabelle mit Watermark | `watermark_column` |

### LISTEN/NOTIFY

Provisa führt `LISTEN <channel>` auf einer dauerhaften PG-Verbindung aus. (REQ-258) Provisa-Mutationen lösen automatisch `NOTIFY` aus. (REQ-565) Externe Schreibprozesse müssen nach Schreibvorgängen `NOTIFY <channel>, '<payload>'` aufrufen. Keine zusätzliche Infrastruktur erforderlich.

### Polling

Provisa führt die Quellabfrage periodisch erneut aus und wählt nur Zeilen aus, bei denen `watermark_column > last_watermark` gilt. (REQ-260) Unterschiede werden als SSE-Ereignisse gesendet. Polling kann harte Löschungen (hard deletes) nicht erkennen — eine entfernte Zeile hinterlässt keine fortschreitende Watermark. Damit eine Löschung sichtbar wird, verwenden Sie eine logische Löschung (soft delete) (z. B. das Setzen eines `deleted_at`-Flags), das die Watermark-Spalte erhöht; die Löschung kommt dann als Update-Ereignis mit dem Soft-Delete-Marker an. (REQ-260)

Tabellen-Polling-Konfiguration (in `provisa.yaml`):
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

### Debezium-CDC

Erfordert einen laufenden Debezium-Connector, der nach Kafka schreibt. (REQ-261) Provisa konsumiert das Kafka-Topic und leitet Änderungsereignisse an verbundene SSE-Clients weiter. (REQ-261)

Der CDC-Transport wird einmal pro Quelle in einem `cdc`-Block konfiguriert; Topics werden als `{topic_prefix}.{schema}.{table}` abgeleitet und nie pro Tabelle wiederholt. (REQ-824) Jede Tabelle wählt dann `strategy: debezium`:
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

## Kafka-Sink-Umleitung

Jede GraphQL-Subscription kann statt zum Client gestreamt zu werden an ein Kafka-Topic umgeleitet werden. (REQ-812) Fügen Sie der Subscription-Anfrage den Header `X-Provisa-Sink` hinzu:

```
POST /data/graphql
Authorization: Bearer <token>
Content-Type: application/json
X-Provisa-Sink: kafka://broker:9092/my-topic
```

Der Server antwortet sofort mit `202 Accepted` und startet eine Hintergrundaufgabe, die: (REQ-812)
1. Tabellenänderungen mit derselben Provider-Auflösung wie SSE überwacht (LISTEN/NOTIFY → asyncpg-Polling → föderiertes Polling)
2. bei jeder Änderung die entsprechende Abfrage erneut ausführt
3. das Ergebnis als JSON-Nachricht im angegebenen Kafka-Topic veröffentlicht

Der Sink läuft für die gesamte Lebensdauer des Serverprozesses. (REQ-812) Starten Sie den Server neu, um ihn zu stoppen (eine persistente Sink-Registrierung über die Admin-API ist geplant).

**URI-Format:** `kafka://[broker:port]/topic`

- Wird `broker:port` weggelassen, wird die Umgebungsvariable `KAFKA_BOOTSTRAP_SERVERS` verwendet (Standard: `localhost:9092`) (REQ-812)
- `topic` ist erforderlich

**Beispiel (curl):**
```bash
curl -X POST http://localhost:8000/data/graphql \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "X-Provisa-Sink: kafka://kafka:9092/orders-live" \
  -d '{"query": "subscription { orders { id status amount } }"}'
# → 202 {"status":"streaming","sink":"kafka://kafka:9092/orders-live","table":"orders"}
```

### Kafka-Sink als zweiter Output auf Konfigurationsebene

Eine Polling-basierte Tabellen-Subscription kann über `provisa.yaml` gleichzeitig in ein Kafka-Topic veröffentlichen. (REQ-282, REQ-286) SSE-Subscription und Kafka-Sink sind beide Outputs derselben Live Query Engine. (REQ-282) Jeder Output verfolgt seine Watermark unabhängig. (REQ-286)

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

Vollständige Konfigurationsreferenz für Sinks siehe [Kafka Sinks](sources.md).

## Sicherheit

Alle Subscription-Modi setzen dieselbe Sicherheits-Pipeline wie reguläre Abfragen durch: (REQ-258, REQ-038)

- RLS-Filter (Sicherheit auf Zeilenebene) werden auf jede ausgegebene Zeile angewendet (REQ-040)
- Maskierte Spalten erscheinen in Ereignissen maskiert (REQ-040)
- Die Rollenautorisierung wird zum Zeitpunkt der Verbindung geprüft (REQ-258)

## Client-Beispiel

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
