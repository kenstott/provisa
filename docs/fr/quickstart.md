# Guide de démarrage rapide pour les développeurs {: #developer-quick-start }

Pour évaluer Provisa sans compiler à partir du code source, consultez le [Guide de démarrage rapide](index.md) — téléchargez le programme d'installation pour macOS, Windows ou Linux et exécutez `provisa start`.

Ce guide est destiné à l'exécution de Provisa **depuis le dépôt** — développement actif, débogage ou contribution.

---

## Prérequis {: #prerequisites }

- **Docker Desktop** (en cours d'exécution)
- **Python 3.12+**
- **Node.js 20+**
- **Git**

---

## 1. Cloner et configurer {: #1-clone-and-set-up }

```bash
git clone https://github.com/kenstott/provisa.git
cd provisa
./setup.sh
```

`setup.sh` crée `.venv/`, installe toutes les dépendances Python via `pip install -e ".[dev]"`, et configure les hooks Git dans `.githooks/`.

---

## 2. Tout démarrer {: #2-start-everything }

```bash
./start-ui.sh
```

Une fois le démarrage terminé, vous verrez :

```yaml
Provisa running:
  Backend: http://localhost:8001  (logs: .logs/server.log)
  UI:      http://localhost:3000
```

**Ce qui est démarré :**

- Services principaux de Docker Compose (`docker-compose.core.yml`) — PostgreSQL, PgBouncer, Trino, Redis
- Surcouche de développement Docker Compose (`docker-compose.dev.yml`) — MinIO, Kafka, MongoDB, Elasticsearch, Neo4j, Fuseki, Debezium, Schema Registry
- API backend sur le port 8001 (rechargement à chaud lors des modifications de `provisa/` et `config/`)
- Serveur de développement Vite de l'UI sur le port 3000 (HMR)
- Traçage OpenTelemetry et Grafana sur `http://localhost:3100`. La pile d'observabilité est un profil docker-compose `observability` facultatif (OTel Collector, Prometheus, Tempo, Grafana), non activé par défaut au niveau de la plateforme ; `start-ui.sh` l'active par commodité de script de développement, sauf si vous passez `--no-observability`.

**Ctrl+C** arrête tout — backend, UI et tous les services Docker — et annule tout correctif de configuration.

**Ctrl+R** redémarre uniquement le backend (utile après une modification de configuration que le rechargement à chaud ne détecte pas).

### Options

`--no-observability` — Désactive le traçage distribué. Par défaut, `start-ui.sh` télécharge l'agent Java OpenTelemetry s'il n'est pas déjà présent, applique un correctif au `jvm.config` de Trino pour le charger, et démarre l'OTel collector, Prometheus, Tempo et Grafana. Passez `--no-observability` pour ignorer tout cela. Le correctif de `jvm.config` est annulé lors de Ctrl+C.

`--seed-data` — Alimente Kafka avec des données de démonstration une fois les services Docker en bon état. Non exécuté par défaut.

`--keep-docker` — Laisse les services Docker Compose en cours d'exécution après Ctrl+C au lieu d'appeler `docker compose down`.

`--reset-volumes` — Efface tous les volumes Docker et redémarre avec un état propre. Utile pour la récupération après une panne de Docker.

`--demo` — Démarre des sources de données de démonstration supplémentaires (schéma PostgreSQL pet-store, mock OpenAPI petstore, SQLite et un GraphQL distant). Alimente automatiquement les utilisateurs et commandes petstore.

`--source=<name>` (`start-ui-install.sh` uniquement, répétable) — Provisionne une source de données optionnelle en plus de `--demo`. Chaque nom correspond à `demo/sources/<name>/`. Le démarrage appelle `demo/sources/provision.py up`, qui démarre le `compose.yml` de la source comme son propre projet Docker Compose (`provisa-demo-<name>`), attend son bilan de santé, et exécute `prime.py` lorsque la source en possède un pour alimenter les données. Le démarrage écrit ensuite une configuration enveloppe dans `${PROVISA_HOME:-~/.provisa}/demo/provisa-with-sources.yaml` qui inclut la configuration de base plus le `fragment.yaml` de chaque source, puis démarre à partir de celle-ci.

Le même `provision.py` est appelé par la suite de tests de bout en bout de l'interface pour mettre en place ces sources (sous le préfixe de projet `provisa-e2e-<name>` sur ses propres ports), de sorte que les données de démonstration montrées et les lignes vérifiées par la suite sont définies une seule fois.

Lors d'un démarrage Docker (sans `--demo`/`--native`), le coordinateur est un conteneur, donc chaque source est jointe au réseau de la pile principale et enregistrée sous `<name>:<container port>` ; lors d'un démarrage natif, elle est enregistrée sous `localhost:<published port>`. Une source dont le fichier `demo/sources/<name>/engine` nomme un moteur que le démarrage n'exécute pas est refusée.

Sources fournies :

| Nom | Port(s) | Notes |
|------|---------|-------|
| `neo4j` | HTTP 27474, Bolt 27687 | Deux tables Cypher (`adopter`, `adopter_referral`) ; graphe alimenté par `seed.cypher` ; tables enregistrées depuis le fragment |
| `mongodb` | 27117 | Source enregistrée ; collection `product_reviews` alimentée par `db/mongo-init.js` ; enregistrer les tables manuellement via Register Table |
| `redis` | 26379 | Source enregistrée ; hachages `support_agent:*` et `agent_status:*` alimentés par `prime.py` ; chaque préfixe s'enregistre comme table via Register Table |
| `cassandra` | 29042 | Source enregistrée ; `shelter_ops.intake_events` alimenté par `prime.py` (nécessite l'extra `cassandra`) ; le keyspace s'enregistre comme schéma via Register Table |
| `sparql` | 23030 | Apache Jena Fuseki ; source et une table adossée à une requête (`volunteer`) enregistrées depuis le fragment, graphe alimenté par `prime.py` ; d'autres tables via Register Table (requête + Aperçu) |
| `prometheus` | 29090 | Source enregistrée ; le serveur s'auto-scrute, de sorte que `up` et les métriques `prometheus_*` s'enregistrent comme tables via Register Table |
| `elasticsearch` | 29200 | Source et mapping d'index enregistrés ; index `support_tickets` alimenté par `prime.py` ; lu en HTTP par le moteur natif, via le connecteur sur Trino |
| `splunk` | mgmt 8089, HEC 8088 | Source enregistrée avec authentification par jeton et `disable_ssl_validation` (le certificat du conteneur est autosigné) ; un index, sept événements d'alerte shelter et le Data Model `shelter_alerts` alimentés par `prime.py`, qui génère aussi le jeton API lu par le fragment comme `PROVISA_DEMO_SPLUNK_TOKEN`. Les Data Models s'enregistrent comme tables via Register Table : sur Trino via le catalogue `splunk`, et sur tous les autres moteurs via le point de terminaison pgwire du pilote Splunk, que le moteur rattache |
| `chinook` | 25433 | Postgres contenant le sous-ensemble Chinook en snake_case que suit l'échantillon de métadonnées Hasura, alimenté par `prime.py` depuis `tests/fixtures/hasura_v2_t1_seed.sql` ; source enregistrée depuis le fragment, et la source sur laquelle atterrit un import Hasura v2 de `tests/fixtures/hasura_v2_t1_metadata.json` |

`--idp=basic|firebase` — Active un fournisseur d'identité pour l'authentification. Sans cet indicateur, le backend s'exécute sans fournisseur d'authentification et toutes les requêtes sont traitées comme `admin`.

---

## 3. Connecter une source de données {: #3-connect-a-data-source }

Provisa lit la configuration depuis `config/`. Ajoutez un fichier source — par exemple `config/sources/my-db.yaml` :

```yaml
sources:
  - id: my-pg
    type: postgresql
    host: localhost
    port: 5432
    database: mydb
    username: myuser
    password: ${MY_DB_PASSWORD}
    tables:
      - id: orders
        publish: true
        columns:
          - name: id
          - name: amount
          - name: region
          - name: customer_id
```

Définissez la variable d'environnement et le backend la détectera au prochain rechargement :

```bash
export MY_DB_PASSWORD=secret
```

Consultez [docs/configuration.md](configuration.md) pour la référence YAML complète et tous les types de sources pris en charge.

---

## 4. Exécuter votre première requête {: #4-run-your-first-query }

```bash
# GraphQL
curl -s -X POST http://localhost:8001/data/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "{ orders { id amount region } }"}' | jq

# SQL — use the /data/sql endpoint
curl -s -X POST http://localhost:8001/data/sql \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT id, amount, region FROM orders LIMIT 5"}' | jq
```

Aucune authentification n'est requise lorsqu'aucune section `auth` n'est présente dans `config/provisa.yaml` (valeur par défaut en développement). Le rôle par défaut est `admin`.

---

## 5. Ouvrir l'UI {: #5-open-the-ui }

Ouvrez `http://localhost:3000` dans un navigateur.

La barre de navigation comporte quatre menus de premier niveau :

- **Explore** — Explorateur de schéma (`/schema`), éditeur GraphQL (`/query`), éditeur Cypher (`/graph`), éditeur SQL (`/sql`)
- **Model** — Vues et commandes
- **Security** — Sécurité au niveau des lignes et politiques de masquage de colonnes
- **Admin** — Vue d'ensemble, domaines, cache, tâches planifiées, état du système, observabilité, utilisateurs, organisations, rôles

L'API GraphQL d'administration se trouve à l'adresse `http://localhost:8001/admin/graphql`.

---

## Dépannage {: #troubleshooting }

**Le backend ne démarre pas** — vérifiez `.logs/server.log`. La cause la plus courante est une variable d'environnement manquante ou un conflit de port sur le 8001.

**Les services Docker ne sont pas en bon état** — exécutez `docker compose -f docker-compose.core.yml -f docker-compose.dev.yml ps` pour voir quel service est bloqué. Le moteur de fédération prend environ 30 secondes au premier démarrage.

**Conflit de port sur le 3000 ou le 8001** — `start-ui.sh` arrête les processus obsolètes sur ces ports avant de démarrer. Si autre chose occupe le port, arrêtez-le manuellement au préalable.

**Redémarrage propre** — arrêtez le script, puis exécutez `./start-ui.sh --reset-volumes` pour effacer tous les volumes et redémarrer.

---

## Étapes suivantes {: #next-steps }

| Objectif | Document |
| ------ | ----- |
| Référence complète de configuration YAML | [configuration.md](configuration.md) |
| Sécurité au niveau des lignes, masquage de colonnes, authentification | [security.md](security.md) |
| Tous les types de sources pris en charge | [sources.md](sources.md) |
| Abonnements en temps réel | [subscriptions.md](subscriptions.md) |
| JDBC, outils de BI, Arrow Flight, Apollo Federation | [integrations.md](integrations.md) |
| Client Python | [python-client.md](python-client.md) |
| Déploiement en production | [deployment.md](deployment.md) |
