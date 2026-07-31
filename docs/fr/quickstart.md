# Guide de démarrage rapide pour les développeurs

Pour évaluer Provisa sans compiler à partir du code source, consultez le [Guide de démarrage rapide](index.md) — téléchargez le programme d'installation pour macOS, Windows ou Linux et exécutez `provisa start`. (REQ-223, REQ-224, REQ-227)

Ce guide est destiné à l'exécution de Provisa **depuis le dépôt** — développement actif, débogage ou contribution.

---

## Prérequis

- **Docker Desktop** (en cours d'exécution)
- **Python 3.12+**
- **Node.js 20+**
- **Git**

---

## 1. Cloner et configurer

```bash
git clone https://github.com/kenstott/provisa.git
cd provisa
./setup.sh
```

`setup.sh` crée `.venv/`, installe toutes les dépendances Python via `pip install -e ".[dev]"`, et configure les hooks Git dans `.githooks/`. [tool-verified: setup.sh lines 5–9]

---

## 2. Tout démarrer

```bash
./start-ui.sh
```

Une fois le démarrage terminé, vous verrez :

```yaml
Provisa running:
  Backend: http://localhost:8001  (logs: .logs/server.log)
  UI:      http://localhost:3000
```

**Ce qui est démarré :** [tool-verified: start-ui.sh]

- Services principaux de Docker Compose (`docker-compose.core.yml`) — PostgreSQL, PgBouncer, Trino, Redis (REQ-055)
- Surcouche de développement Docker Compose (`docker-compose.dev.yml`) — MinIO, Kafka, MongoDB, Elasticsearch, Neo4j, Fuseki, Debezium, Schema Registry (REQ-055)
- API backend sur le port 8001 (rechargement à chaud lors des modifications de `provisa/` et `config/`) (REQ-618)
- Serveur de développement Vite de l'UI sur le port 3000 (HMR)
- Traçage OpenTelemetry et Grafana sur `http://localhost:3100`. La pile d'observabilité est un profil docker-compose `observability` facultatif (OTel Collector, Prometheus, Tempo, Grafana), non activé par défaut au niveau de la plateforme ; `start-ui.sh` l'active par commodité de script de développement, sauf si vous passez `--no-observability`. (REQ-302, REQ-303, REQ-330)

**Ctrl+C** arrête tout — backend, UI et tous les services Docker — et annule tout correctif de configuration. (REQ-619)

**Ctrl+R** redémarre uniquement le backend (utile après une modification de configuration que le rechargement à chaud ne détecte pas). (REQ-619)

### Options

`--no-observability` — Désactive le traçage distribué. Par défaut, `start-ui.sh` télécharge l'agent Java OpenTelemetry s'il n'est pas déjà présent, applique un correctif au `jvm.config` de Trino pour le charger, et démarre l'OTel collector, Prometheus, Tempo et Grafana. Passez `--no-observability` pour ignorer tout cela. Le correctif de `jvm.config` est annulé lors de Ctrl+C. [tool-verified: start-ui.sh lines 15, 67–82] (REQ-330)

`--seed-data` — Alimente Kafka avec des données de démonstration une fois les services Docker en bon état. Non exécuté par défaut. [tool-verified: start-ui.sh lines 14, 173–178]

`--keep-docker` — Laisse les services Docker Compose en cours d'exécution après Ctrl+C au lieu d'appeler `docker compose down`. [tool-verified: start-ui.sh lines 16, 301–306] (REQ-619)

`--reset-volumes` — Efface tous les volumes Docker et redémarre avec un état propre. Utile pour la récupération après une panne de Docker. [tool-verified: start-ui.sh line 19] (REQ-170)

`--demo` — Démarre des sources de données de démonstration supplémentaires (schéma PostgreSQL pet-store, mock OpenAPI petstore, SQLite et un GraphQL distant). Alimente automatiquement les utilisateurs et commandes petstore. [tool-verified: start-ui.sh lines 17, 55–171]

`--idp=basic|firebase` — Active un fournisseur d'identité pour l'authentification. Sans cet indicateur, le backend s'exécute sans fournisseur d'authentification et toutes les requêtes sont traitées comme `admin`. [tool-verified: start-ui.sh line 18; provisa/auth/wiring.py lines 57–60; provisa/auth/middleware.py lines 57–68] (REQ-120, REQ-124)

---

## 3. Connecter une source de données

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

## 4. Exécuter votre première requête

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

Aucune authentification n'est requise lorsqu'aucune section `auth` n'est présente dans `config/provisa.yaml` (valeur par défaut en développement). Le rôle par défaut est `admin`. [tool-verified: provisa/auth/wiring.py lines 57–60; provisa/auth/middleware.py lines 56–68] (REQ-120, REQ-267)

---

## 5. Ouvrir l'UI

Ouvrez `http://localhost:3000` dans un navigateur.

La barre de navigation comporte quatre menus de premier niveau : [tool-verified: provisa-ui/src/components/NavBar.tsx lines 39–80]

- **Explore** — Explorateur de schéma (`/schema`), éditeur GraphQL (`/query`), éditeur Cypher (`/graph`), éditeur SQL (`/sql`)
- **Model** — Vues et commandes
- **Security** — Sécurité au niveau des lignes et politiques de masquage de colonnes (REQ-038, REQ-041)
- **Admin** — Vue d'ensemble, domaines, cache, tâches planifiées, état du système, observabilité, utilisateurs, organisations, rôles

L'API GraphQL d'administration se trouve à l'adresse `http://localhost:8001/admin/graphql`. [tool-verified: provisa/api/app.py line 3389] (REQ-620)

---

## Dépannage

**Le backend ne démarre pas** — vérifiez `.logs/server.log`. La cause la plus courante est une variable d'environnement manquante ou un conflit de port sur le 8001. [tool-verified: start-ui.sh line 202] (REQ-618)

**Les services Docker ne sont pas en bon état** — exécutez `docker compose -f docker-compose.core.yml -f docker-compose.dev.yml ps` pour voir quel service est bloqué. Le moteur de fédération prend environ 30 secondes au premier démarrage. (REQ-055)

**Conflit de port sur le 3000 ou le 8001** — `start-ui.sh` arrête les processus obsolètes sur ces ports avant de démarrer. Si autre chose occupe le port, arrêtez-le manuellement au préalable. [tool-verified: start-ui.sh lines 197–199] (REQ-619)

**Redémarrage propre** — arrêtez le script, puis exécutez `./start-ui.sh --reset-volumes` pour effacer tous les volumes et redémarrer. [tool-verified: start-ui.sh line 19] (REQ-170)

---

## Étapes suivantes

| Objectif | Document |
| ------ | ----- |
| Référence complète de configuration YAML | [configuration.md](configuration.md) |
| Sécurité au niveau des lignes, masquage de colonnes, authentification | [security.md](security.md) |
| Tous les types de sources pris en charge | [sources.md](sources.md) |
| Abonnements en temps réel | [subscriptions.md](subscriptions.md) |
| JDBC, outils de BI, Arrow Flight, Apollo Federation | [integrations.md](integrations.md) |
| Client Python | [python-client.md](python-client.md) |
| Déploiement en production | [deployment.md](deployment.md) |
