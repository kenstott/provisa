# Déploiement

## Choisir un chemin de déploiement

Provisa prend en charge six chemins de déploiement. Choisissez selon votre public et votre contexte opérationnel :

| Chemin | Artefact / Script | Idéal pour |
| ------ | ------------------- | ---------- |
| **Développement** | `start-ui.sh` | Développement depuis les sources, évaluation avec données de démonstration complètes |
| **Installateur macOS** | `Provisa-<version>-macOS.dmg` | Postes de travail de développeurs, évaluation |
| **Installateur Windows** | `Provisa-<version>-windows-x64.exe` | Postes de travail de développeurs, évaluation |
| **AppImage Linux** | `Provisa.AppImage` | Serveurs on-prem, VM cloud, environnements isolés (air-gapped) |
| **VM Cloud (AWS)** | `terraform/deploy.sh` | Déploiement cloud multi-nœuds avec équilibreurs de charge |
| **Kubernetes** | `helm/provisa/` | Équipes exploitant déjà K8s |

### VM ou Kubernetes

Les deux sont de qualité entreprise. Le chemin VM/AppImage est plus simple : aucun cluster à provisionner, aucune politique CNI ou RBAC à configurer, et l'AppImage est entièrement autonome (REQ-223). Il s'intègre naturellement dans l'outillage de gestion de serveurs existant (Ansible, Puppet, agents Datadog, forwarders Splunk, etc.).

Choisissez Kubernetes uniquement si votre équipe exploite déjà un cluster K8s et souhaite que Provisa participe à ce modèle opérationnel (déploiements progressifs, HPA, observabilité unifiée) (REQ-056). Les capacités sont équivalentes — Kubernetes ajoute une surcharge opérationnelle, pas des capacités.

### Acquisition des images et analyse de sécurité

Tous les chemins de production nécessitent d'obtenir les artefacts Provisa avant qu'un déploiement ne puisse s'exécuter. « Isolé » (air-gapped) désigne ce qui se passe au moment de l'installation sur la machine cible — les artefacts doivent d'abord être acquis.

**Installateurs macOS et Windows :** Téléchargez depuis la [page des releases GitHub](https://github.com/provisa/provisa/releases). Entièrement packagés ; aucun accès internet requis après le téléchargement (REQ-227). Destinés au dev/évaluation, pas à la production — aucune passerelle d'analyse d'image n'est attendue.

**Chemin AppImage :** Téléchargez depuis la [page des releases GitHub](https://github.com/provisa/provisa/releases) et transférez vers la machine cible. L'AppImage regroupe toutes les images de composants sous forme de tarballs à l'intérieur d'un système de fichiers squashfs (REQ-294) — la plupart des scanners de registre ne peuvent pas les inspecter sur place. Contactez votre équipe de compte Provisa pour obtenir les empreintes (digests) des images de composants afin de les vérifier indépendamment avec votre scanner.

**Chemin Terraform :** L'AppImage doit être téléversée sur S3 avant d'exécuter `terraform/deploy.sh`. Les nœuds EC2 la téléchargent au démarrage via un rôle IAM — ils nécessitent un accès S3 sortant (direct ou via un point de terminaison de passerelle VPC). Appliquez la même politique d'analyse que pour le chemin AppImage.

**Chemin Helm / Kubernetes :** Les images individuelles doivent être poussées vers un registre accessible au cluster. Ce chemin est le plus compatible avec l'analyse basée sur registre (Prisma Cloud, Aqua, Trivy, AWS Inspector) — les images sont des objets de première classe que les scanners comprennent nativement. Pour les clusters isolés, miroitez les images vers un registre interne et surchargez les références dans `values.yaml` (REQ-294).

---

## Développement (depuis les sources)

### Recommandé : `start-ui.sh`

La façon la plus simple d'exécuter Provisa depuis les sources. Démarre toute l'infrastructure, l'API backend, et le serveur de développement de l'UI en une seule commande (REQ-055). Ctrl+C arrête tout proprement.

**Prérequis :** Docker Desktop, Node.js, virtualenv Python dans `.venv/`

```bash
./start-ui.sh
```

Ce que fait le script :

- Démarre `docker-compose.core.yml` + `docker-compose.dev.yml` (tous les services core + démo) et attend qu'ils soient sains (REQ-055)
- Amorce Kafka avec des données de démonstration
- Synchronise les dépendances Python depuis `.venv/`
- Démarre l'API backend sur le port 8001 (logs vers `.logs/server.log`) (REQ-558)
- Démarre le serveur de développement Vite UI sur le port 3000 (REQ-559)
- Affiche les URL et attend ; Ctrl+C arrête tout et détruit la stack compose

```yaml
Backend: http://localhost:8001
UI:      http://localhost:3000
```

**Options :**

`--reset-volumes` — Exécute `docker compose down -v` avant le démarrage, détruisant tous les volumes Docker (données PostgreSQL, objets MinIO, état Redis, etc.) (REQ-170). À utiliser quand vous voulez repartir sur une base entièrement propre — après un changement de schéma en développement, ou lorsque Docker a planté et laissé des volumes corrompus. **Toutes les données seront perdues.**

`--observability` — Ajoute l'instrumentation complète de traçage et de métriques. Télécharge l'agent Java OpenTelemetry et patche le `jvm.config` de Trino pour le charger, instrumente le backend Provisa avec l'export OTLP, et démarre le collecteur OTel, Prometheus, Tempo et Grafana (`http://localhost:3100`) (REQ-330). Le patch de `jvm.config` est automatiquement annulé sur Ctrl+C.

### Étapes manuelles (backend uniquement, sans UI)

Si vous n'avez besoin que de l'API :

1. Installez [Docker Desktop](https://docs.docker.com/get-docker/)
2. Démarrez les services core :

   ```bash
   docker compose -f docker-compose.core.yml up -d
   ```

3. Démarrez l'API :

   ```bash
   uvicorn main:app --reload --port 8001
   ```

4. Vérifiez : `curl http://localhost:8001/health`

### Stack complète (Provisa en conteneur)

Pour exécuter l'API en tant que conteneur plutôt que sur l'hôte :

```bash
docker compose -f docker-compose.core.yml -f docker-compose.app.yml up -d
```

### Services

**Core (`docker-compose.core.yml`) — toujours requis :**

| Service | Port | Objet |
| --------- | ------ | --------- |
| PostgreSQL | 5432 | Métadonnées de configuration + catalogue Iceberg (REQ-169) |
| PgBouncer | 6432 | Pooling de connexions (REQ-053) |
| Moteur de fédération | 8080 | Fédération de requêtes (REQ-028) |
| Redis | 6379 | Cache de résultats de requête (REQ-371) |
| MinIO | 9000/9001 | Stockage objet compatible S3 (REQ-029, REQ-171) |

**Démo (`docker-compose.dev.yml`) — optionnel, inclus par `start-ui.sh` :**

| Service | Port | Objet |
| --------- | ------ | --------- |
| MongoDB | 27017 | Source NoSQL de démonstration |
| Kafka | 9092 | Source de streaming de démonstration |
| Schema Registry | 8081 | Gestion de schéma Avro/Protobuf de démonstration |
| Debezium | — | Connecteur CDC de démonstration |
| Elasticsearch | 9200 | Source de recherche de démonstration |
| Neo4j | 7474/7687 | Source graphe de démonstration |
| Fuseki | 3030 | Triplestore SPARQL de démonstration |
| OpenTelemetry Collector | — | Collecte de traces (avec `--observability`) (REQ-302) |
| Prometheus | 9090 | Métriques (avec `--observability`) (REQ-330) |
| Tempo | — | Stockage de traces (avec `--observability`) (REQ-330) |
| Grafana | 3100 | Tableaux de bord (avec `--observability`) (REQ-330) |

### Backend de télémétrie (`otlp2sql`)

La stack `--observability` ci-dessus (Collector → Tempo/Prometheus/Grafana) est un
chemin de télémétrie. L'autre est `otlp2sql` (`provisa.observability.otlp2sql`) : un
récepteur OTLP/HTTP qui écrit les traces, métriques et logs dans une base de données SQL
choisie par une URL SQLAlchemy, en extrayant les attributs de span `provisa.*` à l'ingestion
de sorte qu'aucun job de compaction séparé ne s'exécute. Les écritures sont regroupées par lots
(`OTLP2SQL_BATCH_MAX_ROWS`, défaut 1000 ; `OTLP2SQL_BATCH_MAX_SECS`, défaut 2s).

La télémétrie dispose de son propre magasin, séparé de la base de données du plan de contrôle. Sélectionnez
le backend avec `PROVISA_OPS_DB_URL` :

| `PROVISA_OPS_DB_URL` | Backend | Remarques |
| --- | --- | --- |
| *(non défini)* | DuckDB dédié sous `~/.provisa/telemetry/` | par défaut ; pas de serveur, pas de Docker |
| `clickhouse+native://user@host/otel` | ClickHouse | ingestion à haut débit avec fusions d'arrière-plan automatiques |
| `postgresql+psycopg2://user@host/otel` | PostgreSQL | volume modéré |
| `trino://user@host:8080/otel` | Trino / Iceberg | fonctionne techniquement, **non recommandé** — voir ci-dessous |

**Sur `trino://` :** le dialecte SQLAlchemy Trino émet un DDL Trino valide et des
`INSERT`s, ce qui le rend techniquement viable comme backend `otlp2sql`. Ce n'est pas
recommandé au-delà de faibles débits d'ingestion. Chaque vidage de lot devient un
`INSERT` Trino distribué plus un snapshot Iceberg, donc une télémétrie à haut débit
produit de nombreux petits fichiers et snapshots et nécessite toujours périodiquement
`ALTER TABLE ... EXECUTE optimize` / `expire_snapshots` — que `otlp2sql` n'exécute
pas. Cela place également le moteur de requêtes sur le chemin critique d'ingestion.

Pour une télémétrie à haut volume vers Trino/Iceberg, utilisez plutôt `otlp2parquet` : il
écrit du parquet vers le stockage objet sans passer par Trino, et une compaction Trino planifiée
fusionne les fichiers bruts dans les tables Iceberg actives. Pour un moteur unique gérant
à la fois l'ingestion à haut débit et la compaction, préférez ClickHouse.

Pointez les exportateurs OTLP de l'application et de Trino (`OTEL_EXPORTER_OTLP_ENDPOINT`) vers le
endpoint `otlp2sql`, et enregistrez le domaine ops contre le même
`PROVISA_OPS_DB_URL` afin qu'il lise ce que le récepteur a écrit.

---

## Installateur macOS

Pour les postes de travail de développeurs et l'évaluation. Entièrement isolé (air-gapped) — aucun accès internet requis après le téléchargement (REQ-227).

L'installateur de base est une **installation native** : moteur de fédération DuckDB + plan de contrôle SQLite + cache en mémoire (fakeredis), sans Docker, VM, Trino, Redis ou MinIO (REQ-972, REQ-979). Le moteur de fédération est un choix de l'assistant — DuckDB (natif, par défaut), Trino-sur-Docker, ou un moteur externe (REQ-973). L'observabilité est toujours active en auto-télémétrie visible dans Admin ; la stack Docker collector/Prometheus/Grafana est une démonstration externe optionnelle, pas un interrupteur marche/arrêt (REQ-975). Le pack de données de démonstration est optionnel et désactivé par défaut (REQ-978). Trino, la stack d'observabilité Docker, et la démo sont des extensions lourdes résolues en priorité locale (répertoire adjacent à l'installateur, volumes montés, `~/Downloads`, puis release GitHub), afin que les entreprises puissent pré-positionner des tarballs pour les installations isolées (REQ-977).

### Étapes

1. Téléchargez `Provisa-<version>-macOS.dmg` depuis la [page des releases GitHub](https://github.com/provisa/provisa/releases)
2. Ouvrez le DMG et glissez **Provisa.app** vers `/Applications`
3. Double-cliquez sur **Provisa.app** — la configuration au premier lancement s'exécute une fois ; l'assistant propose les choix de moteur, d'observabilité et de démo ci-dessus (REQ-1007)
4. Ouvrez le Terminal :

   ```bash
   provisa start    # start all services
   provisa status   # confirm all services are running
   provisa open     # open the UI in the browser
   ```

   (REQ-224)

### Persistance des données

Toutes les données sont stockées dans `~/.provisa/` (REQ-224). Pour tout supprimer : `provisa uninstall`.

---

## Installateur Windows

Pour les postes de travail de développeurs et l'évaluation. Entièrement isolé (air-gapped) — aucun accès internet requis après le téléchargement (REQ-227).

Comme macOS, l'installateur Windows de base est un **niveau natif** : un runtime Python autonome + le wheel provisa + DuckDB/pg_duckdb + un plan de contrôle SQLite, sans livrer Docker, VM, ni images de conteneur (REQ-979). Le moteur de fédération (Trino), la stack d'observabilité, et le pack de données de démonstration sont ajoutés ultérieurement via des installateurs en couches séparés, dans l'ordre : l'installateur Container (`Provisa-Container-<version>.exe`, qui ajoute WSL2 + containerd + Trino), puis l'installateur Obs (nécessite le niveau container), puis l'installateur Demo (nécessite Core + Obs). Le guide de premier lancement explique comment initialiser le moteur de fédération en exécutant l'installateur Container (REQ-1005).

### Étapes

1. Téléchargez `Provisa-<version>-windows-x64.exe` depuis la [page des releases GitHub](https://github.com/provisa/provisa/releases)
2. Exécutez l'installateur — aucun droit d'administrateur requis ; installe vers `%LOCALAPPDATA%\Programs\Provisa\`
3. Ouvrez **Provisa First Launch** depuis le menu Démarrer — la configuration native s'exécute une fois et affiche les prochaines étapes pour les extensions en couches (REQ-1005)
4. Ouvrez un nouveau terminal :

   ```text
   provisa status
   provisa open
   ```

   (REQ-224)

### Persistance des données

Toutes les données sont stockées dans `%USERPROFILE%\.provisa\`.

---

## AppImage Linux — VM Mono ou Multi-nœuds

### Ce que c'est

`Provisa.AppImage` est un exécutable autonome unique regroupant (REQ-223, REQ-228) :

- Un démon Docker sans root (`dockerd-rootless.sh` + `rootlesskit`) — aucun Docker système ni droits root requis
- Tous les tarballs d'images de conteneur (PostgreSQL, PgBouncer, MinIO, Redis, Moteur de fédération, API Provisa) (REQ-294)
- Le wrapper CLI Provisa et le script de configuration au premier lancement

L'image Provisa est pré-construite au moment du packaging — les sources Python ne sont jamais incluses.

### Quand l'utiliser

- Bare metal on-premises ou VM (nœud unique ou multi-nœuds)
- VM cloud sans cluster K8s
- Environnements isolés (air-gapped) (REQ-294)
- Lorsque vous voulez des opérations plus simples que Kubernetes

---

### Étapes — Nœud unique

1. Téléchargez `Provisa.AppImage` depuis la [page des releases GitHub](https://github.com/provisa/provisa/releases) et transférez-la vers la machine cible
2. Rendez-la exécutable :

   ```bash
   chmod +x Provisa.AppImage
   ```

3. Exécutez la configuration au premier lancement :

   ```bash
   ./Provisa.AppImage
   ```

4. L'assistant de configuration demande :
   - **Rôle** → sélectionnez `primary`
   - **Budget RAM** → quantité de RAM à allouer (0 = tout le disponible) ; détermine le nombre de workers Trino
   - **Nom d'hôte** → l'adresse annoncée de ce nœud
   - **Port API** → par défaut `8000` (REQ-560)
5. La configuration charge toutes les images de conteneur (~2-5 minutes), écrit la configuration, et démarre les services
6. Vérifiez :

   ```bash
   provisa status
   curl http://localhost:8000/health
   ```

---

### Étapes — Multi-nœuds (Primaire)

Exécutez ces étapes d'abord sur le nœud primaire. Les nœuds secondaires doivent être configurés après que le primaire fonctionne.

1. Téléchargez et transférez `Provisa.AppImage` vers la machine primaire
2. Ouvrez les ports de pare-feu requis (les secondaires se connecteront en entrant sur ces ports) :

   | Port | Service |
   | ------ | --------- |
   | 5432 | PostgreSQL |
   | 6379 | Redis |
   | 9000 | MinIO |
   | 8080 | Coordinateur du moteur de fédération |
   | 8000 | API Provisa |

3. Rendez exécutable et exécutez :

   ```bash
   chmod +x Provisa.AppImage
   ./Provisa.AppImage
   ```

4. L'assistant de configuration demande :
   - **Rôle** → sélectionnez `primary`
   - **Budget RAM**, **nom d'hôte**, **port API** → répondez comme pour le nœud unique
5. Une fois la configuration terminée, notez l'**IP privée** de cette machine — les secondaires en ont besoin
6. L'assistant affiche un bloc upstream nginx — sauvegardez-le pour votre configuration d'équilibreur de charge
7. Vérifiez :

   ```bash
   provisa status
   curl http://localhost:8000/health
   ```

---

### Étapes — Multi-nœuds (Chaque secondaire)

Répétez ces étapes sur chaque nœud additionnel après que le primaire fonctionne et est accessible.

1. Téléchargez et transférez `Provisa.AppImage` vers la machine secondaire
2. Confirmez que le secondaire peut atteindre le primaire :

   ```bash
   curl http://<primary-ip>:8000/health
   ```

3. Rendez exécutable et exécutez :

   ```bash
   chmod +x Provisa.AppImage
   ./Provisa.AppImage
   ```

4. L'assistant de configuration demande :
   - **Rôle** → sélectionnez `secondary`
   - **IP primaire** → entrez l'IP du nœud primaire (la connectivité est vérifiée en direct)
   - **Budget RAM**, **nom d'hôte**, **port API** → répondez comme ci-dessus
5. La configuration charge un jeu d'images réduit (pas de PostgreSQL, PgBouncer, MinIO, Redis — ceux-ci ne s'exécutent que sur le primaire) (REQ-561), démarre l'API Provisa et un worker du moteur de fédération
6. Vérifiez :

   ```bash
   provisa status
   curl http://localhost:8000/health
   ```

7. Ajoutez ce nœud à l'upstream de votre équilibreur de charge

---

### Topologie primaire / secondaire

**Le nœud primaire** exécute tous les services singletons :

| Service | Pourquoi singleton |
| --------- | --------------- |
| PostgreSQL | Schéma partagé, configuration de l'application, modèle sémantique |
| Redis | Cache de résultats de requête partagé et état d'abonnement (REQ-371) |
| MinIO | Magasin objet partagé pour les résultats de redirection et les snapshots de MV (REQ-029) |
| Coordinateur du moteur de fédération | Tous les workers (primaire + secondaires) s'enregistrent ici (REQ-028) |

**Les nœuds secondaires** exécutent uniquement :

- API Provisa — sans état ; lit toute la configuration depuis PostgreSQL sur le primaire au démarrage (REQ-057, REQ-562)
- Worker du moteur de fédération — s'auto-enregistre auprès du coordinateur sur le primaire (REQ-028)

Tout l'état applicatif transite par le PostgreSQL du primaire. Aucune synchronisation manuelle requise. (REQ-562)

---

### Premier lancement non interactif (automatisé)

Pour Terraform, cloud-init, ou Ansible — passez des flags au lieu de répondre aux invites :

```bash
# Primary
./Provisa.AppImage --non-interactive --role primary --ram-gb 32

# Secondary
./Provisa.AppImage --non-interactive --role secondary --primary-ip 10.0.0.10 --ram-gb 32
```

Le mode non interactif installe une unité systemd (`/etc/systemd/system/provisa.service`) pour le démarrage automatique. (REQ-563)

| Flag | Description |
| ------ | ------------- |
| `--non-interactive` | Ignore toutes les invites ; installe l'unité systemd |
| `--role primary\|secondary` | Rôle du nœud |
| `--primary-ip <ip>` | IP du nœud primaire (requis pour secondaire) |
| `--ram-gb <n>` | RAM à allouer (0 = tout le disponible) |

---

## Déploiement VM Cloud — Terraform (AWS)

Provisionne un cluster Provisa multi-nœuds complet sur AWS — VPC, groupes de sécurité, instances EC2, ALB, NLB — en une seule commande interactive. (REQ-564)

### Fichiers

| Fichier | Objet |
| ------ | --------- |
| `terraform/deploy.sh` | Wrapper interactif — collecte les paramètres, valide les identifiants, écrit `terraform.tfvars`, exécute apply |
| `terraform/aws/variables.tf` | Toutes les définitions de variables avec valeurs par défaut |
| `terraform/aws/main.tf` | VPC, sous-réseaux, groupes de sécurité, IAM, EC2, ALB, NLB |
| `terraform/aws/outputs.tf` | URL de endpoints et IP des nœuds |

### Étapes

1. Téléchargez `Provisa.AppImage` depuis la [page des releases GitHub](https://github.com/provisa/provisa/releases)

2. Téléversez-la vers un bucket S3 dans votre compte AWS :

   ```bash
   aws s3 cp Provisa.AppImage s3://<your-bucket>/releases/Provisa.AppImage
   ```

3. Assurez-vous que les identifiants AWS sont disponibles dans votre shell (l'un des suivants) :
   - Variables d'environnement : `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY`
   - Profil nommé : `export AWS_PROFILE=my-profile`
   - Session SSO active : `aws sso login`

4. (Optionnel) Si vous voulez un accès SSH aux nœuds, créez une paire de clés EC2 dans votre région cible et notez son nom

5. Exécutez le wrapper de déploiement :

   ```bash
   bash terraform/deploy.sh
   ```

6. Répondez aux questions de l'assistant (voir tableau de référence ci-dessous). Le script vérifie que l'AppImage existe dans S3 avant de continuer et abandonne si ce n'est pas le cas

7. Passez en revue le résumé du déploiement et confirmez

8. Terraform provisionne toute l'infrastructure (~5-10 minutes). Après apply, le script affiche :

   ```text
   api_endpoint      = "http://<alb-dns>:8000"
   flight_endpoint   = "<nlb-dns>:8815"
   primary_ip        = "10.0.x.x"
   secondary_ips     = ["10.0.x.x", ...]
   ```

   (REQ-564, REQ-143)

9. (Optionnel) Pointez les enregistrements DNS vers les noms DNS de l'ALB et du NLB

10. Vérifiez :

    ```bash
    curl http://<api_endpoint>/health
    ```

### Questions de l'assistant

| Question | Défaut | Remarques |
| ---------- | --------- | ------- |
| Fournisseur cloud | — | AWS uniquement aujourd'hui |
| Identifiants AWS | — | Vérifie d'abord une session active |
| Région | `us-east-1` | |
| Nombre de nœuds | `2` | 1 = primaire uniquement, pas de LB ; 2+ = primaire + secondaires + ALB/NLB |
| Type d'instance | `m7i.2xlarge` | Voir le guide de dimensionnement ci-dessous |
| Taille du volume racine | `100 GB` | Par nœud |
| Budget RAM | `0` (toute la RAM) | Détermine le nombre de workers Trino par nœud |
| Bucket S3 | — | Vérifié en direct avant de continuer |
| Clé S3 | `releases/Provisa.AppImage` | |
| Accès SSH | Non | Nécessite un nom de paire de clés existant + CIDR admin |
| CIDR VPC | `10.0.0.0/16` | |

### Guide de dimensionnement des instances

| Type | vCPU | RAM | Workers Trino/nœud | Cas d'usage |
| ------ | ------ | ----- | -------------------- | ---------- |
| `m7i.xlarge` | 4 | 16 GB | 0 | Dev / petits jeux de données |
| `m7i.2xlarge` | 8 | 32 GB | 1 | Petite production |
| `m7i.4xlarge` | 16 | 64 GB | 2 | Production moyenne |
| `m7i.8xlarge` | 32 | 128 GB | 4 | Grande production |

Tous les nœuds contribuent des workers à un seul coordinateur sur le primaire (REQ-028). Un cluster `m7i.4xlarge` à 3 nœuds produit 6 workers Trino au total.

### Ce qui est provisionné

- VPC avec deux sous-réseaux publics sur deux zones de disponibilité (REQ-564)
- Groupes de sécurité : groupe LB (ingress public sur 8000/8815), groupe nœuds (LB → nœuds, intra-cluster, SSH optionnel)
- Rôle IAM + profil d'instance avec S3 GetObject sur le bucket AppImage
- Instance EC2 primaire — exécute le premier lancement en mode `--non-interactive --role primary`
- Instances EC2 secondaires (node_count − 1) — exécutent le premier lancement en mode `--non-interactive --role secondary --primary-ip <primary private IP>` ; dépendent de l'achèvement préalable du primaire
- ALB sur le port 8000 — API HTTP, vérifications de santé `/health` (REQ-560)
- NLB sur le port 8815 — Arrow Flight / gRPC (REQ-143)
- Les deux LB s'attachent à tous les nœuds

### Liste de contrôle des prérequis

- [ ] Permissions IAM : EC2 complet, ELB complet, VPC complet, création de rôle IAM, S3 GetObject sur le bucket AppImage
- [ ] `Provisa.AppImage` téléversée sur S3
- [ ] Les nœuds EC2 ont un accès S3 sortant (internet direct ou point de terminaison de passerelle VPC S3)
- [ ] Une paire de clés EC2 existe dans la région cible (si SSH est nécessaire)
- [ ] Terraform ≥ 1.5 installé localement
- [ ] Enregistrements DNS planifiés pour ALB / NLB (optionnel mais recommandé)
- [ ] Certificat ACM prêt si HTTPS est requis (non inclus dans le Terraform de base)

### Secrets

Aucun secret n'est intégré dans Terraform. L'AppImage génère les identifiants pendant le premier lancement et les écrit dans `~/.provisa/config.yaml` sur chaque nœud (REQ-563). Pour la production, récupérez le jeton admin depuis le nœud primaire après déploiement :

```bash
ssh ubuntu@<primary-public-ip> cat ~/.provisa/config.yaml | grep admin_token
```

---

## Kubernetes / Helm

### Quand l'utiliser

Votre équipe exploite déjà un cluster Kubernetes et veut que Provisa participe à ce modèle opérationnel (REQ-056). Si vous évaluez Provisa ou déployez on-premises sans cluster existant, le chemin AppImage est plus simple.

Remarque : l'AppImage Provisa ne peut pas s'exécuter dans un pod Kubernetes — elle nécessite FUSE et un démon Docker sans root, qui ne sont pas disponibles dans les profils de sécurité de pod standard.

### Étapes

1. Confirmez l'accès au cluster :

   ```bash
   kubectl cluster-info
   ```

2. Récupérez et miroitez les images vers votre registre interne (requis pour les environnements isolés ou analysés ; ignorez si vous tirez directement depuis des registres publics) (REQ-294) :

   | Image | Utilisée pour |
   | ------- | ---------- |
   | `provisa/provisa:<version>` | API Provisa |
   | `trinodb/trino:480` | Coordinateur + workers du moteur de fédération (REQ-169) |
   | `postgres:16` | PostgreSQL intra-cluster (si `postgresql.enabled`) (REQ-169) |
   | `edoburu/pgbouncer:latest` | PgBouncer intra-cluster (si `pgbouncer.enabled`) (REQ-053) |
   | `redis:7.2` | Redis intra-cluster (si `redis.enabled` et pas de `redis.host`) (REQ-371) |
   | `minio/minio:latest` | MinIO intra-cluster (si `minio.enabled`) (REQ-029) |

   Pour les environnements avec analyse de registre :
   - Poussez chaque image vers votre registre de staging
   - Exécutez votre scanner (Prisma Cloud, Aqua, Trivy, AWS Inspector) et obtenez l'approbation
   - Promouvez vers votre registre interne de production

3. Décidez avant l'installation :
   - **PostgreSQL** — intra-cluster (`postgresql.enabled: true`) ou géré externe (`postgresql.host`) ? Externe recommandé pour la production
   - **Redis** — intra-cluster ou externe (`redis.host`) ? Changez le mot de passe par défaut (`redis.password`)
   - **MinIO / S3** — MinIO intra-cluster ou S3 natif ? Pour AWS, utilisez S3 avec un rôle IAM
   - **Secrets** — transmettez via `--set` pour l'évaluation ; utilisez External Secrets ou Vault Agent pour la production

4. Installez le chart :

   ```bash
   helm install provisa helm/provisa/ \
     --set config.pgPassword=<password> \
     --set config.adminToken=<token> \
     --set s3.endpoint=https://s3.amazonaws.com \
     --set s3.bucket=my-provisa-results \
     --namespace provisa --create-namespace
   ```

   Si vous utilisez un registre interne, ajoutez des surcharges d'image :

   ```bash
   --set image.repository=harbor.internal.example.com/provisa/provisa \
   --set image.tag=1.2.3 \
   --set trino.image.repository=harbor.internal.example.com/trinodb/trino \
   --set trino.image.tag=480
   ```

5. Vérifiez que les pods fonctionnent :

   ```bash
   kubectl get pods -n provisa
   ```

6. Vérifiez l'API :

   ```bash
   kubectl port-forward svc/provisa 8000:8000 -n provisa
   curl http://localhost:8000/health
   ```

7. (Optionnel) Activez l'ingress pour l'accès externe — définissez `ingress.enabled: true` et configurez votre contrôleur d'ingress

### Liste de contrôle des prérequis

- [ ] Kubernetes 1.26+, Helm 3.12+
- [ ] Classe de stockage prenant en charge les PVC `ReadWriteOnce` (pour les services avec état intra-cluster)
- [ ] Images disponibles pour le cluster (registre public ou interne)
- [ ] Endpoint PostgreSQL + identifiants (si externe)
- [ ] Endpoint Redis + identifiants (si externe)
- [ ] Bucket S3 + identifiants ou rôle IAM
- [ ] Jeton admin choisi
- [ ] Contrôleur d'ingress configuré (si accès externe requis)

### Valeurs clés

| Valeur | Défaut | Description |
| ------- | --------- | ------------- |
| `replicaCount` | `2` | Répliques de l'API Provisa (sans état) (REQ-057) |
| `config.pgHost` | `postgres` | Hôte PostgreSQL |
| `config.pgPassword` | | Mot de passe PostgreSQL |
| `config.adminToken` | | Jeton bearer de l'API Admin |
| `redis.enabled` | `true` | Déploie un StatefulSet Redis intra-cluster (REQ-371) |
| `redis.host` | `""` | Définir pour utiliser un Redis externe |
| `redis.port` | `6379` | |
| `redis.password` | `"provisa"` | À changer |
| `redis.tls` | `false` | |
| `trino.enabled` | `true` | Déploie le moteur de fédération (REQ-028) |
| `trino.workers` | `2` | Répliques de workers du moteur de fédération (REQ-056) |
| `postgresql.enabled` | `true` | Déploie PostgreSQL intra-cluster (REQ-169) |
| `postgresql.host` | `""` | Définir pour utiliser un PostgreSQL externe |
| `minio.enabled` | `true` | Déploie MinIO intra-cluster (REQ-029) |
| `s3.endpoint` | | URL de endpoint compatible S3 |
| `s3.bucket` | `provisa-results` | Bucket pour la redirection de grands résultats (REQ-029, REQ-137) |
| `ingress.enabled` | `false` | Active l'ingress |

### Mise à l'échelle

```bash
kubectl scale deployment/provisa --replicas=5 --namespace provisa
```

Les workers du moteur de fédération évoluent indépendamment — plus de workers augmentent le débit et la capacité de requêtes concurrentes (REQ-056). (REQ-057)

### Mise à jour de la configuration

```bash
kubectl create configmap provisa-config \
  --from-file=config.yaml=./config.yaml \
  --namespace provisa --dry-run=client -o yaml | kubectl apply -f -
kubectl rollout restart deployment/provisa --namespace provisa
```

---

## Haute disponibilité et récupération

Provisa applique un modèle de récupération à deux niveaux à travers tous les modes de déploiement (REQ-703) :

- **Niveau 1 — erreurs transitoires.** Les opérations de lecture réessaient jusqu'à 30 secondes en cas d'erreurs transitoires en utilisant un backoff exponentiel avec jitter complet. Ajustez le budget avec `PROVISA_RETRY_BUDGET_SECS`. Les opérations d'écriture ne sont jamais réessayées en interne, et les erreurs mémoire ne sont jamais réessayables.
- **Niveau 2 — défaillance de composant.** Un observateur de moteur interne détecte et redémarre les composants logiciels défaillants en 2 à 3 minutes.

Les défaillances au niveau machine et au niveau cluster restent de la responsabilité de l'opérateur — provisionnez des nœuds redondants et un équilibreur de charge (chemins Terraform et Helm ci-dessus) pour la tolérance à la perte de nœud.

## Dépendances du moteur de fédération

Les moteurs de fédération d'entrepôt nécessitent des paquets Python et des composants au niveau système au-delà de l'installation par défaut de Provisa. Tous les paquets Python listés ici sont déclarés dans `pyproject.toml` et installés dans le cadre de l'installation standard `pip install provisa` ou `pip install -e .` [tool-verified: `pyproject.toml` lignes 44–52].

Les paquets Python sont livrés avec l'installation par défaut de Provisa — aucun extra optionnel requis pour aucun moteur d'entrepôt. Les éléments au niveau système (driver ODBC, CLI cloud, clés de compte de service) doivent être installés séparément.

### Paquets Python (déjà dans les dépendances core)

[tool-verified: `pyproject.toml` lignes 41–52]

| Paquet | Moteur | Objet |
| ------- | ------ | ------- |
| `databricks-sql-connector` | Databricks | Connexion à l'entrepôt SQL ; Arrow Cloud Fetch (REQ-987) |
| `snowflake-connector-python[pandas]` | Snowflake | Connexion + `fetch_arrow_table` Arrow-native (REQ-988) |
| `google-cloud-bigquery` | BigQuery | Exécution de requêtes |
| `google-cloud-bigquery-storage` | BigQuery | Storage Read API pour lectures Arrow-native |
| `google-cloud-storage` | BigQuery | Staging GCS pour les liens de table externe |
| `pyodbc` | Fabric, Synapse | Connexion ODBC aux endpoints T-SQL |
| `azure-identity` | Fabric, Synapse | Jeton Azure AD via `DefaultAzureCredential` |
| `clickhouse-connect` | ClickHouse | Lectures columnaires HTTP |
| `protobuf>=6.33.5,<7` | BigQuery, gRPC | Épinglage de compatibilité — `google-cloud-*` et OTel partagent un runtime protobuf ; `<7` les garde alignés |
| `grpcio-status<1.82` | gRPC | S'aligne avec l'épinglage `protobuf<7` |

### Exigences au niveau système

Ce ne sont pas des paquets Python — ils doivent être installés sur l'hôte ou le conteneur qui exécute Provisa.

**Microsoft Fabric et Azure Synapse (ODBC)**

`pyodbc` se connecte via le Microsoft ODBC Driver for SQL Server (`msodbcsql18`). Le driver doit être installé sur l'hôte — pas via pip. [tool-verified: `mssql_warehouse_runtime.py` ligne 84 `"ODBC Driver 18 for SQL Server"` par défaut]

macOS :

```bash
brew install microsoft/mssql-release/msodbcsql18
```

Linux (Ubuntu/Debian) :

```bash
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql18
```

Provisa détecte le driver automatiquement. Pour surcharger le nom du driver (pour les installations non standard), définissez :

```bash
export PROVISA_MSSQL_ODBC_DRIVER="ODBC Driver 17 for SQL Server"
```

**Authentification Azure AD (Fabric et Synapse)**

Les deux moteurs s'authentifient via `azure.identity.DefaultAzureCredential` [tool-verified: `mssql_warehouse_runtime.py:79`, `fabric_shortcuts.py:46`]. `DefaultAzureCredential` vérifie les sources d'identifiants dans l'ordre : variables d'environnement, identité de charge de travail, identité managée, VS Code, `az login`, et autres.

Pour le développement local, `az login` est le chemin le plus simple :

```bash
az login
```

Pour la production, utilisez une identité managée (sur des VM Azure ou AKS) — aucune gestion d'identifiants nécessaire. Pour l'authentification par principal de service, définissez :

```bash
export AZURE_TENANT_ID=<tenant>
export AZURE_CLIENT_ID=<app-id>
export AZURE_CLIENT_SECRET=<secret>
```

**BigQuery (compte de service)**

`google-cloud-bigquery` utilise les Application Default Credentials. Pour le développement local, pointez vers un fichier de clé de compte de service :

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa-key.json
```

Pour la production sur GCP (Cloud Run, GKE avec Workload Identity, Compute Engine), la bibliothèque détecte automatiquement le compte de service attaché — aucune variable d'environnement nécessaire.

Le compte de service a besoin de :

- `roles/bigquery.dataViewer` — lire les données
- `roles/bigquery.jobUser` — exécuter des requêtes
- `roles/bigquery.dataEditor` — créer des tables externes (pour ATTACH)
- `roles/storage.objectViewer` — lire les objets GCS pour les tables externes

**Databricks (certificat CA dans les environnements proxy de développement)**

Si Provisa s'exécute derrière un proxy interceptant le TLS (Charles, mitmproxy, proxies d'entreprise), le connecteur SQL Databricks peut rejeter le certificat du proxy. Transmettez un bundle CA personnalisé :

```bash
export REQUESTS_CA_BUNDLE=/path/to/your/proxy-ca.pem
```

Le connecteur Databricks hérite cela de `requests` — aucune variable d'environnement spécifique à Databricks n'est nécessaire.

### Liste de contrôle par moteur

**Databricks** (REQ-987)

- [ ] `databricks-sql-connector` installé (par défaut)
- [ ] URL du moteur avec `http_path` : `databricks://token:TOKEN@workspace.azuredatabricks.net?http_path=/sql/1.0/warehouses/xxx`
- [ ] Jeton d'accès personnel ou jeton de principal de service
- [ ] `REQUESTS_CA_BUNDLE` défini si derrière un proxy interceptant le TLS

**Snowflake** (REQ-988)

- [ ] `snowflake-connector-python[pandas]` installé (par défaut)
- [ ] URL du moteur : `snowflake://user:pass@account.snowflakecomputing.com/database`
- [ ] `account` dans `PROVISA_ENGINE_URL` ou `federation_hints`

**BigQuery** (REQ-989)

- [ ] `google-cloud-bigquery`, `google-cloud-bigquery-storage`, `google-cloud-storage` installés (par défaut)
- [ ] `GOOGLE_APPLICATION_CREDENTIALS` défini (dev) ou identité de charge de travail configurée (prod)
- [ ] `GOOGLE_CLOUD_PROJECT` défini si le projet ne peut pas être déduit du compte de service
- [ ] Le compte de service dispose des rôles BigQuery Data Viewer + Job User

**Microsoft Fabric** (REQ-989)

- [ ] `pyodbc` + `azure-identity` installés (par défaut)
- [ ] Driver système `msodbcsql18` installé
- [ ] `FABRIC_SQL_SERVER` et `FABRIC_DATABASE` définis
- [ ] Authentification Azure AD : `az login` (dev) ou identité managée / principal de service (prod)
- [ ] `FABRIC_WORKSPACE_ID` défini si utilisation de liens de stockage objet externe

**Azure Synapse** (REQ-989)

- [ ] Mêmes exigences Python + système que Fabric
- [ ] `SYNAPSE_SQL_SERVER` et `SYNAPSE_DATABASE` définis
- [ ] Même configuration d'authentification Azure AD que Fabric

**ClickHouse** (REQ-986)

- [ ] `clickhouse-connect` installé (par défaut)
- [ ] URL du moteur : `clickhouse+http://user:pass@host:8123/database`
- [ ] `secure: "true"` dans `federation_hints` pour TLS (port 8443)

---

## Variables d'environnement

| Variable | Défaut | Objet |
| ---------- | --------- | --------- |
| `PG_PASSWORD` | | Mot de passe PostgreSQL |
| `PROVISA_CONFIG` | `config/provisa.yaml` | Chemin vers le fichier de configuration (REQ-528) |
| `PROVISA_REDIRECT_ENABLED` | `false` | Active la redirection de grands résultats vers S3 (REQ-029, REQ-137) |
| `PROVISA_REDIRECT_THRESHOLD` | `1000` | Seuil de nombre de lignes pour la redirection (REQ-029) |
| `PROVISA_REDIRECT_BUCKET` | `provisa-results` | Bucket S3 (REQ-029) |
| `PROVISA_REDIRECT_ENDPOINT` | | URL de endpoint compatible S3 (REQ-029) |
| `PROVISA_REDIRECT_TTL` | `3600` | TTL de l'URL présignée (secondes) (REQ-141) |
| `REDIS_HOST` | `localhost` | Hôte Redis |
| `REDIS_PORT` | `6379` | Port Redis |
| `REDIS_PASSWORD` | | Mot de passe Redis |
| `REDIS_TLS` | `false` | Active TLS pour Redis |
| `TRINO_HOST` | `localhost` | Hôte du coordinateur du moteur de fédération Trino (REQ-028, REQ-054) |
| `TRINO_PORT` | `8080` | Port HTTP du coordinateur du moteur de fédération Trino (REQ-028, REQ-054) |
| `PROVISA_ENGINE` | `duckdb` | Clé du moteur de fédération actif (REQ-989) ; surcharge la configuration persistée |
| `PROVISA_ENGINE_URL` | | URL de connexion pour les moteurs pilotés par URL (Databricks, Snowflake, ClickHouse, BigQuery, Fabric, Synapse, SQLAlchemy) |
| `PROVISA_MATERIALIZE_URL` | | Surcharge de l'URL du magasin de matérialisation ; par défaut le propre magasin du moteur |
| `PROVISA_MSSQL_ODBC_DRIVER` | `ODBC Driver 18 for SQL Server` | Nom du driver ODBC pour Fabric / Synapse |
| `GOOGLE_APPLICATION_CREDENTIALS` | | Chemin vers la clé JSON de compte de service GCP (BigQuery) |
| `GOOGLE_CLOUD_PROJECT` | | ID du projet GCP (BigQuery ; déduit du compte de service si non défini) |
| `FABRIC_SQL_SERVER` | | Nom d'hôte du endpoint d'analytique SQL Microsoft Fabric |
| `FABRIC_DATABASE` | | Nom de la base de données Fabric |
| `FABRIC_WORKSPACE_ID` | | GUID de l'espace de travail Fabric (requis pour les shortcuts de stockage objet externe) |
| `SYNAPSE_SQL_SERVER` | | Pool SQL dédié Azure Synapse ou nom d'hôte serverless |
| `SYNAPSE_DATABASE` | | Nom de la base de données Synapse |
| `AZURE_TENANT_ID` | | Tenant Azure AD (authentification par principal de service pour Fabric/Synapse) |
| `AZURE_CLIENT_ID` | | ID client d'application Azure AD |
| `AZURE_CLIENT_SECRET` | | Secret client d'application Azure AD |
| `REQUESTS_CA_BUNDLE` | | Chemin de bundle CA personnalisé (connecteur Databricks, proxy TLS de dev) |

---

## Commandes CLI

```bash
provisa start              # Start all services
provisa stop               # Stop all services
provisa restart            # Restart
provisa status             # Show service health
provisa open               # Open the UI in the browser
provisa logs               # Tail service logs
provisa export             # Print current config as YAML to stdout
provisa export FILE        # Write current config as YAML to FILE
provisa import FILE        # Replace running config with YAML from FILE
```

(REQ-224, REQ-164)

### Workflow de promotion de configuration (dev → test → prod)

Tous les réglages spécifiques à l'environnement (chaînes de connexion, secrets, ports) doivent aller dans des variables d'environnement ou des gestionnaires de secrets — pas dans la configuration exportée. Le YAML exporté capture votre modèle sémantique : sources, domaines, rôles, vues. (REQ-164)

```bash
# On dev — export after making changes in the UI
provisa export > config.yaml
git add config.yaml && git commit -m "chore: update semantic model"
git push

# On test/prod — pull and import
git pull
provisa import config.yaml
```
