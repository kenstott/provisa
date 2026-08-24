# Déploiement

## Choisir un chemin de déploiement

Provisa prend en charge six chemins de déploiement. Choisissez selon votre public et votre contexte opérationnel :

| Chemin | Artefact / script | Convient le mieux à |
| ------ | ------------------- | ---------- |
| **Développement** | `start-ui.sh` | Développement depuis les sources, évaluation avec le jeu de démonstration complet |
| **Installeur macOS** | `Provisa-<version>-macOS.dmg` | Postes de développement, évaluation |
| **Installeur Windows** | `Provisa-<version>-windows-x64.exe` | Postes de développement, évaluation |
| **AppImage Linux** | `Provisa.AppImage` | Serveurs sur site, VM cloud, environnements coupés du réseau |
| **VM cloud (AWS)** | `terraform/deploy.sh` | Déploiement cloud multinœud avec répartiteurs de charge |
| **Kubernetes** | `helm/provisa/` | Équipes exploitant déjà K8s |

### VM ou Kubernetes

Les deux sont de qualité entreprise. Le chemin VM/AppImage est plus simple : aucun cluster à provisionner, aucune politique CNI ou RBAC à configurer, et l'AppImage est entièrement autonome (REQ-223). Il s'insère naturellement dans l'outillage de gestion de serveurs existant (Ansible, Puppet, agents Datadog, forwarders Splunk, etc.).

Ne choisissez Kubernetes que si votre équipe exploite déjà un cluster K8s et souhaite que Provisa participe à ce modèle opérationnel (déploiements progressifs, HPA, observabilité unifiée) (REQ-056). Les capacités sont équivalentes — Kubernetes ajoute de la charge opérationnelle, pas des capacités.

### Obtention des images et analyse de sécurité

Tous les chemins de production exigent d'obtenir les artefacts Provisa avant qu'un déploiement puisse s'exécuter. « Coupé du réseau » désigne ce qui se passe au moment de l'installation sur la machine cible — les artefacts doivent d'abord être obtenus.

**Installeurs macOS et Windows :** téléchargez-les depuis la [page des versions GitHub](https://github.com/provisa/provisa/releases). Entièrement empaquetés ; aucun accès Internet requis après le téléchargement (REQ-227). Destinés au développement et à l'évaluation, non à la production — aucune barrière d'analyse d'images n'est attendue.

**Chemin AppImage :** téléchargez depuis la [page des versions GitHub](https://github.com/provisa/provisa/releases) et transférez sur la machine cible. L'AppImage embarque toutes les images de composants sous forme d'archives tar à l'intérieur d'un système de fichiers squashfs (REQ-294) — la plupart des analyseurs de registres ne peuvent pas les inspecter sur place. Contactez votre équipe de compte Provisa pour obtenir les empreintes des images de composants et les vérifier indépendamment avec votre analyseur.

**Chemin Terraform :** l'AppImage doit être téléversée sur S3 avant d'exécuter `terraform/deploy.sh`. Les nœuds EC2 la téléchargent au démarrage via un rôle IAM — ils exigent un accès sortant à S3 (direct ou via un endpoint de passerelle VPC). Appliquez la même politique d'analyse que pour le chemin AppImage.

**Chemin Helm / Kubernetes :** les images individuelles doivent être poussées vers un registre que le cluster peut atteindre. Ce chemin est le plus compatible avec l'analyse fondée sur le registre (Prisma Cloud, Aqua, Trivy, AWS Inspector) — les images sont des objets de première classe que les analyseurs comprennent nativement. Pour les clusters coupés du réseau, mettez les images en miroir sur un registre interne et surchargez les références dans `values.yaml` (REQ-294).

---

## Développement (depuis les sources)

### Recommandé : `start-ui.sh`

La façon la plus simple d'exécuter Provisa depuis les sources. Démarre toute l'infrastructure, l'API du backend et le serveur de développement de l'interface en une seule commande (REQ-055). Ctrl+C arrête tout proprement.

**Prérequis :** Docker Desktop, Node.js, environnement virtuel Python dans `.venv/`

```bash
./start-ui.sh
```

Ce qu'il fait :

- Démarre `docker-compose.core.yml` + `docker-compose.dev.yml` (tous les services de base et de démonstration) et attend leur bonne santé (REQ-055)
- Amorce Kafka avec des données de démonstration
- Synchronise les dépendances Python depuis `.venv/`
- Démarre l'API du backend sur le port 8001 (journaux dans `.logs/server.log`) (REQ-558)
- Démarre le serveur de développement Vite de l'interface sur le port 3000 (REQ-559)
- Affiche les URL et attend ; Ctrl+C arrête tout et démonte compose

```yaml
Backend: http://localhost:8001
UI:      http://localhost:3000
```

**Options :**

`--reset-volumes` — exécute `docker compose down -v` avant de démarrer, détruisant tous les volumes Docker (données PostgreSQL, objets MinIO, état Redis, etc.) (REQ-170). À utiliser lorsque vous voulez repartir de zéro — après un changement de schéma en cours de développement, ou lorsque Docker a planté en laissant des volumes corrompus. **Toutes les données seront perdues.**

`--observability` — ajoute l'instrumentation complète de traçage et de métriques. Télécharge l'agent Java OpenTelemetry et applique un correctif au `jvm.config` de Trino pour le charger, instrumente le backend Provisa avec l'export OTLP, et démarre le collecteur OTel, Prometheus, Tempo et Grafana (`http://localhost:3100`) (REQ-330). Le correctif de `jvm.config` est automatiquement annulé à Ctrl+C.

### Étapes manuelles (backend seul, sans interface)

Si vous n'avez besoin que de l'API :

1. Installez [Docker Desktop](https://docs.docker.com/get-docker/)
2. Démarrez les services de base :

   ```bash
   docker compose -f docker-compose.core.yml up -d
   ```

3. Démarrez l'API :

   ```bash
   uvicorn main:app --reload --port 8001
   ```

4. Vérifiez : `curl http://localhost:8001/health`

### Pile complète (Provisa en conteneur)

Pour exécuter l'API en conteneur plutôt que sur l'hôte :

```bash
docker compose -f docker-compose.core.yml -f docker-compose.app.yml up -d
```

### Services

**Base (`docker-compose.core.yml`) — toujours requis :**

| Service | Port | Rôle |
| --------- | ------ | --------- |
| PostgreSQL | 5432 | Métadonnées de configuration + catalogue Iceberg (REQ-169) |
| PgBouncer | 6432 | Mutualisation des connexions (REQ-053) |
| Moteur de fédération | 8080 | Fédération de requêtes (REQ-028) |
| Redis | 6379 | Cache des résultats de requêtes (REQ-371) |
| MinIO | 9000/9001 | Stockage objet compatible S3 (REQ-029, REQ-171) |

**Démonstration (`docker-compose.dev.yml`) — facultatif, inclus par `start-ui.sh` :**

| Service | Port | Rôle |
| --------- | ------ | --------- |
| MongoDB | 27017 | Source NoSQL de démonstration |
| Kafka | 9092 | Source de flux de démonstration |
| Schema Registry | 8081 | Gestion des schémas Avro/Protobuf de démonstration |
| Debezium | — | Connecteur CDC de démonstration |
| Elasticsearch | 9200 | Source de recherche de démonstration |
| Neo4j | 7474/7687 | Source graphe de démonstration |
| Fuseki | 3030 | Triplestore SPARQL de démonstration |
| Collecteur OpenTelemetry | — | Collecte des traces (avec `--observability`) (REQ-302) |
| Prometheus | 9090 | Métriques (avec `--observability`) (REQ-330) |
| Tempo | — | Stockage des traces (avec `--observability`) (REQ-330) |
| Grafana | 3100 | Tableaux de bord (avec `--observability`) (REQ-330) |

### Backend de télémétrie (`otlp2sql`)

La pile `--observability` ci-dessus (collecteur → Tempo/Prometheus/Grafana) est un
chemin de télémétrie. L'autre est `otlp2sql` (`provisa.observability.otlp2sql`) : un
récepteur OTLP/HTTP qui écrit traces, métriques et journaux dans une base SQL
choisie par une URL SQLAlchemy, en extrayant les attributs de span `provisa.*` à
l'ingestion, si bien qu'aucune tâche de compactage séparée ne s'exécute. Les écritures
sont groupées (`OTLP2SQL_BATCH_MAX_ROWS`, 1000 par défaut ; `OTLP2SQL_BATCH_MAX_SECS`, 2 s par défaut).

La télémétrie dispose de son propre magasin, distinct de la base du plan de contrôle. Sélectionnez
le backend avec `PROVISA_OPS_DB_URL` :

| `PROVISA_OPS_DB_URL` | Backend | Remarques |
| --- | --- | --- |
| *(non défini)* | DuckDB dédié sous `~/.provisa/telemetry/` | par défaut ; sans serveur, sans Docker |
| `clickhouse+native://user@host/otel` | ClickHouse | ingestion à haut débit avec fusions d'arrière-plan automatiques |
| `postgresql+psycopg2://user@host/otel` | PostgreSQL | volume modéré |
| `trino://user@host:8080/otel` | Trino / Iceberg | fonctionne techniquement, **non recommandé** — voir ci-dessous |

**À propos de `trino://` :** le dialecte SQLAlchemy Trino émet du DDL et des
`INSERT` Trino valides, ce qui le rend techniquement viable comme backend `otlp2sql`. Il n'est pas
recommandé au-delà de faibles débits d'ingestion. Chaque vidage de lot devient un
`INSERT` Trino distribué assorti d'un instantané Iceberg, si bien qu'une télémétrie à haut débit
produit quantité de petits fichiers et d'instantanés et exige toujours des
`ALTER TABLE ... EXECUTE optimize` / `expire_snapshots` périodiques — qu'`otlp2sql`
n'exécute pas. Cela place en outre le moteur de requêtes sur le chemin critique de l'ingestion.

Pour une télémétrie à fort volume vers Trino/Iceberg, utilisez plutôt `otlp2parquet` : il
écrit du parquet sur du stockage objet sans passer par Trino, et un compactage Trino
planifié replie les fichiers bruts dans les tables Iceberg vivantes. Pour un moteur
unique qui gère à la fois l'ingestion à haut débit et le compactage, préférez ClickHouse.

Pointez les exportateurs OTLP de l'application et de Trino (`OTEL_EXPORTER_OTLP_ENDPOINT`) vers
l'endpoint `otlp2sql`, et enregistrez le domaine ops contre le même
`PROVISA_OPS_DB_URL` pour qu'il lise ce que le récepteur a écrit.

---

## Installeur macOS

Pour les postes de développement et l'évaluation. Entièrement coupé du réseau — aucun accès Internet requis après le téléchargement (REQ-227).

L'installeur de base réalise une **installation native** : moteur de fédération DuckDB + plan de contrôle SQLite + cache en mémoire (fakeredis), sans Docker, VM, Trino, Redis ni MinIO (REQ-972, REQ-979). Le moteur de fédération est un choix de l'assistant — DuckDB (natif, par défaut), Trino sur Docker, ou un moteur externe (REQ-973). L'observabilité est une autotélémétrie toujours active, consultable dans l'administration ; la pile collecteur/Prometheus/Grafana sur Docker est une démonstration externe facultative, non un interrupteur (REQ-975). Le pack de données de démonstration est facultatif et désactivé par défaut (REQ-978). Trino, la pile d'observabilité Docker et la démonstration sont de lourds compléments résolus en priorité localement (répertoire voisin de l'installeur, volumes montés, `~/Downloads`, puis version GitHub), afin que les entreprises puissent préinstaller les archives pour des installations coupées du réseau (REQ-977).

### Étapes

1. Téléchargez `Provisa-<version>-macOS.dmg` depuis la [page des versions GitHub](https://github.com/provisa/provisa/releases)
2. Ouvrez le DMG et faites glisser **Provisa.app** dans `/Applications`
3. Double-cliquez sur **Provisa.app** — la configuration de premier lancement s'exécute une fois ; l'assistant propose les choix de moteur, d'observabilité et de démonstration ci-dessus (REQ-1007)
4. Ouvrez le Terminal :

   ```bash
   provisa start    # start all services
   provisa status   # confirm all services are running
   provisa open     # open the UI in the browser
   ```

   (REQ-224)

### Persistance des données

Toutes les données sont stockées dans `~/.provisa/` (REQ-224). Pour tout supprimer : `provisa uninstall`.

---

## Installeur Windows

Pour les postes de développement et l'évaluation. Entièrement coupé du réseau — aucun accès Internet requis après le téléchargement (REQ-227).

Comme sur macOS, l'installeur Windows de base constitue un **niveau natif** : un runtime Python autonome + le wheel provisa + DuckDB/pg_duckdb + un plan de contrôle SQLite, sans livrer ni Docker, ni VM, ni images de conteneurs (REQ-979). Le moteur de fédération (Trino), la pile d'observabilité et le pack de données de démonstration s'ajoutent ensuite via des installeurs en couches distincts, dans cet ordre : l'installeur Container (`Provisa-Container-<version>.exe`, qui ajoute WSL2 + containerd + Trino), puis l'installeur Obs (qui exige le niveau conteneur), puis l'installeur Demo (qui exige Core + Obs). Les indications de premier lancement expliquent comment initialiser le moteur de fédération en exécutant l'installeur Container (REQ-1005).

### Étapes

1. Téléchargez `Provisa-<version>-windows-x64.exe` depuis la [page des versions GitHub](https://github.com/provisa/provisa/releases)
2. Exécutez l'installeur — aucun droit d'administration requis ; installe dans `%LOCALAPPDATA%\Programs\Provisa\`
3. Ouvrez **Provisa First Launch** depuis le menu Démarrer — la configuration native s'exécute une fois et affiche les indications d'étapes suivantes pour les compléments en couches (REQ-1005)
4. Ouvrez un nouveau terminal :

   ```text
   provisa status
   provisa open
   ```

   (REQ-224)

### Persistance des données

Toutes les données sont stockées dans `%USERPROFILE%\.provisa\`.

---

## AppImage Linux — VM mononœud ou multinœud

### De quoi il s'agit

`Provisa.AppImage` est un exécutable autonome unique qui embarque (REQ-223, REQ-228) :

- Un démon Docker sans privilèges (`dockerd-rootless.sh` + `rootlesskit`) — sans Docker système ni droits root
- Toutes les archives tar d'images de conteneurs (PostgreSQL, PgBouncer, MinIO, Redis, moteur de fédération, API Provisa) (REQ-294)
- L'enveloppe de la CLI Provisa et le script de configuration de premier lancement

L'image Provisa est préconstruite au moment de l'empaquetage — le code source Python n'y figure jamais.

### Quand l'utiliser

- Serveur physique ou VM sur site (un nœud ou plusieurs)
- VM cloud sans cluster K8s
- Environnements coupés du réseau (REQ-294)
- Lorsque vous voulez une exploitation plus simple que Kubernetes

---

### Étapes — un seul nœud

1. Téléchargez `Provisa.AppImage` depuis la [page des versions GitHub](https://github.com/provisa/provisa/releases) et transférez-la sur la machine cible
2. Rendez-la exécutable :

   ```bash
   chmod +x Provisa.AppImage
   ```

3. Lancez la configuration de premier lancement :

   ```bash
   ./Provisa.AppImage
   ```

4. L'assistant de configuration demande :
   - **Rôle** → sélectionnez `primary`
   - **Budget de RAM** → quantité de RAM à allouer (0 = tout le disponible) ; détermine le nombre de workers Trino
   - **Nom d'hôte** → l'adresse annoncée de ce nœud
   - **Port de l'API** → `8000` par défaut (REQ-560)
5. La configuration charge toutes les images de conteneurs (~2–5 minutes), écrit la configuration et démarre les services
6. Vérifiez :

   ```bash
   provisa status
   curl http://localhost:8000/health
   ```

---

### Étapes — multinœud (nœud primaire)

Exécutez ces étapes sur le nœud primaire d'abord. Les nœuds secondaires doivent être configurés une fois le primaire en fonctionnement.

1. Téléchargez et transférez `Provisa.AppImage` sur la machine primaire
2. Ouvrez les ports de pare-feu requis (les secondaires s'y connecteront en entrée) :

   | Port | Service |
   | ------ | --------- |
   | 5432 | PostgreSQL |
   | 6379 | Redis |
   | 9000 | MinIO |
   | 8080 | Coordinateur du moteur de fédération |
   | 8000 | API Provisa |

3. Rendez-la exécutable et lancez-la :

   ```bash
   chmod +x Provisa.AppImage
   ./Provisa.AppImage
   ```

4. L'assistant de configuration demande :
   - **Rôle** → sélectionnez `primary`
   - **Budget de RAM**, **nom d'hôte**, **port de l'API** → répondez comme pour un nœud unique
5. Une fois la configuration terminée, notez l'**IP privée** de cette machine — les secondaires en ont besoin
6. L'assistant affiche un bloc upstream nginx — conservez-le pour la configuration de votre répartiteur de charge
7. Vérifiez :

   ```bash
   provisa status
   curl http://localhost:8000/health
   ```

---

### Étapes — multinœud (chaque nœud secondaire)

Répétez ces étapes sur chaque nœud supplémentaire une fois le primaire en fonctionnement et joignable.

1. Téléchargez et transférez `Provisa.AppImage` sur la machine secondaire
2. Vérifiez que le secondaire peut joindre le primaire :

   ```bash
   curl http://<primary-ip>:8000/health
   ```

3. Rendez-la exécutable et lancez-la :

   ```bash
   chmod +x Provisa.AppImage
   ./Provisa.AppImage
   ```

4. L'assistant de configuration demande :
   - **Rôle** → sélectionnez `secondary`
   - **IP du primaire** → saisissez l'IP du nœud primaire (la connectivité est vérifiée en direct)
   - **Budget de RAM**, **nom d'hôte**, **port de l'API** → répondez comme ci-dessus
5. La configuration charge un jeu d'images réduit (ni PostgreSQL, ni PgBouncer, ni MinIO, ni Redis — ceux-ci ne tournent que sur le primaire) (REQ-561), démarre l'API Provisa et un worker du moteur de fédération
6. Vérifiez :

   ```bash
   provisa status
   curl http://localhost:8000/health
   ```

7. Ajoutez ce nœud à l'upstream de votre répartiteur de charge

---

### Topologie primaire / secondaire

Le **nœud primaire** exécute tous les services singletons :

| Service | Pourquoi singleton |
| --------- | --------------- |
| PostgreSQL | Schéma partagé, configuration applicative, modèle sémantique |
| Redis | Cache partagé des résultats de requêtes et état des abonnements (REQ-371) |
| MinIO | Magasin d'objets partagé pour les résultats redirigés et les instantanés de vues matérialisées (REQ-029) |
| Coordinateur du moteur de fédération | Tous les workers (primaire + secondaires) s'y enregistrent (REQ-028) |

Les **nœuds secondaires** n'exécutent que :

- L'API Provisa — sans état ; lit toute sa configuration dans le PostgreSQL du primaire au démarrage (REQ-057, REQ-562)
- Un worker du moteur de fédération — s'enregistre lui-même auprès du coordinateur sur le primaire (REQ-028)

Tout l'état applicatif transite par le PostgreSQL du primaire. Aucune synchronisation manuelle requise. (REQ-562)

---

### Premier lancement non interactif (automatisé)

Pour Terraform, cloud-init ou Ansible — passez des options au lieu de répondre aux invites :

```bash
# Primary
./Provisa.AppImage --non-interactive --role primary --ram-gb 32

# Secondary
./Provisa.AppImage --non-interactive --role secondary --primary-ip 10.0.0.10 --ram-gb 32
```

Le mode non interactif installe une unité systemd (`/etc/systemd/system/provisa.service`) pour le démarrage à l'amorçage. (REQ-563)

| Option | Description |
| ------ | ------------- |
| `--non-interactive` | Passer toutes les invites ; installer l'unité systemd |
| `--role primary\|secondary` | Rôle du nœud |
| `--primary-ip <ip>` | IP du nœud primaire (requise pour un secondaire) |
| `--ram-gb <n>` | RAM à allouer (0 = tout le disponible) |

---

## Déploiement sur VM cloud — Terraform (AWS)

Provisionne un cluster Provisa multinœud complet sur AWS — VPC, groupes de sécurité, instances EC2, ALB, NLB — en une seule commande interactive. (REQ-564)

### Fichiers

| Fichier | Rôle |
| ------ | --------- |
| `terraform/deploy.sh` | Enveloppe interactive — recueille les paramètres, valide les identifiants, écrit `terraform.tfvars`, exécute apply |
| `terraform/aws/variables.tf` | Toutes les définitions de variables avec leurs valeurs par défaut |
| `terraform/aws/main.tf` | VPC, sous-réseaux, groupes de sécurité, IAM, EC2, ALB, NLB |
| `terraform/aws/outputs.tf` | URL des endpoints et IP des nœuds |

### Étapes

1. Téléchargez `Provisa.AppImage` depuis la [page des versions GitHub](https://github.com/provisa/provisa/releases)

2. Téléversez-la vers un bucket S3 de votre compte AWS :

   ```bash
   aws s3 cp Provisa.AppImage s3://<your-bucket>/releases/Provisa.AppImage
   ```

3. Assurez-vous que des identifiants AWS sont disponibles dans votre shell (au choix) :
   - Variables d'environnement : `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY`
   - Profil nommé : `export AWS_PROFILE=my-profile`
   - Session SSO active : `aws sso login`

4. (Facultatif) Si vous voulez un accès SSH aux nœuds, créez une paire de clés EC2 dans votre région cible et notez le nom de la paire

5. Exécutez l'enveloppe de déploiement :

   ```bash
   bash terraform/deploy.sh
   ```

6. Répondez aux questions de l'assistant (voir le tableau de référence ci-dessous). Le script vérifie que l'AppImage existe bien sur S3 avant de poursuivre et s'interrompt sinon

7. Relisez le récapitulatif de déploiement et confirmez

8. Terraform provisionne toute l'infrastructure (~5–10 minutes). Après apply, le script affiche :

   ```text
   api_endpoint      = "http://<alb-dns>:8000"
   flight_endpoint   = "<nlb-dns>:8815"
   primary_ip        = "10.0.x.x"
   secondary_ips     = ["10.0.x.x", ...]
   ```

   (REQ-564, REQ-143)

9. (Facultatif) Faites pointer des enregistrements DNS vers les noms DNS de l'ALB et du NLB

10. Vérifiez :

    ```bash
    curl http://<api_endpoint>/health
    ```

### Questions de l'assistant

| Question | Valeur par défaut | Remarques |
| ---------- | --------- | ------- |
| Fournisseur cloud | — | AWS uniquement aujourd'hui |
| Identifiants AWS | — | Cherche d'abord une session active |
| Région | `us-east-1` | |
| Nombre de nœuds | `2` | 1 = primaire seul, sans répartiteur ; 2+ = primaire + secondaires + ALB/NLB |
| Type d'instance | `m7i.2xlarge` | Voir le guide de dimensionnement ci-dessous |
| Taille du volume racine | `100 GB` | Par nœud |
| Budget de RAM | `0` (toute la RAM) | Détermine le nombre de workers Trino par nœud |
| Bucket S3 | — | Vérifié en direct avant de poursuivre |
| Clé S3 | `releases/Provisa.AppImage` | |
| Accès SSH | Non | Exige un nom de paire de clés existante + un CIDR d'administration |
| CIDR du VPC | `10.0.0.0/16` | |

### Guide de dimensionnement des instances

| Type | vCPU | RAM | Workers Trino / nœud | Cas d'usage |
| ------ | ------ | ----- | -------------------- | ---------- |
| `m7i.xlarge` | 4 | 16 Go | 0 | Développement / petits jeux de données |
| `m7i.2xlarge` | 8 | 32 Go | 1 | Petite production |
| `m7i.4xlarge` | 16 | 64 Go | 2 | Production moyenne |
| `m7i.8xlarge` | 32 | 128 Go | 4 | Grande production |

Tous les nœuds fournissent des workers à un unique coordinateur sur le primaire (REQ-028). Un cluster de 3 nœuds `m7i.4xlarge` donne 6 workers Trino au total.

### Ce qui est provisionné

- Un VPC avec deux sous-réseaux publics répartis sur deux zones de disponibilité (REQ-564)
- Des groupes de sécurité : groupe des répartiteurs (entrée publique sur 8000/8815), groupe des nœuds (répartiteur → nœuds, intra-cluster, SSH facultatif)
- Un rôle IAM + profil d'instance avec S3 GetObject sur le bucket de l'AppImage
- L'instance EC2 primaire — exécute le premier lancement en mode `--non-interactive --role primary`
- Les instances EC2 secondaires (node_count − 1) — exécutent le premier lancement en mode `--non-interactive --role secondary --primary-ip <primary private IP>` ; dépendent de l'achèvement du primaire
- Un ALB sur le port 8000 — API HTTP, vérifications de santé sur `/health` (REQ-560)
- Un NLB sur le port 8815 — Arrow Flight / gRPC (REQ-143)
- Les deux répartiteurs sont rattachés à tous les nœuds

### Liste de vérification des prérequis

- [ ] Permissions IAM : EC2 complet, ELB complet, VPC complet, création de rôle IAM, S3 GetObject sur le bucket de l'AppImage
- [ ] `Provisa.AppImage` téléversée sur S3
- [ ] Les nœuds EC2 disposent d'un accès sortant à S3 (Internet direct ou endpoint de passerelle VPC S3)
- [ ] Une paire de clés EC2 existe dans la région cible (si SSH est nécessaire)
- [ ] Terraform ≥ 1.5 installé localement
- [ ] Enregistrements DNS prévus pour l'ALB / le NLB (facultatif mais recommandé)
- [ ] Certificat ACM prêt si HTTPS est requis (non inclus dans le Terraform de base)

### Secrets

Aucun secret n'est intégré à Terraform. L'AppImage génère les identifiants lors du premier lancement et les écrit dans `~/.provisa/config.yaml` sur chaque nœud (REQ-563). En production, récupérez le jeton d'administration depuis le nœud primaire après le déploiement :

```bash
ssh ubuntu@<primary-public-ip> cat ~/.provisa/config.yaml | grep admin_token
```

---

## Kubernetes / Helm

### Quand l'utiliser

Votre équipe exploite déjà un cluster Kubernetes et souhaite que Provisa participe à ce modèle opérationnel (REQ-056). Si vous évaluez Provisa ou déployez sur site sans cluster existant, le chemin AppImage est plus simple.

Remarque : l'AppImage Provisa ne peut pas s'exécuter dans un pod Kubernetes — elle exige FUSE et un démon Docker sans privilèges, indisponibles dans les profils de sécurité de pods standards.

### Étapes

1. Vérifiez l'accès au cluster :

   ```bash
   kubectl cluster-info
   ```

2. Récupérez et mettez en miroir les images sur votre registre interne (requis pour les environnements coupés du réseau ou soumis à analyse ; à passer si vous tirez directement des registres publics) (REQ-294) :

   | Image | Sert à |
   | ------- | ---------- |
   | `provisa/provisa:<version>` | L'API Provisa |
   | `trinodb/trino:480` | Coordinateur et workers du moteur de fédération (REQ-169) |
   | `postgres:16` | PostgreSQL dans le cluster (si `postgresql.enabled`) (REQ-169) |
   | `edoburu/pgbouncer:latest` | PgBouncer dans le cluster (si `pgbouncer.enabled`) (REQ-053) |
   | `redis:7.2` | Redis dans le cluster (si `redis.enabled` et pas de `redis.host`) (REQ-371) |
   | `minio/minio:latest` | MinIO dans le cluster (si `minio.enabled`) (REQ-029) |

   Pour les environnements à registre analysé :
   - Poussez chaque image vers votre registre de préproduction
   - Exécutez votre analyseur (Prisma Cloud, Aqua, Trivy, AWS Inspector) et obtenez l'approbation
   - Promouvez vers votre registre interne de production

3. Décidez avant d'installer :
   - **PostgreSQL** — dans le cluster (`postgresql.enabled: true`) ou managé à l'extérieur (`postgresql.host`) ? L'externe est recommandé en production
   - **Redis** — dans le cluster ou externe (`redis.host`) ? Changez le mot de passe par défaut (`redis.password`)
   - **MinIO / S3** — MinIO dans le cluster ou S3 natif ? Sur AWS, utilisez S3 avec un rôle IAM
   - **Secrets** — passez-les via `--set` pour l'évaluation ; utilisez External Secrets ou Vault Agent en production

4. Installez le chart :

   ```bash
   helm install provisa helm/provisa/ \
     --set config.pgPassword=<password> \
     --set config.adminToken=<token> \
     --set s3.endpoint=https://s3.amazonaws.com \
     --set s3.bucket=my-provisa-results \
     --namespace provisa --create-namespace
   ```

   Si vous utilisez un registre interne, ajoutez les surcharges d'images :

   ```bash
   --set image.repository=harbor.internal.example.com/provisa/provisa \
   --set image.tag=1.2.3 \
   --set trino.image.repository=harbor.internal.example.com/trinodb/trino \
   --set trino.image.tag=480
   ```

5. Vérifiez que les pods tournent :

   ```bash
   kubectl get pods -n provisa
   ```

6. Contrôlez l'API :

   ```bash
   kubectl port-forward svc/provisa 8000:8000 -n provisa
   curl http://localhost:8000/health
   ```

7. (Facultatif) Activez l'ingress pour un accès externe — définissez `ingress.enabled: true` et configurez votre contrôleur d'ingress

### Liste de vérification des prérequis

- [ ] Kubernetes 1.26+, Helm 3.12+
- [ ] Classe de stockage prenant en charge les PVC `ReadWriteOnce` (pour les services à état dans le cluster)
- [ ] Images disponibles pour le cluster (registre public ou interne)
- [ ] Endpoint PostgreSQL + identifiants (si externe)
- [ ] Endpoint Redis + identifiants (si externe)
- [ ] Bucket S3 + identifiants ou rôle IAM
- [ ] Jeton d'administration choisi
- [ ] Contrôleur d'ingress configuré (si un accès externe est nécessaire)

### Valeurs principales

| Valeur | Valeur par défaut | Description |
| ------- | --------- | ------------- |
| `replicaCount` | `2` | Réplicas de l'API Provisa (sans état) (REQ-057) |
| `config.pgHost` | `postgres` | Hôte PostgreSQL |
| `config.pgPassword` | | Mot de passe PostgreSQL |
| `config.adminToken` | | Jeton bearer de l'API d'administration |
| `redis.enabled` | `true` | Déployer un StatefulSet Redis dans le cluster (REQ-371) |
| `redis.host` | `""` | À définir pour utiliser un Redis externe |
| `redis.port` | `6379` | |
| `redis.password` | `"provisa"` | À changer |
| `redis.tls` | `false` | |
| `trino.enabled` | `true` | Déployer le moteur de fédération (REQ-028) |
| `trino.workers` | `2` | Réplicas de workers du moteur de fédération (REQ-056) |
| `postgresql.enabled` | `true` | Déployer PostgreSQL dans le cluster (REQ-169) |
| `postgresql.host` | `""` | À définir pour utiliser un PostgreSQL externe |
| `minio.enabled` | `true` | Déployer MinIO dans le cluster (REQ-029) |
| `s3.endpoint` | | URL de l'endpoint compatible S3 |
| `s3.bucket` | `provisa-results` | Bucket pour la redirection des grands résultats (REQ-029, REQ-137) |
| `ingress.enabled` | `false` | Activer l'ingress |

### Mise à l'échelle

```bash
kubectl scale deployment/provisa --replicas=5 --namespace provisa
```

Les workers du moteur de fédération se dimensionnent indépendamment — davantage de workers augmentent le débit et la capacité de requêtes concurrentes (REQ-056). (REQ-057)

### Mettre à jour la configuration

```bash
kubectl create configmap provisa-config \
  --from-file=config.yaml=./config.yaml \
  --namespace provisa --dry-run=client -o yaml | kubectl apply -f -
kubectl rollout restart deployment/provisa --namespace provisa
```

---

## Haute disponibilité et reprise

Provisa applique un modèle de reprise à deux niveaux à tous les modes de déploiement (REQ-703) :

- **Niveau 1 — erreurs transitoires.** Les opérations de lecture sont réessayées pendant jusqu'à 30 secondes sur erreur transitoire, avec un repli exponentiel à gigue complète. Réglez le budget avec `PROVISA_RETRY_BUDGET_SECS`. Les opérations d'écriture ne sont jamais réessayées en interne, et les erreurs de mémoire ne sont jamais réessayables.
- **Niveau 2 — défaillance d'un composant.** Un surveillant de moteur interne détecte et redémarre les composants logiciels défaillants en 2 à 3 minutes.

Les défaillances au niveau de la machine et du cluster restent de la responsabilité de l'exploitant — provisionnez des nœuds redondants et un répartiteur de charge (chemins Terraform et Helm ci-dessus) pour tolérer la perte d'un nœud.

## Dépendances du moteur de fédération

Les moteurs de fédération d'entrepôts exigent des paquets Python et des composants système au-delà de l'installation Provisa par défaut. Tous les paquets Python listés ici sont déclarés dans `pyproject.toml` et installés dans le cadre du `pip install provisa` ou `pip install -e .` standard [tool-verified: `pyproject.toml` lines 44–52].

Les paquets Python sont livrés avec l'installation Provisa par défaut — aucun extra facultatif n'est requis pour un quelconque moteur d'entrepôt. Les éléments système (pilote ODBC, CLI cloud, clés de comptes de service) doivent être installés séparément.

### Paquets Python (déjà dans les dépendances de base)

[tool-verified: `pyproject.toml` lines 41–52]

| Paquet | Moteur | Rôle |
| ------- | ------ | ------- |
| `databricks-sql-connector` | Databricks | Connexion à l'entrepôt SQL ; Arrow Cloud Fetch (REQ-987) |
| `snowflake-connector-python[pandas]` | Snowflake | Connexion + `fetch_arrow_table` natif Arrow (REQ-988) |
| `google-cloud-bigquery` | BigQuery | Exécution des requêtes |
| `google-cloud-bigquery-storage` | BigQuery | Storage Read API pour des lectures natives Arrow |
| `google-cloud-storage` | BigQuery | Préparation GCS pour les liens de tables externes |
| `pyodbc` | Fabric, Synapse | Connexion ODBC aux endpoints T-SQL |
| `azure-identity` | Fabric, Synapse | Jeton Azure AD via `DefaultAzureCredential` |
| `clickhouse-connect` | ClickHouse | Lectures colonnaires HTTP |
| `protobuf>=6.33.5,<7` | BigQuery, gRPC | Épinglage de compatibilité — `google-cloud-*` et OTel partagent un runtime protobuf ; `<7` les maintient alignés |
| `grpcio-status<1.82` | gRPC | S'aligne sur l'épinglage `protobuf<7` |

### Exigences au niveau du système

Ce ne sont pas des paquets Python — ils doivent être installés sur l'hôte ou le conteneur qui exécute Provisa.

**Microsoft Fabric et Azure Synapse (ODBC)**

`pyodbc` se connecte via le pilote Microsoft ODBC pour SQL Server (`msodbcsql18`). Le pilote doit être installé sur l'hôte — pas via pip. [tool-verified: `mssql_warehouse_runtime.py` line 84 `"ODBC Driver 18 for SQL Server"` default]

macOS :

```bash
brew install microsoft/mssql-release/msodbcsql18
```

Linux (Ubuntu/Debian) :

```bash
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql18
```

Provisa détecte le pilote automatiquement. Pour surcharger le nom du pilote (installations non standards), définissez :

```bash
export PROVISA_MSSQL_ODBC_DRIVER="ODBC Driver 17 for SQL Server"
```

**Authentification Azure AD (Fabric et Synapse)**

Les deux moteurs s'authentifient via `azure.identity.DefaultAzureCredential` [tool-verified: `mssql_warehouse_runtime.py:79`, `fabric_shortcuts.py:46`]. `DefaultAzureCredential` examine les sources d'identifiants dans l'ordre : variables d'environnement, identité de charge de travail, identité managée, VS Code, `az login`, et d'autres.

Pour le développement local, `az login` est le chemin le plus simple :

```bash
az login
```

En production, utilisez une identité managée (sur des VM Azure ou AKS) — aucune gestion d'identifiants nécessaire. Pour une authentification par principal de service, définissez :

```bash
export AZURE_TENANT_ID=<tenant>
export AZURE_CLIENT_ID=<app-id>
export AZURE_CLIENT_SECRET=<secret>
```

**BigQuery (compte de service)**

`google-cloud-bigquery` utilise les Application Default Credentials. Pour le développement local, pointez vers un fichier de clé de compte de service :

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa-key.json
```

En production sur GCP (Cloud Run, GKE avec Workload Identity, Compute Engine), la bibliothèque détecte automatiquement le compte de service attaché — aucune variable d'environnement nécessaire.

Le compte de service a besoin de :

- `roles/bigquery.dataViewer` — lire les données
- `roles/bigquery.jobUser` — exécuter des requêtes
- `roles/bigquery.dataEditor` — créer des tables externes (pour ATTACH)
- `roles/storage.objectViewer` — lire les objets GCS pour les tables externes

**Databricks (certificat d'autorité en environnement à proxy de développement)**

Si Provisa s'exécute derrière un proxy interceptant le TLS (Charles, mitmproxy, proxys d'entreprise), le connecteur SQL Databricks peut rejeter le certificat du proxy. Fournissez un ensemble d'autorités personnalisé :

```bash
export REQUESTS_CA_BUNDLE=/path/to/your/proxy-ca.pem
```

Le connecteur Databricks en hérite depuis `requests` — aucune variable d'environnement propre à Databricks n'est nécessaire.

### Liste de vérification par moteur

**Databricks** (REQ-987)

- [ ] `databricks-sql-connector` installé (par défaut)
- [ ] URL de moteur avec `http_path` : `databricks://token:TOKEN@workspace.azuredatabricks.net?http_path=/sql/1.0/warehouses/xxx`
- [ ] Jeton d'accès personnel ou jeton de principal de service
- [ ] `REQUESTS_CA_BUNDLE` défini si derrière un proxy interceptant le TLS

**Snowflake** (REQ-988)

- [ ] `snowflake-connector-python[pandas]` installé (par défaut)
- [ ] URL de moteur : `snowflake://user:pass@account.snowflakecomputing.com/database`
- [ ] `account` dans `PROVISA_ENGINE_URL` ou `federation_hints`

**BigQuery** (REQ-989)

- [ ] `google-cloud-bigquery`, `google-cloud-bigquery-storage`, `google-cloud-storage` installés (par défaut)
- [ ] `GOOGLE_APPLICATION_CREDENTIALS` défini (développement) ou identité de charge de travail configurée (production)
- [ ] `GOOGLE_CLOUD_PROJECT` défini si le projet ne peut pas être déduit du compte de service
- [ ] Le compte de service détient les rôles BigQuery Data Viewer + Job User

**Microsoft Fabric** (REQ-989)

- [ ] `pyodbc` + `azure-identity` installés (par défaut)
- [ ] Pilote système `msodbcsql18` installé
- [ ] `FABRIC_SQL_SERVER` et `FABRIC_DATABASE` définis
- [ ] Authentification Azure AD : `az login` (développement) ou identité managée / principal de service (production)
- [ ] `FABRIC_WORKSPACE_ID` défini en cas d'utilisation de liens vers un stockage objet externe

**Azure Synapse** (REQ-989)

- [ ] Mêmes exigences Python et système que Fabric
- [ ] `SYNAPSE_SQL_SERVER` et `SYNAPSE_DATABASE` définis
- [ ] Même configuration d'authentification Azure AD que Fabric

**ClickHouse** (REQ-986)

- [ ] `clickhouse-connect` installé (par défaut)
- [ ] URL de moteur : `clickhouse+http://user:pass@host:8123/database`
- [ ] `secure: "true"` dans `federation_hints` pour le TLS (port 8443)

---

## Variables d'environnement

| Variable | Valeur par défaut | Rôle |
| ---------- | --------- | --------- |
| `PG_PASSWORD` | | Mot de passe PostgreSQL |
| `PROVISA_CONFIG` | `config/provisa.yaml` | Chemin du fichier de configuration (REQ-528) |
| `PROVISA_REDIRECT_ENABLED` | `false` | Activer la redirection des grands résultats vers S3 (REQ-029, REQ-137) |
| `PROVISA_REDIRECT_THRESHOLD` | `1000` | Seuil de nombre de lignes déclenchant la redirection (REQ-029) |
| `PROVISA_REDIRECT_BUCKET` | `provisa-results` | Bucket S3 (REQ-029) |
| `PROVISA_REDIRECT_ENDPOINT` | | URL de l'endpoint compatible S3 (REQ-029) |
| `PROVISA_REDIRECT_TTL` | `3600` | TTL des URL présignées (secondes) (REQ-141) |
| `REDIS_HOST` | `localhost` | Hôte Redis |
| `REDIS_PORT` | `6379` | Port Redis |
| `REDIS_PASSWORD` | | Mot de passe Redis |
| `REDIS_TLS` | `false` | Activer le TLS pour Redis |
| `TRINO_HOST` | `localhost` | Hôte du coordinateur du moteur de fédération Trino (REQ-028, REQ-054) |
| `TRINO_PORT` | `8080` | Port HTTP du coordinateur du moteur de fédération Trino (REQ-028, REQ-054) |
| `PROVISA_ENGINE` | `duckdb` | Clé du moteur de fédération actif (REQ-989) ; prime sur la configuration persistée |
| `PROVISA_ENGINE_URL` | | URL de connexion pour les moteurs pilotés par URL (Databricks, Snowflake, ClickHouse, BigQuery, Fabric, Synapse, SQLAlchemy) |
| `PROVISA_MATERIALIZE_URL` | | Surcharge de l'URL du magasin de matérialisation ; par défaut, le magasin propre au moteur |
| `PROVISA_MSSQL_ODBC_DRIVER` | `ODBC Driver 18 for SQL Server` | Nom du pilote ODBC pour Fabric / Synapse |
| `GOOGLE_APPLICATION_CREDENTIALS` | | Chemin du JSON de clé de compte de service GCP (BigQuery) |
| `GOOGLE_CLOUD_PROJECT` | | Identifiant de projet GCP (BigQuery ; déduit du compte de service s'il n'est pas défini) |
| `FABRIC_SQL_SERVER` | | Nom d'hôte de l'endpoint SQL analytics Microsoft Fabric |
| `FABRIC_DATABASE` | | Nom de la base Fabric |
| `FABRIC_WORKSPACE_ID` | | GUID de l'espace de travail Fabric (requis pour les raccourcis vers un stockage objet externe) |
| `SYNAPSE_SQL_SERVER` | | Nom d'hôte du pool SQL dédié ou serverless Azure Synapse |
| `SYNAPSE_DATABASE` | | Nom de la base Synapse |
| `AZURE_TENANT_ID` | | Locataire Azure AD (authentification par principal de service pour Fabric/Synapse) |
| `AZURE_CLIENT_ID` | | Identifiant client de l'application Azure AD |
| `AZURE_CLIENT_SECRET` | | Secret client de l'application Azure AD |
| `REQUESTS_CA_BUNDLE` | | Chemin d'un ensemble d'autorités personnalisé (connecteur Databricks, proxy TLS de développement) |

---

## Commandes de la CLI

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

### Flux de promotion de la configuration (dev → test → prod)

Tous les paramètres propres à un environnement (chaînes de connexion, secrets, ports) relèvent des variables d'environnement ou des gestionnaires de secrets — non de la configuration exportée. Le YAML exporté capture votre modèle sémantique : sources, domaines, rôles, vues. (REQ-164)

```bash
# On dev — export after making changes in the UI
provisa export > config.yaml
git add config.yaml && git commit -m "chore: update semantic model"
git push

# On test/prod — pull and import
git pull
provisa import config.yaml
```


Voir aussi : [Environnements](environments.md) explique comment gérer des copies nommées et isolées par schéma de votre modèle gouverné.
