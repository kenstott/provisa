# Référence CLI

La commande `provisa` est le point d'entrée unique du palier embarqué installé via pip (REQ-1128).
Elle démarre le runtime, gère les licences, déclenche la publication de métadonnées, déploie des modèles, et
contrôle la bannière de maintenance — sans Docker, Node, ni aucun service externe.

Installez-la avec l'extra `embedded`, qui apporte également les extensions DuckDB hors ligne
et le plan de contrôle PostgreSQL embarqué :

```bash
pip install 'provisa[embedded]'
```

**Exigences de plateforme.** `provisa run` requiert Python 3.12 et une plateforme disposant d'une roue (wheel)
pgserver : linux x86_64, macOS ou Windows x86_64. Linux aarch64 n'a ni roue pgserver ni distribution
source, donc le palier embarqué ne s'y exécute pas. Utilisez le palier conteneur sur aarch64.
[tool-verified: `_require_supported_interpreter()` at cli.py:513-546]

---

## Options partagées

Plusieurs sous-commandes appellent l'API HTTP de Provisa. Elles partagent trois indicateurs et deux variables
d'environnement. [tool-verified: `_api_call()` at cli.py:345-376; each subcommand's argparse block
at cli.py:653-773]

| Indicateur | Défaut | Repli sur variable d'environnement |
| --- | --- | --- |
| `--api <url>` | `http://127.0.0.1:8000` | `PROVISA_API_URL` |
| `--token <token>` | _(aucun)_ | `PROVISA_API_TOKEN` |
| `--timeout <seconds>` | 300 (30 pour `maintenance`) | _(aucune)_ |

`--api` est l'URL de base d'une instance Provisa en cours d'exécution. En multilocation, le nom d'hôte nomme
l'organisation — `https://acme.provisa.org` route vers le locataire acme. `--token` est un jeton Bearer ;
lorsqu'il est vide, aucun en-tête `Authorization` n'est envoyé, ce qui est correct pour les déploiements
non authentifiés. [tool-verified: cli.py:314-316, 357-365]

Définissez les deux variables dans votre environnement CI pour éviter de les répéter à chaque appel :

```bash
export PROVISA_API_URL=https://acme.provisa.org
export PROVISA_API_TOKEN=<token>
```

Sous-commandes acceptant ces indicateurs : `metadata export`, `env deploy`, `env fetch`,
`maintenance on`, `maintenance off`, `maintenance status`.

---

## provisa run

Démarre le système Provisa embarqué — serveur API et serveur statique/proxy de l'interface — dans un
processus unique. Pas de Docker, pas de Node, pas de services externes. [tool-verified: cli.py module docstring lines 11-23;
`_cmd_run()` at cli.py:549-602]

```
provisa run [--demo] [--host HOST] [--api-port PORT] [--ui-port PORT]
            [--no-browser] [--reset] [--data-dir DIR]
```

### Indicateurs

| Indicateur | Défaut | Remarques |
| --- | --- | --- |
| `--demo` | désactivé | Charge la démo intégrée — domaines d'exemple pet-store et shelter sur SQLite embarqué (REQ-414) |
| `--host` | `127.0.0.1` | Adresse de liaison pour les deux serveurs |
| `--api-port` | `8000` | Port du serveur API |
| `--ui-port` | `3000` | Port du serveur statique/proxy de l'interface |
| `--no-browser` | désactivé | Ne pas ouvrir de navigateur quand l'interface est prête ; affiche tout de même l'URL |
| `--reset` | désactivé | Supprime et reconstruit le magasin du plan de contrôle embarqué avant le démarrage ; à utiliser après une mise à niveau de Provisa si le démarrage signale une incohérence de schéma |
| `--data-dir` | `~/.provisa/native` | Répertoire contenant le cluster PostgreSQL embarqué et le cache d'extensions DuckDB |

[tool-verified: run subparser at cli.py:609-634]

### Variables d'environnement

`provisa run` lit plusieurs variables supplémentaires avant que les serveurs HTTP ne démarrent.
Définissez-les pour surcharger les défauts que `load_profile("native", ...)` appliquerait sinon.
[tool-verified: `_apply_embedded_env()` at cli.py:83-116]

| Variable | Effet |
| --- | --- |
| `TRINO_HOST` / `TRINO_PORT` | Remplace le moteur DuckDB embarqué par un coordinateur Trino fourni par le client (REQ-1129) |
| `PROVISA_ENGINE_URL` | Autre façon de pointer vers un moteur de fédération externe |
| `PROVISA_CONFIG` | Fichier de configuration à charger ; `--demo` le règle sur la configuration de démo intégrée (REQ-1127) |
| `PROVISA_DEMO` | Réglé à `1` par `--demo` ; marque la session comme une exécution de démo |
| `PROVISA_DEMO_DIR` | Chemin vers le répertoire de données d'exemple de la démo ; réglé par `--demo` |
| `PROVISA_CONFIG_REPLACE` | Réglé à `true` par `--demo` pour autoriser la configuration de démo à écraser une configuration existante |
| `PROVISA_DUCKDB_EXT_DIR` | Répertoire d'extensions DuckDB pré-déployé ; réglé automatiquement depuis le paquet `provisa-duckdb-ext` si présent ; son absence signifie que DuckDB télécharge depuis le réseau à la première utilisation |

[tool-verified: `_apply_demo_config()` at cli.py:67-80; `_apply_embedded_env()` at cli.py:83-116]

### Séquence de démarrage

1. Vérification de plateforme — s'arrête avec un message clair sur un Python non pris en charge ou un pgserver manquant.
2. `--reset` (si demandé) — supprime le cluster PostgreSQL embarqué ; il se reconstruit à l'étape suivante.
3. Configuration de démo (si `--demo`) — règle `PROVISA_CONFIG` et `PROVISA_DEMO_DIR`.
4. Environnement embarqué — démarre le plan de contrôle PostgreSQL, résout son URL de socket, et
   déploie les extensions DuckDB hors ligne si `provisa-duckdb-ext` est installé.
5. Vérification de dérive de schéma — scrute le plan de contrôle en direct à la recherche de colonnes manquantes. Si des colonnes sont trouvées,
   affiche une invite `--reset` et sort avec le code 1. V1 n'a pas de migrations ; une colonne ajoutée dans une
   version plus récente nécessite une réinitialisation. [tool-verified: `_control_plane_drift()` at cli.py:119-162]
6. Les deux serveurs démarrent simultanément. L'annonceur de disponibilité sonde `GET /ready` (pas `/health` — le
   point d'entrée `/ready` confirme que le magasin est rattaché et que le moteur est chaud) et ouvre le navigateur
   dès qu'il retourne 200. [tool-verified: `_announce_ready()` at cli.py:182-224]

### Codes de sortie

| Code | Signification |
| --- | --- |
| 0 | Arrêt propre (Ctrl-C) |
| 1 | Erreur de démarrage (échec de la vérification de plateforme, configuration de démo manquante, dérive de schéma détectée) |

### Exemple

```bash
# Start with the demo data
provisa run --demo

# Start on non-default ports, no browser
provisa run --api-port 8080 --ui-port 4000 --no-browser

# Upgrade: reset the control plane first, then start
provisa run --reset

# Point at an external Trino cluster instead of the embedded DuckDB engine
TRINO_HOST=trino.internal TRINO_PORT=8080 provisa run
```

---

## provisa license apply

Vérifie et installe un fichier de licence hors ligne (REQ-1139). Le fichier est le `license.json` émis par
provisa.dev. [tool-verified: `_cmd_license_apply()` at cli.py:271-280]

```
provisa license apply <file>
```

| Argument | Remarques |
| --- | --- |
| `file` | Chemin du fichier de licence ; l'expansion `~` est appliquée |

Le code de sortie 0 signifie que la licence est valide et installée. Le code de sortie 1 signifie qu'elle a été rejetée ; la
raison est affichée sur stderr. [tool-verified: cli.py:276-280]

```bash
provisa license apply ~/Downloads/license.json
```

---

## provisa license status

Affiche l'ID de la machine, l'état d'essai, les jours écoulés, et la validité de la licence (REQ-1139).
[tool-verified: `_cmd_license_status()` at cli.py:283-299]

```
provisa license status
```

Aucun indicateur. Affiche quatre lignes — ID de la machine, date de première observation, jours écoulés, état d'essai, et
état de licence — et sort avec le code 0. [tool-verified: cli.py:293-299]

```
Machine ID:   a1b2c3d4e5f6...
First seen:   2026-07-01
Elapsed:      83.0 days
Trial:        active
Licensed:     no (no license installed)
```

---

## provisa metadata export

Déclenche la publication de métadonnées à la demande du serveur en cours d'exécution (REQ-1072/REQ-1074). Envoie une requête POST à
`POST /admin/metadata-export/publish` — le même point d'entrée que le bouton **Publish now** de l'onglet Admin
appelle, de sorte que les deux chemins envoient le même instantané complet. [tool-verified: `_cmd_metadata_export()`
at cli.py:302-342]

```
provisa metadata export [--api URL] [--token TOKEN] [--timeout SECONDS]
```

| Indicateur | Défaut | Remarques |
| --- | --- | --- |
| `--api` | `$PROVISA_API_URL`, puis `http://127.0.0.1:8000` | En multilocation, le nom d'hôte nomme l'organisation |
| `--token` | `$PROVISA_API_TOKEN` | Jeton Bearer pour une identité détenant `org_settings` ; à omettre sur les déploiements non authentifiés |
| `--timeout` | `300` | Secondes avant l'abandon de l'appel HTTP |

[tool-verified: cli.py:654-669]

| Code de sortie | Signification |
| --- | --- |
| 0 | Chaque actif publié |
| 1 | Publication partielle ou échec de connexion ; les erreurs par actif s'affichent sur stderr |

[tool-verified: cli.py:335-342]

```bash
provisa metadata export \
  --api  https://acme.provisa.org \
  --token "$PROVISA_API_TOKEN"

# Cron example — daily at 06:00
# 0 6 * * *  provisa metadata export --api https://acme.provisa.org >> /var/log/provisa-export.log 2>&1
```

La référence de configuration complète — fournisseurs, identifiants, `reconcile_cron`, et ce que
l'instantané contient — se trouve dans [Export de métadonnées](metadata-export.md#from-the-command-line).

---

## provisa env deploy

Déploie le modèle à une référence git dans un environnement, faisant de cet arbre le modèle actuel de l'environnement (REQ-1496). C'est la commande qu'exécute un pipeline de déploiement ; la règle est qu'un déploiement est
toujours une invocation portant une identité contre un plan de contrôle nommé. [tool-verified:
`_cmd_env_deploy()` at cli.py:395-417]

```
provisa env deploy --org ORG --env ENV --ref REF
                   [--dry-run] [--seed] [--message MSG]
                   [--api URL] [--token TOKEN] [--timeout SECONDS]
```

| Indicateur | Requis | Remarques |
| --- | --- | --- |
| `--org` | oui | Organisation détenant l'environnement |
| `--env` | oui | Environnement qui détiendra le modèle déployé |
| `--ref` | oui | Branche ou SHA de commit dans le dépôt de l'organisation |
| `--dry-run` | non | Rapporte ce qui changerait ; n'applique rien |
| `--seed` | non | Applique aussi les classes de création uniquement (rôles) ; correct uniquement quand ce déploiement crée l'environnement pour la première fois |
| `--message` | non | Note portée sur une demande d'approbation quand l'environnement cible est protégé |
| `--api` | non | Voir [Options partagées](#options-partagees) |
| `--token` | non | Voir [Options partagées](#options-partagees) |
| `--timeout` | non | Défaut 300 s |

[tool-verified: cli.py:677-711]

| Code de sortie | Signification |
| --- | --- |
| 0 | Déploiement appliqué, ou `--dry-run` terminé |
| 2 | L'environnement est protégé ; le déploiement a seulement été proposé, pas appliqué |

Le code de sortie 2 est intentionnel. Un pipeline qui traiterait une approbation en attente comme un déploiement livré
serait erroné. [tool-verified: cli.py:416-417]

```bash
# Fetch remote branches, then deploy
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"

provisa env deploy \
  --org acme --env prod --ref "origin/main" \
  --message "release: $GIT_COMMIT_MSG" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```

Pour l'explication complète des classes d'environnement, des règles de protection, des rapports de fusion, et du
cycle de vie d'approbation, voir [Environnements](environments.md#the-env-cli-commands).

---

## provisa env fetch

Récupère les branches distantes de l'organisation dans son dépôt Provisa (REQ-1541). Exécutez ceci avant un
déploiement quand vous voulez nommer `origin/<branch>`. [tool-verified: `_cmd_env_fetch()` at
cli.py:420-432]

```
provisa env fetch --org ORG [--api URL] [--token TOKEN] [--timeout SECONDS]
```

| Indicateur | Requis | Remarques |
| --- | --- | --- |
| `--org` | oui | Organisation dont le distant est récupéré |
| `--api` | non | Voir [Options partagées](#options-partagees) |
| `--token` | non | Jeton Bearer pour un administrateur d'organisation |
| `--timeout` | non | Défaut 300 s |

[tool-verified: cli.py:716-733]

Affiche une ligne par branche récupérée — `origin/<name>  <sha12>`. Sort avec le code 0 en cas de succès ; lève
`SystemExit` avec un message d'erreur en cas d'échec HTTP ou de connexion.

```bash
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
# origin/main  a1b2c3d4e5f6
# origin/dev   9f8e7d6c5b4a
```

---

## provisa maintenance on

Lève la bannière de maintenance planifiée sur le déploiement (REQ-1466). Exécutez ceci avant un
travail planifié qui met le plan de données hors service — par exemple, avant de changer
`var.engine_cluster_mode`, ce qui remplace le cluster de moteur et chaque fragment qui s'y trouve (REQ-1465).
[tool-verified: `_cmd_maintenance_on()` at cli.py:483-495]

```
provisa maintenance on [--message MSG] [--ends-at ISO8601]
                       [--api URL] [--token TOKEN] [--timeout SECONDS]
```

| Indicateur | Remarques |
| --- | --- |
| `--message` | Surcharge le libellé standard du déploiement ; le défaut est le message standard du serveur |
| `--ends-at` | Instant ISO-8601 auquel le travail devrait se terminer, par ex. `2026-08-14T22:30:00Z` ; le défaut est l'absence d'estimation |
| `--api` | Voir [Options partagées](#options-partagees) |
| `--token` | Jeton Bearer pour une identité détenant `platform_settings` |
| `--timeout` | Défaut 30 s |

[tool-verified: cli.py:743-773]

Affiche l'état de bannière résultant et sort avec le code 0. Lève `SystemExit` en cas d'échec HTTP ou de connexion.

```bash
provisa maintenance on \
  --message "Engine cluster rolling upgrade" \
  --ends-at "2026-09-22T03:00:00Z" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```

---

## provisa maintenance off

Efface la bannière de maintenance une fois le travail terminé (REQ-1466).
[tool-verified: `_cmd_maintenance_off()` at cli.py:498-501]

```
provisa maintenance off [--api URL] [--token TOKEN] [--timeout SECONDS]
```

Affiche l'état de bannière résultant (active : false) et sort avec le code 0.

```bash
provisa maintenance off --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
# Maintenance notice: OFF
```

---

## provisa maintenance status

Affiche l'état actuel de la bannière de maintenance sans le modifier (REQ-1466).
[tool-verified: `_cmd_maintenance_status()` at cli.py:504-507]

```
provisa maintenance status [--api URL] [--token TOKEN] [--timeout SECONDS]
```

Affiche l'état de la bannière et sort avec le code 0.

```
Maintenance notice: ON
  Message:  Engine cluster rolling upgrade
  Since:    2026-09-22T01:15:00Z
  Ends at:  2026-09-22T03:00:00Z
```

---

## Référence rapide

| Commande | REQ | Ce qu'elle fait |
| --- | --- | --- |
| `provisa run` | REQ-1128 | Démarre l'API + l'interface embarquées |
| `provisa run --demo` | REQ-414 | Démarre avec les données d'exemple pet-store / shelter |
| `provisa run --reset` | REQ-1535 | Reconstruit le plan de contrôle avant de démarrer |
| `provisa license apply <file>` | REQ-1139 | Installe un fichier de licence hors ligne |
| `provisa license status` | REQ-1139 | Affiche l'ID machine et l'état d'essai / de licence |
| `provisa metadata export` | REQ-1072 | Publie l'instantané de métadonnées à la demande |
| `provisa env fetch --org ORG` | REQ-1541 | Récupère les branches distantes dans le dépôt Provisa |
| `provisa env deploy --org ORG --env ENV --ref REF` | REQ-1496 | Déploie une référence dans un environnement |
| `provisa maintenance on` | REQ-1466 | Lève la bannière de maintenance |
| `provisa maintenance off` | REQ-1466 | Efface la bannière de maintenance |
| `provisa maintenance status` | REQ-1466 | Affiche l'état actuel de la bannière |

## Voir aussi

- [Environnements](environments.md) — modèle d'environnement, environnements protégés, cycle de vie d'approbation de déploiement
- [Export de métadonnées](metadata-export.md) — fournisseurs de catalogue, configuration, et contenu de l'instantané
- [Déploiement](deployment.md) — palier conteneur et déploiement cloud
