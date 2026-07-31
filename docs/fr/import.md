# Importer depuis Hasura

Provisa peut convertir des métadonnées Hasura existantes en un `config.yaml` Provisa, en
préservant les tables suivies, les relations, les autorisations et les schémas distants.

## Hasura v2

### Exporter les métadonnées

Depuis votre console ou votre CLI Hasura :

```bash
hasura metadata export --output metadata.yaml
```

Ou utilisez l'API Hasura :

```bash
curl -X POST http://localhost:8080/v1/metadata \
  -H "X-Hasura-Admin-Secret: <secret>" \
  -d '{"type":"export_metadata","args":{}}' \
  > metadata.json
```

### Convertir

Le convertisseur v2 lit un **répertoire** de métadonnées Hasura (la structure produite par
`hasura metadata export`, ou la structure plate `tables.yaml` / `actions.yaml`) et écrit une
configuration Provisa :

```bash
python -m provisa.hasura_v2 ./metadata -o config.yaml
```

Omettez `-o` pour écrire la configuration sur stdout.

Options :

| Option | Objet |
| ------ | --------- |
| `-o`, `--output` | Chemin YAML de sortie (par défaut : stdout) |
| `--source-overrides` | Fichier YAML avec des surcharges de connexion par source (hôte, port, identifiants) |
| `--domain-map` | Correspondances schéma-vers-domaine sous forme de paires `SCHEMA=DOMAIN` |
| `--auth-env-file` | Fichier `.env` avec la configuration d'authentification ; convertit JWT/JWK, le secret admin, et la correspondance des claims |
| `--dry-run` | Analyse et valide sans écrire de sortie |

### Ce qui est converti

| Concept Hasura | Équivalent Provisa |
| --------------- | ------------------- |
| Table suivie | `tables[]` avec `publish: true` |
| Relation d'objet | `relationships[]` avec `cardinality: many-to-one` |
| Relation de tableau | `relationships[]` avec `cardinality: one-to-many` |
| Autorisation de sélection | Visibilité de rôle + filtre RLS |
| Autorisation de colonne | `visible_to` / `writable_by` |
| Autorisation d'insertion/mise à jour/suppression | Mutation `writable_by` + RLS |
| Schéma distant | Enregistrement de source `graphql_remote` |
| Champ calculé | Entrée `functions[]` avec `kind: query` |

### Limites

- **Les actions** se convertissent automatiquement : les actions à gestionnaire HTTP deviennent
  des mutations `webhooks[]` ; les actions avec un gestionnaire non-HTTP (base de données)
  deviennent un espace réservé `functions[]` et émettent un avertissement invitant à revoir le
  gestionnaire
- **Les déclencheurs d'événements (event triggers)** se convertissent en configuration
  `event_triggers` par table (opérations, URL du webhook, politique de nouvelle tentative) et
  émettent un avertissement signalant une fidélité limitée
- **Les schémas distants** se convertissent en entrées de source `graphql_remote`
- **Les fonctions SQL personnalisées** nécessitent une revue — les cas simples se convertissent en
  entrées `functions[]`, les cas complexes nécessitent un travail manuel
- **Les déclencheurs cron** se convertissent en entrées de configuration `scheduler`, en
  préservant l'expression cron et l'indicateur d'activation

---

## Hasura DDN (v3)

### Localiser le projet HML

Le convertisseur DDN lit directement le **répertoire** du projet DDN contenant les fichiers
`.hml` — aucune étape de construction de supergraphe n'est requise. Le premier composant de
répertoire sous la racine du projet est pris comme nom de sous-graphe (subgraph) ; les fichiers
sous `globals/` sont assignés au sous-graphe `globals`.

### Convertir

```bash
python -m provisa.ddn ./my-ddn-project -o config.yaml
```

Omettez `-o` pour écrire la configuration sur stdout.

Options :

| Option | Objet |
| ------ | --------- |
| `-o`, `--output` | Chemin YAML de sortie (par défaut : stdout) |
| `--source-overrides` | Fichier YAML avec des surcharges de connexion par source |
| `--domain-map` | Correspondances sous-graphe-vers-domaine sous forme de paires `SUBGRAPH=DOMAIN` |
| `--aggregates-output` | Chemin de sortie pour le fichier annexe des expressions d'agrégation (par défaut : `<output>-aggregates.yaml`) |
| `--dry-run` | Analyse et valide sans écrire de sortie |

Les métadonnées `AggregateExpression` sont préservées dans un fichier annexe `*-aggregates.yaml`.

### Ce qui est converti

| Concept DDN | Équivalent Provisa |
| ------------ | ------------------- |
| Modèle de sous-graphe | `tables[]` sous une source |
| Relation | `relationships[]` |
| Règle d'autorisation | Filtre RLS |
| Command | Mutation webhook ou vue |
| Connecteur | Entrée de source avec les détails de connexion |

### Limites

- **Les connecteurs lambda** (fonctions TypeScript/Python) nécessitent une configuration manuelle
  du webhook
- **Les plugins de cycle de vie** n'ont pas d'équivalent direct
- **Les modes d'authentification DDN** correspondent aux fournisseurs d'authentification Provisa,
  mais les chemins de claims JWT peuvent nécessiter un ajustement

---

## Après l'import

1. Passez en revue le `config.yaml` généré — prêtez attention aux `warnings` du convertisseur
2. Vérifiez les identifiants de connexion (le convertisseur utilise des valeurs d'espace réservé)
3. Démarrez Provisa et vérifiez que les tables apparaissent dans l'Explorer
4. Exécutez vos requêtes GraphQL existantes — le schéma est compatible pour les modèles courants
5. Soumettez les requêtes pour approbation via l'API Admin ou l'interface avant d'activer la
   gouvernance en production
