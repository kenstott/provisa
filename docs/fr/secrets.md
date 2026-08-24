# Secrets

**Les noms entrent. Les valeurs ne ressortent jamais.**

Aucun endpoint d'API ne renvoie la valeur d'un secret stocké. Aucune interface n'offre de bouton « afficher ». Une personne qui a perdu une valeur la remplace — c'est le même appel que celui qui l'a créée, via le même formulaire. Ce n'est pas une décision de politique : le chemin de lecture n'existe tout simplement pas dans le code. (REQ-1558)

---

## Syntaxe des références

Trois formes de référence sont valides partout où Provisa résout des identifiants :

| Forme | Résolue depuis | Qui peut l'utiliser |
| ------ | -------------- | --------------- |
| `${env:VAR_NAME}` | L'environnement du processus serveur | Configuration de déploiement uniquement |
| `${secret:NAME}` | Le coffre-fort de l'organisation — partagé par tous les membres | Tout champ acceptant une référence d'identifiant |
| `${user:NAME}` | Le coffre-fort personnel de la personne qui agit | Tout champ acceptant une référence d'identifiant |

La résolution est à sécurité intégrée (fail-closed) de bout en bout. Un nom de fournisseur inconnu, un nom non défini et un backend injoignable lèvent tous une erreur. Une référence qui n'a pas pu être résolue n'est jamais remplacée silencieusement par une chaîne vide. (REQ-1557) [tool-verified: `provisa/core/secrets.py:92-117`]

### Format des noms

Les noms de secrets doivent correspondre à `[A-Za-z_][A-Za-z0-9_]*` — lettres, chiffres et tirets bas, commençant par une lettre ou un tiret bas. La contrainte est pratique : `${secret:NAME}` est analysé par la grammaire des références, qui lit jusqu'à l'accolade fermante `}`. Un nom contenant une accolade, une espace ou un deux-points produirait une référence qui s'analyse comme autre chose. [tool-verified: `provisa/core/secrets_store.py:61`]

---

## Deux coffres-forts, un seul service

Chaque organisation possède deux coffres-forts. Tous deux vivent à l'intérieur du même service de secrets. (REQ-1560)

**Coffre-fort de l'organisation** — L'identifiant qu'un administrateur d'organisation y range est partagé. Chaque membre qui référence `${secret:DATABASE_TOKEN}` obtient la même valeur. C'est là que vont les identifiants que l'*organisation* possède : un mot de passe de base de données partagé, une clé de compte de service, un jeton de déploiement. Le coffre-fort de l'organisation exige la capacité `org_settings` en lecture comme en écriture.

**Coffre-fort personnel** — Un identifiant rangé ici appartient à une seule personne. Quand deux personnes détiennent chacune un `GIT_TOKEN`, `${user:GIT_TOKEN}` se résout vers celui de la personne qui agit. Le même texte de référence remet à chacun son propre identifiant. Une personne qui n'a rien rangé obtient une erreur, pas la valeur de quelqu'un d'autre. Aucune capacité ne garde le coffre-fort personnel — détenir son propre identifiant n'est pas un privilège qu'un administrateur accorde. Et il n'existe aucune syntaxe de requête permettant de désigner le coffre-fort d'une autre personne. [tool-verified: `provisa/api/admin/secrets_router.py:86-103`]

La portée fait partie de la référence, elle n'est pas une autorisation qui l'entoure. `${secret:NAME}` et `${user:NAME}` ne répondent jamais l'un pour l'autre.

---

## Choisir un service de secrets

**Admin → Sécurité → Service de secrets.** Le panneau est visible par quiconque détient la capacité `platform_settings`. Chaque backend que connaît la version compilée est listé, que le SDK soit installé ou non. Une ligne grisée vous indique quel paquet Python manque — le panneau le nomme plutôt que de masquer entièrement l'option.

Cinq backends sont livrés :

| Clé | Libellé | Nécessite |
| ----- | ------- | ------- |
| `provisa` | Provisa (intégré, chiffré) | Rien ; c'est la valeur par défaut |
| `hashicorp_vault` | HashiCorp Vault (KV v2) | `hvac` |
| `aws_secrets_manager` | AWS Secrets Manager | `boto3` |
| `gcp_secret_manager` | Google Secret Manager | `google-cloud-secret-manager` |
| `azure_key_vault` | Azure Key Vault (secrets) | `azure-keyvault-secrets` |

[tool-verified: `provisa/core/secrets_registry.py:161-299`]

La sélection est à sécurité intégrée : un backend inconnu ou indisponible lève une erreur au démarrage plutôt que de se rabattre silencieusement sur un autre. (REQ-1557)

### L'identifiant propre au backend

L'identifiant de connexion d'un backend centralisé relève de la configuration du processus. Il provient de `${env:...}` uniquement — jamais de `${secret:...}`. Un service de secrets dont le propre identifiant vit à l'intérieur de lui-même ne peut pas être ouvert : la chaîne de confiance se termine donc dans l'environnement hôte, par conception. Le registre l'impose : toute valeur de configuration figurant dans la spécification d'un backend est résolue avec `providers=("env",)` avant que le backend ne soit construit. [tool-verified: `provisa/core/secrets_registry.py:128-141`]

Exemple — configuration Vault dans `provisa.yaml` :

```yaml
secrets:
  provider: hashicorp_vault
  hashicorp_vault:
    url: https://vault.internal:8200
    token: ${env:VAULT_TOKEN}   # process env only — never ${secret:...}
    mount: secret
```

### Service centralisé ou magasin intégré

Lorsqu'un service centralisé est configuré, Provisa y lit mais n'y écrit pas. Le service centralisé possède la création et la suppression des entrées — ces opérations relèvent de son propre outillage. La page Secrets le dit et n'offre pas de bouton de création. (REQ-1557)

Lorsque le backend intégré `provisa` est actif, la page Secrets est pleinement modifiable : créer, remplacer et supprimer depuis l'interface ou via l'API.

---

## Le magasin intégré de Provisa

La valeur par défaut lorsqu'aucun service centralisé n'est configuré. Chaque ligne de `secrets_store` contient un blob d'enveloppe chiffrée — la colonne `value` est binaire, pas textuelle, et la clé de déchiffrement vit dans l'environnement du processus, pas dans la base de données. Une copie du plan de contrôle privée de la clé maîtresse du déploiement ne contient que du chiffré, rien d'autre. (REQ-1558)

Le chiffrement n'est jamais facultatif. Lorsqu'aucune clé de chiffrement à l'échelle du processus n'est configurée, le magasin se rabat sur un trousseau local. Si l'hôte n'a aucun trousseau pour détenir une clé, le magasin refuse d'écrire plutôt que de stocker la valeur en clair. [tool-verified: `provisa/core/secrets_store.py:130-159`]

**Forme de stockage** [tool-verified: `provisa/core/schema_admin.py:493-505`] :

| Colonne | Type | Rôle |
| -------- | ------ | --------- |
| `org_id` | Text | L'organisation propriétaire de ce secret |
| `owner_id` | Text | `"*"` pour le coffre-fort de l'organisation ; identifiant d'utilisateur pour le coffre-fort personnel |
| `name` | Text | Le nom de référence |
| `value` | LargeBinary | Blob d'enveloppe chiffrée |
| `description` | Text | À quoi sert le secret — jamais déduit de la valeur |
| `updated_by` | Text | Qui l'a défini en dernier |

La colonne `value` n'est sélectionnée par aucune requête de listage. [tool-verified: `provisa/core/secrets_store.py:214-235`]

---

## Endpoints d'API

Toutes les routes sont sous `/admin/orgs/{org_id}`. Le coffre-fort de l'organisation exige `org_settings` dans cette organisation. Le coffre-fort personnel n'exige aucune capacité — le propriétaire est lu depuis l'identité authentifiée ; il n'existe aucun paramètre de requête permettant de désigner le coffre-fort de quelqu'un d'autre.

| Méthode | Chemin | Ce qu'elle fait |
| -------- | ------ | ------------- |
| `GET` | `/secrets` | Lister les noms et références du coffre-fort de l'organisation |
| `PUT` | `/secrets/{name}` | Créer ou remplacer un secret de l'organisation |
| `DELETE` | `/secrets/{name}` | Supprimer un secret de l'organisation |
| `GET` | `/my-secrets` | Lister les noms et références personnels de l'appelant |
| `PUT` | `/my-secrets/{name}` | Créer ou remplacer un des secrets de l'appelant |
| `DELETE` | `/my-secrets/{name}` | Supprimer un des secrets de l'appelant |

Chaque réponse renvoie des métadonnées — nom, description, `updated_at`, `updated_by` et la chaîne `reference` à coller — mais jamais la valeur. Le corps du `PUT` porte `value` (obligatoire) et `description` (facultatif). Un remplacement est le même appel qu'une création : le nom est l'identité, pas un identifiant distinct.

Chaque écriture est consignée dans le journal d'audit. L'entrée nomme l'acteur et le nom du secret. La valeur n'est pas consignée, pas même sa longueur. [tool-verified: `provisa/api/admin/secrets_router.py:106-117`]

---

## Où `${secret:NAME}` se résout

La résolution a lieu à l'intérieur d'une opération liée à un contexte, non à l'import ni au démarrage. Le magasin lit et déchiffre les secrets de l'organisation une seule fois au début de cette opération et conserve la table pour toute sa durée dans un `ContextVar`. En dehors d'une opération liée, `${secret:NAME}` lève une erreur. (REQ-1557) [tool-verified: `provisa/core/secrets_store.py:269-290`]

Deux sites d'appel établissent cette liaison :

**Opérations git distantes.** Lorsque l'URL du dépôt distant d'une organisation contient une référence `${secret:...}` ou `${user:...}` — par exemple un jeton de push intégré à l'URL — le routeur des environnements lie à la fois le coffre-fort de l'organisation et le coffre-fort personnel de l'utilisateur qui agit autour de l'appel git. La forme `${user:GIT_TOKEN}` fait qu'un commit arrive sous l'identifiant de la personne qui l'a poussé, et non sous un compte de service partagé. [tool-verified: `provisa/api/admin/environments_router.py:1263`]

**Lectures de clé d'API d'un fournisseur d'IA.** Lorsque Provisa lit la clé de fournisseur de LLM d'une organisation et que cette clé est stockée sous forme de référence `${secret:NAME}`, `bound_to_request_org` établit le coffre-fort de l'organisation pour cette requête. La référence est résolue à la sortie ; le texte de la référence lui-même n'est jamais envoyé au fournisseur. (REQ-1580) [tool-verified: `provisa/core/org_secrets.py:76-79`]

---

## Clés de fournisseur d'IA d'une organisation comme références de secret

La clé de fournisseur d'IA d'une organisation (Anthropic, OpenAI et d'autres) peut être stockée sous forme de référence `${secret:NAME}` plutôt que de clé littérale. (REQ-1580)

Rangez d'abord la clé dans le coffre-fort de l'organisation :

```
PUT /admin/orgs/{org_id}/secrets/OPENAI_KEY
{ "value": "sk-...", "description": "OpenAI production key" }
```

Puis réglez la configuration d'IA de l'organisation pour qu'elle la référence :

```
vendor key field → ${secret:OPENAI_KEY}
```

La référence est stockée chiffrée dans `org_secrets`. Au moment de la requête, Provisa résout `${secret:OPENAI_KEY}` contre le coffre-fort de l'organisation et remet la clé littérale au SDK du fournisseur. Faire tourner l'entrée du coffre-fort prend effet immédiatement — aucun changement de configuration du côté des paramètres de l'organisation. [tool-verified: `provisa/core/org_secrets.py:64-79`]

---

## Accès de l'administrateur de la plateforme

Un administrateur de la plateforme qui exploite le plan de contrôle n'a aucune lecture des valeurs de secrets d'une organisation. Le garde `org_settings` refuse explicitement `cross_org` et le contournement de la plateforme : administrer le cycle de vie d'une organisation n'est pas une lecture des identifiants que cette organisation conserve. Le serveur l'impose indépendamment de l'interface. (REQ-1361) [tool-verified: `provisa/api/admin/secrets_router.py:53-83`]

---

## Voir aussi

- [Modèle de sécurité](security.md) — contrôle d'accès en couches, authentification et journalisation d'audit
- [Référence de configuration](configuration.md) — syntaxe `${env:VAR}` pour les identifiants au niveau du processus
