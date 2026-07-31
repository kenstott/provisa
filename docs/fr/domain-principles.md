# Principes du modèle de domaine

---

## 1. Gouvernance

### Principes fondamentaux

1. **Toute ressource doit appartenir à un domaine.** Les tables, les vues et les relations sont toutes des actifs de domaine. Il n'existe aucune ressource flottante non gouvernée. Le domaine est l'unité de responsabilité.
2. **Tout domaine doit avoir un data steward.** Un domaine peut exister à l'état en attente jusqu'à ce qu'un data steward lui soit affecté, mais il ne peut pas servir de données gouvernées sans lui.
3. **L'administrateur est propriétaire des sources.** Les sources sont de l'infrastructure, pas des ressources de domaine. L'administrateur enregistre et gère les connexions aux systèmes de données externes.
4. **Les data stewards peuvent revendiquer des tables pour un domaine.** La revendication est exclusive : une table appartient exactement à un domaine. C'est l'acte gouverné qui relie l'infrastructure à la couche sémantique.
5. **Les data stewards peuvent créer des vues intra-domaine à partir des actifs du domaine.** Les vues expriment la logique métier — jointures, agrégations, métriques dérivées — sur des actifs que le data steward possède au sein du même domaine. Les vues créent une nouvelle signification sémantique et nécessitent l'approbation du data steward.
6. **Les analystes peuvent créer des requêtes inter-domaines à partir de relations approuvées.** Les requêtes sont des vues inter-domaines exprimées dans n'importe quel langage de requête pris en charge. Elles ne créent pas de nouvelle sémantique — elles parcourent des chemins de relation approuvés. Aucune approbation supplémentaire n'est requise : la gouvernance est gérée en amont, aux couches Relation et visibilité des colonnes. Le catalogue est le mécanisme d'application : le compilateur rejette les parcours qui ne figurent pas dans le catalogue de relations approuvées.
7. **Tout le monde peut demander l'accès à une ressource de domaine.** L'accès est accordé au niveau de la ressource, pas au niveau de la requête. Si vous avez accès à une ressource, vous pouvez la requêter. La gouvernance est appliquée au moment de l'exécution via le pipeline.

### Ressources : tables et vues en tant que pairs

La distinction entre une table et une vue tient uniquement à l'origine — une table est revendiquée à partir d'une source, une vue est définie par un data steward. Une fois que l'une ou l'autre existe en tant qu'actif de domaine, le modèle de gouvernance les traite de manière identique :

- Les deux sont des actifs de domaine de premier ordre, visibles dans le catalogue
- Les deux peuvent être la cible d'une relation
- Les deux peuvent être accordées en vertu du principe 6
- Les deux sont soumises au même pipeline de gouvernance

Un data steward peut revendiquer des tables de manière privée et n'exposer que des vues sélectionnées comme produits de données destinés au public.

### Composition des vues

Une vue appartient toujours à un seul domaine — il n'existe qu'un seul type de vue, toujours intra-domaine. Une vue existe pour l'un des deux objectifs suivants :

- **Import inter-domaines** — la source est extérieure au domaine. Les données inter-domaines ne peuvent entrer dans un domaine que via une vue, qui agit comme un adaptateur en lecture seule nommant les données externes comme un concept métier du domaine.
- **Dérivation locale** — la source appartient au même domaine. La vue dérive des données nouvelles ou calculées à partir des actifs de domaine existants. Les données nouvelles ou dérivées ne peuvent exister que sous forme de vue.

Une vue peut référencer :

- Des tables revendiquées au sein du même domaine
- Des champs importés d'un autre domaine dans le cadre d'une concession d'accès aux champs
- Une autre vue au sein du même domaine, lorsque la variation a un objectif précis : restriction de champs, agrégation ou enrichissement via une jointure supplémentaire

La profondeur de composition n'est pas appliquée techniquement — le jugement du data steward pendant la revue HITL constitue le mécanisme de contrôle de la qualité.

Chaque vue porte un objectif métier déclaré, énoncé au moment de sa création :

- Fait partie de l'artefact gouverné — les data stewards approuvent en sachant à quoi sert la vue
- Est référencé par les demandes d'accès en vertu du principe 7, afin que le data steward puisse en évaluer la pertinence
- Accompagne la vue depuis sa création tout au long du flux de gouvernance complet

### Requêtes

Une requête parcourt des chemins de relation approuvés sur les actifs de domaine. Contrairement aux vues, les requêtes ne créent pas de nouvelle signification sémantique — elles parcourent la structure approuvée du modèle. Les requêtes peuvent être exprimées dans n'importe quel langage de requête pris en charge (SQL, GraphQL, Cypher).

**Application structurelle :** le catalogue de relations est le mécanisme d'application. Le compilateur valide chaque parcours par rapport aux entrées approuvées du catalogue et rejette les requêtes qui référencent des chemins non approuvés. La gouvernance est structurelle, pas une vérification à l'exécution.

**Aucune approbation requise :** la gouvernance a lieu en amont — aux couches Relation et visibilité des colonnes. Si un utilisateur a accès aux colonnes et que le chemin de parcours est approuvé, la requête constitue un usage valide. Aucun contrôle supplémentaire.

**Différence avec les vues :**

- Vues : intra-domaine, introduisent une nouvelle signification sémantique, sélectionnées par le data steward
- Requêtes : parcourent des relations approuvées, aucune nouvelle sémantique, aucun contrôle d'approbation

**Expression du domaine selon le langage de requête :**

Chaque langage pris en charge exprime le domaine comme un espace de noms structurel natif de ce langage :

| Langage | Expression du domaine | Exemple |
| --- | --- | --- |
| GraphQL | Préfixe du nom de type et de champ | `type sales__Order { ... }`, `query { sales__orders { ... } }` |
| SQL | Nom de schéma | `SELECT * FROM sales.orders` |
| Cypher | Étiquette de nœud supplémentaire (le domaine n'est requis que lorsque le nom de type est ambigu) | `MATCH (o:Sales:Order)` |

Le compilateur résout l'appartenance au domaine à partir de ces positions structurelles — aucune annotation ni indication n'est requise.

### Relations

Une relation est un chemin de parcours approuvé entre deux actifs. Les frontières de domaine n'ont aucune incidence sur ce qu'est une relation — elles déterminent seulement qui l'approuve.

**Approbation :**

- L'approbation est requise de la part de chaque data steward distinct propriétaire d'un actif impliqué dans la relation
- Si un seul data steward possède les deux actifs, une seule approbation est requise. Si deux data stewards sont impliqués, deux approbations sont requises
- Il n'existe pas de classification intra-domaine/inter-domaines — la propriété détermine naturellement la charge d'approbation
- L'approbation d'une relation construit le graphe de dépendances de chaque data steward, ce qui permet des notifications proactives d'évolution du schéma

Les relations sont créées à la demande, pas de manière spéculative. La première équipe ayant le besoin métier effectue le travail ; les équipes suivantes héritent de l'infrastructure.

**Conséquence en matière d'optimisation :** une déclaration de relation n'est pas seulement un artefact de gouvernance — c'est aussi une description structurelle de la forme d'une jointure. Les deux tables, les deux colonnes et le type de jointure qui définissent une relation sont exactement ce dont l'optimiseur de requêtes a besoin pour pré-matérialiser cette jointure. Les relations entre sources différentes génèrent automatiquement des tables de jointure pré-matérialisées ; les relations au sein d'une même source peuvent y adhérer via `materialize: true`. Les data stewards qui réfléchissent à des relations valides et les approuvent obtiennent une accélération des requêtes comme sous-produit direct — le travail de gouvernance et le travail d'optimisation sont un seul et même acte.

### Concessions d'accès aux champs

Une concession d'accès aux champs est une autorisation de domaine à domaine — le Domaine A peut utiliser des champs spécifiques du Domaine B dans ses vues.

**Cycle de vie de la concession :**

- Déclenchée par la création d'une vue lorsque des champs externes sont identifiés comme nécessaires
- Approuvée une fois par le data steward du domaine cible
- Appartient au domaine demandeur, pas à la vue qui l'a déclenchée
- Toute vue ultérieure du domaine demandeur peut utiliser les champs concédés sans intervention supplémentaire inter-domaines
- Les champs supplémentaires non concédés nécessitent une nouvelle demande

**Notification après usage :** lorsqu'une vue est créée en utilisant des champs concédés, le data steward source en est notifié — pas invité à approuver. La notification comprend le nom de la vue, l'objectif métier déclaré, les champs spécifiques utilisés, et quel data steward l'a approuvée. Cela donne au data steward source :

- **Visibilité** — la connaissance de la manière dont ses données sont utilisées
- **Supervision** — des motifs pour soulever une préoccupation si l'usage semble inapproprié
- **Recours** — la capacité de révoquer la concession, invalidant les vues dépendantes

Le compromis : le domaine source approuve l'accès aux champs sans connaître chaque usage futur. L'approbation par vue est correcte en théorie et impraticable en pratique.

### Flux de création de requêtes

Trois étapes, dans l'ordre.

**Étape 1 — Mise en forme (découverte SQL, depuis la page Relations) :**

- L'analyste ouvre l'outil de mise en forme depuis la page Relations pour explorer les chemins de jointure potentiels en SQL brut
- Le SQL est exécuté sur les données accessibles, sous réserve de la RLS et du masquage de colonnes existants
- Les clauses JOIN du SQL sont analysées et présentées comme des propositions de relations candidates
- Les candidats suggérés par la machine (inférence de clé étrangère, inférence sémantique) sont affichés aux côtés de l'exploration SQL de l'analyste dans la même vue
- L'analyste sélectionne les candidats à promouvoir en demande formelle de relation

**Étape 2 — Approbation de la relation** (conséquente — structurelle et permanente) :

- Soumise à chaque data steward distinct propriétaire d'un actif impliqué dans la relation
- S'agit-il d'un chemin de parcours légitime ? La jointure est-elle sémantiquement valide ?
- Tous les data stewards impliqués doivent approuver ; la relation devient une entrée permanente du catalogue

**Étape 3 — Création de la requête :**

- L'analyste construit la requête dans n'importe quel langage pris en charge (SQL, GraphQL, Cypher), en parcourant les chemins de relation approuvés
- Seules les relations approuvées du catalogue sont parcourables — le compilateur l'applique de manière structurelle
- Aucune approbation requise — la visibilité des colonnes et l'approbation de la relation sont les seuls contrôles

### HITL comme contrôle principal

Les règles techniques gèrent ce qui est objectif — le suivi de la provenance des champs, l'application des frontières de domaine, la validation par le compilateur. Le jugement contextuel reste entre les mains du data steward. Des contraintes telles que la profondeur de composition des vues, les exigences d'objectif par requête et les décisions d'approbation des relations relèvent du HITL, et non de règles appliquées par le compilateur.

**Neutralité du domaine source :** le data steward du domaine source approuve la relation une fois et la concession de champs une fois. Par la suite, les domaines en aval opèrent dans les limites de ces concessions :

- **Examen approfondi** au moment de la décision de franchissement de frontière
- **Connaissance légère** par la suite, via des notifications et l'historique des requêtes

---

## 2. Découvrabilité

### Niveaux de découverte

La découverte est structurée selon cinq niveaux de gouvernance croissante. Chaque niveau est un prérequis pour le suivant.

| Niveau | Description | État de gouvernance |
| --- | --- | --- |
| 1 — Schéma de source enregistrée | Chaque table, colonne et type d'une source enregistrée. Visibilité au niveau administrateur. | Aucun — inventaire brut |
| 2 — Tables non revendiquées | Tables introspectées à partir de sources enregistrées sans propriétaire de domaine. Visibles pour les data stewards ayant accès à la source. | Disponible mais non gouverné |
| 3 — Actifs de domaine | Tables revendiquées et vues définies par le data steward. Entièrement gouvernées, possédées, visibles dans le catalogue. | Entièrement gouverné |
| 4 — Relations | Chemins de parcours approuvés entre actifs de niveau 3. Prérequis pour la création de vues inter-domaines. | Approuvé par les deux data stewards |
| 5 — Concessions de champs | Autorisations d'accès aux champs de domaine à domaine. L'accès gouverné le plus spécifique et le plus délibéré. | Approuvé par le data steward source |

Une table non revendiquée est un signal de lacune — si les données nécessaires n'existent qu'au niveau 2, un data steward doit la revendiquer avant que la gouvernance puisse progresser. L'absence de tout candidat à tous les niveaux nécessite une escalade vers l'administrateur.

### Contraintes de clé étrangère

Les contraintes de clé étrangère sont une construction au niveau de la source — elles ne peuvent pas s'étendre sur plusieurs sources de données. Les chemins de jointure entre sources sont dérivés entièrement des relations de catalogue approuvées (niveau 4), qui sont plus solides, ayant été validées par les deux data stewards.

Au sein d'une source :

- Les contraintes de clé étrangère sont présentées automatiquement comme des relations candidates lors de l'enregistrement de la source
- Elles représentent une intention de modélisation explicite — non appliquée dans la plupart des systèmes SQL analytiques, mais déclarée délibérément
- La validation du data steward reste requise avant qu'un candidat ne devienne une relation approuvée

### Hiérarchie de confiance des relations

| Preuve | Confiance |
| --- | --- |
| Relation de catalogue approuvée — entre sources, validée par les deux data stewards | Maximale |
| Contrainte de clé étrangère intra-source — intention de modélisation explicite, non appliquée mais délibérée | Élevée |
| Inférence sémantique intra-source — similarité de nom/type de colonne au sein d'un schéma cohérent | Moyenne |
| Inférence sémantique inter-sources — les conventions de nommage divergent entre systèmes ; risque élevé de faux positifs | Faible |

Les suggestions corroborées par plusieurs types de preuves accumulent de la confiance.

### Sondage et corrélation des données

Pour les candidats inférés sémantiquement, le sondage de données offre une étape de validation :

- **Chevauchement de valeurs** — proportion des valeurs de la colonne source qui apparaissent dans la colonne cible
- **Cardinalité** — si la distribution correspond au type de relation attendu
- **Taux de valeurs nulles** — proportion de la colonne source qui est nulle, indiquant une optionnalité

Une corrélation élevée augmente la confiance ; une corrélation faible supprime ou rétrograde le candidat. Le sondage est une preuve corroborante, pas une preuve absolue — les plages d'entiers peuvent se chevaucher par coïncidence, et l'intégrité référentielle partielle est courante dans les systèmes analytiques. Une marge d'erreur importante subsiste. Le jugement sémantique du data steward est la seule vérification finale fiable.

### Découverte assistée par LLM

Le LLM opère simultanément sur les cinq niveaux, suggérant des relations, des revendications candidates et des chemins de parcours classés par confiance.

**Ce que le LLM présente :**

- Des relations candidates classées par confiance
- Des tables non revendiquées susceptibles de répondre à un besoin de données, avec une invite à initier la revendication
- L'absence de tout candidat — signal pour escalader vers l'administrateur

**Conception de vue à partir d'une description métier :**

L'analyste fournit une description en langage naturel et des contraintes facultatives. Le LLM produit une structure de vue suggérée.

*Entrée :*

- Description métier : entités, métriques, relations, intention
- Contraintes facultatives : filtres, fenêtres temporelles, agrégations, champs exclus, restrictions de sensibilité

*Exemple :*
> « Volumes d'opérations quotidiens par contrepartie sur les 30 derniers jours, contreparties actives uniquement, affichant la raison sociale de la contrepartie et la notation de crédit. Aucune donnée personnelle. »

*Processus du LLM :*

1. Analyse — identifier les entités, métriques, dimensions, filtres, exclusions
2. Recherche — dans tous les niveaux du catalogue, les actifs correspondants
3. Suggestion — actifs de domaine, relations, champs, structure d'agrégation
4. Notation — confiance par composant, fondée sur les preuves de niveau
5. Prérequis — liste ordonnée des revendications, relations et concessions de champs requises
6. Lacunes — entités ou champs sans candidat à aucun niveau, signalés pour escalade vers l'administrateur

*Sortie :*

- Ébauche de requête pour revue et ajustement par l'analyste
- Scores de confiance par composant
- Liste ordonnée des prérequis
- Liste des lacunes

La description métier devient l'objectif métier déclaré de la vue une fois que celle-ci est formellement créée.

**Découverte de relations pilotée par SQL (outil de modélisation) :**

Accessible sous forme de fenêtre modale depuis la page Relations. L'intention est de construire le modèle sémantique — en identifiant les chemins de jointure structurels avant de les formaliser en relations gouvernées.

1. L'analyste écrit du SQL libre sur les tables accessibles (RLS et masquage toujours appliqués)
2. L'AST du SQL est analysé — chaque condition JOIN devient une proposition de relation candidate
3. La liste de candidats est affichée aux côtés des candidats suggérés par la machine (inférence de clé étrangère, inférence sémantique) pour une revue unifiée
4. L'analyste promeut les candidats sélectionnés en demandes formelles de relation
5. Les relations approuvées sont ajoutées au catalogue et deviennent parcourables dans les requêtes

L'outil de modélisation peut afficher toutes les tables enregistrées à des fins d'exploration structurelle, même lorsque l'analyste ne peut pas voir les données sous-jacentes — l'approbation du data steward gouverne l'accès réel aux données, pas la visibilité du schéma.

---

## 3. Usage

### Piste d'audit des requêtes

Chaque requête qui touche un actif de domaine est enregistrée dans un `query_audit_log` en ajout seul. Chaque entrée capture :

- `tenant_id`, `user_id`, `role_id` — le contexte d'identité
- Un hachage SHA-256 de la requête — le texte littéral de la requête n'est jamais stocké
- `table_ids` — les actifs de domaine touchés par la requête
- `source`, `status_code`, `duration_ms`
- `logged_at` — l'horodatage

Le journal est en ajout seul (DELETE et UPDATE sont bloqués au niveau de la base de données) et indexé par `(tenant_id, logged_at)` et `(user_id, logged_at)`.

Le rapport d'historique des requêtes du data steward est une vue agrégée sur ce journal, filtrable par actif, rôle et fenêtre temporelle. Le catalogue est un instrument de gouvernance en direct — les data stewards restent conscients de la manière dont leurs actifs sont utilisés au fur et à mesure, et non après coup.

**Deux mécanismes de visibilité :**

- **Push** — notifications après usage pour les actes structurels (une nouvelle vue a été créée en utilisant vos champs)
- **Pull** — historique des requêtes pour les modèles d'usage à l'exécution
