# Principes du modèle de domaine

---

## 1. Gouvernance

### Principes fondamentaux

1. **Chaque ressource doit appartenir à un domaine.** Les tables, vues et relations sont toutes des actifs de domaine. Il n'existe aucune ressource flottante non gouvernée. Le domaine est l'unité de responsabilité.
2. **Chaque domaine doit avoir un steward.** Un domaine peut exister à l'état en attente jusqu'à ce qu'un steward lui soit assigné, mais il ne peut servir de données gouvernées sans en avoir un.
3. **L'admin possède les sources.** Les sources sont de l'infrastructure, pas des ressources de domaine. L'admin enregistre et gère les connexions aux systèmes de données externes.
4. **Les stewards peuvent revendiquer des tables pour un domaine.** La revendication est exclusive — une table appartient à exactement un domaine. C'est l'acte gouverné qui relie l'infrastructure et la couche sémantique.
5. **Les stewards peuvent créer des vues intra-domaine à partir d'actifs de domaine.** Les vues expriment la logique métier — jointures, agrégations, métriques dérivées — sur des actifs que le steward possède au sein du même domaine. Les vues créent une nouvelle signification sémantique et requièrent l'approbation du steward.
6. **Les analystes peuvent créer des requêtes inter-domaines à partir de relations approuvées.** Les requêtes sont des vues interdomaines exprimées dans n'importe quel langage de requête pris en charge. Elles ne créent pas de nouvelle sémantique — elles parcourent des chemins de relation approuvés. Aucune approbation supplémentaire n'est requise : la gouvernance est gérée en amont, aux couches Relation et visibilité des colonnes. Le catalogue est le mécanisme d'application : le compilateur rejette les parcours qui ne figurent pas dans le catalogue de relations approuvées.
7. **Toute personne peut demander l'accès à une ressource de domaine.** L'accès est accordé au niveau de la ressource, pas au niveau de la requête. Si vous avez accès à une ressource, vous pouvez l'interroger. La gouvernance est appliquée à l'exécution via le pipeline.

### Ressources : tables et vues comme pairs

La distinction entre une table et une vue tient uniquement à l'origine — une table est revendiquée à partir d'une source, une vue est définie par un steward. Une fois que l'une ou l'autre existe en tant qu'actif de domaine, le modèle de gouvernance les traite de façon identique :

- Les deux sont des actifs de domaine de premier ordre visibles dans le catalogue
- Les deux peuvent être la cible d'une relation
- Les deux peuvent être accordées selon le principe 6
- Les deux sont soumises au même pipeline de gouvernance

Un steward peut revendiquer des tables de façon privée et n'exposer que des vues sélectionnées comme produits de données destinés au public.

### Composition des vues

Une vue appartient toujours à un seul domaine — il n'existe qu'un seul type de vue, toujours intra-domaine. Une vue existe pour l'une des deux finalités suivantes :

- **Import inter-domaines** — la source est extérieure au domaine. Les données inter-domaines ne peuvent entrer dans un domaine que via une vue, qui agit comme un adaptateur en lecture seule nommant les données externes comme un concept métier du domaine.
- **Dérivation locale** — la source est dans le même domaine. La vue dérive des données nouvelles ou calculées à partir d'actifs de domaine existants. Les données nouvelles ou dérivées ne peuvent exister que sous forme de vue.

Une vue peut référencer :

- Des tables revendiquées au sein du même domaine
- Des champs importés depuis un autre domaine sous un octroi d'accès aux champs
- Une autre vue au sein du même domaine, où la variation est délibérée : restriction de champs, agrégation, ou enrichissement via une jointure supplémentaire

La profondeur de composition n'est pas techniquement contrainte — le jugement du steward lors de la revue HITL est le mécanisme de contrôle de qualité.

Chaque vue porte une finalité métier déclarée, énoncée à sa création :

- Fait partie de l'artefact gouverné — les stewards approuvent en sachant à quoi sert la vue
- Référencée par les demandes d'accès selon le principe 7 afin que le steward puisse en évaluer la pertinence
- Accompagne la vue depuis sa création tout au long du flux de gouvernance

### Requêtes

Une requête parcourt des chemins de relation approuvés sur des actifs de domaine. Contrairement aux vues, les requêtes ne créent pas de nouvelle signification sémantique — elles parcourent la structure approuvée du modèle. Les requêtes peuvent être exprimées dans n'importe quel langage de requête pris en charge (SQL, GraphQL, Cypher).

**Application structurelle :** le catalogue de relations est le mécanisme d'application. Le compilateur valide chaque parcours par rapport aux entrées approuvées du catalogue et rejette les requêtes qui référencent des chemins non approuvés. La gouvernance est structurelle, pas une vérification à l'exécution.

**Aucune approbation requise :** la gouvernance se fait en amont — aux couches Relation et visibilité des colonnes. Si un utilisateur a accès aux colonnes et que le chemin de parcours est approuvé, la requête est un usage valide. Aucun contrôle supplémentaire.

**Distinction avec les vues :**

- Vues : intra-domaine, introduisent une nouvelle signification sémantique, sélectionnées par le steward
- Requêtes : parcourent des relations approuvées, aucune nouvelle sémantique, aucun contrôle d'approbation

**Expression du domaine selon le langage de requête :**

Chaque langage pris en charge fait apparaître le domaine comme un espace de noms structurel natif à ce langage :

| Langage | Expression du domaine | Exemple |
| --- | --- | --- |
| GraphQL | Préfixe du nom de type et de champ | `type sales__Order { ... }`, `query { sales__orders { ... } }` |
| SQL | Nom du schéma | `SELECT * FROM sales.orders` |
| Cypher | Étiquette de nœud supplémentaire (domaine requis uniquement lorsque le nom de type est ambigu) | `MATCH (o:Sales:Order)` |

Le compilateur résout l'appartenance au domaine à partir de ces positions structurelles — aucune annotation ni indication n'est requise.

### Relations

Une relation est un chemin de parcours approuvé entre deux actifs. Les frontières de domaine n'ont aucune incidence sur ce qu'est une relation — elles déterminent seulement qui l'approuve.

**Approbation :**

- L'approbation est requise de la part de chaque steward distinct possédant un actif impliqué dans la relation
- Si un seul steward possède les deux actifs, une seule approbation est requise. Si deux stewards sont impliqués, deux approbations sont requises
- Il n'existe aucune classification intra-domaine / inter-domaines — la propriété détermine naturellement la charge d'approbation
- Approuver une relation construit le graphe de dépendances de chaque steward, permettant des notifications proactives d'évolution de schéma

Les relations sont créées à la demande, non de façon spéculative. La première équipe ayant le besoin métier effectue le travail ; les équipes suivantes héritent de l'infrastructure.

**Conséquence sur l'optimisation :** une déclaration de relation n'est pas seulement un artefact de gouvernance — c'est aussi une description structurelle d'une forme de jointure. Les deux tables, les deux colonnes et le type de jointure qui définissent une relation sont exactement ce dont l'optimiseur de requêtes a besoin pour pré-matérialiser cette jointure. Les relations inter-sources génèrent automatiquement des tables de jointure pré-matérialisées ; les relations de même source peuvent y adhérer via `materialize: true`. Les stewards qui réfléchissent à des relations valides et les approuvent obtiennent une accélération des requêtes comme sous-produit direct — le travail de gouvernance et le travail d'optimisation sont un seul et même acte.

### Octrois d'accès aux champs

Un octroi d'accès aux champs est une permission de domaine à domaine — le domaine A peut utiliser des champs spécifiques du domaine B dans ses vues.

**Cycle de vie de l'octroi :**

- Déclenché par la création d'une vue lorsque des champs étrangers sont identifiés comme nécessaires
- Approuvé une fois par le steward du domaine cible
- Appartient au domaine demandeur, et non à la vue qui l'a déclenché
- Toute vue ultérieure du domaine demandeur peut utiliser les champs accordés sans implication inter-domaines supplémentaire
- Tout champ supplémentaire non accordé nécessite une nouvelle demande

**Notification a posteriori :** lorsqu'une vue est créée en utilisant des champs accordés, le steward source en est notifié — non pas sollicité pour approbation. La notification comprend le nom de la vue, sa finalité métier déclarée, les champs spécifiques utilisés, et quel steward l'a approuvée. Cela procure au steward source :

- **Visibilité** — connaissance de la façon dont ses données sont utilisées
- **Supervision** — des motifs pour soulever une préoccupation si l'usage semble inapproprié
- **Recours** — la possibilité de révoquer l'octroi, invalidant les vues dépendantes

Le compromis : le domaine source approuve l'accès aux champs sans connaître chaque usage futur. Une approbation par vue est correcte en théorie et inapplicable en pratique.

### Flux de création de requête

Trois étapes, dans l'ordre.

**Étape 1 — Modelage (découverte SQL, depuis la page Relations) :**

- L'analyste ouvre l'outil de modelage depuis la page Relations pour explorer les chemins de jointure potentiels en SQL brut
- Le SQL est exécuté sur les données accessibles, soumis à la sécurité au niveau des lignes et au masquage de colonnes existants
- Les JOIN présents dans le SQL sont analysés et présentés comme des propositions de relation candidates
- Les candidats suggérés par la machine (inférence par clé étrangère, inférence sémantique) sont affichés aux côtés de l'exploration SQL de l'analyste dans la même vue
- L'analyste sélectionne les candidats à promouvoir vers une demande de relation formelle

**Étape 2 — Approbation de la relation** (déterminante — structurelle et permanente) :

- Soumise à chaque steward distinct possédant un actif impliqué dans la relation
- S'agit-il d'un chemin de parcours légitime ? La jointure est-elle sémantiquement valide ?
- Tous les stewards impliqués doivent approuver ; la relation devient une entrée permanente du catalogue

**Étape 3 — Création de requête :**

- L'analyste construit la requête dans n'importe quel langage pris en charge (SQL, GraphQL, Cypher), en parcourant des chemins de relation approuvés
- Seules les relations approuvées du catalogue sont franchissables — le compilateur l'impose de façon structurelle
- Aucune approbation requise — la visibilité des colonnes et l'approbation des relations sont les seuls contrôles

### HITL comme contrôle principal

Les règles techniques traitent ce qui est objectif — le suivi de la provenance des champs, l'application des frontières de domaine, la validation par le compilateur. Le jugement contextuel reste du ressort du steward. Des contraintes telles que la profondeur de composition des vues, les exigences de finalité par requête, et les décisions d'approbation de relation relèvent de HITL, et non de règles imposées par le compilateur.

**Neutralité du domaine source :** le steward du domaine source approuve la relation une fois et l'octroi de champs une fois. Ensuite, les domaines en aval opèrent dans les limites de ces octrois :

- **Attention élevée** au moment de la décision de franchissement de frontière
- **Vigilance légère** par la suite, via les notifications et l'historique des requêtes

---

## 2. Découvrabilité

### Paliers de découverte

La découverte est structurée en cinq paliers de gouvernance croissante. Chaque palier est un prérequis pour le suivant.

| Palier | Description | État de gouvernance |
| --- | --- | --- |
| 1 — Schéma de source enregistré | Chaque table, colonne et type d'une source enregistrée. Visibilité au niveau admin. | Aucune — inventaire brut |
| 2 — Tables non revendiquées | Tables introspectées depuis des sources enregistrées sans propriétaire de domaine. Visibles aux stewards ayant accès à la source. | Disponible mais non gouverné |
| 3 — Actifs de domaine | Tables revendiquées et vues définies par le steward. Entièrement gouvernées, possédées, visibles dans le catalogue. | Entièrement gouverné |
| 4 — Relations | Chemins de parcours approuvés entre actifs du palier 3. Prérequis pour la création de vues inter-domaines. | Approuvé par les deux stewards |
| 5 — Octrois de champs | Permissions d'accès aux champs de domaine à domaine. L'accès gouverné le plus spécifique et le plus délibéré. | Approuvé par le steward source |

Une table non revendiquée est un signal de lacune — si les données nécessaires n'existent qu'au palier 2, un steward doit la revendiquer avant que la gouvernance puisse progresser. L'absence de tout candidat à travers tous les paliers requiert une escalade vers l'admin.

### Contraintes de clé étrangère

Les contraintes de clé étrangère sont une construction au niveau de la source — elles ne peuvent pas s'étendre sur plusieurs sources de données. Les chemins de jointure inter-sources sont dérivés entièrement des relations approuvées du catalogue (palier 4), qui sont plus robustes, ayant été validées par les deux stewards.

Au sein d'une source :

- Les contraintes de clé étrangère sont automatiquement présentées comme des relations candidates lors de l'enregistrement de la source
- Elles représentent une intention de modélisation explicite — non appliquée dans la plupart des systèmes SQL analytiques mais délibérément déclarée
- La validation par le steward reste requise avant qu'un candidat ne devienne une relation approuvée

### Hiérarchie de confiance des relations

| Preuve | Confiance |
| --- | --- |
| Relation approuvée du catalogue — inter-sources, validée par les deux stewards | La plus élevée |
| Contrainte de clé étrangère intra-source — intention de modélisation explicite, non appliquée mais délibérée | Élevée |
| Inférence sémantique intra-source — similarité de nom/type de colonne au sein d'un schéma cohérent | Moyenne |
| Inférence sémantique inter-sources — les conventions de nommage divergent entre systèmes ; risque élevé de faux positifs | Faible |

Les suggestions corroborées par plusieurs types de preuves accumulent de la confiance.

### Sondage et corrélation des données

Pour les candidats inférés sémantiquement, le sondage des données fournit une étape de validation :

- **Chevauchement de valeurs** — proportion des valeurs de la colonne source qui apparaissent dans la colonne cible
- **Cardinalité** — si la distribution correspond au type de relation attendu
- **Taux de valeurs nulles** — proportion de la colonne source qui est nulle, indiquant l'optionalité

Une corrélation élevée renforce la confiance ; une corrélation faible supprime ou rétrograde le candidat. Le sondage est une preuve corroborante, pas une démonstration — des plages d'entiers peuvent se chevaucher par coïncidence et l'intégrité référentielle partielle est courante dans les systèmes analytiques. Une marge d'erreur significative subsiste. Le jugement sémantique du steward est le seul contrôle final fiable.

### Découverte assistée par LLM

Le LLM opère simultanément sur les cinq paliers, suggérant des relations, des revendications candidates et des chemins de parcours classés par confiance.

**Ce que le LLM révèle :**

- Des relations candidates classées par confiance
- Des tables non revendiquées susceptibles de satisfaire un besoin de données, avec une invitation à initier la revendication
- L'absence de tout candidat — signal d'escalade vers l'admin

**Conception de vue à partir d'une description métier :**

L'analyste fournit une description en langage naturel et des contraintes optionnelles. Le LLM produit une structure de vue suggérée.

*Entrée :*

- Description métier : entités, métriques, relations, intention
- Contraintes optionnelles : filtres, fenêtres temporelles, agrégations, champs exclus, restrictions de sensibilité

*Exemple :*
> « Volumes d'échanges quotidiens par contrepartie sur les 30 derniers jours, contreparties actives uniquement, affichant la raison sociale de la contrepartie et sa notation de crédit. Sans DCP. »

*Processus du LLM :*

1. Analyse — identifier les entités, métriques, dimensions, filtres, exclusions
2. Recherche — tous les paliers du catalogue à la recherche d'actifs correspondants
3. Suggestion — actifs de domaine, relations, champs, structure d'agrégation
4. Notation — confiance par composant selon les preuves du palier
5. Prérequis — liste ordonnée des revendications, relations et octrois de champs requis
6. Lacunes — entités ou champs sans candidat dans aucun palier, signalés pour escalade vers l'admin

*Sortie :*

- Requête provisoire pour revue et affinage par l'analyste
- Scores de confiance par composant
- Liste ordonnée des prérequis
- Liste des lacunes

La description métier devient la finalité métier déclarée de la vue une fois celle-ci formellement créée.

**Découverte de relations SQL-first (outil de modelage) :**

Accessible sous forme de fenêtre modale depuis la page Relations. L'intention est de construire le modèle sémantique — en identifiant les chemins de jointure structurels avant de les formaliser en relations gouvernées.

1. L'analyste écrit du SQL libre sur les tables accessibles (la sécurité au niveau des lignes et le masquage restent appliqués)
2. L'AST SQL est analysé — chaque condition JOIN devient une proposition de relation candidate
3. La liste des candidats est affichée aux côtés des candidats suggérés par la machine (inférence par clé étrangère, inférence sémantique) pour une revue unifiée
4. L'analyste promeut les candidats sélectionnés vers des demandes de relation formelles
5. Les relations approuvées sont ajoutées au catalogue et deviennent franchissables dans les requêtes

L'outil de modelage peut afficher toutes les tables enregistrées à des fins d'exploration structurelle, même lorsque l'analyste ne peut pas voir les données sous-jacentes — l'approbation du steward gouverne l'accès réel aux données, non la visibilité du schéma.

---

## 3. Usage

### Piste d'audit des requêtes

Chaque requête qui touche un actif de domaine est enregistrée dans un `query_audit_log` en ajout seul. Chaque entrée capture :

- `tenant_id`, `user_id`, `role_id` — le contexte d'identité
- Un hachage SHA-256 de la requête — le texte littéral de la requête n'est jamais stocké
- `table_ids` — les actifs de domaine touchés par la requête
- `source`, `status_code`, `duration_ms`
- `logged_at` — l'horodatage

Le journal est en ajout seul (DELETE et UPDATE bloqués au niveau de la base de données) et indexé par `(tenant_id, logged_at)` et `(user_id, logged_at)`.

Le rapport d'historique des requêtes du steward est une vue agrégée sur ce journal, filtrable par actif, rôle et fenêtre temporelle. Le catalogue est un instrument de gouvernance vivant — les stewards maintiennent une conscience de la façon dont leurs actifs sont utilisés en temps réel, non après coup.

**Deux mécanismes de visibilité :**

- **Poussée** — notifications a posteriori pour les actes structurels (une nouvelle vue a été créée en utilisant vos champs)
- **Extraction** — historique des requêtes pour les schémas d'usage à l'exécution


