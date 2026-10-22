# Principes du modèle de domaine {: #domain-model-principles }

---

## 1. Gouvernance {: #1-governance }

### Principes fondamentaux {: #core-principles }

1. **Chaque ressource doit appartenir à un domaine.** Les tables, vues et relations sont toutes des actifs de domaine. Il n'existe aucune ressource flottante non gouvernée. Le domaine est l'unité de responsabilité.
2. **Chaque domaine doit avoir un steward.** Un domaine peut exister à l'état en attente jusqu'à ce qu'un steward lui soit assigné, mais il ne peut servir de données gouvernées sans en avoir un.
3. **L'admin possède les sources.** Les sources sont de l'infrastructure, pas des ressources de domaine. L'admin enregistre et gère les connexions aux systèmes de données externes.
4. **Les intendants peuvent revendiquer des tables pour un domaine.** Une revendication est exclusive : une table appartient à exactement un domaine. Revendiquer est l'acte gouverné qui relie l'infrastructure au modèle.
5. **Les intendants peuvent créer des vues intra-domaine à partir des actifs du domaine.** Les vues expriment une logique métier, comme des jointures, des agrégations et des métriques dérivées, sur des actifs que l'intendant possède dans le même domaine. Une vue crée un sens sémantique nouveau et demande l'approbation de l'intendant.
6. **Les analystes peuvent créer des requêtes inter-domaines à partir de relations approuvées.** Les requêtes sont des vues inter-domaines écrites dans n'importe quel langage de requête pris en charge. Elles ne créent aucune sémantique nouvelle ; elles parcourent des chemins de relation approuvés. Aucune autre approbation n'est nécessaire, car la gouvernance intervient plus tôt, aux niveaux relation et visibilité des colonnes. C'est le catalogue qui impose : le compilateur refuse tout parcours absent du catalogue des relations approuvées.
7. **Chacun peut demander l'accès à une ressource de domaine.** L'accès s'accorde par ressource, jamais par requête : si vous avez accès à une ressource, vous pouvez l'interroger. La gouvernance s'applique à l'exécution, dans le pipeline.

### Ressources : tables et vues comme pairs {: #resources-tables-and-views-as-peers }

Une table et une vue ne diffèrent que par leur origine : une table est revendiquée depuis une source, une vue est définie par un intendant. Dès que l'une ou l'autre est un actif de domaine, le modèle de gouvernance les traite de la même façon :

- Les deux sont des actifs de domaine à part entière, visibles dans le catalogue
- Les deux peuvent être la cible d'une relation
- Les deux peuvent être accordées selon le principe 6
- Les deux sont soumises au même pipeline de gouvernance

Un steward peut revendiquer des tables de façon privée et n'exposer que des vues sélectionnées comme produits de données destinés au public.

### Composition des vues {: #view-composition }

Une vue appartient toujours à un seul domaine. Il n'existe qu'un type de vue, toujours intra-domaine. Une vue sert l'un de deux objectifs :

- **Import inter-domaines** — la source est extérieure au domaine. Les données inter-domaines ne peuvent entrer dans un domaine que via une vue, qui agit comme un adaptateur en lecture seule nommant les données externes comme un concept métier du domaine.
- **Dérivation locale** — la source est dans le même domaine. La vue dérive des données nouvelles ou calculées à partir d'actifs de domaine existants. Les données nouvelles ou dérivées ne peuvent exister que sous forme de vue.

Une vue peut référencer :

- Des tables revendiquées au sein du même domaine
- Des champs importés depuis un autre domaine sous un octroi d'accès aux champs
- Une autre vue au sein du même domaine, où la variation est délibérée : restriction de champs, agrégation, ou enrichissement via une jointure supplémentaire

Rien de technique ne limite la profondeur de composition des vues. Le contrôle qualité, c'est le jugement de l'intendant lors de la revue HITL.

Chaque vue porte une finalité métier déclarée, énoncée à sa création :

- Fait partie de l'artefact gouverné, si bien que les stewards approuvent en sachant à quoi sert la vue
- Référencée par les demandes d'accès selon le principe 7 afin que le steward puisse en évaluer la pertinence
- Accompagne la vue depuis sa création tout au long du flux de gouvernance

### Requêtes {: #queries }

Une requête parcourt des chemins de relation approuvés sur des actifs de domaine. Contrairement à une vue, elle ne crée aucun sens sémantique nouveau ; elle suit la structure approuvée du modèle. Une requête peut s'écrire dans n'importe quel langage de requête pris en charge (SQL, GraphQL, Cypher).

**Application structurelle :** C'est le catalogue des relations qui impose. Le compilateur vérifie chaque parcours par rapport aux entrées approuvées du catalogue et refuse une requête qui nomme un chemin non approuvé. La gouvernance est inscrite dans la structure ; il n'y a pas de contrôle séparé à l'exécution.

**Aucune approbation requise :** La gouvernance intervient plus tôt, aux niveaux relation et visibilité des colonnes. Si un utilisateur peut voir les colonnes et que le chemin est approuvé, la requête est un usage valide, sans autre barrière.

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

Le compilateur déduit l'appartenance au domaine de ces positions structurelles ; aucune annotation ni indication n'est nécessaire.

### Relations {: #relationships }

Une relation est un chemin de parcours approuvé entre deux actifs. Les frontières de domaine ne changent rien à ce qu'est une relation ; elles décident seulement qui l'approuve.

**Approbation :**

- Une demande de relation est tranchée par les domaines qu'elle touche : le domaine de sa table source et le domaine de sa table cible
- Une approbation ne compte que si elle vient d'un utilisateur dont le droit de créer des relations atteint l'un de ces domaines ; tout utilisateur de ce type peut la rejeter
- La demande est exécutée dès que deux utilisateurs différents, dont aucun n'est le demandeur, l'ont approuvée et que chaque domaine concerné a un approbateur : une relation entre deux domaines a besoin d'un oui de chaque côté, et une relation à l'intérieur d'un domaine a besoin de deux approbateurs de ce domaine
- Approuver une relation construit le graphe de dépendances de chaque steward, si bien que les stewards apprennent les changements de schéma avant qu'ils ne s'appliquent

Les relations se créent quand quelqu'un en a besoin, jamais par anticipation. La première équipe qui a le besoin métier fait le travail, et les équipes suivantes en héritent.

**Conséquence pour l'optimisation :** Une déclaration de relation est un artefact de gouvernance et aussi une description structurelle d'une forme de jointure. Les deux tables, les deux colonnes et le type de jointure qui définissent une relation sont exactement ce dont l'optimiseur a besoin pour prématérialiser cette jointure. Les relations inter-sources reçoivent automatiquement des tables de jointure prématérialisées, et les relations au sein d'une même source peuvent l'activer avec `materialize: true`. Les intendants qui réfléchissent à des relations valides et les approuvent obtiennent des requêtes plus rapides en conséquence directe : le travail de gouvernance et le travail d'optimisation sont le même acte.

### Octrois d'accès aux champs {: #field-access-grants }

Une autorisation d'accès aux champs est une permission d'un domaine à un autre : le domaine A peut utiliser des champs précis du domaine B dans ses vues.

**Cycle de vie de l'octroi :**

- Déclenché par la création d'une vue lorsque des champs étrangers sont identifiés comme nécessaires
- Approuvé une fois par le steward du domaine cible
- Appartient au domaine demandeur, et non à la vue qui l'a déclenché
- Toute vue ultérieure du domaine demandeur peut utiliser les champs accordés sans implication inter-domaines supplémentaire
- Tout champ supplémentaire non accordé nécessite une nouvelle demande

**Notification après usage :** Quand une vue est créée avec des champs autorisés, l'intendant de la source en est informé, sans qu'on lui demande d'approuver. La notification donne le nom de la vue, son objectif métier déclaré, les champs utilisés et l'intendant qui l'a approuvée. Cela donne à l'intendant de la source :

- **Visibilité** — connaissance de la façon dont ses données sont utilisées
- **Supervision** — des motifs pour soulever une préoccupation si l'usage semble inapproprié
- **Recours** — la possibilité de révoquer l'octroi, invalidant les vues dépendantes

Le compromis : le domaine source approuve l'accès aux champs sans connaître chaque usage futur. Une approbation par vue est correcte en théorie et inapplicable en pratique.

### Flux de création de requête {: #query-creation-workflow }

Trois étapes, dans l'ordre.

**Étape 1 — Modelage (découverte SQL, depuis la page Relations) :**

- L'analyste ouvre l'outil de modelage depuis la page Relations pour essayer des chemins de jointure en SQL brut
- Le SQL est exécuté sur les données accessibles, soumis à la sécurité au niveau des lignes et au masquage de colonnes existants
- Les JOIN présents dans le SQL sont analysés et présentés comme des propositions de relation candidates
- Les candidats suggérés par la machine (inférence par clé étrangère, inférence sémantique) sont affichés aux côtés du SQL de l'analyste dans la même vue
- L'analyste sélectionne les candidats à promouvoir vers une demande de relation formelle

**Étape 2 — Approbation de la relation** (déterminante — structurelle et permanente) :

- Décidée par les domaines que la relation touche, comme décrit sous [Relations](#relationships)
- Est-ce un chemin de parcours légitime ? La jointure est-elle valide sur le plan sémantique ?
- Deux utilisateurs différents, dont aucun n'est le demandeur, doivent approuver, et chaque domaine concerné doit avoir un approbateur ; la relation devient alors une entrée permanente du catalogue

**Étape 3 — Création de requête :**

- L'analyste construit la requête dans n'importe quel langage pris en charge (SQL, GraphQL, Cypher), en parcourant des chemins de relation approuvés
- Seules les relations approuvées du catalogue sont franchissables, et le compilateur l'impose de façon structurelle
- Aucune approbation requise : la visibilité des colonnes et l'approbation des relations sont les seuls contrôles

### HITL comme contrôle principal {: #hitl-as-the-primary-control }

Les règles techniques prennent en charge ce qui est objectif : le suivi de la provenance des champs, l'application des frontières de domaine et la validation par le compilateur. Le jugement qui dépend du contexte reste à l'intendant. Des limites comme la profondeur de composition des vues, un objectif déclaré pour chaque requête et les décisions d'approbation des relations relèvent du HITL, que le compilateur n'impose pas.

**Neutralité du domaine source :** le steward du domaine source approuve la relation une fois et l'octroi de champs une fois. Ensuite, les domaines en aval opèrent dans les limites de ces octrois :

- **Attention élevée** au moment de la décision de franchissement de frontière
- **Vigilance légère** par la suite, via les notifications et l'historique des requêtes

---

## 2. Découvrabilité {: #2-discoverability }

### Paliers de découverte {: #discovery-tiers }

La découverte est structurée en cinq paliers de gouvernance croissante. Chaque palier est un prérequis pour le suivant.

| Palier | Description | État de gouvernance |
| --- | --- | --- |
| 1 — Schéma de source enregistré | Chaque table, colonne et type d'une source enregistrée. Visibilité au niveau admin. | Aucune — inventaire brut |
| 2 — Tables non revendiquées | Tables introspectées depuis des sources enregistrées sans propriétaire de domaine. Visibles aux stewards ayant accès à la source. | Disponible mais non gouverné |
| 3 — Actifs de domaine | Tables revendiquées et vues définies par le steward. Entièrement gouvernées, possédées, visibles dans le catalogue. | Entièrement gouverné |
| 4 — Relations | Chemins de parcours approuvés entre actifs du palier 3. Prérequis pour la création de vues inter-domaines. | Approuvé par les deux stewards |
| 5 — Octrois de champs | Permissions d'accès aux champs de domaine à domaine. L'accès gouverné le plus spécifique et le plus délibéré. | Approuvé par le steward source |

Une table non revendiquée signale un manque. Si les données dont vous avez besoin n'existent qu'au niveau 2, un intendant doit la revendiquer avant que la gouvernance puisse avancer. Quand aucun niveau n'a de candidat, remontez à un administrateur.

### Contraintes de clé étrangère {: #fk-constraints }

Les contraintes FK appartiennent à une seule source, elles ne peuvent donc pas en enjamber plusieurs. Les chemins de jointure inter-sources viennent entièrement des relations approuvées du catalogue (niveau 4), une preuve plus solide puisque les deux intendants les ont validées.

Au sein d'une source :

- Les contraintes de clé étrangère sont automatiquement présentées comme des relations candidates lors de l'enregistrement de la source
- Elles représentent une intention de modélisation explicite : non appliquée dans la plupart des systèmes SQL analytiques mais délibérément déclarée
- La validation par le steward reste requise avant qu'un candidat ne devienne une relation approuvée

### Hiérarchie de confiance des relations {: #relationship-confidence-hierarchy }

| Preuve | Confiance |
| --- | --- |
| Relation approuvée du catalogue — inter-sources, validée par les deux stewards | La plus élevée |
| Contrainte de clé étrangère intra-source — intention de modélisation explicite, non appliquée mais délibérée | Élevée |
| Inférence sémantique intra-source — similarité de nom/type de colonne au sein d'un schéma cohérent | Moyenne |
| Inférence sémantique inter-sources — les conventions de nommage divergent entre systèmes ; risque élevé de faux positifs | Faible |

Les suggestions corroborées par plusieurs types de preuves accumulent de la confiance.

### Sondage et corrélation des données {: #data-probing-and-correlation }

Pour les candidats inférés sémantiquement, le sondage des données fournit une étape de validation :

- **Chevauchement de valeurs** — proportion des valeurs de la colonne source qui apparaissent dans la colonne cible
- **Cardinalité** — si la distribution correspond au type de relation attendu
- **Taux de valeurs nulles** — proportion de la colonne source qui est nulle, indiquant l'optionalité

Une forte corrélation augmente la confiance, une faible corrélation supprime ou rétrograde le candidat. Le sondage corrobore ; il ne prouve pas. Des plages d'entiers peuvent se chevaucher par hasard et une intégrité référentielle partielle est courante dans les systèmes analytiques, donc la marge d'erreur reste grande. Le jugement sémantique de l'intendant est le seul contrôle final fiable.

### Découverte assistée par LLM {: #llm-assisted-discovery }

Le LLM opère simultanément sur les cinq paliers, suggérant des relations, des revendications candidates et des chemins de parcours classés par confiance.

**Ce que le LLM révèle :**

- Des relations candidates classées par confiance
- Des tables non revendiquées susceptibles de satisfaire un besoin de données, avec une invitation à initier la revendication
- L'absence de tout candidat est le signal d'escalade vers l'admin

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

Il s'ouvre en fenêtre modale depuis la page Relations. Son but est de construire le modèle sémantique : trouver des chemins de jointure structurels avant de les formaliser en relations gouvernées.

1. L'analyste écrit du SQL libre sur les tables accessibles (la sécurité au niveau des lignes et le masquage restent appliqués)
2. L'AST SQL est analysé, et chaque condition JOIN devient une proposition de relation candidate
3. La liste des candidats est affichée aux côtés des candidats suggérés par la machine (inférence par clé étrangère, inférence sémantique) pour une revue unifiée
4. L'analyste promeut les candidats sélectionnés vers des demandes de relation formelles
5. Les relations approuvées sont ajoutées au catalogue et deviennent franchissables dans les requêtes

Pendant que vous mettez au point la structure, l'outil de modélisation peut montrer toutes les tables enregistrées, y compris celles dont l'analyste ne peut pas voir les données. La visibilité du schéma est une autre question ; c'est l'approbation de l'intendant qui régit l'accès aux données elles-mêmes.

---

## 3. Usage

### Piste d'audit des requêtes {: #query-audit-trail }

Chaque requête qui touche un actif de domaine est enregistrée dans un `query_audit_log` en ajout seul. Chaque entrée capture :

- `tenant_id`, `user_id`, `role_id` — le contexte d'identité
- Un hachage SHA-256 de la requête ; le texte littéral de la requête n'est jamais stocké
- `table_ids` — les actifs de domaine touchés par la requête
- `source`, `status_code`, `duration_ms`
- `logged_at` — l'horodatage

Le journal est en ajout seul (DELETE et UPDATE bloqués au niveau de la base de données) et indexé par `(tenant_id, logged_at)` et `(user_id, logged_at)`.

Le rapport d'historique des requêtes de l'intendant est une vue agrégée sur ce journal, filtrable par actif, rôle et fenêtre de temps. Le catalogue devient ainsi un instrument de gouvernance vivant : les intendants voient comment leurs actifs sont utilisés au moment où cela se passe, sans l'apprendre après coup.

**Deux mécanismes de visibilité :**

- **Poussée** — notifications a posteriori pour les actes structurels (une nouvelle vue a été créée en utilisant vos champs)
- **Extraction** — historique des requêtes pour les schémas d'usage à l'exécution
