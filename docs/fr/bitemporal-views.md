# Créer une vue bitemporelle {: #create-a-bitemporal-view }

Une vue bitemporelle conserve les deux : quand une chose est arrivée et quand elle a été enregistrée. Vous pouvez alors la lire à une date, ou telle qu'elle était enregistrée à une date, et une correction faite plus tard ne masque jamais ce qui était connu avant elle.

Une vue devient bitemporelle quand vous associez trois de ses colonnes :

- **Clé métier** : la ou les colonnes qui identifient le même enregistrement d'une version à l'autre, comme `transaction_id`. Un paiement corrigé reste le même paiement, donc ses deux versions portent la même clé.
- **Temps de validité** : la colonne qui porte la date que votre activité donne à l'enregistrement, par exemple quand il a été convenu, comptabilisé ou est entré en vigueur.
- **Temps d'enregistrement** : la colonne qui porte le moment où cette version a été saisie ou modifiée, comme `updated_at`.

Le temps d'enregistrement s'appelle temps de transaction dans la littérature sur les bases de données temporelles et temps système dans SQL:2011 ; le temps de validité s'appelle temps applicatif dans SQL:2011, ou date d'effet.

Les trois valeurs associent des colonnes que votre source possède déjà. Provisa décide comment les versions sont conservées ; il n'y a pas de mode de stockage à choisir.

## Trois exemples {: #three-examples }

| Exemple | Temps de validité | Temps d'enregistrement |
| ------- | ----------------- | ---------------------- |
| Un paiement de 100 pour le 3 oct. est corrigé à 120 le 5 oct. | 3 oct., conservé par la correction. | 3 oct. pour la première version ; 5 oct. pour la correction. |
| Une mesure de terrain prise le 3 oct. à 14:10 est saisie le 6 oct. Une faute de frappe y est corrigée le 7 oct. | 3 oct. 14:10, conservé par la correction. | 6 oct. ; la correction reçoit le 7 oct. |
| Une transaction est comptabilisée à l'exécution, date de transaction 3 oct., à 10:02. Le 4 oct., sa quantité s'avère fausse et est rectifiée. | Date de transaction 3 oct., conservée par la rectification. | 3 oct. 10:02 ; la rectification reçoit le 4 oct. |

Lue telle que déclarée le 3 oct., la transaction montre la quantité fausse. Lue selon ce que l'on sait aujourd'hui, elle montre la quantité corrigée.

## Créer la vue {: #create-the-view }

Une vue qui conserve un historique est matérialisée, pour que ses versions soient stockées.

1. Ouvrez **Views** et cliquez sur **+ View**.
2. Définissez la vue dans l'un des deux onglets :
    - **Metrics** : nommez la vue, choisissez son domaine, ses métriques et ses dimensions, puis cliquez sur **Continue**.
    - **SQL** : ouvrez l'éditeur SQL, écrivez et exécutez le `SELECT`, cliquez sur **+ View** dans l'éditeur, nommez la vue et cliquez sur **Continue**.
3. Les réglages de la vue s'ouvrent dans les mêmes sections que son éditeur. Dans **Load and timeliness**, cochez **Materialized View**. La section **Time Travel** apparaît.
4. Réglez **Keep history** sur **Bitemporal**.
5. Choisissez les colonnes **Business key**, **Valid time** et **Recorded time**.
6. Réglez le reste dans les autres sections, puis cliquez sur **Save**. Rien n'est enregistré avant.

Les trois se déclarent ensemble ou pas du tout. Provisa refuse une vue qui n'en nomme que certaines et indique celles qui manquent.

Une table reçoit les mêmes sections quand vous l'enregistrez : choisissez-la sous **+ Table**, cliquez sur **Continue**, et sa section Time Travel se trouve dans **Load and timeliness**. Un modèle reçoit la section quand vous le créez (**+ Model**). Pour un modèle, sa clé est la clé métier, donc la section ne demande que les deux temps.

## La lire {: #read-it }

La vue apparaît dans la liste **Views** avec le badge **HISTORY**. Son panneau de détail indique qu'elle conserve un historique et renvoie vers l'explorateur SQL avec la vue sélectionnée. Réglez les sélecteurs **As of** et **As recorded at** de l'explorateur sur les moments voulus, puis lancez la requête.

Les mêmes temps atteignent toutes les autres surfaces. Une session pgwire les fixe une fois pour chaque instruction qu'elle exécute :

```sql
SET provisa.as_of = '2026-10-03T12:00:00';
SET provisa.as_recorded = '2026-10-03T23:59:59';
```

Les en-têtes HTTP, les arguments de `resolve` et les paramètres REST sont décrits dans [Vues avec historique](configuration.md#views-with-history-time-travel).

## La modifier plus tard {: #change-it-later }

L'éditeur affiche la section Time Travel dans l'onglet **Load and timeliness** de la vue. Une fois la vue construite, ses versions sont enregistrées sous sa clé métier et son temps de validité ; l'éditeur ne propose alors plus ces deux valeurs, ni l'interrupteur qui désactive Time Travel. Le temps d'enregistrement peut encore changer. Pour organiser l'historique sous une autre clé, créez une nouvelle vue.
