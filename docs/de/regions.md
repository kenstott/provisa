# Regionen {: #regions }

Provisa gibt Ihnen zwei Grundbausteine, um Daten zu platzieren: eine **Region** und die **Region-Zuordnung**, also das Anlegen einer Region und das Zuweisen einer Tabelle zu ihr. Keiner der beiden ist für sich eine Richtlinie. Wofür Sie sie einsetzen, fällt in drei Gruppen:

- **Leistung.** Die Latenz zu den Lesenden, wie aktuell ihre Kopie ist, und die Eignung der Engine: wo ihr Rechenzentrum steht, welche Art von Speicher sie liest, wie viel CPU und Arbeitsspeicher sie hat.
- **Kosten.** Wo Arbeit läuft und worauf: eigene Hardware oder Cloud, eine große oder eine kleine Engine, und was das Bewegen von Daten zwischen Orten kostet (Egress).
- **Souveränität.** Wer Daten kontrolliert und wohin sie dürfen: die gesetzlichen Residenzregeln (wo personenbezogene Daten verarbeitet und wo sie gespeichert werden dürfen) und die eigenen Kontrolllinien einer Organisation, etwa ein Geschäftsbereich oder eine Tochtergesellschaft, die ihre Daten und Infrastruktur unter eigener Kontrolle behält.

Die Mechanik ist für alle drei dieselbe. Nur die Regeln, die Sie darüber schreiben, unterscheiden sich.

## Eine Region ist eine Engine mit ihren Speichern an einem Ort {: #a-region-is-an-engine-and-its-stores-in-a-place }

Eine Region ist ein Ort, an dem Provisa arbeitet und aufbewahrt, was die Arbeit hinterlässt. Jede Region hat ihre eigene Engine und ihre eigenen Speicher.

| Speicher | Was er enthält |
| --- | --- |
| `engine` | Die Föderations-Engine, die in dieser Region Anweisungen ausführt |
| `replicas` | Replikate der hier zugeordneten Tabellen und der nicht zugeordneten Tabellen, die diese Region kopiert |
| `views` | Die Kopien materialisierter Views in dieser Region |
| `cache` | Gecachte Antworten |
| `state` | Build- und Aktualisierungsstatus |
| `record` | Der Datensatz jeder hier bedienten Anfrage |

Ein Modell steuert jede Region. Quellen, Tabellen, Beziehungen, Rollen, Zeilen- und Spaltenregeln, Views und Aktualitätseinstellungen werden einmal in einem Modellspeicher deklariert, und jede Region bedient dasselbe Modell, sodass Definitionen zwischen Regionen nicht auseinanderlaufen können. Eine Änderung erreicht jede Region über denselben Stempel, und jede Region gleicht sich dann daran an.

Die Plattform deklariert die physischen Regionen, in denen ihre Knoten laufen, mit der Adresse, unter der jede Region antwortet. Das Modell jeder Organisation wählt aus, welche dieser Regionen sie nutzt, und benennt für jede Rolle in jeder Region ihren eigenen Speicher. Ein Knoten startet in einer Region mit `provisa run --region <id>` und bedient diese Region für jede Organisation, die sie auswählt. Sind Regionen deklariert, verweigert ein ohne `--region` gestarteter Knoten den Start und listet die Regionen auf, die er bedienen könnte.

```yaml
platform:
  regions:
    - { id: eu, address: "https://eu.provisa.example" }
    - { id: us, address: "https://us.provisa.example" }

stores:
  - { id: eu_engine, url: "${secret:eu-trino-url}", kind: trino }
  - { id: us_engine, url: "${secret:us-trino-url}", kind: trino }
  - { id: eu_data,   url: "${secret:eu-pg-url}" }
  - { id: us_data,   url: "${secret:us-pg-url}" }
  - { id: eu_cache,  url: "${secret:eu-redis-url}" }
  - { id: us_cache,  url: "${secret:us-redis-url}" }

regions:
  - { id: eu, engine: eu_engine, replicas: eu_data, views: eu_data, cache: eu_cache, state: eu_data, record: eu_data }
  - { id: us, engine: us_engine, replicas: us_data, views: us_data, cache: us_cache, state: us_data, record: us_data }
```

Beim Laden wird die Konfiguration mit benanntem Grund abgelehnt, wenn die Regionen nicht zusammenpassen. Eine Region muss für jede Rolle einen deklarierten Speicher nennen. Ihre `replicas` und `views` müssen derselbe Speicher sein, und ihr Engine-Speicher muss eine Engine-`kind` nennen. Bei mehr als einer Plattformregion wird eine eingebettete Engine-Datei abgelehnt, weil die anderen Regionen sie nicht erreichen können.

Mehrere Regionen dürfen sich eine Datenbankinstanz, ein Redis oder einen Objektspeicher-Bucket teilen. Alles, was eine Region aufbewahrt, trägt die Region im Namen, sodass ihre Speicher nicht kollidieren.

Ein Deployment, das keine Regionen deklariert, hat eine implizite Region und zeigt Regionen nirgends an. Es gibt kein Regionsfeld und kein `--region`-Flag, und eine Konfiguration, die eine Region nennt, wird abgelehnt.

## Region-Zuordnung: eine Tabelle nennt ihre Heimat {: #region-homing-a-table-names-its-home }

Eine Tabelle kann eine der Regionen ihrer Organisation als Heimat nennen. Ihre Heimat ist die einzige Region, die ihr Replikat aufbaut und aktualisiert, und die einzige, die sie hot halten darf. Jede andere Region liest die Tabelle aus dem Replikat der Heimatregion, an Ort und Stelle, unter der Adresse, die das Modell angibt. Die Zeilen- und Spaltenregeln der Lesenden gehen in diese Anweisung ein, sodass gefiltert wird, wo die Daten zu Hause sind, und nur das die Grenze überquert, was die Lesenden sehen dürfen.

```yaml
tables:
  - source_id: eu-shop
    schema_name: public
    table_name: customers
    region: eu
```
Einige Folgen:

- Keine andere Region liest die Quelle der Tabelle; sie lesen nur das Replikat der Heimat. Eine Quelle, die Sie replizieren, um sie vor Last zu schützen, bleibt geschützt.
- Ist das Replikat nicht aufgebaut oder die Heimatregion nicht erreichbar, wird die Anfrage abgelehnt und die Heimatregion genannt. Die Quelle wird nicht stattdessen live gelesen.
- Eine zugeordnete Tabelle, die nicht repliziert wird, mit `replicate` Never oder einer Einstellung, die noch keine Kopie verlangt hat, wird von jeder Region live aus ihrer Quelle gelesen, wie jede nicht replizierte Tabelle.
- Der Hot-Zähler einer Tabelle ist nur der Verkehr ihrer Heimatregion.
- Auch eine materialisierte View kann eine Region nennen. Sie wird dann nur dort aufgebaut, und jede andere Region liest diese Kopie, genau wie bei einer zugeordneten Tabelle.

Eine Region, die das Replikat einer anderen Region liest, braucht eine Engine, die es lesen kann: `duckdb`, `motherduck`, `pg`, `trino` oder `trino-byo`. Der `replicas`-Speicher der Heimatregion muss ein PostgreSQL-Server sein. Die Konfiguration nennt beides, wenn es nicht passt.

Auch eine Quelle kann eine Region nennen, doch diese Region füllt nur das Formular vor, wenn Sie eine neue Tabelle der Quelle registrieren. Sie entscheidet nichts darüber, wo Daten liegen, und eine spätere Änderung verschiebt keine Tabelle.

## Nicht zugeordnete Tabellen werden dort kopiert, wo sie gelesen werden {: #unhomed-tables-are-copied-where-they-are-read }

Eine Tabelle, die keine Region nennt, darf in jede Region kopiert werden. Wenn ihre `replicate`-Einstellung eine Kopie verlangt, baut die Region, mit der sich ein Lesender verbunden hat, eine in ihrem eigenen Speicher auf. Eine materialisierte View, die keine Region nennt, wird in jeder Region aufgebaut, jede Kopie im View-Speicher dieser Region.

Eine weitere Region bedeutet daher mehr Kopien und mehr Aktualisierungsarbeit. Mehr Governance bedeutet sie nie. Ein Modell steuert jede Kopie, und in jeder Region gelten dieselben Zeilen- und Spaltenregeln. Der Build einer View in einer Region wird als Administrator der Organisation in dieser Region gesteuert, sodass die Kopie jeder Region enthält, was dort aufbewahrt werden darf.

## Drei Gründe für Regionen {: #three-reasons-to-use-regions }

### Leistung {: #performance }

Regionen, die nur der Leistung dienen, deklarieren keine regionsübergreifenden Regeln. Tabellen, Rollen und Regeln sind dieselben, die Sie für eine einzige Region schreiben würden, und die Region-Zuordnung entscheidet, wo gearbeitet wird:

- **Latenz.** Lesende verbinden sich mit der Region in ihrer Nähe, und deren Engine, Cache und Kopien sind lokal für sie.
- **Aktualität.** Eine Tabelle, die dort zugeordnet ist, wo sie sich ändert, wird dort aktualisiert, nahe an ihrer Quelle.
- **Eignung der Engine.** Jede Region betreibt ihre eigene Engine, sodass Sie eine Engine einem Ort anpassen können: dem Rechenzentrum, in dem sie steht, dem Speicher, den sie liest, und der CPU und dem Arbeitsspeicher, die sie hat.

**Beispiel: zwei identische Regionen.** Deklarieren Sie `east` und `west` in zwei Rechenzentren, jede mit derselben Engine-Art und denselben Speicherarten, und ordnen Sie keine Tabellen zu. Jede Region bedient die Lesenden in ihrer Nähe: Sie baut ihre eigenen Kopien dessen auf, was diese lesen, und cacht ihre Antworten. Die beiden können hinter einem Load Balancer stehen und sich die Last teilen. Es gibt einen Aktualisierungszeitplan, den des Modells, und beide Regionen beginnen danach, ihre Kopien zu aktualisieren, doch ihre Engines werden womöglich nicht in derselben Zeit fertig. Zu einem gegebenen Zeitpunkt ist also nicht garantiert, dass beide im selben Zustand sind, und ein Lesender, der von einer Region in die andere wechselt, kann Daten anderer Aktualität sehen. Provisa verschiebt einen Client nicht in die andere Region, wenn eine stoppt; wohin ein Client geht, entscheidet der Load Balancer.

### Kosten {: #cost }

Auch Kostenregionen deklarieren keine regionsübergreifenden Regeln. Sie legen Arbeit dorthin, wo sie am günstigsten läuft:

- **Eigene Hardware und Cloud.** Betreiben Sie eine Region vor Ort, auf Hardware, die Ihnen bereits gehört, für die Tabellen und Lesenden, die dort sind, und eine andere Region in der Cloud für den Rest.
- **Größe der Engine.** Betreiben Sie in einer Region eine große Engine für gelegentliche schwere Arbeit und in einer anderen eine kleine für alltägliche Lesezugriffe. Lesende wählen die Region über ihre Adresse.
- **Egress.** Lesende in anderen Regionen erhalten nur die gefilterten Zeilen aus dem Replikat einer zugeordneten Tabelle, und eine große Tabelle, die einer Region zugeordnet ist, wird nicht in jede Region kopiert.

### Souveränität {: #sovereignty }

Regionen für die Souveränität deklarieren regionsübergreifende Regeln, ob der Grund das Gesetz oder die Organisation ist: Eine Tochtergesellschaft, die ihre Tabellen ihrer eigenen Region zuordnet, lässt sie auf ihrer eigenen Infrastruktur verarbeiten und speichern und entscheidet über diese Regeln, was der Rest der Gruppe liest. Es sind gewöhnliche Zeilen- und Spaltenregeln, geschrieben auf Attribute, die sagen, wo sich der Lesende oder der arbeitende Knoten befindet:

- `current_setting('provisa.region')` ist die Region des Knotens, der die Anfrage beantwortet, wenn die Plattform Regionen deklariert. Ein Zeilenfilter darauf hält die Zeilen einer Region in dieser Region.
- Das Attribut `region` eines Mitglieds lässt es die abgeleiteten Rollen `region` und `region__<value>` halten, sodass ein Masken- oder Freigabeeintrag `analyst+region__eu` nennen kann. Die eigene Hintergrundarbeit eines Knotens trägt seine Region auf dieselbe Weise. Siehe [Rollen: gewährt, abgeleitet und meta](security.md#roles-granted-derived-and-meta).

Zusammen mit der Zuordnung entscheiden diese Regeln, was eine Grenze überquert. Die personenbezogenen Daten einer zugeordneten Tabelle werden in ihrer Heimatregion gespeichert und verarbeitet, und ein Lesender anderswo erhält nur, was seine Regeln erlauben, gefiltert in der Heimat. Eine nicht zugeordnete View, die in jeder Region aufgebaut wird, enthält, was ein Administrator in dieser Region sehen darf, sodass die Kopie in einer Region Zeilen enthalten kann, die die Kopie in einer anderen Region auslässt.

## Wer eine Tabelle zuordnen darf {: #who-may-home-a-table }

Das Setzen, Ändern oder Entfernen der Region einer Tabelle, einer Quelle oder einer materialisierten View bewegt Daten über eine Grenze. Es braucht das Recht `data_residency` zusätzlich zum Besitz des Objekts über seine Domäne. Die Freigabe nennt die Werte, die sie abdeckt: die Regionen der Organisation und "keine Region". Ein Objekt zwischen zwei Regionen zu verschieben braucht eine Freigabe, die beide abdeckt. Eine Speicherung außerhalb der Freigabe wird abgelehnt und der nicht abgedeckte Wert genannt.

Das Recht existiert nur, wenn die Plattform Regionen deklariert. Wer alles andere am Objekt bearbeiten darf, entscheidet weiterhin der Domänenbesitz. Eine Region verleiht kein eigenes Recht; eine Organisation, die die Tabellen einer Region von deren Leuten verwalten lassen will, gibt ihnen eigene Domänen.

Eine Tabelle oder View kann auch auf **Entwurf** gesetzt werden: außer Betrieb und keinem Leser angeboten. Eine über den Admin registrierte Tabelle beginnt als Entwurf. Ihr Steward zeigt sie in der Vorschau nur auf den Knoten der Region, die sie nennt (eine ohne Region dort, wo der Steward ist), und die Kopie, die diese Vorschau landet, wird in dieser Region für die Freigabe der Tabelle aufbewahrt. Eine Kopie, die eine andere Region von ihr hält, aus der Zeit vor dem Setzen oder Ändern ihrer Region, wird wie bei einer veröffentlichten Tabelle stillgelegt und gelöscht. Ein Umzug zwischen Regionen kann über den Entwurf laufen, sodass die Tabelle dazwischen nirgends bereitgestellt wird: Die Seite der ersten Region setzt den Entwurf, und der Halter der zweiten Region übernimmt das Objekt, indem er seine Region setzt.

Siehe auch die [Demo mit zwei Regionen](regions-demo.md).
