# Eine bitemporale Sicht anlegen {: #create-a-bitemporal-view }

Eine bitemporale Sicht hält beides fest: wann etwas geschah und wann es erfasst wurde. Sie können sie dann zum Stand eines Datums lesen oder so, wie sie an einem Datum erfasst war, und eine spätere Korrektur verdeckt nie, was vor ihr bekannt war.

Eine Sicht wird bitemporal, indem Sie drei ihrer Spalten zuordnen:

- **Geschäftsschlüssel**: die Spalte oder Spalten, die denselben Datensatz über Versionen hinweg kennzeichnen, etwa `transaction_id`. Eine korrigierte Zahlung bleibt dieselbe Zahlung, also tragen beide Versionen denselben Schlüssel.
- **Gültigkeitszeit**: die Spalte mit dem Datum, das Ihr Geschäft dem Datensatz gibt, etwa wann er vereinbart, gebucht oder wirksam wurde.
- **Erfassungszeit**: die Spalte mit dem Zeitpunkt, zu dem diese Version eingegeben oder geändert wurde, etwa `updated_at`.

Die Erfassungszeit heißt in der Literatur zu temporalen Datenbanken Transaktionszeit und in SQL:2011 Systemzeit; die Gültigkeitszeit heißt in SQL:2011 Anwendungszeit oder auch Stichtag des Inkrafttretens.

Die drei Werte ordnen Spalten zu, die Ihre Quelle bereits hat. Provisa entscheidet, wie die Versionen gespeichert werden; es gibt keinen Speichermodus zu wählen.

## Drei Beispiele {: #three-examples }

| Beispiel | Gültigkeitszeit | Erfassungszeit |
| -------- | --------------- | -------------- |
| Eine Zahlung über 100 für den 3. Okt. wird am 5. Okt. auf 120 korrigiert. | 3. Okt., von der Korrektur beibehalten. | 3. Okt. für die erste Version; 5. Okt. für die Korrektur. |
| Eine Feldmessung vom 3. Okt. 14:10 wird am 6. Okt. eingetippt. Ein Tippfehler darin wird am 7. Okt. behoben. | 3. Okt. 14:10, von der Behebung beibehalten. | 6. Okt.; die Behebung erhält den 7. Okt. |
| Ein Trade wird bei Ausführung gebucht, Handelstag 3. Okt., um 10:02. Am 4. Okt. stellt sich die Menge als falsch heraus und wird berichtigt. | Handelstag 3. Okt., von der Berichtigung beibehalten. | 3. Okt. 10:02; die Berichtigung erhält den 4. Okt. |

Gelesen wie am 3. Okt. gemeldet, zeigt der Trade die falsche Menge. Gelesen nach heutigem Kenntnisstand, zeigt er die berichtigte.

## Die Sicht anlegen {: #create-the-view }

Eine Sicht mit Historie ist materialisiert, damit ihre Versionen gespeichert werden.

1. Öffnen Sie **Views** und klicken Sie auf **+ View**.
2. Definieren Sie die Sicht auf einem der beiden Reiter:
    - **Metrics**: Benennen Sie die Sicht, wählen Sie Domain, Metriken und Dimensionen und klicken Sie auf **Continue**.
    - **SQL**: Öffnen Sie den SQL-Editor, schreiben und starten Sie das `SELECT`, klicken Sie im Editor auf **+ View**, benennen Sie die Sicht und klicken Sie auf **Continue**.
3. Die Einstellungen der Sicht öffnen sich in denselben Abschnitten wie ihr Editor. Setzen Sie unter **Load and timeliness** das Häkchen bei **Materialized View**. Der Abschnitt **Time Travel** erscheint.
4. Stellen Sie **Keep history** auf **Bitemporal**.
5. Wählen Sie die Spalten für **Business key**, **Valid time** und **Recorded time**.
6. Legen Sie in den übrigen Abschnitten fest, was Sie sonst brauchen, und klicken Sie auf **Save**. Bis dahin wird nichts registriert.

Die drei werden zusammen oder gar nicht angegeben. Provisa weist eine Sicht ab, die nur einige davon nennt, und sagt, welche fehlen.

Eine Tabelle erhält dieselben Abschnitte bei der Registrierung: Wählen Sie sie unter **+ Table**, klicken Sie auf **Continue**, und ihr Abschnitt Time Travel steht unter **Load and timeliness**. Ein Modell erhält den Abschnitt beim Anlegen (**+ Model**). Bei einem Modell ist sein Schlüssel der Geschäftsschlüssel, daher fragt der Abschnitt nur nach den beiden Zeiten.

## Eine Tabelle, die an Ort und Stelle geändert wird {: #a-table-updated-in-place }

Manche Tabellen führen jede Version als eigene Zeile, sodass eine Korrektur eine Zeile mit demselben Geschäftsschlüssel hinzufügt. Andere halten eine Zeile je Schlüssel und überschreiben sie; ihr Geschäftsschlüssel ist also ihr Primärschlüssel. Für eine Tabelle der zweiten Art führt Provisa die Versionen selbst in einem Versionsprotokoll:

- Jede Erfassung liest die Zeilen, die seit der letzten Erfassungszeit im Protokoll erfasst wurden, und fügt die geänderten hinzu.
- Die zulässige Verspätung des Protokolls erweitert dieses Fenster, sodass eine verspätet übernommene Zeile mit einer früheren Erfassungszeit trotzdem gelesen wird.
- Eine Erfassung sieht keine Zeile, die die Quelle gelöscht hat. **Full capture** liest die ganze Tabelle erneut und vermerkt jede gelöschte Zeile als entfernt.
- Das Protokoll beginnt mit seiner ersten Erfassung: Davor hat jede Zeile nur die Version, die sie damals hatte.

Das Protokoll steht unter **Cache** auf dem Reiter **Materialized Store** als **Version log of** der Tabelle, mit seiner Schaltfläche **Full capture**. Ein einfaches Lesen der Tabelle liest die Tabelle; ein Lesen zum Stand einer Zeit oder wie zu einer Zeit erfasst liest ihr Protokoll.

## Sie lesen {: #read-it }

Die Sicht erscheint in der Liste **Views** mit dem Abzeichen **HISTORY**. Ihr Detailbereich sagt, dass sie Historie führt, und verlinkt auf den SQL-Explorer mit der ausgewählten Sicht. Stellen Sie die Auswahlfelder **As of** und **As recorded at** des Explorers auf die gewünschten Zeiten und starten Sie die Abfrage.

Dieselben Zeiten erreichen jede andere Oberfläche. Eine pgwire-Sitzung setzt sie einmal für jede Anweisung, die sie ausführt:

```sql
SET provisa.as_of = '2026-10-03T12:00:00';
SET provisa.as_recorded = '2026-10-03T23:59:59';
```

Die HTTP-Header, `resolve`-Argumente und REST-Parameter stehen unter [Sichten mit Historie](configuration.md#views-with-history-time-travel).

## Später ändern {: #change-it-later }

Der Editor zeigt den Abschnitt Time Travel auf dem Reiter **Load and timeliness** der Sicht. Sobald die Sicht gebaut wurde, sind ihre Versionen unter ihrem Geschäftsschlüssel und ihrer Gültigkeitszeit erfasst; der Editor bietet diese beiden dann nicht mehr an, ebenso wenig den Schalter, der Time Travel ausschaltet. Die Erfassungszeit lässt sich weiterhin ändern. Um die Historie anders zu verschlüsseln, legen Sie eine neue Sicht an.
