# Datenprodukte {: #data-products }

Ein Datenprodukt ist ein benanntes, im Besitz befindliches Bündel von Tabellen, das gemeinsam zur Nutzung veröffentlicht wird. Es ist die Einheit, die der Katalog den Konsumenten bereitstellt — nicht einzelne Tabellen, sondern eine kuratierte Oberfläche, die eine Domäne ausdrücklich als bereit erklärt. Die Felder folgen dem ODPS-Vokabular (Open Data Product Standard), wo Provisa bereits die Source of Truth besitzt.

## Regel der Domäneneigentümerschaft {: #domain-ownership-rule }

Jedes Datenprodukt gehört genau einer Domäne (`domain_id` ist ein Pflichtfeld). Eine Tabelle darf einem Datenprodukt nur beitreten, wenn beide dieselbe `domain_id` teilen. Die UI schränkt den Tabellen-Picker auf die Domäne des Produkts ein; das Backend weist eine `product_id`-Zuweisung zurück, deren Domäne beim Speichern nicht mit der des Produkts übereinstimmt.

Ein Produkt, das Daten aus einer anderen Domäne benötigt, muss diese Daten zunächst als Domänen-Sicht einbringen und dann die Sicht als Mitglied einbeziehen.

## Output Ports

Die einem Datenprodukt zugewiesenen Tabellen und Commands sind seine **Output Ports** — die abfragbare Oberfläche, die Konsumenten sehen. Die Zuweisung einer Tabelle setzt `Table.product_id`; das Entfernen hebt die Mitgliedschaft auf. Eine Tabelle gehört höchstens einem Produkt. Commands in derselben Domäne können ebenfalls als Mitglieder zugewiesen werden.

## Abschnitte des Detailbereichs {: #detail-panel-sections }

Das Öffnen eines Datenprodukts in der Admin-UI zeigt diese Panels:

| Panel | Was es zeigt |
| --- | --- |
| Output Ports | Mitgliedstabellen und ihre Spalten; Mitglieds-Commands; Beispielabfragen (GraphQL, SQL, Cypher, gRPC, JSON:API, REST) |
| Verwandte Begriffe | Glossarbegriffe, die mit den Mitgliedstabellen des Produkts verknüpft sind |
| Verwandte Tabellen | Tabellen, die von Mitgliedstabellen über genehmigte Beziehungen erreichbar sind, aber noch nicht Teil des Produkts sind |
| Beziehungen | Genehmigte Beziehungen zwischen den Mitgliedstabellen dieses Produkts |
| Lineage | Spalten-Lineage-Graph, der Mitgliedstabellen als veröffentlichten Endpunkt sowie jede vorgelagerte Tabelle zeigt. Erfordert die Capability `view_governance` |
| Input Ports | Eingaben mit einem Hop → Transformation → Ausgaben, abgeleitet aus der Lineage. Erfordert `view_governance` |
| Datenqualität | Checker-Tabellen, deren Verträge die Output Ports dieses Produkts scannen; eine Zeile pro Prüfung pro Lauf. Enthält ein Regeln-Modal und die PII-Tag-Anzeige |

## Metadaten-Export {: #metadata-export }

Standardmäßig werden nur einem Produkt zugewiesene Tabellen an externe Kataloge veröffentlicht. `build_snapshot` wendet einen `data_products_only`-Filter an: Nicht zugewiesene Tabellen werden zurückgehalten, ebenso ihre Beziehungskanten, Lineage-Kanten und Governance-Tags. Quellen und Domänen werden immer veröffentlicht.

Ein Produkt ohne exportierte Mitglieder wird nicht veröffentlicht — ein leerer Eintrag würde vortäuschen, dass ein Produkt existiert, ohne dass etwas dahintersteht.

Nur Kataloge mit einem nativen Datenprodukt-Konzept veröffentlichen es als erstklassige Entität; die übrigen veröffentlichen die (bereits gefilterten) Mitgliedstabellen ohne Produktgruppierung:

| Katalog | Veröffentlicht als |
| --- | --- |
| Snowflake Horizon | SHARE + Organisations-Listing (natives Datenprodukt); `publish=false` belässt es als DRAFT, `publish=true` schaltet es live. Das Datenwörterbuch des Listings deckt jedes Mitglied und jede Spalte ab; die ersten fünf Mitglieder werden hervorgehoben, und maskierte Spalten sind in der Vorschau verborgen |
| BigQuery Analytics Hub | Analytics-Hub-Listing (nativ) |
| OpenMetadata | `DataProduct`-Entität (nativ) |
| DataHub | Native `dataProduct`-URN-Entität mit eigenen Properties-/Ownership-Aspekten |
| Collibra | Asset eines `Data Product`-Community-Typs, verknüpft mit Mitgliedstabellen |
| Apache Atlas | Best-effort benutzerdefiniertes `provisa_data_product`-Typedef — Atlas hat keinen nativen Datenprodukt-Typ |
| Atlan | Best-effort benutzerdefinierte `DataProduct`-Typedef-Vermutung — Atlan hat keinen dokumentierten stabilen Typ dafür |
| OpenLineage | Kein Listing — Mitgliedstabellen tragen eine benutzerdefinierte `provisa_data_product`-Facette, die das Produkt benennt |

## Felder {: #fields }

| Feld | Erforderlich | Anmerkungen |
| --- | --- | --- |
| `id` | Ja | Maschinenlesbarer stabiler Bezeichner, z. B. `customer_360` |
| `domain_id` | Ja | Besitzende Domäne; die Mitgliedschaftsregel wird dagegen durchgesetzt |
| `name` | Ja | Anzeigename |
| `owner_role` | Nein | Rolle, die für dieses Produkt rechenschaftspflichtig ist; unterscheidet sich vom Domänen-Steward |
| `team_role` | Nein | Rolle, deren Inhaber das tägliche Arbeitsteam bilden; löst sich zu einzelnen Personen auf |
| `purpose` | Nein | Was dieses Produkt veröffentlicht und warum |
| `limitations` | Nein | Bekannte Einschränkungen, Vorbehalte oder Ausschlüsse |
| `usage` | Nein | Wie dieses Produkt zu konsumieren ist |
| `version` | Nein | z. B. `1.2.0` |
| `status` | Nein | z. B. `proposed`, `active`, `deprecated`, `retired` |
| `sla` | Nein | Service-Level-Zusagen; nur Fließtext — ein Produkt umfasst mehrere Mitgliedstabellen, und ein strukturiertes SLA kann nicht eindeutig benennen, welches Mitglied es beschreibt |
| `support` | Nein | Freitext-Support-Hinweise |
| `support_contact` | Nein | E-Mail oder URL; erforderlich für Organisations-Listing-Manifeste des Snowflake Horizon Catalog |
| `publish` | Nein | `true`, um Horizon-Catalog-Listings sofort zu veröffentlichen; neue Listings sind standardmäßig DRAFT |
| `custom_properties` | Nein | Beliebige Schlüssel-Wert-Metadaten, die nicht von den Standardfeldern abgedeckt werden |
