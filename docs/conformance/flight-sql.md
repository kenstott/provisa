# Arrow Flight SQL Conformance

**Suite:** Apache Arrow Flight SQL integration scenarios. TBD: version recorded from CI.

**Pass rate:** TBD: generated from CI.

## Deliberate differences

### Writes

DML over Flight SQL is supported where the write maps onto the model through governed table writes. A write needing structure the model lacks is refused by name. Transaction commands are a no-op by design.

### Vocabulary fixed by the model

DDL over Flight SQL is not supported. The model is the source of schema.

### Add-ins

No standard add-ins apply to the Flight SQL transport.

## Tools

| Tool | Version | Date | Method |
| --- | --- | --- | --- |
| ADBC Python client | TBD | TBD | Covered by driver |
| ADBC Java client | TBD | TBD | Covered by driver |
| DBeaver (Flight SQL) | TBD | TBD | TBD: replay or manual |
