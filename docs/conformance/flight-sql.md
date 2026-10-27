# Arrow Flight SQL Conformance

**Suite:** ADBC driver validation suite with the ADBC Flight SQL driver. TBD: version recorded from CI.

**Pass rate:** TBD: generated from CI.

The ADBC driver validation suite (github.com/adbc-drivers/validation, Apache-2.0) drives Provisa's Flight SQL endpoint through the ADBC Flight SQL driver: queries of each type, prepared statements, bound parameters, result schemas and catalog metadata. Its queries that create their own tables are skipped, since a Provisa schema is its model. Arrow's own Flight SQL integration scenarios are not used: they pair a scripted client with a scripted server that expects canned inputs, so they cannot test a real server.

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
