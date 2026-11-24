# gRPC Conformance

gRPC publishes interop tests for its own test service, not a conformance suite for another server's service, so the run checks scenarios taken from gRPC's server reflection, health checking and status code documents.

**Suite:** gRPC server reflection, health checking and status code scenarios.

**Pass rate:** written from the first run of conformance-grpc.yml.

## Deliberate differences

### Writes

Writes are supported where the write maps onto the model through governed table writes. A write needing structure the model lacks is refused by name.

### Vocabulary fixed by the model

The gRPC service definition is generated from the model. A service or message the model does not declare is not present in the proto.

### Add-ins

No standard gRPC add-ins apply here.

## Tools

| Tool | Version | Date | Method |
| --- | --- | --- | --- |
| grpcurl | — | — | Not yet verified |
| Postman (gRPC) | — | — | Not yet verified |
