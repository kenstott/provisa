# gRPC Conformance

TBD: suite to be chosen.

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
| grpcurl | TBD | TBD | TBD: replay or manual |
| Postman (gRPC) | TBD | TBD | TBD: replay or manual |
