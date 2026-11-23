# MCP Conformance

The run uses the MCP conformance suite (@modelcontextprotocol/conformance) against the MCP endpoint, plus scenarios taken from the MCP specification for the model's own tools, which the suite's scenarios do not call.

**Suite:** @modelcontextprotocol/conformance server scenarios and MCP specification scenarios.

**Pass rate:** written from the first run of conformance-mcp.yml.

## Deliberate differences

### Writes

Writes through MCP tools are supported where the write maps onto the model through governed table writes. A write needing structure the model lacks is refused by name.

### Vocabulary fixed by the model

MCP tools and resources reflect the model. A tool the model does not expose is not available.

### Add-ins

No standard MCP add-ins apply here.

## Tools

| Tool | Version | Date | Method |
| --- | --- | --- | --- |
| Claude Desktop | — | — | Not yet verified |
| Claude Code | — | — | Not yet verified |
