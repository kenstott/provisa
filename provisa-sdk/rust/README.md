# provisa-sdk (Rust)

Optional registrations for Provisa Rust code sources. Each macro stands for a doc-comment tag, and the compiler checks the registered function's signature against that tag's rules.

```rust
pub fn churn_risk(inputs: RiskInputs) -> Result<Vec<Risk>, String> { /* ... */ }

provisa_sdk::table!(churn_risk, secret = "API_KEY");
```

Provisa reads this as `/// @table` and `/// @requires API_KEY:secret` on `churn_risk`. Registrations go at the crate's root and name a `pub fn` at the crate's root. Every option's value is a literal.

The crate has no dependencies and no procedural macro. At run time it does nothing.

The registrations, their options and the type aliases are listed in [Code Resolvers: Rust](https://provisa.dev/docs/code-sources/#rust).
