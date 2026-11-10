//! A Rust source (REQ-2227): its tagged public functions are tables, commands and a stream.

mod model;

pub use model::{Line, Order, Status};
use serde::{Deserialize, Serialize};

/// The inputs of Orders: who the orders are for.
#[derive(Deserialize)]
pub struct OrdersInputs {
    /// The customer the orders are for.
    pub customer: String,
    /// How many lines each order has; one when left out.
    pub lines: Option<i64>,
}

/// The inputs of Many.
#[derive(Deserialize)]
pub struct ManyInputs {
    pub count: i64,
}

/// A number in a row of its own.
#[derive(Serialize)]
pub struct Numbered {
    pub n: i64,
}

/// What a refund did.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Refunded {
    pub order_id: String,
    pub amount: f64,
}

/// The customer's orders.
/// @table
pub fn orders(inputs: OrdersInputs) -> Vec<Order> {
    let count = inputs.lines.unwrap_or(1);
    vec![Order {
        id: format!("{}-1", inputs.customer),
        status: Status::Open,
        total: 12.5,
        note: None,
        lines: (0..count).map(|i| Line { sku: inputs.customer.clone(), qty: 2 + i }).collect(),
    }]
}

/// As many numbered rows as asked for, from an iterator.
/// @table
pub fn many(inputs: ManyInputs) -> impl Iterator<Item = Numbered> {
    (0..inputs.count).map(|n| Numbered { n })
}

/// Rows as they come; the third fails.
/// @stream
pub fn ticks() -> impl Iterator<Item = Result<Numbered, String>> {
    (0..3).map(|n| if n == 2 { Err("the third tick failed".to_string()) } else { Ok(Numbered { n }) })
}

/// Refund an order.
/// @command
pub fn refund(order_id: String, amount: f64) -> Result<Refunded, String> {
    if amount < 0.0 {
        return Err(format!("a refund of {amount} is not one"));
    }
    Ok(Refunded { order_id, amount })
}

/// The price of a sku.
/// @command
pub fn price(sku: String, quantity: Option<i64>) -> f64 {
    sku.len() as f64 * quantity.unwrap_or(1) as f64
}

/// Reads outside its sandbox.
/// @table
pub fn outside() -> Result<Vec<Numbered>, std::io::Error> {
    std::fs::read_to_string("/etc/hosts")?;
    Ok(vec![])
}

/// Not tagged: not offered.
pub fn helper() -> i64 {
    1
}
