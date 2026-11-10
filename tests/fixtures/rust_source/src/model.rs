use serde::Serialize;

/// An order's status.
#[derive(Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Open,
    Shipped,
}

/// One line of an order.
#[derive(Serialize)]
pub struct Line {
    /// The item's stock-keeping unit.
    pub sku: String,
    pub qty: i64,
}

/// An order.
#[derive(Serialize)]
pub struct Order {
    pub id: String,
    pub status: Status,
    pub total: f64,
    /// A note, when there is one.
    pub note: Option<String>,
    pub lines: Vec<Line>,
}
