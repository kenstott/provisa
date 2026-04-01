// Seed MongoDB with product reviews for cross-source testing with PG
db = db.getSiblingDB('provisa');

db.createCollection('product_reviews');

db.product_reviews.insertMany([
    { product_id: 1, reviewer: "alice", rating: 5, comment: "Excellent widget", created_at: new Date("2025-02-01") },
    { product_id: 1, reviewer: "bob", rating: 4, comment: "Good quality", created_at: new Date("2025-02-15") },
    { product_id: 2, reviewer: "carol", rating: 3, comment: "Average", created_at: new Date("2025-03-01") },
    { product_id: 3, reviewer: "david", rating: 5, comment: "Love this gadget", created_at: new Date("2025-03-10") },
    { product_id: 3, reviewer: "eve", rating: 4, comment: "Works great", created_at: new Date("2025-03-12") },
    { product_id: 4, reviewer: "frank", rating: 2, comment: "Too expensive", created_at: new Date("2025-03-15") },
    { product_id: 5, reviewer: "grace", rating: 5, comment: "Best tool ever", created_at: new Date("2025-03-18") },
    { product_id: 5, reviewer: "henry", rating: 4, comment: "Very useful", created_at: new Date("2025-03-20") },
    { product_id: 6, reviewer: "iris", rating: 3, comment: "Decent for the price", created_at: new Date("2025-03-22") },
    { product_id: 7, reviewer: "jack", rating: 1, comment: "Broke after a week", created_at: new Date("2025-03-25") },
]);

// Create the _schema collection for Trino's MongoDB connector
db.createCollection('_schema');
db.getCollection('_schema').insertOne({
    table: 'product_reviews',
    fields: [
        { name: 'product_id', type: 'bigint', hidden: false },
        { name: 'reviewer', type: 'varchar', hidden: false },
        { name: 'rating', type: 'bigint', hidden: false },
        { name: 'comment', type: 'varchar', hidden: false },
        { name: 'created_at', type: 'date', hidden: false },
    ]
});
