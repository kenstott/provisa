-- Large result test data: 10,000+ orders for redirect threshold testing.
-- Requires customers (IDs 1-20) and products (IDs 1-15) from db/init.sql.

INSERT INTO orders (customer_id, product_id, amount, quantity, region, status, created_at)
SELECT
    (s % 20) + 1 AS customer_id,
    (s % 15) + 1 AS product_id,
    ROUND((RANDOM() * 200 + 5)::numeric, 2) AS amount,
    (s % 5) + 1 AS quantity,
    CASE (s % 5)
        WHEN 0 THEN 'us-east'
        WHEN 1 THEN 'us-west'
        WHEN 2 THEN 'eu-west'
        WHEN 3 THEN 'ap-south'
        ELSE 'us-east'
    END AS region,
    CASE (s % 4)
        WHEN 0 THEN 'completed'
        WHEN 1 THEN 'shipped'
        WHEN 2 THEN 'pending'
        ELSE 'cancelled'
    END AS status,
    TIMESTAMP '2025-01-01 00:00:00' + (s || ' minutes')::interval AS created_at
FROM generate_series(1, 10500) AS s;
