-- Copyright (c) 2026 Kenneth Stott
--
-- This source code is licensed under the Business Source License 1.1
-- found in the LICENSE file in the root directory of this source tree.
--
-- NOTICE: Use of this software for training artificial intelligence or
-- machine learning models is strictly prohibited without explicit written
-- permission from the copyright holder.
--
-- The provisioning demo's warehouse database: what the warehouse keeps beside its feeds.
--
--   products  what is sold, its category and what one costs the business (unit_cost)
--   notices   each "dataset ready" notice a consumer is sent: the dataset, the consumer, and the
--             time it was sent -- the dataset's version time, the moment a pull reads it as of
--   pulls     each pull a consumer made, as the consumer recorded it: when it was told, when it
--             had the rows, how many, how fast, or why it was refused

CREATE TABLE products (
  sku       varchar(8) PRIMARY KEY,
  name      varchar NOT NULL,
  category  varchar NOT NULL,      -- beans | equipment | merchandise | services
  unit_cost numeric(10, 2) NOT NULL
);

INSERT INTO products VALUES
  ('BN-ETH', 'Ethiopia Yirgacheffe 1kg', 'beans', 14.20),
  ('BN-COL', 'Colombia Huila 1kg', 'beans', 12.10),
  ('BN-SUM', 'Sumatra Mandheling 1kg', 'beans', 13.40),
  ('BN-DEC', 'Swiss Water Decaf 1kg', 'beans', 15.80),
  ('BN-ESP', 'House Espresso Blend 1kg', 'beans', 11.30),
  ('EQ-GRD', 'Burr Grinder', 'equipment', 88.00),
  ('EQ-KTL', 'Gooseneck Kettle', 'equipment', 31.50),
  ('EQ-DRP', 'Pour-over Dripper', 'equipment', 9.40),
  ('EQ-SCL', 'Brew Scale', 'equipment', 22.75),
  ('MR-MUG', 'Logo Mug', 'merchandise', 4.10),
  ('MR-TOT', 'Canvas Tote', 'merchandise', 3.60),
  ('SV-CLS', 'Home Barista Class', 'services', 18.00),
  ('SV-SUB', 'Monthly Subscription Setup', 'services', 2.00);

CREATE TABLE notices (
  notice_id    serial PRIMARY KEY,
  dataset      varchar NOT NULL,
  consumer     varchar NOT NULL,
  version_time timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE pulls (
  pull_id      serial PRIMARY KEY,
  notice_id    integer REFERENCES notices (notice_id),
  consumer     varchar NOT NULL,
  dataset      varchar NOT NULL,
  version_time timestamptz,
  notified_at  timestamptz NOT NULL,
  pulled_at    timestamptz NOT NULL,
  rows         bigint,
  seconds      double precision,
  rows_per_sec double precision,
  refused      varchar               -- why the pull was refused, or NULL
);
