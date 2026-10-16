-- Copyright (c) 2026 Kenneth Stott
--
-- This source code is licensed under the Business Source License 1.1
-- found in the LICENSE file in the root directory of this source tree.
--
-- NOTICE: Use of this software for training artificial intelligence or
-- machine learning models is strictly prohibited without explicit written
-- permission from the copyright holder.
--
-- The pharma demo's private data: a hospital system's pharmacy -- the manufacturers it buys from,
-- its formulary and the stock each of its sites holds. The products are real: each NDC is in
-- FDA's NDC directory with 2023 Medicaid utilization, and each manufacturer's FDA firm name is
-- the one FDA's recall records give it (found by tests/integration/test_pharma_demo_lookup.py).
-- The stock is generated, and the same on every build: each value is derived from the NDC and
-- the site, never random.
--
--   manufacturers  the pharmacy's vendor master: each manufacturer, and the name FDA's
--                  enforcement reports give the firm -- what joins it to FDA's drug recalls
--   products       the formulary, by NDC product code: what joins it to FDA's drug shortages, and
--                  by its 9-digit form to Medicaid's drug utilization
--   stock          one row a product a site: the units on hand and the units used a day

CREATE TABLE manufacturers (
  manufacturer_id integer PRIMARY KEY,
  name            varchar NOT NULL,
  -- The firm's name as FDA's enforcement reports (recalls) give it.
  fda_firm_name   varchar NOT NULL
);

INSERT INTO manufacturers VALUES
  (1, 'Teva', 'Teva Pharmaceuticals USA'),
  (2, 'Hikma', 'Hikma Pharmaceuticals USA Inc.'),
  (3, 'Hospira (Pfizer)', 'Hospira Inc.'),
  (4, 'Sandoz', 'Sandoz Inc'),
  (5, 'GSK', 'GlaxoSmithKline, LLC'),
  (6, 'Apotex', 'Apotex Corp.'),
  (7, 'Lupin', 'Lupin Pharmaceuticals Inc.'),
  (8, 'Sanofi', 'Sanofi-Aventis U.S. LLC'),
  (9, 'Glenmark', 'Glenmark Pharmaceuticals Inc., USA'),
  (10, 'Baxter', 'Baxter Healthcare Corporation'),
  (11, 'Camber', 'Camber Pharmaceuticals Inc.'),
  (12, 'Ascend', 'Ascend Laboratories, LLC'),
  (13, 'SpecGx', 'SpecGx, LLC'),
  (14, 'Takeda', 'Takeda Pharmaceuticals America, Inc.'),
  (15, 'Novo Nordisk', 'Novo Nordisk'),
  (16, 'Fresenius Kabi', 'Fresenius Kabi USA, LLC'),
  (17, 'Janssen', 'Janssen Pharmaceuticals, Inc.'),
  (18, 'Epic Pharma', 'Epic Pharma, LLC'),
  (19, 'Rising', 'Rising Pharma Holdings, Inc.'),
  (20, 'TAGI', 'TAGI Pharma, Inc.'),
  (21, 'Genentech', 'Genentech, Inc.'),
  (22, 'Solco', 'Solco Healthcare US, LLC'),
  (23, 'Nephron', 'Nephron Pharmaceuticals Corporation'),
  (24, 'Upsher-Smith', 'Upsher-Smith Laboratories, LLC'),
  (25, 'Civica', 'Civica, Inc.'),
  (26, 'NorthStar Rx', 'NorthStar RxLLC'),
  (27, 'Eli Lilly', 'Eli Lilly and Company');

CREATE TABLE products (
  product_ndc       varchar PRIMARY KEY,   -- NDC product code, labeler-product (e.g. 0409-1891)
  product_ndc9      varchar(9) NOT NULL,   -- the same code as 9 digits, as Medicaid reports it
  generic_name      varchar NOT NULL,
  manufacturer_id   integer NOT NULL REFERENCES manufacturers (manufacturer_id),
  dosage_form       varchar NOT NULL,
  route             varchar NOT NULL,
  therapeutic_class varchar NOT NULL,
  -- Whether the pharmacy's P&T committee lists it as critical: no stock-out is tolerated.
  critical          boolean NOT NULL
);

INSERT INTO products VALUES
  ('0093-0832', '000930832', 'CLONAZEPAM', 1, 'TABLET', 'ORAL', 'Neurology', false),
  ('59417-104', '594170104', 'LISDEXAMFETAMINE', 14, 'CAPSULE', 'ORAL', 'Psychiatry', false),
  ('0169-4060', '001694060', 'LIRAGLUTIDE', 15, 'INJECTION', 'SUBCUTANEOUS', 'Endocrinology', false),
  ('63323-162', '633230162', 'KETOROLAC', 16, 'INJECTION, SOLUTION', 'INTRAMUSCULAR', 'Analgesia', true),
  ('63323-165', '633230165', 'DEXAMETHASONE', 16, 'INJECTION, SOLUTION', 'INTRAMUSCULAR', 'Endocrinology', true),
  ('50458-586', '504580586', 'METHYLPHENIDATE ER', 17, 'TABLET, EXTENDED RELEASE', 'ORAL', 'Psychiatry', false),
  ('0409-9094', '004099094', 'FENTANYL', 3, 'INJECTION, SOLUTION', 'INTRAMUSCULAR', 'Analgesia', true),
  ('0641-0367', '006410367', 'DEXAMETHASONE', 2, 'INJECTION', 'INTRAMUSCULAR', 'Endocrinology', true),
  ('0409-3795', '004093795', 'KETOROLAC', 3, 'INJECTION, SOLUTION', 'INTRAMUSCULAR', 'Analgesia', true),
  ('0409-2305', '004092305', 'MIDAZOLAM', 3, 'INJECTION, SOLUTION', 'INTRAMUSCULAR', 'Anesthesia', true),
  ('0409-1283', '004091283', 'HYDROMORPHONE', 3, 'INJECTION, SOLUTION', 'INTRAMUSCULAR', 'Analgesia', true),
  ('0409-1891', '004091891', 'MORPHINE', 3, 'INJECTION, SOLUTION', 'INTRAVENOUS', 'Analgesia', true),
  ('0185-0842', '001850842', 'AMPHETAMINE/DEXTROAMPHETAMINE', 4, 'TABLET', 'ORAL', 'Psychiatry', false),
  ('42806-344', '428060344', 'AMPHETAMINE/DEXTROAMPHETAMINE', 18, 'TABLET', 'ORAL', 'Psychiatry', false),
  ('0054-3270', '000543270', 'FLUTICASONE', 2, 'SPRAY, METERED', 'NASAL', 'Respiratory', false),
  ('0173-0682', '001730682', 'ALBUTEROL', 5, 'AEROSOL, METERED', 'INHALATION', 'Respiratory', true),
  ('60505-2580', '605052580', 'ATORVASTATIN', 6, 'TABLET, FILM COATED', 'ORAL', 'Cardiovascular', false),
  ('68180-980', '681800980', 'LISINOPRIL', 7, 'TABLET', 'ORAL', 'Cardiovascular', false),
  ('0088-2219', '000882219', 'INSULIN GLARGINE', 8, 'INJECTION, SOLUTION', 'SUBCUTANEOUS', 'Endocrinology', true),
  ('68462-397', '684620397', 'OMEPRAZOLE', 9, 'CAPSULE, DELAYED RELEASE', 'ORAL', 'Gastroenterology', false),
  ('0338-0049', '003380049', 'SODIUM CHLORIDE', 10, 'INJECTION, SOLUTION', 'INTRAVENOUS', 'Fluids', true),
  ('31722-726', '317220726', 'MONTELUKAST', 11, 'TABLET, FILM COATED', 'ORAL', 'Respiratory', false),
  ('0409-2720', '004092720', 'HEPARIN', 3, 'INJECTION, SOLUTION', 'INTRAVENOUS', 'Hematology', true),
  ('0781-2613', '007812613', 'AMOXICILLIN', 4, 'CAPSULE', 'ORAL', 'Infectious Disease', false),
  ('67877-198', '678770198', 'AMLODIPINE', 12, 'TABLET', 'ORAL', 'Cardiovascular', false),
  ('0406-0123', '004060123', 'HYDROCODONE/ACETAMINOPHEN', 13, 'TABLET', 'ORAL', 'Analgesia', false),
  ('57237-014', '572370014', 'TAMSULOSIN', 19, 'CAPSULE', 'ORAL', 'Urology', false),
  ('51224-022', '512240022', 'AZITHROMYCIN', 20, 'TABLET, FILM COATED', 'ORAL', 'Infectious Disease', false),
  ('50242-215', '502420215', 'OMALIZUMAB', 21, 'INJECTION, SOLUTION', 'SUBCUTANEOUS', 'Respiratory', false),
  ('43547-425', '435470425', 'LOSARTAN/HYDROCHLOROTHIAZIDE', 22, 'TABLET, FILM COATED', 'ORAL', 'Cardiovascular', false),
  ('0487-9801', '004879801', 'IPRATROPIUM', 23, 'SOLUTION', 'INHALATION', 'Respiratory', false),
  ('0832-1560', '008321560', 'HALOPERIDOL', 24, 'TABLET', 'ORAL', 'Psychiatry', false),
  ('72572-122', '725720122', 'DEXAMETHASONE', 25, 'INJECTION', 'INTRAMUSCULAR', 'Endocrinology', true),
  ('16714-548', '167140548', 'LACOSAMIDE', 26, 'TABLET', 'ORAL', 'Neurology', false),
  ('0002-5337', '000025337', 'ABEMACICLIB', 27, 'TABLET', 'ORAL', 'Oncology', false),
  ('0169-4524', '001694524', 'SEMAGLUTIDE', 15, 'INJECTION, SOLUTION', 'SUBCUTANEOUS', 'Endocrinology', false);

CREATE TABLE stock (
  product_ndc     varchar NOT NULL REFERENCES products (product_ndc),
  site            varchar NOT NULL,   -- Main Campus | North Hospital | Children's Hospital
  on_hand_units   integer NOT NULL,
  daily_use_units integer NOT NULL,
  PRIMARY KEY (product_ndc, site)
);

-- Each product at each site: a daily use of 5 to 124 units, and 3 to 44 days of it on hand, both
-- read from the md5 of the NDC and the site.
INSERT INTO stock
SELECT p.product_ndc, s.site,
       (5 + ('x' || substr(md5(p.product_ndc || s.site), 1, 4))::bit(16)::int % 120)
         * (3 + ('x' || substr(md5(p.product_ndc || s.site), 5, 4))::bit(16)::int % 42),
       5 + ('x' || substr(md5(p.product_ndc || s.site), 1, 4))::bit(16)::int % 120
FROM products AS p
CROSS JOIN (VALUES ('Main Campus'), ('North Hospital'), ('Children''s Hospital')) AS s (site);
