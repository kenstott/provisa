-- Copyright (c) 2026 Kenneth Stott
--
-- This source code is licensed under the Business Source License 1.1
-- found in the LICENSE file in the root directory of this source tree.
--
-- NOTICE: Use of this software for training artificial intelligence or
-- machine learning models is strictly prohibited without explicit written
-- permission from the copyright holder.
--
-- The agriculture demo's private data: a crop insurer's portfolio of Corn Belt farms, the multi-peril
-- policy each farm buys every crop year, and the claims paid on them, 2010 to 2017. Generated, and
-- the same on every build: each value is derived from a farm's number and the year, never random.
--
--   counties      the counties the portfolio writes in, by their 5-digit FIPS code -- what joins it
--                 to the USDA's cause-of-loss, NOAA's daily weather and NASS's yields in govdata
--   farms         one row a farm: its county, the crop it insures, its insured acres and its
--                 actual production history (APH) yield
--   policies      one row a farm and crop year: the coverage level bought, the liability and the
--                 premium
--   claims        one row an indemnity paid: the farm, crop year, cause of loss, month of loss and
--                 amount
--
-- How often and how hard a year's claims come follows what those seasons were in the Corn Belt:
-- 2012's drought and heat, the wet springs and summers of 2010 and 2013, light years otherwise.
-- Whether the weather explains them is the demo's question, answered against the public data.

CREATE TABLE counties (
  county_fips varchar(5) PRIMARY KEY,
  county_name varchar NOT NULL,
  state_abbr  varchar(2) NOT NULL,
  -- How many of the portfolio's farms the county holds, relative to the others.
  weight      integer NOT NULL
);

INSERT INTO counties VALUES
  ('19169', 'Story', 'IA', 4), ('19153', 'Polk', 'IA', 2), ('19015', 'Boone', 'IA', 3),
  ('19079', 'Hamilton', 'IA', 3), ('19187', 'Webster', 'IA', 3), ('19083', 'Hardin', 'IA', 3),
  ('19127', 'Marshall', 'IA', 3), ('19099', 'Jasper', 'IA', 3), ('19049', 'Dallas', 'IA', 2),
  ('19073', 'Greene', 'IA', 3),
  ('17113', 'McLean', 'IL', 5), ('17019', 'Champaign', 'IL', 4), ('17105', 'Livingston', 'IL', 4),
  ('17053', 'Ford', 'IL', 2), ('17075', 'Iroquois', 'IL', 4), ('17183', 'Vermilion', 'IL', 3),
  ('17115', 'Macon', 'IL', 2), ('17147', 'Piatt', 'IL', 2), ('17039', 'De Witt', 'IL', 2),
  ('17107', 'Logan', 'IL', 3),
  ('18157', 'Tippecanoe', 'IN', 2), ('18007', 'Benton', 'IN', 3), ('18181', 'White', 'IN', 3),
  ('18023', 'Clinton', 'IN', 2), ('18011', 'Boone', 'IN', 2),
  ('31079', 'Hall', 'NE', 2), ('31081', 'Hamilton', 'NE', 3), ('31185', 'York', 'NE', 3),
  ('31159', 'Seward', 'NE', 2), ('31019', 'Buffalo', 'NE', 2),
  ('27129', 'Renville', 'MN', 3), ('27127', 'Redwood', 'MN', 3), ('27091', 'Martin', 'MN', 3),
  ('27043', 'Faribault', 'MN', 2), ('27013', 'Blue Earth', 'MN', 2),
  ('29195', 'Saline', 'MO', 2), ('29007', 'Audrain', 'MO', 2), ('29147', 'Nodaway', 'MO', 2),
  ('39037', 'Darke', 'OH', 2), ('39107', 'Mercer', 'OH', 2);

CREATE TABLE farms (
  farm_id       integer PRIMARY KEY,
  county_fips   varchar(5) NOT NULL REFERENCES counties (county_fips),
  crop          varchar NOT NULL,          -- corn | soybeans
  insured_acres integer NOT NULL,
  aph_yield     numeric(6, 1) NOT NULL     -- actual production history yield, bushels an acre
);

-- Each farm lands in a county in proportion to the county's weight: farm n takes the county whose
-- running weight first passes its hash mod the total weight.
WITH weighted AS (
  SELECT county_fips,
         SUM(weight) OVER (ORDER BY county_fips) AS upto,
         SUM(weight) OVER () AS total
  FROM counties
),
numbered AS (
  SELECT n, abs(hashtext('farm:' || n)) AS h FROM generate_series(1, 1200) AS n
)
INSERT INTO farms
SELECT f.n,
       (SELECT w.county_fips FROM weighted w
        WHERE w.upto > (f.h % (SELECT MAX(total) FROM weighted)) ORDER BY w.upto LIMIT 1),
       CASE WHEN f.h % 5 < 3 THEN 'corn' ELSE 'soybeans' END,
       200 + (f.h / 5) % 1800,
       CASE WHEN f.h % 5 < 3 THEN 160 + ((f.h / 11) % 450) / 10.0
            ELSE 45 + ((f.h / 11) % 200) / 10.0 END
FROM numbered f;

CREATE TABLE policies (
  farm_id        integer NOT NULL REFERENCES farms (farm_id),
  crop_year      integer NOT NULL,
  coverage_level numeric(3, 2) NOT NULL,   -- the share of the APH yield insured: 0.70 to 0.85
  liability      numeric(12, 2) NOT NULL,  -- the most the policy pays: acres x APH x price x level
  premium        numeric(10, 2) NOT NULL,  -- the total premium, before subsidy
  PRIMARY KEY (farm_id, crop_year)
);

-- The projected price a bushel each crop year (corn, soybeans), as the policy prices its guarantee.
WITH prices (crop_year, corn, soybeans) AS (
  VALUES (2010, 3.99, 9.23), (2011, 6.01, 13.49), (2012, 5.68, 12.55), (2013, 5.65, 12.87),
         (2014, 4.62, 11.36), (2015, 4.15, 9.73), (2016, 3.86, 8.85), (2017, 3.96, 10.19)
)
INSERT INTO policies
SELECT f.farm_id, p.crop_year,
       0.70 + 0.05 * (abs(hashtext('level:' || f.farm_id || ':' || p.crop_year)) % 4),
       0, 0
FROM farms f CROSS JOIN prices p;

WITH prices (crop_year, corn, soybeans) AS (
  VALUES (2010, 3.99, 9.23), (2011, 6.01, 13.49), (2012, 5.68, 12.55), (2013, 5.65, 12.87),
         (2014, 4.62, 11.36), (2015, 4.15, 9.73), (2016, 3.86, 8.85), (2017, 3.96, 10.19)
)
UPDATE policies AS p
SET liability = round(f.insured_acres * f.aph_yield * p.coverage_level
                      * CASE f.crop WHEN 'corn' THEN pr.corn ELSE pr.soybeans END, 2),
    -- A rate that rises with the coverage level, as the premium schedule's does.
    premium = round(f.insured_acres * f.aph_yield * p.coverage_level
                    * CASE f.crop WHEN 'corn' THEN pr.corn ELSE pr.soybeans END
                    * (0.035 + (p.coverage_level - 0.70) * 0.30), 2)
FROM farms f, prices pr
WHERE f.farm_id = p.farm_id AND pr.crop_year = p.crop_year;

CREATE TABLE claims (
  claim_id      integer PRIMARY KEY,
  farm_id       integer NOT NULL,
  crop_year     integer NOT NULL,
  cause_of_loss varchar NOT NULL,          -- drought | heat | excess_moisture | hail | freeze
  month_of_loss integer NOT NULL,
  indemnity     numeric(12, 2) NOT NULL,   -- paid, USD
  FOREIGN KEY (farm_id, crop_year) REFERENCES policies (farm_id, crop_year)
);

-- Each crop year's season: how likely a farm is to claim (per thousand), how much of its liability
-- a claim pays at most (per thousand), and the cause and month most of its claims carry.
WITH seasons (crop_year, odds, severity, cause, month_of_loss) AS (
  VALUES (2010, 140, 260, 'excess_moisture', 6), (2011, 90, 180, 'hail', 7),
         (2012, 620, 340, 'drought', 8), (2013, 260, 330, 'excess_moisture', 5),
         (2014, 80, 170, 'hail', 7), (2015, 110, 200, 'excess_moisture', 6),
         (2016, 60, 150, 'hail', 7), (2017, 80, 170, 'drought', 8)
),
draws AS (
  SELECT p.farm_id, p.crop_year, p.liability, s.odds, s.severity, s.cause, s.month_of_loss,
         abs(hashtext('claim:' || p.farm_id || ':' || p.crop_year)) AS h
  FROM policies p JOIN seasons s ON s.crop_year = p.crop_year
)
INSERT INTO claims
SELECT row_number() OVER (ORDER BY d.crop_year, d.farm_id),
       d.farm_id, d.crop_year,
       -- One claim in five is for another peril: hail in a dry year, the year's own otherwise.
       CASE WHEN (d.h / 1000) % 5 = 0 AND d.cause <> 'hail' THEN 'hail'
            WHEN (d.h / 1000) % 5 = 0 THEN 'excess_moisture'
            WHEN d.cause = 'drought' AND (d.h / 1000) % 5 = 1 THEN 'heat'
            ELSE d.cause END,
       CASE WHEN (d.h / 1000) % 5 = 0 THEN 7 ELSE d.month_of_loss END,
       round(d.liability * d.severity / 1000.0 * (300 + (d.h / 7) % 700) / 1000.0, 2)
FROM draws d
WHERE d.h % 1000 < d.odds;
