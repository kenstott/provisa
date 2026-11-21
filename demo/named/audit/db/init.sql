-- Copyright (c) 2026 Kenneth Stott
--
-- This source code is licensed under the Business Source License 1.1
-- found in the LICENSE file in the root directory of this source tree.
--
-- NOTICE: Use of this software for training artificial intelligence or
-- machine learning models is strictly prohibited without explicit written
-- permission from the copyright holder.
--
-- The audit demo's private data: an audit firm's Single Audit clients, nine nonprofits in Vermont,
-- New Hampshire and Maine, with each one's draft schedule of federal awards for 2023 and the
-- federal award records the client holds. Every client is fictional and every record generated:
-- the names are invented, the EINs are of the 00- prefix the IRS never issues, and the award
-- numbers are the firm's own references, so no record here is a real organisation's. The counties
-- and the Assistance Listings are real: they are what joins the records to the public data.
--
--   clients         the firm's clients: the county each is in and the engagement team auditing it
--   grant_schedule  each client's draft Schedule of Expenditures of Federal Awards for its 2023
--                   audit: one line an award, its Assistance Listing, direct or passed through a
--                   state agency, and what the client says it spent
--   award_notices   the federal award records each client holds for its direct awards: one row an
--                   obligation, the federal fiscal year it was obligated in and how much


CREATE TABLE clients (
  client_id        integer PRIMARY KEY,
  client_name      varchar NOT NULL,
  ein              varchar(10) NOT NULL,
  city             varchar NOT NULL,
  state            varchar(2) NOT NULL,
  county_fips      varchar(5) NOT NULL,
  engagement_team  varchar NOT NULL,
  fiscal_year_end  date NOT NULL
);

CREATE TABLE grant_schedule (
  line_id             integer PRIMARY KEY,
  client_id           integer NOT NULL REFERENCES clients (client_id),
  audit_year          integer NOT NULL,
  aln                 varchar NOT NULL,
  program_name        varchar NOT NULL,
  award_number        varchar NOT NULL,
  is_direct           varchar(1) NOT NULL,
  passthrough_entity  varchar,
  amount_expended     numeric(14, 2) NOT NULL
);

CREATE TABLE award_notices (
  notice_id         integer PRIMARY KEY,
  client_id         integer NOT NULL REFERENCES clients (client_id),
  aln               varchar NOT NULL,
  award_number      varchar NOT NULL,
  action_fiscal_year integer NOT NULL,
  obligated_amount  numeric(14, 2) NOT NULL,
  period_start      date NOT NULL,
  period_end        date NOT NULL
);

INSERT INTO clients VALUES
  (1, 'Brindlewood Community Health', '00-9101031', 'Burlington', 'VT', '50007', 'vermont', DATE '2023-06-30'),
  (2, 'Marrowfield Early Learning', '00-9102062', 'Montpelier', 'VT', '50023', 'vermont', DATE '2023-06-30'),
  (3, 'Halverd Housing Alliance', '00-9103093', 'Brattleboro', 'VT', '50025', 'vermont', DATE '2023-12-31'),
  (4, 'Quillon Youth Works', '00-9104124', 'Manchester', 'NH', '33011', 'new_hampshire', DATE '2023-06-30'),
  (5, 'Tessaly Recovery Network', '00-9105155', 'Concord', 'NH', '33013', 'new_hampshire', DATE '2023-06-30'),
  (6, 'Vantrell Rural Health', '00-9106186', 'Berlin', 'NH', '33007', 'new_hampshire', DATE '2023-12-31'),
  (7, 'Corvane Senior Services', '00-9107217', 'Portland', 'ME', '23005', 'maine', DATE '2023-06-30'),
  (8, 'Pellory Food Partners', '00-9108248', 'Bangor', 'ME', '23019', 'maine', DATE '2023-09-30'),
  (9, 'Osterly Family Partners', '00-9109279', 'Presque Isle', 'ME', '23003', 'maine', DATE '2023-06-30');

INSERT INTO grant_schedule VALUES
  (1, 1, 2023, '93.224', 'Health Center Program', 'BCH-93224-01', 'Y', NULL, 2140000.00),
  (2, 1, 2023, '93.918', 'Ryan White HIV/AIDS Part C Early Intervention Services', 'BCH-93918-02', 'Y', NULL, 410000.00),
  (3, 1, 2023, '93.912', 'Rural Health Care Services Outreach', 'BCH-93912-03', 'Y', NULL, 185000.00),
  (4, 1, 2023, '93.959', 'Substance Abuse Prevention and Treatment Block Grant', 'BCH-93959-04', 'N', 'State health agency', 320000.00),
  (5, 2, 2023, '93.600', 'Head Start', 'MEL-93600-01', 'Y', NULL, 3260000.00),
  (6, 2, 2023, '10.558', 'Child and Adult Care Food Program', 'MEL-10558-02', 'N', 'State education agency', 412000.00),
  (7, 2, 2023, '93.575', 'Child Care and Development Block Grant', 'MEL-93575-03', 'N', 'State human services agency', 275000.00),
  (8, 2, 2023, '21.027', 'Coronavirus State and Local Fiscal Recovery Funds', 'MEL-21027-04', 'N', 'State recovery office', 140000.00),
  (9, 3, 2023, '14.267', 'Continuum of Care Program', 'HHA-14267-01', 'Y', NULL, 1480000.00),
  (10, 3, 2023, '14.231', 'Emergency Solutions Grant Program', 'HHA-14231-02', 'N', 'State housing finance agency', 365000.00),
  (11, 3, 2023, '14.267', 'Continuum of Care Program', 'HHA-14267-03', 'Y', NULL, 610000.00),
  (12, 3, 2023, '21.027', 'Coronavirus State and Local Fiscal Recovery Funds', 'HHA-21027-04', 'N', 'State recovery office', 220000.00),
  (13, 4, 2023, '17.259', 'WIOA Youth Activities', 'QYW-17259-01', 'N', 'State workforce board', 690000.00),
  (14, 4, 2023, '94.006', 'AmeriCorps State and National', 'QYW-94006-02', 'N', 'State service commission', 455000.00),
  (15, 4, 2023, '93.912', 'Rural Health Care Services Outreach', 'QYW-93912-03', 'Y', NULL, 160000.00),
  (16, 5, 2023, '93.788', 'Opioid STR', 'TRN-93788-01', 'N', 'State health agency', 1180000.00),
  (17, 5, 2023, '93.959', 'Substance Abuse Prevention and Treatment Block Grant', 'TRN-93959-02', 'N', 'State health agency', 740000.00),
  (18, 5, 2023, '93.224', 'Health Center Program', 'TRN-93224-03', 'Y', NULL, 960000.00),
  (19, 6, 2023, '93.224', 'Health Center Program', 'VRH-93224-01', 'Y', NULL, 1720000.00),
  (20, 6, 2023, '93.912', 'Rural Health Care Services Outreach', 'VRH-93912-02', 'Y', NULL, 210000.00),
  (21, 6, 2023, '93.217', 'Family Planning Services', 'VRH-93217-03', 'Y', NULL, 380000.00),
  (22, 7, 2023, '93.044', 'Special Programs for the Aging, Title III Part B', 'CSS-93044-01', 'N', 'Area agency on aging', 540000.00),
  (23, 7, 2023, '93.045', 'Special Programs for the Aging, Title III Part C', 'CSS-93045-02', 'N', 'Area agency on aging', 1260000.00),
  (24, 7, 2023, '93.568', 'Low-Income Home Energy Assistance', 'CSS-93568-03', 'N', 'State human services agency', 310000.00),
  (25, 7, 2023, '21.027', 'Coronavirus State and Local Fiscal Recovery Funds', 'CSS-21027-04', 'Y', NULL, 175000.00),
  (26, 8, 2023, '10.565', 'Commodity Supplemental Food Program', 'PFP-10565-01', 'N', 'State agriculture agency', 820000.00),
  (27, 8, 2023, '10.558', 'Child and Adult Care Food Program', 'PFP-10558-02', 'N', 'State education agency', 290000.00),
  (28, 9, 2023, '93.600', 'Head Start', 'OFP-93600-01', 'Y', NULL, 2870000.00),
  (29, 9, 2023, '93.600', 'Head Start', 'OFP-93600-02', 'Y', NULL, 640000.00),
  (30, 9, 2023, '93.575', 'Child Care and Development Block Grant', 'OFP-93575-03', 'N', 'State human services agency', 330000.00),
  (31, 9, 2023, '93.217', 'Family Planning Services', 'OFP-93217-04', 'Y', NULL, 150000.00);

INSERT INTO award_notices VALUES
  (1, 1, '93.224', 'BCH-93224-01', 2022, 1980000.00, DATE '2021-10-01', DATE '2024-09-30'),
  (2, 1, '93.224', 'BCH-93224-01', 2023, 2050000.00, DATE '2021-10-01', DATE '2024-09-30'),
  (3, 1, '93.918', 'BCH-93918-02', 2021, 395000.00, DATE '2020-10-01', DATE '2023-09-30'),
  (4, 1, '93.918', 'BCH-93918-02', 2022, 402000.00, DATE '2020-10-01', DATE '2023-09-30'),
  (5, 1, '93.498', 'BCH-93498-05', 2023, 260000.00, DATE '2022-10-01', DATE '2024-09-30'),
  (6, 2, '93.600', 'MEL-93600-01', 2023, 3310000.00, DATE '2022-10-01', DATE '2024-09-30'),
  (7, 3, '14.267', 'HHA-14267-01', 2023, 1525000.00, DATE '2022-10-01', DATE '2024-09-30'),
  (8, 3, '14.267', 'HHA-14267-03', 2022, 640000.00, DATE '2021-10-01', DATE '2023-09-30'),
  (9, 5, '93.224', 'TRN-93224-03', 2023, 1010000.00, DATE '2022-10-01', DATE '2024-09-30'),
  (10, 5, '93.498', 'TRN-93498-04', 2023, 145000.00, DATE '2022-10-01', DATE '2024-09-30'),
  (11, 6, '93.224', 'VRH-93224-01', 2023, 1690000.00, DATE '2022-10-01', DATE '2024-09-30'),
  (12, 6, '93.912', 'VRH-93912-02', 2021, 200000.00, DATE '2020-10-01', DATE '2022-09-30'),
  (13, 6, '93.217', 'VRH-93217-03', 2023, 395000.00, DATE '2022-10-01', DATE '2024-09-30'),
  (14, 8, '14.231', 'PFP-14231-03', 2023, 120000.00, DATE '2022-10-01', DATE '2024-09-30'),
  (15, 9, '93.600', 'OFP-93600-01', 2022, 2790000.00, DATE '2021-10-01', DATE '2024-09-30'),
  (16, 9, '93.600', 'OFP-93600-01', 2023, 2910000.00, DATE '2021-10-01', DATE '2024-09-30'),
  (17, 9, '93.600', 'OFP-93600-02', 2022, 655000.00, DATE '2021-10-01', DATE '2023-09-30');
