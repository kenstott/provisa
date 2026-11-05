-- Copyright (c) 2026 Kenneth Stott
--
-- This source code is licensed under the Business Source License 1.1
-- found in the LICENSE file in the root directory of this source tree.
--
-- NOTICE: Use of this software for training artificial intelligence or
-- machine learning models is strictly prohibited without explicit written
-- permission from the copyright holder.
--
-- The groupexposure demo's database: the corporate loan book of a Prague bank, Vltava Commercial
-- Bank. The bank, its borrowers and its loans are fictional. Each borrower's owner is a real
-- company, which the bank records by its Legal Entity Identifier (LEI). The bank does not record
-- who owns that owner.
--
--   counterparties  each borrower: its name, the bank's sector, its relationship manager
--   owners          the company that owns each borrower, by LEI -- none for an independent
--                   borrower, two for a joint venture
--   facilities      each facility lent to a borrower: term loans, revolving credit and guarantees,
--                   what is committed and what is drawn, in EUR
--   capital         the bank's Tier 1 capital, the base of the large-exposure limit (25% of it,
--                   EU CRR Article 395) for any one client or group of connected clients
--
-- 37 borrowers, EUR 971 million committed.

CREATE TABLE counterparties (
  borrower_id varchar PRIMARY KEY,
  name varchar NOT NULL,
  sector varchar NOT NULL,
  relationship_manager varchar NOT NULL
);

CREATE TABLE owners (
  borrower_id varchar NOT NULL REFERENCES counterparties (borrower_id),
  owner_lei char(20) NOT NULL,
  PRIMARY KEY (borrower_id, owner_lei)
);

CREATE TABLE facilities (
  facility_id integer PRIMARY KEY,
  borrower_id varchar NOT NULL REFERENCES counterparties (borrower_id),
  product varchar NOT NULL,
  committed_eur numeric(14, 2) NOT NULL,
  drawn_eur numeric(14, 2) NOT NULL,
  maturity date NOT NULL
);

CREATE TABLE capital (
  as_of date PRIMARY KEY,
  tier1_eur numeric(14, 2) NOT NULL
);

INSERT INTO counterparties (borrower_id, name, sector, relationship_manager) VALUES
  ('B-001', 'Vltavské vodárny a.s.', 'Utilities', 'Jana Dvořáková'),
  ('B-002', 'Teplárna Holešovice a.s.', 'Utilities', 'Petr Novotný'),
  ('B-003', 'Energie Střední Čechy s.r.o.', 'Utilities', 'Tereza Králová'),
  ('B-004', 'Vodovody Berounka a.s.', 'Utilities', 'Martin Svoboda'),
  ('B-005', 'Morava Aqua a.s.', 'Utilities', 'Jana Dvořáková'),
  ('B-006', 'Pražské tepelné sítě s.r.o.', 'Utilities', 'Petr Novotný'),
  ('B-007', 'Silnice Bohemia a.s.', 'Construction', 'Tereza Králová'),
  ('B-008', 'Elektroinstalace Labe s.r.o.', 'Construction', 'Martin Svoboda'),
  ('B-009', 'Síťové služby Vysočina s.r.o.', 'Technology', 'Jana Dvořáková'),
  ('B-010', 'Mosty a tunely Ostrava a.s.', 'Construction', 'Petr Novotný'),
  ('B-011', 'Stavby Kladno s.r.o.', 'Construction', 'Tereza Králová'),
  ('B-012', 'Průmyslové stavby Plzeň a.s.', 'Construction', 'Martin Svoboda'),
  ('B-013', 'Distribuce Jih s.r.o.', 'Utilities', 'Jana Dvořáková'),
  ('B-014', 'Energie Šumava a.s.', 'Utilities', 'Petr Novotný'),
  ('B-015', 'Zákaznické služby Brno s.r.o.', 'Utilities', 'Tereza Králová'),
  ('B-016', 'Sklotextil Litomyšl s.r.o.', 'Manufacturing', 'Martin Svoboda'),
  ('B-017', 'Litina Bohumín s.r.o.', 'Manufacturing', 'Jana Dvořáková'),
  ('B-018', 'Autosklo Hořice s.r.o.', 'Manufacturing', 'Petr Novotný'),
  ('B-019', 'Stavební hmoty Kolín a.s.', 'Manufacturing', 'Tereza Králová'),
  ('B-020', 'Pojišťovna Hradčany a.s.', 'Insurance', 'Martin Svoboda'),
  ('B-021', 'Pojišťovna Vyšehrad a.s.', 'Insurance', 'Jana Dvořáková'),
  ('B-022', 'Pneu Otrokovice s.r.o.', 'Manufacturing', 'Petr Novotný'),
  ('B-023', 'Gumárny Krnov s.r.o.', 'Manufacturing', 'Tereza Králová'),
  ('B-024', 'Mobilní sítě Morava a.s.', 'Telecoms', 'Martin Svoboda'),
  ('B-025', 'Datová centra Praha s.r.o.', 'Technology', 'Jana Dvořáková'),
  ('B-026', 'Telekomunikační služby Liberec s.r.o.', 'Telecoms', 'Petr Novotný'),
  ('B-027', 'Rádiové sítě Čechy a.s.', 'Telecoms', 'Tereza Králová'),
  ('B-028', 'Čerpací stanice Polabí a.s.', 'Energy', 'Martin Svoboda'),
  ('B-029', 'Inženýring Energo Praha a.s.', 'Engineering', 'Jana Dvořáková'),
  ('B-030', 'Pivovar Lipno n.p.', 'Food and drink', 'Petr Novotný'),
  ('B-031', 'E-obchod Krkonoše a.s.', 'Retail', 'Tereza Králová'),
  ('B-032', 'Pekárny Hanácké a.s.', 'Food and drink', 'Martin Svoboda'),
  ('B-033', 'Velkoobchod Opava a.s.', 'Wholesale', 'Jana Dvořáková'),
  ('B-034', 'Pozemní stavby Zlín a.s.', 'Construction', 'Petr Novotný'),
  ('B-035', 'Monolity Jihlava s.r.o.', 'Construction', 'Tereza Králová'),
  ('B-036', 'Solární farmy Znojmo s.r.o.', 'Energy', 'Martin Svoboda'),
  ('B-037', 'Software Šárka a.s.', 'Technology', 'Jana Dvořáková');

INSERT INTO owners (borrower_id, owner_lei) VALUES
  ('B-001', '315700IJ39O4J6NGER57'),
  ('B-002', '315700IJ39O4J6NGER57'),
  ('B-002', '31570010000000090208'),
  ('B-003', '315700IJ39O4J6NGER57'),
  ('B-004', '315700IJ39O4J6NGER57'),
  ('B-005', '315700IJ39O4J6NGER57'),
  ('B-006', '315700IJ39O4J6NGER57'),
  ('B-007', '213800WFQ334R8UXUG83'),
  ('B-008', '213800WFQ334R8UXUG83'),
  ('B-009', '213800WFQ334R8UXUG83'),
  ('B-010', '213800WFQ334R8UXUG83'),
  ('B-011', '213800WFQ334R8UXUG83'),
  ('B-012', '213800WFQ334R8UXUG83'),
  ('B-013', '549300ML04F32KY6IX85'),
  ('B-014', '549300ML04F32KY6IX85'),
  ('B-015', '549300ML04F32KY6IX85'),
  ('B-016', 'NFONVGN05Z0FMN5PEC35'),
  ('B-017', 'NFONVGN05Z0FMN5PEC35'),
  ('B-018', 'NFONVGN05Z0FMN5PEC35'),
  ('B-019', 'NFONVGN05Z0FMN5PEC35'),
  ('B-020', '31570030000000000259'),
  ('B-021', '31570030000000000259'),
  ('B-022', '529900A7YD9C0LLXM621'),
  ('B-023', '529900A7YD9C0LLXM621'),
  ('B-024', '549300V9QSIG4WX4GJ96'),
  ('B-025', '529900Q33I7A8LA5Y329'),
  ('B-026', '529900Q33I7A8LA5Y329'),
  ('B-027', '213800TB53ELEUKM7Q61'),
  ('B-028', '21380068P1DRHMJ8KU70'),
  ('B-029', 'LAXUQCHT4FH58LRZDY46');

INSERT INTO facilities (facility_id, borrower_id, product, committed_eur, drawn_eur, maturity) VALUES
  (1001, 'B-001', 'Term loan', 24000000, 22080000, DATE '2027-01-28'),
  (1002, 'B-001', 'Revolving credit facility', 14000000, 7700000, DATE '2028-04-28'),
  (1003, 'B-001', 'Bank guarantee', 10000000, 0, DATE '2029-07-28'),
  (1004, 'B-002', 'Term loan', 19000000, 17480000, DATE '2028-06-28'),
  (1005, 'B-002', 'Revolving credit facility', 12000000, 6600000, DATE '2029-09-28'),
  (1006, 'B-003', 'Term loan', 16000000, 14720000, DATE '2029-11-28'),
  (1007, 'B-003', 'Revolving credit facility', 11000000, 6050000, DATE '2030-02-28'),
  (1008, 'B-004', 'Term loan', 13000000, 11960000, DATE '2030-04-28'),
  (1009, 'B-004', 'Revolving credit facility', 9000000, 4950000, DATE '2031-07-28'),
  (1010, 'B-005', 'Term loan', 16000000, 14720000, DATE '2031-09-28'),
  (1011, 'B-005', 'Revolving credit facility', 10000000, 5500000, DATE '2027-12-28'),
  (1012, 'B-006', 'Term loan', 11000000, 10120000, DATE '2027-02-28'),
  (1013, 'B-006', 'Revolving credit facility', 7000000, 3850000, DATE '2028-05-28'),
  (1014, 'B-007', 'Term loan', 22000000, 20240000, DATE '2028-07-28'),
  (1015, 'B-007', 'Revolving credit facility', 13000000, 7150000, DATE '2029-10-28'),
  (1016, 'B-007', 'Bank guarantee', 9000000, 0, DATE '2030-01-28'),
  (1017, 'B-008', 'Term loan', 17000000, 15640000, DATE '2029-12-28'),
  (1018, 'B-008', 'Revolving credit facility', 12000000, 6600000, DATE '2030-03-28'),
  (1019, 'B-009', 'Term loan', 10000000, 9200000, DATE '2030-05-28'),
  (1020, 'B-009', 'Revolving credit facility', 7000000, 3850000, DATE '2031-08-28'),
  (1021, 'B-010', 'Term loan', 20000000, 18400000, DATE '2031-10-28'),
  (1022, 'B-010', 'Revolving credit facility', 13000000, 7150000, DATE '2027-01-28'),
  (1023, 'B-011', 'Term loan', 13000000, 11960000, DATE '2027-03-28'),
  (1024, 'B-011', 'Revolving credit facility', 8000000, 4400000, DATE '2028-06-28'),
  (1025, 'B-012', 'Term loan', 15000000, 13800000, DATE '2028-08-28'),
  (1026, 'B-012', 'Revolving credit facility', 10000000, 5500000, DATE '2029-11-28'),
  (1027, 'B-013', 'Term loan', 26000000, 23920000, DATE '2029-01-28'),
  (1028, 'B-013', 'Revolving credit facility', 16000000, 8800000, DATE '2030-04-28'),
  (1029, 'B-013', 'Bank guarantee', 10000000, 0, DATE '2031-07-28'),
  (1030, 'B-014', 'Term loan', 19000000, 17480000, DATE '2030-06-28'),
  (1031, 'B-014', 'Revolving credit facility', 11000000, 6050000, DATE '2031-09-28'),
  (1032, 'B-014', 'Bank guarantee', 8000000, 0, DATE '2027-12-28'),
  (1033, 'B-015', 'Term loan', 9000000, 8280000, DATE '2031-11-28'),
  (1034, 'B-015', 'Revolving credit facility', 6000000, 3300000, DATE '2027-02-28'),
  (1035, 'B-016', 'Term loan', 13000000, 11960000, DATE '2027-04-28'),
  (1036, 'B-016', 'Revolving credit facility', 9000000, 4950000, DATE '2028-07-28'),
  (1037, 'B-017', 'Term loan', 11000000, 10120000, DATE '2028-09-28'),
  (1038, 'B-017', 'Revolving credit facility', 7000000, 3850000, DATE '2029-12-28'),
  (1039, 'B-018', 'Term loan', 14000000, 12880000, DATE '2029-02-28'),
  (1040, 'B-019', 'Term loan', 16000000, 14720000, DATE '2030-07-28'),
  (1041, 'B-019', 'Revolving credit facility', 11000000, 6050000, DATE '2031-10-28'),
  (1042, 'B-020', 'Term loan', 30000000, 27600000, DATE '2031-12-28'),
  (1043, 'B-020', 'Revolving credit facility', 18000000, 9900000, DATE '2027-03-28'),
  (1044, 'B-020', 'Bank guarantee', 12000000, 0, DATE '2028-06-28'),
  (1045, 'B-021', 'Term loan', 9000000, 8280000, DATE '2027-05-28'),
  (1046, 'B-022', 'Term loan', 21000000, 19320000, DATE '2028-10-28'),
  (1047, 'B-022', 'Revolving credit facility', 12000000, 6600000, DATE '2029-01-28'),
  (1048, 'B-022', 'Bank guarantee', 8000000, 0, DATE '2030-04-28'),
  (1049, 'B-023', 'Term loan', 12000000, 11040000, DATE '2029-03-28'),
  (1050, 'B-024', 'Term loan', 23000000, 21160000, DATE '2030-08-28'),
  (1051, 'B-024', 'Revolving credit facility', 14000000, 7700000, DATE '2031-11-28'),
  (1052, 'B-024', 'Bank guarantee', 9000000, 0, DATE '2027-02-28'),
  (1053, 'B-025', 'Term loan', 20000000, 18400000, DATE '2031-01-28'),
  (1054, 'B-025', 'Revolving credit facility', 14000000, 7700000, DATE '2027-04-28'),
  (1055, 'B-026', 'Term loan', 11000000, 10120000, DATE '2027-06-28'),
  (1056, 'B-026', 'Revolving credit facility', 8000000, 4400000, DATE '2028-09-28'),
  (1057, 'B-027', 'Term loan', 19000000, 17480000, DATE '2028-11-28'),
  (1058, 'B-027', 'Revolving credit facility', 12000000, 6600000, DATE '2029-02-28'),
  (1059, 'B-027', 'Bank guarantee', 8000000, 0, DATE '2030-05-28'),
  (1060, 'B-028', 'Term loan', 17000000, 15640000, DATE '2029-04-28'),
  (1061, 'B-028', 'Revolving credit facility', 11000000, 6050000, DATE '2030-07-28'),
  (1062, 'B-029', 'Term loan', 11000000, 10120000, DATE '2030-09-28'),
  (1063, 'B-030', 'Term loan', 14000000, 12880000, DATE '2031-02-28'),
  (1064, 'B-030', 'Revolving credit facility', 10000000, 5500000, DATE '2027-05-28'),
  (1065, 'B-031', 'Term loan', 24000000, 22080000, DATE '2027-07-28'),
  (1066, 'B-031', 'Revolving credit facility', 14000000, 7700000, DATE '2028-10-28'),
  (1067, 'B-031', 'Bank guarantee', 9000000, 0, DATE '2029-01-28'),
  (1068, 'B-032', 'Term loan', 13000000, 11960000, DATE '2028-12-28'),
  (1069, 'B-033', 'Term loan', 11000000, 10120000, DATE '2029-05-28'),
  (1070, 'B-033', 'Revolving credit facility', 8000000, 4400000, DATE '2030-08-28'),
  (1071, 'B-034', 'Term loan', 10000000, 9200000, DATE '2030-10-28'),
  (1072, 'B-034', 'Revolving credit facility', 6000000, 3300000, DATE '2031-01-28'),
  (1073, 'B-035', 'Term loan', 8000000, 7360000, DATE '2031-03-28'),
  (1074, 'B-036', 'Term loan', 12000000, 11040000, DATE '2027-08-28'),
  (1075, 'B-037', 'Term loan', 6000000, 5520000, DATE '2028-01-28');

INSERT INTO capital (as_of, tier1_eur) VALUES (DATE '2026-09-30', 640000000.00);
