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
-- Bank -- the bank and its book are fictional; its borrowers are real Czech companies, each named by
-- its Legal Entity Identifier (LEI) as the bank books it. The bank knows each borrower and what it
-- lent; it does not record who owns whom.
--
--   counterparties  each borrower, by LEI: its name as booked, the bank's sector, its relationship
--                   manager
--   facilities      each facility lent to a borrower: term loans, revolving credit and guarantees,
--                   what is committed and what is drawn, in EUR
--   capital         the bank's Tier 1 capital, the base of the large-exposure limit (25% of it,
--                   EU CRR Article 395) for any one client or group of connected clients
--
-- 37 borrowers, EUR 971 million committed.

CREATE TABLE counterparties (
  lei char(20) PRIMARY KEY,
  name varchar NOT NULL,
  sector varchar NOT NULL,
  relationship_manager varchar NOT NULL
);

CREATE TABLE facilities (
  facility_id integer PRIMARY KEY,
  lei char(20) NOT NULL REFERENCES counterparties (lei),
  product varchar NOT NULL,
  committed_eur numeric(14, 2) NOT NULL,
  drawn_eur numeric(14, 2) NOT NULL,
  maturity date NOT NULL
);

CREATE TABLE capital (
  as_of date PRIMARY KEY,
  tier1_eur numeric(14, 2) NOT NULL
);

INSERT INTO counterparties (lei, name, sector, relationship_manager) VALUES
  ('315700BBUYTISOHBNX72', 'Pražské vodovody a kanalizace, a.s.', 'Utilities', 'Jana Dvořáková'),
  ('31570010000000032881', 'Pražská teplárenská a.s.', 'Utilities', 'Petr Novotný'),
  ('315700SIBEYEAWSYLB92', 'Veolia Energie ČR, a.s.', 'Utilities', 'Tereza Králová'),
  ('3157006J6U81BE72XQ70', 'Středočeské vodárny, a.s.', 'Utilities', 'Martin Svoboda'),
  ('315700LRO9CWUFOSLP09', 'MORAVSKÁ VODÁRENSKÁ, a.s.', 'Utilities', 'Jana Dvořáková'),
  ('315700E9N4IWMYLZR464', 'Veolia Energie Praha, a.s.', 'Utilities', 'Petr Novotný'),
  ('315700NV6I3O0YKF2J28', 'EUROVIA CS, a.s.', 'Construction', 'Tereza Králová'),
  ('315700MSXDA6CBYQHX43', 'VINCI Energies ČR s.r.o.', 'Construction', 'Martin Svoboda'),
  ('315700JBGD5JC944GN38', 'Axians ICT CZ s.r.o.', 'Technology', 'Jana Dvořáková'),
  ('213800Z39ZW4XHAAUA27', 'Stavby mostů a.s.', 'Construction', 'Petr Novotný'),
  ('315700R4N2U5DETXUT60', 'GJW Praha spol. s r.o.', 'Construction', 'Tereza Králová'),
  ('2138001NW8Y1K1JD6Z84', 'PRŮMSTAV, a.s.', 'Construction', 'Martin Svoboda'),
  ('549300ZH5M6JULTFH175', 'EG.D Holding, s.r.o.', 'Utilities', 'Jana Dvořáková'),
  ('5493004EO3OSZXYHCG78', 'E.ON Energie, a.s.', 'Utilities', 'Petr Novotný'),
  ('529900IOTOEK0X15ES10', 'E.ON Customer Solutions Czechia s.r.o.', 'Utilities', 'Tereza Králová'),
  ('5299001OCDH15TOWRF13', 'Saint-Gobain Adfors CZ s.r.o.', 'Manufacturing', 'Martin Svoboda'),
  ('529900ICUI439HM59347', 'Saint-Gobain PAM CZ s.r.o.', 'Manufacturing', 'Jana Dvořáková'),
  ('529900IMBA1N98URTN21', 'Saint-Gobain Sekurit ČR spol. s r.o.', 'Manufacturing', 'Petr Novotný'),
  ('529900MS61RNNI1PL958', 'Saint-Gobain Construction Products CZ a.s.', 'Manufacturing', 'Tereza Králová'),
  ('31570010000000054609', 'Generali Česká pojišťovna a.s.', 'Insurance', 'Martin Svoboda'),
  ('31570010000000041611', 'Pojišťovna Patricie a.s.', 'Insurance', 'Jana Dvořáková'),
  ('529900LJ3P01VB8JSB31', 'Continental Barum s.r.o.', 'Manufacturing', 'Petr Novotný'),
  ('3157005G1F88901HAL31', 'Continental VÚK s.r.o.', 'Manufacturing', 'Tereza Králová'),
  ('5299008G66F3XXXRZO62', 'Siemens, s.r.o.', 'Manufacturing', 'Martin Svoboda'),
  ('529900R3IPCKHXJDUK29', 'OEZ s.r.o.', 'Manufacturing', 'Jana Dvořáková'),
  ('5299003ILFQKHJYNK282', 'T-Mobile Czech Republic a.s.', 'Telecoms', 'Petr Novotný'),
  ('213800BK4RVHF9F2TI85', 'Vodafone Czech Republic a.s.', 'Telecoms', 'Tereza Králová'),
  ('213800GFNAQ5J7UO4C22', 'Shell Czech Republic a.s.', 'Energy', 'Martin Svoboda'),
  ('3157008NR0WMEIJUCX48', 'Tractebel Engineering a.s.', 'Engineering', 'Jana Dvořáková'),
  ('31570010000000006303', 'Budějovický Budvar, n.p.', 'Food and drink', 'Petr Novotný'),
  ('315700MJRNJSVB95U852', 'Alza.cz a.s.', 'Retail', 'Tereza Králová'),
  ('315700ZK0SK94900L056', 'PEK Group a.s.', 'Food and drink', 'Martin Svoboda'),
  ('8945006E6MDJI98E4T61', 'FAST ČR, a.s.', 'Wholesale', 'Jana Dvořáková'),
  ('315700X3NSNAKRW9CN85', 'BEST, a.s.', 'Construction', 'Petr Novotný'),
  ('3157007JGSGEZ1TESO89', 'Monzas s.r.o.', 'Construction', 'Tereza Králová'),
  ('315700PWW41IJ2RM0G35', 'Bonett Energie s.r.o.', 'Energy', 'Martin Svoboda'),
  ('315700BEDUVR2H0FM180', 'ZONER software, a.s.', 'Technology', 'Jana Dvořáková');

INSERT INTO facilities (facility_id, lei, product, committed_eur, drawn_eur, maturity) VALUES
  (1001, '315700BBUYTISOHBNX72', 'Term loan', 24000000, 22080000, DATE '2027-01-28'),
  (1002, '315700BBUYTISOHBNX72', 'Revolving credit facility', 14000000, 7700000, DATE '2028-04-28'),
  (1003, '315700BBUYTISOHBNX72', 'Bank guarantee', 10000000, 0, DATE '2029-07-28'),
  (1004, '31570010000000032881', 'Term loan', 19000000, 17480000, DATE '2028-06-28'),
  (1005, '31570010000000032881', 'Revolving credit facility', 12000000, 6600000, DATE '2029-09-28'),
  (1006, '315700SIBEYEAWSYLB92', 'Term loan', 16000000, 14720000, DATE '2029-11-28'),
  (1007, '315700SIBEYEAWSYLB92', 'Revolving credit facility', 11000000, 6050000, DATE '2030-02-28'),
  (1008, '3157006J6U81BE72XQ70', 'Term loan', 13000000, 11960000, DATE '2030-04-28'),
  (1009, '3157006J6U81BE72XQ70', 'Revolving credit facility', 9000000, 4950000, DATE '2031-07-28'),
  (1010, '315700LRO9CWUFOSLP09', 'Term loan', 16000000, 14720000, DATE '2031-09-28'),
  (1011, '315700LRO9CWUFOSLP09', 'Revolving credit facility', 10000000, 5500000, DATE '2027-12-28'),
  (1012, '315700E9N4IWMYLZR464', 'Term loan', 11000000, 10120000, DATE '2027-02-28'),
  (1013, '315700E9N4IWMYLZR464', 'Revolving credit facility', 7000000, 3850000, DATE '2028-05-28'),
  (1014, '315700NV6I3O0YKF2J28', 'Term loan', 22000000, 20240000, DATE '2028-07-28'),
  (1015, '315700NV6I3O0YKF2J28', 'Revolving credit facility', 13000000, 7150000, DATE '2029-10-28'),
  (1016, '315700NV6I3O0YKF2J28', 'Bank guarantee', 9000000, 0, DATE '2030-01-28'),
  (1017, '315700MSXDA6CBYQHX43', 'Term loan', 17000000, 15640000, DATE '2029-12-28'),
  (1018, '315700MSXDA6CBYQHX43', 'Revolving credit facility', 12000000, 6600000, DATE '2030-03-28'),
  (1019, '315700JBGD5JC944GN38', 'Term loan', 10000000, 9200000, DATE '2030-05-28'),
  (1020, '315700JBGD5JC944GN38', 'Revolving credit facility', 7000000, 3850000, DATE '2031-08-28'),
  (1021, '213800Z39ZW4XHAAUA27', 'Term loan', 20000000, 18400000, DATE '2031-10-28'),
  (1022, '213800Z39ZW4XHAAUA27', 'Revolving credit facility', 13000000, 7150000, DATE '2027-01-28'),
  (1023, '315700R4N2U5DETXUT60', 'Term loan', 13000000, 11960000, DATE '2027-03-28'),
  (1024, '315700R4N2U5DETXUT60', 'Revolving credit facility', 8000000, 4400000, DATE '2028-06-28'),
  (1025, '2138001NW8Y1K1JD6Z84', 'Term loan', 15000000, 13800000, DATE '2028-08-28'),
  (1026, '2138001NW8Y1K1JD6Z84', 'Revolving credit facility', 10000000, 5500000, DATE '2029-11-28'),
  (1027, '549300ZH5M6JULTFH175', 'Term loan', 26000000, 23920000, DATE '2029-01-28'),
  (1028, '549300ZH5M6JULTFH175', 'Revolving credit facility', 16000000, 8800000, DATE '2030-04-28'),
  (1029, '549300ZH5M6JULTFH175', 'Bank guarantee', 10000000, 0, DATE '2031-07-28'),
  (1030, '5493004EO3OSZXYHCG78', 'Term loan', 19000000, 17480000, DATE '2030-06-28'),
  (1031, '5493004EO3OSZXYHCG78', 'Revolving credit facility', 11000000, 6050000, DATE '2031-09-28'),
  (1032, '5493004EO3OSZXYHCG78', 'Bank guarantee', 8000000, 0, DATE '2027-12-28'),
  (1033, '529900IOTOEK0X15ES10', 'Term loan', 9000000, 8280000, DATE '2031-11-28'),
  (1034, '529900IOTOEK0X15ES10', 'Revolving credit facility', 6000000, 3300000, DATE '2027-02-28'),
  (1035, '5299001OCDH15TOWRF13', 'Term loan', 13000000, 11960000, DATE '2027-04-28'),
  (1036, '5299001OCDH15TOWRF13', 'Revolving credit facility', 9000000, 4950000, DATE '2028-07-28'),
  (1037, '529900ICUI439HM59347', 'Term loan', 11000000, 10120000, DATE '2028-09-28'),
  (1038, '529900ICUI439HM59347', 'Revolving credit facility', 7000000, 3850000, DATE '2029-12-28'),
  (1039, '529900IMBA1N98URTN21', 'Term loan', 14000000, 12880000, DATE '2029-02-28'),
  (1040, '529900MS61RNNI1PL958', 'Term loan', 16000000, 14720000, DATE '2030-07-28'),
  (1041, '529900MS61RNNI1PL958', 'Revolving credit facility', 11000000, 6050000, DATE '2031-10-28'),
  (1042, '31570010000000054609', 'Term loan', 30000000, 27600000, DATE '2031-12-28'),
  (1043, '31570010000000054609', 'Revolving credit facility', 18000000, 9900000, DATE '2027-03-28'),
  (1044, '31570010000000054609', 'Bank guarantee', 12000000, 0, DATE '2028-06-28'),
  (1045, '31570010000000041611', 'Term loan', 9000000, 8280000, DATE '2027-05-28'),
  (1046, '529900LJ3P01VB8JSB31', 'Term loan', 21000000, 19320000, DATE '2028-10-28'),
  (1047, '529900LJ3P01VB8JSB31', 'Revolving credit facility', 12000000, 6600000, DATE '2029-01-28'),
  (1048, '529900LJ3P01VB8JSB31', 'Bank guarantee', 8000000, 0, DATE '2030-04-28'),
  (1049, '3157005G1F88901HAL31', 'Term loan', 12000000, 11040000, DATE '2029-03-28'),
  (1050, '5299008G66F3XXXRZO62', 'Term loan', 20000000, 18400000, DATE '2030-08-28'),
  (1051, '5299008G66F3XXXRZO62', 'Revolving credit facility', 14000000, 7700000, DATE '2031-11-28'),
  (1052, '529900R3IPCKHXJDUK29', 'Term loan', 11000000, 10120000, DATE '2031-01-28'),
  (1053, '529900R3IPCKHXJDUK29', 'Revolving credit facility', 8000000, 4400000, DATE '2027-04-28'),
  (1054, '5299003ILFQKHJYNK282', 'Term loan', 23000000, 21160000, DATE '2027-06-28'),
  (1055, '5299003ILFQKHJYNK282', 'Revolving credit facility', 14000000, 7700000, DATE '2028-09-28'),
  (1056, '5299003ILFQKHJYNK282', 'Bank guarantee', 9000000, 0, DATE '2029-12-28'),
  (1057, '213800BK4RVHF9F2TI85', 'Term loan', 19000000, 17480000, DATE '2028-11-28'),
  (1058, '213800BK4RVHF9F2TI85', 'Revolving credit facility', 12000000, 6600000, DATE '2029-02-28'),
  (1059, '213800BK4RVHF9F2TI85', 'Bank guarantee', 8000000, 0, DATE '2030-05-28'),
  (1060, '213800GFNAQ5J7UO4C22', 'Term loan', 17000000, 15640000, DATE '2029-04-28'),
  (1061, '213800GFNAQ5J7UO4C22', 'Revolving credit facility', 11000000, 6050000, DATE '2030-07-28'),
  (1062, '3157008NR0WMEIJUCX48', 'Term loan', 11000000, 10120000, DATE '2030-09-28'),
  (1063, '31570010000000006303', 'Term loan', 14000000, 12880000, DATE '2031-02-28'),
  (1064, '31570010000000006303', 'Revolving credit facility', 10000000, 5500000, DATE '2027-05-28'),
  (1065, '315700MJRNJSVB95U852', 'Term loan', 24000000, 22080000, DATE '2027-07-28'),
  (1066, '315700MJRNJSVB95U852', 'Revolving credit facility', 14000000, 7700000, DATE '2028-10-28'),
  (1067, '315700MJRNJSVB95U852', 'Bank guarantee', 9000000, 0, DATE '2029-01-28'),
  (1068, '315700ZK0SK94900L056', 'Term loan', 13000000, 11960000, DATE '2028-12-28'),
  (1069, '8945006E6MDJI98E4T61', 'Term loan', 11000000, 10120000, DATE '2029-05-28'),
  (1070, '8945006E6MDJI98E4T61', 'Revolving credit facility', 8000000, 4400000, DATE '2030-08-28'),
  (1071, '315700X3NSNAKRW9CN85', 'Term loan', 10000000, 9200000, DATE '2030-10-28'),
  (1072, '315700X3NSNAKRW9CN85', 'Revolving credit facility', 6000000, 3300000, DATE '2031-01-28'),
  (1073, '3157007JGSGEZ1TESO89', 'Term loan', 8000000, 7360000, DATE '2031-03-28'),
  (1074, '315700PWW41IJ2RM0G35', 'Term loan', 12000000, 11040000, DATE '2027-08-28'),
  (1075, '315700BEDUVR2H0FM180', 'Term loan', 6000000, 5520000, DATE '2028-01-28');

INSERT INTO capital (as_of, tier1_eur) VALUES (DATE '2026-09-30', 640000000.00);
