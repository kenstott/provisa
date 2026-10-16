-- Copyright (c) 2026 Kenneth Stott
--
-- This source code is licensed under the Business Source License 1.1
-- found in the LICENSE file in the root directory of this source tree.
--
-- NOTICE: Use of this software for training artificial intelligence or
-- machine learning models is strictly prohibited without explicit written
-- permission from the copyright holder.
--
-- A database of the data vault demo (REQ-2063), loaded from Microsoft's Northwind
-- sample database (samples/databases/northwind-pubs/instnwnd.sql in microsoft/sql-server-samples;
-- MIT licence, Copyright (c) Microsoft Corporation -- see db/NORTHWIND-LICENSE).
-- Its customers, suppliers, categories, products, orders and order lines, split in two:
--
--   orders  the order system: customers, products and their suppliers and categories, orders and
--           their lines
--   crm     the CRM: its own copy of the customers, as accounts, with each contact's email
--
-- Customers, products and accounts are kept insert-only, as a data vault's staging receives them:
-- every row carries its load date, and a change arrives as a new row (setup.yaml's commands send
-- the demo's scripted changes so). The vault (config.yaml) is Provisa's views over both; none of it
-- is here.

CREATE ROLE provisa LOGIN PASSWORD 'provisa';
CREATE SCHEMA crm AUTHORIZATION provisa;
SET ROLE provisa;
SET search_path = crm;

-- Insert-only, as the vault's staging receives it: a change arrives as a new row with its load
-- date, and no row is ever overwritten.
CREATE TABLE account (
    account_code text NOT NULL,
    account_name text NOT NULL,
    contact_name text,
    contact_title text,
    contact_email text,
    phone text,
    address text,
    city text,
    postal_code text,
    country text,
    load_date timestamp NOT NULL,
    PRIMARY KEY (account_code, load_date)
);

INSERT INTO account VALUES
    ('ALFKI', 'Alfreds Futterkiste', 'Maria Anders', 'Sales Representative', 'maria.anders@alfreds.example', '030-0074321', 'Obere Str. 57', 'Berlin', '12209', 'Germany', '1996-07-01 09:00:00'),
    ('ANATR', 'Ana Trujillo Emparedados y helados', 'Ana Trujillo', 'Owner', 'ana.trujillo@ana.example', '(5) 555-4729', 'Avda. de la Constitución 2222', 'México D.F.', '05021', 'Mexico', '1996-07-01 09:00:00'),
    ('ANTON', 'Antonio Moreno Taquería', 'Antonio Moreno', 'Owner', 'antonio.moreno@antonio.example', '(5) 555-3932', 'Mataderos  2312', 'México D.F.', '05023', 'Mexico', '1996-07-01 09:00:00'),
    ('AROUT', 'Around the Horn', 'Thomas Hardy', 'Sales Representative', 'thomas.hardy@around.example', '(171) 555-7788', '120 Hanover Sq.', 'London', 'WA1 1DP', 'UK', '1996-07-01 09:00:00'),
    ('BERGS', 'Berglunds snabbköp', 'Christina Berglund', 'Order Administrator', 'christina.berglund@berglunds.example', '0921-12 34 65', 'Berguvsvägen  8', 'Luleå', 'S-958 22', 'Sweden', '1996-07-01 09:00:00'),
    ('BLAUS', 'Blauer See Delikatessen', 'Hanna Moos', 'Sales Representative', 'hanna.moos@blauer.example', '0621-08460', 'Forsterstr. 57', 'Mannheim', '68306', 'Germany', '1996-07-01 09:00:00'),
    ('BLONP', 'Blondesddsl père et fils', 'Frédérique Citeaux', 'Marketing Manager', 'frederique.citeaux@blondesddsl.example', '88.60.15.31', '24, place Kléber', 'Strasbourg', '67000', 'France', '1996-07-01 09:00:00'),
    ('BOLID', 'Bólido Comidas preparadas', 'Martín Sommer', 'Owner', 'martin.sommer@bolido.example', '(91) 555 22 82', 'C/ Araquil, 67', 'Madrid', '28023', 'Spain', '1996-07-01 09:00:00'),
    ('BONAP', 'Bon app''', 'Laurence Lebihan', 'Owner', 'laurence.lebihan@bon.example', '91.24.45.40', '12, rue des Bouchers', 'Marseille', '13008', 'France', '1996-07-01 09:00:00'),
    ('BOTTM', 'Bottom-Dollar Markets', 'Elizabeth Lincoln', 'Accounting Manager', 'elizabeth.lincoln@bottomdollar.example', '(604) 555-4729', '23 Tsawassen Blvd.', 'Tsawassen', 'T2F 8M4', 'Canada', '1996-07-01 09:00:00'),
    ('BSBEV', 'B''s Beverages', 'Victoria Ashworth', 'Sales Representative', 'victoria.ashworth@bs.example', '(171) 555-1212', 'Fauntleroy Circus', 'London', 'EC2 5NT', 'UK', '1996-07-01 09:00:00'),
    ('CACTU', 'Cactus Comidas para llevar', 'Patricio Simpson', 'Sales Agent', 'patricio.simpson@cactus.example', '(1) 135-5555', 'Cerrito 333', 'Buenos Aires', '1010', 'Argentina', '1996-07-01 09:00:00'),
    ('CENTC', 'Centro comercial Moctezuma', 'Francisco Chang', 'Marketing Manager', 'francisco.chang@centro.example', '(5) 555-3392', 'Sierras de Granada 9993', 'México D.F.', '05022', 'Mexico', '1996-07-01 09:00:00'),
    ('CHOPS', 'Chop-suey Chinese', 'Yang Wang', 'Owner', 'yang.wang@chopsuey.example', '0452-076545', 'Hauptstr. 29', 'Bern', '3012', 'Switzerland', '1996-07-01 09:00:00'),
    ('COMMI', 'Comércio Mineiro', 'Pedro Afonso', 'Sales Associate', 'pedro.afonso@comercio.example', '(11) 555-7647', 'Av. dos Lusíadas, 23', 'Sao Paulo', '05432-043', 'Brazil', '1996-07-01 09:00:00'),
    ('CONSH', 'Consolidated Holdings', 'Elizabeth Brown', 'Sales Representative', 'elizabeth.brown@consolidated.example', '(171) 555-2282', 'Berkeley Gardens 12  Brewery', 'London', 'WX1 6LT', 'UK', '1996-07-01 09:00:00'),
    ('DRACD', 'Drachenblut Delikatessen', 'Sven Ottlieb', 'Order Administrator', 'sven.ottlieb@drachenblut.example', '0241-039123', 'Walserweg 21', 'Aachen', '52066', 'Germany', '1996-07-01 09:00:00'),
    ('DUMON', 'Du monde entier', 'Janine Labrune', 'Owner', 'janine.labrune@du.example', '40.67.88.88', '67, rue des Cinquante Otages', 'Nantes', '44000', 'France', '1996-07-01 09:00:00'),
    ('EASTC', 'Eastern Connection', 'Ann Devon', 'Sales Agent', 'ann.devon@eastern.example', '(171) 555-0297', '35 King George', 'London', 'WX3 6FW', 'UK', '1996-07-01 09:00:00'),
    ('ERNSH', 'Ernst Handel', 'Roland Mendel', 'Sales Manager', 'roland.mendel@ernst.example', '7675-3425', 'Kirchgasse 6', 'Graz', '8010', 'Austria', '1996-07-01 09:00:00'),
    ('FAMIA', 'Familia Arquibaldo', 'Aria Cruz', 'Marketing Assistant', 'aria.cruz@familia.example', '(11) 555-9857', 'Rua Orós, 92', 'Sao Paulo', '05442-030', 'Brazil', '1996-07-01 09:00:00'),
    ('FISSA', 'FISSA Fabrica Inter. Salchichas S.A.', 'Diego Roel', 'Accounting Manager', 'diego.roel@fissa.example', '(91) 555 94 44', 'C/ Moralzarzal, 86', 'Madrid', '28034', 'Spain', '1996-07-01 09:00:00'),
    ('FOLIG', 'Folies gourmandes', 'Martine Rancé', 'Assistant Sales Agent', 'martine.rance@folies.example', '20.16.10.16', '184, chaussée de Tournai', 'Lille', '59000', 'France', '1996-07-01 09:00:00'),
    ('FOLKO', 'Folk och fä HB', 'Maria Larsson', 'Owner', 'maria.larsson@folk.example', '0695-34 67 21', 'Åkergatan 24', 'Bräcke', 'S-844 67', 'Sweden', '1996-07-01 09:00:00'),
    ('FRANK', 'Frankenversand', 'Peter Franken', 'Marketing Manager', 'peter.franken@frankenversand.example', '089-0877310', 'Berliner Platz 43', 'München', '80805', 'Germany', '1996-07-01 09:00:00'),
    ('FRANR', 'France restauration', 'Carine Schmitt', 'Marketing Manager', 'carine.schmitt@france.example', '40.32.21.21', '54, rue Royale', 'Nantes', '44000', 'France', '1996-07-01 09:00:00'),
    ('FRANS', 'Franchi S.p.A.', 'Paolo Accorti', 'Sales Representative', 'paolo.accorti@franchi.example', '011-4988260', 'Via Monte Bianco 34', 'Torino', '10100', 'Italy', '1996-07-01 09:00:00'),
    ('FURIB', 'Furia Bacalhau e Frutos do Mar', 'Lino Rodriguez', 'Sales Manager', 'lino.rodriguez@furia.example', '(1) 354-2534', 'Jardim das rosas n. 32', 'Lisboa', '1675', 'Portugal', '1996-07-01 09:00:00'),
    ('GALED', 'Galería del gastrónomo', 'Eduardo Saavedra', 'Marketing Manager', 'eduardo.saavedra@galeria.example', '(93) 203 4560', 'Rambla de Cataluña, 23', 'Barcelona', '08022', 'Spain', '1996-07-01 09:00:00'),
    ('GODOS', 'Godos Cocina Típica', 'José Pedro Freyre', 'Sales Manager', 'jose.freyre@godos.example', '(95) 555 82 82', 'C/ Romero, 33', 'Sevilla', '41101', 'Spain', '1996-07-01 09:00:00'),
    ('GOURL', 'Gourmet Lanchonetes', 'André Fonseca', 'Sales Associate', 'andre.fonseca@gourmet.example', '(11) 555-9482', 'Av. Brasil, 442', 'Campinas', '04876-786', 'Brazil', '1996-07-01 09:00:00'),
    ('GREAL', 'Great Lakes Food Market', 'Howard Snyder', 'Marketing Manager', 'howard.snyder@great.example', '(503) 555-7555', '2732 Baker Blvd.', 'Eugene', '97403', 'USA', '1996-07-01 09:00:00'),
    ('GROSR', 'GROSELLA-Restaurante', 'Manuel Pereira', 'Owner', 'manuel.pereira@grosellarestaurante.example', '(2) 283-2951', '5ª Ave. Los Palos Grandes', 'Caracas', '1081', 'Venezuela', '1996-07-01 09:00:00'),
    ('HANAR', 'Hanari Carnes', 'Mario Pontes', 'Accounting Manager', 'mario.pontes@hanari.example', '(21) 555-0091', 'Rua do Paço, 67', 'Rio de Janeiro', '05454-876', 'Brazil', '1996-07-01 09:00:00'),
    ('HILAA', 'HILARION-Abastos', 'Carlos Hernández', 'Sales Representative', 'carlos.hernandez@hilarionabastos.example', '(5) 555-1340', 'Carrera 22 con Ave. Carlos Soublette #8-35', 'San Cristóbal', '5022', 'Venezuela', '1996-07-01 09:00:00'),
    ('HUNGC', 'Hungry Coyote Import Store', 'Yoshi Latimer', 'Sales Representative', 'yoshi.latimer@hungry.example', '(503) 555-6874', 'City Center Plaza 516 Main St.', 'Elgin', '97827', 'USA', '1996-07-01 09:00:00'),
    ('HUNGO', 'Hungry Owl All-Night Grocers', 'Patricia McKenna', 'Sales Associate', 'patricia.mckenna@hungry.example', '2967 542', '8 Johnstown Road', 'Cork', NULL, 'Ireland', '1996-07-01 09:00:00'),
    ('ISLAT', 'Island Trading', 'Helen Bennett', 'Marketing Manager', 'helen.bennett@island.example', '(198) 555-8888', 'Garden House Crowther Way', 'Cowes', 'PO31 7PJ', 'UK', '1996-07-01 09:00:00'),
    ('KOENE', 'Königlich Essen', 'Philip Cramer', 'Sales Associate', 'philip.cramer@koniglich.example', '0555-09876', 'Maubelstr. 90', 'Brandenburg', '14776', 'Germany', '1996-07-01 09:00:00'),
    ('LACOR', 'La corne d''abondance', 'Daniel Tonini', 'Sales Representative', 'daniel.tonini@la.example', '30.59.84.10', '67, avenue de l''Europe', 'Versailles', '78000', 'France', '1996-07-01 09:00:00'),
    ('LAMAI', 'La maison d''Asie', 'Annette Roulet', 'Sales Manager', 'annette.roulet@la.example', '61.77.61.10', '1 rue Alsace-Lorraine', 'Toulouse', '31000', 'France', '1996-07-01 09:00:00'),
    ('LAUGB', 'Laughing Bacchus Wine Cellars', 'Yoshi Tannamuri', 'Marketing Assistant', 'yoshi.tannamuri@laughing.example', '(604) 555-3392', '1900 Oak St.', 'Vancouver', 'V3F 2K1', 'Canada', '1996-07-01 09:00:00'),
    ('LAZYK', 'Lazy K Kountry Store', 'John Steel', 'Marketing Manager', 'john.steel@lazy.example', '(509) 555-7969', '12 Orchestra Terrace', 'Walla Walla', '99362', 'USA', '1996-07-01 09:00:00'),
    ('LEHMS', 'Lehmanns Marktstand', 'Renate Messner', 'Sales Representative', 'renate.messner@lehmanns.example', '069-0245984', 'Magazinweg 7', 'Frankfurt a.M.', '60528', 'Germany', '1996-07-01 09:00:00'),
    ('LETSS', 'Let''s Stop N Shop', 'Jaime Yorres', 'Owner', 'jaime.yorres@lets.example', '(415) 555-5938', '87 Polk St. Suite 5', 'San Francisco', '94117', 'USA', '1996-07-01 09:00:00'),
    ('LILAS', 'LILA-Supermercado', 'Carlos González', 'Accounting Manager', 'carlos.gonzalez@lilasupermercado.example', '(9) 331-6954', 'Carrera 52 con Ave. Bolívar #65-98 Llano Largo', 'Barquisimeto', '3508', 'Venezuela', '1996-07-01 09:00:00'),
    ('LINOD', 'LINO-Delicateses', 'Felipe Izquierdo', 'Owner', 'felipe.izquierdo@linodelicateses.example', '(8) 34-56-12', 'Ave. 5 de Mayo Porlamar', 'I. de Margarita', '4980', 'Venezuela', '1996-07-01 09:00:00'),
    ('LONEP', 'Lonesome Pine Restaurant', 'Fran Wilson', 'Sales Manager', 'fran.wilson@lonesome.example', '(503) 555-9573', '89 Chiaroscuro Rd.', 'Portland', '97219', 'USA', '1996-07-01 09:00:00'),
    ('MAGAA', 'Magazzini Alimentari Riuniti', 'Giovanni Rovelli', 'Marketing Manager', 'giovanni.rovelli@magazzini.example', '035-640230', 'Via Ludovico il Moro 22', 'Bergamo', '24100', 'Italy', '1996-07-01 09:00:00'),
    ('MAISD', 'Maison Dewey', 'Catherine Dewey', 'Sales Agent', 'catherine.dewey@maison.example', '(02) 201 24 67', 'Rue Joseph-Bens 532', 'Bruxelles', 'B-1180', 'Belgium', '1996-07-01 09:00:00'),
    ('MEREP', 'Mère Paillarde', 'Jean Fresnière', 'Marketing Assistant', 'jean.fresniere@mere.example', '(514) 555-8054', '43 rue St. Laurent', 'Montréal', 'H1J 1C3', 'Canada', '1996-07-01 09:00:00'),
    ('MORGK', 'Morgenstern Gesundkost', 'Alexander Feuer', 'Marketing Assistant', 'alexander.feuer@morgenstern.example', '0342-023176', 'Heerstr. 22', 'Leipzig', '04179', 'Germany', '1996-07-01 09:00:00'),
    ('NORTS', 'North/South', 'Simon Crowther', 'Sales Associate', 'simon.crowther@northsouth.example', '(171) 555-7733', 'South House 300 Queensbridge', 'London', 'SW7 1RZ', 'UK', '1996-07-01 09:00:00'),
    ('OCEAN', 'Océano Atlántico Ltda.', 'Yvonne Moncada', 'Sales Agent', 'yvonne.moncada@oceano.example', '(1) 135-5333', 'Ing. Gustavo Moncada 8585 Piso 20-A', 'Buenos Aires', '1010', 'Argentina', '1996-07-01 09:00:00'),
    ('OLDWO', 'Old World Delicatessen', 'Rene Phillips', 'Sales Representative', 'rene.phillips@old.example', '(907) 555-7584', '2743 Bering St.', 'Anchorage', '99508', 'USA', '1996-07-01 09:00:00'),
    ('OTTIK', 'Ottilies Käseladen', 'Henriette Pfalzheim', 'Owner', 'henriette.pfalzheim@ottilies.example', '0221-0644327', 'Mehrheimerstr. 369', 'Köln', '50739', 'Germany', '1996-07-01 09:00:00'),
    ('PARIS', 'Paris spécialités', 'Marie Bertrand', 'Owner', 'marie.bertrand@paris.example', '(1) 42.34.22.66', '265, boulevard Charonne', 'Paris', '75012', 'France', '1996-07-01 09:00:00'),
    ('PERIC', 'Pericles Comidas clásicas', 'Guillermo Fernández', 'Sales Representative', 'guillermo.fernandez@pericles.example', '(5) 552-3745', 'Calle Dr. Jorge Cash 321', 'México D.F.', '05033', 'Mexico', '1996-07-01 09:00:00'),
    ('PICCO', 'Piccolo und mehr', 'Georg Pipps', 'Sales Manager', 'georg.pipps@piccolo.example', '6562-9722', 'Geislweg 14', 'Salzburg', '5020', 'Austria', '1996-07-01 09:00:00'),
    ('PRINI', 'Princesa Isabel Vinhos', 'Isabel de Castro', 'Sales Representative', 'isabel.castro@princesa.example', '(1) 356-5634', 'Estrada da saúde n. 58', 'Lisboa', '1756', 'Portugal', '1996-07-01 09:00:00'),
    ('QUEDE', 'Que Delícia', 'Bernardo Batista', 'Accounting Manager', 'bernardo.batista@que.example', '(21) 555-4252', 'Rua da Panificadora, 12', 'Rio de Janeiro', '02389-673', 'Brazil', '1996-07-01 09:00:00'),
    ('QUEEN', 'Queen Cozinha', 'Lúcia Carvalho', 'Marketing Assistant', 'lucia.carvalho@queen.example', '(11) 555-1189', 'Alameda dos Canàrios, 891', 'Sao Paulo', '05487-020', 'Brazil', '1996-07-01 09:00:00'),
    ('QUICK', 'QUICK-Stop', 'Horst Kloss', 'Accounting Manager', 'horst.kloss@quickstop.example', '0372-035188', 'Taucherstraße 10', 'Cunewalde', '01307', 'Germany', '1996-07-01 09:00:00'),
    ('RANCH', 'Rancho grande', 'Sergio Gutiérrez', 'Sales Representative', 'sergio.gutierrez@rancho.example', '(1) 123-5555', 'Av. del Libertador 900', 'Buenos Aires', '1010', 'Argentina', '1996-07-01 09:00:00'),
    ('RATTC', 'Rattlesnake Canyon Grocery', 'Paula Wilson', 'Assistant Sales Representative', 'paula.wilson@rattlesnake.example', '(505) 555-5939', '2817 Milton Dr.', 'Albuquerque', '87110', 'USA', '1996-07-01 09:00:00'),
    ('REGGC', 'Reggiani Caseifici', 'Maurizio Moroni', 'Sales Associate', 'maurizio.moroni@reggiani.example', '0522-556721', 'Strada Provinciale 124', 'Reggio Emilia', '42100', 'Italy', '1996-07-01 09:00:00'),
    ('RICAR', 'Ricardo Adocicados', 'Janete Limeira', 'Assistant Sales Agent', 'janete.limeira@ricardo.example', '(21) 555-3412', 'Av. Copacabana, 267', 'Rio de Janeiro', '02389-890', 'Brazil', '1996-07-01 09:00:00'),
    ('RICSU', 'Richter Supermarkt', 'Michael Holz', 'Sales Manager', 'michael.holz@richter.example', '0897-034214', 'Grenzacherweg 237', 'Genève', '1203', 'Switzerland', '1996-07-01 09:00:00'),
    ('ROMEY', 'Romero y tomillo', 'Alejandra Camino', 'Accounting Manager', 'alejandra.camino@romero.example', '(91) 745 6200', 'Gran Vía, 1', 'Madrid', '28001', 'Spain', '1996-07-01 09:00:00'),
    ('SANTG', 'Santé Gourmet', 'Jonas Bergulfsen', 'Owner', 'jonas.bergulfsen@sante.example', '07-98 92 35', 'Erling Skakkes gate 78', 'Stavern', '4110', 'Norway', '1996-07-01 09:00:00'),
    ('SAVEA', 'Save-a-lot Markets', 'Jose Pavarotti', 'Sales Representative', 'jose.pavarotti@savealot.example', '(208) 555-8097', '187 Suffolk Ln.', 'Boise', '83720', 'USA', '1996-07-01 09:00:00'),
    ('SEVES', 'Seven Seas Imports', 'Hari Kumar', 'Sales Manager', 'hari.kumar@seven.example', '(171) 555-1717', '90 Wadhurst Rd.', 'London', 'OX15 4NB', 'UK', '1996-07-01 09:00:00'),
    ('SIMOB', 'Simons bistro', 'Jytte Petersen', 'Owner', 'jytte.petersen@simons.example', '31 12 34 56', 'Vinbæltet 34', 'Kobenhavn', '1734', 'Denmark', '1996-07-01 09:00:00'),
    ('SPECD', 'Spécialités du monde', 'Dominique Perrier', 'Marketing Manager', 'dominique.perrier@specialites.example', '(1) 47.55.60.10', '25, rue Lauriston', 'Paris', '75016', 'France', '1996-07-01 09:00:00'),
    ('SPLIR', 'Split Rail Beer & Ale', 'Art Braunschweiger', 'Sales Manager', 'art.braunschweiger@split.example', '(307) 555-4680', 'P.O. Box 555', 'Lander', '82520', 'USA', '1996-07-01 09:00:00'),
    ('SUPRD', 'Suprêmes délices', 'Pascale Cartrain', 'Accounting Manager', 'pascale.cartrain@supremes.example', '(071) 23 67 22 20', 'Boulevard Tirou, 255', 'Charleroi', 'B-6000', 'Belgium', '1996-07-01 09:00:00'),
    ('THEBI', 'The Big Cheese', 'Liz Nixon', 'Marketing Manager', 'liz.nixon@the.example', '(503) 555-3612', '89 Jefferson Way Suite 2', 'Portland', '97201', 'USA', '1996-07-01 09:00:00'),
    ('THECR', 'The Cracker Box', 'Liu Wong', 'Marketing Assistant', 'liu.wong@the.example', '(406) 555-5834', '55 Grizzly Peak Rd.', 'Butte', '59801', 'USA', '1996-07-01 09:00:00'),
    ('TOMSP', 'Toms Spezialitäten', 'Karin Josephs', 'Marketing Manager', 'karin.josephs@toms.example', '0251-031259', 'Luisenstr. 48', 'Münster', '44087', 'Germany', '1996-07-01 09:00:00'),
    ('TORTU', 'Tortuga Restaurante', 'Miguel Angel Paolino', 'Owner', 'miguel.paolino@tortuga.example', '(5) 555-2933', 'Avda. Azteca 123', 'México D.F.', '05033', 'Mexico', '1996-07-01 09:00:00'),
    ('TRADH', 'Tradição Hipermercados', 'Anabela Domingues', 'Sales Representative', 'anabela.domingues@tradicao.example', '(11) 555-2167', 'Av. Inês de Castro, 414', 'Sao Paulo', '05634-030', 'Brazil', '1996-07-01 09:00:00'),
    ('TRAIH', 'Trail''s Head Gourmet Provisioners', 'Helvetius Nagy', 'Sales Associate', 'helvetius.nagy@trails.example', '(206) 555-8257', '722 DaVinci Blvd.', 'Kirkland', '98034', 'USA', '1996-07-01 09:00:00'),
    ('VAFFE', 'Vaffeljernet', 'Palle Ibsen', 'Sales Manager', 'palle.ibsen@vaffeljernet.example', '86 21 32 43', 'Smagsloget 45', 'Århus', '8200', 'Denmark', '1996-07-01 09:00:00'),
    ('VICTE', 'Victuailles en stock', 'Mary Saveley', 'Sales Agent', 'mary.saveley@victuailles.example', '78.32.54.86', '2, rue du Commerce', 'Lyon', '69004', 'France', '1996-07-01 09:00:00'),
    ('VINET', 'Vins et alcools Chevalier', 'Paul Henriot', 'Accounting Manager', 'paul.henriot@vins.example', '26.47.15.10', '59 rue de l''Abbaye', 'Reims', '51100', 'France', '1996-07-01 09:00:00'),
    ('WANDK', 'Die Wandernde Kuh', 'Rita Müller', 'Sales Representative', 'rita.muller@die.example', '0711-020361', 'Adenauerallee 900', 'Stuttgart', '70563', 'Germany', '1996-07-01 09:00:00'),
    ('WARTH', 'Wartian Herkku', 'Pirkko Koskitalo', 'Accounting Manager', 'pirkko.koskitalo@wartian.example', '981-443655', 'Torikatu 38', 'Oulu', '90110', 'Finland', '1996-07-01 09:00:00'),
    ('WELLI', 'Wellington Importadora', 'Paula Parente', 'Sales Manager', 'paula.parente@wellington.example', '(14) 555-8122', 'Rua do Mercado, 12', 'Resende', '08737-363', 'Brazil', '1996-07-01 09:00:00'),
    ('WHITC', 'White Clover Markets', 'Karl Jablonski', 'Owner', 'karl.jablonski@white.example', '(206) 555-4112', '305 - 14th Ave. S. Suite 3B', 'Seattle', '98128', 'USA', '1996-07-01 09:00:00'),
    ('WILMK', 'Wilman Kala', 'Matti Karttunen', 'Owner/Marketing Assistant', 'matti.karttunen@wilman.example', '90-224 8858', 'Keskuskatu 45', 'Helsinki', '21240', 'Finland', '1996-07-01 09:00:00'),
    ('WOLZA', 'Wolski  Zajazd', 'Zbyszek Piestrzeniewicz', 'Owner', 'zbyszek.piestrzeniewicz@wolski.example', '(26) 642-7012', 'ul. Filtrowa 68', 'Warszawa', '01-012', 'Poland', '1996-07-01 09:00:00');


