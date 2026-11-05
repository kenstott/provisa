-- Copyright (c) 2026 Kenneth Stott
--
-- This source code is licensed under the Business Source License 1.1
-- found in the LICENSE file in the root directory of this source tree.
--
-- NOTICE: Use of this software for training artificial intelligence or
-- machine learning models is strictly prohibited without explicit written
-- permission from the copyright holder.
--
-- The insider demo's database (REQ-2056): Northgate Partners, a fictional bank. Everything is
-- generated (generate.py writes the db/*.tsv files loaded here). The bank's mail is in its
-- Microsoft 365 tenant and the watch list in the control room's workbook; this is the bank's own:
--
--   the bank          employee, desk_reviewer (whose first-level review a desk's mail is),
--                     personal_mail_domain (the personal mail providers compliance lists)
--   its customers     customer, and customer_access: the bank's own log of who read their records
--   deal_match        per message and deal it speaks of: the flagged entity (the deal's company), how
--                     strongly it names the deal, how close it reads to it, whether its sender is on
--                     the deal's team, whether it went to one of the deal's counterparties and whether
--                     it was sent before the announcement; written by match_deals (matching.py)
--                     from the mail and the watch list, so it names their keys without a reference
CREATE ROLE provisa LOGIN PASSWORD 'provisa';
CREATE SCHEMA bank AUTHORIZATION provisa;
SET search_path = bank;

CREATE TABLE employee (employee_id text PRIMARY KEY, name text NOT NULL, email text NOT NULL UNIQUE,
                       desk text NOT NULL);
CREATE TABLE desk_reviewer (desk text PRIMARY KEY, reviewer text NOT NULL);
CREATE TABLE personal_mail_domain (domain text PRIMARY KEY);
CREATE TABLE customer (customer_id text PRIMARY KEY, name text NOT NULL, email text NOT NULL,
                       ssn text NOT NULL, account_no text NOT NULL);
CREATE TABLE customer_access (access_id integer PRIMARY KEY, employee_id text NOT NULL REFERENCES employee,
                              accessed_at timestamp NOT NULL, table_name text NOT NULL,
                              records_read integer NOT NULL);
CREATE TABLE deal_match (message_id text NOT NULL, deal_id text NOT NULL,
                         flagged_entity text NOT NULL,
                         name_match double precision NOT NULL, meaning_match double precision NOT NULL,
                         signals text NOT NULL, sender_on_team integer NOT NULL,
                         to_counterparty integer NOT NULL, before_announcement integer NOT NULL,
                         PRIMARY KEY (message_id, deal_id));

COPY employee FROM '/docker-entrypoint-initdb.d/data/employee.tsv';
COPY desk_reviewer FROM '/docker-entrypoint-initdb.d/data/desk_reviewer.tsv';
COPY personal_mail_domain FROM '/docker-entrypoint-initdb.d/data/personal_mail_domain.tsv';
COPY customer FROM '/docker-entrypoint-initdb.d/data/customer.tsv';
COPY customer_access FROM '/docker-entrypoint-initdb.d/data/customer_access.tsv';

ALTER TABLE employee OWNER TO provisa;
ALTER TABLE desk_reviewer OWNER TO provisa;
ALTER TABLE personal_mail_domain OWNER TO provisa;
ALTER TABLE customer OWNER TO provisa;
ALTER TABLE customer_access OWNER TO provisa;
ALTER TABLE deal_match OWNER TO provisa;
