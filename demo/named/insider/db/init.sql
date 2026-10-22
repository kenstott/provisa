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
-- generated (generate.py writes the db/*.tsv files loaded here).
--
--   the watch list    deal, deal_team (who was brought over the wall, and when),
--                     deal_counterparty (the deal's own counterparties' mail domains)
--   the bank          employee, desk_reviewer (whose first-level review a desk's mail is)
--   its customers     customer, and customer_access: the bank's own log of who read their records
--   the mailbox       message, recipient (internal, external or personal)
--   screening         the control room's screening scope: which watch-list deals each message is
--                     screened against -- every message against every deal
--   deal_match        per message and deal it speaks of: the flagged entity (the deal's company), how
--                     strongly it names the deal, how close it reads to it, whether its sender is on
--                     the deal's team, whether it went to one of the deal's counterparties and whether
--                     it was sent before the deal was announced -- written by match_deals (setup.yaml)

CREATE ROLE provisa LOGIN PASSWORD 'provisa';
CREATE SCHEMA bank AUTHORIZATION provisa;
SET search_path = bank;

CREATE TABLE employee (employee_id text PRIMARY KEY, name text NOT NULL, email text NOT NULL,
                       desk text NOT NULL);
CREATE TABLE desk_reviewer (desk text PRIMARY KEY, reviewer text NOT NULL);
CREATE TABLE deal (deal_id text PRIMARY KEY, code_name text NOT NULL, legal_name text NOT NULL,
                   aliases text NOT NULL, ticker text NOT NULL, deal_type text NOT NULL,
                   description text NOT NULL, announced_on date);
CREATE TABLE deal_team (deal_id text NOT NULL REFERENCES deal, employee_id text NOT NULL REFERENCES employee,
                        over_wall_at timestamp NOT NULL, PRIMARY KEY (deal_id, employee_id));
CREATE TABLE deal_counterparty (deal_id text NOT NULL REFERENCES deal, domain text NOT NULL,
                                PRIMARY KEY (deal_id, domain));
CREATE TABLE customer (customer_id text PRIMARY KEY, name text NOT NULL, email text NOT NULL,
                       ssn text NOT NULL, account_no text NOT NULL);
CREATE TABLE customer_access (access_id integer PRIMARY KEY, employee_id text NOT NULL REFERENCES employee,
                              accessed_at timestamp NOT NULL, table_name text NOT NULL,
                              records_read integer NOT NULL);
CREATE TABLE message (message_id text PRIMARY KEY, sender_id text NOT NULL REFERENCES employee,
                      sent_at timestamp NOT NULL, subject text NOT NULL, body text NOT NULL,
                      outside_recipients integer NOT NULL, personal_recipients integer NOT NULL);
CREATE TABLE recipient (message_id text NOT NULL REFERENCES message, address text NOT NULL,
                        domain text NOT NULL, kind text NOT NULL CHECK (kind IN ('internal', 'external', 'personal')),
                        PRIMARY KEY (message_id, address));
CREATE TABLE screening (message_id text NOT NULL REFERENCES message, deal_id text NOT NULL REFERENCES deal,
                        PRIMARY KEY (message_id, deal_id));
CREATE TABLE deal_match (message_id text NOT NULL REFERENCES message, deal_id text NOT NULL REFERENCES deal,
                         flagged_entity text NOT NULL,
                         name_match double precision NOT NULL, meaning_match double precision NOT NULL,
                         signals text NOT NULL, sender_on_team integer NOT NULL,
                         to_counterparty integer NOT NULL, before_announcement integer NOT NULL,
                         PRIMARY KEY (message_id, deal_id));

COPY employee FROM '/docker-entrypoint-initdb.d/data/employee.tsv';
COPY desk_reviewer FROM '/docker-entrypoint-initdb.d/data/desk_reviewer.tsv';
COPY deal FROM '/docker-entrypoint-initdb.d/data/deal.tsv';
COPY deal_team FROM '/docker-entrypoint-initdb.d/data/deal_team.tsv';
COPY deal_counterparty FROM '/docker-entrypoint-initdb.d/data/deal_counterparty.tsv';
COPY customer FROM '/docker-entrypoint-initdb.d/data/customer.tsv';
COPY customer_access FROM '/docker-entrypoint-initdb.d/data/customer_access.tsv';
COPY message FROM '/docker-entrypoint-initdb.d/data/message.tsv';
COPY recipient FROM '/docker-entrypoint-initdb.d/data/recipient.tsv';
INSERT INTO screening (message_id, deal_id) SELECT m.message_id, d.deal_id FROM message AS m, deal AS d;

ALTER TABLE employee OWNER TO provisa;
ALTER TABLE desk_reviewer OWNER TO provisa;
ALTER TABLE deal OWNER TO provisa;
ALTER TABLE deal_team OWNER TO provisa;
ALTER TABLE deal_counterparty OWNER TO provisa;
ALTER TABLE customer OWNER TO provisa;
ALTER TABLE customer_access OWNER TO provisa;
ALTER TABLE message OWNER TO provisa;
ALTER TABLE recipient OWNER TO provisa;
ALTER TABLE screening OWNER TO provisa;
ALTER TABLE deal_match OWNER TO provisa;
