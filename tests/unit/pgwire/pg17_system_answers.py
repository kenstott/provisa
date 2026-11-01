# Copyright (c) 2026 Kenneth Stott
# Canary: 7c0d42e8-b16f-4e93-a572-d9c38f1e6a04
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What PostgreSQL 17 (postgres:17.11, in its default state) answered for system_corpus: statement
-> ("ok", type oid, rows) or ("err", SQLSTATE, message). Written by
record_pg17_system_answers.py, never by hand."""

# Requirements: REQ-2167, REQ-2171
# fmt: off

from __future__ import annotations

from typing import Any

RECORDED: dict[str, tuple[Any, Any, Any]] = {'SELECT pg_catalog."pg_get_wal_replay_pause_state"() AS v': ('err',
                                                              '55000',
                                                              'recovery is not in progress'),
 'SELECT pg_catalog."pg_is_wal_replay_paused"() AS v': ('err',
                                                        '55000',
                                                        'recovery is not in progress'),
 'SELECT pg_catalog."pg_wal_replay_pause"() AS v': ('err', '55000', 'recovery is not in progress'),
 'SELECT pg_catalog."pg_wal_replay_resume"() AS v': ('err', '55000', 'recovery is not in progress'),
 'SELECT pg_catalog."pg_last_committed_xact"() AS v': ('err',
                                                       '55000',
                                                       'could not get commit timestamp data'),
 'SELECT pg_catalog."pg_ls_logdir"() AS v': ('err',
                                             '58P01',
                                             'could not open directory "log": No such file or '
                                             'directory'),
 'SELECT pg_catalog."pg_replication_origin_session_reset"() AS v': ('err',
                                                                    '55000',
                                                                    'no replication origin is '
                                                                    'configured'),
 'SELECT pg_catalog."pg_sync_replication_slots"() AS v': ('err',
                                                          '55000',
                                                          'replication slots can only be '
                                                          'synchronized to a standby server'),
 'SELECT pg_get_wal_replay_pause_state() AS v': ('err', '55000', 'recovery is not in progress'),
 'SELECT pg_is_wal_replay_paused() AS v': ('err', '55000', 'recovery is not in progress'),
 'SELECT pg_wal_replay_pause() AS v': ('err', '55000', 'recovery is not in progress'),
 'SELECT pg_wal_replay_resume() AS v': ('err', '55000', 'recovery is not in progress'),
 'SELECT pg_last_committed_xact() AS v': ('err', '55000', 'could not get commit timestamp data'),
 'SELECT pg_ls_logdir() AS v': ('err',
                                '58P01',
                                'could not open directory "log": No such file or directory'),
 'SELECT pg_replication_origin_session_reset() AS v': ('err',
                                                       '55000',
                                                       'no replication origin is configured'),
 'SELECT pg_sync_replication_slots() AS v': ('err',
                                             '55000',
                                             'replication slots can only be synchronized to a '
                                             'standby server'),
 'SELECT pg_catalog."pg_logical_slot_get_changes"(NULL::name, NULL::pg_lsn, NULL::integer, VARIADIC NULL::text[]) AS v': ('err',
                                                                                                                          '55000',
                                                                                                                          'logical '
                                                                                                                          'decoding '
                                                                                                                          'requires '
                                                                                                                          '"wal_level" '
                                                                                                                          '>= '
                                                                                                                          '"logical"'),
 'SELECT pg_catalog."pg_logical_slot_get_changes"(\'s\'::name, NULL::pg_lsn, NULL::integer) AS v': ('err',
                                                                                                    '55000',
                                                                                                    'logical '
                                                                                                    'decoding '
                                                                                                    'requires '
                                                                                                    '"wal_level" '
                                                                                                    '>= '
                                                                                                    '"logical"'),
 'SELECT pg_catalog."pg_logical_slot_get_changes"(\'s\'::name, NULL::pg_lsn, 1::integer, \'k\'::text, \'v\'::text) AS v': ('err',
                                                                                                                           '55000',
                                                                                                                           'logical '
                                                                                                                           'decoding '
                                                                                                                           'requires '
                                                                                                                           '"wal_level" '
                                                                                                                           '>= '
                                                                                                                           '"logical"'),
 'SELECT pg_catalog."pg_logical_slot_peek_changes"(NULL::name, NULL::pg_lsn, NULL::integer, VARIADIC NULL::text[]) AS v': ('err',
                                                                                                                           '55000',
                                                                                                                           'logical '
                                                                                                                           'decoding '
                                                                                                                           'requires '
                                                                                                                           '"wal_level" '
                                                                                                                           '>= '
                                                                                                                           '"logical"'),
 'SELECT pg_catalog."pg_logical_slot_peek_changes"(\'s\'::name, NULL::pg_lsn, NULL::integer) AS v': ('err',
                                                                                                     '55000',
                                                                                                     'logical '
                                                                                                     'decoding '
                                                                                                     'requires '
                                                                                                     '"wal_level" '
                                                                                                     '>= '
                                                                                                     '"logical"'),
 'SELECT pg_catalog."pg_logical_slot_peek_changes"(\'s\'::name, NULL::pg_lsn, 1::integer, \'k\'::text, \'v\'::text) AS v': ('err',
                                                                                                                            '55000',
                                                                                                                            'logical '
                                                                                                                            'decoding '
                                                                                                                            'requires '
                                                                                                                            '"wal_level" '
                                                                                                                            '>= '
                                                                                                                            '"logical"'),
 'SELECT pg_catalog."pg_logical_slot_get_binary_changes"(NULL::name, NULL::pg_lsn, NULL::integer, VARIADIC NULL::text[]) AS v': ('err',
                                                                                                                                 '55000',
                                                                                                                                 'logical '
                                                                                                                                 'decoding '
                                                                                                                                 'requires '
                                                                                                                                 '"wal_level" '
                                                                                                                                 '>= '
                                                                                                                                 '"logical"'),
 'SELECT pg_catalog."pg_logical_slot_get_binary_changes"(\'s\'::name, NULL::pg_lsn, NULL::integer) AS v': ('err',
                                                                                                           '55000',
                                                                                                           'logical '
                                                                                                           'decoding '
                                                                                                           'requires '
                                                                                                           '"wal_level" '
                                                                                                           '>= '
                                                                                                           '"logical"'),
 'SELECT pg_catalog."pg_logical_slot_get_binary_changes"(\'s\'::name, NULL::pg_lsn, 1::integer, \'k\'::text, \'v\'::text) AS v': ('err',
                                                                                                                                  '55000',
                                                                                                                                  'logical '
                                                                                                                                  'decoding '
                                                                                                                                  'requires '
                                                                                                                                  '"wal_level" '
                                                                                                                                  '>= '
                                                                                                                                  '"logical"'),
 'SELECT pg_catalog."pg_logical_slot_peek_binary_changes"(NULL::name, NULL::pg_lsn, NULL::integer, VARIADIC NULL::text[]) AS v': ('err',
                                                                                                                                  '55000',
                                                                                                                                  'logical '
                                                                                                                                  'decoding '
                                                                                                                                  'requires '
                                                                                                                                  '"wal_level" '
                                                                                                                                  '>= '
                                                                                                                                  '"logical"'),
 'SELECT pg_catalog."pg_logical_slot_peek_binary_changes"(\'s\'::name, NULL::pg_lsn, NULL::integer) AS v': ('err',
                                                                                                            '55000',
                                                                                                            'logical '
                                                                                                            'decoding '
                                                                                                            'requires '
                                                                                                            '"wal_level" '
                                                                                                            '>= '
                                                                                                            '"logical"'),
 'SELECT pg_catalog."pg_logical_slot_peek_binary_changes"(\'s\'::name, NULL::pg_lsn, 1::integer, \'k\'::text, \'v\'::text) AS v': ('err',
                                                                                                                                   '55000',
                                                                                                                                   'logical '
                                                                                                                                   'decoding '
                                                                                                                                   'requires '
                                                                                                                                   '"wal_level" '
                                                                                                                                   '>= '
                                                                                                                                   '"logical"'),
 'SELECT pg_catalog."pg_notify"(\'conformance\'::text, NULL::text) AS v': ('ok', 2278, ['']),
 'SELECT pg_catalog."pg_notify"(\'c\'::text, \'p\'::text) AS v': ('ok', 2278, ['']),
 'SELECT pg_catalog."pg_notify"(NULL::text, \'p\'::text) AS v': ('err',
                                                                 '22023',
                                                                 'channel name cannot be empty'),
 'SELECT pg_catalog."pg_notify"(\'\'::text, \'p\'::text) AS v': ('err',
                                                                 '22023',
                                                                 'channel name cannot be empty'),
 'SELECT pg_catalog."pg_notify"(repeat(\'x\', 63), \'p\'::text) AS v': ('ok', 2278, ['']),
 'SELECT pg_catalog."pg_notify"(repeat(\'x\', 64), \'p\'::text) AS v': ('err',
                                                                        '22023',
                                                                        'channel name too long'),
 'SELECT pg_catalog."pg_notify"(\'c\'::text, repeat(\'x\', 7999)) AS v': ('ok', 2278, ['']),
 'SELECT pg_catalog."pg_notify"(\'c\'::text, repeat(\'x\', 8000)) AS v': ('err',
                                                                          '22023',
                                                                          'payload string too '
                                                                          'long'),
 'SELECT pg_catalog."pg_collation_for"(NULL::text) AS v': ('ok', 25, ['"default"']),
 'SELECT pg_catalog."pg_collation_for"(\'a\'::text) AS v': ('ok', 25, ['"default"']),
 'SELECT pg_catalog."pg_collation_for"(\'a\'::varchar) AS v': ('ok', 25, ['"default"']),
 'SELECT pg_catalog."pg_collation_for"(\'a\'::name) AS v': ('ok', 25, ['"C"']),
 'SELECT pg_catalog."pg_collation_for"(\'a\') AS v': ('ok', 25, [None]),
 'SELECT pg_catalog."pg_collation_for"(NULL::integer) AS v': ('err',
                                                              '42804',
                                                              'collations are not supported by '
                                                              'type integer'),
 'SELECT pg_catalog."pg_collation_for"(1::integer) AS v': ('err',
                                                           '42804',
                                                           'collations are not supported by type '
                                                           'integer'),
 'SELECT pg_catalog."pg_collation_for"(1.5::numeric) AS v': ('err',
                                                             '42804',
                                                             'collations are not supported by type '
                                                             'numeric'),
 'SELECT pg_catalog."pg_collation_for"(true::boolean) AS v': ('err',
                                                              '42804',
                                                              'collations are not supported by '
                                                              'type boolean'),
 'SELECT pg_catalog."pg_column_compression"(NULL::integer) AS v': ('ok', 25, [None]),
 'SELECT pg_catalog."pg_column_compression"(1::integer) AS v': ('ok', 25, [None]),
 'SELECT pg_catalog."pg_column_compression"(\'a\'::text) AS v': ('ok', 25, [None]),
 'SELECT pg_catalog."pg_column_compression"(repeat(\'x\', 10000)) AS v': ('ok', 25, [None]),
 'SELECT pg_catalog."pg_column_toast_chunk_id"(NULL::integer) AS v': ('ok', 26, [None]),
 'SELECT pg_catalog."pg_column_toast_chunk_id"(1::integer) AS v': ('ok', 26, [None]),
 'SELECT pg_catalog."pg_column_toast_chunk_id"(\'a\'::text) AS v': ('ok', 26, [None]),
 'SELECT pg_catalog."pg_column_toast_chunk_id"(repeat(\'x\', 10000)) AS v': ('ok', 26, [None]),
 'SELECT pg_catalog."regexp_replace"(\'abc\'::text, \'b\'::text, \'abc\'::text, 3::integer, 3::integer) AS v': ('ok',
                                                                                                                25,
                                                                                                                ['abc']),
 'SELECT pg_catalog."regexp_replace"(NULL::text, NULL::text, NULL::text, NULL::integer, NULL::integer) AS v': ('ok',
                                                                                                               25,
                                                                                                               [None]),
 'SELECT pg_catalog."regexp_replace"(NULL::text, \'b\'::text, \'x\'::text, 1::integer, 1::integer) AS v': ('ok',
                                                                                                           25,
                                                                                                           [None]),
 'SELECT pg_catalog."regexp_replace"(\'abc\'::text, \'b\'::text, \'x\'::text, NULL::integer, 1::integer) AS v': ('ok',
                                                                                                                 25,
                                                                                                                 [None]),
 'SELECT pg_catalog."regexp_replace"(\'abcabc\'::text, \'b\'::text, \'X\'::text, 1::integer, 1::integer) AS v': ('ok',
                                                                                                                 25,
                                                                                                                 ['aXcabc']),
 'SELECT pg_catalog."regexp_replace"(\'abcabc\'::text, \'b\'::text, \'X\'::text, 1::integer, 2::integer) AS v': ('ok',
                                                                                                                 25,
                                                                                                                 ['abcaXc']),
 'SELECT pg_catalog."regexp_replace"(\'abcabc\'::text, \'b\'::text, \'X\'::text, 1::integer, 3::integer) AS v': ('ok',
                                                                                                                 25,
                                                                                                                 ['abcabc']),
 'SELECT pg_catalog."regexp_replace"(\'abcabc\'::text, \'b\'::text, \'X\'::text, 1::integer, 0::integer) AS v': ('ok',
                                                                                                                 25,
                                                                                                                 ['aXcaXc']),
 'SELECT pg_catalog."regexp_replace"(\'abcabc\'::text, \'b\'::text, \'X\'::text, 3::integer, 1::integer) AS v': ('ok',
                                                                                                                 25,
                                                                                                                 ['abcaXc']),
 'SELECT pg_catalog."regexp_replace"(\'abcabc\'::text, \'b\'::text, \'X\'::text, 3::integer, 0::integer) AS v': ('ok',
                                                                                                                 25,
                                                                                                                 ['abcaXc']),
 'SELECT pg_catalog."regexp_replace"(\'abcabc\'::text, \'b\'::text, \'X\'::text, 7::integer, 1::integer) AS v': ('ok',
                                                                                                                 25,
                                                                                                                 ['abcabc']),
 'SELECT pg_catalog."regexp_replace"(\'abcabc\'::text, \'(b)(c)\'::text, \'\\2\\1\'::text, 1::integer, 2::integer) AS v': ('ok',
                                                                                                                           25,
                                                                                                                           ['abcacb']),
 'SELECT pg_catalog."regexp_replace"(\'abcabc\'::text, \'(b)(c)\'::text, \'$\\2\'::text, 1::integer, 2::integer) AS v': ('ok',
                                                                                                                         25,
                                                                                                                         ['abca$c']),
 'SELECT pg_catalog."regexp_replace"(\'a雪b雪c\'::text, \'雪\'::text, \'X\'::text, 2::integer, 2::integer) AS v': ('ok',
                                                                                                                25,
                                                                                                                ['a雪bXc']),
 'SELECT pg_catalog."regexp_replace"(\'abc\'::text, \'b\'::text, \'x\'::text, 0::integer, 1::integer) AS v': ('err',
                                                                                                              '22023',
                                                                                                              'invalid '
                                                                                                              'value '
                                                                                                              'for '
                                                                                                              'parameter '
                                                                                                              '"start": '
                                                                                                              '0'),
 'SELECT pg_catalog."regexp_replace"(\'abc\'::text, \'b\'::text, \'x\'::text, 1::integer, -1::integer) AS v': ('err',
                                                                                                               '22023',
                                                                                                               'invalid '
                                                                                                               'value '
                                                                                                               'for '
                                                                                                               'parameter '
                                                                                                               '"n": '
                                                                                                               '-1')}
