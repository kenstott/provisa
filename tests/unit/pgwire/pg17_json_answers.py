# Copyright (c) 2026 Kenneth Stott
# Canary: 3d9a6f70-14c8-4e2b-b7d5-08f1a2c6e93d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What PostgreSQL 17 answered: statement -> (column type OIDs, rows' text), or ("ERROR", SQLSTATE,
message). Recorded by record_pg_json_answers, not derived."""

# Requirements: REQ-2167, REQ-2171
# fmt: off

RECORDED: dict = {'SELECT \'"\\u00e9\\n\\/"\'::jsonb': ([3802], [('"é\\n/"',)]),
 'SELECT \'"a"\'::jsonb ? \'a\'': ([16], [('t',)]),
 "SELECT '1'::jsonb ? 'a'": ([16], [('f',)]),
 "SELECT '1'::jsonb @> '[1]'::jsonb": ([16], [('f',)]),
 "SELECT '1'::jsonb || '[2]'::jsonb": ([3802], [('[1, 2]',)]),
 "SELECT '1.0'::jsonb = '1'::jsonb": ([16], [('t',)]),
 'SELECT \'["a","b"]\'::jsonb - 1': ([3802], [('["a"]',)]),
 'SELECT \'["a","b"]\'::jsonb ? \'a\'': ([16], [('t',)]),
 'SELECT \'["a"]\'::jsonb @> \'"a"\'::jsonb': ([16], [('t',)]),
 "SELECT '[1,2]'::jsonb < '[1,3]'::jsonb": ([16], [('t',)]),
 "SELECT '[1,2]'::jsonb @> '1'::jsonb": ([16], [('t',)]),
 "SELECT '[1.0]'::jsonb @> '[1]'::jsonb": ([16], [('t',)]),
 'SELECT \'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\'::json #> \'{-1}\'': ([114],
                                                                                           [('true',)]),
 'SELECT \'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\'::json #> \'{3,a,0}\'': ([114],
                                                                                              [('1',)]),
 'SELECT \'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\'::json #> \'{4,1}\'': ([114],
                                                                                            [('4',)]),
 'SELECT \'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\'::json #>> \'{-1}\'': ([25],
                                                                                            [('true',)]),
 'SELECT \'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\'::json #>> \'{3,a,0}\'': ([25],
                                                                                               [('1',)]),
 'SELECT \'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\'::json #>> \'{4,1}\'': ([25],
                                                                                             [('4',)]),
 'SELECT \'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\'::json -> -1': ([114],
                                                                                     [('true',)]),
 'SELECT \'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\'::json -> 0': ([114],
                                                                                    [('1.50',)]),
 'SELECT \'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\'::json -> 3': ([114],
                                                                                    [('{"b": 2, '
                                                                                      '"a": '
                                                                                      '[1]}',)]),
 'SELECT \'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\'::json -> 9': ([114],
                                                                                    [(None,)]),
 'SELECT \'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\'::json -> i::int FROM (VALUES (1, 0), (2, 3), (3, -1), (4, 9)) t(n, i) ORDER BY n': ([114],
                                                                                                                                                          [('1.50',),
                                                                                                                                                           ('{"b": '
                                                                                                                                                            '2, '
                                                                                                                                                            '"a": '
                                                                                                                                                            '[1]}',),
                                                                                                                                                           ('true',),
                                                                                                                                                           (None,)]),
 'SELECT \'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\'::json ->> -1': ([25],
                                                                                      [('true',)]),
 'SELECT \'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\'::json ->> 0': ([25],
                                                                                     [('1.50',)]),
 'SELECT \'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\'::json ->> 3': ([25],
                                                                                     [('{"b": 2, '
                                                                                       '"a": '
                                                                                       '[1]}',)]),
 'SELECT \'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\'::json ->> 9': ([25],
                                                                                     [(None,)]),
 'SELECT \'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\'::json ->> i::int FROM (VALUES (1, 0), (2, 3), (3, -1), (4, 9)) t(n, i) ORDER BY n': ([25],
                                                                                                                                                           [('1.50',),
                                                                                                                                                            ('{"b": '
                                                                                                                                                             '2, '
                                                                                                                                                             '"a": '
                                                                                                                                                             '[1]}',),
                                                                                                                                                            ('true',),
                                                                                                                                                            (None,)]),
 'SELECT \'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\'::jsonb #> \'{-1}\'': ([3802],
                                                                                            [('true',)]),
 'SELECT \'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\'::jsonb #> \'{3,a,0}\'': ([3802],
                                                                                               [('1',)]),
 'SELECT \'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\'::jsonb #> \'{4,1}\'': ([3802],
                                                                                             [('4',)]),
 'SELECT \'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\'::jsonb #>> \'{-1}\'': ([25],
                                                                                             [('true',)]),
 'SELECT \'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\'::jsonb #>> \'{3,a,0}\'': ([25],
                                                                                                [('1',)]),
 'SELECT \'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\'::jsonb #>> \'{4,1}\'': ([25],
                                                                                              [('4',)]),
 'SELECT \'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\'::jsonb -> -1': ([3802],
                                                                                      [('true',)]),
 'SELECT \'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\'::jsonb -> 0': ([3802],
                                                                                     [('1.50',)]),
 'SELECT \'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\'::jsonb -> 3': ([3802],
                                                                                     [('{"a": [1], '
                                                                                       '"b": '
                                                                                       '2}',)]),
 'SELECT \'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\'::jsonb -> 9': ([3802],
                                                                                     [(None,)]),
 'SELECT \'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\'::jsonb -> i FROM (VALUES (1), (9)) t(i)': ([3802],
                                                                                                                 [('"x"',),
                                                                                                                  (None,)]),
 'SELECT \'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\'::jsonb -> i::int FROM (VALUES (1, 0), (2, 3), (3, -1), (4, 9)) t(n, i) ORDER BY n': ([3802],
                                                                                                                                                           [('1.50',),
                                                                                                                                                            ('{"a": '
                                                                                                                                                             '[1], '
                                                                                                                                                             '"b": '
                                                                                                                                                             '2}',),
                                                                                                                                                            ('true',),
                                                                                                                                                            (None,)]),
 'SELECT \'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\'::jsonb ->> -1': ([25],
                                                                                       [('true',)]),
 'SELECT \'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\'::jsonb ->> 0': ([25],
                                                                                      [('1.50',)]),
 'SELECT \'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\'::jsonb ->> 3': ([25],
                                                                                      [('{"a": '
                                                                                        '[1], "b": '
                                                                                        '2}',)]),
 'SELECT \'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\'::jsonb ->> 9': ([25],
                                                                                      [(None,)]),
 'SELECT \'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\'::jsonb ->> i::int FROM (VALUES (1, 0), (2, 3), (3, -1), (4, 9)) t(n, i) ORDER BY n': ([25],
                                                                                                                                                            [('1.50',),
                                                                                                                                                             ('{"a": '
                                                                                                                                                              '[1], '
                                                                                                                                                              '"b": '
                                                                                                                                                              '2}',),
                                                                                                                                                             ('true',),
                                                                                                                                                             (None,)]),
 "SELECT '[1]'::jsonb -> NULL::int": ([3802], [(None,)]),
 "SELECT '[1]'::jsonb <@ '[1,2]'::jsonb": ([16], [('t',)]),
 "SELECT '[1]'::jsonb ? '1'": ([16], [('f',)]),
 "SELECT '[1]'::jsonb || '[2]'::jsonb": ([3802], [('[1, 2]',)]),
 'SELECT \'[1]\'::jsonb || \'{"a":1}\'::jsonb': ([3802], [('[1, {"a": 1}]',)]),
 "SELECT '[1e2, 1.0E+1, 1.50, -0, 0.10]'::jsonb": ([3802], [('[100, 10, 1.50, 0, 0.10]',)]),
 "SELECT '[[1,2]]'::jsonb @> '[1]'::jsonb": ([16], [('f',)]),
 "SELECT '[]'::jsonb @> '[]'::jsonb": ([16], [('t',)]),
 'SELECT \'[{"a":1,"b":2}]\'::jsonb @> \'[{"a":1},{"b":2}]\'::jsonb': ([16], [('t',)]),
 'SELECT \'{"1": "k"}\'::jsonb #> \'{1}\'': ([3802], [('"k"',)]),
 'SELECT \'{"1":2}\'::jsonb -> i::int, \'{"1":2}\'::jsonb -> i::text FROM (VALUES (1)) t(i)': ([3802,
                                                                                                3802],
                                                                                               [(None,
                                                                                                 '2')]),
 'SELECT \'{"a":   1}\'::json': ([114], [('{"a":   1}',)]),
 'SELECT \'{"a":  {"b":   1}}\'::json -> \'a\'': ([114], [('{"b":   1}',)]),
 'SELECT \'{"a": "\\u00e9"}\'::json': ([114], [('{"a": "\\u00e9"}',)]),
 'SELECT \'{"a": {"b": 1}}\'::jsonb -> \'a\' -> \'b\'': ([3802], [('1',)]),
 'SELECT \'{"a": {"b": [1, 2]}, "c": 3}\'::json -> \'a\'': ([114], [('{"b": [1, 2]}',)]),
 'SELECT \'{"a":1,"b":2}\'::jsonb - \'a\'': ([3802], [('{"b": 2}',)]),
 'SELECT \'{"a":1,"b":2}\'::jsonb = \'{"b":2,"a":1}\'::jsonb': ([16], [('t',)]),
 'SELECT \'{"a":1,"b":2}\'::jsonb ?& ARRAY[\'a\',\'b\']': ([16], [('t',)]),
 'SELECT \'{"a":1,"b":2}\'::jsonb ?& ARRAY[\'a\',\'c\']': ([16], [('f',)]),
 'SELECT \'{"a":1,"b":2}\'::jsonb ?| ARRAY[\'b\',\'c\']': ([16], [('t',)]),
 'SELECT \'{"a":1,"b":2}\'::jsonb ?| ARRAY[\'x\']': ([16], [('f',)]),
 'SELECT \'{"a":1,"b":[1,2,{"c":3}]}\'::jsonb @> \'{"a":1}\'::jsonb': ([16], [('t',)]),
 'SELECT \'{"a":1,"b":[1,2,{"c":3}]}\'::jsonb @> \'{"b":[{"c":3},2]}\'::jsonb': ([16], [('t',)]),
 'SELECT \'{"a":1,"b":{"c":2}}\'::json -> \'b\'': ([114], [('{"c":2}',)]),
 'SELECT \'{"a":1}\'::jsonb <@ \'{"a":1,"b":2}\'::jsonb': ([16], [('t',)]),
 'SELECT \'{"a":1}\'::jsonb ? \'a\'': ([16], [('t',)]),
 'SELECT \'{"a":1}\'::jsonb ? \'b\'': ([16], [('f',)]),
 'SELECT \'{"a":1}\'::jsonb @> \'[]\'::jsonb': ([16], [('f',)]),
 'SELECT \'{"a":1}\'::jsonb @> \'{"a":2}\'::jsonb': ([16], [('f',)]),
 'SELECT \'{"a":1}\'::jsonb @? \'$.a\'': ([16], [('t',)]),
 'SELECT \'{"a":1}\'::jsonb || \'{"a":2,"b":3}\'::jsonb': ([3802], [('{"a": 2, "b": 3}',)]),
 'SELECT \'{"a":{"b":1,"c":2}}\'::jsonb #- \'{a,b}\'': ([3802], [('{"a": {"c": 2}}',)]),
 'SELECT \'{"a":{"b":1}}\'::jsonb @> \'{"a":{}}\'::jsonb': ([16], [('t',)]),
 'SELECT \'{"a.b": 1, "$c": 2, "d\\"e": 3}\'::jsonb -> \'a.b\'': ([3802], [('1',)]),
 'SELECT \'{"a.b": 1, "$c": 2}\'::jsonb -> \'$c\'': ([3802], [('2',)]),
 'SELECT \'{"b": 1, "a": 2, "b": 3}\'::jsonb': ([3802], [('{"a": 2, "b": 3}',)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::json #> \'{a,k}\'': ([114],
                                                                                                                   [('"é\\n"',)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::json #> \'{a,z,2,y}\'': ([114],
                                                                                                                       [('null',)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::json #> \'{a,z,2}\'': ([114],
                                                                                                                     [('{"y": '
                                                                                                                       'null}',)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::json #> \'{b}\'': ([114],
                                                                                                                 [('1.50',)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::json #> \'{nope,x}\'': ([114],
                                                                                                                      [(None,)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::json #> \'{}\'': ([114],
                                                                                                                [('{"b": '
                                                                                                                  '1.50, '
                                                                                                                  '"a": '
                                                                                                                  '{"z": '
                                                                                                                  '[1, '
                                                                                                                  '2, '
                                                                                                                  '{"y": '
                                                                                                                  'null}], '
                                                                                                                  '"k": '
                                                                                                                  '"é\\n"}, '
                                                                                                                  '"n": '
                                                                                                                  'null, '
                                                                                                                  '"t": '
                                                                                                                  'true}',)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::json #>> \'{a,k}\'': ([25],
                                                                                                                    [('é\n',)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::json #>> \'{a,z,2,y}\'': ([25],
                                                                                                                        [(None,)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::json #>> \'{a,z,2}\'': ([25],
                                                                                                                      [('{"y": '
                                                                                                                        'null}',)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::json #>> \'{b}\'': ([25],
                                                                                                                  [('1.50',)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::json #>> \'{nope,x}\'': ([25],
                                                                                                                       [(None,)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::json #>> \'{}\'': ([25],
                                                                                                                 [('{"b": '
                                                                                                                   '1.50, '
                                                                                                                   '"a": '
                                                                                                                   '{"z": '
                                                                                                                   '[1, '
                                                                                                                   '2, '
                                                                                                                   '{"y": '
                                                                                                                   'null}], '
                                                                                                                   '"k": '
                                                                                                                   '"é\\n"}, '
                                                                                                                   '"n": '
                                                                                                                   'null, '
                                                                                                                   '"t": '
                                                                                                                   'true}',)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::json -> \'a\'': ([114],
                                                                                                               [('{"z": '
                                                                                                                 '[1, '
                                                                                                                 '2, '
                                                                                                                 '{"y": '
                                                                                                                 'null}], '
                                                                                                                 '"k": '
                                                                                                                 '"é\\n"}',)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::json -> \'b\'': ([114],
                                                                                                               [('1.50',)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::json -> \'n\'': ([114],
                                                                                                               [('null',)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::json -> \'nope\'': ([114],
                                                                                                                  [(None,)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::json -> \'t\'': ([114],
                                                                                                               [('true',)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::json -> k::text FROM (VALUES (1, \'a\'), (2, \'b\'), (3, \'$.b\'), (4, \'nope\')) t(n, k) ORDER BY n': ([114],
                                                                                                                                                                                                      [('{"z": '
                                                                                                                                                                                                        '[1, '
                                                                                                                                                                                                        '2, '
                                                                                                                                                                                                        '{"y": '
                                                                                                                                                                                                        'null}], '
                                                                                                                                                                                                        '"k": '
                                                                                                                                                                                                        '"é\\n"}',),
                                                                                                                                                                                                       ('1.50',),
                                                                                                                                                                                                       (None,),
                                                                                                                                                                                                       (None,)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::json ->> \'a\'': ([25],
                                                                                                                [('{"z": '
                                                                                                                  '[1, '
                                                                                                                  '2, '
                                                                                                                  '{"y": '
                                                                                                                  'null}], '
                                                                                                                  '"k": '
                                                                                                                  '"é\\n"}',)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::json ->> \'b\'': ([25],
                                                                                                                [('1.50',)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::json ->> \'n\'': ([25],
                                                                                                                [(None,)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::json ->> \'nope\'': ([25],
                                                                                                                   [(None,)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::json ->> \'t\'': ([25],
                                                                                                                [('true',)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::json ->> k::text FROM (VALUES (1, \'a\'), (2, \'b\'), (3, \'$.b\'), (4, \'nope\')) t(n, k) ORDER BY n': ([25],
                                                                                                                                                                                                       [('{"z": '
                                                                                                                                                                                                         '[1, '
                                                                                                                                                                                                         '2, '
                                                                                                                                                                                                         '{"y": '
                                                                                                                                                                                                         'null}], '
                                                                                                                                                                                                         '"k": '
                                                                                                                                                                                                         '"é\\n"}',),
                                                                                                                                                                                                        ('1.50',),
                                                                                                                                                                                                        (None,),
                                                                                                                                                                                                        (None,)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::jsonb #> \'{a,k}\'': ([3802],
                                                                                                                    [('"é\\n"',)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::jsonb #> \'{a,z,2,y}\'': ([3802],
                                                                                                                        [('null',)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::jsonb #> \'{a,z,2}\'': ([3802],
                                                                                                                      [('{"y": '
                                                                                                                        'null}',)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::jsonb #> \'{b}\'': ([3802],
                                                                                                                  [('1.50',)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::jsonb #> \'{nope,x}\'': ([3802],
                                                                                                                       [(None,)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::jsonb #> \'{}\'': ([3802],
                                                                                                                 [('{"a": '
                                                                                                                   '{"k": '
                                                                                                                   '"é\\n", '
                                                                                                                   '"z": '
                                                                                                                   '[1, '
                                                                                                                   '2, '
                                                                                                                   '{"y": '
                                                                                                                   'null}]}, '
                                                                                                                   '"b": '
                                                                                                                   '1.50, '
                                                                                                                   '"n": '
                                                                                                                   'null, '
                                                                                                                   '"t": '
                                                                                                                   'true}',)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::jsonb #>> \'{a,k}\'': ([25],
                                                                                                                     [('é\n',)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::jsonb #>> \'{a,z,2,y}\'': ([25],
                                                                                                                         [(None,)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::jsonb #>> \'{a,z,2}\'': ([25],
                                                                                                                       [('{"y": '
                                                                                                                         'null}',)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::jsonb #>> \'{b}\'': ([25],
                                                                                                                   [('1.50',)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::jsonb #>> \'{nope,x}\'': ([25],
                                                                                                                        [(None,)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::jsonb #>> \'{}\'': ([25],
                                                                                                                  [('{"a": '
                                                                                                                    '{"k": '
                                                                                                                    '"é\\n", '
                                                                                                                    '"z": '
                                                                                                                    '[1, '
                                                                                                                    '2, '
                                                                                                                    '{"y": '
                                                                                                                    'null}]}, '
                                                                                                                    '"b": '
                                                                                                                    '1.50, '
                                                                                                                    '"n": '
                                                                                                                    'null, '
                                                                                                                    '"t": '
                                                                                                                    'true}',)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::jsonb -> \'a\'': ([3802],
                                                                                                                [('{"k": '
                                                                                                                  '"é\\n", '
                                                                                                                  '"z": '
                                                                                                                  '[1, '
                                                                                                                  '2, '
                                                                                                                  '{"y": '
                                                                                                                  'null}]}',)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::jsonb -> \'b\'': ([3802],
                                                                                                                [('1.50',)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::jsonb -> \'n\'': ([3802],
                                                                                                                [('null',)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::jsonb -> \'nope\'': ([3802],
                                                                                                                   [(None,)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::jsonb -> \'t\'': ([3802],
                                                                                                                [('true',)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::jsonb -> k::text -> \'z\' ->> i::int FROM (VALUES (\'a\', 1)) t(k, i)': ([25],
                                                                                                                                                                       [('2',)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::jsonb -> k::text FROM (VALUES (1, \'a\'), (2, \'b\'), (3, \'$.b\'), (4, \'nope\')) t(n, k) ORDER BY n': ([3802],
                                                                                                                                                                                                       [('{"k": '
                                                                                                                                                                                                         '"é\\n", '
                                                                                                                                                                                                         '"z": '
                                                                                                                                                                                                         '[1, '
                                                                                                                                                                                                         '2, '
                                                                                                                                                                                                         '{"y": '
                                                                                                                                                                                                         'null}]}',),
                                                                                                                                                                                                        ('1.50',),
                                                                                                                                                                                                        (None,),
                                                                                                                                                                                                        (None,)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::jsonb ->> \'a\'': ([25],
                                                                                                                 [('{"k": '
                                                                                                                   '"é\\n", '
                                                                                                                   '"z": '
                                                                                                                   '[1, '
                                                                                                                   '2, '
                                                                                                                   '{"y": '
                                                                                                                   'null}]}',)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::jsonb ->> \'b\'': ([25],
                                                                                                                 [('1.50',)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::jsonb ->> \'n\'': ([25],
                                                                                                                 [(None,)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::jsonb ->> \'nope\'': ([25],
                                                                                                                    [(None,)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::jsonb ->> \'t\'': ([25],
                                                                                                                 [('true',)]),
 'SELECT \'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\'::jsonb ->> k::text FROM (VALUES (1, \'a\'), (2, \'b\'), (3, \'$.b\'), (4, \'nope\')) t(n, k) ORDER BY n': ([25],
                                                                                                                                                                                                        [('{"k": '
                                                                                                                                                                                                          '"é\\n", '
                                                                                                                                                                                                          '"z": '
                                                                                                                                                                                                          '[1, '
                                                                                                                                                                                                          '2, '
                                                                                                                                                                                                          '{"y": '
                                                                                                                                                                                                          'null}]}',),
                                                                                                                                                                                                         ('1.50',),
                                                                                                                                                                                                         (None,),
                                                                                                                                                                                                         (None,)]),
 'SELECT \'{"bb":1,"a":2,"é":3}\'::jsonb': ([3802], [('{"a": 2, "bb": 1, "é": 3}',)]),
 'SELECT \'{"z": {"y": 1, "b": 2}}\'::jsonb ->> \'z\'': ([25], [('{"b": 2, "y": 1}',)]),
 "SELECT '{}'::jsonb @> '{}'::jsonb": ([16], [('t',)]),
 "SELECT * FROM json_array_elements('1')": ('ERROR',
                                            '22023',
                                            'cannot call json_array_elements on a scalar'),
 'SELECT * FROM json_array_elements(\'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\')': ([114],
                                                                                                     [('1.50',),
                                                                                                      ('"x"',),
                                                                                                      ('null',),
                                                                                                      ('{"b": '
                                                                                                       '2, '
                                                                                                       '"a": '
                                                                                                       '[1]}',),
                                                                                                      ('[3, '
                                                                                                       '4]',),
                                                                                                      ('1e2',),
                                                                                                      ('true',)]),
 "SELECT * FROM json_array_elements('[]')": ([114], []),
 'SELECT * FROM json_array_elements(\'{"a":1}\')': ('ERROR',
                                                    '22023',
                                                    'cannot call json_array_elements on a '
                                                    'non-array'),
 'SELECT * FROM json_array_elements(NULL)': ([114], []),
 'SELECT * FROM json_array_elements_text(\'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\')': ([25],
                                                                                                          [('1.50',),
                                                                                                           ('x',),
                                                                                                           (None,),
                                                                                                           ('{"b": '
                                                                                                            '2, '
                                                                                                            '"a": '
                                                                                                            '[1]}',),
                                                                                                           ('[3, '
                                                                                                            '4]',),
                                                                                                           ('1e2',),
                                                                                                           ('true',)]),
 "SELECT * FROM json_each('[1]')": ('ERROR', '22023', 'cannot deconstruct an array as an object'),
 'SELECT * FROM json_each(\'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\')': ([25,
                                                                                                                   114],
                                                                                                                  [('b',
                                                                                                                    '1.50'),
                                                                                                                   ('a',
                                                                                                                    '{"z": '
                                                                                                                    '[1, '
                                                                                                                    '2, '
                                                                                                                    '{"y": '
                                                                                                                    'null}], '
                                                                                                                    '"k": '
                                                                                                                    '"é\\n"}'),
                                                                                                                   ('n',
                                                                                                                    'null'),
                                                                                                                   ('t',
                                                                                                                    'true')]),
 'SELECT * FROM json_each(NULL)': ([25, 114], []),
 'SELECT * FROM json_each_text(\'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\')': ([25,
                                                                                                                        25],
                                                                                                                       [('b',
                                                                                                                         '1.50'),
                                                                                                                        ('a',
                                                                                                                         '{"z": '
                                                                                                                         '[1, '
                                                                                                                         '2, '
                                                                                                                         '{"y": '
                                                                                                                         'null}], '
                                                                                                                         '"k": '
                                                                                                                         '"é\\n"}'),
                                                                                                                        ('n',
                                                                                                                         None),
                                                                                                                        ('t',
                                                                                                                         'true')]),
 "SELECT * FROM json_object_keys('[1]')": ('ERROR',
                                           '22023',
                                           'cannot call json_object_keys on an array'),
 'SELECT * FROM json_object_keys(\'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\')': ([25],
                                                                                                                         [('b',),
                                                                                                                          ('a',),
                                                                                                                          ('n',),
                                                                                                                          ('t',)]),
 'SELECT * FROM json_to_record(\'{"a":1,"b":"x","c":[1,2],"d":{"e": 1}}\') AS t(a int, b text, c int[], d json, z text)': ([23,
                                                                                                                            25,
                                                                                                                            1007,
                                                                                                                            114,
                                                                                                                            25],
                                                                                                                           [('1',
                                                                                                                             'x',
                                                                                                                             '{1,2}',
                                                                                                                             '{"e": '
                                                                                                                             '1}',
                                                                                                                             None)]),
 "SELECT * FROM json_to_recordset('[]') AS t(v integer)": ([23], []),
 'SELECT * FROM json_to_recordset(\'[{"a":1},{"a":2,"b":"y"}]\') AS t(a int, b text)': ([23, 25],
                                                                                        [('1',
                                                                                          None),
                                                                                         ('2',
                                                                                          'y')]),
 "SELECT * FROM jsonb_array_elements('1')": ('ERROR',
                                             '22023',
                                             'cannot extract elements from a scalar'),
 'SELECT * FROM jsonb_array_elements(\'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\')': ([3802],
                                                                                                      [('1.50',),
                                                                                                       ('"x"',),
                                                                                                       ('null',),
                                                                                                       ('{"a": '
                                                                                                        '[1], '
                                                                                                        '"b": '
                                                                                                        '2}',),
                                                                                                       ('[3, '
                                                                                                        '4]',),
                                                                                                       ('100',),
                                                                                                       ('true',)]),
 "SELECT * FROM jsonb_array_elements('[]')": ([3802], []),
 'SELECT * FROM jsonb_array_elements(\'{"a":1}\')': ('ERROR',
                                                     '22023',
                                                     'cannot extract elements from an object'),
 'SELECT * FROM jsonb_array_elements(NULL)': ([3802], []),
 'SELECT * FROM jsonb_array_elements_text(\'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\')': ([25],
                                                                                                           [('1.50',),
                                                                                                            ('x',),
                                                                                                            (None,),
                                                                                                            ('{"a": '
                                                                                                             '[1], '
                                                                                                             '"b": '
                                                                                                             '2}',),
                                                                                                            ('[3, '
                                                                                                             '4]',),
                                                                                                            ('100',),
                                                                                                            ('true',)]),
 "SELECT * FROM jsonb_each('[1]')": ('ERROR', '22023', 'cannot call jsonb_each on a non-object'),
 'SELECT * FROM jsonb_each(\'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\')': ([25,
                                                                                                                    3802],
                                                                                                                   [('a',
                                                                                                                     '{"k": '
                                                                                                                     '"é\\n", '
                                                                                                                     '"z": '
                                                                                                                     '[1, '
                                                                                                                     '2, '
                                                                                                                     '{"y": '
                                                                                                                     'null}]}'),
                                                                                                                    ('b',
                                                                                                                     '1.50'),
                                                                                                                    ('n',
                                                                                                                     'null'),
                                                                                                                    ('t',
                                                                                                                     'true')]),
 'SELECT * FROM jsonb_each(NULL)': ([25, 3802], []),
 'SELECT * FROM jsonb_each_text(\'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\')': ([25,
                                                                                                                         25],
                                                                                                                        [('a',
                                                                                                                          '{"k": '
                                                                                                                          '"é\\n", '
                                                                                                                          '"z": '
                                                                                                                          '[1, '
                                                                                                                          '2, '
                                                                                                                          '{"y": '
                                                                                                                          'null}]}'),
                                                                                                                         ('b',
                                                                                                                          '1.50'),
                                                                                                                         ('n',
                                                                                                                          None),
                                                                                                                         ('t',
                                                                                                                          'true')]),
 "SELECT * FROM jsonb_object_keys('[1]')": ('ERROR',
                                            '22023',
                                            'cannot call jsonb_object_keys on an array'),
 'SELECT * FROM jsonb_object_keys(\'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\')': ([25],
                                                                                                                          [('a',),
                                                                                                                           ('b',),
                                                                                                                           ('n',),
                                                                                                                           ('t',)]),
 'SELECT * FROM jsonb_to_record(\'{"a":1,"b":"x","c":[1,2],"d":{"e": 1}}\') AS t(a int, b text, c int[], d json, z text)': ([23,
                                                                                                                             25,
                                                                                                                             1007,
                                                                                                                             114,
                                                                                                                             25],
                                                                                                                            [('1',
                                                                                                                              'x',
                                                                                                                              '{1,2}',
                                                                                                                              '{"e": '
                                                                                                                              '1}',
                                                                                                                              None)]),
 "SELECT * FROM jsonb_to_recordset('[]') AS t(v integer)": ([23], []),
 'SELECT * FROM jsonb_to_recordset(\'[{"a":1},{"a":2,"b":"y"}]\') AS t(a int, b text)': ([23, 25],
                                                                                         [('1',
                                                                                           None),
                                                                                          ('2',
                                                                                           'y')]),
 'SELECT * FROM pg_catalog."json_to_recordset"(\'[]\') AS t(v integer)': ([23], []),
 'SELECT * FROM pg_catalog."jsonb_array_elements"(\'[1]\')': ([3802], [('1',)]),
 'SELECT * FROM pg_catalog."jsonb_to_recordset"(\'[]\') AS t(v integer)': ([23], []),
 "SELECT NULL::jsonb -> 'a'": ([3802], [(None,)]),
 "SELECT NULL::jsonb ? 'a'": ([16], [(None,)]),
 "SELECT NULL::jsonb @> '[1]'::jsonb": ([16], [(None,)]),
 "SELECT count(*) FROM (VALUES ('[1,2]'::json), ('[3]'::json)) s(d), json_array_elements(s.d) e": ([20],
                                                                                                   [('3',)]),
 "SELECT count(*) FROM (VALUES ('[1,2]'::jsonb), ('[3]'::jsonb)) s(d), jsonb_array_elements(s.d) e": ([20],
                                                                                                      [('3',)]),
 'SELECT d -> \'a\' FROM (VALUES (\'{"a":1.50}\'::json)) t(d)': ([114], [('1.50',)]),
 'SELECT d -> \'a\' FROM (VALUES (\'{"a":1.50}\'::jsonb)) t(d)': ([3802], [('1.50',)]),
 'SELECT d -> \'a\' FROM (VALUES (\'{"a":null}\'::json)) t(d)': ([114], [('null',)]),
 'SELECT d -> \'a\' FROM (VALUES (\'{"a":null}\'::jsonb)) t(d)': ([3802], [('null',)]),
 'SELECT d -> \'a\' FROM (VALUES (\'{"a":{"y":1,"b":[1.50]}}\'::json)) t(d)': ([114],
                                                                               [('{"y":1,"b":[1.50]}',)]),
 'SELECT d -> \'a\' FROM (VALUES (\'{"a":{"y":1,"b":[1.50]}}\'::jsonb)) t(d)': ([3802],
                                                                                [('{"b": [1.50], '
                                                                                  '"y": 1}',)]),
 'SELECT d -> \'a\' FROM (VALUES (\'{"b":1}\'::json)) t(d)': ([114], [(None,)]),
 'SELECT d -> \'a\' FROM (VALUES (\'{"b":1}\'::jsonb)) t(d)': ([3802], [(None,)]),
 'SELECT d ? \'a\' FROM (VALUES (\'{"a":1}\'::jsonb), (\'["a"]\'::jsonb), (\'1\'::jsonb)) t(d)': ([16],
                                                                                                  [('t',),
                                                                                                   ('t',),
                                                                                                   ('f',)]),
 'SELECT d @> \'{"a":1}\' FROM (VALUES (\'{"a":1,"b":2}\'::jsonb), (\'{"a":2}\'::jsonb)) t(d)': ([16],
                                                                                                 [('t',),
                                                                                                  ('f',)]),
 'SELECT json_agg(x ORDER BY x DESC) FROM (VALUES (1), (2)) t(x)': ([114], [('[2, 1]',)]),
 'SELECT json_agg(x) FROM (VALUES (\'a\'), (\'b"c\')) t(x)': ([114], [('["a", "b\\"c"]',)]),
 'SELECT json_agg(x) FROM (VALUES (1)) t(x) WHERE false': ([114], [(None,)]),
 'SELECT json_agg(x) FROM (VALUES (1), (NULL), (2)) t(x)': ([114], [('[1, null, 2]',)]),
 'SELECT json_agg(x) FROM (VALUES (1.50)) t(x)': ([114], [('[1.50]',)]),
 'SELECT json_agg_strict(x) FROM (VALUES (1), (NULL), (2)) t(x)': ([114], [('[1, 2]',)]),
 'SELECT json_agg_strict(x) FROM (VALUES (NULL::int)) t(x)': ([114], [('[]',)]),
 'SELECT json_array_element(\'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\', -1)': ([114],
                                                                                                 [('true',)]),
 'SELECT json_array_element(\'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\', 0)': ([114],
                                                                                                [('1.50',)]),
 'SELECT json_array_element(\'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\', 3)': ([114],
                                                                                                [('{"b": '
                                                                                                  '2, '
                                                                                                  '"a": '
                                                                                                  '[1]}',)]),
 'SELECT json_array_element(\'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\', 9)': ([114],
                                                                                                [(None,)]),
 'SELECT json_array_element(\'{"a":1}\', 0)': ([114], [(None,)]),
 'SELECT json_array_element_text(\'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\', 1)': ([25],
                                                                                                     [('x',)]),
 'SELECT json_array_element_text(\'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\', 2)': ([25],
                                                                                                     [(None,)]),
 'SELECT json_array_element_text(\'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\', 3)': ([25],
                                                                                                     [('{"b": '
                                                                                                       '2, '
                                                                                                       '"a": '
                                                                                                       '[1]}',)]),
 'SELECT json_array_elements(\'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\')': ([114],
                                                                                              [('1.50',),
                                                                                               ('"x"',),
                                                                                               ('null',),
                                                                                               ('{"b": '
                                                                                                '2, '
                                                                                                '"a": '
                                                                                                '[1]}',),
                                                                                               ('[3, '
                                                                                                '4]',),
                                                                                               ('1e2',),
                                                                                               ('true',)]),
 'SELECT json_array_elements_text(\'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\')': ([25],
                                                                                                   [('1.50',),
                                                                                                    ('x',),
                                                                                                    (None,),
                                                                                                    ('{"b": '
                                                                                                     '2, '
                                                                                                     '"a": '
                                                                                                     '[1]}',),
                                                                                                    ('[3, '
                                                                                                     '4]',),
                                                                                                    ('1e2',),
                                                                                                    ('true',)]),
 "SELECT json_array_length('1')": ('ERROR', '22023', 'cannot get array length of a scalar'),
 "SELECT json_array_length('[1,[2,3],4]')": ([23], [('3',)]),
 "SELECT json_array_length('[]')": ([23], [('0',)]),
 'SELECT json_array_length(\'{"a":1}\')': ('ERROR',
                                           '22023',
                                           'cannot get array length of a non-array'),
 'SELECT json_array_length(NULL::json)': ([23], [(None,)]),
 'SELECT json_build_array()': ([114], [('[]',)]),
 "SELECT json_build_array(1, 'a', NULL, true, 1.50)": ([114], [('[1, "a", null, true, 1.50]',)]),
 'SELECT json_build_array(1, json_build_array(2, 3))': ([114], [('[1, [2, 3]]',)]),
 'SELECT json_build_array(VARIADIC ARRAY[1, 2])': ([114], [('[1, 2]',)]),
 'SELECT json_build_array(VARIADIC NULL::int[])': ([114], [(None,)]),
 "SELECT json_build_object('a')": ('ERROR',
                                   '22023',
                                   'argument list must have even number of elements'),
 "SELECT json_build_object('b', 1, 'a', 'x', 'c', NULL)": ([114],
                                                           [('{"b" : 1, "a" : "x", "c" : null}',)]),
 "SELECT json_build_object('b', 1.50, 'a', json_build_object('z', 1, 'y', 2))": ([114],
                                                                                 [('{"b" : 1.50, '
                                                                                   '"a" : {"z" : '
                                                                                   '1, "y" : '
                                                                                   '2}}',)]),
 'SELECT json_build_object()': ([114], [('{}',)]),
 'SELECT json_build_object(1, 2, true, 3)': ([114], [('{"1" : 2, "true" : 3}',)]),
 'SELECT json_build_object(NULL, 1)': ('ERROR', '22004', 'null value not allowed for object key'),
 'SELECT json_build_object(VARIADIC NULL::text[])': ([114], [(None,)]),
 'SELECT json_extract_path(\'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\', \'a\', \'nope\')': ([114],
                                                                                                                                    [(None,)]),
 'SELECT json_extract_path(\'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\', \'a\', \'z\', \'2\')': ([114],
                                                                                                                                        [('{"y": '
                                                                                                                                          'null}',)]),
 'SELECT json_extract_path(\'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\', VARIADIC ARRAY[\'a\', \'k\'])': ([114],
                                                                                                                                                 [('"é\\n"',)]),
 'SELECT json_extract_path(\'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\', VARIADIC NULL::text[])': ([114],
                                                                                                                                          [(None,)]),
 'SELECT json_extract_path_text(\'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\', \'a\', \'k\')': ([25],
                                                                                                                                      [('é\n',)]),
 'SELECT json_extract_path_text(\'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\', \'a\', \'z\')': ([25],
                                                                                                                                      [('[1, '
                                                                                                                                        '2, '
                                                                                                                                        '{"y": '
                                                                                                                                        'null}]',)]),
 'SELECT json_extract_path_text(\'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\', \'n\')': ([25],
                                                                                                                               [(None,)]),
 "SELECT json_object('{a,1,b,2}')": ([114], [('{"a" : "1", "b" : "2"}',)]),
 "SELECT json_object('{a,NULL}')": ([114], [('{"a" : null}',)]),
 "SELECT json_object('{a,b}', '{1,2}')": ([114], [('{"a" : "1", "b" : "2"}',)]),
 "SELECT json_object('{a}')": ('ERROR', '2202E', 'array must have even number of elements'),
 "SELECT json_object_agg('k', x) FROM (VALUES (1)) t(x) WHERE false": ([114], [(None,)]),
 "SELECT json_object_agg(k, x) FROM (VALUES ('b', 1), ('a', NULL)) t(k, x)": ([114],
                                                                              [('{ "b" : 1, "a" : '
                                                                                'null }',)]),
 "SELECT json_object_agg(k, x) FROM (VALUES ('b', 1), ('b', 3)) t(k, x)": ([114],
                                                                           [('{ "b" : 1, "b" : 3 '
                                                                             '}',)]),
 'SELECT json_object_agg(k, x) FROM (VALUES (NULL::text, 1)) t(k, x)': ('ERROR',
                                                                        '22004',
                                                                        'null value not allowed '
                                                                        'for object key'),
 "SELECT json_object_agg_strict(k, x) FROM (VALUES ('b', 1), ('a', NULL)) t(k, x)": ([114],
                                                                                     [('{ "b" : 1 '
                                                                                       '}',)]),
 "SELECT json_object_agg_unique(k, x) FROM (VALUES ('b', 1), ('a', 3)) t(k, x)": ([114],
                                                                                  [('{ "b" : 1, '
                                                                                    '"a" : 3 }',)]),
 "SELECT json_object_agg_unique(k, x) FROM (VALUES ('b', 1), ('b', 3)) t(k, x)": ('ERROR',
                                                                                  '22030',
                                                                                  'duplicate JSON '
                                                                                  'object key '
                                                                                  'value: "b"'),
 "SELECT json_object_agg_unique_strict(k, x) FROM (VALUES ('b', 1), ('a', NULL)) t(k, x)": ([114],
                                                                                            [('{ '
                                                                                              '"b" '
                                                                                              ': 1 '
                                                                                              '}',)]),
 "SELECT json_object_field('[1]', 'a')": ([114], [(None,)]),
 'SELECT json_object_field(\'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\', \'a\')': ([114],
                                                                                                                          [('{"z": '
                                                                                                                            '[1, '
                                                                                                                            '2, '
                                                                                                                            '{"y": '
                                                                                                                            'null}], '
                                                                                                                            '"k": '
                                                                                                                            '"é\\n"}',)]),
 'SELECT json_object_field(\'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\', \'b\')': ([114],
                                                                                                                          [('1.50',)]),
 'SELECT json_object_field(\'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\', \'nope\')': ([114],
                                                                                                                             [(None,)]),
 'SELECT json_object_field_text(\'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\', \'a\')': ([25],
                                                                                                                               [('{"z": '
                                                                                                                                 '[1, '
                                                                                                                                 '2, '
                                                                                                                                 '{"y": '
                                                                                                                                 'null}], '
                                                                                                                                 '"k": '
                                                                                                                                 '"é\\n"}',)]),
 'SELECT json_object_field_text(\'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\', \'n\')': ([25],
                                                                                                                               [(None,)]),
 'SELECT json_object_field_text(\'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\', \'t\')': ([25],
                                                                                                                               [('true',)]),
 "SELECT json_object_keys('1')": ('ERROR', '22023', 'cannot call json_object_keys on a scalar'),
 'SELECT json_object_keys(\'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\')': ([25],
                                                                                                                  [('b',),
                                                                                                                   ('a',),
                                                                                                                   ('n',),
                                                                                                                   ('t',)]),
 'SELECT json_strip_nulls(\'{"a":null,"b":{"c":null,"d":1},"e":[null,{"f":null}]}\')': ([114],
                                                                                        [('{"b":{"d":1},"e":[null,{}]}',)]),
 'SELECT json_strip_nulls(NULL::json)': ([114], [(None,)]),
 'SELECT json_to_record(\'{"a":1}\')': ('ERROR',
                                        '0A000',
                                        'could not determine row type for result of '
                                        'json_to_record'),
 'SELECT json_to_record(NULL::json)': ([2249], [(None,)]),
 "SELECT json_typeof(' [1]')": ([25], [('array',)]),
 'SELECT json_typeof(\'"a"\')': ([25], [('string',)]),
 "SELECT json_typeof('1.5')": ([25], [('number',)]),
 "SELECT json_typeof('false')": ([25], [('boolean',)]),
 "SELECT json_typeof('null')": ([25], [('null',)]),
 "SELECT json_typeof('true')": ([25], [('boolean',)]),
 'SELECT json_typeof(\'{"a":1}\')': ([25], [('object',)]),
 'SELECT json_typeof(NULL::json)': ([25], [(None,)]),
 'SELECT jsonb_agg(x ORDER BY x DESC) FROM (VALUES (1), (2)) t(x)': ([3802], [('[2, 1]',)]),
 'SELECT jsonb_agg(x) FROM (VALUES (\'a\'), (\'b"c\')) t(x)': ([3802], [('["a", "b\\"c"]',)]),
 'SELECT jsonb_agg(x) FROM (VALUES (1)) t(x) WHERE false': ([3802], [(None,)]),
 'SELECT jsonb_agg(x) FROM (VALUES (1), (NULL), (2)) t(x)': ([3802], [('[1, null, 2]',)]),
 'SELECT jsonb_agg(x) FROM (VALUES (1.50)) t(x)': ([3802], [('[1.50]',)]),
 'SELECT jsonb_agg_strict(x) FROM (VALUES (1), (NULL), (2)) t(x)': ([3802], [('[1, 2]',)]),
 'SELECT jsonb_agg_strict(x) FROM (VALUES (NULL::int)) t(x)': ([3802], [('[]',)]),
 'SELECT jsonb_array_element(\'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\', -1)': ([3802],
                                                                                                  [('true',)]),
 'SELECT jsonb_array_element(\'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\', 0)': ([3802],
                                                                                                 [('1.50',)]),
 'SELECT jsonb_array_element(\'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\', 3)': ([3802],
                                                                                                 [('{"a": '
                                                                                                   '[1], '
                                                                                                   '"b": '
                                                                                                   '2}',)]),
 'SELECT jsonb_array_element(\'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\', 9)': ([3802],
                                                                                                 [(None,)]),
 'SELECT jsonb_array_element(\'{"a":1}\', 0)': ([3802], [(None,)]),
 'SELECT jsonb_array_element_text(\'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\', 1)': ([25],
                                                                                                      [('x',)]),
 'SELECT jsonb_array_element_text(\'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\', 2)': ([25],
                                                                                                      [(None,)]),
 'SELECT jsonb_array_element_text(\'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\', 3)': ([25],
                                                                                                      [('{"a": '
                                                                                                        '[1], '
                                                                                                        '"b": '
                                                                                                        '2}',)]),
 'SELECT jsonb_array_elements(\'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\')': ([3802],
                                                                                               [('1.50',),
                                                                                                ('"x"',),
                                                                                                ('null',),
                                                                                                ('{"a": '
                                                                                                 '[1], '
                                                                                                 '"b": '
                                                                                                 '2}',),
                                                                                                ('[3, '
                                                                                                 '4]',),
                                                                                                ('100',),
                                                                                                ('true',)]),
 'SELECT jsonb_array_elements_text(\'[1.50, "x", null, {"b": 2, "a": [1]}, [3, 4], 1e2, true]\')': ([25],
                                                                                                    [('1.50',),
                                                                                                     ('x',),
                                                                                                     (None,),
                                                                                                     ('{"a": '
                                                                                                      '[1], '
                                                                                                      '"b": '
                                                                                                      '2}',),
                                                                                                     ('[3, '
                                                                                                      '4]',),
                                                                                                     ('100',),
                                                                                                     ('true',)]),
 "SELECT jsonb_array_length('1')": ('ERROR', '22023', 'cannot get array length of a scalar'),
 "SELECT jsonb_array_length('[1,[2,3],4]')": ([23], [('3',)]),
 "SELECT jsonb_array_length('[]')": ([23], [('0',)]),
 'SELECT jsonb_array_length(\'{"a":1}\')': ('ERROR',
                                            '22023',
                                            'cannot get array length of a non-array'),
 'SELECT jsonb_array_length(NULL)': ([23], [(None,)]),
 'SELECT jsonb_array_length(NULL::jsonb)': ([23], [(None,)]),
 'SELECT jsonb_build_array()': ([3802], [('[]',)]),
 "SELECT jsonb_build_array(1, 'a', NULL, true, 1.50)": ([3802], [('[1, "a", null, true, 1.50]',)]),
 'SELECT jsonb_build_array(1, jsonb_build_array(2, 3))': ([3802], [('[1, [2, 3]]',)]),
 'SELECT jsonb_build_array(VARIADIC ARRAY[1, 2])': ([3802], [('[1, 2]',)]),
 'SELECT jsonb_build_array(VARIADIC NULL::int[])': ([3802], [(None,)]),
 "SELECT jsonb_build_object('a')": ('ERROR',
                                    '22023',
                                    'argument list must have even number of elements'),
 "SELECT jsonb_build_object('b', 1, 'a', 'x', 'c', NULL)": ([3802],
                                                            [('{"a": "x", "b": 1, "c": null}',)]),
 "SELECT jsonb_build_object('b', 1.50, 'a', jsonb_build_object('z', 1, 'y', 2))": ([3802],
                                                                                   [('{"a": {"y": '
                                                                                     '2, "z": 1}, '
                                                                                     '"b": '
                                                                                     '1.50}',)]),
 'SELECT jsonb_build_object()': ([3802], [('{}',)]),
 'SELECT jsonb_build_object(1, 2, true, 3)': ([3802], [('{"1": 2, "true": 3}',)]),
 'SELECT jsonb_build_object(NULL, 1)': ('ERROR', '22023', 'argument 1: key must not be null'),
 'SELECT jsonb_build_object(VARIADIC NULL::text[])': ([3802], [(None,)]),
 "SELECT jsonb_cmp('2.5', '10')": ([23], [('-1',)]),
 "SELECT jsonb_concat('[1, 2]', '[3]')": ([3802], [('[1, 2, 3]',)]),
 "SELECT jsonb_concat('[1]', NULL::jsonb)": ([3802], [(None,)]),
 'SELECT jsonb_concat(\'{"a":1}\', \'{"a":2,"b":3}\')': ([3802], [('{"a": 2, "b": 3}',)]),
 "SELECT jsonb_concat(NULL::jsonb, '[1]')": ([3802], [(None,)]),
 'SELECT jsonb_contained(\'{"a":1}\', \'{"a":1,"b":2}\')': ([16], [('t',)]),
 'SELECT jsonb_contained(NULL::jsonb, NULL::jsonb)': ([16], [(None,)]),
 'SELECT jsonb_contains(\'{"a":1,"b":2}\', \'{"a":1}\')': ([16], [('t',)]),
 'SELECT jsonb_contains(NULL::jsonb, NULL::jsonb)': ([16], [(None,)]),
 "SELECT jsonb_delete('1', 'a')": ('ERROR', '22023', 'cannot delete from scalar'),
 'SELECT jsonb_delete(\'["a","b",1]\', \'a\')': ([3802], [('["b", 1]',)]),
 'SELECT jsonb_delete(\'["a","b",1]\', -1)': ([3802], [('["a", "b"]',)]),
 'SELECT jsonb_delete(\'["a","b",1]\', 1)': ([3802], [('["a", 1]',)]),
 'SELECT jsonb_delete(\'{"a":1,"b":2,"c":3}\', \'a\', \'c\')': ([3802], [('{"b": 2}',)]),
 'SELECT jsonb_delete(\'{"a":1,"b":2,"c":3}\', VARIADIC ARRAY[\'a\', \'c\'])': ([3802],
                                                                                [('{"b": 2}',)]),
 'SELECT jsonb_delete(\'{"a":1,"b":2}\', \'a\')': ([3802], [('{"b": 2}',)]),
 'SELECT jsonb_delete(\'{"a":1}\', 0)': ('ERROR',
                                         '22023',
                                         'cannot delete from object using integer index'),
 'SELECT jsonb_delete(\'{"a":1}\'::jsonb, NULL::text)': ([3802], [(None,)]),
 "SELECT jsonb_delete(NULL::jsonb, 'a')": ([3802], [(None,)]),
 'SELECT jsonb_delete(NULL::jsonb, 0)': ([3802], [(None,)]),
 'SELECT jsonb_delete(NULL::jsonb, VARIADIC NULL::text[])': ([3802], [(None,)]),
 "SELECT jsonb_delete_path('[1,[2,3]]', '{1,-1}')": ([3802], [('[1, [2]]',)]),
 'SELECT jsonb_delete_path(\'{"a":1}\', NULL::text[])': ([3802], [(None,)]),
 'SELECT jsonb_delete_path(\'{"a":{"b":1,"c":2}}\', \'{a,b}\')': ([3802], [('{"a": {"c": 2}}',)]),
 "SELECT jsonb_delete_path(NULL::jsonb, '{a}')": ([3802], [(None,)]),
 "SELECT jsonb_eq('1.0', '1')": ([16], [('t',)]),
 'SELECT jsonb_exists(\'{"a":1}\', \'a\')': ([16], [('t',)]),
 "SELECT jsonb_exists(NULL::jsonb, 'a')": ([16], [(None,)]),
 'SELECT jsonb_exists_all(\'{"a":1}\', \'{a,b}\')': ([16], [('f',)]),
 'SELECT jsonb_exists_all(\'{"a":1}\', NULL::text[])': ([16], [(None,)]),
 'SELECT jsonb_exists_any(\'{"a":1}\', \'{a,b}\')': ([16], [('t',)]),
 'SELECT jsonb_exists_any(NULL::jsonb, NULL::text[])': ([16], [(None,)]),
 'SELECT jsonb_extract_path(\'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\', \'a\', \'nope\')': ([3802],
                                                                                                                                     [(None,)]),
 'SELECT jsonb_extract_path(\'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\', \'a\', \'z\', \'2\')': ([3802],
                                                                                                                                         [('{"y": '
                                                                                                                                           'null}',)]),
 'SELECT jsonb_extract_path(\'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\', VARIADIC ARRAY[\'a\', \'k\'])': ([3802],
                                                                                                                                                  [('"é\\n"',)]),
 'SELECT jsonb_extract_path(\'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\', VARIADIC NULL::text[])': ([3802],
                                                                                                                                           [(None,)]),
 'SELECT jsonb_extract_path_text(\'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\', \'a\', \'k\')': ([25],
                                                                                                                                       [('é\n',)]),
 'SELECT jsonb_extract_path_text(\'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\', \'a\', \'z\')': ([25],
                                                                                                                                       [('[1, '
                                                                                                                                         '2, '
                                                                                                                                         '{"y": '
                                                                                                                                         'null}]',)]),
 'SELECT jsonb_extract_path_text(\'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\', \'n\')': ([25],
                                                                                                                                [(None,)]),
 'SELECT jsonb_insert(\'{"a":1}\', \'{a}\', \'9\')': ('ERROR',
                                                      '22023',
                                                      'cannot replace existing key'),
 'SELECT jsonb_insert(\'{"a":1}\', \'{b}\', \'9\')': ([3802], [('{"a": 1, "b": 9}',)]),
 'SELECT jsonb_insert(\'{"a":[1,2]}\', \'{a,1}\', \'9\')': ([3802], [('{"a": [1, 9, 2]}',)]),
 'SELECT jsonb_insert(\'{"a":[1,2]}\', \'{a,1}\', \'9\', true)': ([3802], [('{"a": [1, 2, 9]}',)]),
 "SELECT jsonb_insert(NULL::jsonb, '{a}', '1')": ([3802], [(None,)]),
 "SELECT jsonb_lt('1', '2')": ([16], [('t',)]),
 "SELECT jsonb_object('{a,1,b,2}')": ([3802], [('{"a": "1", "b": "2"}',)]),
 "SELECT jsonb_object('{a,NULL}')": ([3802], [('{"a": null}',)]),
 "SELECT jsonb_object('{a,b}', '{1,2}')": ([3802], [('{"a": "1", "b": "2"}',)]),
 "SELECT jsonb_object('{a}')": ('ERROR', '2202E', 'array must have even number of elements'),
 'SELECT jsonb_object(NULL::text[])': ([3802], [(None,)]),
 'SELECT jsonb_object(NULL::text[], NULL::text[])': ([3802], [(None,)]),
 "SELECT jsonb_object_agg('k', x) FROM (VALUES (1)) t(x) WHERE false": ([3802], [(None,)]),
 "SELECT jsonb_object_agg(k, x) FROM (VALUES ('b', 1), ('a', NULL)) t(k, x)": ([3802],
                                                                               [('{"a": null, "b": '
                                                                                 '1}',)]),
 "SELECT jsonb_object_agg(k, x) FROM (VALUES ('b', 1), ('b', 3)) t(k, x)": ([3802],
                                                                            [('{"b": 3}',)]),
 'SELECT jsonb_object_agg(k, x) FROM (VALUES (NULL::text, 1)) t(k, x)': ('ERROR',
                                                                         '22023',
                                                                         'field name must not be '
                                                                         'null'),
 "SELECT jsonb_object_agg_strict(k, x) FROM (VALUES ('b', 1), ('a', NULL)) t(k, x)": ([3802],
                                                                                      [('{"b": '
                                                                                        '1}',)]),
 "SELECT jsonb_object_agg_unique(k, x) FROM (VALUES ('b', 1), ('a', 3)) t(k, x)": ([3802],
                                                                                   [('{"a": 3, '
                                                                                     '"b": 1}',)]),
 "SELECT jsonb_object_agg_unique(k, x) FROM (VALUES ('b', 1), ('b', 3)) t(k, x)": ('ERROR',
                                                                                   '22030',
                                                                                   'duplicate JSON '
                                                                                   'object key '
                                                                                   'value'),
 "SELECT jsonb_object_agg_unique_strict(k, x) FROM (VALUES ('b', 1), ('a', NULL)) t(k, x)": ([3802],
                                                                                             [('{"b": '
                                                                                               '1}',)]),
 "SELECT jsonb_object_field('[1]', 'a')": ([3802], [(None,)]),
 'SELECT jsonb_object_field(\'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\', \'a\')': ([3802],
                                                                                                                           [('{"k": '
                                                                                                                             '"é\\n", '
                                                                                                                             '"z": '
                                                                                                                             '[1, '
                                                                                                                             '2, '
                                                                                                                             '{"y": '
                                                                                                                             'null}]}',)]),
 'SELECT jsonb_object_field(\'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\', \'b\')': ([3802],
                                                                                                                           [('1.50',)]),
 'SELECT jsonb_object_field(\'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\', \'nope\')': ([3802],
                                                                                                                              [(None,)]),
 'SELECT jsonb_object_field_text(\'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\', \'a\')': ([25],
                                                                                                                                [('{"k": '
                                                                                                                                  '"é\\n", '
                                                                                                                                  '"z": '
                                                                                                                                  '[1, '
                                                                                                                                  '2, '
                                                                                                                                  '{"y": '
                                                                                                                                  'null}]}',)]),
 'SELECT jsonb_object_field_text(\'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\', \'n\')': ([25],
                                                                                                                                [(None,)]),
 'SELECT jsonb_object_field_text(\'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\', \'t\')': ([25],
                                                                                                                                [('true',)]),
 "SELECT jsonb_object_keys('1')": ('ERROR', '22023', 'cannot call jsonb_object_keys on a scalar'),
 'SELECT jsonb_object_keys(\'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\')': ([25],
                                                                                                                   [('a',),
                                                                                                                    ('b',),
                                                                                                                    ('n',),
                                                                                                                    ('t',)]),
 'SELECT jsonb_path_exists(\'{"a":[1,2,3]}\', \'$.a[*] ? (@ > 2)\')': ([16], [('t',)]),
 "SELECT jsonb_path_query_first('[1,2,3]', '$[*] ? (@ > 1)')": ([3802], [('2',)]),
 'SELECT jsonb_pretty(\'{"a":[1,{"b":2}]}\')': ([25],
                                                [('{\n'
                                                  '    "a": [\n'
                                                  '        1,\n'
                                                  '        {\n'
                                                  '            "b": 2\n'
                                                  '        }\n'
                                                  '    ]\n'
                                                  '}',)]),
 "SELECT jsonb_set('1', '{a}', '9')": ('ERROR', '22023', 'cannot set path in scalar'),
 'SELECT jsonb_set(\'{"a":1}\', \'{a}\', NULL::jsonb)': ([3802], [(None,)]),
 'SELECT jsonb_set(\'{"a":1}\', \'{b}\', \'9\', false)': ([3802], [('{"a": 1}',)]),
 'SELECT jsonb_set(\'{"a":1}\', \'{b}\', \'[1, 2]\')': ([3802], [('{"a": 1, "b": [1, 2]}',)]),
 'SELECT jsonb_set(\'{"a":1}\', NULL::text[], \'1\')': ([3802], [(None,)]),
 'SELECT jsonb_set(\'{"a":[1,2]}\', \'{a,0}\', \'9\')': ([3802], [('{"a": [9, 2]}',)]),
 'SELECT jsonb_set(\'{"a":[1,2]}\', \'{a,5}\', \'9\')': ([3802], [('{"a": [1, 2, 9]}',)]),
 'SELECT jsonb_set(\'{"a":{"b":1}}\', \'{a,b}\', \'9\')': ([3802], [('{"a": {"b": 9}}',)]),
 'SELECT jsonb_set(\'{"a":{"b":1}}\', \'{a,c}\', \'9\')': ([3802], [('{"a": {"b": 1, "c": 9}}',)]),
 "SELECT jsonb_set(NULL::jsonb, '{a}', '1')": ([3802], [(None,)]),
 'SELECT jsonb_set(d, \'{a}\', \'9\') FROM (VALUES (\'{"a":1,"b":2}\'::jsonb)) t(d)': ([3802],
                                                                                       [('{"a": 9, '
                                                                                         '"b": '
                                                                                         '2}',)]),
 'SELECT jsonb_set_lax(\'{"a":1}\', \'{a}\', NULL)': ([3802], [('{"a": null}',)]),
 'SELECT jsonb_set_lax(\'{"a":1}\', \'{a}\', NULL, true, \'delete_key\')': ([3802], [('{}',)]),
 "SELECT jsonb_strip_nulls('[null,1]')": ([3802], [('[null, 1]',)]),
 'SELECT jsonb_strip_nulls(\'{"a":null,"b":{"c":null,"d":1},"e":[null,{"f":null}]}\')': ([3802],
                                                                                         [('{"b": '
                                                                                           '{"d": '
                                                                                           '1}, '
                                                                                           '"e": '
                                                                                           '[null, '
                                                                                           '{}]}',)]),
 'SELECT jsonb_strip_nulls(NULL::jsonb)': ([3802], [(None,)]),
 'SELECT jsonb_to_record(\'{"a":1}\')': ('ERROR',
                                         '0A000',
                                         'could not determine row type for result of '
                                         'jsonb_to_record'),
 'SELECT jsonb_to_record(NULL::jsonb)': ([2249], [(None,)]),
 "SELECT jsonb_typeof(' [1]')": ([25], [('array',)]),
 'SELECT jsonb_typeof(\'"a"\')': ([25], [('string',)]),
 "SELECT jsonb_typeof('1.5')": ([25], [('number',)]),
 "SELECT jsonb_typeof('false')": ([25], [('boolean',)]),
 "SELECT jsonb_typeof('null')": ([25], [('null',)]),
 "SELECT jsonb_typeof('true')": ([25], [('boolean',)]),
 'SELECT jsonb_typeof(\'{"a":1}\')': ([25], [('object',)]),
 'SELECT jsonb_typeof(NULL::jsonb)': ([25], [(None,)]),
 'SELECT k, w FROM json_each(\'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\') AS t(k, w)': ([25,
                                                                                                                                 114],
                                                                                                                                [('b',
                                                                                                                                  '1.50'),
                                                                                                                                 ('a',
                                                                                                                                  '{"z": '
                                                                                                                                  '[1, '
                                                                                                                                  '2, '
                                                                                                                                  '{"y": '
                                                                                                                                  'null}], '
                                                                                                                                  '"k": '
                                                                                                                                  '"é\\n"}'),
                                                                                                                                 ('n',
                                                                                                                                  'null'),
                                                                                                                                 ('t',
                                                                                                                                  'true')]),
 'SELECT k, w FROM jsonb_each(\'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\') AS t(k, w)': ([25,
                                                                                                                                  3802],
                                                                                                                                 [('a',
                                                                                                                                   '{"k": '
                                                                                                                                   '"é\\n", '
                                                                                                                                   '"z": '
                                                                                                                                   '[1, '
                                                                                                                                   '2, '
                                                                                                                                   '{"y": '
                                                                                                                                   'null}]}'),
                                                                                                                                  ('b',
                                                                                                                                   '1.50'),
                                                                                                                                  ('n',
                                                                                                                                   'null'),
                                                                                                                                  ('t',
                                                                                                                                   'true')]),
 'SELECT key FROM json_each(\'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\')': ([25],
                                                                                                                    [('b',),
                                                                                                                     ('a',),
                                                                                                                     ('n',),
                                                                                                                     ('t',)]),
 'SELECT key FROM jsonb_each(\'{"b": 1.50, "a": {"z": [1, 2, {"y": null}], "k": "é\\n"}, "n": null, "t": true}\')': ([25],
                                                                                                                     [('a',),
                                                                                                                      ('b',),
                                                                                                                      ('n',),
                                                                                                                      ('t',)]),
 "SELECT row_to_json(t) FROM (VALUES (1, 'a', NULL::int)) t(x, y, z)": ([114],
                                                                        [('{"x":1,"y":"a","z":null}',)]),
 "SELECT row_to_json(t, true) FROM (VALUES (1, 'a')) t(x, y)": ([114], [('{"x":1,\n "y":"a"}',)]),
 'SELECT s.i, e.key, e.value FROM (VALUES (1, \'{"a": 1.50}\'::json), (2, \'{"b": [1, 2]}\'::json)) s(i, d), json_each(s.d) e ORDER BY 1': ([23,
                                                                                                                                             25,
                                                                                                                                             114],
                                                                                                                                            [('1',
                                                                                                                                              'a',
                                                                                                                                              '1.50'),
                                                                                                                                             ('2',
                                                                                                                                              'b',
                                                                                                                                              '[1, '
                                                                                                                                              '2]')]),
 'SELECT s.i, e.key, e.value FROM (VALUES (1, \'{"a": 1.50}\'::jsonb), (2, \'{"b": [1, 2]}\'::jsonb)) s(i, d), jsonb_each(s.d) e ORDER BY 1': ([23,
                                                                                                                                                25,
                                                                                                                                                3802],
                                                                                                                                               [('1',
                                                                                                                                                 'a',
                                                                                                                                                 '1.50'),
                                                                                                                                                ('2',
                                                                                                                                                 'b',
                                                                                                                                                 '[1, '
                                                                                                                                                 '2]')]),
 'SELECT to_json(\'a"b\'::text)': ([114], [('"a\\"b"',)]),
 'SELECT to_json(1.50)': ([114], [('1.50',)]),
 'SELECT to_json(ARRAY[1, 2])': ([114], [('[1,2]',)]),
 "SELECT to_json(DATE '2024-01-02')": ([114], [('"2024-01-02"',)]),
 'SELECT to_json(NULL::int)': ([114], [(None,)]),
 "SELECT to_json(TIMESTAMP '2024-01-02 03:04:05')": ([114], [('"2024-01-02T03:04:05"',)]),
 'SELECT to_json(true)': ([114], [('true',)]),
 'SELECT to_jsonb(\'a"b\'::text)': ([3802], [('"a\\"b"',)]),
 'SELECT to_jsonb(1.50)': ([3802], [('1.50',)]),
 'SELECT to_jsonb(ARRAY[1, 2])': ([3802], [('[1, 2]',)]),
 "SELECT to_jsonb(DATE '2024-01-02')": ([3802], [('"2024-01-02"',)]),
 'SELECT to_jsonb(NULL::int)': ([3802], [(None,)]),
 "SELECT to_jsonb(TIMESTAMP '2024-01-02 03:04:05')": ([3802], [('"2024-01-02T03:04:05"',)]),
 'SELECT to_jsonb(true)': ([3802], [('true',)]),
 "SELECT value FROM json_array_elements('[1, 2]') AS t": ([114], [('1',), ('2',)]),
 "SELECT value FROM jsonb_array_elements('[1, 2]') AS t": ([3802], [('1',), ('2',)]),
 "SELECT x FROM json_array_elements('[1, 2]') AS t(x)": ([114], [('1',), ('2',)]),
 "SELECT x FROM jsonb_array_elements('[1, 2]') AS t(x)": ([3802], [('1',), ('2',)])}
