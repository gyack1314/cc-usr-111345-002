"""多版本可见性、读己写、空读登记与冻结快照测试。"""
import tempfile
import unittest
from pathlib import Path

from serlake import Lakehouse, Column, Schema
from serlake.storage import jload


class Mvcctests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix='serlake-mvcc-')
        schema = Schema('rules', [
            Column('id', 'TEXT', nullable=False),
            Column('tenant', 'TEXT', nullable=False),
            Column('lower', 'INTEGER'),
            Column('upper', 'INTEGER'),
            Column('active', 'BOOLEAN'),
        ], key_columns=('tenant', 'lower'))
        self.house = Lakehouse.create(
            Path(self.tmp) / 'house', {'rules': schema})
        t = self.house.begin('load')
        t.insert('rules', {'id': 'r1', 'tenant': 'A', 'lower': 1,
                           'upper': 2, 'active': False})
        self.house.commit(t)

    def tearDown(self):
        self.house.close()

    def test_frozen_snapshot_hides_concurrent_commit(self):
        reader = self.house.begin('long-read')
        rows = reader.range_read('rules', ('A', 0), ('A', 100))
        self.assertEqual([r['id'] for r in rows], ['r1'])

        writer = self.house.begin('concurrent-write')
        writer.insert('rules', {'id': 'r2', 'tenant': 'A', 'lower': 5,
                                'upper': 6, 'active': True})
        self.house.commit(writer)

        # 老快照仍只见 r1；新事务能见到两行。
        rows = reader.range_read('rules', ('A', 0), ('A', 100))
        self.assertEqual([r['id'] for r in rows], ['r1'])
        fresh = self.house.begin('fresh-read')
        self.assertEqual(
            [r['id'] for r in fresh.range_read('rules', ('A', 0), ('A', 100))],
            ['r1', 'r2'])

    def test_empty_read_is_registered(self):
        t = self.house.begin('empty')
        result = t.range_read('rules', ('Z', 0), ('Z', 100))
        self.assertEqual(result, [])
        self.house.abort(t)
        reads = self.house.storage.reads_of(t.tid)
        self.assertEqual(len(reads), 1)
        self.assertEqual(jload(reads[0]['observed_row_ids_json']), [])
        self.assertEqual(jload(reads[0]['observed_vids_json']), [])

    def test_read_your_own_writes(self):
        t = self.house.begin('own-writes')
        t.insert('rules', {'id': 'r9', 'tenant': 'A', 'lower': 9,
                           'upper': 10, 'active': True})
        point = t.point_read('rules', ('A', 9))
        self.assertIsNotNone(point)
        self.assertEqual(point['id'], 'r9')
        rows = t.range_read('rules', ('A', 0), ('A', 100))
        self.assertIn('r9', [r['id'] for r in rows])
        # 更新后点读新键、旧键消失。
        t.update('rules', 'r9', {'id': 'r9', 'tenant': 'A', 'lower': 90,
                                 'upper': 10, 'active': True})
        self.assertIsNone(t.point_read('rules', ('A', 9)))
        self.assertEqual(t.point_read('rules', ('A', 90))['id'], 'r9')
        # 删除后读不到。
        t.delete('rules', 'r9')
        self.assertIsNone(t.point_read('rules', ('A', 90)))
        self.house.abort(t)

    def test_range_read_excludes_own_moved_and_deleted_rows(self):
        """回归测试：更新把键移出区间、或删除后，旧快照行不得再被返回。"""
        t = self.house.begin('move')
        t.update('rules', 'r1', {'id': 'r1', 'tenant': 'A', 'lower': 50,
                                 'upper': 2, 'active': False})
        # 区间 [0,20) 不再包含 r1（键已迁到 50）。
        narrow = t.range_read('rules', ('A', 0), ('A', 20))
        self.assertEqual([r['id'] for r in narrow], [])
        # 全区间包含迁移后的 r1，且只出现一次。
        full = t.range_read('rules', ('A', 0), ('A', 100))
        self.assertEqual([r['id'] for r in full], ['r1'])
        t.delete('rules', 'r1')
        self.assertEqual(t.range_read('rules', ('A', 0), ('A', 100)), [])
        self.house.abort(t)

    def test_external_reader_sees_only_committed(self):
        t = self.house.begin('staged')
        t.insert('rules', {'id': 'rX', 'tenant': 'A', 'lower': 50,
                           'upper': 51, 'active': True})
        # 未提交：外部快照读不可见。
        self.assertIsNone(self.house.snapshot_read_point('rules', ('A', 50)))
        self.house.abort(t)

    def test_insert_duplicate_identity_rejected(self):
        t = self.house.begin('dup')
        with self.assertRaises(Exception):
            t.insert('rules', {'id': 'r1', 'tenant': 'A', 'lower': 2,
                               'upper': 3, 'active': True})
        self.house.abort(t)

    def test_unique_key_enforced_in_txn(self):
        t = self.house.begin('dup-key')
        t.insert('rules', {'id': 'r2', 'tenant': 'A', 'lower': 2,
                           'upper': 3, 'active': True})  # 新键 (A,2)
        # 另一行插入同一排序键，事务内唯一键检查立即拦截。
        with self.assertRaises(Exception):
            t.insert('rules', {'id': 'r3', 'tenant': 'A', 'lower': 2,
                               'upper': 4, 'active': True})
        self.house.abort(t)


if __name__ == '__main__':
    unittest.main()
