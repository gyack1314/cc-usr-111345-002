"""认证器测试：写偏斜、幻行、三事务环、不相交放行、首提者胜。"""
import tempfile
import unittest
from pathlib import Path

from serlake import Lakehouse, Column, Schema
from serlake.errors import (
    SerializationCycle, WriteWriteConflict, UniquenessViolation)
from serlake.checker import HistoryChecker


def build_house(tmp):
    schema = Schema('rules', [
        Column('id', 'TEXT', nullable=False),
        Column('tenant', 'TEXT', nullable=False),
        Column('lower', 'INTEGER'),
        Column('upper', 'INTEGER'),
        Column('active', 'BOOLEAN'),
    ], key_columns=('tenant', 'lower'))
    return Lakehouse.create(Path(tmp) / 'house', {'rules': schema}), schema


class CertificationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix='serlake-cert-')
        self.house, _ = build_house(self.tmp)

    def tearDown(self):
        self.house.close()

    def test_write_skew_rejected(self):
        t1 = self.house.begin('ws-1')
        t1.range_read('rules', ('A', 0), ('A', 100))
        t1.insert('rules', {'id': 'a1', 'tenant': 'A', 'lower': 1,
                            'upper': 2, 'active': True})
        t2 = self.house.begin('ws-2')
        t2.range_read('rules', ('A', 0), ('A', 100))
        t2.insert('rules', {'id': 'a2', 'tenant': 'A', 'lower': 2,
                            'upper': 3, 'active': True})
        self.house.commit(t1)
        with self.assertRaises(SerializationCycle) as ctx:
            self.house.commit(t2)
        # 环见证必须是 rw 边且携带区间谓词。
        self.assertTrue(all(e['edge_type'] == 'rw'
                            for e in ctx.exception.chain_edges))
        self.assertTrue(any(e['witness']['predicate']['kind'] == 'range'
                            for e in ctx.exception.chain_edges))
        verdict = HistoryChecker(self.house).verify()
        self.assertFalse(verdict.failures)

    def test_phantom_empty_range_cycle(self):
        t1 = self.house.begin('ph-1')
        t1.range_read('rules', ('C', 0), ('C', 100))  # 空读
        t1.insert('rules', {'id': 'c1', 'tenant': 'C', 'lower': 1,
                            'upper': 2, 'active': True})
        t2 = self.house.begin('ph-2')
        t2.range_read('rules', ('C', 0), ('C', 100))  # 同样空读
        t2.insert('rules', {'id': 'c2', 'tenant': 'C', 'lower': 2,
                            'upper': 3, 'active': True})
        self.house.commit(t1)
        with self.assertRaises(SerializationCycle) as ctx:
            self.house.commit(t2)
        self.assertTrue(all(e['witness']['empty_read']
                            for e in ctx.exception.chain_edges))

    def test_three_transaction_cycle(self):
        load = self.house.begin('load')
        for rid, tenant in (('x1', 'X'), ('x2', 'Y'), ('x3', 'Z')):
            load.insert('rules', {'id': rid, 'tenant': tenant, 'lower': 0,
                                  'upper': 1, 'active': False})
        self.house.commit(load)

        t1 = self.house.begin('c1')
        t1.point_read('rules', ('X', 0))
        t1.update('rules', 'x3', {'id': 'x3', 'tenant': 'Z', 'lower': 0,
                                  'upper': 1, 'active': True})
        t2 = self.house.begin('c2')
        t2.point_read('rules', ('Y', 0))
        t2.update('rules', 'x1', {'id': 'x1', 'tenant': 'X', 'lower': 0,
                                  'upper': 1, 'active': True})
        t3 = self.house.begin('c3')
        t3.point_read('rules', ('Z', 0))
        t3.update('rules', 'x2', {'id': 'x2', 'tenant': 'Y', 'lower': 0,
                                  'upper': 1, 'active': True})
        self.house.commit(t1)
        self.house.commit(t2)
        with self.assertRaises(SerializationCycle) as ctx:
            self.house.commit(t3)
        # 环上有三个事务、三条 rw 边、点键见证。
        self.assertEqual(len(ctx.exception.chain), 4)  # 首尾重复
        self.assertTrue(all(e['edge_type'] == 'rw'
                            for e in ctx.exception.chain_edges))
        self.assertTrue(all(e['witness']['predicate']['kind'] == 'point'
                            for e in ctx.exception.chain_edges))

    def test_disjoint_commits_have_no_edges(self):
        t1 = self.house.begin('d1')
        t1.range_read('rules', ('E', 0), ('E', 100))
        t1.insert('rules', {'id': 'e1', 'tenant': 'E', 'lower': 1,
                            'upper': 2, 'active': True})
        t2 = self.house.begin('d2')
        t2.range_read('rules', ('F', 0), ('F', 100))
        t2.insert('rules', {'id': 'f1', 'tenant': 'F', 'lower': 1,
                            'upper': 2, 'active': True})
        self.house.commit(t1)
        receipt = self.house.commit(t2)
        self.assertEqual(receipt.edges_added,
                         {'ww': 0, 'wr': 0, 'rw': 0})

    def test_first_committer_wins(self):
        load = self.house.begin('load')
        load.insert('rules', {'id': 'g1', 'tenant': 'G', 'lower': 1,
                              'upper': 2, 'active': False})
        self.house.commit(load)
        t1 = self.house.begin('w1')
        t1.point_read('rules', ('G', 1))
        t1.update('rules', 'g1', {'id': 'g1', 'tenant': 'G', 'lower': 1,
                                  'upper': 2, 'active': True})
        t2 = self.house.begin('w2')
        t2.point_read('rules', ('G', 1))
        t2.update('rules', 'g1', {'id': 'g1', 'tenant': 'G', 'lower': 1,
                                  'upper': 9, 'active': False})
        self.house.commit(t1)
        with self.assertRaises(WriteWriteConflict):
            self.house.commit(t2)

    def test_unique_key_violation_on_concurrent_insert(self):
        t1 = self.house.begin('u1')
        t1.insert('rules', {'id': 'g2', 'tenant': 'G', 'lower': 5,
                            'upper': 6, 'active': True})
        t2 = self.house.begin('u2')
        t2.insert('rules', {'id': 'g3', 'tenant': 'G', 'lower': 5,
                            'upper': 7, 'active': False})
        self.house.commit(t1)
        with self.assertRaises(UniquenessViolation):
            self.house.commit(t2)

    def test_key_migration_both_sides_considered(self):
        load = self.house.begin('load')
        load.insert('rules', {'id': 'd1', 'tenant': 'D', 'lower': 10,
                              'upper': 20, 'active': False})
        self.house.commit(load)
        t3 = self.house.begin('km-1')
        t3.range_read('rules', ('D', 0), ('D', 30))
        t3.insert('rules', {'id': 'd4', 'tenant': 'D', 'lower': 85,
                            'upper': 88, 'active': True})
        t4 = self.house.begin('km-2')
        t4.range_read('rules', ('D', 70), ('D', 90))
        t4.update('rules', 'd1', {'id': 'd1', 'tenant': 'D', 'lower': 80,
                                  'upper': 90, 'active': True})
        self.house.commit(t4)
        with self.assertRaises(SerializationCycle) as ctx:
            self.house.commit(t3)
        details = ' '.join(e['witness']['detail']
                           for e in ctx.exception.chain_edges)
        self.assertIn('更新前键命中', details)
        self.assertIn('更新后/插入键命中', details)


if __name__ == '__main__':
    unittest.main()


class InFlightDependencyTests(unittest.TestCase):
    """两个事务都在活跃、按相反顺序提交：仅看"快照后已提交"会漏边。"""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix='serlake-inflight-')
        self.house, _ = build_house(self.tmp)
        load = self.house.begin('load')
        load.insert('rules', {'id': 'x1', 'tenant': 'X', 'lower': 1,
                              'upper': 2, 'active': False})
        load.insert('rules', {'id': 'y1', 'tenant': 'Y', 'lower': 1,
                              'upper': 2, 'active': False})
        self.house.commit(load)

    def tearDown(self):
        self.house.close()

    def test_reverse_commit_order_skew_detected(self):
        # T1 读 X 写 Y；T2 读 Y 写 X。两者都在活跃时交错提交。
        t1 = self.house.begin('if-1')
        t1.point_read('rules', ('X', 1))
        t1.update('rules', 'y1', {'id': 'y1', 'tenant': 'Y', 'lower': 1,
                                  'upper': 2, 'active': True})
        t2 = self.house.begin('if-2')
        t2.point_read('rules', ('Y', 1))
        t2.update('rules', 'x1', {'id': 'x1', 'tenant': 'X', 'lower': 1,
                                  'upper': 2, 'active': True})
        # 重要：T1 先提交（它的快照 seq 更小），T2 后提交。
        self.house.commit(t1)
        with self.assertRaises(Exception):
            # 必须被拒绝：T1 读了 X 而 T2 正在写 X，反向亦然。
            self.house.commit(t2)


class UniqueKeyLivenessTests(unittest.TestCase):
    """唯一键复核只看每行最终活版本，历史/墓碑版本不得占用键。"""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix='serlake-uniq-')
        self.house, _ = build_house(self.tmp)
        load = self.house.begin('load')
        load.insert('rules', {'id': 'a', 'tenant': 'T', 'lower': 1,
                              'upper': 2, 'active': False})
        self.house.commit(load)

    def tearDown(self):
        self.house.close()

    def test_key_freed_by_migration_is_reusable(self):
        move = self.house.begin('move')
        move.update('rules', 'a', {'id': 'a', 'tenant': 'T', 'lower': 50,
                                   'upper': 2, 'active': False})
        self.house.commit(move)
        insert = self.house.begin('reuse')
        insert.insert('rules', {'id': 'b', 'tenant': 'T', 'lower': 1,
                                'upper': 3, 'active': True})
        self.house.commit(insert)  # 旧版本 (T,1) 不再占用
        self.assertEqual(
            sorted((r['id'], r['lower'])
                   for r in self.house.snapshot_scan('rules')),
            [('a', 50), ('b', 1)])

    def test_key_freed_by_delete_is_reusable(self):
        delete = self.house.begin('del')
        delete.delete('rules', 'a')
        self.house.commit(delete)
        insert = self.house.begin('reins')
        insert.insert('rules', {'id': 'c', 'tenant': 'T', 'lower': 1,
                                'upper': 9, 'active': True})
        self.house.commit(insert)  # 墓碑不占用键
        self.assertEqual([r['id'] for r in self.house.snapshot_scan('rules')],
                         ['c'])

    def test_real_duplicate_still_rejected(self):
        u1 = self.house.begin('d1')
        u1.insert('rules', {'id': 'd', 'tenant': 'T', 'lower': 7,
                            'upper': 8, 'active': True})
        u2 = self.house.begin('d2')
        u2.insert('rules', {'id': 'e', 'tenant': 'T', 'lower': 7,
                            'upper': 9, 'active': True})
        self.house.commit(u1)
        with self.assertRaises(UniquenessViolation):
            self.house.commit(u2)


class KeySwapTests(unittest.TestCase):
    """同事务内键交换：其它行被本事务改写后，旧版本不得阻塞交换。"""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix='serlake-swap-')
        self.house, _ = build_house(self.tmp)
        load = self.house.begin('load')
        load.insert('rules', {'id': 'a', 'tenant': 'T', 'lower': 1,
                              'upper': 2, 'active': False})
        load.insert('rules', {'id': 'b', 'tenant': 'T', 'lower': 5,
                              'upper': 2, 'active': False})
        self.house.commit(load)

    def tearDown(self):
        self.house.close()

    def test_key_swap_within_transaction_commits(self):
        swap = self.house.begin('swap')
        swap.update('rules', 'a', {'id': 'a', 'tenant': 'T', 'lower': 100,
                                   'upper': 3, 'active': False})
        swap.update('rules', 'b', {'id': 'b', 'tenant': 'T', 'lower': 1,
                                   'upper': 2, 'active': False})
        swap.update('rules', 'a', {'id': 'a', 'tenant': 'T', 'lower': 5,
                                   'upper': 3, 'active': False})
        self.house.commit(swap)
        self.assertEqual(
            sorted((r['id'], r['lower'])
                   for r in self.house.snapshot_scan('rules')),
            [('a', 5), ('b', 1)])

    def test_intra_transaction_duplicate_key_rejected(self):
        dup = self.house.begin('dup')
        # 先把 b 从 (T,5) 迁走，再把 a、b 都改到 (T,5)，构造事务内终态重复。
        dup.update('rules', 'b', {'id': 'b', 'tenant': 'T', 'lower': 9,
                                  'upper': 4, 'active': False})
        dup.update('rules', 'a', {'id': 'a', 'tenant': 'T', 'lower': 5,
                                  'upper': 3, 'active': False})
        with self.assertRaises(Exception):
            dup.update('rules', 'b', {'id': 'b', 'tenant': 'T', 'lower': 5,
                                      'upper': 6, 'active': False})
        self.house.abort(dup)
