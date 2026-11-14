"""崩溃重放、幂等发布、回收水位与保留预算测试。"""
import tempfile
import unittest
from pathlib import Path

from serlake import Lakehouse, Column, Schema
from serlake.errors import (
    RetentionBudgetExceeded, SerlakeError, SerializationCycle)
from serlake.checker import HistoryChecker


def open_fresh(tmp, budget=10000):
    schema = Schema('rules', [
        Column('id', 'TEXT', nullable=False),
        Column('tenant', 'TEXT', nullable=False),
        Column('lower', 'INTEGER'),
        Column('upper', 'INTEGER'),
        Column('active', 'BOOLEAN'),
    ], key_columns=('tenant', 'lower'))
    root = Path(tmp) / 'house'
    if root.exists():
        import shutil
        shutil.rmtree(root)
    return Lakehouse.create(root, {'rules': schema},
                            footprint_budget=budget), root


class ReplayTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix='serlake-replay-')
        self.house, self.root = open_fresh(self.tmp)

    def tearDown(self):
        self.house.close()

    def test_crash_before_commit_leaves_no_publish(self):
        t = self.house.begin('crash')
        t.insert('rules', {'id': 'r1', 'tenant': 'A', 'lower': 1,
                           'upper': 2, 'active': True})

        def hook(point, result):
            if point == 'after_certification':
                raise RuntimeError('断电')

        with self.assertRaises(RuntimeError):
            self.house.commit(t, crash_hook=hook)
        # 同一连接内立刻可见的事实：没有任何已发布版本。
        self.assertEqual(self.house.snapshot_scan('rules'), [])
        self.house.close()

        house2 = Lakehouse.open(self.root)
        self.assertEqual(house2.snapshot_scan('rules'), [])
        # 同操作标识得到明确中止结论，而不是重复发布。
        with self.assertRaises(SerlakeError):
            house2.begin('crash')
        retry = house2.begin('crash-retry')
        retry.insert('rules', {'id': 'r1', 'tenant': 'A', 'lower': 1,
                               'upper': 2, 'active': True})
        receipt = house2.commit(retry)
        self.assertEqual(len(house2.snapshot_scan('rules')), 1)
        house2.close()
        self.house = Lakehouse.open(self.root)  # 供 tearDown 关闭

    def test_retry_after_commit_returns_same_receipt(self):
        t = self.house.begin('op')
        t.insert('rules', {'id': 'r1', 'tenant': 'A', 'lower': 1,
                           'upper': 2, 'active': True})
        first = self.house.commit(t)
        replayed = self.house.begin('op')
        self.assertTrue(hasattr(replayed, 'receipt'))
        self.assertEqual(replayed.receipt.commit_seq, first.commit_seq)
        self.assertEqual(len(self.house.snapshot_scan('rules')), 1)

    def test_no_half_committed_after_crash_then_checker(self):
        t = self.house.begin('op-x')
        t.insert('rules', {'id': 'x1', 'tenant': 'X', 'lower': 1,
                           'upper': 2, 'active': True})

        def hook(point, result):
            if point == 'after_commit':
                raise RuntimeError('提交后失联')

        # after_commit 崩溃：版本已原子生效，重连必须只看到一份。
        with self.assertRaises(RuntimeError):
            self.house.commit(t, crash_hook=hook)
        self.house.close()
        house2 = Lakehouse.open(self.root)
        replayed = house2.begin('op-x')
        self.assertTrue(hasattr(replayed, 'receipt'))
        self.assertEqual(len(house2.snapshot_scan('rules')), 1)
        verdict = HistoryChecker(house2).verify()
        self.assertFalse(verdict.failures)
        house2.close()
        self.house = Lakehouse.open(self.root)


class RetentionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix='serlake-gc-')

    def test_long_reader_blocks_gc(self):
        house, _ = open_fresh(self.tmp, budget=10000)
        reader = house.begin('reader')
        reader.range_read('rules', ('H', 0), ('H', 100))
        # 制造一条真实依赖边（reader 空读 -> writer 写）。
        writer = house.begin('w-edge')
        writer.insert('rules', {'id': 'h0', 'tenant': 'H', 'lower': 0,
                                'upper': 1, 'active': True})
        late_reader = house.begin('r-edge')
        late_reader.range_read('rules', ('H', 0), ('H', 100))
        house.commit(writer)
        house.commit(late_reader)
        edges_before = len(house.dependency_edges())
        self.assertEqual(edges_before, 1)
        for i in range(1, 3):
            t = house.begin(f'w{i}')
            t.insert('rules', {'id': f'h{i}', 'tenant': 'H', 'lower': i,
                               'upper': i + 1, 'active': True})
            house.commit(t)
        report = house.retention_report()
        pinned = [c for c in report['retained_committed']
                  if c['commit_seq'] > reader.snapshot_seq]
        self.assertTrue(pinned)
        self.assertTrue(all(not c['gc_eligible'] for c in pinned))
        outcome = house.terminate_longest_reader()
        self.assertTrue(outcome['archived_txns'])
        after = house.retention_report()
        self.assertLess(after['footprint']['total'], report['footprint']['total'])
        # 边不随足迹回收，回收后仍能给出串行次序。
        self.assertEqual(len(house.dependency_edges()), edges_before)
        # 已提交：writer、late_reader、w1、w2 共 4 个；被终止的长读者不在图内。
        self.assertEqual(len(house.serializable_order()), 4)
        house.close()

    def test_budget_exhaustion_is_explicit(self):
        house, _ = open_fresh(self.tmp, budget=4)
        # 活跃读者钉住回收水位，使已提交足迹不能被归档，预算逐步耗尽。
        reader = house.begin('reader')
        reader.range_read('rules', ('A', 0), ('A', 100))
        t = house.begin('first')
        t.insert('rules', {'id': 'r1', 'tenant': 'A', 'lower': 1,
                           'upper': 2, 'active': True})
        house.commit(t)
        self.assertGreaterEqual(house.storage.footprint_size()['total'], 1)
        # 把预算压到当前足迹之下模拟预算耗尽：新事务必须被显式拒绝，
        # 而不是静默降低隔离级别。
        house.storage.set_budget(1)
        with self.assertRaises(RetentionBudgetExceeded):
            house.begin('over-budget')
        house.abort(reader)
        house.close()

    def test_edges_survive_footprint_gc(self):
        house, _ = open_fresh(self.tmp)
        t1 = house.begin('a1')
        t1.range_read('rules', ('A', 0), ('A', 100))
        t1.insert('rules', {'id': 'a1', 'tenant': 'A', 'lower': 1,
                            'upper': 2, 'active': True})
        house.commit(t1)
        t2 = house.begin('a2')
        t2.range_read('rules', ('B', 0), ('B', 100))
        t2.insert('rules', {'id': 'b1', 'tenant': 'B', 'lower': 1,
                            'upper': 2, 'active': True})
        house.commit(t2)
        edge_count_before = len(house.dependency_edges())
        house.run_gc()
        self.assertEqual(len(house.dependency_edges()), edge_count_before)
        # 串行次序仍可从保留的边推导。
        order = house.serializable_order()
        self.assertEqual(set(order), {t1.tid, t2.tid})
        house.close()


if __name__ == '__main__':
    unittest.main()
