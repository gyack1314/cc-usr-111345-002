"""并发模型测试：长事务不持全库写锁，多线程下认证仍然正确。"""
import tempfile
import threading
import unittest
from pathlib import Path

from serlake import Lakehouse, Column, Schema
from serlake.errors import SerializationCycle


class ConcurrencyTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix='serlake-conc-')
        schema = Schema('rules', [
            Column('id', 'TEXT', nullable=False),
            Column('tenant', 'TEXT', nullable=False),
            Column('lower', 'INTEGER'),
            Column('upper', 'INTEGER'),
            Column('active', 'BOOLEAN'),
        ], key_columns=('tenant', 'lower'))
        self.house = Lakehouse.create(
            Path(self.tmp) / 'house', {'rules': schema})
        load = self.house.begin('load')
        load.insert('rules', {'id': 'r0', 'tenant': 'A', 'lower': 0,
                              'upper': 1, 'active': False})
        self.house.commit(load)

    def tearDown(self):
        self.house.close()

    def test_long_reader_does_not_block_writers(self):
        # 长读事务持有句柄期间，其他线程的写事务必须能持续提交
        # （WAL + 短临界区，而不是全库写锁串行化）。
        started = threading.Event()
        release = threading.Event()
        errors = []

        def long_read():
            try:
                t = self.house.begin('long')
                t.range_read('rules', ('Z', 0), ('Z', 100))
                started.set()
                # 保持 ACTIVE 直到写者们完成后被释放。
                release.wait(timeout=5)
                self.house.abort(t)
            except BaseException as exc:  # 测试线程内捕获所有异常
                errors.append(exc)

        thread = threading.Thread(target=long_read)
        thread.start()
        started.wait(timeout=5)

        seq_list = []
        for i in range(3):
            t = self.house.begin(f'writer-{i}')
            t.insert('rules', {'id': f'w{i}', 'tenant': 'W',
                               'lower': i, 'upper': i + 1,
                               'active': True})
            seq_list.append(self.house.commit(t).commit_seq)
        release.set()
        thread.join(timeout=5)

        self.assertFalse(errors)
        self.assertEqual(seq_list, [2, 3, 4])  # 写者未被长读者阻塞
        self.assertEqual(
            len([r for r in self.house.snapshot_scan('rules')
                 if r['tenant'] == 'W']), 3)

    def test_concurrent_certification_serializes_correctly(self):
        # 两个线程同时提交写偏斜事务，恰好一个必须被拒绝。
        outcomes = []
        lock = threading.Lock()
        both_started = threading.Barrier(2)

        def worker(op, rid, lower):
            try:
                t = self.house.begin(op)
                t.range_read('rules', ('A', 0), ('A', 100))
                t.insert('rules', {'id': rid, 'tenant': 'A', 'lower': lower,
                                   'upper': lower + 1, 'active': True})
                # 保证两个事务在任一提交之前都已完成读与写暂存。
                both_started.wait(timeout=5)
                self.house.commit(t)
                with lock:
                    outcomes.append('committed')
            except SerializationCycle:
                with lock:
                    outcomes.append('rejected')
            except BaseException as exc:
                with lock:
                    outcomes.append(f'error:{type(exc).__name__}:{exc}')

        t1 = threading.Thread(target=worker, args=('cc-1', 'cc1', 50))
        t2 = threading.Thread(target=worker, args=('cc-2', 'cc2', 60))
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        self.assertEqual(sorted(outcomes), ['committed', 'rejected'],
                         msg=f'实际结果: {outcomes}')


if __name__ == '__main__':
    unittest.main()
