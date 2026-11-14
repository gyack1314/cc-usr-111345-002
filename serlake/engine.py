"""引擎门面：湖仓生命周期、事务句柄、回收预算与崩溃重放。

并发模型对应需求中的边界：
- 业务事务执行期间不持有任何全库写锁（WAL 下读不阻塞读写）；
- 仅认证+发布进入一次短暂 IMMEDIATE 临界事务，快照、写文件、
  认证依据与事务身份在临界区内绑定，版本/回执/依赖元数据一起生效；
- 同一 operation_id 重试：已提交则原样返回回执，不重复发布；
  认证后崩溃因临界事务原子回滚，重放就是重新认证（无半提交状态）。

回收原则：
- 水位 = 所有 ACTIVE 事务中最小的 snapshot_seq；
- 已提交事务 T 满足 T.commit_seq <= 水位时，任何现存或未来的活动事务
  快照都已包含 T，不可能再有新边连向 T 的足迹，详细读写谓词可归档；
- 图节点与已物化边始终保留（环见证还要用），只回收详细足迹；
- 预算不足时 begin 显式拒绝，或调用 terminate_longest_reader() 终止
  最老的可终止只读事务——绝不静默降低隔离级别。
"""
from dataclasses import dataclass
from pathlib import Path

from .certifier import Certifier
from .errors import (
    ConflictAbort, RetentionBudgetExceeded,
    TransactionStateError, SerlakeError)
from .journal import EVENT_TERMINATE, EVENT_GC
from .schema import Schema
from .storage import Storage, jdump, jload
from .transaction import Transaction


@dataclass
class CommitReceipt:
    tid: int
    operation_id: str
    commit_seq: int
    snapshot_seq: int
    read_count: int
    write_count: int
    edges_added: dict
    serial_order: list

    def to_dict(self):
        return {
            'tid': self.tid, 'operation_id': self.operation_id,
            'commit_seq': self.commit_seq, 'snapshot_seq': self.snapshot_seq,
            'read_count': self.read_count, 'write_count': self.write_count,
            'edges_added': self.edges_added,
            'serial_order': self.serial_order}


class Lakehouse:
    def __init__(self, storage, schemas):
        self.storage = storage
        self.schemas = schemas
        self.certifier = Certifier(self)
        self._handles = {}

    # ---- 生命周期 ---------------------------------------------------
    @classmethod
    def create(cls, path, schemas, footprint_budget=10000):
        for name in schemas:
            (Path(path) / 'data' / 'tables' / name).mkdir(
                parents=True, exist_ok=True)
        storage = Storage.create(path)
        storage.set_meta('schemas', {name: s.to_dict()
                                     for name, s in schemas.items()})
        storage.set_meta('footprint_budget', int(footprint_budget))
        return cls.open(path)

    @classmethod
    def open(cls, path):
        storage = Storage.open(path)
        schema_data = storage.get_meta('schemas', {})
        schemas = {name: Schema.from_dict(data)
                   for name, data in schema_data.items()}
        house = cls(storage, schemas)
        house.reconcile()
        return house

    def close(self):
        self.storage.close()

    def reconcile(self):
        """本地调度点重放：清理崩溃残留并返回处理摘要。

        原子临界事务保证不存在 COMMITTING 中间态；这里处理：
        1) 暂存了写文件却未提交、崩溃后仍为 ACTIVE 的事务：标记中止；
        2) 没有任何事务归属的孤儿暂存版本：删文件删记录。
        已 ABORTED 事务的暂存版本特意保留——它们永不生效，但独立历史
        检查器重放"被拒历史为何不可串行化"时需要完整行内容。
        同一 operation_id 的重试走 begin 的幂等分支，不产生重复发布。
        """
        conn = self.storage.conn
        summary = {'stale_active_aborted': [], 'orphan_versions': 0}
        known_tids = {r['tid'] for r in conn.execute(
            'SELECT tid FROM txns').fetchall()}
        for staged in conn.execute(
                'SELECT DISTINCT creator_tid FROM row_versions '
                'WHERE staged=1').fetchall():
            tid = staged['creator_tid']
            if tid not in known_tids:
                continue
            txn = self.storage.get_txn(tid)
            if txn['state'] == 'ACTIVE':
                conn.execute(
                    'UPDATE txns SET state=?, abort_reason=? WHERE tid=?',
                    ('ABORTED', 'crash_before_commit', tid))
                self.storage.append_audit({
                    'event': EVENT_TERMINATE, 'tid': tid,
                    'reason': 'crash_before_commit'})
                summary['stale_active_aborted'].append(tid)
        orphans = conn.execute(
            'SELECT vid,data_path,creator_tid FROM row_versions '
            'WHERE staged=1').fetchall()
        for row in orphans:
            if row['creator_tid'] in known_tids:
                continue  # 属于已登记事务（含已中止）的版本保留供审计
            if row['data_path']:
                p = self.storage.path / row['data_path']
                if p.exists():
                    p.unlink()
            conn.execute('DELETE FROM row_versions WHERE vid=?', (row['vid'],))
            summary['orphan_versions'] += 1
        return summary

    # ---- 事务开始 ---------------------------------------------------
    def begin(self, operation_id):
        """返回事务句柄；同一 operation_id 重试返回原句柄或既有回执。"""
        existing = self.storage.find_operation(operation_id)
        if existing is not None:
            if existing['state'] == 'COMMITTED':
                order = self.serializable_order()
                return _ReplayedReceipt(self._receipt(existing, order))
            if existing['state'] == 'ABORTED':
                raise SerlakeError(
                    f"操作 {operation_id} 已中止: "
                    f"{existing['abort_reason']}，请换新操作标识重试")
            # ACTIVE：崩溃后重连，原句柄已失效，重建句柄继续使用。
            handle = self._open_handle(existing)
            self._handles[existing['tid']] = handle
            return handle

        self._enforce_budget_for_begin()
        txn_row, _ = self.storage.begin_txn(operation_id)
        handle = self._open_handle(txn_row)
        self._handles[txn_row['tid']] = handle
        return handle

    def _open_handle(self, txn_row):
        return Transaction(
            self, txn_row['tid'], txn_row['snapshot_seq'],
            txn_row['operation_id'])

    def _enforce_budget_for_begin(self):
        footprint = self.storage.footprint_size()
        budget = self.storage.budget()
        # 前瞻判断：新事务至少要登记 begin 审计与事务行，且任何首次读/写
        # 都会立刻增加足迹；预算为 0 或当前已达上限都必须显式拒绝。
        if budget <= 0 or footprint['total'] >= budget:
            raise RetentionBudgetExceeded(
                f"保留预算耗尽: 已保留足迹 {footprint['total']} >= "
                f"预算 {budget}；请先回收或终止长读事务，"
                '隔离级别不会自动降低',
                footprint=footprint, budget=budget)

    # ---- 提交/中止 --------------------------------------------------
    def commit(self, txn, crash_hook=None):
        if isinstance(txn, _ReplayedReceipt):
            return txn.receipt
        try:
            receipt_data, order = self.certifier.certify_and_publish(
                txn, crash_hook=crash_hook)
        except ConflictAbort as exc:
            # 临界事务已原子回滚：版本未生效、边未写入。在新事务里
            # 持久化 ABORT 与审计事件，保证拒绝本身也是可重放的事实。
            self._persist_abort(txn.tid, exc)
            txn.mark_finished('ABORTED')
            self._handles.pop(txn.tid, None)
            raise
        txn.mark_finished('COMMITTED')
        self._handles.pop(txn.tid, None)
        self.run_gc()
        return self._receipt(receipt_data, order)

    def _persist_abort(self, tid, exc):
        reason = {
            'WriteWriteConflict': 'write_write_conflict',
            'UniquenessViolation': 'uniqueness_violation',
            'SerializationCycle': 'serialization_cycle',
        }.get(type(exc).__name__, type(exc).__name__)
        self.storage.abort_txn(tid, reason, witness=getattr(exc, 'witness', None))
        metrics = self.certifier.metrics
        metrics.aborted += 1
        metrics.record_attempt(tid, f'aborted:{reason}',
                               {'witness': getattr(exc, 'witness', None)})

    def abort(self, txn, reason='user_abort'):
        if isinstance(txn, _ReplayedReceipt):
            raise TransactionStateError('该操作已提交，不能中止')
        self.storage.abort_txn(txn.tid, reason)
        txn.mark_finished('ABORTED')
        self._handles.pop(txn.tid, None)

    def _receipt(self, txn_row_or_data, order):
        if 'receipt_json' in txn_row_or_data and \
                txn_row_or_data.get('receipt_json'):
            data = jload(txn_row_or_data['receipt_json'])
        else:
            data = txn_row_or_data
        return CommitReceipt(
            tid=data['tid'], operation_id=data['operation_id'],
            commit_seq=data['commit_seq'], snapshot_seq=data['snapshot_seq'],
            read_count=data['read_count'], write_count=data['write_count'],
            edges_added=data['edges_added'], serial_order=order)

    # ---- 只读快照查询（外部读者只见已提交版本） --------------------
    def snapshot_read_point(self, table_name, key):
        schema = self.schemas[table_name]
        from .predicate import PointPredicate
        predicate = PointPredicate(schema, tuple(key))
        seq = self.storage.current_commit_seq()
        for row_id, version in self.storage.visible_versions(
                seq, table_name).items():
            if version['is_delete']:
                continue
            row = self.storage.read_version_file(version['data_path'])
            if predicate.contains(schema.sort_key(row)):
                return row
        return None

    def snapshot_scan(self, table_name):
        seq = self.storage.current_commit_seq()
        schema = self.schemas[table_name]
        rows = []
        for version in self.storage.visible_versions(seq, table_name).values():
            if version['is_delete']:
                continue
            rows.append(self.storage.read_version_file(version['data_path']))
        from .transaction import _key_cmp
        return sorted(rows, key=_key_cmp(schema))

    # ---- 图与串行次序 ----------------------------------------------
    def dependency_edges(self):
        return self.storage.all_edges()

    def serializable_order(self):
        from .graph import DependencyGraph
        return DependencyGraph.from_storage(self.storage).topological_order()

    def explain_serialization(self):
        from .graph import DependencyGraph
        graph = DependencyGraph.from_storage(self.storage)
        order = graph.topological_order()
        return {
            'serial_order': [f'T{tid}' for tid in order],
            'edge_checks': graph.explain_order(order),
        }

    # ---- 回收与预算 -------------------------------------------------
    def retention_report(self):
        active = self.storage.active_txns()
        watermark = min((t['snapshot_seq'] for t in active),
                        default=self.storage.current_commit_seq())
        footprint = self.storage.footprint_size()
        retained_committed = []
        for t in self.storage.committed_txns():
            reads = self.storage.reads_of(t['tid'])
            writes = self.storage.writes_of(t['tid'])
            retained_committed.append({
                'tid': t['tid'], 'commit_seq': t['commit_seq'],
                'snapshot_seq': t['snapshot_seq'],
                'reads_kept': len(reads), 'writes_kept': len(writes),
                'gc_eligible': t['commit_seq'] <= watermark})
        return {
            'watermark': watermark,
            'active_count': len(active),
            'active_readers': [
                {'tid': t['tid'], 'snapshot_seq': t['snapshot_seq'],
                 'readonly': bool(t['is_readonly'])} for t in active],
            'budget': self.storage.budget(),
            'footprint': footprint,
            'retained_committed': retained_committed,
            'gc_history': self.storage.gc_rows()}

    def run_gc(self):
        """把水位以下已提交事务的详细足迹移到冷归档；图边始终保留。"""
        conn = self.storage.conn
        active = self.storage.active_txns()
        watermark = min((t['snapshot_seq'] for t in active),
                        default=self.storage.current_commit_seq())
        moved = []
        for t in self.storage.committed_txns():
            if t['commit_seq'] > watermark:
                continue
            already = conn.execute(
                'SELECT 1 FROM gc_log WHERE tid=?', (t['tid'],)).fetchone()
            if already:
                continue
            reads = self.storage.reads_of(t['tid'])
            writes = self.storage.writes_of(t['tid'])
            if not reads and not writes:
                continue
            archive_dir = self.storage.path / 'data' / 'archive'
            archive_dir.mkdir(parents=True, exist_ok=True)
            archive_path = archive_dir / f'txn-{t["tid"]}-footprint.json'
            archive_path.write_text(
                jdump({'tid': t['tid'], 'reads': reads, 'writes': writes}),
                encoding='utf-8')
            conn.execute('DELETE FROM txn_reads WHERE tid=?', (t['tid'],))
            conn.execute('DELETE FROM txn_writes WHERE tid=?', (t['tid'],))
            conn.execute(
                'INSERT INTO gc_log(watermark,tid,moved_reads,'
                'moved_writes,archive_path) VALUES(?,?,?,?,?)',
                (watermark, t['tid'], len(reads), len(writes),
                 str(archive_path.relative_to(self.storage.path))))
            self.storage.append_audit({
                'event': EVENT_GC, 'tid': t['tid'], 'watermark': watermark,
                'moved_reads': len(reads), 'moved_writes': len(writes)})
            moved.append(t['tid'])
        return {'watermark': watermark, 'archived_txns': moved}

    def terminate_longest_reader(self):
        """预算压力下的显式处置：终止最老的只读 ACTIVE 事务并回收。"""
        active = [t for t in self.storage.active_txns() if t['is_readonly']]
        if not active:
            raise SerlakeError('没有可终止的只读事务')
        victim = min(active, key=lambda t: t['snapshot_seq'])
        self.storage.abort_txn(victim['tid'], 'terminated_for_retention')
        self.storage.append_audit({
            'event': EVENT_TERMINATE, 'tid': victim['tid'],
            'reason': 'terminated_for_retention'})
        handle = self._handles.pop(victim['tid'], None)
        if handle is not None:
            handle.mark_finished('ABORTED')
        gc_result = self.run_gc()
        return {'terminated_tid': victim['tid'], **gc_result}

    # ---- 审计 -------------------------------------------------------
    def audit_events(self):
        return self.storage.audit_events()

    def metrics(self):
        return self.certifier.metrics.snapshot()


class _ReplayedReceipt:
    """幂等重试的包装：commit() 识别后直接返回原回执，绝不重复发布。"""

    def __init__(self, receipt):
        self.receipt = receipt
