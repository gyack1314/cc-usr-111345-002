"""多版本存储：SQLite 元数据 + 不可变行版本文件。

锁的边界（回应"不能把整个业务事务期间都持有全库写锁"）：
- begin / 读 / 写暂存全部走 WAL 下的自动提交语句，读连接永远不申请写锁；
- 只有认证+正式发布进入一次 BEGIN IMMEDIATE 短事务，临界区内完成
  足迹查询、建边、环检测、版本生效、回执与审计事件的原子写入；
- 因此长读事务持有多久都不会阻塞其他事务的读写，只影响回收水位。

行版本模型：
- 每次插入/更新/删除产生一个新版本文件 data/tables/<表>/<vid>.json；
- row_versions 只在事务提交时获得 create_seq，未提交版本对任何外部读者不可见；
- 同一 row_id 的可见版本 = create_seq <= 快照 seq 的最大 vid；墓碑即删除版本。
"""
import json
import os
import sqlite3
import threading
from pathlib import Path

from .errors import SerlakeError, TransactionStateError

SCHEMA_VERSION = 1

DDL = """
CREATE TABLE IF NOT EXISTS meta(
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS txns(
    tid INTEGER PRIMARY KEY,
    operation_id TEXT UNIQUE NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('ACTIVE','COMMITTED','ABORTED')),
    snapshot_seq INTEGER NOT NULL,
    is_readonly INTEGER NOT NULL DEFAULT 1,
    created_order INTEGER NOT NULL,
    commit_seq INTEGER,
    receipt_json TEXT,
    abort_reason TEXT
);
CREATE TABLE IF NOT EXISTS row_versions(
    vid INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT NOT NULL,
    row_id TEXT NOT NULL,
    creator_tid INTEGER NOT NULL,
    create_seq INTEGER,
    predecessor_vid INTEGER,
    is_delete INTEGER NOT NULL DEFAULT 0,
    sort_key_json TEXT NOT NULL,
    data_path TEXT NOT NULL,
    staged INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_versions_lookup
    ON row_versions(table_name, row_id, create_seq);
CREATE TABLE IF NOT EXISTS txn_reads(
    rid INTEGER PRIMARY KEY AUTOINCREMENT,
    tid INTEGER NOT NULL,
    order_no INTEGER NOT NULL,
    table_name TEXT NOT NULL,
    predicate_json TEXT NOT NULL,
    observed_vids_json TEXT NOT NULL,
    observed_row_ids_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_reads_tid ON txn_reads(tid);
CREATE TABLE IF NOT EXISTS txn_writes(
    wid INTEGER PRIMARY KEY AUTOINCREMENT,
    tid INTEGER NOT NULL,
    order_no INTEGER NOT NULL,
    table_name TEXT NOT NULL,
    row_id TEXT NOT NULL,
    op_kind TEXT NOT NULL CHECK(op_kind IN ('INSERT','UPDATE','DELETE')),
    before_vid INTEGER,
    after_vid INTEGER NOT NULL,
    key_before_json TEXT,
    key_after_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_writes_tid ON txn_writes(tid);
CREATE TABLE IF NOT EXISTS edges(
    edge_id INTEGER PRIMARY KEY AUTOINCREMENT,
    from_tid INTEGER NOT NULL,
    to_tid INTEGER NOT NULL,
    edge_type TEXT NOT NULL CHECK(edge_type IN ('ww','wr','rw')),
    witness_json TEXT NOT NULL,
    commit_seq INTEGER NOT NULL,
    UNIQUE(from_tid, to_tid, edge_type, witness_json)
);
CREATE INDEX IF NOT EXISTS idx_edges_from ON edges(from_tid);
CREATE INDEX IF NOT EXISTS idx_edges_to ON edges(to_tid);
CREATE TABLE IF NOT EXISTS audit_journal(
    journal_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS gc_log(
    gc_id INTEGER PRIMARY KEY AUTOINCREMENT,
    watermark INTEGER NOT NULL,
    tid INTEGER NOT NULL,
    moved_reads INTEGER NOT NULL,
    moved_writes INTEGER NOT NULL,
    archive_path TEXT NOT NULL
);
"""


def jdump(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def jload(text):
    return json.loads(text)


class Storage:
    """文件与 SQLite 的封装；不含认证策略，只提供原子性原语。

    连接模型：每个线程一个独立连接（thread-local），全部打开 WAL。
    读路径在自己的连接上自动提交，互不干扰；认证+发布用进程内
    _write_lock 串行进入 IMMEDIATE 临界事务，SQLite 层再用文件锁
    兜底，保证多线程/多进程下临界区仍然互斥。
    """

    def __init__(self, path):
        self.path = Path(path)
        self.db_path = self.path / 'meta.db'
        self._write_lock = threading.Lock()
        self._local = threading.local()
        self._all_conns = []
        self._conn_guard = threading.Lock()

    def _new_connection(self):
        conn = sqlite3.connect(
            self.db_path, isolation_level=None, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA synchronous=FULL')
        conn.execute('PRAGMA foreign_keys=ON')
        conn.execute('PRAGMA busy_timeout=10000')
        with self._conn_guard:
            self._all_conns.append(conn)
        return conn

    def _connect(self):
        """主线程初始化连接（建库 DDL 也走该连接）。"""
        self._local.conn = self._new_connection()
        return self._local.conn

    @property
    def conn(self):
        """当前线程的专属连接，惰性创建。"""
        conn = getattr(self._local, 'conn', None)
        if conn is None:
            conn = self._new_connection()
            self._local.conn = conn
        return conn

    # ---- 打开/创建 --------------------------------------------------
    @classmethod
    def create(cls, path):
        root = Path(path)
        root.mkdir(parents=True, exist_ok=True)
        (root / 'data' / 'tables').mkdir(parents=True, exist_ok=True)
        (root / 'data' / 'archive').mkdir(parents=True, exist_ok=True)
        store = cls(path)
        conn = store._connect()
        conn.executescript(DDL)
        # isolation_level=None 为自动提交模式，下面每条语句独立生效。
        store.set_meta('schema_version', SCHEMA_VERSION)
        store.set_meta('commit_seq', 0)
        store.set_meta('next_tid', 0)
        store.set_meta('created_counter', 0)
        store.set_meta('footprint_budget', 10000)
        store.set_meta('schemas', {})
        return store

    @classmethod
    def open(cls, path):
        store = cls(path)
        if not (Path(path) / 'meta.db').exists():
            raise SerlakeError(f'湖仓不存在或未初始化: {path}')
        conn = store._connect()
        conn.executescript(DDL)
        return store

    def conn_commit(self):
        self.conn.execute('COMMIT')

    def close(self):
        with self._conn_guard:
            for conn in self._all_conns:
                conn.close()
            self._all_conns.clear()
        self._local = threading.local()

    # ---- meta -------------------------------------------------------
    def get_meta(self, key, default=None):
        row = self.conn.execute(
            'SELECT value FROM meta WHERE key=?', (key,)).fetchone()
        return default if row is None else jload(row['value'])

    def set_meta(self, key, value):
        self.conn.execute(
            'INSERT INTO meta(key,value) VALUES(?,?) '
            'ON CONFLICT(key) DO UPDATE SET value=excluded.value',
            (key, jdump(value)))

    # ---- 审计日志（自动提交版；提交临界区内改用 critical 连接） -----
    def append_audit(self, event):
        self.conn.execute(
            'INSERT INTO audit_journal(event_json) VALUES(?)',
            (jdump(event),))

    def audit_events(self):
        rows = self.conn.execute(
            'SELECT event_json FROM audit_journal ORDER BY journal_id').fetchall()
        return [jload(r['event_json']) for r in rows]

    # ---- 事务登记 ---------------------------------------------------
    def begin_txn(self, operation_id):
        # 自动提交语句，不持 _write_lock（该锁只保护认证+发布临界区，
        # 且不可重入）；SQLite 自身的序列化写入保证登记原子性。
        existing = self.conn.execute(
            'SELECT * FROM txns WHERE operation_id=?',
            (operation_id,)).fetchone()
        if existing is not None:
            return dict(existing), True
        snapshot = self.current_commit_seq()
        # tid 与 created_order 的自增必须是单条原子语句：
        # "读 meta 再写" 在多线程/多连接下会撞主键。
        tid = self._bump_counter('next_tid')
        created = self._bump_counter('created_counter')
        self.conn.execute(
            'INSERT INTO txns(tid,operation_id,state,snapshot_seq,'
            'is_readonly,created_order) VALUES(?,?,?,?,1,?)',
            (tid, operation_id, 'ACTIVE', snapshot, created))
        self.append_audit({
            'event': 'begin', 'tid': tid, 'operation_id': operation_id,
            'snapshot_seq': snapshot})
        return self.get_txn(tid), False

    def _bump_counter(self, key):
        """原子自增 meta 计数器并返回新值。

        使用 UPDATE ... RETURNING 让 "读取旧值 + 写回新值" 成为单条语句；
        SQLite 保证该语句在数据库写锁下原子执行，多连接并发也不会丢更新。
        value 列声明为 TEXT，数值经 json 文本存储，这里显式 CAST 后自增。
        """
        row = self.conn.execute(
            "UPDATE meta SET value = CAST(CAST(value AS INTEGER) + 1 "
            "AS TEXT) WHERE key=? RETURNING value",
            (key,)).fetchone()
        if row is None:
            raise SerlakeError(f'计数器 {key} 未初始化')
        return int(row['value'])

    def current_commit_seq(self):
        return int(self.get_meta('commit_seq', 0))

    def get_txn(self, tid):
        row = self.conn.execute('SELECT * FROM txns WHERE tid=?', (tid,)).fetchone()
        if row is None:
            raise SerlakeError(f'事务不存在: {tid}')
        return dict(row)

    def find_operation(self, operation_id):
        row = self.conn.execute(
            'SELECT * FROM txns WHERE operation_id=?',
            (operation_id,)).fetchone()
        return None if row is None else dict(row)

    def active_txns(self):
        rows = self.conn.execute(
            "SELECT * FROM txns WHERE state='ACTIVE' ORDER BY tid").fetchall()
        return [dict(r) for r in rows]

    def committed_txns(self):
        rows = self.conn.execute(
            "SELECT * FROM txns WHERE state='COMMITTED' ORDER BY commit_seq").fetchall()
        return [dict(r) for r in rows]

    def mark_writable(self, tid):
        self.conn.execute(
            'UPDATE txns SET is_readonly=0 WHERE tid=?', (tid,))

    def abort_txn(self, tid, reason, witness=None):
        txn = self.get_txn(tid)
        if txn['state'] != 'ACTIVE':
            raise TransactionStateError(f'事务 {tid} 已结束，不能中止')
        self.conn.execute(
            'UPDATE txns SET state=?, abort_reason=? WHERE tid=?',
            ('ABORTED', reason, tid))
        self.append_audit({
            'event': 'abort', 'tid': tid, 'reason': reason,
            'witness': witness or {}})

    # ---- 行版本文件 -------------------------------------------------
    def _version_path(self, table_name, vid, is_delete):
        suffix = '.tombstone.json' if is_delete else '.json'
        return self.path / 'data' / 'tables' / table_name / f'{vid}{suffix}'

    def add_staged_version(self, tid, table_name, row_id, full_row,
                           is_delete, predecessor_vid, sort_key):
        """写入不可变版本文件并登记未生效版本（自动提交，不持长锁）。"""
        cur = self.conn.execute(
            'INSERT INTO row_versions(table_name,row_id,creator_tid,'
            'predecessor_vid,is_delete,sort_key_json,data_path,staged) '
            'VALUES(?,?,?,?,?,?,?,1)',
            (table_name, row_id, tid, predecessor_vid, int(is_delete),
             jdump(list(sort_key)), ''))
        vid = cur.lastrowid
        version_path = self._version_path(table_name, vid, is_delete)
        version_path.parent.mkdir(parents=True, exist_ok=True)
        payload = (
            {'__tombstone__': True, 'row_id': row_id, 'table': table_name}
            if is_delete else full_row)
        tmp = version_path.with_suffix(version_path.suffix + '.tmp')
        with open(tmp, 'w', encoding='utf-8') as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, version_path)
        self.conn.execute(
            'UPDATE row_versions SET data_path=? WHERE vid=?',
            (str(version_path.relative_to(self.path)), vid))
        return vid

    def read_version_file(self, data_path):
        if not data_path:
            return None
        return jload(Path(self.path / data_path).read_text(encoding='utf-8'))

    def visible_versions(self, snapshot_seq, table_name):
        """快照下表内每个 row_id 的最新可见版本（墓碑保留为删除标记）。"""
        rows = self.conn.execute(
            'SELECT * FROM row_versions WHERE table_name=? AND staged=0 '
            'AND create_seq<=? ORDER BY vid',
            (table_name, snapshot_seq)).fetchall()
        latest = {}
        for row in rows:
            latest[row['row_id']] = dict(row)
        return latest

    def get_version(self, vid):
        row = self.conn.execute(
            'SELECT * FROM row_versions WHERE vid=?', (vid,)).fetchone()
        return None if row is None else dict(row)

    def staged_versions_of(self, tid):
        rows = self.conn.execute(
            'SELECT * FROM row_versions WHERE creator_tid=? AND staged=1 '
            'ORDER BY vid', (tid,)).fetchall()
        return [dict(r) for r in rows]

    # ---- 足迹登记 ---------------------------------------------------
    def record_read(self, tid, order_no, table_name, predicate_dict,
                    observed_vids, observed_row_ids):
        self.conn.execute(
            'INSERT INTO txn_reads(tid,order_no,table_name,predicate_json,'
            'observed_vids_json,observed_row_ids_json) VALUES(?,?,?,?,?,?)',
            (tid, order_no, table_name, jdump(predicate_dict),
             jdump(observed_vids), jdump(observed_row_ids)))

    def reads_of(self, tid):
        rows = self.conn.execute(
            'SELECT * FROM txn_reads WHERE tid=? ORDER BY order_no',
            (tid,)).fetchall()
        return [dict(r) for r in rows]

    def record_write(self, tid, order_no, table_name, row_id, op_kind,
                     before_vid, after_vid, key_before, key_after):
        self.mark_writable(tid)
        self.conn.execute(
            'INSERT INTO txn_writes(tid,order_no,table_name,row_id,op_kind,'
            'before_vid,after_vid,key_before_json,key_after_json) '
            'VALUES(?,?,?,?,?,?,?,?,?)',
            (tid, order_no, table_name, row_id, op_kind, before_vid,
             after_vid,
             None if key_before is None else jdump(list(key_before)),
             None if key_after is None else jdump(list(key_after))))

    def writes_of(self, tid):
        rows = self.conn.execute(
            'SELECT * FROM txn_writes WHERE tid=? ORDER BY order_no',
            (tid,)).fetchall()
        return [dict(r) for r in rows]

    # ---- 依赖边 -----------------------------------------------------
    def all_edges(self):
        rows = self.conn.execute(
            'SELECT * FROM edges ORDER BY edge_id').fetchall()
        return [dict(r) for r in rows]

    def insert_edge(self, conn, from_tid, to_tid, edge_type, witness, seq):
        conn.execute(
            'INSERT OR IGNORE INTO edges(from_tid,to_tid,edge_type,'
            'witness_json,commit_seq) VALUES(?,?,?,?,?)',
            (from_tid, to_tid, edge_type, jdump(witness), seq))

    # ---- 原子认证+发布临界区 ---------------------------------------
    def critical_section(self, work, crash_hook=None):
        """进入 IMMEDIATE 短事务执行 work(conn)，work 返回要发布的结果。

        work 内的全部读写都在同一连接同一事务里：认证依据、版本生效、
        回执、审计事件要么一起可见，要么一起回滚，中间不存在竞态窗口。
        crash_hook('after_certification') 在 COMMIT 前抛出，模拟提交前中断。
        """
        with self._write_lock:
            conn = self.conn
            conn.execute('BEGIN IMMEDIATE')
            committed = False
            try:
                result = work(conn)
                if crash_hook is not None:
                    crash_hook('after_certification', result)
                conn.execute('COMMIT')
                committed = True
                if crash_hook is not None:
                    # 提交后失联：事务已原子生效，钩子异常只模拟调用方失联。
                    crash_hook('after_commit', result)
                return result
            except BaseException:
                if not committed:
                    conn.execute('ROLLBACK')
                raise

    # ---- 回收与预算 -------------------------------------------------
    def footprint_size(self):
        reads = self.conn.execute(
            'SELECT COUNT(*) AS c FROM txn_reads').fetchone()['c']
        writes = self.conn.execute(
            'SELECT COUNT(*) AS c FROM txn_writes').fetchone()['c']
        edges = self.conn.execute(
            'SELECT COUNT(*) AS c FROM edges').fetchone()['c']
        return {'reads': reads, 'writes': writes, 'edges': edges,
                'total': reads + writes + edges}

    def budget(self):
        return int(self.get_meta('footprint_budget'))

    def set_budget(self, value):
        self.set_meta('footprint_budget', int(value))

    def gc_rows(self):
        return [dict(r) for r in self.conn.execute(
            'SELECT * FROM gc_log ORDER BY gc_id').fetchall()]
