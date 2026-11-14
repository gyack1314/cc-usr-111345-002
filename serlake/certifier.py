"""可串行化认证器：基于实际读写依赖的 MVSG 环检测。

认证不是把事务排队，也不是比较表指针，而是：
1. 取候选事务冻结快照之后提交的每个已提交事务 C；
2. 用 C 的读谓词（含空结果谓词）对候选的写键做命中测试 -> rw 反依赖；
3. 用候选的读所见版本追溯创建者 -> wr 依赖；
4. 同 row_id 的写交集 -> ww 覆盖（同键并发写策略：首提者胜）；
5. 边全部加入后做 DFS 环检测，环中每条边带点键/区间见证；
6. 无环才发布：版本生效、回执、边、审计事件在同一临界事务原子写入。

活动事务之间不物化边：后提交者认证时两者都已提交，信息完备，
且不存在"只比较提交时间"——边的方向由快照可见性与谓词命中决定。
"""
from .errors import (
    WriteWriteConflict, UniquenessViolation,
    TransactionStateError)
from .graph import DependencyGraph
from .predicate import predicate_from_dict
from .storage import jdump, jload
from .journal import EVENT_COMMIT


class CertificationMetrics:
    """认证访问量统计：每次 commit 认证实际扫描了多少足迹条目。"""

    def __init__(self):
        self.attempts = 0
        self.committed = 0
        self.aborted = 0
        self.committed_tx_scanned = 0
        self.predicate_tests = 0
        self.write_effects_tested = 0
        self.read_records_scanned = 0
        self.edges_added = {'ww': 0, 'wr': 0, 'rw': 0}
        self.history = []

    def snapshot(self):
        return {
            'attempts': self.attempts,
            'committed': self.committed,
            'aborted': self.aborted,
            'committed_tx_scanned': self.committed_tx_scanned,
            'predicate_tests': self.predicate_tests,
            'write_effects_tested': self.write_effects_tested,
            'read_records_scanned': self.read_records_scanned,
            'edges_added': dict(self.edges_added),
        }

    def record_attempt(self, tid, outcome, detail):
        self.history.append({'tid': tid, 'outcome': outcome, **detail})


class Certifier:
    def __init__(self, engine):
        self.engine = engine
        self.storage = engine.storage
        self.schemas = engine.schemas
        self.metrics = CertificationMetrics()

    # ---- 主入口 -----------------------------------------------------
    def certify_and_publish(self, txn, crash_hook=None):
        storage = self.storage
        tid = txn.tid

        def work(conn):
            self.metrics.attempts += 1
            row = conn.execute('SELECT * FROM txns WHERE tid=?', (tid,)).fetchone()
            if row is None:
                raise TransactionStateError(f'事务 T{tid} 不存在')
            if row['state'] != 'ACTIVE':
                raise TransactionStateError(
                    f'事务 T{tid} 状态为 {row["state"]}，不能再次认证')

            candidate_writes = [dict(r) for r in conn.execute(
                'SELECT * FROM txn_writes WHERE tid=? ORDER BY order_no',
                (tid,)).fetchall()]
            candidate_reads = [dict(r) for r in conn.execute(
                'SELECT * FROM txn_reads WHERE tid=? ORDER BY order_no',
                (tid,)).fetchall()]
            snapshot_seq = row['snapshot_seq']
            detail = {'scanned_committed': 0, 'predicate_tests': 0,
                      'write_effects': 0, 'read_records': 0,
                      'edges': {'ww': 0, 'wr': 0, 'rw': 0}}

            graph = self._load_committed_graph(conn)
            graph.add_node(tid)
            new_edges = []   # (from, to, type, witness)

            # 只与"候选快照之后已提交"的事务建边。为什么这已充分：
            # 认证被 _write_lock + IMMEDIATE 串行化，后提交者认证时，任何
            # 先提交者若在它 begin 之后提交，必然 commit_seq > 它的
            # snapshot_seq，因而落在 later_committed 里；若先提交者早于它
            # begin，则它快照已包含对方，无需边。故"两个都在活跃、按相反
            # 顺序提交"的情形会在第二次认证时被看到，不会漏边。
            # 不扫描在途事务：它们可能中止，把边连到中止节点会污染图。
            later_committed = [dict(r) for r in conn.execute(
                "SELECT * FROM txns WHERE state='COMMITTED' "
                "AND commit_seq>? ORDER BY commit_seq",
                (snapshot_seq,)).fetchall()]
            detail['scanned_committed'] = len(later_committed)
            self.metrics.committed_tx_scanned += len(later_committed)

            # ---- 1) ww：同 row_id 并发写，首提者胜 ---------------
            self._check_write_write(conn, tid, candidate_writes,
                                    later_committed, new_edges, detail)
            # ---- 2) 提交时唯一键复核（并发插入同键） -------------
            self._check_unique_keys(conn, tid, candidate_writes, detail)
            # ---- 3a) rw：已提交读谓词命中候选写键（反依赖） ------
            for other in later_committed:
                self._build_rw_edges(conn, other['tid'], tid,
                                     candidate_writes, new_edges, detail)
            # ---- 3b) rw：候选读谓词命中已提交写键（反向反依赖） --
            for other in later_committed:
                other_writes = [dict(r) for r in conn.execute(
                    'SELECT * FROM txn_writes WHERE tid=? ORDER BY order_no',
                    (other['tid'],)).fetchall()]
                self._build_rw_edges(
                    conn, tid, other['tid'], other_writes,
                    new_edges, detail, reader_is_candidate=True,
                    candidate_reads=candidate_reads)
            # ---- 4) wr：候选读到了谁创建的版本 -------------------
            self._build_wr_edges(conn, tid, candidate_reads,
                                 new_edges, detail)

            for from_tid, to_tid, edge_type, witness in new_edges:
                graph.add_edge(from_tid, to_tid, edge_type, witness)
                self.metrics.edges_added[edge_type] += 1
                detail['edges'][edge_type] += 1

            # ---- 5) 环检测：拒绝尚未提交的候选 -------------------
            # 注意：这里不写 ABORT 状态——临界区回滚会把它一起撤销，
            # 由引擎在回滚后的新事务里持久化拒绝结果与审计事件。
            graph.assert_acyclic_with(tid)

            # ---- 6) 原子发布 --------------------------------------
            commit_seq = self._next_commit_seq(conn)
            self._publish_versions(conn, tid, commit_seq, candidate_writes)
            for from_tid, to_tid, edge_type, witness in new_edges:
                conn.execute(
                    'INSERT OR IGNORE INTO edges(from_tid,to_tid,edge_type,'
                    'witness_json,commit_seq) VALUES(?,?,?,?,?)',
                    (from_tid, to_tid, edge_type, jdump(witness), commit_seq))

            receipt = {
                'tid': tid,
                'operation_id': row['operation_id'],
                'commit_seq': commit_seq,
                'snapshot_seq': snapshot_seq,
                'read_count': len(candidate_reads),
                'write_count': len(candidate_writes),
                'edges_added': detail['edges'],
            }
            conn.execute(
                'UPDATE txns SET state=?, commit_seq=?, receipt_json=? '
                'WHERE tid=?',
                ('COMMITTED', commit_seq, jdump(receipt), tid))
            conn.execute(
                'INSERT INTO audit_journal(event_json) VALUES(?)',
                (jdump({'event': EVENT_COMMIT, **receipt}),))
            self.metrics.committed += 1
            self.metrics.read_records_scanned += detail['read_records']
            self.metrics.predicate_tests += detail['predicate_tests']
            self.metrics.write_effects_tested += detail['write_effects']
            self.metrics.record_attempt(tid, 'committed', detail)
            return receipt, graph.topological_order()

        return storage.critical_section(work, crash_hook=crash_hook)

    # ---- ww ---------------------------------------------------------
    def _check_write_write(self, conn, tid, candidate_writes,
                           later_committed, new_edges, detail):
        candidate_ids = {(w['table_name'], w['row_id']): w
                         for w in candidate_writes}
        for other in later_committed:
            other_writes = conn.execute(
                'SELECT * FROM txn_writes WHERE tid=?',
                (other['tid'],)).fetchall()
            for ow in other_writes:
                key = (ow['table_name'], ow['row_id'])
                if key in candidate_ids:
                    cw = candidate_ids[key]
                    witness = {
                        'from_tid': other['tid'], 'to_tid': tid,
                        'type': 'same_row_write',
                        'detail': f"表 {key[0]} 行 {key[1]} 同键并发写",
                        'table': key[0], 'row_id': key[1],
                        'committed_write_id': ow['wid'],
                        'candidate_write_id': cw['wid']}
                    new_edges.append((other['tid'], tid, 'ww', witness))
                    # 首提者胜：这是硬性冲突，直接拒绝候选。
                    raise WriteWriteConflict(
                        f"首提者胜: T{other['tid']} 已提交对 "
                        f"{key[0]}.{key[1]} 的写，候选 T{tid} 被拒绝",
                        witness=witness)

    # ---- 唯一键复核 -------------------------------------------------
    def _check_unique_keys(self, conn, tid, candidate_writes, detail):
        schema_by_table = self.schemas
        # 候选提交后的活键（同事务内取每个 row_id 的最后一条写）。
        final_effect = {}
        for w in candidate_writes:
            final_effect[(w['table_name'], w['row_id'])] = w
        # 先验证候选内部终态：两个不同的 row_id 不能在提交后同键。
        for table_name, schema in schema_by_table.items():
            live = {}
            for (tbl, row_id), w in final_effect.items():
                if tbl != table_name or w['op_kind'] == 'DELETE':
                    continue
                key = tuple(jload(w['key_after_json']))
                for other_id, other_key in live.items():
                    if schema.compare_keys(key, other_key) == 0:
                        raise UniquenessViolation(
                            f'候选事务内部唯一键冲突: 表 {table_name} 键 '
                            f'{list(key)} 同时属于行 {other_id} 与 {row_id}',
                            witness={'table': table_name,
                                     'key': list(key),
                                     'rows': [other_id, row_id],
                                     'type': 'intra_txn_unique_key'})
                live[row_id] = key
        for (table_name, row_id), w in final_effect.items():
            if w['op_kind'] == 'DELETE':
                continue
            after_key = jload(w['key_after_json'])
            # 候选事务自身改写的其它行不能算占用：它们在本事务提交后
            # 的最终键由 final_effect 决定（支持同事务内键交换）。
            own_rows = {rid for (tbl, rid) in final_effect if tbl == table_name}
            # 只看每行"当前最终活版本"——不是历史里的每一个版本。
            # 用子查询取每个 row_id 的最大已生效 vid，再要求它非墓碑：
            # 已迁移/已删除行的旧版本不能继续占用唯一键。
            rows = conn.execute(
                'SELECT rv.* FROM row_versions rv '
                'JOIN (SELECT row_id, MAX(vid) AS max_vid '
                '      FROM row_versions WHERE table_name=? '
                '      AND staged=0 GROUP BY row_id) latest '
                '  ON rv.vid = latest.max_vid '
                'WHERE rv.table_name=? AND rv.is_delete=0',
                (table_name, table_name)).fetchall()
            schema = schema_by_table[table_name]
            for vr in rows:
                # 候选自身（含本事务改写的行）的旧版本不算占用。
                if vr['row_id'] in own_rows:
                    continue
                existing_key = jload(vr['sort_key_json'])
                detail['predicate_tests'] += 1
                self.metrics.predicate_tests += 1
                if schema.compare_keys(tuple(existing_key),
                                       tuple(after_key)) == 0:
                    witness = {
                        'from_tid': vr['creator_tid'], 'to_tid': tid,
                        'type': 'unique_key',
                        'detail': f'表 {table_name} 唯一键 {after_key} 冲突',
                        'table': table_name,
                        'row_id': vr['row_id'], 'key': after_key}
                    raise UniquenessViolation(
                        f'唯一键冲突: 表 {table_name} 键 {after_key} '
                        f"已被 T{vr['creator_tid']} 提交的行 {vr['row_id']} 占用",
                        witness=witness)

    # ---- rw 反依赖 --------------------------------------------------
    def _build_rw_edges(self, conn, reader_tid, writer_tid,
                        writer_writes, new_edges, detail,
                        reader_is_candidate=False, candidate_reads=None):
        if reader_is_candidate:
            read_rows = candidate_reads
        else:
            read_rows = conn.execute(
                'SELECT * FROM txn_reads WHERE tid=? ORDER BY order_no',
                (reader_tid,)).fetchall()
        detail['read_records'] += len(read_rows)
        for rr in read_rows:
            predicate_data = jload(rr['predicate_json'])
            schema = self.schemas[rr['table_name']]
            predicate = predicate_from_dict(schema, predicate_data)
            observed_vids = set(jload(rr['observed_vids_json']))
            for cw in writer_writes:
                if cw['table_name'] != rr['table_name']:
                    continue
                detail['write_effects'] += 1
                self.metrics.write_effects_tested += 1
                detail['predicate_tests'] += 1
                self.metrics.predicate_tests += 1
                hit_key, hit_side = self._write_hits_predicate(
                    schema, predicate, cw)
                if hit_key is None:
                    continue
                # 关键：写者的新版本读者快照不可见才构成反依赖。
                # 读者实际所见 vid 集合里若已包含该版本，说明它读到了，
                # 那是 wr 依赖而不是 rw 反依赖。
                if cw['after_vid'] in observed_vids:
                    continue
                witness = {
                    'from_tid': reader_tid, 'to_tid': writer_tid,
                    'type': f'{predicate.kind}_predicate_miss',
                    'detail': (
                        f"T{reader_tid} 的{predicate.describe()}"
                        f"未见到 T{writer_tid} 对行 {cw['row_id']} 的写"
                        f"（{hit_side}键命中）"),
                    'table': cw['table_name'],
                    'row_id': cw['row_id'],
                    'read_id': rr['rid'], 'write_id': cw['wid'],
                    'predicate': predicate_data,
                    'hit_key': list(hit_key),
                    'empty_read': not observed_vids}
                new_edges.append((reader_tid, writer_tid, 'rw', witness))

    def _write_hits_predicate(self, schema, predicate, write_row):
        """更新迁移键时，更新前/更新后两侧键都要参与命中测试。"""
        if write_row['op_kind'] == 'DELETE':
            key = tuple(jload(write_row['key_before_json']))
            if predicate.contains(key):
                return key, '删除前'
            return None, None
        after_key = tuple(jload(write_row['key_after_json']))
        if predicate.contains(after_key):
            return after_key, '更新后/插入'
        if write_row['op_kind'] == 'UPDATE' \
                and write_row['key_before_json'] is not None:
            before_key = tuple(jload(write_row['key_before_json']))
            if predicate.contains(before_key):
                return before_key, '更新前'
        return None, None

    # ---- wr 依赖 ----------------------------------------------------
    def _build_wr_edges(self, conn, tid, candidate_reads, new_edges, detail):
        for rr in candidate_reads:
            observed_vids = jload(rr['observed_vids_json'])
            detail['read_records'] += 1
            for vid in observed_vids:
                version = conn.execute(
                    'SELECT creator_tid FROM row_versions WHERE vid=?',
                    (vid,)).fetchone()
                if version is None:
                    continue
                creator = version['creator_tid']
                if creator == tid:
                    continue  # 读己写不产生跨事务边
                witness = {
                    'from_tid': creator, 'to_tid': tid,
                    'type': 'observed_version',
                    'detail': (
                        f"T{tid} 读记录 {rr['rid']} 实际返回了 "
                        f"T{creator} 创建的版本 v{vid}"),
                    'table': rr['table_name'], 'vid': vid,
                    'read_id': rr['rid']}
                new_edges.append((creator, tid, 'wr', witness))

    # ---- 图加载与发布 ----------------------------------------------
    def _load_committed_graph(self, conn):
        graph = DependencyGraph()
        for r in conn.execute(
                "SELECT tid FROM txns WHERE state='COMMITTED'").fetchall():
            graph.add_node(r['tid'])
        for edge in conn.execute('SELECT * FROM edges').fetchall():
            graph.add_edge(edge['from_tid'], edge['to_tid'],
                           edge['edge_type'], jload(edge['witness_json']),
                           edge['edge_id'])
        return graph

    def _next_commit_seq(self, conn):
        # 临界区本已串行化，但用单条原子 UPDATE 更稳妥（多进程也正确）。
        row = conn.execute(
            "UPDATE meta SET value = CAST(CAST(value AS INTEGER) + 1 "
            "AS TEXT) WHERE key='commit_seq' RETURNING value").fetchone()
        return int(row['value'])

    def _publish_versions(self, conn, tid, commit_seq, candidate_writes):
        # 同一事务的版本按 wid 顺序生效，前驱链已在暂存时记录。
        vids = [w['after_vid'] for w in candidate_writes]
        if not vids:
            return
        placeholders = ','.join('?' for _ in vids)
        conn.execute(
            f'UPDATE row_versions SET create_seq=?, staged=0 '
            f'WHERE vid IN ({placeholders})',
            [commit_seq, *vids])
