"""独立历史检查器。

独立性体现在：
- 不读取 edges 表、不调用 graph/certifier 的任何函数，认证器的结论一概不复用；
- 输入只有：冻结初态、audit_journal 事件流、不可变版本文件；
- 自己实现状态重放与读谓词求值（仅复用无策略的 schema/predicate 纯数据类型），
  对全部已提交事务枚举串行排列（小历史全排列），逐事务按其内部操作顺序重放，
  校验每次点读/区间读的返回身份集合与完整行内容完全一致。

一个排列要成为合法串行次序，必须满足：
1. 每个事务的读结果（含空结果）与它在并发历史中实际记录的一致；
2. 全部写重放后，终态与湖仓当前已发布状态逐行相等。
被环拒绝的事务则必须验证：不存在任何满足条件 1 的排列（拒绝才是正确的）。
"""
import itertools
from collections import defaultdict

from .predicate import predicate_from_dict
from .journal import (
    EVENT_BEGIN, EVENT_READ, EVENT_WRITE, EVENT_COMMIT, EVENT_ABORT)


class _Missing:
    """哨兵：审计事件引用的版本文件已不存在（仅可能来自被中止事务）。"""


_MISSING = _Missing()


class CheckerVerdict:
    def __init__(self):
        self.legal_orders = []
        self.failures = []
        self.checked_permutations = 0
        self.pruned_permutations = 0
        self.abort_checks = []

    def to_dict(self):
        return {
            'legal_orders': self.legal_orders,
            'failures': self.failures,
            'checked_permutations': self.checked_permutations,
            'pruned_permutations': self.pruned_permutations,
            'abort_checks': self.abort_checks}


class HistoryChecker:
    def __init__(self, engine):
        self.engine = engine
        self.storage = engine.storage
        self.schemas = engine.schemas

    # ---- 事件分组 ---------------------------------------------------
    def _group_transactions(self):
        events = self.storage.audit_events()
        grouped = defaultdict(list)
        begin_order = []
        for event in events:
            tid = event.get('tid')
            if tid is None or event['event'] in ('gc', 'terminate'):
                continue
            if event['event'] == EVENT_BEGIN and tid not in grouped:
                begin_order.append(tid)
            grouped[tid].append(event)
        return begin_order, grouped

    def _initial_state(self):
        """冻结初态：快照 seq=0 下各表 row_id -> 完整行。"""
        state = {}
        for table_name in self.schemas:
            table = {}
            for row_id, version in self.storage.visible_versions(
                    0, table_name).items():
                if not version['is_delete']:
                    table[row_id] = self.storage.read_version_file(
                        version['data_path'])
            state[table_name] = table
        return state

    # ---- 单事务串行重放 --------------------------------------------
    def _replay_in_state(self, tid, txn_events, state):
        """在给定串行状态上按事件顺序重放一个事务。

        返回 (ok, mismatch)；写直接作用于 state 的副本由调用方管理，
        读不修改 state。写校验（身份存在/唯一键）也独立检查一遍。
        """
        for event in txn_events:
            kind = event['event']
            if kind == EVENT_READ:
                ok, mismatch = self._replay_read(event, state)
                if not ok:
                    return False, mismatch
            elif kind == EVENT_WRITE:
                ok, mismatch = self._replay_write(event, state)
                if not ok:
                    return False, mismatch
            # begin/commit/abort 不改变串行状态。
        return True, None

    def _replay_read(self, event, state):
        table_name = event['table']
        schema = self.schemas[table_name]
        predicate = predicate_from_dict(schema, event['predicate'])
        table = state.get(table_name, {})
        matched = {}
        for row_id, row in table.items():
            if predicate.contains(schema.sort_key(row)):
                matched[row_id] = row
        expected_ids = set(event['observed_row_ids'])
        actual_ids = set(matched)
        if expected_ids != actual_ids:
            return False, {
                'tid': event['tid'], 'read_id': event['order_no'],
                'reason': '返回行身份集合与串行重放不一致',
                'expected': sorted(expected_ids),
                'actual': sorted(actual_ids),
                'predicate': predicate.describe(),
                'recorded_empty': event.get('empty')}
        # 完整行内容也要一致（不只是主键）。版本已被清理的中止事务
        # 只读场景下跳过内容比对，身份集合检查已经覆盖幻行问题。
        for row_id in matched:
            recorded = self._recorded_row(event, row_id)
            if recorded is _MISSING:
                continue
            if recorded is not None and recorded != matched[row_id]:
                return False, {
                    'tid': event['tid'], 'read_id': event['order_no'],
                    'reason': '返回行内容与串行重放不一致',
                    'row_id': row_id}
        # 空结果读取必须在串行次序下同样为空。
        if event.get('empty') and actual_ids:
            return False, {
                'tid': event['tid'], 'read_id': event['order_no'],
                'reason': '记录为空读取，串行次序下却存在匹配行（幻行）'}
        return True, None

    def _recorded_row(self, read_event, row_id):
        """通过 observed vid 找回读取时实际返回的完整行。"""
        events_order = read_event['observed_row_ids']
        vids = read_event['observed_vids']
        if row_id not in events_order:
            return None
        vid = vids[events_order.index(row_id)]
        version = self.storage.get_version(vid)
        # 被中止事务的暂存版本在重开时由 reconcile 清理；
        # 已提交事务的版本必须仍在，否则历史本身已损坏。
        if version is None:
            return _MISSING
        if version['is_delete']:
            return None
        return self.storage.read_version_file(version['data_path'])

    def _replay_write(self, event, state):
        table_name = event['table']
        row_id = event['row_id']
        table = state.setdefault(table_name, {})
        op = event['op']
        version = self.storage.get_version(event['vid'])
        if version is None:
            # 被中止事务的暂存版本已被 reconcile 清理：无法取得完整行，
            # 这种事务只参与"拒绝是否正确"的可行性分析。
            return False, {'tid': event['tid'],
                           'reason': f'版本 v{event["vid"]} 已随中止清理'}
        payload = self.storage.read_version_file(version['data_path'])
        if op == 'INSERT':
            if row_id in table:
                return False, {'tid': event['tid'],
                               'reason': f'串行重放插入时行 {row_id} 已存在'}
            table[row_id] = payload
        elif op == 'UPDATE':
            if row_id not in table:
                return False, {'tid': event['tid'],
                               'reason': f'串行重放更新时行 {row_id} 不存在'}
            table[row_id] = payload
        elif op == 'DELETE':
            if row_id not in table:
                return False, {'tid': event['tid'],
                               'reason': f'串行重放删除时行 {row_id} 不存在'}
            del table[row_id]
        return True, None

    # ---- 全排列枚举 -------------------------------------------------
    def verify(self, max_permutations=5040):
        verdict = CheckerVerdict()
        begin_order, grouped = self._group_transactions()
        committed = [tid for tid in begin_order
                     if any(e['event'] == EVENT_COMMIT for e in grouped[tid])]
        aborted = [tid for tid in begin_order
                   if any(e['event'] == EVENT_ABORT for e in grouped[tid])]
        base_state = self._initial_state()

        if len(committed) > 0:
            import math
            total = math.factorial(len(committed))
            if total > max_permutations:
                verdict.failures.append({
                    'reason': '历史规模超过检查器枚举上限',
                    'transactions': len(committed),
                    'permutations': total})
                return verdict

        for perm in itertools.permutations(committed):
            verdict.checked_permutations += 1
            state = {table: dict(rows) for table, rows in base_state.items()}
            ok = True
            mismatch = None
            for tid in perm:
                ok, mismatch = self._replay_in_state(
                    tid, grouped[tid], state)
                if not ok:
                    verdict.pruned_permutations += 1
                    break
            if ok:
                order = [f'T{tid}' for tid in perm]
                # 终态必须与当前已发布状态逐行相等。
                final_ok, final_diff = self._compare_final_state(state)
                if final_ok:
                    verdict.legal_orders.append(order)
                else:
                    verdict.failures.append({
                        'reason': '排列满足读取但终态与已发布状态不符',
                        'order': order, 'diff': final_diff})

        if not verdict.legal_orders and committed:
            verdict.failures.append({
                'reason': '已提交历史不存在任何合法串行次序！'})

        # 被拒绝事务分两类核验：
        # - serialization_cycle：必须证明任何串行位置都不可行；
        # - write_write_conflict / uniqueness_violation：首提者胜等显式
        #   冲突策略，本就可能存在数学上的串行次序，策略正确性由
        #   "已提交版本确实占用该键/该行"这一事实单独核验。
        for tid in aborted:
            abort_event = next(e for e in grouped[tid]
                               if e['event'] == EVENT_ABORT)
            reason = abort_event.get('reason')
            check = {'tid': tid, 'reason': reason}
            if reason == 'serialization_cycle':
                feasible = self._could_serialize(
                    tid, grouped[tid], committed, grouped, base_state)
                check['rejection_correct'] = not feasible
                if feasible:
                    check['detail'] = '存在可串行化插入位置，拒绝可能误判'
                    verdict.failures.append(check)
            else:
                check['policy_reject'] = True
                check['rejection_correct'] = self._verify_policy_conflict(
                    tid, grouped[tid], reason, state_after_commit=base_state)
                if not check['rejection_correct']:
                    verdict.failures.append(check)
            verdict.abort_checks.append(check)
        return verdict

    def _verify_policy_conflict(self, tid, txn_events, reason,
                                state_after_commit):
        """策略拒绝核验：被拒写针对的键/行确实已被某个已提交版本占用。"""
        write_events = [e for e in txn_events if e['event'] == EVENT_WRITE]
        if not write_events:
            return False
        seq = self.storage.current_commit_seq()
        for event in write_events:
            table_name = event['table']
            row_id = event['row_id']
            if reason == 'write_write_conflict':
                # 同 row_id 在已发布状态中存在即说明首提者已占位。
                published = self.storage.visible_versions(seq, table_name)
                if row_id not in published or published[row_id]['is_delete']:
                    return False
            elif reason == 'uniqueness_violation':
                # 不同 row_id 但同排序键：在已发布活行里找同键他行。
                schema = self.schemas[table_name]
                target_key = event['key_after']
                for published_id, version in \
                        self.storage.visible_versions(seq, table_name).items():
                    if published_id == row_id or version['is_delete']:
                        continue
                    import json as _json
                    existing_key = _json.loads(version['sort_key_json'])
                    if schema.compare_keys(
                            tuple(existing_key), tuple(target_key)) == 0:
                        break
                else:
                    return False
        return True

    def _could_serialize(self, candidate, candidate_events,
                         committed, grouped, base_state):
        """被中止事务是否本可以串行化：枚举插入到已提交序列的每个位置。"""
        for position in range(len(committed) + 1):
            order = list(committed[:position]) + [candidate] + \
                    list(committed[position:])
            state = {table: dict(rows) for table, rows in base_state.items()}
            ok = True
            for tid in order:
                ok, _ = self._replay_in_state(
                    tid, grouped[tid], state)
                if not ok:
                    break
            if ok:
                return True
        return False

    def _compare_final_state(self, replayed_state):
        seq = self.storage.current_commit_seq()
        for table_name, schema in self.schemas.items():
            published = {}
            for row_id, version in self.storage.visible_versions(
                    seq, table_name).items():
                if not version['is_delete']:
                    published[row_id] = self.storage.read_version_file(
                        version['data_path'])
            if set(published) != set(replayed_state.get(table_name, {})):
                return False, {
                    'table': table_name,
                    'missing': sorted(set(published) -
                                      set(replayed_state.get(table_name, {}))),
                    'extra': sorted(set(replayed_state.get(table_name, {})) -
                                    set(published))}
            for row_id, row in published.items():
                if row != replayed_state[table_name][row_id]:
                    return False, {
                        'table': table_name, 'row_id': row_id,
                        'reason': '行内容不一致'}
        return True, None
