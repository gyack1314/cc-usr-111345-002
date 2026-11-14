"""事务对象：冻结多表快照上的读/写 API 与读写足迹。

可见性规则：
- begin 时记录 snapshot_seq（全局提交序号），事务全程基于该冻结快照；
- 事务内读：先看本事务暂存写（读己写），未命中再看快照可见版本；
- 外部读者通过独立连接的快照读，只能看到 create_seq 已生效的完整版本；
- 每次读都把谓词与所见 vid 集合登记进 txn_reads——空结果也登记，
  谓词而不是返回主键才是幻行认证的依据。
"""
import functools

from .errors import (
    ConstraintViolation, TransactionStateError, SerlakeError)
from .predicate import PointPredicate, RangePredicate
from .storage import jload
from .journal import EVENT_READ, EVENT_WRITE


def _key_cmp(schema):
    """functools.cmp_to_key 适配器：按 schema 绑定的复合键字典序。"""
    def cmp(a, b):
        return schema.compare_keys(schema.sort_key(a), schema.sort_key(b))
    return functools.cmp_to_key(cmp)


class Transaction:
    def __init__(self, engine, tid, snapshot_seq, operation_id):
        self.engine = engine
        self.storage = engine.storage
        self.schemas = engine.schemas
        self.tid = tid
        self.snapshot_seq = snapshot_seq
        self.operation_id = operation_id
        self.state = 'ACTIVE'

        self._read_counter = 0
        self._write_counter = 0
        # row_id -> {'op','vid','row','key_before','key_after','before_vid'}
        self._own_writes = {}
        # 快照物化缓存：table -> {row_id: row_versions 行}
        self._snapshot_cache = {}
        # 写后最终活键集合（用于本事务内唯一键检查）
        self._live_keys = {}

    # ---- 内部工具 ---------------------------------------------------
    def _schema(self, table_name):
        if table_name not in self.schemas:
            raise SerlakeError(f'未知表: {table_name}')
        return self.schemas[table_name]

    def _check_active(self):
        if self.state != 'ACTIVE':
            raise TransactionStateError(
                f'事务 T{self.tid} 已 {self.state}，不能继续读写')

    def _snapshot_table(self, table_name):
        if table_name not in self._snapshot_cache:
            self._snapshot_cache[table_name] = \
                self.storage.visible_versions(self.snapshot_seq, table_name)
        return self._snapshot_cache[table_name]

    def _load_row(self, version_row):
        payload = self.storage.read_version_file(version_row['data_path'])
        if payload.get('__tombstone__'):
            return None
        return payload

    def _current(self, table_name, row_id):
        """返回 (row_or_None, 来源描述)；来源为 ('own', 记录) 或 ('snapshot', 版本行)。"""
        own = self._own_writes.get((table_name, row_id))
        if own is not None:
            return own['row'], ('own', own)
        version_row = self._snapshot_table(table_name).get(row_id)
        if version_row is None:
            return None, ('snapshot', None)
        return self._load_row(version_row), ('snapshot', version_row)

    def _register_read(self, table_name, predicate, observed, audit_kind):
        self._read_counter += 1
        order_no = self._read_counter
        vids = [v['vid'] for v in observed]
        row_ids = [v['row_id'] for v in observed]
        self.storage.record_read(
            self.tid, order_no, table_name, predicate.to_dict(), vids, row_ids)
        self.storage.append_audit({
            'event': EVENT_READ, 'tid': self.tid, 'order_no': order_no,
            'kind': audit_kind, 'table': table_name,
            'predicate': predicate.to_dict(),
            'observed_vids': vids, 'observed_row_ids': row_ids,
            'empty': not observed})

    # ---- 读 API -----------------------------------------------------
    def point_read(self, table_name, key):
        """稳定主键点读，返回完整行或 None；None 同样登记为一次读取。"""
        self._check_active()
        schema = self._schema(table_name)
        predicate = PointPredicate(schema, tuple(key))
        observed = []
        result_row = None

        # 先在本事务写集合里按键找（行身份与键分离，必须遍历同表暂存写）。
        own_hit = None
        for (table, row_id), write in self._own_writes.items():
            if table == table_name and write['row'] is not None \
                    and predicate.contains(schema.sort_key(write['row'])):
                own_hit = (row_id, write)
                break
        if own_hit is not None:
            row_id, write = own_hit
            observed.append({'vid': write['vid'], 'row_id': row_id})
            result_row = write['row']
        else:
            for row_id, version_row in self._snapshot_table(table_name).items():
                row = self._load_row(version_row)
                if row is None:
                    continue
                if predicate.contains(schema.sort_key(row)):
                    observed.append({'vid': version_row['vid'],
                                     'row_id': row_id})
                    result_row = row
                    break
        self._register_read(table_name, predicate, observed, 'point')
        return None if result_row is None else dict(result_row)

    def range_read(self, table_name, lower=None, upper=None,
                   include_lower=True, include_upper=False):
        """复合键区间读，返回按 schema 键序排序的完整行列表（可为空列表）。"""
        self._check_active()
        schema = self._schema(table_name)
        predicate = RangePredicate(
            schema, lower=lower, upper=upper,
            include_lower=include_lower, include_upper=include_upper)
        observed = []
        rows_out = []

        own_by_id = {row_id: write
                     for (table, row_id), write in self._own_writes.items()
                     if table == table_name}
        snapshot = self._snapshot_table(table_name)

        # 快照行：本事务已写过的 row_id 一律以暂存写为准，不能再看旧版本
        # （否则更新把键移出区间、或删除后，旧行仍会被错误返回）。
        for row_id, version_row in snapshot.items():
            if row_id in own_by_id:
                continue
            key = tuple(jload(version_row['sort_key_json']))
            if not predicate.contains(key):
                continue
            row = self._load_row(version_row)
            if row is not None:
                observed.append({'vid': version_row['vid'], 'row_id': row_id})
                rows_out.append(row)

        # 本事务自己的写：按写后行为准（插入/更新命中则加入，删除不加入）。
        for row_id, write in own_by_id.items():
            if write['row'] is None:
                continue
            key = schema.sort_key(write['row'])
            if not predicate.contains(key):
                continue
            observed.append({'vid': write['vid'], 'row_id': row_id})
            rows_out.append(write['row'])

        rows_out = sorted(rows_out, key=_key_cmp(schema))
        observed.sort(key=lambda item: item['row_id'])
        self._register_read(table_name, predicate, observed, 'range')
        return [dict(r) for r in rows_out]

    # ---- 写 API -----------------------------------------------------
    def insert(self, table_name, row):
        """完整行插入；行身份在表内必须不存在于快照与本事务写集合。"""
        self._check_active()
        schema = self._schema(table_name)
        clean = schema.validate_row(row)
        row_id = schema.row_id(clean)
        key = schema.sort_key(clean)
        if (table_name, row_id) in self._own_writes:
            raise ConstraintViolation(
                f'行 {row_id} 在本事务内已有待提交写，不能重复插入')
        snapshot_row = self._snapshot_table(table_name).get(row_id)
        if snapshot_row is not None and not snapshot_row['is_delete']:
            raise ConstraintViolation(
                f'插入违反行身份唯一: 表 {table_name} 行 {row_id} 已存在')
        self._check_unique_key(table_name, schema, key, exclude_row=row_id)
        return self._stage_write('INSERT', table_name, row_id, clean,
                                 before_vid=(snapshot_row['vid']
                                             if snapshot_row else None),
                                 key_before=None, key_after=key)

    def update(self, table_name, row_id, new_row):
        """完整行更新（必须提交完整新像，不支持局部补丁）。"""
        self._check_active()
        schema = self._schema(table_name)
        clean = schema.validate_row(new_row)
        if schema.row_id(clean) != row_id:
            raise ConstraintViolation('更新不能改变行身份列')
        current, source = self._current(table_name, row_id)
        if current is None:
            raise ConstraintViolation(
                f'更新目标不存在: 表 {table_name} 行 {row_id}')
        key_before = schema.sort_key(current)
        key_after = schema.sort_key(clean)
        if key_before != key_after:
            self._check_unique_key(table_name, schema, key_after,
                                   exclude_row=row_id)
        # 前驱版本：暂存写的 vid 或快照可见版本的 vid（两者都有 'vid'）。
        before_vid = source[1]['vid']
        return self._stage_write('UPDATE', table_name, row_id, clean,
                                 before_vid=before_vid,
                                 key_before=key_before, key_after=key_after)

    def delete(self, table_name, row_id):
        """完整行删除，产生墓碑版本。"""
        self._check_active()
        schema = self._schema(table_name)
        current, source = self._current(table_name, row_id)
        if current is None:
            raise ConstraintViolation(
                f'删除目标不存在: 表 {table_name} 行 {row_id}')
        key_before = schema.sort_key(current)
        tombstone = None
        before_vid = source[1]['vid']
        return self._stage_write('DELETE', table_name, row_id, tombstone,
                                 before_vid=before_vid,
                                 key_before=key_before, key_after=None)

    def _check_unique_key(self, table_name, schema, key, exclude_row):
        # 本事务已写过的行以暂存写为准，其快照旧版本不能继续算作占用。
        own_rows = {row_id for (table, row_id) in self._own_writes
                    if table == table_name}
        # 快照活键（排除本事务已改写的行）。
        for row_id, version_row in self._snapshot_table(table_name).items():
            if row_id == exclude_row or row_id in own_rows \
                    or version_row['is_delete']:
                continue
            existing_key = tuple(jload(version_row['sort_key_json']))
            if schema.compare_keys(existing_key, key) == 0:
                raise ConstraintViolation(
                    f'唯一键冲突: 表 {table_name} 键 {list(key)} '
                    f'已被行 {row_id} 占用')
        # 本事务暂存活键。
        for (table, row_id), write in self._own_writes.items():
            if table != table_name or row_id == exclude_row \
                    or write['row'] is None:
                continue
            if schema.compare_keys(schema.sort_key(write['row']), key) == 0:
                raise ConstraintViolation(
                    f'唯一键冲突（事务内）: 表 {table_name} 键 {list(key)} '
                    f'已被行 {row_id} 占用')

    def _stage_write(self, op_kind, table_name, row_id, clean,
                     before_vid, key_before, key_after):
        is_delete = op_kind == 'DELETE'
        sort_key = key_before if is_delete else key_after
        vid = self.storage.add_staged_version(
            self.tid, table_name, row_id,
            full_row=clean, is_delete=is_delete,
            predecessor_vid=before_vid, sort_key=sort_key)
        self._write_counter += 1
        self.storage.record_write(
            self.tid, self._write_counter, table_name, row_id, op_kind,
            before_vid, vid, key_before, key_after)
        self._own_writes[(table_name, row_id)] = {
            'op': op_kind, 'vid': vid, 'row': clean,
            'key_before': key_before, 'key_after': key_after,
            'before_vid': before_vid}
        self.storage.append_audit({
            'event': EVENT_WRITE, 'tid': self.tid,
            'order_no': self._write_counter, 'op': op_kind,
            'table': table_name, 'row_id': row_id, 'vid': vid,
            'key_before': None if key_before is None else list(key_before),
            'key_after': None if key_after is None else list(key_after)})
        return vid

    # ---- 结束 -------------------------------------------------------
    def mark_finished(self, state):
        self.state = state

    @property
    def is_readonly(self):
        return not self._own_writes
