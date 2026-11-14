"""serlake：本地湖仓的可串行化事务认证引擎。

模块分层：
- schema/predicate：与 schema 绑定的键序、null 位置、区间开闭语义；
- storage：SQLite 元数据 + 不可变数据文件的多版本存储；
- transaction：冻结快照上的点读/区间读/插入/更新/删除与读写足迹；
- graph/certifier：读写依赖图（ww/wr/rw）、环认证与原子提交；
- engine：门面、保留预算、回收与崩溃重放；
- journal/checker：只追加审计日志与独立历史检查器。
"""
from .errors import (
    SerlakeError,
    ConflictAbort,
    WriteWriteConflict,
    UniquenessViolation,
    SerializationCycle,
    RetentionBudgetExceeded,
    TransactionStateError,
    ConstraintViolation,
)
from .schema import Column, Schema
from .predicate import PointPredicate, RangePredicate, UNBOUND
from .engine import Lakehouse, CommitReceipt

__all__ = [
    'Lakehouse', 'CommitReceipt', 'Column', 'Schema',
    'PointPredicate', 'RangePredicate', 'UNBOUND',
    'SerlakeError', 'ConflictAbort', 'WriteWriteConflict',
    'UniquenessViolation', 'SerializationCycle',
    'RetentionBudgetExceeded', 'TransactionStateError',
    'ConstraintViolation',
]
