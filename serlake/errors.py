"""引擎异常体系。所有拒绝都是显式的，不存在静默降级。"""


class SerlakeError(Exception):
    """引擎基础异常。"""


class TransactionStateError(SerlakeError):
    """事务状态机非法转换（例如对已结束事务继续读写）。"""


class ConstraintViolation(SerlakeError):
    """schema/键/行结构约束被违反。"""


class ConflictAbort(SerlakeError):
    """认证拒绝的基类，携带见证信息。"""

    def __init__(self, message, witness=None):
        super().__init__(message)
        self.witness = witness or {}


class WriteWriteConflict(ConflictAbort):
    """同键并发写，首提者胜（first-committer-wins）策略拒绝候选。"""


class UniquenessViolation(ConflictAbort):
    """提交时发现候选的活键与已提交行的唯一键冲突。"""


class SerializationCycle(ConflictAbort):
    """依赖图成环，无可串行化次序；携带完整事务链与每边见证。"""

    def __init__(self, message, chain, edges):
        super().__init__(message, witness={'chain': chain, 'edges': edges})
        self.chain = chain
        self.chain_edges = edges


class RetentionBudgetExceeded(SerlakeError):
    """保留预算不足：必须显式拒绝或终止可终止事务，不能降低隔离级别。"""

    def __init__(self, message, footprint, budget):
        super().__init__(message)
        self.footprint = footprint
        self.budget = budget
