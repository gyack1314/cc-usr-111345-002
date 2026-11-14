"""读取谓词：稳定主键点读与类型明确的复合键区间读。

关键语义：
- UNBOUND 是槽位级哨兵，区别于真实值 None（NULL 本身是合法键值）；
- 区间按复合键字典序判定，include_lower/include_upper 决定两端开闭；
- 谓词独立于返回行存在：空结果的读取同样登记谓词，幻行认证靠它；
- 谓词的 JSON 表示持久化到 SQLite，认证与独立检查器都从同一表示重建。
"""
from .errors import ConstraintViolation


class _Unbound:
    """槽位哨兵：该键列方向上无边界。"""
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self):
        return 'UNBOUND'

    def __bool__(self):
        return False


UNBOUND = _Unbound()


def _validate_slot(schema, column_name, value):
    if value is UNBOUND:
        return UNBOUND
    col = schema.by_name[column_name]
    return col.check_value(value)


class PointPredicate:
    """稳定主键点读：谓词就是一个完整复合键。"""

    kind = 'point'

    def __init__(self, schema, key):
        if len(key) != len(schema.key_columns):
            raise ConstraintViolation('点读键长度与 schema 键列不符')
        self.schema = schema
        self.key = tuple(
            _validate_slot(schema, name, value)
            for name, value in zip(schema.key_columns, key))
        if any(v is UNBOUND for v in self.key):
            raise ConstraintViolation('点读键不能包含 UNBOUND 槽位')

    def contains(self, key):
        return self.schema.compare_keys(self.key, key) == 0

    def to_dict(self):
        return {'kind': 'point', 'table': self.schema.name, 'key': list(self.key)}

    @classmethod
    def from_dict(cls, schema, data):
        return cls(schema, tuple(data['key']))

    def describe(self):
        return f"点读 {self.schema.name}{list(self.key)}"

    def __repr__(self):
        return f'PointPredicate({self.schema.name}, {list(self.key)})'


class RangePredicate:
    """复合键区间读，逐槽位边界 + 整体开闭标志。

    字典序判定示例（两列键 (tenant, lower)）：下界 ('A', 10) 闭区间意味着
    所有 ('A', >=10) 以及所有 (>'A', 任意) 都在区间内，('A', <10) 不在。
    """

    kind = 'range'

    def __init__(self, schema, lower=None, upper=None,
                 include_lower=True, include_upper=False):
        width = len(schema.key_columns)
        lower = tuple(UNBOUND if _is_unbound(v) else v
                      for v in (lower if lower is not None else (UNBOUND,) * width))
        upper = tuple(UNBOUND if _is_unbound(v) else v
                      for v in (upper if upper is not None else (UNBOUND,) * width))
        if len(lower) != width or len(upper) != width:
            raise ConstraintViolation('区间边界宽度与 schema 键列不符')
        self.schema = schema
        self.lower = tuple(
            _validate_slot(schema, name, value)
            for name, value in zip(schema.key_columns, lower))
        self.upper = tuple(
            _validate_slot(schema, name, value)
            for name, value in zip(schema.key_columns, upper))
        self.include_lower = bool(include_lower)
        self.include_upper = bool(include_upper)
        # 双闭区间且上下界相同是合法的（单键区间）；下界>上界则拒绝。
        if all(v is not UNBOUND for v in self.lower) and \
                all(v is not UNBOUND for v in self.upper):
            cmp = schema.compare_keys(self.lower, self.upper)
            if cmp > 0 or (cmp == 0 and not (self.include_lower and self.include_upper)):
                raise ConstraintViolation('区间下界不小于上界，或等值键却使用开区间')

    def _above_lower(self, key):
        """字典序 key >= lower（按 include_lower 决定等号）。

        逐列扫描：第一个非 UNBOUND 且不等的列决定大小；若所有绑定列都
        相等，则前缀匹配成立（剩余列任意，含 NULL，都不会更小）。
        一旦某列遇到 UNBOUND，说明 lower 在该方向上不再约束，整体成立。
        """
        for name, bound, value in zip(self.schema.key_columns, self.lower, key):
            if bound is UNBOUND:
                return True
            cmp = self.schema.compare_scalar(name, value, bound)
            if cmp > 0:
                return True
            if cmp < 0:
                return False
            # cmp == 0：继续比较后续列。
        return self.include_lower

    def _below_upper(self, key):
        """字典序 key <= upper（按 include_upper 决定等号）。"""
        for name, bound, value in zip(self.schema.key_columns, self.upper, key):
            if bound is UNBOUND:
                return True
            cmp = self.schema.compare_scalar(name, value, bound)
            if cmp < 0:
                return True
            if cmp > 0:
                return False
        return self.include_upper

    def contains(self, key):
        if len(key) != len(self.schema.key_columns):
            raise ConstraintViolation('待判定键宽度与 schema 不符')
        return self._above_lower(key) and self._below_upper(key)

    def to_dict(self):
        return {
            'kind': 'range', 'table': self.schema.name,
            'lower': [None if v is UNBOUND else v for v in self.lower],
            'upper': [None if v is UNBOUND else v for v in self.upper],
            'lower_unbound': [v is UNBOUND for v in self.lower],
            'upper_unbound': [v is UNBOUND for v in self.upper],
            'include_lower': self.include_lower,
            'include_upper': self.include_upper,
        }

    @classmethod
    def from_dict(cls, schema, data):
        def restore(values, unbound_flags):
            return tuple(UNBOUND if flag else value
                         for value, flag in zip(values, unbound_flags))
        return cls(
            schema,
            lower=restore(data['lower'], data['lower_unbound']),
            upper=restore(data['upper'], data['upper_unbound']),
            include_lower=data['include_lower'],
            include_upper=data['include_upper'])

    def describe(self):
        lo = '(' if not self.include_lower else '['
        hi = ')' if not self.include_upper else ']'
        show = lambda t: ['*' if v is UNBOUND else v for v in t]
        return f"区间 {self.schema.name} {lo}{show(self.lower)} .. {show(self.upper)}{hi}"

    def __repr__(self):
        return (f'RangePredicate({self.schema.name}, {self.describe()})')


def _is_unbound(value):
    return value is UNBOUND


def predicate_from_dict(schema, data):
    """从持久化表示重建谓词（认证器与独立检查器共用的纯函数）。"""
    if data['kind'] == 'point':
        return PointPredicate.from_dict(schema, data)
    if data['kind'] == 'range':
        return RangePredicate.from_dict(schema, data)
    raise ConstraintViolation(f"未知谓词类型: {data['kind']}")
