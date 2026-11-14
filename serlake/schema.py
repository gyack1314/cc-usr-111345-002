"""表 schema：列类型、复合稳定主键、null 位置与排序键全部与 schema 绑定。

设计约定：
- 每张表有一个不可变的行身份列（默认 "id"，类型必须为 TEXT），行 id 不随更新改变；
- key_columns 组成类型明确的复合排序键，其值可以被更新（键迁移，见 predicate 测试）；
- nulls_pos 全表统一：'first' 时 NULL 排在任何非空值之前，'last' 时排在之后；
- 比较是逐列字典序，逐列类型在 schema 里声明，不依赖 Python 隐式跨类型比较。
"""
from .errors import ConstraintViolation

# 支持的列类型与 Python 承载类型的对应关系。
COLUMN_TYPES = {
    'TEXT': str,
    'INTEGER': int,
    'REAL': (int, float),
    'BOOLEAN': bool,
}


class Column:
    """一列的声明：名字、类型、是否可空。"""

    __slots__ = ('name', 'col_type', 'nullable')

    def __init__(self, name, col_type, nullable=True):
        if col_type not in COLUMN_TYPES:
            raise ConstraintViolation(f'未知列类型: {col_type}')
        if not isinstance(name, str) or not name:
            raise ConstraintViolation('列名必须是非空字符串')
        self.name = name
        self.col_type = col_type
        self.nullable = nullable

    def check_value(self, value):
        """校验并归一化单列值（拒绝 bool 与 int 混用等隐式情况）。"""
        if value is None:
            if not self.nullable:
                raise ConstraintViolation(f'列 {self.name} 不可为 NULL')
            return None
        expected = COLUMN_TYPES[self.col_type]
        # bool 是 int 的子类：INTEGER 列不接受 bool，BOOLEAN 列只接受 bool。
        if self.col_type == 'INTEGER' and isinstance(value, bool):
            raise ConstraintViolation(f'列 {self.name} 需要 INTEGER，收到 BOOLEAN')
        if self.col_type == 'BOOLEAN':
            if not isinstance(value, bool):
                raise ConstraintViolation(f'列 {self.name} 需要 BOOLEAN')
        elif not isinstance(value, expected):
            raise ConstraintViolation(
                f'列 {self.name} 需要 {self.col_type}，收到 {type(value).__name__}')
        return value


class Schema:
    """一张表的完整 schema 与键工具。"""

    def __init__(self, name, columns, key_columns, identity_column='id',
                 nulls_pos='last'):
        if nulls_pos not in ('first', 'last'):
            raise ConstraintViolation("nulls_pos 只能是 'first' 或 'last'")
        by_name = {}
        for col in columns:
            if not isinstance(col, Column):
                raise ConstraintViolation('columns 必须是 Column 列表')
            if col.name in by_name:
                raise ConstraintViolation(f'列名重复: {col.name}')
            by_name[col.name] = col
        if identity_column not in by_name:
            raise ConstraintViolation(f'行身份列 {identity_column} 不在 schema 中')
        id_col = by_name[identity_column]
        if id_col.col_type != 'TEXT' or id_col.nullable:
            raise ConstraintViolation('行身份列必须是非空 TEXT')
        if not key_columns:
            raise ConstraintViolation('至少需要一个键列')
        for key in key_columns:
            if key not in by_name:
                raise ConstraintViolation(f'键列 {key} 不在 schema 中')
            if key == identity_column:
                raise ConstraintViolation('行身份列不得兼任排序键列')
        self.name = name
        self.columns = tuple(columns)
        self.column_names = tuple(col.name for col in columns)
        self.by_name = by_name
        self.key_columns = tuple(key_columns)
        self.identity_column = identity_column
        self.nulls_pos = nulls_pos

    # ---- 行结构校验 -------------------------------------------------
    def validate_row(self, row):
        """完整行插入/更新校验：字段集合必须恰好等于 schema 列集合。"""
        if not isinstance(row, dict):
            raise ConstraintViolation('行必须是对象')
        if set(row) != set(self.column_names):
            raise ConstraintViolation(
                f'表 {self.name} 行字段集合不符: '
                f'期望 {sorted(self.column_names)}，实际 {sorted(row)}')
        clean = {}
        for col in self.columns:
            clean[col.name] = col.check_value(row[col.name])
        if not clean[self.identity_column]:
            raise ConstraintViolation('行身份不能为空字符串')
        return clean

    def row_id(self, row):
        return row[self.identity_column]

    def sort_key(self, row):
        """抽取复合排序键（tuple，含 None 槽位）。"""
        return tuple(row[k] for k in self.key_columns)

    # ---- 与 schema 绑定的逐列比较 -----------------------------------
    def compare_scalar(self, column_name, a, b):
        """单列比较，a/b 均可能为 None。返回 -1/0/1。"""
        if a is None and b is None:
            return 0
        if a is None:
            return -1 if self.nulls_pos == 'first' else 1
        if b is None:
            return 1 if self.nulls_pos == 'first' else -1
        if a < b:
            return -1
        if a > b:
            return 1
        return 0

    def compare_keys(self, a, b):
        """复合键字典序比较；a、b 是与 key_columns 等长的 tuple。"""
        if len(a) != len(self.key_columns) or len(b) != len(self.key_columns):
            raise ConstraintViolation('复合键长度与 schema 不符')
        for column_name, av, bv in zip(self.key_columns, a, b):
            cmp = self.compare_scalar(column_name, av, bv)
            if cmp != 0:
                return cmp
        return 0

    def to_dict(self):
        return {
            'name': self.name,
            'columns': [
                {'name': c.name, 'type': c.col_type, 'nullable': c.nullable}
                for c in self.columns],
            'key_columns': list(self.key_columns),
            'identity_column': self.identity_column,
            'nulls_pos': self.nulls_pos,
        }

    @classmethod
    def from_dict(cls, data):
        return cls(
            data['name'],
            [Column(c['name'], c['type'], c['nullable']) for c in data['columns']],
            data['key_columns'],
            identity_column=data['identity_column'],
            nulls_pos=data['nulls_pos'],
        )
