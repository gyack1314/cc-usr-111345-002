"""谓词测试：开闭区间、UNBOUND 槽位、null 键、键迁移命中。"""
import unittest

from serlake import Column, Schema
from serlake.predicate import PointPredicate, RangePredicate, UNBOUND
from serlake.errors import ConstraintViolation


def make_schema(nulls_pos='last'):
    return Schema('rules', [
        Column('id', 'TEXT', nullable=False),
        Column('tenant', 'TEXT', nullable=False),
        Column('lower', 'INTEGER'),
        Column('upper', 'INTEGER'),
        Column('active', 'BOOLEAN'),
    ], key_columns=('tenant', 'lower'), nulls_pos=nulls_pos)


class PredicateTests(unittest.TestCase):
    def setUp(self):
        self.schema = make_schema()

    def test_point_equality(self):
        p = PointPredicate(self.schema, ('A', 10))
        self.assertTrue(p.contains(('A', 10)))
        self.assertFalse(p.contains(('A', 11)))
        self.assertFalse(p.contains(('B', 10)))

    def test_point_rejects_unbound(self):
        with self.assertRaises(ConstraintViolation):
            PointPredicate(self.schema, ('A', UNBOUND))

    def test_open_closed_bounds(self):
        half = RangePredicate(self.schema,
                              lower=('A', 10), upper=('A', 20))
        self.assertTrue(half.contains(('A', 10)))    # 下界闭
        self.assertTrue(half.contains(('A', 15)))
        self.assertFalse(half.contains(('A', 20)))   # 上界开
        full = RangePredicate(self.schema,
                              lower=('A', 10), upper=('A', 20),
                              include_upper=True)
        self.assertTrue(full.contains(('A', 20)))
        open_lower = RangePredicate(self.schema,
                                    lower=('A', 10), upper=('A', 20),
                                    include_lower=False)
        self.assertFalse(open_lower.contains(('A', 10)))

    def test_lexicographic_prefix_semantics(self):
        p = RangePredicate(self.schema,
                           lower=('A', 10), upper=('A', 20))
        self.assertTrue(p.contains(('A', 15)))
        self.assertFalse(p.contains(('A', 9)))
        # 首列不同即由首列决定：B > A，越过整个 A 区间上界。
        self.assertFalse(p.contains(('B', 0)))
        self.assertFalse(p.contains(('@', 99)))
        # 前缀区间 [A10, *) 对首列更大的键全部成立。
        open_hi = RangePredicate(
            self.schema, lower=('A', 10), upper=(UNBOUND, UNBOUND))
        self.assertTrue(open_hi.contains(('B', 0)))
        self.assertFalse(open_hi.contains(('A', 9)))

    def test_unbound_slots(self):
        # 下界 ('A', UNBOUND)：首列必须 >= A，第二列起不约束。
        p = RangePredicate(self.schema,
                           lower=('A', UNBOUND), upper=(UNBOUND, UNBOUND))
        self.assertTrue(p.contains(('A', None)))
        self.assertTrue(p.contains(('A', -1000)))
        self.assertTrue(p.contains(('Z', 1)))     # Z > A，第二列通配
        self.assertFalse(p.contains(('@', 1)))   # @ < A

    def test_null_bound_is_concrete(self):
        # 真实 NULL 与 UNBOUND 不同：NULL 是具体下界值。
        p = RangePredicate(self.schema,
                           lower=('A', None), upper=(UNBOUND, UNBOUND))
        # nulls last：NULL 最大，(A,5) < (A,NULL) 不满足下界。
        self.assertFalse(p.contains(('A', 5)))
        self.assertTrue(p.contains(('A', None)))   # 下界闭，等值成立
        self.assertTrue(p.contains(('Z', 5)))      # 首列更大直接成立

    def test_null_key_in_range_nulls_last(self):
        # 下界第二列固定为 0：nulls last 下 NULL 最大，(A,NULL) > (A,0)，
        # 上界 UNBOUND 不限制，因此键在区间内——null 位置影响的是相对次序。
        p = RangePredicate(self.schema,
                           lower=('A', 0), upper=(UNBOUND, UNBOUND))
        self.assertTrue(p.contains(('A', None)))
        # 构造 NULL 落在下界之下的场景：下界为具体大值、nulls first。
        schema_first = make_schema(nulls_pos='first')
        p_first = RangePredicate(schema_first,
                                 lower=('A', 10), upper=(UNBOUND, UNBOUND))
        self.assertFalse(p_first.contains(('A', None)))  # NULL 最小
        p_last = RangePredicate(make_schema(nulls_pos='last'),
                                lower=('A', 10), upper=(UNBOUND, UNBOUND))
        self.assertTrue(p_last.contains(('A', None)))   # NULL 最大

    def test_invalid_range_rejected(self):
        with self.assertRaises(ConstraintViolation):
            RangePredicate(self.schema,
                           lower=('B', 0), upper=('A', 0))
        with self.assertRaises(ConstraintViolation):
            RangePredicate(self.schema,
                           lower=('A', 10), upper=('A', 10),
                           include_lower=False, include_upper=True)

    def test_roundtrip_preserves_unbound(self):
        p = RangePredicate(self.schema,
                           lower=('A', UNBOUND), upper=(UNBOUND, UNBOUND))
        rebuilt = RangePredicate.from_dict(self.schema, p.to_dict())
        self.assertTrue(rebuilt.contains(('A', 5)))
        self.assertTrue(rebuilt.contains(('B', 5)))
        self.assertFalse(rebuilt.contains(('@', 5)))
        self.assertTrue(rebuilt.contains(('A', None)))
        # 含真实 NULL 槽位的谓词也要无损往返，语义与原谓词一致。
        p2 = RangePredicate(self.schema,
                            lower=('A', None), upper=(UNBOUND, UNBOUND))
        rebuilt2 = RangePredicate.from_dict(self.schema, p2.to_dict())
        self.assertFalse(rebuilt2.contains(('A', 5)))   # nulls last
        self.assertTrue(rebuilt2.contains(('A', None)))
        self.assertTrue(rebuilt2.contains(('Z', 5)))


if __name__ == '__main__':
    unittest.main()
