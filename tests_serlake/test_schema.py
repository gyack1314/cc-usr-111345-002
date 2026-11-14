"""schema 绑定的列类型、null 位置与复合键字典序测试。"""
import unittest

from serlake import Column, Schema
from serlake.errors import ConstraintViolation


class SchemaTests(unittest.TestCase):
    def setUp(self):
        self.schema = Schema('t', [
            Column('id', 'TEXT', nullable=False),
            Column('region', 'TEXT'),
            Column('seq', 'INTEGER'),
            Column('flag', 'BOOLEAN'),
        ], key_columns=('region', 'seq'), nulls_pos='first')

    def test_nulls_first_ordering(self):
        cmp = self.schema.compare_keys
        self.assertEqual(cmp((None, 1), ('a', 0)), -1)
        self.assertEqual(cmp(('a', 0), (None, 1)), 1)
        self.assertEqual(cmp((None, 1), (None, 2)), -1)
        self.assertEqual(cmp((None, 1), (None, 1)), 0)

    def test_nulls_last_ordering(self):
        schema = Schema('t2', self.schema.columns, ('region', 'seq'),
                        nulls_pos='last')
        cmp = schema.compare_keys
        self.assertEqual(cmp((None, 1), ('a', 0)), 1)
        self.assertEqual(cmp(('a', 0), (None, 1)), -1)

    def test_lexicographic_composite_order(self):
        cmp = self.schema.compare_keys
        self.assertEqual(cmp(('a', 5), ('a', 9)), -1)
        self.assertEqual(cmp(('a', 9), ('b', 0)), -1)
        self.assertEqual(cmp(('b', 0), ('a', 9)), 1)

    def test_type_rejection(self):
        with self.assertRaises(ConstraintViolation):
            self.schema.validate_row(
                {'id': 'r1', 'region': 'x', 'seq': True, 'flag': False})
        with self.assertRaises(ConstraintViolation):
            self.schema.validate_row(
                {'id': 'r1', 'region': 1, 'seq': 2, 'flag': False})

    def test_full_row_field_set_must_match(self):
        with self.assertRaises(ConstraintViolation):
            self.schema.validate_row(
                {'id': 'r1', 'region': 'x', 'seq': 1})  # 缺 flag
        with self.assertRaises(ConstraintViolation):
            self.schema.validate_row(
                {'id': 'r1', 'region': 'x', 'seq': 1,
                 'flag': False, 'extra': 1})

    def test_identity_cannot_be_key(self):
        with self.assertRaises(ConstraintViolation):
            Schema('t3', [Column('id', 'TEXT', nullable=False)],
                   key_columns=('id',))

    def test_serialization_roundtrip(self):
        data = self.schema.to_dict()
        rebuilt = Schema.from_dict(data)
        self.assertEqual(rebuilt.key_columns, self.schema.key_columns)
        self.assertEqual(rebuilt.nulls_pos, self.schema.nulls_pos)
        self.assertEqual(
            rebuilt.compare_keys((None, 1), ('a', 1)),
            self.schema.compare_keys((None, 1), ('a', 1)))


if __name__ == '__main__':
    unittest.main()
