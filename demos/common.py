"""演示场景公共工具：建库、建表、结果打印。"""
import shutil
from pathlib import Path

from serlake import Lakehouse, Column, Schema


def rules_schema():
    """结算规则表：行身份 id 不可变，排序键 (tenant, lower) 可迁移。"""
    return Schema('rules', [
        Column('id', 'TEXT', nullable=False),
        Column('tenant', 'TEXT', nullable=False),
        Column('lower', 'INTEGER'),
        Column('upper', 'INTEGER'),
        Column('active', 'BOOLEAN'),
    ], key_columns=('tenant', 'lower'), nulls_pos='last')


def counters_schema():
    """带可空键列的计数表，用于演示 null 位置与复合键边界。"""
    return Schema('counters', [
        Column('id', 'TEXT', nullable=False),
        Column('region', 'TEXT'),
        Column('seq', 'INTEGER'),
        Column('value', 'INTEGER'),
    ], key_columns=('region', 'seq'), nulls_pos='first')


def fresh_house(base, name, budget=10000, extra=()):
    root = Path(base) / name
    if root.exists():
        shutil.rmtree(root)
    schemas = {'rules': rules_schema()}
    for schema in extra:
        schemas[schema.name] = schema
    return Lakehouse.create(root, schemas, footprint_budget=budget), root


def banner(title):
    line = '=' * 78
    print(f'\n{line}\n{title}\n{line}')


def show_metrics(house):
    metrics = house.metrics()
    print('认证访问量:', metrics)
    report = house.retention_report()
    print('保留足迹:', report['footprint'], ' 预算:', report['budget'],
          ' 回收水位:', report['watermark'])
    if report['gc_history']:
        print('回收记录:', [
            {'tid': g['tid'], 'watermark': g['watermark'],
             'reads': g['moved_reads'], 'writes': g['moved_writes']}
            for g in report['gc_history']])
    return metrics, report


def show_checker(house):
    from serlake.checker import HistoryChecker
    verdict = HistoryChecker(house).verify()
    print('独立历史检查器:',
          '合法串行次序 =', verdict.legal_orders,
          ' 枚举排列 =', verdict.checked_permutations,
          ' 剪枝 =', verdict.pruned_permutations)
    for check in verdict.abort_checks:
        print('  拒绝核验 T%s: %s -> 拒绝正确=%s' % (
            check['tid'], check['reason'], check['rejection_correct']))
    if verdict.failures:
        print('  !! 检查失败:', verdict.failures)
    return verdict
