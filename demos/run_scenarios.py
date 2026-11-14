"""题目要求的全部认证场景演示。

场景清单：
1. 双事务写偏斜（同一区间各插互斥规则：成本修订任务的原始事故）
2. 空范围插入的幻行（含合法单插与双向幻行成环两个子场景）
3. 三事务依赖环（点读+更新，环链与点键见证）
4. 相交但仍合法的访问（区间相交、无共同写键、含键迁移两侧命中）
5. 不相交并发（粗粒度表版本判断会误杀，本引擎放行）
6. 同键并发写策略（首提者胜）与崩溃重放/幂等
7. 长读事务、回收水位、保留预算与显式拒绝/终止
"""
import tempfile
from pathlib import Path

from serlake.errors import (
    SerializationCycle, WriteWriteConflict, UniquenessViolation,
    RetentionBudgetExceeded, SerlakeError)
from common import (
    fresh_house, banner, show_metrics, show_checker, rules_schema,
    counters_schema)


def scenario_1_write_skew(base):
    banner('场景1：双事务写偏斜——两条成本修订任务的原始事故')
    house, _ = fresh_house(base, 's1')
    load = house.begin('s1-load')
    load.insert('rules', {'id': 'r0', 'tenant': 'A', 'lower': 0,
                          'upper': 10, 'active': False})
    house.commit(load)

    t1 = house.begin('s1-revise-1')
    t1.range_read('rules', lower=('A', 0), upper=('A', 100))  # 谓词登记
    t1.insert('rules', {'id': 'r1', 'tenant': 'A', 'lower': 10,
                        'upper': 20, 'active': True})

    t2 = house.begin('s1-revise-2')
    t2.range_read('rules', lower=('A', 0), upper=('A', 100))
    t2.insert('rules', {'id': 'r2', 'tenant': 'A', 'lower': 20,
                        'upper': 30, 'active': True})

    house.commit(t1)
    print('T2(第一条修订) 已提交；第二条修订认证中……')
    try:
        house.commit(t2)
        print('!! 不应发生：两条互斥规则都提交了')
    except SerializationCycle as exc:
        print('第二条修订被拒绝，环链:',
              ' -> '.join('T%d' % t for t in exc.chain))
        for edge in exc.chain_edges:
            print('  %s 见证: %s' % (
                edge['edge_type'], edge['witness']['detail']))

    # 业务侧用新操作标识重试：此时快照已含 r1，谓词不再互斥。
    retry = house.begin('s1-revise-2-retry')
    retry.range_read('rules', lower=('A', 0), upper=('A', 100))
    retry.insert('rules', {'id': 'r2', 'tenant': 'A', 'lower': 20,
                           'upper': 30, 'active': True})
    receipt = house.commit(retry)
    print('换新操作标识重试成功，提交序号 =', receipt.commit_seq)
    print('终态规则键:', [(r['id'], r['tenant'], r['lower'])
                          for r in house.snapshot_scan('rules')])
    show_metrics(house)
    show_checker(house)
    house.close()


def scenario_2_phantom(base):
    banner('场景2：空范围插入——空结果也是读取，幻行靠谓词而非返回主键')
    house, _ = fresh_house(base, 's2')

    # 2a 单向反依赖：读者随后自己提交，无环，合法（幻行被记录但不构成拒绝）。
    t1 = house.begin('s2-a-reader')
    empty = t1.range_read('rules', lower=('B', 0), upper=('B', 100))
    assert empty == []
    print('2a 空范围读取返回', len(empty), '行，谓词已登记（含空结果标记）')
    t2 = house.begin('s2-a-inserter')
    t2.insert('rules', {'id': 'b1', 'tenant': 'B', 'lower': 5,
                        'upper': 8, 'active': True})
    house.commit(t2)
    t1.insert('rules', {'id': 'b2', 'tenant': 'B', 'lower': 50,
                        'upper': 60, 'active': False})
    receipt = house.commit(t1)
    print('2a 单向 rw 反依赖无环，读者后提交成功，序号 =', receipt.commit_seq)

    # 2b 双向幻行：两事务都对同一空范围做"先查后插"，环必须被拒绝。
    t3 = house.begin('s2-b-1')
    t3.range_read('rules', lower=('C', 0), upper=('C', 100))
    t3.insert('rules', {'id': 'c1', 'tenant': 'C', 'lower': 1,
                        'upper': 2, 'active': True})
    t4 = house.begin('s2-b-2')
    t4.range_read('rules', lower=('C', 0), upper=('C', 100))  # 同样为空
    t4.insert('rules', {'id': 'c2', 'tenant': 'C', 'lower': 2,
                        'upper': 3, 'active': True})
    house.commit(t3)
    try:
        house.commit(t4)
        print('!! 不应发生：幻行环未被拦截')
    except SerializationCycle as exc:
        print('2b 双向幻行环被拒绝，环链:',
              ' -> '.join('T%d' % t for t in exc.chain))
        for edge in exc.chain_edges:
            w = edge['witness']
            print('  %s 空读=%s 见证: %s' % (
                edge['edge_type'], w['empty_read'], w['detail']))
    show_metrics(house)
    show_checker(house)
    house.close()


def scenario_3_three_txn_cycle(base):
    banner('场景3：三事务依赖环——点读见证与完整事务链')
    house, _ = fresh_house(base, 's3')
    load = house.begin('s3-load')
    for rid, tenant in (('x1', 'X'), ('x2', 'Y'), ('x3', 'Z')):
        load.insert('rules', {'id': rid, 'tenant': tenant, 'lower': 0,
                              'upper': 1, 'active': False})
    house.commit(load)

    t1 = house.begin('s3-t1')
    t1.point_read('rules', ('X', 0))
    t1.update('rules', 'x3', {'id': 'x3', 'tenant': 'Z', 'lower': 0,
                              'upper': 1, 'active': True})

    t2 = house.begin('s3-t2')
    t2.point_read('rules', ('Y', 0))
    t2.update('rules', 'x1', {'id': 'x1', 'tenant': 'X', 'lower': 0,
                              'upper': 1, 'active': True})

    t3 = house.begin('s3-t3')
    t3.point_read('rules', ('Z', 0))
    t3.update('rules', 'x2', {'id': 'x2', 'tenant': 'Y', 'lower': 0,
                              'upper': 1, 'active': True})

    house.commit(t1)
    house.commit(t2)
    try:
        house.commit(t3)
        print('!! 三事务环未被拦截')
    except SerializationCycle as exc:
        print('第三个提交者被拒绝，环链:',
              ' -> '.join('T%d' % t for t in exc.chain))
        for edge in exc.chain_edges:
            print('  %s 见证: %s' % (
                edge['edge_type'], edge['witness']['detail']))
    show_metrics(house)
    show_checker(house)
    house.close()


def scenario_4_intersecting_but_legal(base):
    banner('场景4：相交但仍合法——区间相交不必然冲突，键迁移两侧都检查')
    house, _ = fresh_house(base, 's4')
    load = house.begin('s4-load')
    load.insert('rules', {'id': 'd1', 'tenant': 'D', 'lower': 10,
                          'upper': 20, 'active': False})
    house.commit(load)

    # 两事务读区间在 [40,50) 相交，但写键分别在双方谓词都够不到的两端：
    # t1 读 [0,50)，写键 95（高于 t2 的上界 90，也高于自己的上界 50）；
    # t2 读 [40,90)，写键 20（低于自己下界 40，也低于 t1 下界 0 不成立，
    # 20 落在 t1 的 [0,50) 内——因此改用负向不行，换成区间外的精确键）。
    # 最终构造：t1 写 95，t2 写 20 但让 t1 谓词改为 [30,50)，于是
    # 20 < 30 不命中 t1，95 > 90 不命中 t2，读区间仍在 [40,50) 相交。
    t1 = house.begin('s4-t1')
    t1.range_read('rules', lower=('D', 30), upper=('D', 50))
    t1.insert('rules', {'id': 'd2', 'tenant': 'D', 'lower': 95,
                        'upper': 99, 'active': True})

    t2 = house.begin('s4-t2')
    t2.range_read('rules', lower=('D', 40), upper=('D', 90))
    t2.insert('rules', {'id': 'd3', 'tenant': 'D', 'lower': 20,
                        'upper': 25, 'active': True})

    r1 = house.commit(t1)
    r2 = house.commit(t2)
    print('两个相交区间事务均提交，序号:', r1.commit_seq, r2.commit_seq)
    print('  T1 新增边:', r1.edges_added, ' T2 新增边:', r2.edges_added)

    # 键迁移环：更新把键从一个区间搬进另一个区间，前后两侧都参与命中。
    # t3 读低区间并写高键；t4 读高区间并把 d1 的键从低区间迁进高区间。
    t3 = house.begin('s4-t3')
    t3.range_read('rules', lower=('D', 0), upper=('D', 30))   # 含 d1(键10)
    t3.insert('rules', {'id': 'd4', 'tenant': 'D', 'lower': 85,
                        'upper': 88, 'active': True})           # 写高键
    t4 = house.begin('s4-t4')
    t4.range_read('rules', lower=('D', 70), upper=('D', 90))  # 含 d4(键85)
    # d1 键 (D,10) -> (D,80)：迁出 t3 的低区间、迁入 t4 的高区间。
    t4.update('rules', 'd1', {'id': 'd1', 'tenant': 'D', 'lower': 80,
                              'upper': 90, 'active': True})
    house.commit(t4)
    try:
        house.commit(t3)
        print('!! 键迁移反依赖未被识别')
    except SerializationCycle as exc:
        print('键迁移形成反依赖环，拒绝链:',
              ' -> '.join('T%d' % t for t in exc.chain))
        for edge in exc.chain_edges:
            print('  %s 见证: %s' % (
                edge['edge_type'], edge['witness']['detail']))
    show_metrics(house)
    show_checker(house)
    house.close()


def scenario_5_disjoint(base):
    banner('场景5：不相交并发——表指针变化也不该误杀')
    house, _ = fresh_house(base, 's5')
    t1 = house.begin('s5-t1')
    t1.range_read('rules', lower=('E', 0), upper=('E', 100))
    t1.insert('rules', {'id': 'e1', 'tenant': 'E', 'lower': 1,
                        'upper': 2, 'active': True})
    t2 = house.begin('s5-t2')
    t2.range_read('rules', lower=('F', 0), upper=('F', 100))
    t2.insert('rules', {'id': 'f1', 'tenant': 'F', 'lower': 1,
                        'upper': 2, 'active': True})
    r1 = house.commit(t1)
    r2 = house.commit(t2)
    print('不相交租户的事务均提交，序号:', r1.commit_seq, r2.commit_seq)
    print('  T2 认证新增边（应为空）:', r2.edges_added)
    assert r2.edges_added == {'ww': 0, 'wr': 0, 'rw': 0}
    show_metrics(house)
    show_checker(house)
    house.close()


def scenario_6_ww_and_crash(base):
    banner('场景6：同键并发写首提者胜 + 崩溃重放/幂等发布')
    house, root = fresh_house(base, 's6')
    load = house.begin('s6-load')
    load.insert('rules', {'id': 'g1', 'tenant': 'G', 'lower': 1,
                          'upper': 2, 'active': False})
    house.commit(load)

    t1 = house.begin('s6-w-1')
    t1.point_read('rules', ('G', 1))
    t1.update('rules', 'g1', {'id': 'g1', 'tenant': 'G', 'lower': 1,
                              'upper': 2, 'active': True})
    t2 = house.begin('s6-w-2')
    t2.point_read('rules', ('G', 1))
    t2.update('rules', 'g1', {'id': 'g1', 'tenant': 'G', 'lower': 1,
                              'upper': 9, 'active': True})
    house.commit(t1)
    try:
        house.commit(t2)
        print('!! 同键并发写未被拦截')
    except WriteWriteConflict as exc:
        print('首提者胜，候选被拒绝:', exc.witness['detail'])

    # 并发插入同唯一键（不同 row_id，同排序键）：唯一键复核。
    t3 = house.begin('s6-u-1')
    t3.insert('rules', {'id': 'g2', 'tenant': 'G', 'lower': 5,
                        'upper': 6, 'active': True})
    t4 = house.begin('s6-u-2')
    t4.insert('rules', {'id': 'g3', 'tenant': 'G', 'lower': 5,
                        'upper': 7, 'active': False})
    house.commit(t3)
    try:
        house.commit(t4)
    except UniquenessViolation as exc:
        print('唯一键复核拒绝:', exc.witness['detail'])

    # 崩溃注入：认证通过、COMMIT 前断电。
    t5 = house.begin('s6-crash')
    t5.insert('rules', {'id': 'g4', 'tenant': 'G', 'lower': 40,
                        'upper': 50, 'active': True})

    def crash_after_certification(point, result):
        if point == 'after_certification':
            raise RuntimeError('模拟断电：认证后、COMMIT 前进程退出')

    try:
        house.commit(t5, crash_hook=crash_after_certification)
    except RuntimeError as exc:
        print('注入崩溃:', exc)
    house.close()

    # 重开重放：reconcile 标记残留；同一操作重试不产生重复发布。
    house = LakehouseHouseReopen(root)
    print('重开 reconcile 摘要:', house_reconcile_note(house))
    try:
        house.begin('s6-crash')  # 同 operation_id：崩溃在 COMMIT 前
        print('!! 不应恢复已中止事务')
    except SerlakeError as exc:
        print('同操作重试得到明确中止结论（无半提交、无重复发布）:', exc)
    retry = house.begin('s6-crash-retry')
    retry.insert('rules', {'id': 'g4', 'tenant': 'G', 'lower': 40,
                           'upper': 50, 'active': True})
    receipt = house.commit(retry)
    ids_before = [r['id'] for r in house.snapshot_scan('rules')]

    # 提交后失联：同一 operation_id 重连必须返回原回执，版本不增加。
    replayed = house.begin('s6-crash-retry')
    assert hasattr(replayed, 'receipt')
    print('提交后失联重连返回同一回执，序号 =',
          replayed.receipt.commit_seq, '（与首次 =',
          receipt.commit_seq, '一致）')
    ids_after = [r['id'] for r in house.snapshot_scan('rules')]
    assert ids_before == ids_after and replayed.receipt.commit_seq == \
        receipt.commit_seq
    show_checker(house)
    house.close()


def house_reconcile_note(house):
    # reconcile 在 open 时已执行；通过审计日志展示其决定。
    events = [e for e in house.audit_events()
              if e['event'] == 'terminate'
              and e.get('reason') == 'crash_before_commit']
    return {'中止的崩溃残留事务': [e['tid'] for e in events]}


def LakehouseHouseReopen(root):
    from serlake import Lakehouse
    return Lakehouse.open(root)


def scenario_7_retention(base):
    banner('场景7：长读事务、回收水位与保留预算的显式处置')
    # 极小预算，迫使回收与拒绝逻辑可见。
    house, _ = fresh_house(base, 's7', budget=12)
    # 长读事务先开始，把回收水位钉在 seq=0。
    long_reader = house.begin('s7-long-reader')
    long_reader.range_read('rules', lower=('H', 0), upper=('H', 100))
    print('长读事务 T%d 开始，快照水位固定在 seq=%d' % (
        long_reader.tid, long_reader.snapshot_seq))

    # 制造一条真实依赖边：writer 先开始并暂存写，reader 在相同快照上
    # 读到空区间，随后 writer 先提交、reader 后提交。reader 认证时其空读
    # 谓词命中 writer 的写键且快照看不到该版本 ⇒ reader -> writer 的 rw 边。
    writer = house.begin('s7-edge-writer')
    writer.insert('rules', {'id': 'h0', 'tenant': 'H', 'lower': 0,
                            'upper': 1, 'active': False})
    reader = house.begin('s7-edge-reader')
    reader.range_read('rules', lower=('H', 0), upper=('H', 100))
    house.commit(writer)
    house.commit(reader)

    for i in range(1, 3):
        t = house.begin(f's7-writer-{i}')
        t.insert('rules', {'id': f'h{i}', 'tenant': 'H', 'lower': i,
                           'upper': i + 1, 'active': True})
        house.commit(t)
    report = house.retention_report()
    # 长读者钉住水位：只有 commit_seq <= 长读者快照的已提交事务可回收。
    pinned = [c for c in report['retained_committed']
              if c['commit_seq'] > long_reader.snapshot_seq]
    print('长读者存活时水位 =', report['watermark'],
          '，其后的已提交事务均不可回收:',
          all(not c['gc_eligible'] for c in pinned))
    print('当前保留足迹:', report['footprint'], '/ 预算', report['budget'])
    edges_before = len(house.dependency_edges())
    print('回收前依赖边数 =', edges_before)

    # 终止长读者后，水位推进，回收可以归档详细足迹（边仍保留）。
    outcome = house.terminate_longest_reader()
    print('终止长读者 T%d，回收归档事务:' % outcome['terminated_tid'],
          outcome['archived_txns'])
    report = house.retention_report()
    print('回收后保留足迹:', report['footprint'])
    edges_after = len(house.dependency_edges())
    print('回收后依赖边数 =', edges_after, '（边不随足迹回收）')
    assert edges_after == edges_before
    print('回收后仍能解释串行次序:', house.explain_serialization()['serial_order'])

    # 预算耗尽时新事务必须被显式拒绝，而不是悄悄降低隔离级别。
    house.storage.set_budget(1)
    try:
        house.begin('s7-over-budget')
        print('!! 预算限制未生效')
    except RetentionBudgetExceeded as exc:
        print('预算耗尽显式拒绝:', str(exc).split('；')[0])
    house.close()


def main():
    base = tempfile.mkdtemp(prefix='serlake-demo-')
    print('演示湖仓根目录:', base)
    scenario_1_write_skew(base)
    scenario_2_phantom(base)
    scenario_3_three_txn_cycle(base)
    scenario_4_intersecting_but_legal(base)
    scenario_5_disjoint(base)
    scenario_6_ww_and_crash(base)
    scenario_7_retention(base)
    banner('全部场景结束')


if __name__ == '__main__':
    main()
