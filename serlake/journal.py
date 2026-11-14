"""审计事件类型与只追加日志的读取工具。

事件序列（每个事务）：
  begin -> {read,write}* -> (commit | abort)
commit 事件与版本生效、边、回执在同一个 SQLite 临界事务里写入，
因此"已发布"与"日志可见"是同一个原子事实，重放不会产生幽灵提交。

独立历史检查器（checker.py）只消费本日志 + 初始快照 + 版本文件，
不读取 edges 表、不调用认证器，保证结论来源相互独立。
"""

EVENT_BEGIN = 'begin'
EVENT_READ = 'read'
EVENT_WRITE = 'write'
EVENT_COMMIT = 'commit'
EVENT_ABORT = 'abort'
EVENT_GC = 'gc'
EVENT_TERMINATE = 'terminate'


def split_histories(events):
    """把扁平事件流按 tid 分组，组内保持日志顺序。"""
    grouped = {}
    order = []
    for event in events:
        tid = event.get('tid')
        if tid is None:
            continue
        if tid not in grouped:
            grouped[tid] = []
            order.append(tid)
        grouped[tid].append(event)
    return order, grouped


def committed_histories(events):
    """只返回最终提交事务的事件序列（按 begin 顺序）。"""
    order, grouped = split_histories(events)
    result = []
    for tid in order:
        txn_events = grouped[tid]
        if any(e['event'] == EVENT_COMMIT for e in txn_events):
            result.append((tid, txn_events))
    return result


def aborted_histories(events):
    order, grouped = split_histories(events)
    return [(tid, grouped[tid]) for tid in order
            if any(e['event'] == EVENT_ABORT for e in grouped[tid])]
