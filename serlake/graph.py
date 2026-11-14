"""多版本串行图（MVSG）的边语义与环检测。

三类有向边 from -> to 都表示同一个约束：在任何等价串行次序中 from 必须先于 to。

- ww  写-写覆盖：两事务写同一 row_id，from 的版本先于 to 的版本生效；
- wr  写-读依赖：to 的某次读取实际返回了 from 创建的行版本；
- rw  读-写反依赖：from 的读取谓词覆盖了 to 写入的键，但 from 的快照
       看不到 to 的版本（含空结果读取！），因此串行次序中 from 必须在前。
       这正是写偏斜与幻行的依赖形式。

每条边携带 witness：点键或区间谓词描述 + row_id + 具体读写记录号，
环被发现时整条事务链与每边见证一起输出，而不是只给提交时间。
"""
import json
from collections import defaultdict

from .errors import SerializationCycle


EDGE_LABEL = {'ww': '后写覆盖', 'wr': '读到版本', 'rw': '读-写反依赖'}


class DependencyGraph:
    """内存中的有向多重图；构建数据来自 SQLite edges 表，环检测是纯函数。"""

    def __init__(self):
        self.adj = defaultdict(list)   # node -> [(to, edge_type, witness, edge_id)]
        self.nodes = set()

    def add_node(self, tid):
        self.nodes.add(tid)

    def add_edge(self, from_tid, to_tid, edge_type, witness, edge_id=None):
        if edge_type not in EDGE_LABEL:
            raise ValueError(f'未知边类型: {edge_type}')
        self.nodes.add(from_tid)
        self.nodes.add(to_tid)
        # 同节点对同类型同见证去重（持久层也有 UNIQUE 约束，双保险）。
        for existing in self.adj[from_tid]:
            if existing[0] == to_tid and existing[1] == edge_type \
                    and existing[2] == witness:
                return
        self.adj[from_tid].append((to_tid, edge_type, witness, edge_id))

    @classmethod
    def from_storage(cls, storage):
        graph = cls()
        for tid in (r['tid'] for r in storage.committed_txns()):
            graph.add_node(tid)
        for edge in storage.all_edges():
            graph.add_edge(edge['from_tid'], edge['to_tid'],
                           edge['edge_type'],
                           json.loads(edge['witness_json']),
                           edge['edge_id'])
        return graph

    def find_cycle_through(self, root):
        """从 root 出发的 DFS；找到回边即返回环。

        返回 (chain, chain_edges)：chain 是事务 id 环（首尾相同），
        chain_edges 依次给出每条边的类型与见证；无环返回 None。
        """
        path = [root]
        path_edges = []
        on_path = {root}

        def visit(node):
            for to_tid, edge_type, witness, _ in self.adj[node]:
                if to_tid in on_path:
                    start = path.index(to_tid)
                    return (path[start:] + [to_tid],
                            path_edges[start:] + [(edge_type, witness)])
                path.append(to_tid)
                path_edges.append((edge_type, witness))
                on_path.add(to_tid)
                found = visit(to_tid)
                if found is not None:
                    return found
                path.pop()
                path_edges.pop()
                on_path.discard(to_tid)
            return None

        return visit(root)

    def assert_acyclic_with(self, candidate_tid):
        """候选事务的边已加入后调用；成环则抛 SerializationCycle。"""
        cycle = self.find_cycle_through(candidate_tid)
        if cycle is None:
            return
        chain, chain_edges = cycle
        rendered = [
            {'edge_type': edge_type, 'label': EDGE_LABEL[edge_type],
             'witness': witness}
            for edge_type, witness in chain_edges]
        chain_text = ' -> '.join(f'T{n}' for n in chain)
        edge_text = '; '.join(
            f"T{witness['from_tid']} {EDGE_LABEL[edge_type]} "
            f"T{witness['to_tid']} [{witness['detail']}]"
            for edge_type, witness in chain_edges)
        raise SerializationCycle(
            f'依赖图成环 {chain_text}（{edge_text}），'
            '不存在与所有读取一致的串行次序',
            chain=chain, edges=rendered)

    def topological_order(self):
        """Kahn 拓扑排序：图无环时返回一个合法串行次序。"""
        indegree = defaultdict(int)
        for node in self.nodes:
            indegree.setdefault(node, 0)
        for from_tid, outs in self.adj.items():
            for to_tid, *_ in outs:
                indegree[to_tid] += 1
        ready = sorted(n for n, d in indegree.items() if d == 0)
        order = []
        while ready:
            node = ready.pop(0)
            order.append(node)
            for to_tid, *_ in self.adj[node]:
                indegree[to_tid] -= 1
                if indegree[to_tid] == 0:
                    ready.append(to_tid)
                    ready.sort()
        if len(order) != len(self.nodes):
            raise SerializationCycle(
                '已提交图存在环，无法产生串行次序', chain=[], edges=[])
        return order

    def explain_order(self, order):
        """把给定串行次序与边约束对照，输出逐条边的满足情况。"""
        position = {tid: i for i, tid in enumerate(order)}
        report = []
        for from_tid, outs in self.adj.items():
            for to_tid, edge_type, witness, _ in outs:
                report.append({
                    'from': from_tid, 'to': to_tid,
                    'edge_type': edge_type, 'label': EDGE_LABEL[edge_type],
                    'satisfied': position[from_tid] < position[to_tid],
                    'witness': witness,
                })
        return report
