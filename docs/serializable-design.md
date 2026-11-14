# serlake：基于实际读写依赖的可串行化认证

本文说明 `serlake/` 的设计与证明结构，对应 `demos/run_scenarios.py` 的场景 1–7。
引擎与原有 `transaction_data/` 资料检查代码相互独立，不修改既有资料包。

## 1. 问题与拒绝的两种错误做法

结算仓的事故形态：两个成本修订任务各自读到「当前没有其他生效规则」，再各插一条规则。
它们没有写同一个主键，快照隔离下都提交，结果出现两条互斥规则。要判定这种历史是否
可串行化，只有两个可靠的依据：

- **不能把事务排成长队**——那会牺牲并发，且与正确性无关；
- **不能只比较提交前后的表指针**——表版本变了不代表两个事务真的冲突（场景 5 的
  不相交并发会被误杀），表版本没变也不代表没有幻行。

因此认证必须建立在**实际的读写依赖**上：谁读了哪个版本、谁覆盖了谁的写、
谁的读取谓词覆盖了谁的写键却看不到它。这三类关系组成多版本串行图（MVSG）：

| 边 | 含义 | 何时产生 |
|----|------|----------|
| `ww` 后写覆盖 | 同 `row_id` 两个写版本，先后生效 | 两事务写同一行身份 |
| `wr` 读到版本 | 读者实际返回了写者创建的版本 | 读记录的 `observed_vids` 含该版本 |
| `rw` 写-读反依赖 | 读者谓词覆盖写者的键，但快照看不到该版本 | 谓词命中 + 版本不在所见集合 |

在任何等价串行次序中，`w -> r` 都要求 `w` 在 `r` 之前。图无环当且仅当存在与该历史
所有读取一致的串行次序（这是多版本可串行化判定的标准结论，见
Bernstein–Goodman 的 MVSG 理论）。

## 2. 已提交版本、活跃事务与读写足迹的表示

### 2.1 版本

- 每次插入/更新/删除写一个不可变版本文件 `data/tables/<表>/<vid>.json`，
  删除写墓碑（`*.tombstone.json`）；
- `row_versions` 登记 `(vid, table, row_id, creator_tid, create_seq, predecessor_vid, is_delete, sort_key_json)`；
- 事务开始时 `create_seq` 为空、`staged=1`，**仅创建者可见**；提交临界事务内
  一次性置 `create_seq=<全局提交序号>` 且 `staged=0`；
- 快照 `S` 下某行 `row_id` 的可见版本 = `create_seq <= S` 的最大 `vid`；
  墓碑即「该行在快照 S 下不存在」（含更新的键迁移、删除）。

外部读者只走 `visible_versions(current_seq, table)`，永远看不到 `staged=1` 或
`create_seq` 未生效的版本，所以只能读到完整已提交版本。

### 2.2 事务身份与快照

- `txns(tid, operation_id UNIQUE, state, snapshot_seq, is_readonly, commit_seq, receipt_json)`；
- `begin` 记录 `snapshot_seq = current_commit_seq()`，该快照在事务整个生命周期冻结；
- `operation_id` 是业务幂等键：重复 `begin` 已提交返回原回执，已中止给出明确结论，
  ACTIVE 则重建句柄。这是「同一操作重试不重复发布」的落点。

### 2.3 读写足迹

- `txn_reads(tid, order_no, table, predicate_json, observed_vids_json, observed_row_ids_json)`：
  **每一次点读或区间读都登记谓词与所见版本**，空结果同样登记
  （`observed_*` 为空列表，审计事件带 `empty=true`）。幻行判定靠谓词，不靠返回主键。
- `txn_writes(tid, order_no, table, row_id, op_kind, before_vid, after_vid, key_before_json, key_after_json)`：
  保存完整的旧/新像与更新前后的**两侧键**。

谓词与 schema 绑定：

- 点读 = 完整复合键；区间读 = 逐槽位边界 + `include_lower/include_upper`；
- `UNBOUND` 是槽位哨兵（某列起不约束），与真实 `NULL` 值语义不同——`NULL` 是具体键值，
  其相对位置由 `nulls_pos='first'|'last'` 决定；
- 比较是逐列字典序，列类型在 schema 声明（`TEXT/INTEGER/REAL/BOOLEAN`），
  `BOOLEAN` 与 `INTEGER` 不隐式互转，拒绝跨类型比较。

## 3. 认证：只建真实依赖的边

候选事务 `T` 提交时（`certifier.py`），取 `T.snapshot_seq` 之后已提交的事务集合 `Later`，
在**一次 `BEGIN IMMEDIATE` 临界事务**内做四件事：

1. **ww**：`T` 的写与 `Later` 的写若同 `(table, row_id)`，硬冲突，首提者胜，直接拒绝
   （`WriteWriteConflict`）。
2. **唯一键复核**：`T` 提交后活键若已被任何已生效版本占用（不同 `row_id`、同排序键），
   拒绝（`UniquenessViolation`）。捕获「并发插入同键」。
3. **rw 反依赖，双向**：
   - `Later` 的每个读谓词命中 `T` 的写键（`T` 的快照看不到该版本）⇒ `Later -> T`
   - `T` 的每个读谓词命中 `Later` 的写键（`Later` 快照看不到该版本）⇒ `T -> Later`
   - 更新迁移键时，`_write_hits_predicate` 同时测试 `key_before` 与 `key_after`，
     两侧命中都算（见证里标注「更新前键命中 / 更新后/插入键命中」）。
4. **wr**：`T` 的读记录 `observed_vids` 追溯创建者 `C` ⇒ `C -> T`（读己写的版本不影响）。

边全部加入后，从 `T` 出发做 DFS 环检测（`graph.py:find_cycle_through`）：
- 无环 ⇒ 发布；
- 有环 ⇒ 抛出 `SerializationCycle`，携带完整事务链（首尾重复）与每条边的
  `{edge_type, label, witness}`，见证包含点键或区间谓词、`row_id`、读/写记录号。
  **注意环的方向由快照可见性与谓词命中决定，与提交时间无关**——`rw` 边经常指向
  「提交更早」的事务，这正是不能用提交时间排序冒充证明的原因。

### 3.1 为什么不会成环却误报

- 场景 4：两事务读区间在 `[40,50)` 相交，写键分别落在双方谓词之外 ⇒ 无 rw 边 ⇒ 放行。
- 场景 5：不相交租户的读与写互不命中 ⇒ 零条边 ⇒ 放行（表版本变化被忽略，不误杀）。

### 3.2 为什么不会漏掉写偏斜与幻行

- 场景 1：两事务都读同一区间、各插一条落在区间内的互斥规则。`T2` 提交时
  `T1` 的读谓词命中 `T2` 的插入键 ⇒ `T1 -> T2`；`T1` 的泛化记录也被读出，
  它的谓词同样命中 `T1` 的插入键 ⇒ `T2 -> T1`。两向 rw 边成二环，拒绝第二条。
- 场景 2b：两个空区间读都登记了谓词；插入键落在该空区间 ⇒ 双向 rw ⇒ 拒绝。
  若只有一个方向（2a），无环，读者可后提交——幻行被允许但被记录。

## 4. 原子性：认证与正式提交之间不留竞态

`Storage.critical_section(work)` 是唯一持 `_write_lock` 的地方，内部 `BEGIN IMMEDIATE`：

1. `work(conn)` 在同一连接同一事务内完成：查候选足迹、读 `Later` 足迹、建边、环检测、
   分配 `commit_seq`、`row_versions` 生效、`edges` 落库；
2. 回执 `receipt_json`、`state='COMMITTED'`、`audit_journal` 的 `commit` 事件、
   `commit_seq` 自增**与上述发布在同一个 COMMIT 中生效**；
3. 任一步抛异常 ⇒ 整个临界事务 `ROLLBACK`，版本不生效、边不存在、审计不出现。

因此：

- **校验后另一个事务提交**：那个事务要么在 `Later` 集合里（已计入边），
  要么在临界区外被 `_write_lock`/SQLite 文件锁挡在本次认证之外，下次提交时再计入；
- **提交前中断**：临界事务回滚，`reconcile()` 把残留 ACTIVE 事务标为
  `crash_before_commit`，重试同 `operation_id` 得到明确中止结论（无半提交）；
- **提交后失联**：COMMIT 已原子生效，重连 `begin(same operation_id)` 返回原回执，
  版本不重复（`test_retry_after_commit_returns_same_receipt`）；
- **同一操作重试**：`operation_id` 幂等分支保证不重复发布。

锁的边界：`begin`/读/写暂存都是自动提交语句，读路径**不加写锁**。
场景 7 与 `test_long_reader_does_not_block_writers` 证明长读事务存活期间，
其他线程的写事务仍能连续提交——不是「整个业务事务期间持有全库写锁」。

多线程安全：每个线程一条 WAL 连接；`tid`/`commit_seq` 用
`UPDATE ... RETURNING` 单语句原子自增（修掉了最初「读 meta 再写」的竞争）。

## 5. 回收、保留预算与显式拒绝

- **水位** `watermark = min(ACTIVE 事务的 snapshot_seq)`（无活跃事务时为当前提交序号）；
- 已提交事务 `T` 满足 `T.commit_seq <= watermark` 时，任何现存或未来的活动事务快照
  都已包含 `T`，**不可能再有新边连向 `T` 的详细足迹**，于是 `txn_reads/txn_writes`
  可移到 `data/archive/txn-<tid>-footprint.json`；
- **图节点与已物化边始终保留**——环见证与串行次序解释还要用；
- 长读事务会钉住水位，使更多已提交足迹不可回收；这由 `retention_report()` 显式暴露
  （每事务的 `gc_eligible` 标志、水位、活跃读者快照）；
- **预算不足**：`begin` 前置检查，足迹 ≥ 预算（或预算 ≤ 0）抛
  `RetentionBudgetExceeded`；或调用 `terminate_longest_reader()` 终止最老的只读
  ACTIVE 事务再回收。**绝不静默降低隔离级别**。
- 注：水位以下仍可能存在的环只涉及**已提交**事务——那种历史本身已非法，
  认证器从不产生；所以回收不影响「活动事务 + 候选」环的判定。

## 6. 独立历史检查器（`checker.py`）

独立性：**不读 `edges` 表，不调用 `graph`/`certifier` 任何函数**。输入只有
冻结初态、`audit_journal` 事件流、不可变版本文件。它：

1. 从 `visible_versions(0, table)` 重建冻结初态；
2. 对全部已提交事务**枚举全排列**（小历史；超过 `5040` 显式报错而不是抽样）；
3. 每个排列按事务内事件顺序重放：插入/更新/删除直接改状态副本，每次读用
   **独立实现的谓词求值**找出匹配行，比对 `observed_row_ids`（空读必须仍为空）
   与行内容；
4. 每个满足读取的排列还要核对**重放终态 == 发布状态**（逐行相等）；
5. 被拒事务分类核验：
   - `serialization_cycle` ⇒ 枚举插入到已提交序列的每个位置，所有位置都不可行
     才算「拒绝正确」；
   - `write_write_conflict`/`uniqueness_violation` ⇒ 策略性拒绝，核验「该行/该键
     确实已被已提交版本占用」，不要求「不存在数学串行次序」（首提者胜本就是策略）；
6. 篡改 `edges` 表不影响检查结论（`test_checker_ignores_poisoned_edges_table`）；
   篡改发布元数据会产生终态不一致失败（`test_checker_detects_content_drift`）。

## 7. 演示输出导读（`demos/run_scenarios.py`）

每个场景都打印：拒绝链与点键/区间见证、认证访问量（`predicate_tests`、
`write_effects_tested`、`committed_tx_scanned`、各类边计数）、保留足迹与回收记录、
独立检查器的合法串行次序与拒绝核验。终态由 `snapshot_scan` 读回，
串行合法性由检查器从历史独立重放解释，而不是引用认证器结论。

运行：

```sh
PYTHONPATH=. python3 demos/run_scenarios.py
PYTHONPATH=. python3 -m unittest discover -s tests_serlake -v
```

## 8. 已知边界

- 区间谓词是**前缀/字典序**语义（复合键区间），不是任意布尔表达式；
- 认证统计是全量扫描 `Later` 足迹（正确优先，索引优化留待后续）；
- 检查器全排列适用于小规模历史（>5040 个已提交事务时显式拒绝，不静默抽样）；
- 归档的冷足迹不参与后续认证（水位语义保证其不可能产生新边）。
