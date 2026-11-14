# 湖仓事务的谓词冲突认证与可串行化提交

本项目保存仓库与湖表开发所需的本地资料，提供内容摘要、记录身份与登记关系检查。当前读取入口只返回表数量及身份检查结果，不执行关系计划、版本合并、聚合或查询规划。

资料表包含rules、transactions、reads、writes、schedule。字段单位和关系见 docs/data-contract.md。data/normal.json登记常规资料，data/boundary.json登记特殊业务情形，data/broken-digest.json用于检查资料损坏的拒绝行为。JSONL文件按清单内SHA256和字节长度绑定。

运行环境为 Python 3.11及以上，当前仅使用标准库，无需额外服务。包内io.py负责资料读取，domain.py负责表集合与引用检查，cli.py提供本地检查入口。

```sh
python -m unittest discover -s tests -v
python -m compileall -q transaction_data tests
python -m transaction_data.cli data/normal.json
python -m transaction_data.cli data/boundary.json
```

容器内使用同样的检查入口：

```sh
docker build -t transaction-data .
docker run --rm transaction-data
```

## 可串行化事务认证引擎 `serlake/`

在上述资料检查之外，`serlake/` 提供一个独立的本地湖仓事务引擎：冻结多表快照、
稳定主键点读、schema 绑定的复合键区间读、完整行插入/更新/删除；提交时按**实际
读写依赖**（ww 写覆盖 / wr 读到版本 / rw 读-写反依赖）构建多版本串行图，成环即拒绝
未提交候选并输出完整事务链与点键/区间见证。认证与正式发布在同一 SQLite 临界事务内
原子生效，长读事务不持全库写锁，回收水位与保留预算显式管理，并附带不复用认证器结论
的独立历史检查器。

设计说明见 [docs/serializable-design.md](docs/serializable-design.md)。

```sh
PYTHONPATH=. python3 demos/run_scenarios.py          # 七个认证场景演示
PYTHONPATH=. python3 -m unittest discover -s tests_serlake -v
```
