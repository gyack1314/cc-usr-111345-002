# 业务资料约定

资料展示两事务冻结同一初态的访问意图，不代表认证器已经运行。reads的lower/upper表示tenant与规则lower组成的排序键范围；returned记录范围读返回身份，active条件可在事务内判断。writes.before/after为完整旧新像，null整行表示不存在。schedule.order只是本地可控调度，不证明串行合法性。

每行id在该表内唯一，文件顺序不代表业务事件顺序。清单schema=1，各表登记文件名、字节长度、SHA256、行数及完整字段集合，未知版本或内容变化应重新登记。业务身份、金额、版本及测量数值均为本地资料，不代表任何现实机构的经营数据。

- rules：id、tenant、lower、upper、active。
- transactions：id、snapshot、operation。
- reads：id、transaction、kind、table、tenant、lower、upper、include_lower、include_upper、returned。
- writes：id、transaction、table、row_id、before、after。
- schedule：id、transaction、event、order。

normal与boundary是相互独立的资料包，不能直接拼接重复id。引用字段按本项目domain.py的REFERENCES登记。读取检查只检查完整性、身份及这些直接引用，不替代仓库查询语义、快照可见性或递归完备性判断。
