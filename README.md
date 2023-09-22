SimpleETL
--
简单的小程序，用于每天将数据从sqlserver同步到mysql。

env
-- 
* [x] .NET 6

Features
--

* 数据源
  * [x] SQL SERVER
  * [x] Oracle
  * [x] MySQL
* 目标库
  * [x] SQL SERVER
  * [x] Oracle
  * [x] MYSQL
* [ ] 定时任务
 * [x] 代码添加任务

Note
--

**不同数据库之间的数据类型是不同的，在推送时，需要考虑目标库中的字段类型。**

例如：将SQL server的数据推送到Oracle中，SQL server 的DateTime是无法与Oracle的Date相兼容的，需要Oracle目标库自行转换。

在[ConsoleHost](/SimpleETL.ConsoleHost)项目，[M_BulkCopyDemo.cs](/SimpleETL.ConsoleHost/Database/M_BulkCopyDemo.cs#L21-L23C46)中的`CREATETIME`使用varchar保存时间数据，`CREATETICKS` 使用long保存时间数据的timestamp

M_BulkCopyDemo.cs：

![image](https://github.com/YuanjianZhang/SimpleETL/assets/33444819/8483324c-4f3b-4ef3-bde0-731b80170ff7)
