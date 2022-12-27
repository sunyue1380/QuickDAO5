# 数据库提供者

QuickDAO默认内置支持以下数据库:

* H2(1.4.200)
* SQLite
* MySQL
* MariaDB
* Postgresql
* Oracle
* SQLServer

如果用户想支持更多数据库，可以通过实现DatabaseProvider接口实现。

DatabaseProvider方法含义如下:

* getDatabaseControlInstance: 负责实现DatabaseControl接口
* getDatabaseDefinitionInstance: 负责实现DatabaseDefinition接口
* getDatabaseManipulationInstance: 负责实现DatabaseManipulation接口
* getConditionInstance: 负责实现Condition接口
* getSubConditionInstance: 负责实现SubCondition接口
* comment: 返回数据库注释语句
* escape: 返回数据库转义语句
* returnGeneratedKeys: 是否返回自增id
* getTypeFieldMapping: 返回默认Java类型与数据库类型映射关系表
* name: 数据库名称。

> QuickDAO会使用name方法来匹配jdbcUrl