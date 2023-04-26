package cn.schoolwow.quickdao.dao;

import cn.schoolwow.quickdao.dao.dql.condition.Condition;
import cn.schoolwow.quickdao.dao.transaction.Transaction;
import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.provider.DatabaseProvider;

import javax.sql.DataSource;
import java.util.Map;
import java.util.function.Consumer;

public interface DAOOperation {
    /**
     * 数据库查询语句
     *
     * @param clazz 实体类表
     */
    <T> Condition<T> query(Class<T> clazz);

    /**
     * 数据库查询语句
     *
     * @param tableName 指定表名
     */
    Condition query(String tableName);

    /**
     * 数据库查询语句
     *
     * @param condition 子查询
     */
    Condition query(Condition condition);

    /**
     * 开启事务
     */
    Transaction startTransaction();

    /**
     * 开启事务
     */
    void startTransaction(Consumer<Transaction> transactionConsumer);

    /**
     * 获取连接池
     */
    DataSource getDataSource();

    /**
     * 获取扫描的所有实体类信息
     */
    Map<String, Entity> getEntityMap();

    /**
     * 获取实体类表
     *
     * @param clazz 实体类
     */
    Entity getEntity(Class clazz);

    /**
     * 获取实体类表
     *
     * @param tableName 数据库表名
     */
    Entity getEntity(String tableName);

    /**
     * 获取数据库提供者
     */
    DatabaseProvider getDatabaseProvider();

    /**
     * 获取配置信息
     */
    QuickDAOConfig getQuickDAOConfig();

}
