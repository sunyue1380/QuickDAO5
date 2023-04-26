package cn.schoolwow.quickdao.dao;

import cn.schoolwow.quickdao.dao.transaction.TransactionOperation;
import cn.schoolwow.quickdao.domain.internal.ThrowingConsumer;

import java.sql.ResultSet;
import java.util.List;

/**
 * 执行器接口
 */
public interface ConnectionExecutor extends TransactionOperation {
    /**
     * 指定名称
     *
     * @param name 执行名称
     */
    ConnectionExecutor name(String name);

    /**
     * 指定SQL语句
     *
     * @param sql 语句
     */
    ConnectionExecutor sql(String sql);

    /**
     * 是否返回自增主键
     *
     * @param returnGeneratedKeys 是否返回自增主键
     */
    ConnectionExecutor returnGeneratedKeys(boolean returnGeneratedKeys);

    /**
     * 指定参数
     *
     * @param parameters 参数列表
     */
    ConnectionExecutor parameters(List parameters);

    /**
     * 指定参数
     *
     * @param parameters 批处理参数列表
     */
    ConnectionExecutor batchParameters(List parameters);

    /**
     * 开启事务
     */
    ConnectionExecutor startTransaction();

    /**
     * 执行查询语句,返回结果集是否存在
     */
    boolean executeAndCheckExists();

    /**
     * 执行查询语句
     */
    void executeQuery(ThrowingConsumer<ResultSet> resultSetConsumer);

    /**
     * 执行更新语句
     */
    int executeUpdate();

    /**
     * 使用批处理
     */
    ConnectionExecutor startBatch();

    /**
     * 执行批处理
     *
     * @return 影响行数
     */
    int executeBatch();

    /**
     * 清空批处理
     */
    ConnectionExecutor clearBatch();

    /**
     * 关闭批处理
     */
    ConnectionExecutor closeBatch();

    /**
     * 获取自增主键
     *
     * @return 获取自增主键
     */
    String getGeneratedKeys();

}
