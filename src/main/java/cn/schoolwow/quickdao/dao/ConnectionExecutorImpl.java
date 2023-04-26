package cn.schoolwow.quickdao.dao;

import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.external.StatementListener;
import cn.schoolwow.quickdao.domain.internal.ThrowingConsumer;
import cn.schoolwow.quickdao.exception.SQLRuntimeException;
import cn.schoolwow.quickdao.util.ParametersUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Collection;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConnectionExecutorImpl implements ConnectionExecutor {
    private Logger logger = LoggerFactory.getLogger(ConnectionExecutor.class);

    /**
     * 数据库连接池
     */
    private DataSource dataSource;

    /**
     * 事务连接
     */
    private Connection transactionConnection;

    /**
     * 批处理语句
     */
    private PreparedStatement batchPrepareStatement;

    /**
     * 数据库名称
     */
    private String databaseName;

    /**
     * 数据库配置项
     */
    private QuickDAOConfig quickDAOConfig;

    /**
     * 是否返回自增主键
     */
    private boolean returnGeneratedKeys;

    /**
     * 语句名称
     */
    private String name;

    /**
     * SQL日志
     */
    private String sql;

    /**
     * 当前参数
     */
    private List parameters;

    /**
     * 自增结果
     */
    private String generatedKeys;

    public ConnectionExecutorImpl(QuickDAOConfig quickDAOConfig) {
        this.dataSource = quickDAOConfig.dataSource;
        this.databaseName = quickDAOConfig.databaseProvider.name();
        this.quickDAOConfig = quickDAOConfig;
    }

    @Override
    public ConnectionExecutor name(String name) {
        this.name = name;
        return this;
    }

    @Override
    public ConnectionExecutor sql(String sql) {
        this.sql = sql;
        return this;
    }

    @Override
    public ConnectionExecutor returnGeneratedKeys(boolean returnGeneratedKeys) {
        this.returnGeneratedKeys = returnGeneratedKeys;
        return this;
    }

    @Override
    public ConnectionExecutor parameters(List parameters) {
        if (null == parameters || parameters.size() == 0) {
            return this;
        }
        this.parameters = parameters;
        return this;
    }

    @Override
    public ConnectionExecutor batchParameters(List parameters) {
        if(null==batchPrepareStatement){
            throw new IllegalArgumentException("请先调用startBatch方法!");
        }
        String formatSQL = ParametersUtil.replaceStatementPlaceholder(sql, parameters);
        try {
            ParametersUtil.setPrepareStatementParameter(batchPrepareStatement, parameters, databaseName);
            batchPrepareStatement.addBatch();
            logger.debug("[更新-预处理],名称:{}, 执行语句:{}", name, formatSQL);
        } catch (SQLException e) {
            logger.warn("添加批处理SQL语句失败,名称:{},原始SQL:{}", name, formatSQL);
            try {
                batchPrepareStatement.close();
            } catch (SQLException ex) {
                logger.error("关闭批处理语句异常", ex);
            }
            throw new SQLRuntimeException(e);
        }
        return this;
    }

    @Override
    public ConnectionExecutor startTransaction() {
        try {
            transactionConnection = dataSource.getConnection();
            transactionConnection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
        return this;
    }

    @Override
    public void setTransactionIsolation(int transactionIsolation) {
        try {
            transactionConnection.setTransactionIsolation(transactionIsolation);
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

    @Override
    public Savepoint setSavePoint(String name) {
        try {
            Savepoint savepoint = transactionConnection.setSavepoint(name);
            return savepoint;
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

    @Override
    public void rollback() {
        try {
            transactionConnection.rollback();
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

    @Override
    public void rollback(Savepoint savePoint) {
        try {
            transactionConnection.rollback(savePoint);
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

    @Override
    public void commit() {
        try {
            transactionConnection.commit();
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

    @Override
    public boolean executeAndCheckExists() {
        AtomicBoolean atomicBoolean = new AtomicBoolean();
        executeQuery((resultSet) -> {
            if (resultSet.next()) {
                atomicBoolean.set(true);
            } else {
                atomicBoolean.set(false);
            }
        });
        return atomicBoolean.get();
    }

    @Override
    public void executeQuery(ThrowingConsumer<ResultSet> resultSetThrowingConsumer) {
        beforeExecute();
        long startTime = System.currentTimeMillis();
        String formatSQL = ParametersUtil.replaceStatementPlaceholder(sql, parameters);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ) {
            ParametersUtil.setPrepareStatementParameter(preparedStatement, parameters, databaseName);
            try (ResultSet resultSet = preparedStatement.executeQuery();) {
                long endTime = System.currentTimeMillis();
                logger.debug("[查询]名称:{},耗时:{}ms,执行语句:{}", name, endTime - startTime, formatSQL);
                resultSetThrowingConsumer.accept(resultSet);
            }
        } catch (SQLException e) {
            logger.warn("SQL语句执行失败,名称:{},原始SQL:{}", name, formatSQL);
            throw new SQLRuntimeException(e);
        }
    }

    @Override
    public int executeUpdate() {
        beforeExecute();
        String formatSQL = ParametersUtil.replaceStatementPlaceholder(sql, parameters);
        long startTime = System.currentTimeMillis();
        String executeSQL = sql;
        int indexOfSemicolon = sql.indexOf(";");
        try {
            int effect = 0;
            if (indexOfSemicolon>=0&&indexOfSemicolon!=sql.length()-1) {
                StringTokenizer st = new StringTokenizer(sql, ";");
                while (st.hasMoreTokens()) {
                    executeSQL = st.nextToken();
                    effect += doExecuteUpdate(executeSQL, null);
                }
            } else {
                effect = doExecuteUpdate(executeSQL, parameters);
            }
            long endTime = System.currentTimeMillis();
            logger.debug("[更新]名称:{},耗时:{}ms,影响行数:{},执行语句:{}", name, endTime - startTime, effect, formatSQL);
            return effect;
        } catch (SQLException e) {
            logger.warn("SQL语句执行失败,名称:{},原始SQL:{}", name, executeSQL);
            if(indexOfSemicolon>=0&&indexOfSemicolon!=sql.length()-1){
                formatSQL = formatSQL.replace(";", ";\r\n");
                logger.warn("SQL语句执行失败,名称:{},完整执行SQL:\n{}", name, formatSQL);
            }
            throw new SQLRuntimeException(e);
        }
    }

    @Override
    public ConnectionExecutor startBatch() {
        try {
            if (null == transactionConnection) {
                batchPrepareStatement = dataSource.getConnection().prepareStatement(sql);
            } else {
                batchPrepareStatement = transactionConnection.prepareStatement(sql);
            }
        } catch (SQLException e) {
            logger.warn("SQL语句执行失败,名称:{},原始SQL:{}", name, sql);
            throw new SQLRuntimeException(e);
        }
        return this;
    }

    @Override
    public int executeBatch() {
        try {
            int effect = 0;
            int[] batches = batchPrepareStatement.executeBatch();
            for (int batch : batches) {
                switch (batch) {
                    case Statement.SUCCESS_NO_INFO: {
                        effect += 1;
                    }
                    break;
                    case Statement.EXECUTE_FAILED: {
                    }
                    break;
                    default: {
                        effect += batch;
                    }
                }
            }
            return effect;
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

    @Override
    public ConnectionExecutor clearBatch() {
        if (null != batchPrepareStatement) {
            try {
                batchPrepareStatement.clearBatch();
            } catch (SQLException e) {
                throw new SQLRuntimeException(e);
            }
        }
        return this;
    }

    @Override
    public ConnectionExecutor closeBatch() {
        try {
            if (null != batchPrepareStatement) {
                batchPrepareStatement.close();
                if(null==transactionConnection){
                    //如果没有开启事务,则该批处理对应连接也要关闭
                    batchPrepareStatement.getConnection().close();
                }
            }
        }catch (SQLException e){
            throw new SQLRuntimeException(e);
        }
        return this;
    }

    @Override
    public String getGeneratedKeys() {
        return generatedKeys;
    }

    @Override
    public void close() {
        closeBatch();
        try {
            if (null != transactionConnection) {
                transactionConnection.close();
                transactionConnection = null;
            }
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

    /**执行sql操作前的预处理*/
    private void beforeExecute(){
        //由于一个方法可能在内部调用多次executeQuery或者executeUpdate,执行sql前检查参数是否需要清空
        if(!this.sql.contains("?")&&null!=this.parameters&&!this.parameters.isEmpty()){
            this.parameters = null;
        }
        for (StatementListener statementListener : quickDAOConfig.databaseOption.statementListener) {
            String userSQL = statementListener.beforeExecute(name, sql, parameters);
            if (null != userSQL && !userSQL.isEmpty()) {
                this.sql = userSQL;
            }
        }
    }

    /**
     * 执行更新操作
     */
    private int doExecuteUpdate(String sql, Collection parameters) throws SQLException {
        Connection connection = transactionConnection;
        if (null == connection) {
            connection = dataSource.getConnection();
        }
        try (PreparedStatement preparedStatement = returnGeneratedKeys ?
                connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS) :
                connection.prepareStatement(sql);
        ) {
            ParametersUtil.setPrepareStatementParameter(preparedStatement, parameters, databaseName);
            int effect = preparedStatement.executeUpdate();
            if (returnGeneratedKeys && !"oracle".equalsIgnoreCase(databaseName)) {
                try (ResultSet resultSet = preparedStatement.getGeneratedKeys();) {
                    if (resultSet.next()) {
                        generatedKeys = resultSet.getString(1);
                    }
                }
            }
            return effect;
        } finally {
            if (null == transactionConnection) {
                connection.close();
            }
        }
    }

}
