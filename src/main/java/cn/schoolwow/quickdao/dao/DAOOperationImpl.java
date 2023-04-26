package cn.schoolwow.quickdao.dao;

import cn.schoolwow.quickdao.dao.dql.condition.AbstractCondition;
import cn.schoolwow.quickdao.dao.dql.condition.Condition;
import cn.schoolwow.quickdao.dao.transaction.Transaction;
import cn.schoolwow.quickdao.dao.transaction.TransactionInvocationHandler;
import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.internal.Query;
import cn.schoolwow.quickdao.provider.DatabaseProvider;
import cn.schoolwow.quickdao.statement.dql.response.GetResponseArrayDatabaseStatement;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Map;
import java.util.function.Consumer;

public class DAOOperationImpl implements DAOOperation {
    private Logger logger = LoggerFactory.getLogger(DAOOperationImpl.class);

    private QuickDAOConfig quickDAOConfig;

    public DAOOperationImpl(QuickDAOConfig quickDAOConfig) {
        this.quickDAOConfig = quickDAOConfig;
    }

    @Override
    public <T> Condition<T> query(Class<T> clazz) {
        Entity entity = quickDAOConfig.getEntityByClassName(clazz.getName());
        if (null == entity) {
            throw new IllegalArgumentException("不存在的实体类:" + clazz.getName() + "!");
        }
        return query(entity);
    }

    @Override
    public Condition query(String tableName) {
        Entity entity = quickDAOConfig.getDatabaseEntityByTableName(tableName);
        if (null == entity) {
            for (String virtualTableName : quickDAOConfig.databaseOption.virtualTableNameList) {
                if (virtualTableName.equalsIgnoreCase(tableName)) {
                    entity = new Entity();
                    entity.tableName = virtualTableName;
                    entity.properties = new ArrayList<>();
                    break;
                }
            }
        }
        if (null == entity) {
            throw new IllegalArgumentException("不存在的表名:" + tableName + "!");
        }
        return query(entity);
    }

    @Override
    public Condition query(Condition condition) {
        Query fromQuery = ((AbstractCondition) condition).query;
        condition.execute();

        Entity entity = new Entity();
        entity.clazz = JSONObject.class;
        entity.properties = new ArrayList<>();
        AbstractCondition condition1 = (AbstractCondition) query(entity);
        condition1.query.fromQuery = fromQuery;
        entity.tableName = "( " + new GetResponseArrayDatabaseStatement(fromQuery).getStatement() + ")";
        return condition1;
    }

    @Override
    public Transaction startTransaction() {
        TransactionInvocationHandler transactionInvocationHandler = new TransactionInvocationHandler(quickDAOConfig);
        Transaction transactionProxy = (Transaction) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class<?>[]{Transaction.class}, transactionInvocationHandler);
        return transactionProxy;
    }

    @Override
    public void startTransaction(Consumer<Transaction> transactionConsumer) {
        Transaction transaction = startTransaction();
        try {
            transactionConsumer.accept(transaction);
            transaction.commit();
        } catch (Exception e) {
            transaction.rollback();
            throw new RuntimeException(e);
        } finally {
            transaction.close();
        }
    }

    @Override
    public DataSource getDataSource() {
        return quickDAOConfig.dataSource;
    }

    @Override
    public Map<String, Entity> getEntityMap() {
        return quickDAOConfig.entityMap;
    }

    @Override
    public Entity getEntity(Class clazz) {
        return quickDAOConfig.entityMap.values().stream().filter(entity -> entity.clazz.getName().equalsIgnoreCase(clazz.getName())).findFirst().orElse(null);
    }

    @Override
    public Entity getEntity(String tableName) {
        return quickDAOConfig.entityMap.values().stream().filter(entity -> entity.tableName.equalsIgnoreCase(tableName)).findFirst().orElse(null);
    }

    @Override
    public DatabaseProvider getDatabaseProvider() {
        return quickDAOConfig.databaseProvider;
    }

    @Override
    public QuickDAOConfig getQuickDAOConfig() {
        return this.quickDAOConfig;
    }

    /**
     * 数据库查询
     */
    private Condition query(Entity entity) {
        Query query = new Query();
        query.entity = entity;
        query.quickDAOConfig = quickDAOConfig;
        return quickDAOConfig.databaseProvider.getConditionInstance(query);
    }
}