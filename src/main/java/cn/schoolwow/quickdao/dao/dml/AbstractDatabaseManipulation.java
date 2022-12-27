package cn.schoolwow.quickdao.dao.dml;

import cn.schoolwow.quickdao.annotation.IdStrategy;
import cn.schoolwow.quickdao.dao.sql.AbstractDatabaseDAO;
import cn.schoolwow.quickdao.dao.statement.DatabaseStatement;
import cn.schoolwow.quickdao.dao.statement.DatabaseStatementOption;
import cn.schoolwow.quickdao.dao.statement.manipulation.*;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.util.ParametersUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class AbstractDatabaseManipulation extends AbstractDatabaseDAO implements DatabaseManipulation {
    private Logger logger = LoggerFactory.getLogger(AbstractDatabaseManipulation.class);

    /**
     * 执行语句选项
     */
    private DatabaseStatementOption option = new DatabaseStatementOption();

    public AbstractDatabaseManipulation(QuickDAOConfig quickDAOConfig) {
        super(quickDAOConfig);
    }

    @Override
    public DatabaseManipulation returnGeneratedKeys(boolean returnGeneratedKeys) {
        option.returnGeneratedKeys = returnGeneratedKeys;
        return this;
    }

    @Override
    public DatabaseManipulation batch(boolean batch) {
        option.batch = batch;
        if (option.batch) {
            //开启了批处理就无法获取自增id
            option.returnGeneratedKeys = false;
        }
        return this;
    }

    @Override
    public DatabaseManipulation perBatchCount(int perBatchCount) {
        option.perBatchCount = perBatchCount;
        return this;
    }

    @Override
    public DatabaseManipulation partColumn(String... fieldNames) {
        option.partColumnSet.addAll(Arrays.asList(fieldNames));
        return this;
    }

    @Override
    public int insert(String tableName, JSONObject instance) {
        option.entity = quickDAOConfig.getDatabaseEntityByTableName(tableName);
        DatabaseStatement databaseStatement = new InsertJSONObjectDatabaseStatement(option, quickDAOConfig);
        int effect = connectionExecutor.returnGeneratedKeys(false)
                .name(databaseStatement.name())
                .sql(databaseStatement.getStatement())
                .parameters(databaseStatement.getParameters(instance))
                .executeUpdate();
        return effect;
    }

    @Override
    public int insert(String tableName, JSONArray jsonArray) {
        option.entity = quickDAOConfig.getDatabaseEntityByTableName(tableName);
        DatabaseStatement databaseStatement = new InsertJSONArrayDatabaseStatement(option, quickDAOConfig);
        connectionExecutor.returnGeneratedKeys(false).name(databaseStatement.name()).sql(databaseStatement.getStatement());
        int effect = 0;
        for (int i = 0; i < jsonArray.size(); i += option.perBatchCount) {
            connectionExecutor.startBatch();
            try {
                int end = Math.min(i + option.perBatchCount, jsonArray.size());
                for (int j = i; j < end; j++) {
                    List parameters = databaseStatement.getParameters(jsonArray.getJSONObject(j));
                    connectionExecutor.parameters(parameters);
                }
                effect += connectionExecutor.executeBatch();
            }finally {
                connectionExecutor.closeBatch();
            }
        }
        return effect;
    }

    @Override
    public int insert(Object instance) {
        if (null == instance) {
            return 0;
        }
        return insert(new Object[]{instance});
    }

    @Override
    public int insert(Object[] instances) {
        if (null == instances || instances.length == 0) {
            return 0;
        }
        option.entity = quickDAOConfig.getEntityByClassName(instances[0].getClass().getName());
        return executeManipulationStatement(instances, new InsertDatabaseStatement(option, quickDAOConfig));
    }

    @Override
    public int insert(Collection instanceCollection) {
        if (null == instanceCollection || instanceCollection.size() == 0) {
            return 0;
        }
        return insert(instanceCollection.toArray(new Object[0]));
    }

    @Override
    public int insertIgnore(Object instance) {
        if (null == instance || exist(instance)) {
            return 0;
        }
        return insert(instance);
    }

    @Override
    public int insertIgnore(Object[] instances) {
        if (null == instances || instances.length == 0) {
            return 0;
        }
        List insertList = new ArrayList();
        for (Object instance : instances) {
            if (!exist(instance)) {
                insertList.add(instance);
            }
        }
        if (insertList.isEmpty()) {
            return 0;
        }
        return insert(insertList.toArray(new Object[0]));
    }

    @Override
    public int insertIgnore(Collection instanceCollection) {
        return insertIgnore(instanceCollection.toArray(new Object[0]));
    }

    @Override
    public int update(Object instance) {
        if (null == instance) {
            return 0;
        }
        return update(new Object[]{instance});
    }

    @Override
    public int update(Object[] instances) {
        if (null == instances || instances.length == 0) {
            return 0;
        }
        option.returnGeneratedKeys = false;
        option.batch = true;
        option.entity = quickDAOConfig.getEntityByClassName(instances[0].getClass().getName());
        int effect = 0;
        if (!option.entity.uniqueProperties.isEmpty()) {
            //根据唯一性约束更新
            effect = executeManipulationStatement(instances, new UpdateByUniqueKeyDatabaseStatement(option, quickDAOConfig));
        } else if (null != option.entity.id) {
            //根据id更新
            effect = executeManipulationStatement(instances, new UpdateByIdDatabaseStatement(option, quickDAOConfig));
        } else {
            logger.warn("实例无唯一性约束又无id,忽略更新操作!");
        }
        return effect;
    }

    @Override
    public int update(Collection instanceCollection) {
        if (null == instanceCollection || instanceCollection.size() == 0) {
            return 0;
        }
        return update(instanceCollection.toArray(new Object[0]));
    }

    @Override
    public int save(Object instance) {
        if (null == instance) {
            return 0;
        }
        if (exist(instance)) {
            return update(instance);
        } else {
            return insert(instance);
        }
    }

    @Override
    public int save(Object[] instances) {
        if (null == instances || instances.length == 0) {
            return 0;
        }
        List insertList = new ArrayList();
        List updateList = new ArrayList();
        int effect = 0;
        for (Object instance : instances) {
            if (exist(instance)) {
                updateList.add(instance);
            } else {
                insertList.add(instance);
            }
        }
        effect += update(updateList.toArray(new Object[0]));
        effect += insert(insertList.toArray(new Object[0]));
        return effect;
    }

    @Override
    public int save(Collection instanceCollection) {
        if (null == instanceCollection || instanceCollection.size() == 0) {
            return 0;
        }
        return save(instanceCollection.toArray(new Object[0]));
    }

    @Override
    public int delete(Object instance) {
        if (null == instance) {
            return 0;
        }
        return delete(new Object[]{instance});
    }

    @Override
    public int delete(Object[] instances) {
        if (null == instances || instances.length == 0) {
            return 0;
        }
        option.returnGeneratedKeys = false;
        option.batch = true;
        option.entity = quickDAOConfig.getEntityByClassName(instances[0].getClass().getName());
        int effect = 0;
        if (!option.entity.uniqueProperties.isEmpty()) {
            //根据唯一性约束删除
            effect = executeManipulationStatement(instances, new DeleteByUniqueKeyDatabaseStatement(option, quickDAOConfig));
        } else if (null != option.entity.id) {
            //根据id删除
            option.columnName = option.entity.id.column;
            effect = executeManipulationStatement(instances, new DeleteByPropertyDatabaseStatement(option, quickDAOConfig));
        } else {
            logger.warn("实例无唯一性约束又无id,忽略删除操作!");
        }
        return effect;
    }

    @Override
    public int delete(Collection instanceCollection) {
        if (null == instanceCollection || instanceCollection.size() == 0) {
            return 0;
        }
        return delete(instanceCollection.toArray(new Object[0]));
    }

    /**
     * 执行操纵语言
     *
     * @param databaseStatement 语言类型
     */
    private int executeManipulationStatement(Object[] instances, DatabaseStatement databaseStatement) {
        int effect = 0;
        connectionExecutor.name(databaseStatement.name()).sql(databaseStatement.getStatement());
        option.entity = quickDAOConfig.getEntityByClassName(instances[0].getClass().getName());
        if (option.batch) {
            effect += executeByBatch(instances, databaseStatement);
        } else {
            effect += executeByOneByOne(instances, databaseStatement);
        }
        return effect;
    }

    /**
     * 使用批处理方式执行语句
     */
    private int executeByBatch(Object[] instances, DatabaseStatement databaseStatement) {
        int effect = 0;
        connectionExecutor.sql(databaseStatement.getStatement()).startBatch();
        try {
            for (int i = 0; i < instances.length; i += option.perBatchCount) {
                int end = Math.min(i + option.perBatchCount, instances.length);
                logger.trace("批处理,总个数:{},当前范围:{}-{}", instances.length, i, end);
                for (int j = i; j < end; j++) {
                    List parameters = databaseStatement.getParameters(instances[j]);
                    connectionExecutor.parameters(parameters);
                }
                effect += connectionExecutor.executeBatch();
                connectionExecutor.clearBatch();
            }
        }finally {
            connectionExecutor.closeBatch();
        }
        return effect;
    }

    /**
     * 使用预处理语句一个一个执行语句
     */
    private int executeByOneByOne(Object[] instances, DatabaseStatement databaseStatement) {
        if (null == option.returnGeneratedKeys) {
            option.returnGeneratedKeys = quickDAOConfig.databaseProvider.returnGeneratedKeys();
        }
        connectionExecutor.returnGeneratedKeys(option.returnGeneratedKeys);

        int effect = 0;
        for (Object instance : instances) {
            List parameters = databaseStatement.getParameters(instance);
            effect += connectionExecutor.parameters(parameters).executeUpdate();
            //设置自增id
            if (option.returnGeneratedKeys && null != option.entity.id && option.entity.id.strategy.equals(IdStrategy.AutoIncrement)) {
                String[] generatedKeysValue = new String[1];
                switch (quickDAOConfig.databaseProvider.name().toLowerCase()) {
                    case "oracle": {
                        String getIdValueSQL = "select " + option.entity.tableName + "_seq.currVal from dual";
                        connectionExecutor.name("获取自增id").sql(getIdValueSQL).executeQuery((resultSet) -> {
                            if (resultSet.next()) {
                                generatedKeysValue[0] = resultSet.getString(1);
                            }
                        });
                    }
                    break;
                    default: {
                        generatedKeysValue[0] = connectionExecutor.getGeneratedKeys();
                    }
                    break;
                }
                ParametersUtil.setGeneratedKeysValue(instance, option.entity, generatedKeysValue[0]);
            }
        }
        return effect;
    }
}
