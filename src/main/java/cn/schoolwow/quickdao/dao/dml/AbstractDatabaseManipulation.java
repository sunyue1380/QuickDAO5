package cn.schoolwow.quickdao.dao.dml;

import cn.schoolwow.quickdao.dao.sql.AbstractDatabaseDAO;
import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.internal.ManipulationOption;
import cn.schoolwow.quickdao.statement.dml.DMLDatabaseStatement;
import cn.schoolwow.quickdao.statement.dml.instance.*;
import cn.schoolwow.quickdao.statement.dml.json.*;
import cn.schoolwow.quickdao.util.ValidateUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;

public class AbstractDatabaseManipulation extends AbstractDatabaseDAO implements DatabaseManipulation {
    private Logger logger = LoggerFactory.getLogger(AbstractDatabaseManipulation.class);

    /**
     * 执行语句选项
     */
    private ManipulationOption option = new ManipulationOption();

    public AbstractDatabaseManipulation(QuickDAOConfig quickDAOConfig) {
        super(quickDAOConfig);
        option.connectionExecutor = connectionExecutor;
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
    public DatabaseManipulation uniqueFieldNames(String... uniqueFieldNames) {
        option.uniqueFieldNames.addAll(Arrays.asList(uniqueFieldNames));
        return this;
    }

    @Override
    public int insert(String tableName, JSONObject instance) {
        return executeUpdate(new InsertJSONObjectDatabaseStatement(tableName, instance, option, quickDAOConfig));
    }

    @Override
    public int insert(String tableName, JSONArray instances) {
        return executeUpdate(new InsertJSONArrayDatabaseStatement(tableName, instances, option, quickDAOConfig));
    }

    @Override
    public int insertIgnore(String tableName, JSONObject instance) {
        ValidateUtil.checkUniqueFieldNames(option);
        return executeUpdate(new InsertIgnoreJSONObjectDatabaseStatement(tableName, instance, option, quickDAOConfig));
    }

    @Override
    public int insertIgnore(String tableName, JSONArray instances) {
        return executeUpdate(new InsertIgnoreJSONArrayDatabaseStatement(tableName, instances, option, quickDAOConfig));
    }

    @Override
    public int update(String tableName, JSONObject instance) {
        ValidateUtil.checkUniqueFieldNames(option);
        return executeUpdate(new UpdateJSONObjectDatabaseStatement(tableName, instance, option, quickDAOConfig));
    }

    @Override
    public int update(String tableName, JSONArray instances) {
        ValidateUtil.checkUniqueFieldNames(option);
        return executeUpdate(new UpdateJSONArrayDatabaseStatement(tableName, instances, option, quickDAOConfig));
    }

    @Override
    public int delete(String tableName, JSONObject instance) {
        ValidateUtil.checkUniqueFieldNames(option);
        return executeUpdate(new DeleteJSONObjectDatabaseStatement(tableName, instance, option, quickDAOConfig));
    }

    @Override
    public int delete(String tableName, JSONArray instances) {
        ValidateUtil.checkUniqueFieldNames(option);
        return executeUpdate(new DeleteJSONArrayDatabaseStatement(tableName, instances, option, quickDAOConfig));
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
        DMLDatabaseStatement databaseStatement = option.batch?
                new InsertInstanceBatchDatabaseStatement(instances, option, quickDAOConfig):
                new InsertInstanceDatabaseStatement(instances, option, quickDAOConfig);
        int effect = databaseStatement.executeUpdate();
        return effect;
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
        return executeUpdate(new InsertIgnoreInstanceDatabaseStatement(instances, option, quickDAOConfig));
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
        return executeUpdate(new UpdateInstanceDatabaseStatement(instances, option, quickDAOConfig));
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
        return executeUpdate(new SaveInstanceDatabaseStatement(instances, option, quickDAOConfig));
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
        return executeUpdate(new DeleteInstanceDatabaseStatement(instances, option, quickDAOConfig));
    }

    @Override
    public int delete(Collection instanceCollection) {
        if (null == instanceCollection || instanceCollection.size() == 0) {
            return 0;
        }
        return delete(instanceCollection.toArray(new Object[0]));
    }

    @Override
    public int truncate(Class clazz) {
        Entity entity = quickDAOConfig.getEntityByClassName(clazz.getName());
        return truncate(entity.tableName);
    }

    @Override
    public int truncate(String tableName) {
        int effect = rawUpdate("truncate table " + quickDAOConfig.databaseProvider.escape(tableName));
        return effect;
    }

    /**执行更新语句*/
    private int executeUpdate(DMLDatabaseStatement databaseStatement){
        int effect = databaseStatement.executeUpdate();
        return effect;
    }

}
