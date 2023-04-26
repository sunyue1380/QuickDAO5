package cn.schoolwow.quickdao.dao.sql;

import cn.schoolwow.quickdao.dao.ConnectionExecutor;
import cn.schoolwow.quickdao.dao.ConnectionExecutorImpl;
import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.internal.SFunction;
import cn.schoolwow.quickdao.statement.dql.DQLDatabaseStatement;
import cn.schoolwow.quickdao.statement.dql.instance.ExistAnyDatabaseStatement;
import cn.schoolwow.quickdao.statement.dql.instance.ExistDatabaseStatement;
import cn.schoolwow.quickdao.statement.dql.instance.FetchListDatabaseStatement;
import cn.schoolwow.quickdao.statement.dql.instance.FetchNullDatabaseStatement;
import cn.schoolwow.quickdao.util.LambdaUtils;
import cn.schoolwow.quickdao.util.ResponseUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * 数据库操作实例
 */
public class AbstractDatabaseDAO implements DatabaseDAO {
    /**
     * 数据库执行器
     */
    public ConnectionExecutor connectionExecutor;

    /**
     * 数据库配置对象
     */
    protected QuickDAOConfig quickDAOConfig;

    public AbstractDatabaseDAO(QuickDAOConfig quickDAOConfig) {
        this.quickDAOConfig = quickDAOConfig;
        this.connectionExecutor = new ConnectionExecutorImpl(quickDAOConfig);
    }

    @Override
    public boolean exist(Object instance) {
        if (null == instance) {
            return false;
        }
        int count = new ExistDatabaseStatement(instance, quickDAOConfig).getCount();
        return count>0;
    }

    @Override
    public boolean existAny(Object... instances) {
        int count = new ExistAnyDatabaseStatement(instances, quickDAOConfig).getCount();
        return count>0;
    }

    @Override
    public boolean existAll(Object... instances) {
        int count = new ExistAnyDatabaseStatement(instances, quickDAOConfig).getCount();
        return count==instances.length;
    }

    @Override
    public boolean existAny(Collection instances) {
        return existAny(instances.toArray());
    }

    @Override
    public boolean existAll(Collection instances) {
        return existAll(instances.toArray());
    }

    @Override
    public <T> T fetch(Class<T> clazz, long id) {
        Entity entity = quickDAOConfig.getEntityByClassName(clazz.getName());
        return fetch(clazz, entity.id.column, id);
    }

    @Override
    public <T> T fetch(Class<T> clazz, String field, Object value) {
        List<T> list = fetchList(clazz, field, value);
        if (null == list || list.isEmpty()) {
            return null;
        }
        return list.get(0);
    }

    @Override
    public <T> List<T> fetchList(Class<T> clazz, String fieldName, Object value) {
        Entity entity = quickDAOConfig.getEntityByClassName(clazz.getName());
        if (null == entity) {
            throw new IllegalArgumentException("数据库表不存在!实体类名:" + clazz.getName());
        }
        Property property = entity.getPropertyByFieldName(fieldName);
        if(null==property){
            throw new IllegalArgumentException("数据库表字段不存在!实体类名:" + clazz.getName()+",字段:"+fieldName);
        }
        JSONArray array = fetchList(entity, property.column, value);
        return array.toJavaList(clazz);
    }

    @Override
    public <T> T fetch(Class<T> clazz, SFunction<T, ?> field, Object value) {
        String convertField = LambdaUtils.resolveLambdaProperty(field);
        return fetch(clazz, convertField, value);
    }

    @Override
    public <T> List<T> fetchList(Class<T> clazz, SFunction<T, ?> field, Object value) {
        String convertField = LambdaUtils.resolveLambdaProperty(field);
        return fetchList(clazz, convertField, value);
    }

    @Override
    public JSONObject fetch(String tableName, String columnName, Object value) {
        JSONArray array = fetchList(tableName, columnName, value);
        if (null == array || array.isEmpty()) {
            return null;
        }
        return array.getJSONObject(0);
    }

    @Override
    public JSONArray fetchList(String tableName, String columnName, Object value) {
        Entity entity = quickDAOConfig.getDatabaseEntityByTableName(tableName);
        if (null == entity) {
            throw new IllegalArgumentException("数据库表不存在!表名:" + tableName);
        }
        JSONArray array = fetchList(entity, columnName, value);
        return array;
    }

    @Override
    public JSONArray rawSelect(String selectSQL, Object... parameters) {
        JSONArray array = new JSONArray();
        connectionExecutor.name("用户自定义").sql(selectSQL)
                .parameters(Arrays.asList(parameters))
                .executeQuery(resultSet -> {
                    ResponseUtil.getRawSelectArray(resultSet, array);
                });
        return array;
    }

    @Override
    public int delete(Class clazz, long id) {
        Entity entity = quickDAOConfig.getEntityByClassName(clazz.getName());
        return delete(clazz, entity.id.column, id);
    }

    @Override
    public int delete(Class clazz, String id) {
        Entity entity = quickDAOConfig.getEntityByClassName(clazz.getName());
        return delete(clazz, entity.id.column, id);
    }

    @Override
    public int delete(Class clazz, String field, Object value) {
        Entity entity = quickDAOConfig.getEntityByClassName(clazz.getName());
        String columnName = entity.getColumnNameByFieldName(field);
        return delete(entity.tableName, columnName, value);
    }

    @Override
    public int delete(String tableName, String columnName, Object value) {
        int effect = rawUpdate("delete from " + quickDAOConfig.databaseProvider.escape(tableName) + " where " + quickDAOConfig.databaseProvider.escape(columnName) + " = ?", value);
        return effect;
    }

    @Override
    public <T> int delete(Class<T> clazz, SFunction<T, ?> field, Object value) {
        String fieldName = LambdaUtils.resolveLambdaProperty(field);
        return delete(clazz, fieldName, value);
    }

    @Override
    public int clear(Class clazz) {
        Entity entity = quickDAOConfig.getEntityByClassName(clazz.getName());
        int effect = rawUpdate("delete from " + quickDAOConfig.databaseProvider.escape(entity.tableName));
        return effect;
    }

    @Override
    public int rawUpdate(String updateSQL, Object... parameters) {
        int effect = connectionExecutor.name("用户自定义").sql(updateSQL).parameters(Arrays.asList(parameters)).executeUpdate();
        return effect;
    }

    /**
     * 根据单个属性查询
     */
    private JSONArray fetchList(Entity entity, String columnName, Object value) {
        DQLDatabaseStatement databaseStatement = (null == value) ?
                new FetchNullDatabaseStatement(entity, columnName, quickDAOConfig) :
                new FetchListDatabaseStatement(entity, columnName, value, quickDAOConfig);
        JSONArray array = databaseStatement.getArray();
        return array;
    }

}
