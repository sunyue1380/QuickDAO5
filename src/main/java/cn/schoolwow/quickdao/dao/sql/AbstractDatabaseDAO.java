package cn.schoolwow.quickdao.dao.sql;

import cn.schoolwow.quickdao.dao.ConnectionExecutor;
import cn.schoolwow.quickdao.dao.ConnectionExecutorImpl;
import cn.schoolwow.quickdao.dao.statement.DatabaseStatement;
import cn.schoolwow.quickdao.dao.statement.DatabaseStatementOption;
import cn.schoolwow.quickdao.dao.statement.query.FetchDatabaseStatement;
import cn.schoolwow.quickdao.dao.statement.query.FetchNullDatabaseStatement;
import cn.schoolwow.quickdao.dao.statement.query.SelectCountByIdDatabaseStatement;
import cn.schoolwow.quickdao.dao.statement.query.SelectCountByUniqueKeyDatabaseStatement;
import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.internal.SFunction;
import cn.schoolwow.quickdao.util.LambdaUtils;
import cn.schoolwow.quickdao.util.ResponseUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.sql.ResultSetMetaData;
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
        DatabaseStatementOption databaseStatementOption = new DatabaseStatementOption();
        DatabaseStatement databaseStatement = null;

        databaseStatementOption.entity = quickDAOConfig.getEntityByClassName(instance.getClass().getName());
        if (!databaseStatementOption.entity.uniqueProperties.isEmpty()) {
            databaseStatement = new SelectCountByUniqueKeyDatabaseStatement(databaseStatementOption, quickDAOConfig);
        } else if (null != databaseStatementOption.entity.id) {
            databaseStatement = new SelectCountByIdDatabaseStatement(databaseStatementOption, quickDAOConfig);
        } else {
            throw new IllegalArgumentException("该实例无唯一性约束又无id值,无法判断!类名:" + instance.getClass().getName());
        }
        String sql = databaseStatement.getStatement();
        int[] count = new int[1];
        connectionExecutor.name(databaseStatement.name())
                .sql(sql)
                .parameters(databaseStatement.getParameters(instance))
                .executeQuery(resultSet -> {
                    if (resultSet.next()) {
                        count[0] = resultSet.getInt(1);
                    }
                });
        return count[0] > 0;
    }

    @Override
    public boolean existAny(Object... instances) {
        for (Object instance : instances) {
            if (exist(instance)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean existAll(Object... instances) {
        for (Object instance : instances) {
            if (!exist(instance)) {
                return false;
            }
        }
        return true;
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
        JSONArray array = fetchList(entity, fieldName, value);
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
        connectionExecutor.name("用户自定义").sql(selectSQL).parameters(Arrays.asList(parameters)).executeQuery(resultSet -> {
            ResultSetMetaData metaData = resultSet.getMetaData();
            String[] columnLables = new String[metaData.getColumnCount()];
            for (int i = 1; i <= columnLables.length; i++) {
                columnLables[i - 1] = metaData.getColumnLabel(i);
            }
            while (resultSet.next()) {
                JSONObject o = new JSONObject();
                for (int i = 1; i <= columnLables.length; i++) {
                    o.put(columnLables[i - 1], resultSet.getObject(i));
                }
                array.add(o);
            }
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
        String key = "deleteByProperty_" + tableName + "_" + columnName + "_" + quickDAOConfig.databaseProvider.name();
        if (!quickDAOConfig.statementCache.containsKey(key)) {
            StringBuilder builder = new StringBuilder();
            builder.append("delete from " + quickDAOConfig.databaseProvider.escape(tableName) + " where " + quickDAOConfig.databaseProvider.escape(columnName) + " = ?");
            quickDAOConfig.statementCache.put(key, builder.toString());
        }
        String sql = quickDAOConfig.statementCache.get(key);
        int effect = connectionExecutor.name("根据单个字段删除").sql(sql).parameters(Arrays.asList(value)).executeUpdate();
        return effect;
    }

    @Override
    public <T> int delete(Class<T> clazz, SFunction<T, ?> field, Object value) {
        String fieldName = LambdaUtils.resolveLambdaProperty(field);
        return delete(clazz, fieldName, value);
    }

    @Override
    public int clear(Class clazz) {
        String key = "clear_" + clazz.getName() + "_" + quickDAOConfig.databaseProvider.name();
        if (!quickDAOConfig.statementCache.containsKey(key)) {
            Entity entity = quickDAOConfig.getEntityByClassName(clazz.getName());
            quickDAOConfig.statementCache.put(key, "delete from " + quickDAOConfig.databaseProvider.escape(entity.tableName));
        }
        String sql = quickDAOConfig.statementCache.get(key);
        int effect = connectionExecutor.name("清空表").sql(sql).executeUpdate();
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
    private JSONArray fetchList(Entity entity, String fieldName, Object value) {
        DatabaseStatementOption databaseStatementOption = new DatabaseStatementOption();
        databaseStatementOption.entity = entity;
        Property property = databaseStatementOption.entity.getPropertyByFieldName(fieldName);
        databaseStatementOption.columnName = property.column;
        DatabaseStatement databaseStatement = (null == value) ? new FetchNullDatabaseStatement(databaseStatementOption, quickDAOConfig) : new FetchDatabaseStatement(databaseStatementOption, quickDAOConfig);
        JSONArray array = new JSONArray();
        connectionExecutor.name(databaseStatement.name())
                .sql(databaseStatement.getStatement())
                .parameters(databaseStatement.getParameters(value))
                .executeQuery(resultSet -> {
                    while (resultSet.next()) {
                        JSONObject object = ResponseUtil.getObject(databaseStatementOption.entity, "t", resultSet, quickDAOConfig.databaseProvider);
                        array.add(object);
                    }
                });
        return array;
    }
}
