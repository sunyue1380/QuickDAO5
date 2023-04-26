package cn.schoolwow.quickdao.statement.dql.instance;

import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.statement.dql.AbstractDQLDatabaseStatement;
import cn.schoolwow.quickdao.util.ParametersUtil;

import java.util.ArrayList;
import java.util.List;

/**根据实体类唯一性约束查询个数*/
public class SelectCountByUniqueKeyDatabaseStatement extends AbstractDQLDatabaseStatement {
    /**实体类信息*/
    private Entity entity;

    /**实例*/
    private Object instance;

    public SelectCountByUniqueKeyDatabaseStatement(Object instance, QuickDAOConfig quickDAOConfig) {
        super(quickDAOConfig);
        this.entity = quickDAOConfig.getEntityByClassName(instance.getClass().getName());
        this.instance = instance;
    }

    @Override
    public String getStatement() {
        String key = "selectCountByUniqueKey_" + entity.tableName + "_" + quickDAOConfig.databaseProvider.name();
        if (!quickDAOConfig.statementCache.containsKey(key)) {
            StringBuilder builder = new StringBuilder();
            builder.append("select count(1) from " + quickDAOConfig.databaseProvider.escape(entity.tableName) + " where ");
            for (Property property : entity.uniqueProperties) {
                builder.append(quickDAOConfig.databaseProvider.escape(property.column) + "= " + (null == property.function ? "?" : property.function) + " and ");
            }
            builder.delete(builder.length() - 5, builder.length());
            quickDAOConfig.statementCache.put(key, builder.toString());
        }
        String sql = quickDAOConfig.statementCache.get(key);
        return sql;
    }

    @Override
    public List getParameters() {
        List parameterList = new ArrayList();
        for (Property property : entity.uniqueProperties) {
            Object value = ParametersUtil.getFieldValueFromInstance(instance, property.name);
            parameterList.add(value);
        }
        return parameterList;
    }

    @Override
    public String name() {
        return "根据实体类唯一性约束查询个数";
    }
}
