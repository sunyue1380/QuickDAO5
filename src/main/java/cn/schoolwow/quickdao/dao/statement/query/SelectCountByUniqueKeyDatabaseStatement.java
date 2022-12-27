package cn.schoolwow.quickdao.dao.statement.query;

import cn.schoolwow.quickdao.dao.statement.AbstractDatabaseStatement;
import cn.schoolwow.quickdao.dao.statement.DatabaseStatementOption;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.util.ParametersUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * 根据唯一性约束获取行数
 */
public class SelectCountByUniqueKeyDatabaseStatement extends AbstractDatabaseStatement {

    public SelectCountByUniqueKeyDatabaseStatement(DatabaseStatementOption databaseStatementOption, QuickDAOConfig quickDAOConfig) {
        super(databaseStatementOption, quickDAOConfig);
    }

    @Override
    public String getStatement() {
        String key = "selectCountByUniqueKey_" + option.entity.tableName + "_" + quickDAOConfig.databaseProvider.name();
        if (!quickDAOConfig.statementCache.containsKey(key)) {
            StringBuilder builder = new StringBuilder();
            builder.append("select count(1) from " + quickDAOConfig.databaseProvider.escape(option.entity.tableName) + " where ");
            for (Property property : option.entity.uniqueProperties) {
                builder.append(quickDAOConfig.databaseProvider.escape(property.column) + "= " + (null == property.function ? "?" : property.function) + " and ");
            }
            builder.delete(builder.length() - 5, builder.length());
            quickDAOConfig.statementCache.put(key, builder.toString());
        }
        String sql = quickDAOConfig.statementCache.get(key);
        return sql;
    }

    @Override
    public List getParameters(Object instance) {
        List parameterList = new ArrayList();
        for (Property property : option.entity.uniqueProperties) {
            Object value = ParametersUtil.getFieldValueFromInstance(instance, property.name);
            parameterList.add(value);
        }
        return parameterList;
    }

    @Override
    public String name() {
        return "根据唯一性约束获取行数";
    }
}
