package cn.schoolwow.quickdao.dao.statement.manipulation;

import cn.schoolwow.quickdao.dao.statement.AbstractDatabaseStatement;
import cn.schoolwow.quickdao.dao.statement.DatabaseStatementOption;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.util.ParametersUtil;

import java.util.Arrays;
import java.util.List;

/**
 * 根据id删除记录语句
 */
public class DeleteByPropertyDatabaseStatement extends AbstractDatabaseStatement {

    public DeleteByPropertyDatabaseStatement(DatabaseStatementOption databaseStatementOption, QuickDAOConfig quickDAOConfig) {
        super(databaseStatementOption, quickDAOConfig);
    }

    @Override
    public String getStatement() {
        String key = "deleteByProperty_" + option.entity.tableName + "_" + option.columnName + "_" + quickDAOConfig.databaseProvider.name();
        if (!quickDAOConfig.statementCache.containsKey(key)) {
            StringBuilder builder = new StringBuilder();
            Property property = option.entity.getPropertyByFieldName(option.columnName);
            builder.append("delete from " + quickDAOConfig.databaseProvider.escape(option.entity.tableName) + " where " + quickDAOConfig.databaseProvider.escape(property.column) + " = ?");
            quickDAOConfig.statementCache.put(key, builder.toString());
        }
        String sql = quickDAOConfig.statementCache.get(key);
        return sql;
    }

    @Override
    public List getParameters(Object instance) {
        Property property = option.entity.getPropertyByFieldName(option.columnName);
        Object value = ParametersUtil.getFieldValueFromInstance(instance, property.name);
        return Arrays.asList(value);
    }

    @Override
    public String name() {
        return "根据指定字段删除记录";
    }
}
