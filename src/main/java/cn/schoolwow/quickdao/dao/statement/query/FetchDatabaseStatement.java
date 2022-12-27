package cn.schoolwow.quickdao.dao.statement.query;

import cn.schoolwow.quickdao.dao.statement.AbstractDatabaseStatement;
import cn.schoolwow.quickdao.dao.statement.DatabaseStatementOption;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;

import java.util.Arrays;
import java.util.List;

public class FetchDatabaseStatement extends AbstractDatabaseStatement {

    public FetchDatabaseStatement(DatabaseStatementOption databaseStatementOption, QuickDAOConfig quickDAOConfig) {
        super(databaseStatementOption, quickDAOConfig);
    }

    @Override
    public String getStatement() {
        String key = "fetch_" + option.entity.tableName + "_" + option.columnName + "_" + quickDAOConfig.databaseProvider.name();
        if (!quickDAOConfig.statementCache.containsKey(key)) {
            StringBuilder builder = new StringBuilder("select ");
            builder.append(columns(option.entity, "t"));
            Property property = option.entity.getPropertyByFieldName(option.columnName);
            builder.append(" from " + quickDAOConfig.databaseProvider.escape(option.entity.tableName) + " t where t." + quickDAOConfig.databaseProvider.escape(property.column) + " = " + (null == property.function ? "?" : property.function) + "");
            quickDAOConfig.statementCache.put(key, builder.toString());
        }
        String sql = quickDAOConfig.statementCache.get(key);
        return sql;
    }

    @Override
    public List getParameters(Object instance) {
        return Arrays.asList(instance);
    }

    @Override
    public String name() {
        return "单字段查询";
    }

}
