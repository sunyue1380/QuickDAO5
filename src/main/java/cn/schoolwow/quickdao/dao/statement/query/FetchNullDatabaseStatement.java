package cn.schoolwow.quickdao.dao.statement.query;

import cn.schoolwow.quickdao.dao.statement.AbstractDatabaseStatement;
import cn.schoolwow.quickdao.dao.statement.DatabaseStatementOption;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;

import java.util.Arrays;
import java.util.List;

public class FetchNullDatabaseStatement extends AbstractDatabaseStatement {

    public FetchNullDatabaseStatement(DatabaseStatementOption databaseStatementOption, QuickDAOConfig quickDAOConfig) {
        super(databaseStatementOption, quickDAOConfig);
    }

    @Override
    public String getStatement() {
        String key = "fetchNull_" + option.entity.tableName + "_" + option.columnName + "_" + quickDAOConfig.databaseProvider.name();
        if (!quickDAOConfig.statementCache.containsKey(key)) {
            StringBuilder builder = new StringBuilder("select ");
            builder.append(columns(option.entity, "t"));
            Property property = option.entity.getPropertyByFieldName(option.columnName);
            builder.append(" from " + quickDAOConfig.databaseProvider.escape(option.entity.tableName) + " as t where t." + quickDAOConfig.databaseProvider.escape(property.column) + " is null");
            quickDAOConfig.statementCache.put(key, builder.toString());
        }
        String sql = quickDAOConfig.statementCache.get(key);
        return sql;
    }

    @Override
    public List getParameters(Object instance) {
        return Arrays.asList();
    }

    @Override
    public String name() {
        return "单字段null数据查询";
    }

}
