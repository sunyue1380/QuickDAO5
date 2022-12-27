package cn.schoolwow.quickdao.dao.statement.query;

import cn.schoolwow.quickdao.dao.statement.AbstractDatabaseStatement;
import cn.schoolwow.quickdao.dao.statement.DatabaseStatementOption;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.util.ParametersUtil;

import java.util.Arrays;
import java.util.List;

/**
 * 根据唯一性约束获取行数
 */
public class SelectCountByIdDatabaseStatement extends AbstractDatabaseStatement {

    public SelectCountByIdDatabaseStatement(DatabaseStatementOption databaseStatementOption, QuickDAOConfig quickDAOConfig) {
        super(databaseStatementOption, quickDAOConfig);
    }

    @Override
    public String getStatement() {
        String key = "selectCountById_" + option.entity.tableName + "_" + quickDAOConfig.databaseProvider.name();
        if (!quickDAOConfig.statementCache.containsKey(key)) {
            StringBuilder builder = new StringBuilder();
            builder.append("select count(1) from " + quickDAOConfig.databaseProvider.escape(option.entity.tableName) + " where ");
            builder.append(quickDAOConfig.databaseProvider.escape(option.entity.id.column) + " = " + (null == option.entity.id.function ? "?" : option.entity.id.function) + " ");
            quickDAOConfig.statementCache.put(key, builder.toString());
        }
        String sql = quickDAOConfig.statementCache.get(key);
        return sql;
    }

    @Override
    public List getParameters(Object instance) {
        Object value = ParametersUtil.getFieldValueFromInstance(instance, option.entity.id.name);
        return Arrays.asList(value);
    }

    @Override
    public String name() {
        return "根据ID获取行数";
    }
}
