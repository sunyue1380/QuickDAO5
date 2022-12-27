package cn.schoolwow.quickdao.dao.statement.query;

import cn.schoolwow.quickdao.dao.statement.DatabaseStatementOption;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.internal.Query;

import java.util.List;

/**
 * 根据查询条件进行更新
 */
public class GetConditionDeleteStatement extends AbstractResponseDatabaseStatement {
    private Query query;

    public GetConditionDeleteStatement(DatabaseStatementOption databaseStatementOption, QuickDAOConfig quickDAOConfig) {
        super(databaseStatementOption, quickDAOConfig);
        query = databaseStatementOption.query;
    }

    @Override
    public String getStatement() {
        sqlBuilder.append("delete from " + query.quickDAOConfig.databaseProvider.escape(query.entity.tableName));
        sqlBuilder.append(" " + query.where.replace(query.tableAliasName + ".", ""));
        String sql = sqlBuilder.toString();
        return sql;
    }

    @Override
    public List getParameters(Object instance) {
        addMainTableParameters(query);
        return parameters;
    }

    @Override
    public String name() {
        return "根据查询条件删除记录";
    }

}
