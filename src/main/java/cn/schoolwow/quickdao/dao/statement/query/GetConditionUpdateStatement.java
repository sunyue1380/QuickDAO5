package cn.schoolwow.quickdao.dao.statement.query;

import cn.schoolwow.quickdao.dao.statement.DatabaseStatementOption;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.internal.Query;

import java.util.List;

/**
 * 根据查询条件进行更新
 */
public class GetConditionUpdateStatement extends AbstractResponseDatabaseStatement {
    private Query query;

    public GetConditionUpdateStatement(DatabaseStatementOption databaseStatementOption, QuickDAOConfig quickDAOConfig) {
        super(databaseStatementOption, quickDAOConfig);
        query = databaseStatementOption.query;
    }

    @Override
    public String getStatement() {
        sqlBuilder.append("update " + query.quickDAOConfig.databaseProvider.escape(query.entity.tableName) + " ");
        sqlBuilder.append(query.setBuilder.toString() + " " + query.where.replace(query.tableAliasName + ".", ""));
        String sql = sqlBuilder.toString();
        return sql;
    }

    @Override
    public List getParameters(Object instance) {
        for (Object parameter : query.updateParameterList) {
            parameters.add(parameter);
        }
        addMainTableParameters(query);
        return parameters;
    }

    @Override
    public String name() {
        return "根据查询条件更新记录";
    }

}
