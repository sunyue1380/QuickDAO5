package cn.schoolwow.quickdao.statement.dql.response;

import cn.schoolwow.quickdao.domain.internal.Query;

import java.util.List;

/**根据查询条件更新记录*/
public class ResponseUpdateDatabaseStatement extends AbstractDQLResponseDatabaseStatement {

    public ResponseUpdateDatabaseStatement(Query query) {
        super(query);
    }

    @Override
    public String getStatement() {
        builder.append("update " + query.quickDAOConfig.databaseProvider.escape(query.entity.tableName) + " ");
        builder.append(query.setBuilder.toString() + " " + query.where.replace(query.tableAliasName + ".", ""));
        String sql = builder.toString();
        return sql;
    }

    @Override
    public List getParameters() {
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
