package cn.schoolwow.quickdao.statement.dql.response;

import cn.schoolwow.quickdao.domain.internal.Query;

import java.util.List;

/**根据查询条件删除记录*/
public class ResponseDeleteDatabaseStatement extends AbstractDQLResponseDatabaseStatement {

    public ResponseDeleteDatabaseStatement(Query query) {
        super(query);
    }

    @Override
    public String getStatement() {
        builder.append("delete from " + query.quickDAOConfig.databaseProvider.escape(query.entity.tableName));
        builder.append(" " + query.where.replace(query.tableAliasName + ".", ""));
        String sql = builder.toString();
        return sql;
    }

    @Override
    public List getParameters() {
        addMainTableParameters(query);
        return parameters;
    }

    @Override
    public String name() {
        return "根据查询条件删除记录";
    }
}
