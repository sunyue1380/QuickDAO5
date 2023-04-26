package cn.schoolwow.quickdao.statement.dql.response;

import cn.schoolwow.quickdao.domain.internal.Query;

import java.util.List;

/**获取返回记录个数*/
public class ResponseCountDatabaseStatement extends AbstractDQLResponseDatabaseStatement {

    public ResponseCountDatabaseStatement(Query query) {
        super(query);
    }

    @Override
    public String getStatement() {
        builder.append("select count(1) from ( select " + query.distinct + " ");
        //如果有指定列,则添加指定列
        if (query.column.length() > 0) {
            builder.append(query.column);
        } else {
            builder.append(columns(query.entity, query.tableAliasName));
        }
        builder.append(" from " + query.quickDAOConfig.databaseProvider.escape(query.entity.tableName) + " " + query.tableAliasName);
        addJoinTableStatement();
        builder.append(" " + query.where + " " + query.groupBy + " " + query.having + " ) foo");
        String sql = builder.toString();
        return sql;
    }

    @Override
    public List getParameters() {
        addArraySQLParameters(query, query);
        return parameters;
    }

    @Override
    public String name() {
        return "获取行数";
    }
}
