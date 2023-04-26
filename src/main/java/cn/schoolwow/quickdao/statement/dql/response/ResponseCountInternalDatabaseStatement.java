package cn.schoolwow.quickdao.statement.dql.response;

import cn.schoolwow.quickdao.domain.internal.Query;

import java.util.List;

/**获取返回记录个数(内部使用)*/
public class ResponseCountInternalDatabaseStatement extends AbstractDQLResponseDatabaseStatement {

    public ResponseCountInternalDatabaseStatement(Query query) {
        super(query);
    }

    @Override
    public String getStatement() {
        query.parameterIndex = 1;
        builder.append("select count(1) from ( select " + query.distinct + " ");
        //如果有指定列,则添加指定列
        if (query.column.length() > 0) {
            builder.append(query.column);
        } else {
            builder.append(columns(query.entity, query.tableAliasName));
        }
        builder.append(" from");
        if (quickDAOConfig.databaseOption.virtualTableNameList.contains(query.entity.tableName)) {
            //虚拟表
            builder.append(" " + query.entity.tableName);
        } else if (query.entity.tableName.startsWith("(")) {
            //子查询
            builder.append(" " + query.entity.tableName + " " + query.tableAliasName);
        } else {
            //普通表查询
            builder.append(" " + query.quickDAOConfig.databaseProvider.escape(query.entity.tableName) + " " + query.tableAliasName);
        }
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
        return "获取行数(内部)";
    }
}
