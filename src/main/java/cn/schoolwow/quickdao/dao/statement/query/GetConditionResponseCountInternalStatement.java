package cn.schoolwow.quickdao.dao.statement.query;

import cn.schoolwow.quickdao.dao.statement.DatabaseStatementOption;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.internal.Query;

import java.util.List;

/**
 * 获取返回结果行数
 */
public class GetConditionResponseCountInternalStatement extends AbstractResponseDatabaseStatement {
    private Query query;

    public GetConditionResponseCountInternalStatement(DatabaseStatementOption databaseStatementOption, QuickDAOConfig quickDAOConfig) {
        super(databaseStatementOption, quickDAOConfig);
        query = databaseStatementOption.query;
    }

    @Override
    public String getStatement() {
        query.parameterIndex = 1;
        sqlBuilder.append("select count(1) from ( select " + query.distinct + " ");
        //如果有指定列,则添加指定列
        if (query.column.length() > 0) {
            sqlBuilder.append(query.column);
        } else {
            sqlBuilder.append(columns(query.entity, query.tableAliasName));
        }
        sqlBuilder.append(" from");
        if (quickDAOConfig.databaseOption.virtualTableNameList.contains(query.entity.tableName)) {
            //虚拟表
            sqlBuilder.append(" " + query.entity.tableName);
        } else if (query.entity.tableName.startsWith("(")) {
            //子查询
            sqlBuilder.append(" " + query.entity.tableName + " " + query.tableAliasName);
        } else {
            //普通表查询
            sqlBuilder.append(" " + query.quickDAOConfig.databaseProvider.escape(query.entity.tableName) + " " + query.tableAliasName);
        }
        addJoinTableStatement();
        sqlBuilder.append(" " + query.where + " " + query.groupBy + " " + query.having + " ) foo");
        String sql = sqlBuilder.toString();
        return sql;
    }

    @Override
    public List getParameters(Object instance) {
        addArraySQLParameters(query, query);
        return parameters;
    }

    @Override
    public String name() {
        return "获取行数(内部)";
    }

}
