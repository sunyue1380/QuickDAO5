package cn.schoolwow.quickdao.dao.statement.query;

import cn.schoolwow.quickdao.dao.statement.DatabaseStatementOption;
import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.internal.Query;
import cn.schoolwow.quickdao.domain.internal.SubQuery;

import java.util.List;

/**
 * 获取返回结果列表
 */
public class GetConditionResponseArraySQLStatement extends AbstractResponseDatabaseStatement {
    private Query query;

    public GetConditionResponseArraySQLStatement(DatabaseStatementOption databaseStatementOption, QuickDAOConfig quickDAOConfig) {
        super(databaseStatementOption, quickDAOConfig);
        query = databaseStatementOption.query;
    }

    @Override
    public String getStatement() {
        sqlBuilder.append("select " + query.distinct + " ");
        //如果有指定列,则添加指定列
        if (query.column.length() > 0) {
            sqlBuilder.append(query.column);
        } else if (query.excludeColumnList.isEmpty()) {
            sqlBuilder.append(columns(query.entity, query.tableAliasName));
        } else {
            sqlBuilder.append(columnsExclude(query.entity, query.tableAliasName, query.excludeColumnList));
        }
        if (query.compositField) {
            for (SubQuery subQuery : query.subQueryList) {
                sqlBuilder.append("," + columns(subQuery.entity, subQuery.tableAliasName));
            }
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
        sqlBuilder.append(" " + query.where + " " + query.groupBy + " " + query.having);
        String sql = sqlBuilder.toString();
        return sql;
    }

    @Override
    public List getParameters(Object instance) {
        return parameters;
    }

    @Override
    public String name() {
        return "执行复杂查询";
    }

    /**
     * 返回排除指定列名后的SQL语句
     */
    private String columnsExclude(Entity entity, String tableAlias, List<String> excludeColumnList) {
        StringBuilder builder = new StringBuilder();
        for (Property property : entity.properties) {
            if (excludeColumnList.contains(property.name) || excludeColumnList.contains(property.column)) {
                continue;
            }
            builder.append(tableAlias + "." + quickDAOConfig.databaseProvider.escape(property.column) + " as " + tableAlias + "_" + property.column + ",");
        }
        builder.deleteCharAt(builder.length() - 1);
        return builder.toString();
    }
}
