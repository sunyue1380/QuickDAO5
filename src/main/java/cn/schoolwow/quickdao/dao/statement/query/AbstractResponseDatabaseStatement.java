package cn.schoolwow.quickdao.dao.statement.query;

import cn.schoolwow.quickdao.dao.statement.AbstractDatabaseStatement;
import cn.schoolwow.quickdao.dao.statement.DatabaseStatementOption;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.internal.Query;
import cn.schoolwow.quickdao.domain.internal.SubQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 复杂查询语句抽象类
 */
public abstract class AbstractResponseDatabaseStatement extends AbstractDatabaseStatement {
    /**
     * 查询对象
     */
    protected Query query;

    /**
     * SQL语句
     */
    protected StringBuilder sqlBuilder = new StringBuilder();

    /**
     * 参数列表
     */
    protected List parameters = new ArrayList();

    public AbstractResponseDatabaseStatement(DatabaseStatementOption databaseStatementOption, QuickDAOConfig quickDAOConfig) {
        super(databaseStatementOption, quickDAOConfig);
        query = databaseStatementOption.query;
    }

    /**
     * 添加外键关联查询条件
     */
    protected void addJoinTableStatement() {
        for (SubQuery subQuery : query.subQueryList) {
            sqlBuilder.append(" " + subQuery.join + " ");
            if (null == subQuery.subQuerySQLBuilder) {
                sqlBuilder.append(query.quickDAOConfig.databaseProvider.escape(subQuery.entity.tableName));
            } else {
                sqlBuilder.append("(" + subQuery.subQuerySQLBuilder.toString() + ")");
            }
            sqlBuilder.append(" " + subQuery.tableAliasName);
            if (null != subQuery.primaryField && null != subQuery.joinTableField) {
                sqlBuilder.append(" on ");
                if (subQuery.parentSubQuery == null) {
                    sqlBuilder.append(query.tableAliasName + "." + query.quickDAOConfig.databaseProvider.escape(subQuery.primaryField) + " = " + subQuery.tableAliasName + "." + query.quickDAOConfig.databaseProvider.escape(subQuery.joinTableField) + " ");
                } else {
                    sqlBuilder.append(subQuery.tableAliasName + "." + query.quickDAOConfig.databaseProvider.escape(subQuery.joinTableField) + " = " + subQuery.parentSubQuery.tableAliasName + "." + query.quickDAOConfig.databaseProvider.escape(subQuery.primaryField) + " ");
                }
                if (!subQuery.onConditionMap.isEmpty()) {
                    Set<Map.Entry<String, String>> entrySet = subQuery.onConditionMap.entrySet();
                    for (Map.Entry<String, String> entry : entrySet) {
                        sqlBuilder.append(" and ");
                        if (subQuery.parentSubQuery == null) {
                            sqlBuilder.append(query.tableAliasName + "." + query.quickDAOConfig.databaseProvider.escape(entry.getKey()) + " = " + subQuery.tableAliasName + "." + query.quickDAOConfig.databaseProvider.escape(entry.getValue()) + " ");
                        } else {
                            sqlBuilder.append(subQuery.tableAliasName + "." + query.quickDAOConfig.databaseProvider.escape(entry.getValue()) + " = " + subQuery.parentSubQuery.tableAliasName + "." + query.quickDAOConfig.databaseProvider.escape(entry.getKey()) + " ");
                        }
                    }
                }
            }
        }
    }

    /**
     * 添加Array语句参数
     *
     * @param query     当前query对象
     * @param mainQuery 主query
     */
    protected void addArraySQLParameters(Query query, Query mainQuery) {
        for (Query selectQuery : query.selectQueryList) {
            addArraySQLParameters(selectQuery, mainQuery);
        }
        //from子查询
        if (null != query.fromQuery) {
            addArraySQLParameters(query.fromQuery, mainQuery);
        }
        //关联子查询
        for (SubQuery subQuery : query.subQueryList) {
            if (null != subQuery.subQuery) {
                addArraySQLParameters(subQuery.subQuery, mainQuery);
            }
        }
        addMainTableParameters(query);
    }

    /**
     * 添加主表参数
     */
    protected void addMainTableParameters(Query query) {
        for (Object parameter : query.parameterList) {
            parameters.add(parameter);
        }
        for (Object parameter : query.havingParameterList) {
            parameters.add(parameter);
        }
    }
}
