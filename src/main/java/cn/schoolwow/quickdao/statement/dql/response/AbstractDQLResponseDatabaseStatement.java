package cn.schoolwow.quickdao.statement.dql.response;

import cn.schoolwow.quickdao.domain.internal.Query;
import cn.schoolwow.quickdao.domain.internal.SubQuery;
import cn.schoolwow.quickdao.statement.dql.AbstractDQLDatabaseStatement;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AbstractDQLResponseDatabaseStatement extends AbstractDQLDatabaseStatement {
    /**Condition参数*/
    protected Query query;
    
    /**SQL语句拼接*/
    protected StringBuilder builder = new StringBuilder();

    /**SQL参数*/
    protected List parameters = new ArrayList<>();
    
    public AbstractDQLResponseDatabaseStatement(Query query) {
        super(query.quickDAOConfig);
        this.query = query;
    }

    /**
     * 添加外键关联查询条件
     */
    protected void addJoinTableStatement() {
        for (SubQuery subQuery : query.subQueryList) {
            builder.append(" " + subQuery.join + " ");
            if (null == subQuery.subQuerySQLBuilder) {
                builder.append(query.quickDAOConfig.databaseProvider.escape(subQuery.entity.tableName));
            } else {
                builder.append("(" + subQuery.subQuerySQLBuilder.toString() + ")");
            }
            builder.append(" " + subQuery.tableAliasName);
            if (null != subQuery.primaryField && null != subQuery.joinTableField) {
                builder.append(" on ");
                if (subQuery.parentSubQuery == null) {
                    builder.append(query.tableAliasName + "." + query.quickDAOConfig.databaseProvider.escape(subQuery.primaryField) + " = " + subQuery.tableAliasName + "." + query.quickDAOConfig.databaseProvider.escape(subQuery.joinTableField) + " ");
                } else {
                    builder.append(subQuery.tableAliasName + "." + query.quickDAOConfig.databaseProvider.escape(subQuery.joinTableField) + " = " + subQuery.parentSubQuery.tableAliasName + "." + query.quickDAOConfig.databaseProvider.escape(subQuery.primaryField) + " ");
                }
                if (!subQuery.onConditionMap.isEmpty()) {
                    Set<Map.Entry<String, String>> entrySet = subQuery.onConditionMap.entrySet();
                    for (Map.Entry<String, String> entry : entrySet) {
                        builder.append(" and ");
                        if (subQuery.parentSubQuery == null) {
                            builder.append(query.tableAliasName + "." + query.quickDAOConfig.databaseProvider.escape(entry.getKey()) + " = " + subQuery.tableAliasName + "." + query.quickDAOConfig.databaseProvider.escape(entry.getValue()) + " ");
                        } else {
                            builder.append(subQuery.tableAliasName + "." + query.quickDAOConfig.databaseProvider.escape(entry.getValue()) + " = " + subQuery.parentSubQuery.tableAliasName + "." + query.quickDAOConfig.databaseProvider.escape(entry.getKey()) + " ");
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
