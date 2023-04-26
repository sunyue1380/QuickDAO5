package cn.schoolwow.quickdao.statement.dql.response;

import cn.schoolwow.quickdao.dao.dql.condition.AbstractCondition;
import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.internal.Query;
import cn.schoolwow.quickdao.domain.internal.SubQuery;
import cn.schoolwow.quickdao.util.ResponseUtil;
import com.alibaba.fastjson.JSONArray;

import java.util.List;

/**获取返回记录列表*/
public class GetResponseArrayDatabaseStatement extends AbstractDQLResponseDatabaseStatement {

    public GetResponseArrayDatabaseStatement(Query query) {
        super(query);
    }

    public <E> List<E> getSingleColumnList(Class<E> clazz){
        int count = new ResponseCountInternalDatabaseStatement(query).getCount();
        JSONArray array = new JSONArray(count);
        connectionExecutor.name(name())
                .sql(getStatement())
                .parameters(getParameters())
                .executeQuery(resultSet -> {
                    while (resultSet.next()) {
                        array.add(resultSet.getObject(1));
                    }
                });
        return array.toJavaList(clazz);
    }

    @Override
    public JSONArray getArray(){
        int count = new ResponseCountInternalDatabaseStatement(query).getCount();
        JSONArray array = new JSONArray(count);
        connectionExecutor.name(name())
                .sql(getStatement())
                .parameters(getParameters())
                .executeQuery(resultSet -> {
                    ResponseUtil.getResponseArray(resultSet, query, array);
                });
        return array;
    }

    @Override
    public String getStatement() {
        if (query.unionList.isEmpty()) {
            getUnionArraySQL(query);
            builder.append(" " + query.orderBy + " " + query.limit);
        } else {
            getUnionArraySQL(query);
            for (AbstractCondition abstractCondition : query.unionList) {
                switch (abstractCondition.query.unionType) {
                    case Union: {
                        builder.append(" union ");
                    }
                    break;
                    case UnionAll: {
                        builder.append(" union all ");
                    }
                    break;
                    default: {
                        throw new IllegalArgumentException("不支持的Union类型!当前类型:" + abstractCondition.query.unionType);
                    }
                }
                getUnionArraySQL(abstractCondition.query);
            }
            builder.append(" " + query.orderBy + " " + query.limit);
        }
        String sql = builder.toString();
        return sql;
    }

    @Override
    public List getParameters() {
        addArraySQLParameters(query, query);
        //添加union语句
        for (AbstractCondition abstractCondition : query.unionList) {
            Query unionQuery = abstractCondition.query;
            for (SubQuery subQuery : unionQuery.subQueryList) {
                if (null != subQuery.subQuery) {
                    addMainTableParameters(subQuery.subQuery);
                }
            }
            addMainTableParameters(unionQuery);
            for (Object parameter : unionQuery.havingParameterList) {
                parameters.add(parameter);
            }
        }
        return parameters;
    }

    @Override
    public String name() {
        return "执行复杂查询";
    }

    /**
     * 获取Union查询时的SQL语句
     */
    private void getUnionArraySQL(Query query) {
        builder.append("select " + query.distinct + " ");
        //如果有指定列,则添加指定列
        if (query.column.length() > 0) {
            builder.append(query.column);
        } else if (query.excludeColumnList.isEmpty()) {
            builder.append(columns(query.entity, query.tableAliasName));
        } else {
            builder.append(columnsExclude(query.entity, query.tableAliasName, query.excludeColumnList));
        }
        if (query.compositField) {
            for (SubQuery subQuery : query.subQueryList) {
                builder.append("," + columns(subQuery.entity, subQuery.tableAliasName));
            }
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
        builder.append(" " + query.where + " " + query.groupBy + " " + query.having);
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
