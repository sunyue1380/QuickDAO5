package cn.schoolwow.quickdao.dao.dql.response;

import cn.schoolwow.quickdao.dao.sql.AbstractDatabaseDAO;
import cn.schoolwow.quickdao.dao.statement.DatabaseStatement;
import cn.schoolwow.quickdao.dao.statement.DatabaseStatementOption;
import cn.schoolwow.quickdao.dao.statement.query.*;
import cn.schoolwow.quickdao.domain.external.PageVo;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.internal.Query;
import cn.schoolwow.quickdao.domain.internal.SubQuery;
import cn.schoolwow.quickdao.util.ResponseUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class AbstractResponse<T> extends AbstractDatabaseDAO implements Response<T> {
    /**
     * 查询对象参数
     */
    public Query query;

    public AbstractResponse(QuickDAOConfig quickDAOConfig) {
        super(quickDAOConfig);
    }

    @Override
    public long count() {
        long[] count = new long[1];
        DatabaseStatementOption databaseStatementOption = new DatabaseStatementOption();
        databaseStatementOption.query = query;
        DatabaseStatement getResponseCountStatement = new GetConditionResponseCountStatement(databaseStatementOption, query.quickDAOConfig);
        connectionExecutor.name(getResponseCountStatement.name())
                .sql(getResponseCountStatement.getStatement())
                .parameters(getResponseCountStatement.getParameters(null))
                .executeQuery((resultSet) -> {
                    if (resultSet.next()) {
                        count[0] = resultSet.getLong(1);
                    }
                });
        return count[0];
    }

    @Override
    public int update() {
        DatabaseStatementOption databaseStatementOption = new DatabaseStatementOption();
        databaseStatementOption.query = query;
        DatabaseStatement getConditionUpdateStatement = new GetConditionUpdateStatement(databaseStatementOption, query.quickDAOConfig);
        int effect = connectionExecutor.name(getConditionUpdateStatement.name())
                .sql(getConditionUpdateStatement.getStatement())
                .parameters(getConditionUpdateStatement.getParameters(null))
                .executeUpdate();
        return effect;
    }

    @Override
    public int delete() {
        DatabaseStatementOption databaseStatementOption = new DatabaseStatementOption();
        databaseStatementOption.query = query;
        DatabaseStatement getConditionDeleteStatement = new GetConditionDeleteStatement(databaseStatementOption, query.quickDAOConfig);
        int effect = connectionExecutor.name(getConditionDeleteStatement.name())
                .sql(getConditionDeleteStatement.getStatement())
                .parameters(getConditionDeleteStatement.getParameters(null))
                .executeUpdate();
        return effect;
    }

    @Override
    public T getOne() {
        List<T> list = getList();
        if (list == null || list.size() == 0) {
            return null;
        } else {
            return list.get(0);
        }
    }

    @Override
    public <E> E getOne(Class<E> clazz) {
        List<E> list = getList(clazz);
        if (list == null || list.size() == 0) {
            return null;
        } else {
            return list.get(0);
        }
    }

    @Override
    public <E> E getSingleColumn(Class<E> clazz) {
        List<E> list = getSingleColumnList(clazz);
        if (list == null || list.size() == 0) {
            return null;
        } else {
            return list.get(0);
        }
    }

    @Override
    public <E> List<E> getSingleColumnList(Class<E> clazz) {
        int count = getRowCount();
        JSONArray array = new JSONArray(count);
        DatabaseStatementOption databaseStatementOption = new DatabaseStatementOption();
        databaseStatementOption.query = query;
        DatabaseStatement getConditionResponseArrayStatement = new GetConditionResponseArrayStatement(databaseStatementOption, query.quickDAOConfig);
        connectionExecutor.name(getConditionResponseArrayStatement.name())
                .sql(getConditionResponseArrayStatement.getStatement())
                .parameters(getConditionResponseArrayStatement.getParameters(null))
                .executeQuery(resultSet -> {
                    while (resultSet.next()) {
                        array.add(resultSet.getObject(1));
                    }
                });
        return array.toJavaList(clazz);
    }

    @Override
    public List getList() {
        return getList(query.entity.clazz);
    }

    @Override
    public <E> List<E> getList(Class<E> clazz) {
        return getArray().toJavaList(clazz);
    }

    @Override
    public PageVo<T> getPagingList() {
        return getPagingList(query.entity.clazz);
    }

    @Override
    public <E> PageVo<E> getPagingList(Class<E> clazz) {
        if (null == clazz) {
            clazz = (Class<E>) JSONObject.class;
        }
        query.pageVo.setList(getArray().toJavaList(clazz));
        setPageVo();
        return query.pageVo;
    }

    @Override
    public JSONObject getObject() {
        JSONArray array = getArray();
        if (null == array || array.isEmpty()) {
            return null;
        }
        return array.getJSONObject(0);
    }

    @Override
    public JSONArray getArray() {
        int count = getRowCount();
        JSONArray array = new JSONArray(count);
        DatabaseStatementOption databaseStatementOption = new DatabaseStatementOption();
        databaseStatementOption.query = query;
        DatabaseStatement getConditionResponseArrayStatement = new GetConditionResponseArrayStatement(databaseStatementOption, query.quickDAOConfig);
        connectionExecutor.name(getConditionResponseArrayStatement.name())
                .sql(getConditionResponseArrayStatement.getStatement())
                .parameters(getConditionResponseArrayStatement.getParameters(null))
                .executeQuery(resultSet -> {
                    if (query.column.length() > 0) {
                        if (null == query.columnTypeMapping) {
                            query.columnTypeMapping = query.quickDAOConfig.queryColumnTypeMapping;
                        }

                        ResultSetMetaData metaData = resultSet.getMetaData();
                        Property[] properties = new Property[metaData.getColumnCount()];
                        for (int i = 1; i <= properties.length; i++) {
                            properties[i - 1] = new Property();
                            properties[i - 1].columnLabel = metaData.getColumnLabel(i);
                            properties[i - 1].column = metaData.getColumnName(i);
                            properties[i - 1].columnType = metaData.getColumnTypeName(i);
                            properties[i - 1].className = metaData.getColumnClassName(i);
                            if (null != query.columnTypeMapping) {
                                Class type = query.columnTypeMapping.apply(properties[i - 1]);
                                if (null != type) {
                                    properties[i - 1].clazz = type;
                                }
                            }
                        }

                        while (resultSet.next()) {
                            JSONObject o = new JSONObject(true);
                            for (int i = 1; i <= properties.length; i++) {
                                if (null == properties[i - 1].clazz) {
                                    o.put(properties[i - 1].columnLabel, resultSet.getObject(i));
                                } else {
                                    o.put(properties[i - 1].columnLabel, resultSet.getObject(i, properties[i - 1].clazz));
                                }
                            }
                            array.add(o);
                        }
                    } else {
                        while (resultSet.next()) {
                            JSONObject o = ResponseUtil.getObject(query.entity, query.excludeColumnList, query.tableAliasName, resultSet, query.quickDAOConfig.databaseProvider);
                            if (query.compositField) {
                                getCompositObject(resultSet, o);
                            }
                            array.add(o);
                        }
                    }
                });
        return array;
    }

    @Override
    public String toString() {
        return query.toString();
    }

    /**
     * 获取复杂对象
     *
     * @param resultSet 结果集
     * @param o         复杂对象
     */
    private void getCompositObject(ResultSet resultSet, JSONObject o) throws SQLException {
        for (SubQuery subQuery : query.subQueryList) {
            if (null == subQuery.compositField || subQuery.compositField.isEmpty()) {
                continue;
            }
            JSONObject subObject = ResponseUtil.getObject(subQuery.entity, subQuery.excludeColumnList, subQuery.tableAliasName, resultSet, query.quickDAOConfig.databaseProvider);
            SubQuery parentSubQuery = subQuery.parentSubQuery;
            if (parentSubQuery == null) {
                o.put(subQuery.compositField, subObject);
            } else {
                List<String> fieldNames = new ArrayList<>();
                while (parentSubQuery != null) {
                    fieldNames.add(parentSubQuery.compositField);
                    parentSubQuery = parentSubQuery.parentSubQuery;
                }
                JSONObject oo = o;
                for (int i = fieldNames.size() - 1; i >= 0; i--) {
                    oo = oo.getJSONObject(fieldNames.get(i));
                }
                oo.put(subQuery.compositField, subObject);
            }
        }
    }

    /**
     * 设置分页对象
     */
    private void setPageVo() {
        if (query.pageVo == null) {
            throw new IllegalArgumentException("请先调用page()函数!");
        }
        query.pageVo.setTotalSize(count());
        query.pageVo.setTotalPage((int) (query.pageVo.getTotalSize() / query.pageVo.getPageSize() + (query.pageVo.getTotalSize() % query.pageVo.getPageSize() > 0 ? 1 : 0)));
        query.pageVo.setHasMore(query.pageVo.getCurrentPage() < query.pageVo.getTotalPage());
    }

    /**
     * 获取记录个数
     */
    protected int getRowCount() {
        int count[] = new int[1];
        DatabaseStatementOption databaseStatementOption = new DatabaseStatementOption();
        databaseStatementOption.query = query;
        DatabaseStatement getResponseCountStatement = new GetConditionResponseCountInternalStatement(databaseStatementOption, query.quickDAOConfig);
        connectionExecutor.name(getResponseCountStatement.name())
                .sql(getResponseCountStatement.getStatement())
                .parameters(getResponseCountStatement.getParameters(null))
                .executeQuery(resultSet -> {
                    if (resultSet.next()) {
                        count[0] = resultSet.getInt(1);
                    }
                });
        return count[0];
    }
}
