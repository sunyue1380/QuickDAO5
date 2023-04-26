package cn.schoolwow.quickdao.dao.dql.response;

import cn.schoolwow.quickdao.domain.external.PageVo;
import cn.schoolwow.quickdao.domain.internal.Query;
import cn.schoolwow.quickdao.statement.dql.response.GetResponseArrayDatabaseStatement;
import com.alibaba.fastjson.JSONArray;

import java.util.List;

public class OracleResponse<T> extends AbstractResponse<T> {

    public OracleResponse(Query query) {
        super(query.quickDAOConfig);
    }

    @Override
    public List getList() {
        return getList(getOracleQuery().entity.clazz);
    }

    @Override
    public PageVo<T> getPagingList() {
        return getPagingList(getOracleQuery().entity.clazz);
    }

    @Override
    public JSONArray getArray() {
        JSONArray array = new GetResponseArrayDatabaseStatement(getOracleQuery()).getArray();
        return array;
    }

    /**
     * 获取返回结果Query
     */
    private Query getOracleQuery() {
        //oracle分页操作
        if ("where rn >= ?".equals(query.where)) {
            return query.fromQuery.fromQuery;
        }
        return this.query;
    }
}
