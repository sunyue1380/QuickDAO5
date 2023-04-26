package cn.schoolwow.quickdao.dao.dql.response;

import cn.schoolwow.quickdao.dao.sql.AbstractDatabaseDAO;
import cn.schoolwow.quickdao.domain.external.PageVo;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.internal.Query;
import cn.schoolwow.quickdao.statement.dql.response.GetResponseArrayDatabaseStatement;
import cn.schoolwow.quickdao.statement.dql.response.ResponseCountDatabaseStatement;
import cn.schoolwow.quickdao.statement.dql.response.ResponseDeleteDatabaseStatement;
import cn.schoolwow.quickdao.statement.dql.response.ResponseUpdateDatabaseStatement;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class AbstractResponse<T> extends AbstractDatabaseDAO implements Response<T> {
    private Logger logger = LoggerFactory.getLogger(AbstractResponse.class);
    /**
     * 查询对象参数
     */
    public Query query;

    public AbstractResponse(QuickDAOConfig quickDAOConfig) {
        super(quickDAOConfig);
    }

    @Override
    public long count() {
        int count = new ResponseCountDatabaseStatement(query).getCount();
        return count;
    }

    @Override
    public int update() {
        int effect = new ResponseUpdateDatabaseStatement(query).executeUpdate();
        return effect;
    }

    @Override
    public int delete() {
        int effect = new ResponseDeleteDatabaseStatement(query).executeUpdate();
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
        List<E> list = new GetResponseArrayDatabaseStatement(query).getSingleColumnList(clazz);
        return list;
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
        query.pageVo.setTotalSize(count());
        query.pageVo.setTotalPage((int) (query.pageVo.getTotalSize() / query.pageVo.getPageSize() + (query.pageVo.getTotalSize() % query.pageVo.getPageSize() > 0 ? 1 : 0)));
        query.pageVo.setHasMore(query.pageVo.getCurrentPage() < query.pageVo.getTotalPage());
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
        JSONArray array = new GetResponseArrayDatabaseStatement(query).getArray();
        return array;
    }

    @Override
    public String toString() {
        return query.toString();
    }

}
