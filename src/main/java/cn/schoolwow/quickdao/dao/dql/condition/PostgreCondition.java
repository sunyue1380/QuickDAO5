package cn.schoolwow.quickdao.dao.dql.condition;

import cn.schoolwow.quickdao.domain.external.PageVo;
import cn.schoolwow.quickdao.domain.internal.Query;

public class PostgreCondition<T> extends AbstractCondition<T> {

    public PostgreCondition(Query query) {
        super(query);
    }

    @Override
    public Condition<T> limit(long offset, long limit) {
        query.limit = "limit " + limit + " offset " + offset;
        return this;
    }

    @Override
    public Condition<T> page(int pageNum, int pageSize) {
        query.limit = "limit " + pageSize + " offset " + (pageNum - 1) * pageSize;
        query.pageVo = new PageVo<>();
        query.pageVo.setPageSize(pageSize);
        query.pageVo.setCurrentPage(pageNum);
        return this;
    }
}
