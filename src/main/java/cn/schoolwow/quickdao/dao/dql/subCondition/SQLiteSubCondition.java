package cn.schoolwow.quickdao.dao.dql.subCondition;

import cn.schoolwow.quickdao.domain.internal.SubQuery;

public class SQLiteSubCondition extends AbstractSubCondition {
    public SQLiteSubCondition(SubQuery subQuery) {
        super(subQuery);
    }

    @Override
    public SubCondition rightJoin() {
        throw new UnsupportedOperationException("SQLite目前不支持右外连接和全外连接!");
    }
}
