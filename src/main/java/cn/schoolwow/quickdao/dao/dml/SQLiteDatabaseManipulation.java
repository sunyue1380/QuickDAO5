package cn.schoolwow.quickdao.dao.dml;

import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SQLiteDatabaseManipulation extends AbstractDatabaseManipulation {
    private Logger logger = LoggerFactory.getLogger(SQLiteDatabaseManipulation.class);

    public SQLiteDatabaseManipulation(QuickDAOConfig quickDAOConfig) {
        super(quickDAOConfig);
    }

    @Override
    public int truncate(String tableName) {
        throw new UnsupportedOperationException("SQLite数据库不支持truncate操作!");
    }
}
