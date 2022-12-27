package cn.schoolwow.quickdao.provider;

import cn.schoolwow.quickdao.dao.dml.AbstractDatabaseManipulation;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;

public abstract class AbstractDatabaseProvider implements DatabaseProvider {
    /**
     * 获取数据库定义语言实例
     */
    @Override
    public AbstractDatabaseManipulation getDatabaseManipulationInstance(QuickDAOConfig quickDAOConfig) {
        return new AbstractDatabaseManipulation(quickDAOConfig);
    }

    @Override
    public boolean returnGeneratedKeys() {
        return true;
    }

}
