package cn.schoolwow.quickdao.statement.dml.instance;

import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.internal.ManipulationOption;

/**批量插入记录*/
public class InsertInstanceBatchDatabaseStatement extends InsertInstanceDatabaseStatement {
    public InsertInstanceBatchDatabaseStatement(Object[] instances, ManipulationOption option, QuickDAOConfig quickDAOConfig) {
        super(instances, option, quickDAOConfig);
    }

    @Override
    public int executeUpdate(){
        return executeBatch(instances);
    }

    @Override
    public String name() {
        return "批量插入记录";
    }
}
