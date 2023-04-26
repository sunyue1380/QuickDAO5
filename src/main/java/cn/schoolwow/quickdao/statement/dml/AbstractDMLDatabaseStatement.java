package cn.schoolwow.quickdao.statement.dml;

import cn.schoolwow.quickdao.dao.ConnectionExecutor;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.internal.ManipulationOption;
import cn.schoolwow.quickdao.statement.AbstractDatabaseStatement;

/**DML语句抽象类*/
public abstract class AbstractDMLDatabaseStatement extends AbstractDatabaseStatement implements DMLDatabaseStatement{
    /**执行选项*/
    protected ManipulationOption option;

    /**SQL执行对象*/
    protected ConnectionExecutor connectionExecutor;

    /**数组下标*/
    protected int index;

    public AbstractDMLDatabaseStatement(ManipulationOption option, QuickDAOConfig quickDAOConfig) {
        super(quickDAOConfig);
        this.option = option;
        this.connectionExecutor = option.connectionExecutor;
    }

    @Override
    public int executeUpdate(){
        int effect = connectionExecutor.returnGeneratedKeys(option.returnGeneratedKeys)
                .name(name())
                .sql(getStatement())
                .parameters(getParameters())
                .executeUpdate();
        return effect;
    }
}
