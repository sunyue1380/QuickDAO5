package cn.schoolwow.quickdao.statement;

import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class AbstractDatabaseStatement implements DatabaseStatement{
    protected Logger logger = LoggerFactory.getLogger(AbstractDatabaseStatement.class);

    /**配置类*/
    protected QuickDAOConfig quickDAOConfig;

    public AbstractDatabaseStatement(QuickDAOConfig quickDAOConfig) {
        this.quickDAOConfig = quickDAOConfig;
    }

    /**
     * 获取sql语句
     * */
    protected String getStatement(){
        throw new UnsupportedOperationException("当前不支持获取执行语句!");
    }

    /**
     * 获取参数列表
     * */
    protected List getParameters(){
        throw new UnsupportedOperationException("当前不支持获取语句参数!");
    }

    /**
     * 获取语句名称
     * */
    protected String name(){
        throw new UnsupportedOperationException("当前不支持获取语句名称!");
    }
}
