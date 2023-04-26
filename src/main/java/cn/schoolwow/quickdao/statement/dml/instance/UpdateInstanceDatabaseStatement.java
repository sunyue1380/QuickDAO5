package cn.schoolwow.quickdao.statement.dml.instance;

import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.internal.ManipulationOption;

/**更新记录*/
public class UpdateInstanceDatabaseStatement extends AbstractDMLInstanceDatabaseStatement {
    /**实例列表*/
    private Object[] instances;

    public UpdateInstanceDatabaseStatement(Object[] instances, ManipulationOption option, QuickDAOConfig quickDAOConfig) {
        super(option, quickDAOConfig);
        this.instances = instances;
    }

    @Override
    public int executeUpdate(){
        option.returnGeneratedKeys = false;
        Entity entity = quickDAOConfig.getEntityByClassName(instances[0].getClass().getName());
        int effect = 0;
        if (!entity.uniqueProperties.isEmpty()) {
            //根据唯一性约束更新
            effect = new UpdateInstanceByUniqueKeyDatabaseStatement(instances, option, quickDAOConfig).executeUpdate();
        } else if (null != entity.id) {
            //根据id更新
            effect = new UpdateInstanceByIdDatabaseStatement(instances, option, quickDAOConfig).executeUpdate();
        } else {
            logger.warn("指定实体类无唯一性约束又无id,忽略更新操作!实体类:"+entity.clazz.getName());
        }
        return effect;
    }

}
