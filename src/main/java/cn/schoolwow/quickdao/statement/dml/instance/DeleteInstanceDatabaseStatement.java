package cn.schoolwow.quickdao.statement.dml.instance;

import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.internal.ManipulationOption;

/**删除记录*/
public class DeleteInstanceDatabaseStatement extends AbstractDMLInstanceDatabaseStatement {
    /**实例列表*/
    private Object[] instances;

    public DeleteInstanceDatabaseStatement(Object[] instances, ManipulationOption option, QuickDAOConfig quickDAOConfig) {
        super(option, quickDAOConfig);
        this.instances = instances;
    }

    @Override
    public int executeUpdate(){
        Entity entity = quickDAOConfig.getEntityByClassName(instances[0].getClass().getName());
        int effect = 0;
        if (!entity.uniqueProperties.isEmpty()) {
            if(entity.uniqueProperties.size()==1){
                //根据单个唯一性约束字段删除
                effect = new DeleteInstanceBySingleFieldDatabaseStatement(instances, entity.uniqueProperties.get(0), option, quickDAOConfig).executeUpdate();
            }else{
                //根据实体类唯一性约束删除
                effect = new DeleteInstanceByUniqueKeyDatabaseStatement(instances, option, quickDAOConfig).executeUpdate();
            }
        } else if (null != entity.id) {
            //根据id删除
            effect = new DeleteInstanceByPropertyDatabaseStatement(instances, entity.id, option, quickDAOConfig).executeUpdate();
        } else {
            logger.warn("指定实体类无唯一性约束又无id,忽略删除操作!实体类:"+entity.clazz.getName());
        }
        return effect;
    }

}
