package cn.schoolwow.quickdao.statement.dql.instance;

import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.statement.dql.AbstractDQLDatabaseStatement;

/**查询实例数组是否在数据库中存在所有记录*/
public class ExistAllDatabaseStatement extends AbstractDQLDatabaseStatement {
    /**实体类信息*/
    private Entity entity;

    /**实例*/
    private Object[] instances;

    public ExistAllDatabaseStatement(Object[] instances, QuickDAOConfig quickDAOConfig) {
        super(quickDAOConfig);
        this.entity = quickDAOConfig.getEntityByClassName(instances[0].getClass().getName());
        this.instances = instances;
    }

    @Override
    public int getCount(){
        int count = 0;
        if(!entity.uniqueProperties.isEmpty()){
            if(entity.uniqueProperties.size()==1){
                Property property = entity.uniqueProperties.get(0);
                count = new SelectCountBySingleFieldDatabaseStatement(instances, property, quickDAOConfig).getCount();
            }else{
                for(Object instance:instances){
                    count = new SelectCountByUniqueKeyDatabaseStatement(instance, quickDAOConfig).getCount();
                    if(count==0){
                        //只要有一个不存在即可跳出循环
                        break;
                    }
                }
            }
        }else if(null!=entity.id){
            count = new SelectCountBySingleFieldDatabaseStatement(instances, entity.id, quickDAOConfig).getCount();
        }else{
            throw new IllegalArgumentException("该实例无唯一性约束又无id值,无法判断!类名:" + instances[0].getClass().getName());
        }
        return count;
    }

}
