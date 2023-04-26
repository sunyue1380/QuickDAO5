package cn.schoolwow.quickdao.statement.dql.instance;

import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.statement.dql.AbstractDQLDatabaseStatement;

/**查询实例是否存在*/
public class ExistDatabaseStatement extends AbstractDQLDatabaseStatement {
    /**实例*/
    private Object instance;

    /**实体类信息*/
    private Entity entity;

    public ExistDatabaseStatement(Object instance, QuickDAOConfig quickDAOConfig) {
        super(quickDAOConfig);
        this.entity = quickDAOConfig.getEntityByClassName(instance.getClass().getName());
        this.instance = instance;
    }

    @Override
    public int getCount(){
        int count = 0;
        if(!entity.uniqueProperties.isEmpty()){
            count = new SelectCountByUniqueKeyDatabaseStatement(instance, quickDAOConfig).getCount();
        }else if(null!=entity.id){
            count = new SelectCountBySingleFieldDatabaseStatement(new Object[]{instance}, entity.id, quickDAOConfig).getCount();
        }else{
            throw new IllegalArgumentException("该实例无唯一性约束又无id值,无法判断!类名:" + instance.getClass().getName());
        }
        return count;
    }

}
