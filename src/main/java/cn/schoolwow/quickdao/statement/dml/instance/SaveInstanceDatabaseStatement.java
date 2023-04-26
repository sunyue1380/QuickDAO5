package cn.schoolwow.quickdao.statement.dml.instance;

import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.internal.ManipulationOption;

import java.util.ArrayList;
import java.util.List;

/**保存实例列表*/
public class SaveInstanceDatabaseStatement extends AbstractDMLInstanceDatabaseStatement {
    /**实例列表*/
    private Object[] instances;

    public SaveInstanceDatabaseStatement(Object[] instances, ManipulationOption option, QuickDAOConfig quickDAOConfig) {
        super(option, quickDAOConfig);
        this.instances = instances;
    }

    @Override
    public int executeUpdate(){
        List insertInstances = new ArrayList();
        List updateInstances = new ArrayList();
        Entity entity = quickDAOConfig.getEntityByClassName(instances[0].getClass().getName());
        if(entity.uniqueProperties.size()==1){
            Property uniqueProperty = entity.uniqueProperties.get(0);
            distinguishInstancesBySingleField(instances, entity.tableName, uniqueProperty, insertInstances, updateInstances);
        }else if(entity.uniqueProperties.size()>1){
            distinguishInstancesByMultipleField(instances, insertInstances, updateInstances);
        }else if(null!=entity.id){
            distinguishInstancesBySingleField(instances, entity.tableName, entity.id, insertInstances, updateInstances);
        }
        int effect = 0;
        if(!insertInstances.isEmpty()){
            effect += new InsertInstanceBatchDatabaseStatement(insertInstances.toArray(new Object[0]), option, quickDAOConfig).executeUpdate();
        }
        if(!updateInstances.isEmpty()){
            effect += new UpdateInstanceDatabaseStatement(updateInstances.toArray(new Object[0]), option, quickDAOConfig).executeUpdate();
        }
        return effect;
    }

}
