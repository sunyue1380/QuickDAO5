package cn.schoolwow.quickdao.statement.dml.instance;

import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.internal.ManipulationOption;
import cn.schoolwow.quickdao.statement.dml.AbstractDMLDatabaseStatement;

import java.util.ArrayList;
import java.util.List;

/**忽略插入实例列表*/
public class InsertIgnoreInstanceDatabaseStatement extends AbstractDMLInstanceDatabaseStatement {
    /**实例列表*/
    private Object[] instances;

    public InsertIgnoreInstanceDatabaseStatement(Object[] instances, ManipulationOption option, QuickDAOConfig quickDAOConfig) {
        super(option, quickDAOConfig);
        this.instances = instances;
    }

    @Override
    public int executeUpdate(){
        Entity entity = quickDAOConfig.getEntityByClassName(instances[0].getClass().getName());
        List insertInstances = new ArrayList();
        if(entity.uniqueProperties.size()==1){
            Property uniqueProperty = entity.uniqueProperties.get(0);
            distinguishInstancesBySingleField(instances, entity.tableName, uniqueProperty, insertInstances, null);
        }else if(entity.uniqueProperties.size()>1){
            distinguishInstancesByMultipleField(instances, insertInstances, null);
        }else if(null!=entity.id){
            distinguishInstancesBySingleField(instances, entity.tableName, entity.id, insertInstances, null);
        }
        if(insertInstances.isEmpty()){
            return 0;
        }
        AbstractDMLDatabaseStatement insertInstanceBatchDatabaseStatement = new InsertInstanceBatchDatabaseStatement(insertInstances.toArray(new Object[0]),option,quickDAOConfig);
        int effect = insertInstanceBatchDatabaseStatement.executeUpdate();
        return effect;
    }

    @Override
    public String name() {
        return "InsertIgnore方式插入记录";
    }

}
