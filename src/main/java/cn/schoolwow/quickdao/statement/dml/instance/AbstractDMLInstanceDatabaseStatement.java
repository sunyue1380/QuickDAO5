package cn.schoolwow.quickdao.statement.dml.instance;

import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.internal.ManipulationOption;
import cn.schoolwow.quickdao.statement.dml.AbstractDMLDatabaseStatement;
import cn.schoolwow.quickdao.statement.dql.DQLDatabaseStatement;
import cn.schoolwow.quickdao.statement.dql.instance.SelectCountByUniqueKeyDatabaseStatement;
import cn.schoolwow.quickdao.statement.dql.instance.SelectExistsValueBySingleFieldDatabaseStatement;
import cn.schoolwow.quickdao.util.ParametersUtil;

import java.util.ArrayList;
import java.util.List;

public class AbstractDMLInstanceDatabaseStatement extends AbstractDMLDatabaseStatement {

    public AbstractDMLInstanceDatabaseStatement(ManipulationOption option, QuickDAOConfig quickDAOConfig) {
        super(option, quickDAOConfig);
    }

    /**根据单个字段区分数据是否存在*/
    protected void distinguishInstancesBySingleField(Object[] instances, String tableName, Property property, List insertInstances, List updateInstances){
        List parameters = new ArrayList(instances.length);
        for(int i=0;i<instances.length;i++){
            Object value = ParametersUtil.getFieldValueFromInstance(instances[i], property.name);
            parameters.add(value);
        }
        DQLDatabaseStatement selectExistsValueBySingleFieldDatabaseStatement = new SelectExistsValueBySingleFieldDatabaseStatement(tableName, property.column, parameters, quickDAOConfig);
        List existValues = selectExistsValueBySingleFieldDatabaseStatement.getSingleColumnList();
        for(int i=0;i<instances.length;i++){
            Object value = ParametersUtil.getFieldValueFromInstance(instances[i], property.name);
            if(null!=insertInstances&&!existValues.contains(value)){
                insertInstances.add(instances[i]);
            }else if(null!=updateInstances){
                updateInstances.add(instances[i]);
            }
        }
    }

    /**根据多个字段区分数据是否存在*/
    protected void distinguishInstancesByMultipleField(Object[] instances, List insertInstances, List updateInstances){
        for(int i=0;i<instances.length;i++){
            Object instance = instances[i];
            DQLDatabaseStatement selectCountByUniqueKeyDatabaseStatement = new SelectCountByUniqueKeyDatabaseStatement(instance, quickDAOConfig);
            int count = selectCountByUniqueKeyDatabaseStatement.getCount();
            if(null!=insertInstances&&count<=0){
                insertInstances.add(instance);
            }else if(null!=updateInstances){
                updateInstances.add(instance);
            }
        }
    }

    /**使用batch方式执行语句*/
    protected int executeBatch(Object[] instances){
        int effect = 0;
        connectionExecutor.name(name()).sql(getStatement()).startBatch();
        try {
            for (int i = 0; i < instances.length; i += option.perBatchCount) {
                int end = Math.min(i + option.perBatchCount, instances.length);
                logger.trace("批处理,总个数:{},当前范围:{}-{}", instances.length, i, end);
                for (int j = i; j < end; j++) {
                    this.index = j;
                    List parameters = getParameters();
                    connectionExecutor.batchParameters(parameters);
                }
                effect += connectionExecutor.executeBatch();
                connectionExecutor.clearBatch();
            }
        }finally {
            connectionExecutor.closeBatch();
        }
        return effect;
    }

    /**获取部分列*/
    protected List<Property> getPartColumnPropertyList(Entity entity){
        List<Property> propertyList = null;
        if(option.partColumnSet.isEmpty()){
            propertyList = entity.properties;
        }else{
            //获取部分更新字段属性列表
            propertyList = new ArrayList<>();
            for(Property property:entity.properties){
                if(option.partColumnSet.contains(property.column)||option.partColumnSet.contains(property.name)){
                    propertyList.add(property);
                }
            }
        }
        if(propertyList.isEmpty()){
            throw new IllegalArgumentException("请检查partColumn方法参数是否合法!数据库表["+entity.tableName+"]不存在指定字段"+option.partColumnSet);
        }
        return propertyList;
    }
}
