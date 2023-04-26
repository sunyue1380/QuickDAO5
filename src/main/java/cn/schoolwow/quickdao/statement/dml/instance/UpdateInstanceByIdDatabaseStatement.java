package cn.schoolwow.quickdao.statement.dml.instance;

import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.internal.ManipulationOption;
import cn.schoolwow.quickdao.util.ParametersUtil;

import java.util.ArrayList;
import java.util.List;

/**根据ID更新记录*/
public class UpdateInstanceByIdDatabaseStatement extends AbstractDMLInstanceDatabaseStatement {
    /**实体类信息*/
    private Entity entity;

    /**实例列表*/
    private Object[] instances;

    /**属性列表*/
    private List<Property> propertyList;

    public UpdateInstanceByIdDatabaseStatement(Object[] instances, ManipulationOption option, QuickDAOConfig quickDAOConfig) {
        super(option, quickDAOConfig);
        this.entity = quickDAOConfig.getEntityByClassName(instances[0].getClass().getName());
        this.instances = instances;
        this.propertyList = getPartColumnPropertyList(entity);
    }

    @Override
    public int executeUpdate(){
        return executeBatch(instances);
    }

    @Override
    public String getStatement() {
        String key = "updateById_" + entity.tableName + "_" + quickDAOConfig.databaseProvider.name();
        if(option.partColumnSet.isEmpty()){
            if(!quickDAOConfig.statementCache.contains(key)){
                String sql = generateUpdateByIdStatement(entity.properties);
                quickDAOConfig.statementCache.put(key, sql);
            }
            return quickDAOConfig.statementCache.get(key);
        }
        String sql = generateUpdateByIdStatement(propertyList);
        return sql;
    }

    @Override
    public List getParameters() {
        Object instance = instances[index];
        List parameterList = new ArrayList();
        for(Property property:propertyList){
            if (property.id) {
                continue;
            }
            if (property.createdAt) {
                continue;
            }
            if (property.updateAt) {
                ParametersUtil.setCurrentDateTime(property, instance);
            }
            Object value = null;
            if (null != quickDAOConfig.databaseOption.updateColumnValueFunction) {
                value = quickDAOConfig.databaseOption.updateColumnValueFunction.apply(property);
            }
            if (null == value) {
                value = ParametersUtil.getFieldValueFromInstance(instance, property.name);
            }
            parameterList.add(value);
        }
        //再设置id属性
        Object idValue = ParametersUtil.getFieldValueFromInstance(instance, entity.id.name);
        parameterList.add(idValue);
        return parameterList;
    }

    @Override
    public String name() {
        return "根据ID更新记录";
    }

    /**获取并缓存SQL语句*/
    private String generateUpdateByIdStatement(List<Property> propertyList){
        StringBuilder builder = new StringBuilder();
        builder.append("update " + quickDAOConfig.databaseProvider.escape(entity.tableName) + " set ");
        for (Property property : propertyList) {
            if (property.id) {
                continue;
            }
            if (property.createdAt) {
                continue;
            }
            builder.append(quickDAOConfig.databaseProvider.escape(property.column) + " = ?,");
        }
        builder.deleteCharAt(builder.length() - 1);
        builder.append(" where " + quickDAOConfig.databaseProvider.escape(entity.id.column) + " = ?");
        return builder.toString();
    }

}
