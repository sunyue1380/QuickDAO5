package cn.schoolwow.quickdao.statement.dml.instance;

import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.internal.ManipulationOption;
import cn.schoolwow.quickdao.util.ParametersUtil;

import java.util.ArrayList;
import java.util.List;

/**根据唯一性约束更新记录*/
public class UpdateInstanceByUniqueKeyDatabaseStatement extends AbstractDMLInstanceDatabaseStatement {
    /**实体类信息*/
    private Entity entity;

    /**实例列表*/
    private Object[] instances;

    /**属性列表*/
    private List<Property> propertyList;

    public UpdateInstanceByUniqueKeyDatabaseStatement(Object[] instances, ManipulationOption option, QuickDAOConfig quickDAOConfig) {
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
        String key = "updateByUniqueKey_" + entity.tableName + "_" + quickDAOConfig.databaseProvider.name();
        if(option.partColumnSet.isEmpty()){
            if(!quickDAOConfig.statementCache.contains(key)){
                String sql = generateUpdateByUniqueKeyStatement(entity.properties);
                quickDAOConfig.statementCache.put(key, sql);
            }
            return quickDAOConfig.statementCache.get(key);
        }
        String sql = generateUpdateByUniqueKeyStatement(propertyList);
        return sql;
    }

    @Override
    public List getParameters() {
        Object instance = instances[index];
        List parameterList = new ArrayList();
        for(Property property:propertyList){
            if (property.id || entity.uniqueProperties.contains(property)) {
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
        for (Property property : entity.uniqueProperties) {
            Object value = ParametersUtil.getFieldValueFromInstance(instance, property.name);
            parameterList.add(value);
        }
        return parameterList;
    }

    @Override
    public String name() {
        return "根据唯一性约束更新记录";
    }

    /**获取并缓存SQL语句*/
    private String generateUpdateByUniqueKeyStatement(List<Property> propertyList){
        StringBuilder builder = new StringBuilder();
        builder.append("update " + quickDAOConfig.databaseProvider.escape(entity.tableName) + " set ");
        for(Property property:propertyList){
            if (property.id || entity.uniqueProperties.contains(property)) {
                continue;
            }
            if (property.createdAt) {
                continue;
            }
            builder.append(quickDAOConfig.databaseProvider.escape(property.column) + " = " + (null == property.function ? "?" : property.function) + ",");
        }
        builder.deleteCharAt(builder.length() - 1);
        builder.append(" where ");
        for (Property property : entity.properties) {
            if (entity.uniqueProperties.contains(property) && !property.id) {
                builder.append(quickDAOConfig.databaseProvider.escape(property.column) + " = ? and ");
            }
        }
        builder.delete(builder.length() - 5, builder.length());
        return builder.toString();
    }

}
