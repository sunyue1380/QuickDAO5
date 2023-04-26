package cn.schoolwow.quickdao.statement.dml.instance;

import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.internal.ManipulationOption;
import cn.schoolwow.quickdao.util.ParametersUtil;

import java.util.Arrays;
import java.util.List;

/**根据指定列删除记录*/
public class DeleteInstanceByPropertyDatabaseStatement extends AbstractDMLInstanceDatabaseStatement {
    /**实体类信息*/
    private Entity entity;

    /**实例列表*/
    private Object[] instances;

    /**数据库列*/
    private Property property;

    public DeleteInstanceByPropertyDatabaseStatement(Object[] instances, Property property, ManipulationOption option, QuickDAOConfig quickDAOConfig) {
        super(option, quickDAOConfig);
        this.entity = quickDAOConfig.getEntityByClassName(instances[0].getClass().getName());
        this.instances = instances;
        this.property =  property;
    }

    @Override
    public int executeUpdate(){
        return executeBatch(instances);
    }

    @Override
    public String getStatement() {
        String key = "deleteByProperty_" + entity.tableName + "_" + property.column + "_" + quickDAOConfig.databaseProvider.name();
        if(!quickDAOConfig.statementCache.contains(key)){
            String sql = generateDeleteByPropertyStatement();
            quickDAOConfig.statementCache.put(key, sql);
        }
        return quickDAOConfig.statementCache.get(key);
    }

    @Override
    public List getParameters() {
        Object instance = instances[index];
        Object value = ParametersUtil.getFieldValueFromInstance(instance, property.name);
        return Arrays.asList(value);
    }

    @Override
    public String name() {
        return "根据指定属性删除记录";
    }

    /**获取并缓存SQL语句*/
    private String generateDeleteByPropertyStatement(){
        return "delete from " + quickDAOConfig.databaseProvider.escape(entity.tableName) + " where " + quickDAOConfig.databaseProvider.escape(property.column) + " = ?";
    }

}
