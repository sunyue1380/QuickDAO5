package cn.schoolwow.quickdao.statement.dml.instance;

import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.internal.ManipulationOption;
import cn.schoolwow.quickdao.util.ParametersUtil;

import java.util.ArrayList;
import java.util.List;

/**根据唯一性约束删除记录*/
public class DeleteInstanceByUniqueKeyDatabaseStatement extends AbstractDMLInstanceDatabaseStatement {
    /**实体类信息*/
    private Entity entity;

    /**实例列表*/
    private Object[] instances;

    public DeleteInstanceByUniqueKeyDatabaseStatement(Object[] instances, ManipulationOption option, QuickDAOConfig quickDAOConfig) {
        super(option, quickDAOConfig);
        this.entity = quickDAOConfig.getEntityByClassName(instances[0].getClass().getName());
        this.instances = instances;
    }

    @Override
    public int executeUpdate(){
        //TODO 优化点 唯一字段只有一个时,改成in查询
        return executeBatch(instances);
    }

    @Override
    public String getStatement() {
        String key = "deleteByUniqueKey_" + entity.tableName + "_" + quickDAOConfig.databaseProvider.name();
        if(!quickDAOConfig.statementCache.contains(key)){
            String sql = generateDeleteByUniqueKeyStatement();
            quickDAOConfig.statementCache.put(key, sql);
        }
        return quickDAOConfig.statementCache.get(key);
    }

    @Override
    public List getParameters() {
        Object instance = instances[index];
        List parameterList = new ArrayList();
        for (Property property : entity.uniqueProperties) {
            Object value = ParametersUtil.getFieldValueFromInstance(instance, property.name);
            parameterList.add(value);
        }
        return parameterList;
    }

    @Override
    public String name() {
        return "根据唯一性约束删除记录";
    }

    /**获取并缓存SQL语句*/
    private String generateDeleteByUniqueKeyStatement(){
        StringBuilder builder = new StringBuilder();
        builder.append("delete from " + quickDAOConfig.databaseProvider.escape(entity.tableName) + " where ");
        for (Property property : entity.uniqueProperties) {
            builder.append(quickDAOConfig.databaseProvider.escape(property.column) + " = " + (null == property.function ? "?" : property.function) + ",");
        }
        builder.deleteCharAt(builder.length() - 1);
        return builder.toString();
    }

}
