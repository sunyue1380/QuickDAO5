package cn.schoolwow.quickdao.statement.dml.instance;

import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.internal.ManipulationOption;
import cn.schoolwow.quickdao.util.ParametersUtil;

import java.util.ArrayList;
import java.util.List;

/**根据单个唯一性约束字段删除记录*/
public class DeleteInstanceBySingleFieldDatabaseStatement extends AbstractDMLInstanceDatabaseStatement {
    /**实体类信息*/
    private Entity entity;

    /**实例列表*/
    private Object[] instances;

    /**字段*/
    private Property property;

    public DeleteInstanceBySingleFieldDatabaseStatement(Object[] instances, Property property, ManipulationOption option, QuickDAOConfig quickDAOConfig) {
        super(option, quickDAOConfig);
        this.entity = quickDAOConfig.getEntityByClassName(instances[0].getClass().getName());
        this.instances = instances;
        this.property = property;
    }

    @Override
    public String getStatement() {
        StringBuilder builder = new StringBuilder();
        builder.append("delete from " + quickDAOConfig.databaseProvider.escape(entity.tableName) + " where " + property.column + " in (");
        for(int i=0;i<instances.length;i++){
            builder.append("?,");
        }
        builder.deleteCharAt(builder.length() - 1);
        builder.append(")");
        return builder.toString();
    }

    @Override
    public List getParameters() {
        List parameterList = new ArrayList();
        for(int i=0;i<instances.length;i++){
            Object value = ParametersUtil.getFieldValueFromInstance(instances[i], property.name);
            parameterList.add(value);
        }
        return parameterList;
    }

    @Override
    public String name() {
        return "根据单个唯一性约束字段删除记录";
    }

}
