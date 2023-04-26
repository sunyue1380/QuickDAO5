package cn.schoolwow.quickdao.statement.dql.instance;

import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.statement.dql.AbstractDQLDatabaseStatement;
import cn.schoolwow.quickdao.util.ParametersUtil;

import java.util.List;

/**根据单个唯一字段获取存在记录个数*/
public class SelectCountBySingleFieldDatabaseStatement extends AbstractDQLDatabaseStatement {
    /**实例列表*/
    private Object[] instances;

    /**字段*/
    private Property property;

    private Entity entity;

    public SelectCountBySingleFieldDatabaseStatement(Object[] instances, Property property, QuickDAOConfig quickDAOConfig) {
        super(quickDAOConfig);
        this.instances = instances;
        this.property = property;
        this.entity = quickDAOConfig.getEntityByClassName(instances[0].getClass().getName());
    }

    @Override
    public String getStatement() {
        StringBuilder builder = new StringBuilder("select count(1) from " + quickDAOConfig.databaseProvider.escape(entity.tableName) + " where " + property.column + " in (");
        for(int i=0;i<instances.length;i++){
            builder.append("?,");
        }
        builder.deleteCharAt(builder.length() - 1);
        builder.append(")");
        String sql = builder.toString();
        return sql;
    }

    @Override
    public List getParameters() {
        List parameterList = ParametersUtil.getFieldValueListFromInstance(instances, property.name);
        return parameterList;
    }

    @Override
    public String name() {
        return "根据单列查询个数";
    }
}
