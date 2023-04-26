package cn.schoolwow.quickdao.statement.dml.instance;

import cn.schoolwow.quickdao.annotation.IdStrategy;
import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.internal.ManipulationOption;
import cn.schoolwow.quickdao.util.ParametersUtil;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**插入记录*/
public class InsertInstanceDatabaseStatement extends AbstractDMLInstanceDatabaseStatement {
    /**实体类信息*/
    private Entity entity;

    /**实例列表*/
    protected Object[] instances;

    /**属性列表*/
    private List<Property> propertyList;

    public InsertInstanceDatabaseStatement(Object[] instances, ManipulationOption option, QuickDAOConfig quickDAOConfig) {
        super(option, quickDAOConfig);
        this.entity = quickDAOConfig.getEntityByClassName(instances[0].getClass().getName());
        this.instances = instances;
        this.propertyList = getPartColumnPropertyList(entity);
    }

    @Override
    public int executeUpdate(){
        connectionExecutor.name(name())
                .returnGeneratedKeys(option.returnGeneratedKeys)
                .sql(getStatement());
        int effect = 0;
        for(int i=0;i<instances.length;i++){
            this.index = i;
            Object instance = instances[i];
            List parameters = getParameters();
            effect += connectionExecutor.parameters(parameters).executeUpdate();
            //设置自增id
            if (option.returnGeneratedKeys && null != entity.id && entity.id.strategy.equals(IdStrategy.AutoIncrement)) {
                String[] generatedKeysValue = new String[1];
                switch (quickDAOConfig.databaseProvider.name().toLowerCase()) {
                    case "oracle": {
                        String getIdValueSQL = "select " + entity.tableName + "_seq.currVal from dual";
                        connectionExecutor.name("获取自增id").sql(getIdValueSQL).executeQuery((resultSet) -> {
                            if (resultSet.next()) {
                                generatedKeysValue[0] = resultSet.getString(1);
                            }
                        });
                    }
                    break;
                    default: {
                        generatedKeysValue[0] = connectionExecutor.getGeneratedKeys();
                    }
                    break;
                }
                ParametersUtil.setGeneratedKeysValue(instance, entity, generatedKeysValue[0]);
            }
        }
        return effect;
    }

    @Override
    public String getStatement() {
        String key = "insert_" + entity.tableName + "_" + quickDAOConfig.databaseProvider.name();
        if(option.partColumnSet.isEmpty()){
            if(!quickDAOConfig.statementCache.contains(key)){
                String sql = generateInsertStatement(entity.properties);
                quickDAOConfig.statementCache.put(key, sql);
            }
            return quickDAOConfig.statementCache.get(key);
        }
        String sql = generateInsertStatement(propertyList);
        return sql;
    }

    @Override
    public List getParameters() {
        List parameterList = new ArrayList();
        Object instance = instances[index];
        for (Property property : propertyList) {
            if (property.id && property.strategy == IdStrategy.AutoIncrement) {
                continue;
            }
            if (property.id && property.strategy == IdStrategy.IdGenerator) {
                setNextGenerateId(property, instance);
            }
            if (property.createdAt || property.updateAt) {
                ParametersUtil.setCurrentDateTime(property, instance);
            }
            Object value = null;
            if (null != quickDAOConfig.databaseOption.insertColumnValueFunction) {
                value = quickDAOConfig.databaseOption.insertColumnValueFunction.apply(property);
            }
            if (null == value) {
                value = ParametersUtil.getFieldValueFromInstance(instance, property.name);
            }
            parameterList.add(value);
        }
        return parameterList;
    }

    @Override
    public String name() {
        return "插入记录";
    }

    /**获取并缓存SQL语句*/
    private String generateInsertStatement(List<Property> properties){
        StringBuilder builder = new StringBuilder();
        builder.append("insert into " + quickDAOConfig.databaseProvider.escape(entity.tableName) + "(");
        for (Property property : properties) {
            if (property.id && property.strategy == IdStrategy.AutoIncrement) {
                continue;
            }
            builder.append(quickDAOConfig.databaseProvider.escape(property.column) + ",");
        }
        builder.deleteCharAt(builder.length() - 1);
        builder.append(") values(");
        for (Property property : properties) {
            if (property.id && property.strategy == IdStrategy.AutoIncrement) {
                continue;
            }
            builder.append((null == property.function ? "?" : property.function) + ",");
        }
        builder.deleteCharAt(builder.length() - 1);
        builder.append(")");
        return builder.toString();
    }

    private void setNextGenerateId(Property property, Object instance){
        Field idField = ParametersUtil.getFieldFromInstance(instance, property.name);
        String value = quickDAOConfig.databaseOption.idGenerator.getNextId();
        try {
            switch (idField.getType().getName()) {
                case "int": {
                    idField.setInt(instance, Integer.parseInt(value));
                }
                break;
                case "java.lang.Integer": {
                    idField.set(instance, Integer.parseInt(value));
                }
                break;
                case "long": {
                    idField.setLong(instance, Long.parseLong(value));
                }
                break;
                case "java.lang.Long": {
                    idField.set(instance, Long.parseLong(value));
                }
                break;
                case "java.lang.String": {
                    idField.set(instance, value);
                }
                break;
                default: {
                    throw new IllegalArgumentException("当前仅支持int,long,String类型的自增主键!自增字段名称:" + idField.getName() + ",类型:" + idField.getType().getName() + "!");
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("设置自增字段值时发生异常", e);
        }
    }
}
