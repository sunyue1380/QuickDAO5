package cn.schoolwow.quickdao.dao.statement.manipulation;

import cn.schoolwow.quickdao.annotation.IdStrategy;
import cn.schoolwow.quickdao.dao.statement.AbstractDatabaseStatement;
import cn.schoolwow.quickdao.dao.statement.DatabaseStatementOption;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.util.ParametersUtil;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * 插入记录语句
 */
public class InsertDatabaseStatement extends AbstractDatabaseStatement {

    public InsertDatabaseStatement(DatabaseStatementOption databaseStatementOption, QuickDAOConfig quickDAOConfig) {
        super(databaseStatementOption, quickDAOConfig);
    }

    @Override
    public String getStatement() {
        String key = "insert_" + option.entity.tableName + "_" + quickDAOConfig.databaseProvider.name();
        if (!quickDAOConfig.statementCache.containsKey(key) || !option.partColumnSet.isEmpty()) {
            StringBuilder builder = new StringBuilder();
            builder.append("insert into " + quickDAOConfig.databaseProvider.escape(option.entity.tableName) + "(");
            for (Property property : option.entity.properties) {
                if (skipPartColumn(property)) {
                    continue;
                }
                if (property.id && property.strategy == IdStrategy.AutoIncrement) {
                    continue;
                }
                builder.append(quickDAOConfig.databaseProvider.escape(property.column) + ",");
            }
            builder.deleteCharAt(builder.length() - 1);
            builder.append(") values(");
            for (Property property : option.entity.properties) {
                if (property.id && property.strategy == IdStrategy.AutoIncrement) {
                    continue;
                }
                builder.append((null == property.function ? "?" : property.function) + ",");
            }
            builder.deleteCharAt(builder.length() - 1);
            builder.append(")");
            if (option.partColumnSet.isEmpty()) {
                quickDAOConfig.statementCache.put(key, builder.toString());
            }
        }
        return quickDAOConfig.statementCache.get(key);
    }

    @Override
    public List getParameters(Object instance) {
        List parameterList = new ArrayList();
        for (Property property : option.entity.properties) {
            if (skipPartColumn(property)) {
                continue;
            }
            if (property.id && property.strategy == IdStrategy.AutoIncrement) {
                continue;
            }
            if (property.id && property.strategy == IdStrategy.IdGenerator) {
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
}
