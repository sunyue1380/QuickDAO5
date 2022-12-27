package cn.schoolwow.quickdao.dao.statement.manipulation;

import cn.schoolwow.quickdao.dao.statement.AbstractDatabaseStatement;
import cn.schoolwow.quickdao.dao.statement.DatabaseStatementOption;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.util.ParametersUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * 根据唯一性约束更新记录语句
 */
public class UpdateByUniqueKeyDatabaseStatement extends AbstractDatabaseStatement {

    public UpdateByUniqueKeyDatabaseStatement(DatabaseStatementOption databaseStatementOption, QuickDAOConfig quickDAOConfig) {
        super(databaseStatementOption, quickDAOConfig);
    }

    @Override
    public String getStatement() {
        String key = "updateByUniqueKey_" + option.entity.tableName + "_" + quickDAOConfig.databaseProvider.name();
        if (!quickDAOConfig.statementCache.containsKey(key) || !option.partColumnSet.isEmpty()) {
            StringBuilder builder = new StringBuilder();
            builder.append("update " + quickDAOConfig.databaseProvider.escape(option.entity.tableName) + " set ");
            for (Property property : option.entity.properties) {
                if (skipPartColumn(property)) {
                    continue;
                }
                if (property.id || option.entity.uniqueProperties.contains(property)) {
                    continue;
                }
                if (property.createdAt) {
                    continue;
                }
                builder.append(quickDAOConfig.databaseProvider.escape(property.column) + " = " + (null == property.function ? "?" : property.function) + ",");
            }
            builder.deleteCharAt(builder.length() - 1);
            builder.append(" where ");
            for (Property property : option.entity.properties) {
                if (option.entity.uniqueProperties.contains(property) && !property.id) {
                    builder.append(quickDAOConfig.databaseProvider.escape(property.column) + " = ? and ");
                }
            }
            builder.delete(builder.length() - 5, builder.length());
            if (option.partColumnSet.isEmpty()) {
                quickDAOConfig.statementCache.put(key, builder.toString());
            }
            return builder.toString();
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
            if (property.id || option.entity.uniqueProperties.contains(property)) {
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
        for (Property property : option.entity.uniqueProperties) {
            Object value = ParametersUtil.getFieldValueFromInstance(instance, property.name);
            parameterList.add(value);
        }
        return parameterList;
    }

    @Override
    public String name() {
        return "根据唯一性约束更新记录";
    }
}
