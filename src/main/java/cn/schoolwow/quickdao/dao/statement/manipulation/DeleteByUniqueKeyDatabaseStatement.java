package cn.schoolwow.quickdao.dao.statement.manipulation;

import cn.schoolwow.quickdao.dao.statement.AbstractDatabaseStatement;
import cn.schoolwow.quickdao.dao.statement.DatabaseStatementOption;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.util.ParametersUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * 根据唯一性约束删除数据
 */
public class DeleteByUniqueKeyDatabaseStatement extends AbstractDatabaseStatement {

    public DeleteByUniqueKeyDatabaseStatement(DatabaseStatementOption databaseStatementOption, QuickDAOConfig quickDAOConfig) {
        super(databaseStatementOption, quickDAOConfig);
    }

    @Override
    public String getStatement() {
        String key = "deleteByUniqueKey_" + option.entity.tableName + "_" + quickDAOConfig.databaseProvider.name();
        if (!quickDAOConfig.statementCache.containsKey(key)) {
            StringBuilder builder = new StringBuilder();
            builder.append("delete from " + quickDAOConfig.databaseProvider.escape(option.entity.tableName) + " where ");
            for (Property property : option.entity.uniqueProperties) {
                builder.append(quickDAOConfig.databaseProvider.escape(property.column) + " = " + (null == property.function ? "?" : property.function) + ",");
            }
            builder.deleteCharAt(builder.length() - 1);
            quickDAOConfig.statementCache.put(key, builder.toString());
        }
        return quickDAOConfig.statementCache.get(key);
    }

    @Override
    public List getParameters(Object instance) {
        List parameterList = new ArrayList();
        for (Property property : option.entity.uniqueProperties) {
            Object value = ParametersUtil.getFieldValueFromInstance(instance, property.name);
            parameterList.add(value);
        }
        return parameterList;
    }

    @Override
    public String name() {
        return "根据唯一性约束删除记录";
    }
}
