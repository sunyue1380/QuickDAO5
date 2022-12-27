package cn.schoolwow.quickdao.dao.statement.manipulation;

import cn.schoolwow.quickdao.dao.statement.AbstractDatabaseStatement;
import cn.schoolwow.quickdao.dao.statement.DatabaseStatementOption;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * 插入JSON对象
 */
public class InsertJSONArrayDatabaseStatement extends AbstractDatabaseStatement {

    public InsertJSONArrayDatabaseStatement(DatabaseStatementOption databaseStatementOption, QuickDAOConfig quickDAOConfig) {
        super(databaseStatementOption, quickDAOConfig);
    }

    @Override
    public String getStatement() {
        StringBuilder builder = new StringBuilder("insert into " + quickDAOConfig.databaseProvider.escape(option.entity.tableName) + "(");
        for (Property property : option.entity.properties) {
            builder.append(quickDAOConfig.databaseProvider.escape(property.column) + ",");
        }
        builder.deleteCharAt(builder.length() - 1);
        builder.append(") values(");
        for (Property property : option.entity.properties) {
            builder.append("?,");
        }
        builder.deleteCharAt(builder.length() - 1);
        builder.append(")");
        String sql = builder.toString();
        return sql;
    }

    @Override
    public List getParameters(Object instance) {
        JSONObject jsonObject = (JSONObject) instance;
        List parameterList = new ArrayList();
        for (Property property : option.entity.properties) {
            Object value = jsonObject.get(property.column.toLowerCase());
            if(null==value){
                value = jsonObject.get(property.column.toUpperCase());
            }
            parameterList.add(value);
        }
        return parameterList;
    }

    @Override
    public String name() {
        return "JSONArray方式插入";
    }

}
