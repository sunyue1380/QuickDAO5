package cn.schoolwow.quickdao.statement.dml.json;

import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.internal.ManipulationOption;
import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**更新JSONObject数据*/
public class UpdateJSONObjectDatabaseStatement extends AbstractDMLJSONDatabaseStatement {
    /**表名*/
    private String tableName;

    /**json实例*/
    protected JSONObject instance;

    /**列名集合*/
    private List<String> columns;

    public UpdateJSONObjectDatabaseStatement(String tableName, JSONObject instance, ManipulationOption option, QuickDAOConfig quickDAOConfig) {
        super(option, quickDAOConfig);
        this.tableName = tableName;
        this.instance = instance;
        columns = getPartColumns(instance);
    }

    @Override
    public String getStatement() {
        StringBuilder builder = new StringBuilder("update " + quickDAOConfig.databaseProvider.escape(tableName) + " set ");
        for(String column:columns){
            builder.append(quickDAOConfig.databaseProvider.escape(column) + " = ?,");
        }
        builder.deleteCharAt(builder.length() - 1);
        builder.append(" where");
        for(String uniqueFieldName:option.uniqueFieldNames){
            builder.append(" "+uniqueFieldName + " = ? and");
        }
        builder.delete(builder.length()-4,builder.length());
        String sql = builder.toString();
        return sql;
    }

    @Override
    public List getParameters() {
        List parameterList = new ArrayList();
        for(String column:columns){
            Object value = instance.get(column);
            parameterList.add(value);
        }
        for(String uniqueFieldName:option.uniqueFieldNames){
            Object value = instance.get(uniqueFieldName);
            parameterList.add(value);
        }
        return parameterList;
    }

    @Override
    public String name() {
        return "JSONObject更新";
    }
}
