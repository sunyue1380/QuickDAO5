package cn.schoolwow.quickdao.statement.dml.json;

import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.internal.ManipulationOption;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**插入JSONArray数据*/
public class InsertJSONArrayDatabaseStatement extends AbstractDMLJSONDatabaseStatement {
    /**表名*/
    private String tableName;

    /**json实例*/
    private JSONArray instances;

    /**列名集合*/
    private List<String> columns;

    public InsertJSONArrayDatabaseStatement(String tableName, JSONArray instances, ManipulationOption option, QuickDAOConfig quickDAOConfig) {
        super(option, quickDAOConfig);
        this.tableName = tableName;
        this.instances = instances;
        option.returnGeneratedKeys = false;
        columns = getPartColumns(instances.getJSONObject(0));
    }

    @Override
    public int executeUpdate(){
        return executeBatch(instances);
    }

    @Override
    public String getStatement() {
        StringBuilder builder = new StringBuilder("insert into " + quickDAOConfig.databaseProvider.escape(tableName) + "(");
        for(String column:columns){
            builder.append(quickDAOConfig.databaseProvider.escape(column) + ",");
        }
        builder.deleteCharAt(builder.length() - 1);
        builder.append(") values(");
        for(String column:columns){
            builder.append("?,");
        }
        builder.deleteCharAt(builder.length() - 1);
        builder.append(")");
        String sql = builder.toString();
        return sql;
    }

    @Override
    public List getParameters() {
        JSONObject instance = instances.getJSONObject(index);
        List parameterList = new ArrayList();
        for(String column:columns){
            Object value = instance.get(column);
            parameterList.add(value);
        }
        return parameterList;
    }

    @Override
    public String name() {
        return "JSONArray插入";
    }
}
