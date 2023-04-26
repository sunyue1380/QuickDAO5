package cn.schoolwow.quickdao.statement.dml.json;

import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.internal.ManipulationOption;
import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**插入JSONObject数据*/
public class InsertJSONObjectDatabaseStatement extends AbstractDMLJSONDatabaseStatement {
    /**表名*/
    private String tableName;

    /**json实例*/
    private JSONObject instance;

    /**列名集合*/
    private List<String> columns;

    public InsertJSONObjectDatabaseStatement(String tableName, JSONObject instance, ManipulationOption option, QuickDAOConfig quickDAOConfig) {
        super(option, quickDAOConfig);
        this.tableName = tableName;
        this.instance = instance;
        columns = getPartColumns(instance);
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
        List parameterList = new ArrayList();
        for(String column:columns){
            Object value = instance.get(column);
            parameterList.add(value);
        }
        return parameterList;
    }

    @Override
    public String name() {
        return "JSONObject插入";
    }
}
