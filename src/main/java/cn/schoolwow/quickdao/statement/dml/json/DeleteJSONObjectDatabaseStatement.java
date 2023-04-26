package cn.schoolwow.quickdao.statement.dml.json;

import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.internal.ManipulationOption;
import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**删除JSONObject数据*/
public class DeleteJSONObjectDatabaseStatement extends AbstractDMLJSONDatabaseStatement {
    /**表名*/
    protected String tableName;

    /**json实例*/
    protected JSONObject instance;

    public DeleteJSONObjectDatabaseStatement(String tableName, JSONObject instance, ManipulationOption option, QuickDAOConfig quickDAOConfig) {
        super(option, quickDAOConfig);
        this.tableName = tableName;
        this.instance = instance;
    }

    @Override
    public String getStatement() {
        StringBuilder builder = new StringBuilder("delete from " + quickDAOConfig.databaseProvider.escape(tableName) + " where ");
        for(String uniqueFieldName:option.uniqueFieldNames){
            builder.append(quickDAOConfig.databaseProvider.escape(uniqueFieldName) + " = ? and");
        }
        builder.delete(builder.length()-4,builder.length());
        String sql = builder.toString();
        return sql;
    }

    @Override
    public List getParameters() {
        List parameterList = new ArrayList();
        for(String uniqueFieldName:option.uniqueFieldNames){
            Object value = instance.get(uniqueFieldName);
            parameterList.add(value);
        }
        return parameterList;
    }

    @Override
    public String name() {
        return "JSONObject删除";
    }
}
