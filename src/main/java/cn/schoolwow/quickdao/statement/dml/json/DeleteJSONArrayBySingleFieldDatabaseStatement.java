package cn.schoolwow.quickdao.statement.dml.json;

import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.internal.ManipulationOption;
import cn.schoolwow.quickdao.statement.dml.AbstractDMLDatabaseStatement;
import com.alibaba.fastjson.JSONArray;

import java.util.ArrayList;
import java.util.List;

/**删除JSONArray数据*/
public class DeleteJSONArrayBySingleFieldDatabaseStatement extends AbstractDMLDatabaseStatement {
    /**表名*/
    private String tableName;

    /**json实例*/
    private JSONArray instances;

    /**字段名*/
    private String fieldName;

    public DeleteJSONArrayBySingleFieldDatabaseStatement(String tableName, JSONArray instances, String fieldName, ManipulationOption option, QuickDAOConfig quickDAOConfig) {
        super(option, quickDAOConfig);
        this.tableName = tableName;
        this.instances = instances;
        this.fieldName = fieldName;
    }

    @Override
    public String getStatement() {
        StringBuilder builder = new StringBuilder();
        builder.append("delete from " + quickDAOConfig.databaseProvider.escape(tableName) + " where " + fieldName + " in (");
        for(int i=0;i<instances.size();i++){
            builder.append("?,");
        }
        builder.deleteCharAt(builder.length() - 1);
        builder.append(")");
        return builder.toString();
    }

    @Override
    public List getParameters() {
        List parameterList = new ArrayList();
        for(int i=0;i<instances.size();i++){
            Object value = instances.getJSONObject(i).get(fieldName);
            parameterList.add(value);
        }
        return parameterList;
    }

    @Override
    public String name() {
        return "JSONArray根据单个唯一字段删除";
    }
}
