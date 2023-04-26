package cn.schoolwow.quickdao.statement.dml.json;

import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.internal.ManipulationOption;
import com.alibaba.fastjson.JSONArray;

import java.util.List;

/**更新JSONArray数据*/
public class UpdateJSONArrayDatabaseStatement extends UpdateJSONObjectDatabaseStatement {
    /**json实例*/
    private JSONArray instances;

    public UpdateJSONArrayDatabaseStatement(String tableName, JSONArray instances, ManipulationOption option, QuickDAOConfig quickDAOConfig) {
        super(tableName, instances.getJSONObject(0), option, quickDAOConfig);
        this.instances = instances;
    }

    @Override
    public int executeUpdate(){
        return executeBatch(instances);
    }

    @Override
    public List getParameters() {
        this.instance = instances.getJSONObject(index);
        return super.getParameters();
    }

    @Override
    public String name() {
        return "JSONArray更新";
    }
}
