package cn.schoolwow.quickdao.statement.dml.json;

import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.internal.ManipulationOption;
import com.alibaba.fastjson.JSONArray;

import java.util.List;

/**删除JSONArray数据*/
public class DeleteJSONArrayDatabaseStatement extends DeleteJSONObjectDatabaseStatement {
    /**json实例*/
    private JSONArray instances;

    public DeleteJSONArrayDatabaseStatement(String tableName, JSONArray instances, ManipulationOption option, QuickDAOConfig quickDAOConfig) {
        super(tableName, instances.getJSONObject(0), option, quickDAOConfig);
        this.instances = instances;
    }

    @Override
    public int executeUpdate(){
        int effect = 0;
        if(option.uniqueFieldNames.size()==1){
            String uniqueFieldName = option.uniqueFieldNames.iterator().next();
            effect = new DeleteJSONArrayBySingleFieldDatabaseStatement(tableName, instances, uniqueFieldName, option, quickDAOConfig).executeUpdate();
        }else{
            effect = executeBatch(instances);
        }
        return effect;
    }

    @Override
    public List getParameters() {
        this.instance = instances.getJSONObject(index);
        return super.getParameters();
    }

    @Override
    public String name() {
        return "JSONArray删除";
    }
}
