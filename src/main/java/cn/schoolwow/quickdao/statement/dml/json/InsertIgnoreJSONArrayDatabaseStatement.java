package cn.schoolwow.quickdao.statement.dml.json;

import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.internal.ManipulationOption;
import cn.schoolwow.quickdao.statement.dml.AbstractDMLDatabaseStatement;
import com.alibaba.fastjson.JSONArray;

/**忽略插入JSONArray*/
public class InsertIgnoreJSONArrayDatabaseStatement extends AbstractDMLJSONDatabaseStatement {
    /**表名*/
    private String tableName;

    /**json实例*/
    private JSONArray instances;

    public InsertIgnoreJSONArrayDatabaseStatement(String tableName, JSONArray instances, ManipulationOption option, QuickDAOConfig quickDAOConfig) {
        super(option, quickDAOConfig);
        this.tableName = tableName;
        this.instances = instances;
    }

    @Override
    public int executeUpdate(){
        JSONArray insertArray = new JSONArray();
        if(option.uniqueFieldNames.size()==1){
            String uniqueFieldName = option.uniqueFieldNames.iterator().next();
            distinguishJSONArrayBySingleField(tableName, instances, uniqueFieldName, insertArray, null);
        }else{
            distinguishJSONArrayByMultipleField(tableName, instances, insertArray, null);
        }
        if(insertArray.isEmpty()){
            return 0;
        }
        AbstractDMLDatabaseStatement insertJSONArrayDatabaseStatement = new InsertJSONArrayDatabaseStatement(tableName,insertArray,option,quickDAOConfig);
        int effect = insertJSONArrayDatabaseStatement.executeUpdate();
        return effect;
    }

    @Override
    public String name() {
        return "InsertIgnoreJSONArray插入";
    }
}
