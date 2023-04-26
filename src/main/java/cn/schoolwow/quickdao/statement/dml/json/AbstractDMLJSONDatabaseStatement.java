package cn.schoolwow.quickdao.statement.dml.json;

import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.internal.ManipulationOption;
import cn.schoolwow.quickdao.statement.dml.AbstractDMLDatabaseStatement;
import cn.schoolwow.quickdao.statement.dql.DQLDatabaseStatement;
import cn.schoolwow.quickdao.statement.dql.instance.SelectExistsValueBySingleFieldDatabaseStatement;
import cn.schoolwow.quickdao.statement.dql.json.SelectCountByUniqueFieldsDatabaseStatement;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class AbstractDMLJSONDatabaseStatement extends AbstractDMLDatabaseStatement {

    public AbstractDMLJSONDatabaseStatement(ManipulationOption option, QuickDAOConfig quickDAOConfig) {
        super(option, quickDAOConfig);
    }

    /**根据单个字段区分数据是否存在*/
    protected void distinguishJSONArrayBySingleField(String tableName, JSONArray instances, String column, JSONArray insertArray, JSONArray updateArray){
        List parameters = new ArrayList(instances.size());
        for(int i=0;i<instances.size();i++){
            Object value = instances.getJSONObject(i).get(column);
            if (null != value) {
                parameters.add(value);
            }
        }
        DQLDatabaseStatement selectExistsValueBySingleFieldDatabaseStatement = new SelectExistsValueBySingleFieldDatabaseStatement(tableName, column, parameters, quickDAOConfig);
        List existValues = selectExistsValueBySingleFieldDatabaseStatement.getSingleColumnList();
        for(int i=0;i<instances.size();i++){
            JSONObject instance = instances.getJSONObject(i);
            Object value = instance.get(column);
            if(null!=insertArray&&!existValues.contains(value)){
                insertArray.add(instance);
            }else if(null!=updateArray){
                updateArray.add(instance);
            }
        }
    }

    /**根据多个字段区分数据是否存在*/
    protected void distinguishJSONArrayByMultipleField(String tableName, JSONArray instances, JSONArray insertArray, JSONArray updateArray){
        for(int i=0;i<instances.size();i++){
            JSONObject instance = instances.getJSONObject(i);
            DQLDatabaseStatement selectCountByUniqueFieldsDatabaseStatement = new SelectCountByUniqueFieldsDatabaseStatement(tableName, instance, option.uniqueFieldNames, quickDAOConfig);
            int count = selectCountByUniqueFieldsDatabaseStatement.getCount();
            if(null!=insertArray&&count<=0){
                insertArray.add(instance);
            }else if(null!=updateArray){
                updateArray.add(instance);
            }
        }
    }

    /**batch方式执行语句*/
    protected int executeBatch(JSONArray instances){
        connectionExecutor.returnGeneratedKeys(false)
                .name(name())
                .sql(getStatement());
        int effect = 0;
        for (int i = 0; i < instances.size(); i += option.perBatchCount) {
            connectionExecutor.startBatch();
            try {
                int end = Math.min(i + option.perBatchCount, instances.size());
                for (int j = i; j < end; j++) {
                    this.index = j;
                    List parameters = getParameters();
                    connectionExecutor.batchParameters(parameters);
                }
                effect += connectionExecutor.executeBatch();
            }finally {
                connectionExecutor.closeBatch();
            }
        }
        return effect;
    }

    /**获取JSONObject的部分列*/
    protected List<String> getPartColumns(JSONObject instance){
        List<String> columns = null;
        if(option.partColumnSet.isEmpty()){
            Set<String> keySet = instance.keySet();
            columns = new ArrayList<>(keySet.size());
            for(String key:keySet){
                columns.add(key);
            }
        }else{
            columns = new ArrayList<>(option.partColumnSet);
        }
        return columns;
    }

}
