package cn.schoolwow.quickdao.statement.dml.json;

import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.internal.ManipulationOption;
import cn.schoolwow.quickdao.statement.dml.AbstractDMLDatabaseStatement;
import cn.schoolwow.quickdao.statement.dql.DQLDatabaseStatement;
import cn.schoolwow.quickdao.statement.dql.json.SelectCountByUniqueFieldsDatabaseStatement;
import com.alibaba.fastjson.JSONObject;

/**忽略插入JSONObject*/
public class InsertIgnoreJSONObjectDatabaseStatement extends AbstractDMLJSONDatabaseStatement {
    /**表名*/
    private String tableName;

    /**实例*/
    private JSONObject instance;

    public InsertIgnoreJSONObjectDatabaseStatement(String tableName, JSONObject instance, ManipulationOption option, QuickDAOConfig quickDAOConfig) {
        super(option, quickDAOConfig);
        this.tableName = tableName;
        this.instance = instance;
    }

    @Override
    public int executeUpdate(){
        int effect = 0;
        DQLDatabaseStatement selectCountByFieldsDatabaseStatement = new SelectCountByUniqueFieldsDatabaseStatement(tableName, instance, option.uniqueFieldNames, quickDAOConfig);
        int count = selectCountByFieldsDatabaseStatement.getCount();
        if(count<=0){
            AbstractDMLDatabaseStatement insertJSONObjectDatabaseStatement = new InsertJSONObjectDatabaseStatement(tableName, instance, option, quickDAOConfig);
            effect = insertJSONObjectDatabaseStatement.executeUpdate();
        }
        return effect;
    }

    @Override
    public String name() {
        return "InsertIgnoreJSONObject插入";
    }
}
