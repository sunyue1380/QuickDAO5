package cn.schoolwow.quickdao.statement.dql.json;

import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.statement.dql.AbstractDQLDatabaseStatement;
import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**根据唯一字段列表获取个数*/
public class SelectCountByUniqueFieldsDatabaseStatement extends AbstractDQLDatabaseStatement {
    /**表名*/
    private String tableName;

    /**实例*/
    private JSONObject instance;

    /**唯一字段*/
    private Set<String> uniqueFieldNames;

    public SelectCountByUniqueFieldsDatabaseStatement(String tableName, JSONObject instance, Set<String> uniqueFieldNames, QuickDAOConfig quickDAOConfig) {
        super(quickDAOConfig);
        this.tableName = tableName;
        this.instance = instance;
        this.uniqueFieldNames = uniqueFieldNames;

    }

    @Override
    public String getStatement() {
        StringBuilder builder = new StringBuilder("select count(1) from " + quickDAOConfig.databaseProvider.escape(tableName) + " where");
        for(String fieldName:uniqueFieldNames){
            builder.append(" "+fieldName+" = ? and");
        }
        builder.delete(builder.length()-4,builder.length());
        String sql = builder.toString();
        return sql;
    }

    @Override
    public List getParameters() {
        List parameters = new ArrayList();
        for(String fieldName:uniqueFieldNames){
            parameters.add(instance.get(fieldName));
        }
        return parameters;
    }

    @Override
    public String name() {
        return "根据指定列查询个数";
    }
}
