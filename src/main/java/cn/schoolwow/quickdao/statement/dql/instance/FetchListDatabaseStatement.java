package cn.schoolwow.quickdao.statement.dql.instance;

import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.statement.dql.AbstractDQLDatabaseStatement;
import com.alibaba.fastjson.JSONArray;

import java.util.Arrays;
import java.util.List;

/**单字段查询*/
public class FetchListDatabaseStatement extends AbstractDQLDatabaseStatement {
    /**实体类信息*/
    private Entity entity;

    /**字段*/
    private String column;

    /**值*/
    private Object value;

    public FetchListDatabaseStatement(Entity entity, String column, Object value, QuickDAOConfig quickDAOConfig) {
        super(quickDAOConfig);
        this.entity = entity;
        this.column = column;
        this.value = value;
    }

    @Override
    public JSONArray getArray(){
        return getArray(entity, "t");
    }
    
    @Override
    public String getStatement() {
        String key = "fetch_" + entity.tableName + "_" + column + "_" + quickDAOConfig.databaseProvider.name();
        if (!quickDAOConfig.statementCache.containsKey(key)) {
            StringBuilder builder = new StringBuilder("select ");
            builder.append(columns(entity, "t"));
            Property property = entity.getPropertyByFieldName(column);
            builder.append(" from " + quickDAOConfig.databaseProvider.escape(entity.tableName) + " t where t." + quickDAOConfig.databaseProvider.escape(property.column) + " = " + (null == property.function ? "?" : property.function) + "");
            quickDAOConfig.statementCache.put(key, builder.toString());
        }
        String sql = quickDAOConfig.statementCache.get(key);
        return sql;
    }

    @Override
    public List getParameters() {
        return Arrays.asList(value);
    }

    @Override
    public String name() {
        return "单字段查询";
    }
}
