package cn.schoolwow.quickdao.statement.dql.instance;

import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.statement.dql.AbstractDQLDatabaseStatement;
import com.alibaba.fastjson.JSONArray;

import java.util.Collections;
import java.util.List;

/**查询null记录*/
public class FetchNullDatabaseStatement extends AbstractDQLDatabaseStatement {
    /**实体类信息*/
    private Entity entity;
    
    /**字段*/
    private String column;

    public FetchNullDatabaseStatement(Entity entity, String column, QuickDAOConfig quickDAOConfig) {
        super(quickDAOConfig);
        this.entity = entity;
        this.column = column;
    }

    @Override
    public JSONArray getArray(){
        return getArray(entity, "t");
    }

    @Override
    public String getStatement() {
        String key = "fetchNull_" + entity.tableName + "_" + column + "_" + quickDAOConfig.databaseProvider.name();
        if (!quickDAOConfig.statementCache.containsKey(key)) {
            StringBuilder builder = new StringBuilder("select ");
            builder.append(columns(entity, "t"));
            Property property = entity.getPropertyByFieldName(column);
            builder.append(" from " + quickDAOConfig.databaseProvider.escape(entity.tableName) + " as t where t." + quickDAOConfig.databaseProvider.escape(property.column) + " is null");
            quickDAOConfig.statementCache.put(key, builder.toString());
        }
        String sql = quickDAOConfig.statementCache.get(key);
        return sql;
    }

    @Override
    public List getParameters() {
        return Collections.emptyList();
    }

    @Override
    public String name() {
        return "查询字段为NULL的记录";
    }
}
