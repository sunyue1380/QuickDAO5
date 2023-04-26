package cn.schoolwow.quickdao.statement.dql.instance;

import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.statement.dql.AbstractDQLDatabaseStatement;

import java.util.List;

/**根据单个唯一字段获取存在记录*/
public class SelectExistsValueBySingleFieldDatabaseStatement extends AbstractDQLDatabaseStatement {
    /**表名*/
    private String tableName;

    /**字段名称*/
    private String fieldName;

    /**值列表*/
    private List parameters;

    public SelectExistsValueBySingleFieldDatabaseStatement(String tableName, String fieldName, List parameters, QuickDAOConfig quickDAOConfig) {
        super(quickDAOConfig);
        this.tableName = tableName;
        this.fieldName = fieldName;
        this.parameters = parameters;
    }

    @Override
    public String getStatement() {
        StringBuilder builder = new StringBuilder("select " + fieldName + " from " + quickDAOConfig.databaseProvider.escape(tableName) + " where " + fieldName + " in (");
        for(int i=0;i<parameters.size();i++){
            builder.append("?,");
        }
        builder.deleteCharAt(builder.length() - 1);
        builder.append(")");
        String sql = builder.toString();
        return sql;
    }

    @Override
    public List getParameters() {
        return parameters;
    }

    @Override
    public String name() {
        return "根据单列查询";
    }
}
