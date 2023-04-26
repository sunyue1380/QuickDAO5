package cn.schoolwow.quickdao.provider;

import cn.schoolwow.quickdao.dao.dcl.AbstractDabaseControl;
import cn.schoolwow.quickdao.dao.dcl.PostgreDatabaseControl;
import cn.schoolwow.quickdao.dao.ddl.AbstractDatabaseDefinition;
import cn.schoolwow.quickdao.dao.ddl.PostgreDatabaseDefinition;
import cn.schoolwow.quickdao.dao.dql.condition.Condition;
import cn.schoolwow.quickdao.dao.dql.condition.PostgreCondition;
import cn.schoolwow.quickdao.dao.dql.subCondition.AbstractSubCondition;
import cn.schoolwow.quickdao.dao.dql.subCondition.SubCondition;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.internal.Query;
import cn.schoolwow.quickdao.domain.internal.SubQuery;

import java.util.HashMap;
import java.util.Map;

public class PostgreDatabaseProvider extends AbstractDatabaseProvider {

    @Override
    public AbstractDatabaseDefinition getDatabaseDefinitionInstance(QuickDAOConfig quickDAOConfig) {
        return new PostgreDatabaseDefinition(quickDAOConfig);
    }

    @Override
    public AbstractDabaseControl getDatabaseControlInstance(QuickDAOConfig quickDAOConfig) {
        return new PostgreDatabaseControl(quickDAOConfig);
    }

    @Override
    public Condition getConditionInstance(Query query) {
        return new PostgreCondition(query);
    }

    @Override
    public SubCondition getSubConditionInstance(SubQuery subQuery) {
        return new AbstractSubCondition(subQuery);
    }

    @Override
    public String comment(String comment) {
        return "";
    }

    @Override
    public String escape(String value) {
        return "\"" + value + "\"";
    }

    @Override
    public Map<String, String> getTypeFieldMapping() {
        Map<String, String> fieldTypeMapping = new HashMap<>();
        fieldTypeMapping.put("byte", "BOOLEAN");
        fieldTypeMapping.put("java.lang.Byte", "BOOLEAN");
        fieldTypeMapping.put("[B", "BIT");
        fieldTypeMapping.put("boolean", "BOOLEAN");
        fieldTypeMapping.put("java.lang.Boolean", "BOOLEAN");
        fieldTypeMapping.put("char", "CHAR");
        fieldTypeMapping.put("java.lang.Character", "CHARACTER");
        fieldTypeMapping.put("short", "SMALLINT");
        fieldTypeMapping.put("java.lang.Short", "SMALLINT");
        fieldTypeMapping.put("int", "INT");
        fieldTypeMapping.put("java.lang.Integer", "INTEGER");
        fieldTypeMapping.put("float", "FLOAT4");
        fieldTypeMapping.put("java.lang.Float", "FLOAT4");
        fieldTypeMapping.put("long", "BIGINT");
        fieldTypeMapping.put("java.lang.Long", "BIGINT");
        fieldTypeMapping.put("double", "FLOAT8");
        fieldTypeMapping.put("java.lang.Double", "FLOAT8");
        fieldTypeMapping.put("java.lang.String", "VARCHAR(255)");
        fieldTypeMapping.put("java.util.Date", "TIMESTAMP");
        fieldTypeMapping.put("java.sql.Date", "DATE");
        fieldTypeMapping.put("java.sql.Time", "TIME");
        fieldTypeMapping.put("java.sql.Timestamp", "TIMESTAMP");
        fieldTypeMapping.put("java.time.LocalDate", "DATE");
        fieldTypeMapping.put("java.time.LocalDateTime", "TIMESTAMP");
        fieldTypeMapping.put("java.sql.Array", "");
        fieldTypeMapping.put("java.math.BigDecimal", "DECIMAL");
        fieldTypeMapping.put("java.sql.Blob", "TEXT");
        fieldTypeMapping.put("java.sql.Clob", "TEXT");
        fieldTypeMapping.put("java.sql.NClob", "TEXT");
        fieldTypeMapping.put("java.sql.Ref", "");
        fieldTypeMapping.put("java.net.URL", "");
        fieldTypeMapping.put("java.sql.RowId", "");
        fieldTypeMapping.put("java.sql.SQLXML", "");
        fieldTypeMapping.put("java.io.InputStream", "TEXT");
        fieldTypeMapping.put("java.io.Reader", "TEXT");
        return fieldTypeMapping;
    }

    @Override
    public String name() {
        return "postgresql";
    }
}
