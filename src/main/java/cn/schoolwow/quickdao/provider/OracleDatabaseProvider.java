package cn.schoolwow.quickdao.provider;

import cn.schoolwow.quickdao.dao.dcl.AbstractDabaseControl;
import cn.schoolwow.quickdao.dao.dcl.OracleDatabaseControl;
import cn.schoolwow.quickdao.dao.ddl.AbstractDatabaseDefinition;
import cn.schoolwow.quickdao.dao.ddl.OracleDatabaseDefinition;
import cn.schoolwow.quickdao.dao.dql.condition.Condition;
import cn.schoolwow.quickdao.dao.dql.condition.OracleCondition;
import cn.schoolwow.quickdao.dao.dql.subCondition.AbstractSubCondition;
import cn.schoolwow.quickdao.dao.dql.subCondition.SubCondition;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.internal.Query;
import cn.schoolwow.quickdao.domain.internal.SubQuery;

import java.util.HashMap;
import java.util.Map;

public class OracleDatabaseProvider extends AbstractDatabaseProvider {

    @Override
    public AbstractDatabaseDefinition getDatabaseDefinitionInstance(QuickDAOConfig quickDAOConfig) {
        return new OracleDatabaseDefinition(quickDAOConfig);
    }

    @Override
    public AbstractDabaseControl getDatabaseControlInstance(QuickDAOConfig quickDAOConfig) {
        return new OracleDatabaseControl(quickDAOConfig);
    }

    @Override
    public Condition getConditionInstance(Query query) {
        return new OracleCondition(query);
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
        fieldTypeMapping.put("byte", "");
        fieldTypeMapping.put("java.lang.Byte", "");
        fieldTypeMapping.put("[B", "");
        fieldTypeMapping.put("boolean", "");
        fieldTypeMapping.put("char", "CHAR");
        fieldTypeMapping.put("java.lang.Character", "CHAR");
        fieldTypeMapping.put("short", "INTEGER");
        fieldTypeMapping.put("java.lang.Short", "INTEGER");
        fieldTypeMapping.put("int", "INTEGER");
        fieldTypeMapping.put("java.lang.Integer", "INTEGER");
        fieldTypeMapping.put("float", "BINARY_FLOAT");
        fieldTypeMapping.put("java.lang.Float", "BINARY_FLOAT");
        fieldTypeMapping.put("long", "INTEGER");
        fieldTypeMapping.put("java.lang.Long", "INTEGER");
        fieldTypeMapping.put("double", "BINARY_DOUBLE");
        fieldTypeMapping.put("java.lang.Double", "BINARY_DOUBLE");
        fieldTypeMapping.put("java.lang.String", "VARCHAR2(255)");
        fieldTypeMapping.put("java.util.Date", "TIMESTAMP");
        fieldTypeMapping.put("java.sql.Date", "DATE");
        fieldTypeMapping.put("java.sql.Timestamp", "TIMESTAMP");
        fieldTypeMapping.put("java.time.LocalDate", "DATE");
        fieldTypeMapping.put("java.time.LocalDateTime", "TIMESTAMP");
        fieldTypeMapping.put("java.sql.Array", "");
        fieldTypeMapping.put("java.math.BigDecimal", "INTEGER");
        fieldTypeMapping.put("java.sql.Blob", "BLOB");
        fieldTypeMapping.put("java.sql.Clob", "CLOB");
        fieldTypeMapping.put("java.sql.NClob", "NCLOB");
        fieldTypeMapping.put("java.sql.Ref", "");
        fieldTypeMapping.put("java.net.URL", "");
        fieldTypeMapping.put("java.sql.RowId", "");
        fieldTypeMapping.put("java.sql.SQLXML", "");
        fieldTypeMapping.put("java.io.InputStream", "");
        fieldTypeMapping.put("java.io.Reader", "");
        return fieldTypeMapping;
    }

    @Override
    public String name() {
        return "oracle";
    }
}
