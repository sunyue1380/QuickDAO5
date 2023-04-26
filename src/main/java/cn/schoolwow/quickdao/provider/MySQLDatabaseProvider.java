package cn.schoolwow.quickdao.provider;

import cn.schoolwow.quickdao.dao.dcl.AbstractDabaseControl;
import cn.schoolwow.quickdao.dao.dcl.MySQLDatabaseControl;
import cn.schoolwow.quickdao.dao.ddl.AbstractDatabaseDefinition;
import cn.schoolwow.quickdao.dao.ddl.MySQLDatabaseDefinition;
import cn.schoolwow.quickdao.dao.dql.condition.AbstractCondition;
import cn.schoolwow.quickdao.dao.dql.condition.Condition;
import cn.schoolwow.quickdao.dao.dql.subCondition.AbstractSubCondition;
import cn.schoolwow.quickdao.dao.dql.subCondition.SubCondition;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.internal.Query;
import cn.schoolwow.quickdao.domain.internal.SubQuery;

import java.util.HashMap;
import java.util.Map;

public class MySQLDatabaseProvider extends AbstractDatabaseProvider {

    @Override
    public AbstractDatabaseDefinition getDatabaseDefinitionInstance(QuickDAOConfig quickDAOConfig) {
        return new MySQLDatabaseDefinition(quickDAOConfig);
    }

    @Override
    public AbstractDabaseControl getDatabaseControlInstance(QuickDAOConfig quickDAOConfig) {
        return new MySQLDatabaseControl(quickDAOConfig);
    }

    @Override
    public Condition getConditionInstance(Query query) {
        return new AbstractCondition(query);
    }

    @Override
    public SubCondition getSubConditionInstance(SubQuery subQuery) {
        return new AbstractSubCondition(subQuery);
    }

    @Override
    public String comment(String comment) {
        return "comment \"" + comment + "\"";
    }

    @Override
    public String escape(String value) {
        return "`" + value + "`";
    }

    @Override
    public Map<String, String> getTypeFieldMapping() {
        Map<String, String> fieldTypeMapping = new HashMap<>();
        fieldTypeMapping.put("byte", "TINYINT");
        fieldTypeMapping.put("java.lang.Byte", "TINYINT");
        fieldTypeMapping.put("[B", "LONGBLOB");
        fieldTypeMapping.put("boolean", "TINYINT");
        fieldTypeMapping.put("java.lang.Boolean", "TINYINT");
        fieldTypeMapping.put("char", "TINYINT");
        fieldTypeMapping.put("java.lang.Character", "TINYINT");
        fieldTypeMapping.put("short", "SMALLINT");
        fieldTypeMapping.put("java.lang.Short", "SMALLINT");
        fieldTypeMapping.put("int", "INT");
        fieldTypeMapping.put("java.lang.Integer", "INTEGER(11)");
        fieldTypeMapping.put("float", "FLOAT(4,2)");
        fieldTypeMapping.put("java.lang.Float", "FLOAT(4,2)");
        fieldTypeMapping.put("long", "BIGINT");
        fieldTypeMapping.put("java.lang.Long", "BIGINT");
        fieldTypeMapping.put("double", "DOUBLE(5,2)");
        fieldTypeMapping.put("java.lang.Double", "DOUBLE(5,2)");
        fieldTypeMapping.put("java.lang.String", "VARCHAR(255)");
        fieldTypeMapping.put("java.util.Date", "DATETIME");
        fieldTypeMapping.put("java.sql.Date", "DATE");
        fieldTypeMapping.put("java.sql.Time", "TIME");
        fieldTypeMapping.put("java.sql.Timestamp", "TIMESTAMP");
        fieldTypeMapping.put("java.time.LocalDate", "DATE");
        fieldTypeMapping.put("java.time.LocalDateTime", "DATETIME");
        fieldTypeMapping.put("java.sql.Array", "");
        fieldTypeMapping.put("java.math.BigDecimal", "DECIMAL");
        fieldTypeMapping.put("java.sql.Blob", "BLOB");
        fieldTypeMapping.put("java.sql.Clob", "TEXT");
        fieldTypeMapping.put("java.sql.NClob", "TEXT");
        fieldTypeMapping.put("java.sql.Ref", "");
        fieldTypeMapping.put("java.net.URL", "");
        fieldTypeMapping.put("java.sql.RowId", "");
        fieldTypeMapping.put("java.sql.SQLXML", "");
        fieldTypeMapping.put("java.io.InputStream", "LONGTEXT");
        fieldTypeMapping.put("java.io.Reader", "LONGTEXT");
        return fieldTypeMapping;
    }

    @Override
    public String name() {
        return "mysql";
    }
}
