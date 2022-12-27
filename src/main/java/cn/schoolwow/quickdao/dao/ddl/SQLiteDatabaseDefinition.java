package cn.schoolwow.quickdao.dao.ddl;

import cn.schoolwow.quickdao.annotation.IdStrategy;
import cn.schoolwow.quickdao.annotation.IndexType;
import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.IndexField;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class SQLiteDatabaseDefinition extends AbstractDatabaseDefinition {
    private Logger logger = LoggerFactory.getLogger(SQLiteDatabaseDefinition.class);

    public SQLiteDatabaseDefinition(QuickDAOConfig quickDAOConfig) {
        super(quickDAOConfig);
    }

    @Override
    public boolean hasTable(String tableName) {
        String hasTableSQL = "select name from sqlite_master where type='table' and name = '" + tableName + "';";
        return connectionExecutor.name("判断表是否存在").sql(hasTableSQL).executeAndCheckExists();
    }

    @Override
    public boolean hasColumn(String tableName, String columnName) {
        String hasTableColumnSQL = "select * from sqlite_master where name = '" + tableName + "' and sql like '%" + columnName + "%'";
        return connectionExecutor.name("判断表指定列是否存在").sql(hasTableColumnSQL).executeAndCheckExists();
    }

    @Override
    public List<Entity> getDatabaseEntityList() {
        List<Entity> entityList = new ArrayList<>();
        String getEntityListSQL = "select name from sqlite_master where type='table' and name != 'sqlite_sequence';";
        connectionExecutor.name("获取表列表").sql(getEntityListSQL).executeQuery(resultSet -> {
            while (resultSet.next()) {
                Entity entity = new Entity();
                entity.tableName = resultSet.getString("name");
                entityList.add(entity);
            }
        });
        getIndex(entityList);
        return entityList;
    }

    @Override
    public Entity getDatabaseEntity(String tableName) {
        List<Entity> entityList = new ArrayList<>();
        String getEntityListSQL = "select name from sqlite_master where type='table' and name = '"+tableName+"';";
        connectionExecutor.name("获取表列表").sql(getEntityListSQL).executeQuery(resultSet -> {
            while (resultSet.next()) {
                Entity entity = new Entity();
                entity.tableName = resultSet.getString("name");
                entity.properties = getPropertyList(tableName);
                entityList.add(entity);
            }
        });
        getIndex(entityList);
        if(entityList.isEmpty()){
            return null;
        }
        return entityList.get(0);
    }

    @Override
    public List<Property> getPropertyList(String tableName) {
        String getEntityPropertyListSQL = "PRAGMA table_info(`" + tableName + "`);";
        List<Property> propertyList = new ArrayList<>();
        connectionExecutor.name("获取表字段列表信息").sql(getEntityPropertyListSQL).executeQuery(resultSet -> {
            while (resultSet.next()) {
                Property property = new Property();
                getProperty(resultSet, property);
                propertyList.add(property);
            }
        });
        return propertyList;
    }


    @Override
    public Property getProperty(String tableName, String columnName) {
        List<Property> propertyList = getPropertyList(tableName);
        for(Property property:propertyList){
            if(property.column.equalsIgnoreCase(columnName)){
                return property;
            }
        }
        return null;
    }

    @Override
    public void create(Entity entity) {
        StringBuilder createTableBuilder = new StringBuilder();
        if (quickDAOConfig.databaseOption.openForeignKey && null != entity.foreignKeyProperties && entity.foreignKeyProperties.size() > 0) {
            //手动开启外键约束
            createTableBuilder.append("PRAGMA foreign_keys = ON;");
        }
        createTableBuilder.append("create table " + quickDAOConfig.databaseProvider.escape(entity.tableName) + "(");
        for (Property property : entity.properties) {
            if (property.id && property.strategy == IdStrategy.AutoIncrement) {
                createTableBuilder.append(getAutoIncrementSQL(property));
            } else {
                createTableBuilder.append(quickDAOConfig.databaseProvider.escape(property.column) + " " + property.columnType + (null == property.length ? "" : "(" + property.length + ")"));
                if (property.notNull) {
                    createTableBuilder.append(" not null");
                }
                if (null != property.defaultValue && !property.defaultValue.isEmpty()) {
                    createTableBuilder.append(" default " + property.defaultValue);
                }
                if (null != property.comment) {
                    createTableBuilder.append(" " + quickDAOConfig.databaseProvider.comment(property.comment));
                }
                if (null != property.escapeCheck && !property.escapeCheck.isEmpty()) {
                    createTableBuilder.append(" check " + property.escapeCheck);
                }
            }
            createTableBuilder.append(",");
        }
        if (quickDAOConfig.databaseOption.openForeignKey && null != entity.foreignKeyProperties && entity.foreignKeyProperties.size() > 0) {
            for (Property property : entity.foreignKeyProperties) {
                createTableBuilder.append("foreign key(" + quickDAOConfig.databaseProvider.escape(property.column) + ") references ");
                String operation = property.foreignKey.foreignKeyOption().getOperation();
                createTableBuilder.append(quickDAOConfig.databaseProvider.escape(quickDAOConfig.getEntityByClassName(property.foreignKey.table().getName()).tableName) + "(" + quickDAOConfig.databaseProvider.escape(property.foreignKey.field()) + ") ON DELETE " + operation + " ON UPDATE " + operation);
                createTableBuilder.append(",");
            }
        }
        createTableBuilder.deleteCharAt(createTableBuilder.length() - 1);
        createTableBuilder.append(")");
        if (null != entity.comment) {
            createTableBuilder.append(" " + quickDAOConfig.databaseProvider.comment(entity.comment));
        }
        createTableBuilder.append(";");
        //创建索引
        for (IndexField indexField : entity.indexFieldList) {
            createTableBuilder.append(getCreateIndexStatement(indexField));
        }
        String createTableSQL = createTableBuilder.toString();
        connectionExecutor.name("生成新表").sql(createTableSQL).executeUpdate();
    }

    @Override
    public void alterColumn(Property property) {
        StringBuilder alterColumnBuilder = new StringBuilder("PRAGMA legacy_alter_table = ON;");
        String oldTableName = quickDAOConfig.databaseProvider.escape(property.entity.tableName + "_old_" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmm")));
        alterColumnBuilder.append("create table " + oldTableName + " as select * from " + quickDAOConfig.databaseProvider.escape(property.entity.tableName)+";");
        alterColumnBuilder.append("drop table " + quickDAOConfig.databaseProvider.escape(property.entity.tableName)+";");

        alterColumnBuilder.append("PRAGMA legacy_alter_table = OFF;");
        Entity entity = getDatabaseEntity(property.entity.tableName);
        for(Property property1:entity.properties){
            if(property1.column.equalsIgnoreCase(property.column)){
                property1.replaceField(property);
                break;
            }
        }
        alterColumnBuilder.append(getCreateTableStatement(entity));
        long count = quickDAOConfig.dao.query(entity.tableName).execute().count();
        alterColumnBuilder.append("insert into " + quickDAOConfig.databaseProvider.escape("sqlite_sequence") + " (name, seq) values ('" + entity.tableName + "', '" + count + "');");
        alterColumnBuilder.append("insert into " + quickDAOConfig.databaseProvider.escape(entity.tableName) + "(");
        for (Property property1 : entity.properties) {
            alterColumnBuilder.append(quickDAOConfig.databaseProvider.escape(property1.column) + ",");
        }
        alterColumnBuilder.deleteCharAt(alterColumnBuilder.length() - 1);
        alterColumnBuilder.append(") select ");
        for (Property property1 : entity.properties) {
            alterColumnBuilder.append(quickDAOConfig.databaseProvider.escape(property1.column) + ",");
        }
        alterColumnBuilder.deleteCharAt(alterColumnBuilder.length() - 1);
        alterColumnBuilder.append(" from " + oldTableName + ";");
        alterColumnBuilder.append("drop table " + oldTableName + ";");

        String alterColumnSQL = alterColumnBuilder.toString();
        connectionExecutor.name("修改列").sql(alterColumnSQL).executeUpdate();
        quickDAOConfig.deleteDatabaseEntityCache(property.entity.tableName);
    }

    @Override
    public Property dropColumn(String tableName, String columnName) {
        Property deletedProperty = getProperty(tableName, columnName);
        if (null == deletedProperty) {
            throw new IllegalArgumentException("被删除的列不存在!表名:" + tableName + ",列名:" + columnName);
        }
        deletedProperty.entity = getDatabaseEntity(tableName);
        StringBuilder dropColumnBuilder = new StringBuilder();
        String temporaryTableName = quickDAOConfig.databaseProvider.escape(tableName + "_temporary_" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmm")));
        dropColumnBuilder.append("create table " + temporaryTableName + " as select ");
        for(Property property:deletedProperty.entity.properties){
            if(property.column.equalsIgnoreCase(columnName)){
                continue;
            }
            dropColumnBuilder.append(property.column+",");
        }
        dropColumnBuilder.deleteCharAt(dropColumnBuilder.length()-1);
        dropColumnBuilder.append(" from " + tableName + " where 1 = 1;");
        dropColumnBuilder.append("drop table " + tableName + ";");
        dropColumnBuilder.append("create table " + tableName + " as select * from " + temporaryTableName+";");
        dropColumnBuilder.append("drop table " + temporaryTableName + ";");
        String dropColumnSQL = dropColumnBuilder.toString();
        connectionExecutor.name("删除列").sql(dropColumnSQL).executeUpdate();
        quickDAOConfig.deleteDatabaseEntityCache(tableName);
        return deletedProperty;
    }

    @Override
    public boolean hasIndex(String tableName, String indexName) {
        String hasIndexSQL = "select name from sqlite_master where type = 'index' and name = '" + indexName + "';";
        return connectionExecutor.name("判断索引是否存在").sql(hasIndexSQL).executeAndCheckExists();
    }

    @Override
    public List<IndexField> getIndexField(String tableName) {
        List<IndexField> indexFieldList = new ArrayList<>();
        String getIndexSQL = "select tbl_name, sql from sqlite_master where type='index' and sql is not null and tbl_name = '" + tableName + "';";
        connectionExecutor.name("获取索引信息").sql(getIndexSQL).executeQuery(resultSet -> {
            while (resultSet.next()) {
                IndexField indexField = getIndexField(resultSet);
                if (null != indexField) {
                    indexFieldList.add(indexField);
                }
            }
        });
        return indexFieldList;
    }

    @Override
    public void enableForeignConstraintCheck(boolean enable) {
        String foreignConstraintCheckSQL = "PRAGMA foreign_keys = " + enable + ";";
        connectionExecutor.name(enable ? "启用外键约束检查" : "禁用外键约束检查").sql(foreignConstraintCheckSQL).executeUpdate();
    }

    @Override
    protected String getAutoIncrementSQL(Property property) {
        return property.column + " " + property.columnType + (null == property.length ? "" : "(" + property.length + ")") + " primary key autoincrement";
    }

    @Override
    protected void getIndex(List<Entity> entityList) {
        String getIndexSQL = "select tbl_name, sql from sqlite_master where type='index' and sql is not null;";
        connectionExecutor.name("获取索引信息").sql(getIndexSQL).executeQuery(resultSet -> {
            while (resultSet.next()) {
                for (Entity entity : entityList) {
                    if (!entity.tableName.equalsIgnoreCase(resultSet.getString("tbl_name"))) {
                        continue;
                    }
                    IndexField indexField = getIndexField(resultSet);
                    if (null != indexField) {
                        entity.indexFieldList.add(indexField);
                    }
                    break;
                }
            }
        });
    }

    /**
     * 获取列信息
     */
    private void getProperty(ResultSet resultSet, Property property) throws SQLException {
        property.column = resultSet.getString("name");
        property.columnType = resultSet.getString("type");
        if (property.columnType.contains("(") && property.columnType.contains(")")) {
            String lengthString = property.columnType.substring(property.columnType.indexOf("(") + 1, property.columnType.indexOf(")"));
            if (lengthString.matches("\\d+")) {
                property.length = Integer.parseInt(lengthString);
            }
            property.columnType = property.columnType.substring(0, property.columnType.indexOf("("));
        }
        property.notNull = "1".equals(resultSet.getString("notnull"));
        if (null != resultSet.getString("dflt_value")) {
            property.defaultValue = resultSet.getString("dflt_value");
        }
        if (1 == resultSet.getInt("pk")) {
            property.id = true;
            property.strategy = IdStrategy.AutoIncrement;
        }
    }

    /**
     * 获取索引信息
     */
    private IndexField getIndexField(ResultSet resultSet) throws SQLException {
        String sql = resultSet.getString("sql");
        if (!sql.contains("\"") && !sql.contains("`")) {
            logger.warn("SQLite获取索引,索引不包含\"也不包含`,跳过获取,原始索引字符串:{}", sql);
            return null;
        }

        String[] tokens = sql.split("[\"|`]");
        IndexField indexField = new IndexField();
        if (tokens[0].contains("UNIQUE")) {
            indexField.indexType = IndexType.UNIQUE;
        } else {
            indexField.indexType = IndexType.NORMAL;
        }
        indexField.indexName = tokens[1];
        indexField.tableName = tokens[3];
        for (int i = 5; i < tokens.length - 1; i++) {
            indexField.columns.add(tokens[i]);
        }
        return indexField;
    }

    /**
     * 获取创建表语句
     */
    private String getCreateTableStatement(Entity entity) {
        StringBuilder builder = new StringBuilder();
        if (quickDAOConfig.databaseOption.openForeignKey && null != entity.foreignKeyProperties && entity.foreignKeyProperties.size() > 0) {
            //手动开启外键约束
            builder.append("PRAGMA foreign_keys = ON;");
        }
        builder.append("create table " + quickDAOConfig.databaseProvider.escape(entity.tableName) + "(");
        for (Property property : entity.properties) {
            if (property.id && property.strategy == IdStrategy.AutoIncrement) {
                builder.append(getAutoIncrementSQL(property));
            } else {
                builder.append(quickDAOConfig.databaseProvider.escape(property.column) + " " + property.columnType + (null == property.length ? "" : "(" + property.length + ")"));
                if (property.notNull) {
                    builder.append(" not null");
                }
                if (null != property.defaultValue && !property.defaultValue.isEmpty()) {
                    builder.append(" default " + property.defaultValue);
                }
                if (null != property.comment) {
                    builder.append(" " + quickDAOConfig.databaseProvider.comment(property.comment));
                }
                if (null != property.escapeCheck && !property.escapeCheck.isEmpty()) {
                    builder.append(" check " + property.escapeCheck);
                }
            }
            builder.append(",");
        }
        if (quickDAOConfig.databaseOption.openForeignKey && null != entity.foreignKeyProperties && entity.foreignKeyProperties.size() > 0) {
            for (Property property : entity.foreignKeyProperties) {
                builder.append("foreign key(" + quickDAOConfig.databaseProvider.escape(property.column) + ") references ");
                String operation = property.foreignKey.foreignKeyOption().getOperation();
                builder.append(quickDAOConfig.databaseProvider.escape(quickDAOConfig.getEntityByClassName(property.foreignKey.table().getName()).tableName) + "(" + quickDAOConfig.databaseProvider.escape(property.foreignKey.field()) + ") ON DELETE " + operation + " ON UPDATE " + operation);
                builder.append(",");
            }
        }
        builder.deleteCharAt(builder.length() - 1);
        builder.append(")");
        if (null != entity.comment) {
            builder.append(" " + quickDAOConfig.databaseProvider.comment(entity.comment));
        }
        builder.append(";");
        //创建索引
        for (IndexField indexField : entity.indexFieldList) {
            builder.append(getCreateIndexStatement(indexField));
        }
        return builder.toString();
    }
}
