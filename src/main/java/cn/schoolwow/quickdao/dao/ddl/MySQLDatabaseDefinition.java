package cn.schoolwow.quickdao.dao.ddl;

import cn.schoolwow.quickdao.annotation.IdStrategy;
import cn.schoolwow.quickdao.annotation.IndexType;
import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.IndexField;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MySQLDatabaseDefinition extends AbstractDatabaseDefinition {

    public MySQLDatabaseDefinition(QuickDAOConfig quickDAOConfig) {
        super(quickDAOConfig);
    }

    @Override
    public boolean hasTable(String tableName) {
        String hasTableSQL = "show tables like ?;";
        return connectionExecutor.name("判断表是否存在").sql(hasTableSQL).parameters(Arrays.asList(tableName)).executeAndCheckExists();
    }

    @Override
    public boolean hasColumn(String tableName, String columnName) {
        String hasTableColumnSQL = "select table_name, column_name, data_type, character_maximum_length, numeric_precision, is_nullable, column_key, extra, column_default, column_comment from information_schema.`columns` where table_name = ? and column_name = ?;";
        List parameters = Arrays.asList(tableName,columnName);
        return connectionExecutor.name("判断表指定列是否存在").sql(hasTableColumnSQL).parameters(parameters).executeAndCheckExists();
    }

    @Override
    public List<String> getTableNameList(){
        String getEntityListSQL = "show table status;";
        List<String> tableNames = new ArrayList<>();
        connectionExecutor.name("获取表名列表").sql(getEntityListSQL).executeQuery((resultSet) -> {
            while (resultSet.next()) {
                tableNames.add(resultSet.getString("name"));
            }
        });
        return tableNames;
    }

    @Override
    public List<Entity> getDatabaseEntityList() {
        String getEntityListSQL = "show table status;";
        List<Entity> entityList = new ArrayList<>();
        //获取列表
        connectionExecutor.name("获取表列表").sql(getEntityListSQL).executeQuery((resultSet) -> {
            while (resultSet.next()) {
                Entity entity = new Entity();
                entity.tableName = resultSet.getString("name");
                entity.comment = resultSet.getString("comment").replace("\"", "\\\"");
                entityList.add(entity);
            }
        });

        //获取字段信息
        String getEntityPropertyListSQL = "select table_name, column_name, data_type, character_maximum_length, numeric_precision, is_nullable, column_key, extra, column_default, column_comment from information_schema.`columns` where table_schema = '" + getDatabaseName() + "';";
        connectionExecutor.name("获取表字段信息").sql(getEntityPropertyListSQL).executeQuery((resultSet) -> {
            while (resultSet.next()) {
                for (Entity entity : entityList) {
                    if (!entity.tableName.equalsIgnoreCase(resultSet.getString("table_name"))) {
                        continue;
                    }
                    //添加字段信息
                    Property property = new Property();
                    getProperty(resultSet, property);
                    entity.properties.add(property);
                    break;
                }
            }
        });
        getIndex(entityList);
        return entityList;
    }

    @Override
    public Entity getDatabaseEntity(String tableName) {
        String getEntityListSQL = "show table status;";
        Entity entity = new Entity();
        connectionExecutor.name("获取表列表").sql(getEntityListSQL).executeQuery((resultSet) -> {
            while (resultSet.next()) {
                if (!resultSet.getString("name").equalsIgnoreCase(tableName)) {
                    continue;
                }
                entity.tableName = resultSet.getString("name");
                entity.comment = resultSet.getString("comment").replace("\"", "\\\"");
                entity.properties = getPropertyList(tableName);
                entity.indexFieldList = getIndexField(tableName);
                break;
            }
        });
        return null == entity.tableName ? null : entity;
    }

    @Override
    public List<Property> getPropertyList(Class clazz) {
        Entity entity = quickDAOConfig.entityMap.get(clazz.getName());
        if (null == entity) {
            throw new IllegalArgumentException("实体类不存在!实体类:" + clazz.getName());
        }
        return getPropertyList(entity.tableName);
    }

    @Override
    public List<Property> getPropertyList(String tableName) {
        String getEntityPropertyListSQL = "select table_name, column_name, data_type, character_maximum_length, numeric_precision, is_nullable, column_key, extra, column_default, column_comment from information_schema.`columns` where table_schema = '" + getDatabaseName() + "'  and table_name = ?;";
        List<Property> propertyList = new ArrayList<>();
        connectionExecutor.name("获取表字段列表信息").sql(getEntityPropertyListSQL).parameters(Arrays.asList(tableName)).executeQuery((resultSet) -> {
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
        String getEntityPropertyListSQL = "select table_name, column_name, data_type, character_maximum_length, numeric_precision, is_nullable, column_key, extra, column_default, column_comment from information_schema.`columns` where table_schema = '" + getDatabaseName() + "'  and table_name = ? and column_name = ?;";
        List parameters = Arrays.asList(tableName, columnName);
        Property property = new Property();
        connectionExecutor.name("获取表字段信息").sql(getEntityPropertyListSQL).parameters(parameters).executeQuery((resultSet) -> {
            if (resultSet.next()) {
                getProperty(resultSet, property);
            }
        });
        return null == property.column ? null : property;
    }

    @Override
    public void create(Entity entity) {
        StringBuilder builder = new StringBuilder("create table " + quickDAOConfig.databaseProvider.escape(entity.tableName) + "(");
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
        for (IndexField indexField : entity.indexFieldList) {
            if (null == indexField.columns || indexField.columns.isEmpty()) {
                logger.warn("忽略索引,该索引字段信息为空!表:{},索引名称:{}", entity.tableName, indexField.indexName);
                continue;
            }
            switch (indexField.indexType) {
                case UNIQUE: {
                    builder.append("unique");
                }
                case NORMAL: {
                    builder.append(" index " + quickDAOConfig.databaseProvider.escape(indexField.indexName) + " (");
                    for (String column : indexField.columns) {
                        builder.append(quickDAOConfig.databaseProvider.escape(column) + ",");
                    }
                    builder.deleteCharAt(builder.length() - 1);
                    builder.append(")");
                    if (null != indexField.using && !indexField.using.isEmpty()) {
                        builder.append(" using " + indexField.using);
                    }
                    if (null != indexField.comment && !indexField.comment.isEmpty()) {
                        builder.append(" " + quickDAOConfig.databaseProvider.comment(indexField.comment));
                    }
                    builder.append(",");
                }
                break;
                case FULLTEXT: {
                    builder.append("fulltext(" + indexField.columns.get(0) + "),");
                }
                break;
            }
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
        if (!entity.properties.isEmpty()) {
            builder.append(")");
        }
        if (null != entity.comment) {
            builder.append(" " + quickDAOConfig.databaseProvider.comment(entity.comment));
        }
        builder.append(";");
        String createTableSQL = builder.toString();
        connectionExecutor.name("生成新表").sql(createTableSQL).executeUpdate();
    }

    @Override
    public boolean hasIndex(String tableName, String indexName) {
        String hasIndexSQL = "show index from " + quickDAOConfig.databaseProvider.escape(tableName) + " where key_name = ?;";
        return connectionExecutor.name("判断索引是否存在").sql(hasIndexSQL).parameters(Arrays.asList(indexName)).executeAndCheckExists();
    }

    @Override
    public List<IndexField> getIndexField(String tableName) {
        String getIndexSQL = "select table_name, index_name, non_unique, column_name, index_type, index_comment from information_schema.`statistics` where table_schema = '" + getDatabaseName() + "' and table_name = ?;";
        List<IndexField> indexFieldList = new ArrayList<>();
        connectionExecutor.name("获取索引信息").sql(getIndexSQL).parameters(Arrays.asList(tableName)).executeQuery((resultSet) -> {
            while (resultSet.next()) {
                String indexName = resultSet.getString("index_name");
                IndexField indexField = null;
                for (IndexField indexField1 : indexFieldList) {
                    if (indexField1.indexName.equals(indexName)) {
                        indexField = indexField1;
                        break;
                    }
                }
                if (null == indexField) {
                    indexField = new IndexField();
                    indexField.indexType = resultSet.getInt("non_unique") == 0 ? IndexType.UNIQUE : IndexType.NORMAL;
                    if ("FULLTEXT".equals(resultSet.getString("index_type"))) {
                        indexField.indexType = IndexType.FULLTEXT;
                    }
                    indexField.indexName = resultSet.getString("index_name");
                    indexField.columns.add(resultSet.getString("column_name"));
                    indexField.using = resultSet.getString("index_type");
                    indexField.comment = resultSet.getString("index_comment");
                    indexFieldList.add(indexField);
                } else {
                    indexField.columns.add(resultSet.getString("column_name"));
                }
            }
        });
        return indexFieldList;
    }

    @Override
    public void dropIndex(String tableName, String indexName) {
        String dropIndexSQL = "drop index " + quickDAOConfig.databaseProvider.escape(indexName) + " on " + quickDAOConfig.databaseProvider.escape(tableName) + ";";
        connectionExecutor.name("删除索引").sql(dropIndexSQL).executeUpdate();
    }

    @Override
    public void enableForeignConstraintCheck(boolean enable) {
        String foreignConstraintCheckSQL = "set foreign_key_checks = " + (enable ? 1 : 0) + ";";
        connectionExecutor.name(enable ? "启用外键约束检查" : "禁用外键约束检查").sql(foreignConstraintCheckSQL).executeUpdate();
    }

    @Override
    protected String getAutoIncrementSQL(Property property) {
        return property.column + " " + property.columnType + (null == property.length ? "" : "(" + property.length + ")") + " primary key auto_increment";
    }

    @Override
    protected void getIndex(List<Entity> entityList) {
        String getIndexSQL = "select table_name, index_name, non_unique, column_name, index_type, index_comment from information_schema.`statistics` where table_schema = '" + getDatabaseName() + "';";
        connectionExecutor.name("获取索引信息").sql(getIndexSQL).executeQuery((resultSet) -> {
            while (resultSet.next()) {
                for (Entity entity : entityList) {
                    if (!entity.tableName.equalsIgnoreCase(resultSet.getString("table_name"))) {
                        continue;
                    }
                    String indexName = resultSet.getString("index_name");
                    IndexField indexField = null;
                    for (IndexField indexField1 : entity.indexFieldList) {
                        if (indexField1.indexName.equals(indexName)) {
                            indexField = indexField1;
                            break;
                        }
                    }
                    if (null == indexField) {
                        indexField = new IndexField();
                        indexField.indexType = resultSet.getInt("non_unique") == 0 ? IndexType.UNIQUE : IndexType.NORMAL;
                        if ("FULLTEXT".equals(resultSet.getString("index_type"))) {
                            indexField.indexType = IndexType.FULLTEXT;
                        }
                        indexField.indexName = resultSet.getString("index_name");
                        if ("PRIMARY".equalsIgnoreCase(indexField.indexName)) {
                            for (Property property : entity.properties) {
                                if (property.column.equals(resultSet.getString("column_name"))) {
                                    property.id = true;
                                }
                            }
                        } else {
                            indexField.columns.add(resultSet.getString("column_name"));
                            indexField.using = resultSet.getString("index_type");
                            indexField.comment = resultSet.getString("index_comment");
                            entity.indexFieldList.add(indexField);
                        }
                    } else {
                        indexField.columns.add(resultSet.getString("column_name"));
                    }
                    break;
                }
            }
        });
    }

    /**
     * 获取属性信息
     */
    private void getProperty(ResultSet resultSet, Property property) throws SQLException {
        property.column = resultSet.getString("column_name");
        //无符号填充0 => float unsigned zerofill
        property.columnType = resultSet.getString("data_type");
        if (property.columnType.contains(" ")) {
            property.columnType = property.columnType.substring(0, property.columnType.indexOf(" ")).trim();
        }
        Object characterMaximumLength = resultSet.getObject("character_maximum_length");
        if (null != characterMaximumLength && characterMaximumLength.toString().length() < 7) {
            property.length = Integer.parseInt(characterMaximumLength.toString());
        }
        Object numericPrecision = resultSet.getObject("numeric_precision");
        if (null != numericPrecision) {
            property.length = Integer.parseInt(numericPrecision.toString());
        }
        property.notNull = "NO".equals(resultSet.getString("is_nullable"));
        String key = resultSet.getString("column_key");
        if ("PRI".equals(key)) {
            property.id = true;
        }
        if ("auto_increment".equals(resultSet.getString("extra"))) {
            property.id = true;
            property.strategy = IdStrategy.AutoIncrement;
        } else {
            property.strategy = IdStrategy.None;
        }
        if (null != resultSet.getString("column_default")) {
            property.defaultValue = resultSet.getString("column_default");
            if (!property.defaultValue.contains("CURRENT_TIMESTAMP") && !property.defaultValue.contains("'")) {
                property.defaultValue = "'" + property.defaultValue + "'";
            }
        }
        property.comment = resultSet.getString("column_comment").replace("\"", "\\\"");
    }

    /**
     * 获取数据名称
     */
    private String getDatabaseName() {
        String[] databaseNames = new String[1];
        connectionExecutor.name("获取数据库名称").sql("select database();").executeQuery((resultSet) -> {
            if (resultSet.next()) {
                databaseNames[0] = resultSet.getString(1);
            }
        });
        if (null == databaseNames[0]) {
            throw new RuntimeException("数据库名称获取失败!");
        }
        return databaseNames[0];
    }
}
