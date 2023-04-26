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

public class PostgreDatabaseDefinition extends AbstractDatabaseDefinition {
    public PostgreDatabaseDefinition(QuickDAOConfig quickDAOConfig) {
        super(quickDAOConfig);
    }

    @Override
    public boolean hasTable(String tableName) {
        String hasTableSQL = "select tablename from pg_tables where schemaname = 'public' and tablename = ?;";
        return connectionExecutor.name("判断表是否存在").sql(hasTableSQL).parameters(Arrays.asList(tableName)).executeAndCheckExists();
    }

    @Override
    public boolean hasColumn(String tableName, String columnName) {
        String hasTableColumnSQL = "select pg_class.relname as table_name, attname as column_name from pg_attribute join pg_class on pg_attribute.attrelid = pg_class.oid where attnum > 0 and atttypid > 0 and pg_class.relname = ? and attname = ?;";
        List parameters = Arrays.asList(tableName, columnName);
        return connectionExecutor.name("判断表指定列是否存在").sql(hasTableColumnSQL).parameters(parameters).executeAndCheckExists();
    }

    @Override
    public List<String> getTableNameList(){
        String getEntityListSQL = "select relname as name from pg_class c where relkind = 'r' and relname not like 'pg_%' and relname not like 'sql_%' order by relname;";
        List<String> tableNames = new ArrayList<>();
        connectionExecutor.name("获取表列表").sql(getEntityListSQL).executeQuery(resultSet -> {
            while (resultSet.next()) {
                tableNames.add(resultSet.getString("name"));
            }
        });
        return tableNames;
    }

    @Override
    public List<Entity> getDatabaseEntityList() {
        String getEntityListSQL = "select relname as name,cast(obj_description(relfilenode,'pg_class') as varchar) as comment from pg_class c where  relkind = 'r' and relname not like 'pg_%' and relname not like 'sql_%' order by relname;";
        List<Entity> entityList = new ArrayList<>();
        connectionExecutor.name("获取表名列表").sql(getEntityListSQL).executeQuery(resultSet -> {
            while (resultSet.next()) {
                Entity entity = new Entity();
                entity.tableName = resultSet.getString("name");
                entity.comment = resultSet.getString("comment");
                entity.properties = getPropertyList(entity.tableName);
                entityList.add(entity);
            }
        });
        getIndex(entityList);
        return entityList;
    }

    @Override
    public Entity getDatabaseEntity(String tableName) {
        Entity entity = new Entity();
        String getEntitySQL = "select relname as name,cast(obj_description(relfilenode,'pg_class') as varchar) as comment from pg_class c where relkind = 'r' and relname = ? order by relname;";
        connectionExecutor.name("获取表列表").sql(getEntitySQL).parameters(Arrays.asList(tableName)).executeQuery(resultSet -> {
            if (resultSet.next()) {
                entity.tableName = resultSet.getString("name");
                entity.comment = resultSet.getString("comment");
                entity.properties = getPropertyList(entity.tableName);
                entity.indexFieldList = getIndexField(tableName);
            }
        });
        return null == entity.tableName ? null : entity;
    }

    @Override
    public List<Property> getPropertyList(String tableName) {
        List<Property> propertyList = new ArrayList<>();
        //获取表字段信息
        String getEntityPropertyListSQL = "select pg_class.relname as table_name, attname as column_name, attnum as oridinal_position, attnotnull as notnull, format_type(atttypid,atttypmod) as type, col_description(attrelid, attnum) as comment from pg_attribute join pg_class on pg_attribute.attrelid = pg_class.oid where attnum > 0 and atttypid > 0 and pg_class.relname = ?;";
        List parameters = Arrays.asList(tableName);
        connectionExecutor.name("获取表字段信息").sql(getEntityPropertyListSQL).parameters(parameters).executeQuery(resultSet -> {
            while (resultSet.next()) {
                Property property = new Property();
                property.column = resultSet.getString("column_name");
                property.columnType = resultSet.getString("type");
                property.notNull = "t".equals(resultSet.getString("notnull"));
                property.comment = resultSet.getString("comment");
                propertyList.add(property);
            }
        });
        //提取默认值和主键信息
        String getEntityPropertyTypeListSQL = "select table_name,ordinal_position,column_name,column_default,is_nullable,udt_name,character_maximum_length,column_default from information_schema.columns where table_name = ?;";
        connectionExecutor.name("获取表字段类型信息").sql(getEntityPropertyTypeListSQL).parameters(parameters).executeQuery(resultSet -> {
            while (resultSet.next()) {
                for (Property property : propertyList) {
                    if (property.column.equalsIgnoreCase(resultSet.getString("column_name"))) {
                        getProperty(resultSet, property);
                        break;
                    }
                }
            }
        });
        return propertyList;
    }

    @Override
    public Property getProperty(String tableName, String columnName) {
        Property property = new Property();
        //获取表字段信息
        String getEntityPropertyListSQL = "select pg_class.relname as table_name, attname as column_name, attnum as oridinal_position, attnotnull as notnull, format_type(atttypid,atttypmod) as type, col_description(attrelid, attnum) as comment from pg_attribute join pg_class on pg_attribute.attrelid = pg_class.oid where attnum > 0 and atttypid > 0 and pg_class.relname = ? and attname = ?;";
        List parameters = Arrays.asList(tableName, columnName);
        connectionExecutor.name("获取表字段信息").sql(getEntityPropertyListSQL).parameters(parameters).executeQuery(resultSet -> {
            if (resultSet.next()) {
                property.column = resultSet.getString("column_name");
                property.columnType = resultSet.getString("type");
                property.notNull = "t".equals(resultSet.getString("notnull"));
                property.comment = resultSet.getString("comment");
            }
        });
        //提取默认值和主键信息
        String getEntityPropertyTypeListSQL = "select table_name, ordinal_position,column_name,column_default,is_nullable,udt_name,character_maximum_length,column_default from information_schema.columns where table_name = ? and column_name = ?;";
        connectionExecutor.name("获取表字段类型信息").sql(getEntityPropertyTypeListSQL).parameters(parameters).executeQuery(resultSet -> {
            if (resultSet.next()) {
                getProperty(resultSet, property);
            }
        });
        return property;
    }

    @Override
    public void create(Entity entity) {
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
                if (null != property.defaultValue&&!property.defaultValue.isEmpty()) {
                    builder.append(" default '" + property.defaultValue + "'");
                }
                if (property.notNull) {
                    builder.append(" not null");
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
        //创建注释
        if (null != entity.comment) {
            builder.append("comment on table \"" + entity.tableName + "\" is '" + entity.comment + "';");
        }
        for (Property property : entity.properties) {
            if (property.comment == null) {
                continue;
            }
            builder.append("comment on column \"" + entity.tableName + "\".\"" + property.column + "\" is '" + property.comment + "';");
        }
        String createTableSQL = builder.toString();
        connectionExecutor.name("生成新表").sql(createTableSQL).executeUpdate();
    }

    @Override
    public void alterColumn(Property property) {
        String commonSQL = "alter table " + quickDAOConfig.databaseProvider.escape(property.entity.tableName) + " alter column " + quickDAOConfig.databaseProvider.escape(property.column);
        StringBuilder alterColumnBuilder = new StringBuilder();
        //修改类型
        alterColumnBuilder.append(commonSQL + " type " + property.columnType + (null == property.length ? "" : "(" + property.length + ")") + ";");
        //设置非空
        if (property.notNull) {
            alterColumnBuilder.append(commonSQL + " set not null;");
        }
        //设置默认值
        if (null != property.defaultValue && !property.defaultValue.isEmpty()) {
            alterColumnBuilder.append(commonSQL + " set default " + property.defaultValue + ";");
        }
        String alterColumnSQL = alterColumnBuilder.toString();
        connectionExecutor.name("修改列").sql(alterColumnSQL).executeUpdate();
        quickDAOConfig.deleteDatabaseEntityCache(property.entity.tableName);
    }

    @Override
    public boolean hasIndex(String tableName, String indexName) {
        String hasIndexSQL = "select indexname from pg_indexes where tablename = ? and indexname = ?;";
        List parameters = Arrays.asList(tableName, indexName);
        return connectionExecutor.name("判断索引是否存在").sql(hasIndexSQL).parameters(parameters).executeAndCheckExists();
    }

    @Override
    public List<IndexField> getIndexField(String tableName) {
        String getIndexSQL = "select tablename,indexname,indexdef from pg_indexes where tablename = ?;";
        List<IndexField> indexFieldList = new ArrayList<>();
        connectionExecutor.name("获取索引信息").sql(getIndexSQL).parameters(Arrays.asList(tableName)).executeQuery(resultSet -> {
            while (resultSet.next()) {
                IndexField indexField = getIndexField(resultSet);
                indexFieldList.add(indexField);
            }
        });
        return indexFieldList;
    }

    @Override
    public void createIndex(IndexField indexField) {
        String createIndexSQL = getCreateIndexStatement(indexField);
        connectionExecutor.name("创建索引").sql(createIndexSQL).executeUpdate();
    }

    @Override
    protected String getAutoIncrementSQL(Property property) {
        return quickDAOConfig.databaseProvider.escape(property.column) + " SERIAL UNIQUE PRIMARY KEY";
    }

    @Override
    protected void getIndex(List<Entity> entityList) {
        String getIndexSQL = "select tablename,indexname,indexdef from pg_indexes;";
        connectionExecutor.name("获取索引信息").sql(getIndexSQL).executeQuery(resultSet -> {
            while (resultSet.next()) {
                for (Entity entity : entityList) {
                    if (!entity.tableName.equalsIgnoreCase(resultSet.getString("tablename"))) {
                        continue;
                    }
                    IndexField indexField = getIndexField(resultSet);
                    entity.indexFieldList.add(indexField);
                    break;
                }
            }
        });
    }

    @Override
    protected String getCreateIndexStatement(IndexField indexField) {
        StringBuilder createIndexBuilder = new StringBuilder("create");
        switch (indexField.indexType) {
            case NORMAL: {
            }
            break;
            case UNIQUE: {
                createIndexBuilder.append(" unique");
            }
            break;
            case FULLTEXT: {
                createIndexBuilder.append(" fulltext");
            }
            break;
        }
        createIndexBuilder.append(" index " + quickDAOConfig.databaseProvider.escape(indexField.indexName) + " on " + quickDAOConfig.databaseProvider.escape(indexField.tableName));
        if (null != indexField.using && !indexField.using.isEmpty()) {
            createIndexBuilder.append(" using " + indexField.using);
        }
        createIndexBuilder.append("(");
        for (String column : indexField.columns) {
            createIndexBuilder.append(quickDAOConfig.databaseProvider.escape(column) + ",");
        }
        createIndexBuilder.deleteCharAt(createIndexBuilder.length() - 1);
        createIndexBuilder.append(")");
        if (null != indexField.comment && !indexField.comment.isEmpty()) {
            createIndexBuilder.append(" " + quickDAOConfig.databaseProvider.comment(indexField.comment));
        }
        createIndexBuilder.append(";");
        return createIndexBuilder.toString();
    }

    /**
     * 获取属性信息
     */
    private void getProperty(ResultSet resultSet, Property property) throws SQLException {
        property.columnType = resultSet.getString("udt_name");
        Object characterMaximumLength = resultSet.getObject("character_maximum_length");
        if (null != characterMaximumLength && characterMaximumLength.toString().length() < 7) {
            property.length = Integer.parseInt(characterMaximumLength.toString());
        }
        if (null != resultSet.getString("column_default")) {
            property.defaultValue = resultSet.getString("column_default");
            if(property.defaultValue.startsWith("nextval(")){
                property.id = true;
                property.strategy = IdStrategy.AutoIncrement;
            }
        }
    }

    /**
     * 获取索引信息
     */
    private IndexField getIndexField(ResultSet resultSet) throws SQLException {
        IndexField indexField = new IndexField();
        indexField.tableName = resultSet.getString("tablename");
        indexField.indexName = resultSet.getString("indexname");

        String def = resultSet.getString("indexdef");
        if (def.contains("UNIQUE INDEX")) {
            indexField.indexType = IndexType.UNIQUE;
        } else {
            indexField.indexType = IndexType.NORMAL;
        }
        indexField.using = def.substring(def.indexOf("USING") + "USING".length(), def.indexOf("(")).replace("\"", "");
        String[] columns = def.substring(def.indexOf("(") + 1, def.indexOf(")")).split(",");
        for (int i = 0; i < columns.length; i++) {
            indexField.columns.add(columns[i]);
        }
        return indexField;
    }
}
