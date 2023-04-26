package cn.schoolwow.quickdao.dao.ddl;

import cn.schoolwow.quickdao.annotation.IdStrategy;
import cn.schoolwow.quickdao.annotation.IndexType;
import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.IndexField;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SQLServerDatabaseDefinition extends AbstractDatabaseDefinition {

    public SQLServerDatabaseDefinition(QuickDAOConfig quickDAOConfig) {
        super(quickDAOConfig);
    }

    @Override
    public boolean hasTable(String tableName) {
        String hasTableSQL = "select name from sysobjects where xtype='u' and name = ?;";
        return connectionExecutor.name("判断表是否存在").sql(hasTableSQL).parameters(Arrays.asList(tableName)).executeAndCheckExists();
    }

    @Override
    public boolean hasColumn(String tableName, String columnName) {
        String hasTableColumnSQL = "select table_name, column_name from information_schema.columns where table_name = ? and column_name = ?;";
        List parameters = Arrays.asList(tableName, columnName);
        return connectionExecutor.name("判断表指定列是否存在").sql(hasTableColumnSQL).parameters(parameters).executeAndCheckExists();
    }

    @Override
    public List<String> getTableNameList(){
        String getEntityListSQL = "select name from sysobjects where xtype='u' order by name;";
        List<String> tableNames = new ArrayList<>();
        connectionExecutor.name("获取表名列表").sql(getEntityListSQL).executeQuery(resultSet -> {
            while (resultSet.next()) {
                tableNames.add(resultSet.getString("name"));
            }
        });
        return tableNames;
    }

    @Override
    public List<Entity> getDatabaseEntityList() {
        String getEntityListSQL = "select name from sysobjects where xtype='u' order by name;";
        List<Entity> entityList = new ArrayList<>();
        connectionExecutor.name("获取表列表").sql(getEntityListSQL).executeQuery(resultSet -> {
            while (resultSet.next()) {
                Entity entity = new Entity();
                entity.tableName = resultSet.getString("name");
                entity.properties = getPropertyList(entity.tableName);
                entityList.add(entity);
            }
        });
        //获取表注释
        String getEntityCommentSQL = "select so.name table_name, isnull(convert(varchar(255),value),'') comment from sys.extended_properties ex_p left join sys.sysobjects so on ex_p.major_id = so.id where ex_p.minor_id=0;";
        connectionExecutor.name("获取表注释").sql(getEntityCommentSQL).executeQuery(resultSet -> {
            while (resultSet.next()) {
                for (Entity entity : entityList) {
                    if (!entity.tableName.equalsIgnoreCase(resultSet.getString("table_name"))) {
                        continue;
                    }
                    entity.comment = resultSet.getString("comment");
                    break;
                }
            }
        });
        getIndex(entityList);
        return entityList;
    }

    @Override
    public Entity getDatabaseEntity(String tableName) {
        Entity entity = new Entity();
        //获取所有表
        String getEntityListSQL = "select name from sysobjects where xtype='u' and name = ? order by name;";
        List parameters = Arrays.asList(tableName);
        connectionExecutor.name("获取表列表").sql(getEntityListSQL).parameters(parameters).executeQuery(resultSet -> {
            if (resultSet.next()) {
                entity.tableName = resultSet.getString("name");
                entity.properties = getPropertyList(entity.tableName);
            }
        });
        if (null != entity.tableName) {
            //获取表注释
            String getEntityCommentSQL = "select so.name table_name, isnull(convert(varchar(255),value),'') comment from sys.extended_properties ex_p left join sys.sysobjects so on ex_p.major_id = so.id where ex_p.minor_id=0 and so.name = ?;";
            connectionExecutor.name("获取表注释").sql(getEntityCommentSQL).parameters(parameters).executeQuery(resultSet -> {
                if (resultSet.next()) {
                    entity.comment = resultSet.getString("comment");
                }
            });
        }
        if (null != entity.tableName) {
            entity.indexFieldList = getIndexField(tableName);
        }
        if(null == entity.tableName){
            return null;
        }
        return entity;
    }

    @Override
    public List<Property> getPropertyList(String tableName) {
        String getEntityPropertyTypeListSQL = "select table_name,ordinal_position,column_name,data_type,is_nullable,character_maximum_length from information_schema.columns where table_name = ?;";
        List parameters = Arrays.asList(tableName);
        List<Property> properties = new ArrayList<>();
        connectionExecutor.name("获取表字段类型信息").sql(getEntityPropertyTypeListSQL).parameters(parameters).executeQuery(resultSet -> {
            while (resultSet.next()) {
                Property property = new Property();
                property.column = resultSet.getString("column_name");
                property.columnType = resultSet.getString("data_type");
                try {
                    String characterMaximumLength = resultSet.getString("character_maximum_length");
                    property.length = Integer.parseInt(characterMaximumLength);
                }catch (Exception e){
                }
                property.notNull = "NO".equals(resultSet.getString("is_nullable"));
                properties.add(property);
            }
        });
        String getPropertyCommentList = "select b.name table_name, c.name, convert(varchar(255),a.value) value from sys.extended_properties a, sysobjects b, sys.columns c where a.major_id = b.id and c.object_id = b.id and c.column_id = a.minor_id and b.name = ?;";
        connectionExecutor.name("获取字段注释").sql(getPropertyCommentList).parameters(parameters).executeQuery(resultSet -> {
            while (resultSet.next()) {
                for (Property property : properties) {
                    if (property.column.equalsIgnoreCase(resultSet.getString("name"))) {
                        property.comment = resultSet.getString("value");
                        break;
                    }
                }
            }
        });
        //判断主键
        String getPrimaryKeyColumn = "select column_name from information_schema.key_column_usage where table_name = ?;";
        connectionExecutor.name("获取表主键").sql(getPrimaryKeyColumn).parameters(parameters).executeQuery(resultSet -> {
            if (resultSet.next()) {
                for (Property property : properties) {
                    if (property.column.equalsIgnoreCase(resultSet.getString("column_name"))) {
                        property.id = true;
                        property.strategy = IdStrategy.AutoIncrement;
                        break;
                    }
                }
            }
        });
        return properties;
    }

    @Override
    public Property getProperty(String tableName, String columnName) {
        Property property = new Property();
        //获取字段信息
        String getEntityPropertyTypeListSQL = "select table_name,ordinal_position,column_name,data_type,is_nullable,character_maximum_length from information_schema.columns where table_name = ? and column_name = ?;";
        List parameters = Arrays.asList(tableName, columnName);
        connectionExecutor.name("获取表字段类型信息").sql(getEntityPropertyTypeListSQL).parameters(parameters).executeQuery(resultSet -> {
            if (resultSet.next()) {
                property.column = resultSet.getString("column_name");
                property.columnType = resultSet.getString("data_type");
                try {
                    String characterMaximumLength = resultSet.getString("character_maximum_length");
                    property.length = Integer.parseInt(characterMaximumLength);
                }catch (Exception e){
                }
                property.notNull = "NO".equals(resultSet.getString("is_nullable"));
            }
        });
        //获取字段注释
        String getPropertyCommentList = "select b.name table_name, c.name, convert(varchar(255),a.value) value from sys.extended_properties a, sysobjects b, sys.columns c where a.major_id = b.id and c.object_id = b.id and c.column_id = a.minor_id and b.name = ? and c.name = ?;";
        connectionExecutor.name("获取字段注释").sql(getPropertyCommentList).parameters(parameters).executeQuery(resultSet -> {
            if (resultSet.next()) {
                property.comment = resultSet.getString("value");
            }
        });
        //判断主键
        String getPrimaryKeyColumn = "select column_name from information_schema.key_column_usage where table_name = ?;";
        connectionExecutor.name("获取表主键").sql(getPrimaryKeyColumn).parameters(Arrays.asList(tableName)).executeQuery(resultSet -> {
            if (resultSet.next()) {
                if (property.column.equalsIgnoreCase(resultSet.getString("column_name"))) {
                    property.id = true;
                    property.strategy = IdStrategy.AutoIncrement;
                }
            }
        });
        return null == property.column ? null : property;
    }

    @Override
    public void create(Entity entity) {
        StringBuilder createTableBuilder = new StringBuilder("create table " + quickDAOConfig.databaseProvider.escape(entity.tableName) + "(");
        for (Property property : entity.properties) {
            if (property.id && property.strategy == IdStrategy.AutoIncrement) {
                createTableBuilder.append(getAutoIncrementSQL(property));
            } else {
                createTableBuilder.append(quickDAOConfig.databaseProvider.escape(property.column) + " " + property.columnType + (null == property.length ? "" : "(" + property.length + ")"));
                if (property.notNull) {
                    createTableBuilder.append(" not null");
                }
                if (null != property.defaultValue) {
                    createTableBuilder.append(" default '" + property.defaultValue + "'");
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
        createTableBuilder.append(");");
        //添加注释
        if (null != entity.comment) {
            createTableBuilder.append("EXEC sp_addextendedproperty 'MS_Description',N'" + entity.comment + "','SCHEMA','dbo','table',N'" + entity.tableName + "';");
        }
        for (Property property : entity.properties) {
            if (null != property.comment) {
                createTableBuilder.append("EXEC sp_addextendedproperty 'MS_Description',N'" + property.comment + "','SCHEMA','dbo','table',N'" + entity.tableName + "','column',N'" + property.column + "';");
            }
        }
        //创建索引
        for (IndexField indexField : entity.indexFieldList) {
            createTableBuilder.append(getCreateIndexStatement(indexField));
        }
        createTableBuilder.append(";");
        String createTableSQL = createTableBuilder.toString();
        connectionExecutor.name("生成新表").sql(createTableSQL).executeUpdate();
    }

    @Override
    public boolean hasIndex(String tableName, String indexName) {
        String hasIndexSQL = "select name from sys.indexes WHERE object_id=OBJECT_ID(?, N'U') and name = ?;";
        List parameters = Arrays.asList(tableName, indexName);
        return connectionExecutor.name("判断索引是否存在").sql(hasIndexSQL).parameters(parameters).executeAndCheckExists();
    }

    @Override
    public List<IndexField> getIndexField(String tableName) {
        String getIndexSQL = "select i.is_unique,i.name,col.name col_name from sys.indexes i left join sys.index_columns ic on ic.object_id = i.object_id and ic.index_id = i.index_id left join (select * from sys.all_columns where object_id = object_id(?, N'U' )) col on ic.column_id = col.column_id where i.object_id = object_id(?, N'U' ) and i.index_id > 0;";
        List parameters = Arrays.asList(tableName, tableName);
        List<IndexField> indexFieldList = new ArrayList<>();
        connectionExecutor.name("获取索引信息").sql(getIndexSQL).parameters(parameters).executeQuery(resultSet -> {
            while (resultSet.next()) {
                IndexField indexField = new IndexField();
                if (resultSet.getBoolean("is_unique")) {
                    indexField.indexType = IndexType.UNIQUE;
                } else {
                    indexField.indexType = IndexType.NORMAL;
                }
                indexField.indexName = resultSet.getString("name");
                //判断是否已经存在该索引
                IndexField existIndexField = indexFieldList.stream().filter(indexField1 -> indexField1.indexName.equals(indexField.indexName)).findFirst().orElse(null);
                if (null != existIndexField) {
                    existIndexField.columns.add(resultSet.getNString("col_name"));
                } else {
                    indexField.columns.add(resultSet.getNString("col_name"));
                    indexFieldList.add(indexField);
                }
            }
        });
        return indexFieldList;
    }

    @Override
    public void dropIndex(String tableName, String indexName) {
        String dropIndexSQL = "drop index " + quickDAOConfig.databaseProvider.escape(tableName) + "." + quickDAOConfig.databaseProvider.escape(indexName) + ";";
        connectionExecutor.name("删除索引").sql(dropIndexSQL).executeUpdate();
    }

    @Override
    protected String getAutoIncrementSQL(Property property) {
        return property.column + " " + property.columnType + (null == property.length ? "" : "(" + property.length + ")") + " identity(1,1) unique ";
    }

    @Override
    protected void getIndex(List<Entity> entityList) {
        for (Entity entity : entityList) {
            String getIndexSQL = "select i.is_unique,i.name,col.name col_name from sys.indexes i left join sys.index_columns ic on ic.object_id = i.object_id and ic.index_id = i.index_id left join (select * from sys.all_columns where object_id = object_id( '" + entity.tableName + "', N'U' )) col on ic.column_id = col.column_id where i.object_id = object_id('" + entity.tableName + "', N'U' ) and i.index_id > 0;";
            connectionExecutor.name("获取索引信息").sql(getIndexSQL).executeQuery(resultSet -> {
                while (resultSet.next()) {
                    IndexField indexField = new IndexField();
                    if (resultSet.getBoolean("is_unique")) {
                        indexField.indexType = IndexType.UNIQUE;
                    } else {
                        indexField.indexType = IndexType.NORMAL;
                    }
                    indexField.indexName = resultSet.getString("name");
                    //判断是否已经存在该索引
                    IndexField existIndexField = entity.indexFieldList.stream().filter(indexField1 -> indexField1.indexName.equals(indexField.indexName)).findFirst().orElse(null);
                    if (null != existIndexField) {
                        existIndexField.columns.add(resultSet.getNString("col_name"));
                    } else {
                        indexField.columns.add(resultSet.getNString("col_name"));
                        entity.indexFieldList.add(indexField);
                    }
                }
            });
        }
    }

}