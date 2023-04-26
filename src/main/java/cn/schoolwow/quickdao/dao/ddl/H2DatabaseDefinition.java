package cn.schoolwow.quickdao.dao.ddl;

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

public class H2DatabaseDefinition extends MySQLDatabaseDefinition {

    public H2DatabaseDefinition(QuickDAOConfig quickDAOConfig) {
        super(quickDAOConfig);
    }

    @Override
    public boolean hasTable(String tableName) {
        String hasTableSQL = "select table_name from information_schema.tables where table_name = ?;";
        return connectionExecutor.name("判断表是否存在").sql(hasTableSQL).parameters(Arrays.asList(tableName.toUpperCase())).executeAndCheckExists();
    }

    @Override
    public boolean hasColumn(String tableName, String columnName) {
        String hasTableColumnSQL = "select table_name, column_name from information_schema.`columns` where table_schema = 'PUBLIC' and table_name = ? and column_name = ?;";
        List parameters = Arrays.asList(tableName.toUpperCase(),columnName.toUpperCase());
        return connectionExecutor.name("判断表指定列是否存在").sql(hasTableColumnSQL).parameters(parameters).executeAndCheckExists();
    }

    @Override
    public List<String> getTableNameList(){
        String getEntityListSQL = "show tables;";
        List<String> tableNames = new ArrayList<>();
        connectionExecutor.name("获取表名列表").sql(getEntityListSQL).executeQuery((resultSet) -> {
            while (resultSet.next()) {
                tableNames.add(resultSet.getString(1));
            }
        });
        return tableNames;
    }

    @Override
    public List<Entity> getDatabaseEntityList() {
        String getEntityListSQL = "show tables;";
        List<Entity> entityList = new ArrayList<>();
        connectionExecutor.name("获取表列表").sql(getEntityListSQL).executeQuery((resultSet) -> {
            while (resultSet.next()) {
                Entity entity = new Entity();
                entity.tableName = resultSet.getString(1);
                entity.properties = getPropertyList(entity.tableName);
                entityList.add(entity);
            }
        });
        getIndex(entityList);
        return entityList;
    }

    @Override
    public Entity getDatabaseEntity(String tableName) {
        List<Entity> entityList = getDatabaseEntityList();
        for(Entity entity:entityList){
            if(entity.tableName.equalsIgnoreCase(tableName)){
                entity.properties = getPropertyList(entity.tableName);
                entity.indexFieldList = getIndexField(tableName);
                return entity;
            }
        }
        return null;
    }

    @Override
    public List<Property> getPropertyList(String tableName) {
        String getTableColumnSQL = "select table_name, column_name, type_name, character_maximum_length, is_nullable, column_default from information_schema.`columns` where table_schema = 'PUBLIC' and table_name = ?;";
        List<Property> propertyList = new ArrayList<>();
        connectionExecutor.name("获取表字段列表").sql(getTableColumnSQL).parameters(Arrays.asList(tableName.toUpperCase())).executeQuery((resultSet) -> {
            while (resultSet.next()) {
                Property property = new Property();
                getProperty(property, resultSet);
                propertyList.add(property);
            }
        });
        return propertyList;
    }

    @Override
    public Property getProperty(String tableName, String columnName) {
        String getTableColumnSQL = "select table_name, column_name, type_name, character_maximum_length, is_nullable, column_default from information_schema.`columns` where table_schema = 'PUBLIC' and table_name = ? and column_name = ?;";
        List parameters = Arrays.asList(tableName.toUpperCase(), columnName.toUpperCase());
        Property property = new Property();
        connectionExecutor.name("获取表字段信息").sql(getTableColumnSQL).parameters(parameters).executeQuery((resultSet) -> {
            if (resultSet.next()) {
                getProperty(property, resultSet);
            }
        });
        return null == property.column ? null : property;
    }

    @Override
    public boolean hasIndex(String tableName, String indexName) {
        String hasIndexExistsSQL = "select index_name from information_schema.indexes where index_name = ?;";
        return connectionExecutor.name("判断索引是否存在").sql(hasIndexExistsSQL).parameters(Arrays.asList(indexName.toUpperCase())).executeAndCheckExists();
    }

    @Override
    public List<IndexField> getIndexField(String tableName) {
        String getIndexSQL = "select table_name, sql from information_schema.indexes where table_name = ?";
        List<IndexField> indexFieldList = new ArrayList<>();
        connectionExecutor.name("获取索引信息").sql(getIndexSQL).parameters(Arrays.asList(tableName.toUpperCase())).executeQuery((resultSet) -> {
            while (resultSet.next()) {
                IndexField indexField = getIndexField(resultSet);
                indexFieldList.add(indexField);
            }
        });
        return indexFieldList;
    }

    @Override
    protected void getIndex(List<Entity> entityList) {
        String getIndexSQL = "select table_name, sql from information_schema.indexes";
        connectionExecutor.name("获取索引信息").sql(getIndexSQL).executeQuery((resultSet) -> {
            while (resultSet.next()) {
                for (Entity entity : entityList) {
                    if (!entity.tableName.equalsIgnoreCase(resultSet.getString("table_name"))) {
                        continue;
                    }
                    IndexField indexField = getIndexField(resultSet);
                    entity.indexFieldList.add(indexField);
                    break;
                }
            }
        });
    }

    /**
     * 提取属性字段信息
     */
    private Property getProperty(Property property, ResultSet resultSet) throws SQLException {
        property.column = resultSet.getString("column_name");
        //无符号填充0 => float unsigned zerofill
        property.columnType = resultSet.getString("type_name");
        if (property.columnType.contains(" ")) {
            property.columnType = property.columnType.substring(0, property.columnType.indexOf(" "));
        }
        Object character_maximum_length = resultSet.getObject("character_maximum_length");
        if (null != character_maximum_length && character_maximum_length.toString().length() < 7) {
            property.length = Integer.parseInt(character_maximum_length.toString());
        }
        property.notNull = "NO".equals(resultSet.getString("is_nullable"));
        if (null != resultSet.getString("column_default")) {
            property.defaultValue = resultSet.getString("column_default");
        }
        return property;
    }

    /**
     * 提取索引字段信息
     */
    private IndexField getIndexField(ResultSet resultSet) throws SQLException {
        String sql = resultSet.getString("sql");
        String[] tokens = sql.split("\"");
        IndexField indexField = new IndexField();
        if (tokens[0].contains("UNIQUE")) {
            indexField.indexType = IndexType.UNIQUE;
        } else {
            indexField.indexType = IndexType.NORMAL;
        }
        indexField.indexName = tokens[3];
        indexField.tableName = tokens[7];
        for (int i = 9; i < tokens.length - 1; i++) {
            indexField.columns.add(tokens[i]);
        }
        return indexField;
    }
}
