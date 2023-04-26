package cn.schoolwow.quickdao.dao.ddl;

import cn.schoolwow.quickdao.annotation.IndexType;
import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.IndexField;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class OracleDatabaseDefinition extends PostgreDatabaseDefinition{

    public OracleDatabaseDefinition(QuickDAOConfig quickDAOConfig) {
        super(quickDAOConfig);
    }

    @Override
    public boolean hasTable(String tableName) {
        String hasTableSQL = "select table_name from user_tables where table_name = ?";
        return connectionExecutor.name("判断表是否存在").sql(hasTableSQL).parameters(Arrays.asList(tableName)).executeAndCheckExists();
    }

    @Override
    public boolean hasColumn(String tableName, String columnName) {
        String hasTableColumnSQL = "select table_name, column_name, data_type, nullable, data_length from user_tab_columns where table_name = ? and column_name = ?";
        List parameters = Arrays.asList(tableName, columnName);
        return connectionExecutor.name("判断表指定列是否存在").sql(hasTableColumnSQL).parameters(parameters).executeAndCheckExists();
    }

    @Override
    public List<String> getTableNameList(){
        String getEntityListSQL = "select user_tables.table_name from user_tables";
        List<String> tableNames = new ArrayList<>();
        connectionExecutor.name("获取表名列表").sql(getEntityListSQL).executeQuery((resultSet)->{
            while (resultSet.next()) {
                tableNames.add(resultSet.getString("table_name"));
            }
        });
        return tableNames;
    }

    @Override
    public List<Entity> getDatabaseEntityList() {
        String getEntityListSQL = "select user_tables.table_name, user_tab_comments.comments from user_tables left join user_tab_comments on user_tables.table_name = user_tab_comments.table_name";
        List<Entity> entityList = new ArrayList<>();
        connectionExecutor.name("获取表列表").sql(getEntityListSQL).executeQuery((resultSet)->{
            while (resultSet.next()) {
                Entity entity = new Entity();
                entity.tableName = resultSet.getString("table_name");
                entity.comment = resultSet.getString("comments");
                entity.properties = getPropertyList(entity.tableName);
                entityList.add(entity);
            }
        });
        getIndex(entityList);
        return entityList;
    }

    @Override
    public Entity getDatabaseEntity(String tableName) {
        String getEntitySQL = "select user_tables.table_name,user_tab_comments.comments from user_tables left join user_tab_comments on user_tables.table_name = user_tab_comments.table_name where user_tables.table_name = ?";
        Entity entity = new Entity();
        connectionExecutor.name("获取表列表").sql(getEntitySQL).parameters(Arrays.asList(tableName)).executeQuery((resultSet)->{
            if(resultSet.next()){
                entity.tableName = resultSet.getString("table_name");
                entity.comment = resultSet.getString("comments");
                entity.properties = getPropertyList(entity.tableName);
                entity.indexFieldList = getIndexField(tableName);
            }
        });
        return null==entity.tableName?null:entity;
    }

    @Override
    public List<Property> getPropertyList(String tableName)  {
        String getEntityPropertyListSQL = "select table_name, column_name, data_type, nullable, data_length from user_tab_columns where table_name = ?";
        List<Property> propertyList = new ArrayList<>();
        connectionExecutor.name("获取表字段信息").sql(getEntityPropertyListSQL).parameters(Arrays.asList(tableName)).executeQuery((resultSet)->{
            while(resultSet.next()){
                Property property = new Property();
                property.column = resultSet.getString("column_name");
                property.columnType = resultSet.getString("data_type");
                if(property.columnType.contains(" ")){
                    property.columnType = property.columnType.substring(0,property.columnType.indexOf(" "));
                }
                String dataLength = resultSet.getString("data_length");
                if(property.columnType.toLowerCase().contains("char")&&null!=dataLength&&!dataLength.isEmpty()){
                    property.columnType += "(" + dataLength + ")";
                }
                property.notNull = "N".equals(resultSet.getString("nullable"));
                propertyList.add(property);
            }
        });
        String getPropertyCommentList = "select table_name, column_name, comments from user_col_comments where table_name = '"+tableName+"'";
        connectionExecutor.name("获取字段列表注释").sql(getPropertyCommentList).executeQuery((resultSet)->{
            while (resultSet.next()){
                for(Property property:propertyList){
                    if(property.column.equalsIgnoreCase(resultSet.getString("column_name"))){
                        property.comment = resultSet.getString("comments");
                        break;
                    }
                }
            }
        });
        return propertyList;
    }

    @Override
    public Property getProperty(String tableName, String columnName)  {
        String getEntityPropertyListSQL = "select table_name, column_name, data_type, nullable, data_length from user_tab_columns where table_name = ? and column_name = ?";
        List parameters = Arrays.asList(tableName, columnName);
        Property property = new Property();
        connectionExecutor.name("获取表字段信息").sql(getEntityPropertyListSQL).parameters(parameters).executeQuery((resultSet)->{
            if(resultSet.next()){
                property.column = resultSet.getString("column_name");
                property.columnType = resultSet.getString("data_type");
                if(property.columnType.contains(" ")){
                    property.columnType = property.columnType.substring(0,property.columnType.indexOf(" "));
                }
                String dataLength = resultSet.getString("data_length");
                if(property.columnType.toLowerCase().contains("char")&&null!=dataLength&&!dataLength.isEmpty()){
                    property.columnType += "(" + dataLength + ")";
                }
                property.notNull = "N".equals(resultSet.getString("nullable"));
            }
        });
        if(null!=property.column){
            String getPropertyCommentList = "select table_name, column_name, comments from user_col_comments where table_name = '"+tableName+"' and column_name = '"+columnName+"'";
            connectionExecutor.name("获取字段注释").sql(getPropertyCommentList).executeQuery((resultSet)->{
                if(resultSet.next()){
                    property.comment = resultSet.getString("comments");
                }
            });
        }
        return null==property.column?null:property;
    }

    @Override
    public void create(Entity entity) {
        super.create(entity);
        createSequence(entity);
    }

    @Override
    public boolean hasIndex(String tableName, String indexName) {
        String hasIndexSQL = "select index_name from user_indexes where table_name = ? and index_name = ?";
        List parameters = Arrays.asList(tableName, indexName);
        return connectionExecutor.name("判断索引是否存在").sql(hasIndexSQL).parameters(parameters).executeAndCheckExists();
    }

    @Override
    public List<IndexField> getIndexField(String tableName){
        List<IndexField> indexFieldList = new ArrayList<>();
        //获取索引信息
        String getIndexSQL = "select table_name, index_name,uniqueness from user_indexes where table_name = ?";
        connectionExecutor.name("获取索引信息").sql(getIndexSQL).parameters(Arrays.asList(tableName)).executeQuery((resultSet)->{
            while (resultSet.next()) {
                IndexField indexField = new IndexField();
                indexField.tableName = tableName;
                if("UNIQUE".equalsIgnoreCase(resultSet.getString("uniqueness"))){
                    indexField.indexType = IndexType.UNIQUE;
                }else{
                    indexField.indexType = IndexType.NORMAL;
                }
                indexField.indexName = resultSet.getString("index_name");
                indexFieldList.add(indexField);
            }
        });
        //获取索引字段信息
        //TODO 这里卡住了
//        String getIndexFieldsSQL = "select table_name,index_name,column_name from user_ind_columns where table_name = '"+tableName+"'";
//        connectionExecutor.name("获取索引字段信息").sql(getIndexFieldsSQL).executeQuery((resultSet)->{
//            while (resultSet.next()) {
//                String indexName = resultSet.getString("index_name");
//                IndexField existIndexField = indexFieldList.stream().filter(indexField1 -> indexField1.indexName.equals(indexName)).findFirst().orElse(null);
//                if(null==existIndexField){
//                    continue;
//                }
//                existIndexField.columns.add(resultSet.getString("column_name"));
//            }
//        });
        return indexFieldList;
    }

    @Override
    protected String getAutoIncrementSQL(Property property) {
        return quickDAOConfig.databaseProvider.escape(property.column) + " number not null";
    }

    @Override
    protected void getIndex(List<Entity> entityList)  {
        String getIndexSQL = "select table_name, index_name,uniqueness from user_indexes";
        connectionExecutor.name("获取索引信息").sql(getIndexSQL).executeQuery((resultSet)->{
            while (resultSet.next()) {
                for(Entity entity:entityList) {
                    String tableName = resultSet.getString("table_name");
                    if (!entity.tableName.equalsIgnoreCase(tableName)) {
                        continue;
                    }
                    IndexField indexField = new IndexField();
                    indexField.tableName = tableName;
                    if("UNIQUE".equalsIgnoreCase(resultSet.getString("uniqueness"))){
                        indexField.indexType = IndexType.UNIQUE;
                    }else{
                        indexField.indexType = IndexType.NORMAL;
                    }
                    indexField.indexName = resultSet.getString("index_name");
                    entity.indexFieldList.add(indexField);
                    break;
                }
            }
        });
        String getIndexColumnsSQL = "select table_name, index_name,column_name from user_ind_columns";
        connectionExecutor.name("获取索引字段信息").sql(getIndexColumnsSQL).executeQuery((resultSet)->{
            while (resultSet.next()) {
                String indexName = resultSet.getString("index_name");
                for(Entity entity:entityList) {
                    if (!entity.tableName.equalsIgnoreCase(resultSet.getString("table_name"))) {
                        continue;
                    }
                    IndexField existIndexField = entity.indexFieldList.stream().filter(indexField1 -> indexField1.indexName.equals(indexName)).findFirst().orElse(null);
                    if(null==existIndexField){
                        continue;
                    }
                    existIndexField.columns.add(resultSet.getString("column_name"));
                    break;
                }
            }
        });
    }

    /**创建序列和触发器*/
    private void createSequence(Entity entity) {
        if(null==entity.id){
            return;
        }
        StringBuilder createSequenceBuilder = new StringBuilder();
        //创建sequence
        String sequenceExistSQL = "select sequence_name from user_sequences where sequence_name= '" + entity.tableName.toUpperCase() + "_SEQ'";
        connectionExecutor.name("判断序列是否存在").sql(sequenceExistSQL).executeQuery((resultSet)->{
            if(resultSet.next()){
                //删除序列
                createSequenceBuilder.append("drop sequence " + entity.tableName.toUpperCase() + "_SEQ;");
            }
        });
        //创建序列
        createSequenceBuilder.append("create sequence " + entity.tableName + "_seq increment by 1 start with 1 minvalue 1 maxvalue 9999999999999 nocache order;");
        connectionExecutor.name("创建序列").sql(createSequenceBuilder.toString()).executeUpdate();
        //创建触发器
        String createTrigger = "create or replace trigger " + entity.tableName + "_trigger " +
                "before insert on " + quickDAOConfig.databaseProvider.escape(entity.tableName) + " " +
                "for each row when(new.\"" + entity.id.column + "\" is null) " +
                "begin select " + entity.tableName + "_seq.nextval into:new.\"" + entity.id.column + "\" from dual; end;";
        connectionExecutor.name("创建触发器").sql(createTrigger).executeUpdate();
    }
}