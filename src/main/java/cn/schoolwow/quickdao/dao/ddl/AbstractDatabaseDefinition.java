package cn.schoolwow.quickdao.dao.ddl;

import cn.schoolwow.quickdao.annotation.IdStrategy;
import cn.schoolwow.quickdao.dao.sql.AbstractDatabaseDAO;
import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.IndexField;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class AbstractDatabaseDefinition extends AbstractDatabaseDAO implements DatabaseDefinition {
    protected Logger logger = LoggerFactory.getLogger(AbstractDatabaseDefinition.class);

    public AbstractDatabaseDefinition(QuickDAOConfig quickDAOConfig) {
        super(quickDAOConfig);
    }

    @Override
    public boolean hasTable(Class clazz) {
        Entity entity = quickDAOConfig.getEntityByClassName(clazz.getName());
        return hasTable(entity.tableName);
    }

    @Override
    public boolean hasTable(String tableName) {
        throw new UnsupportedOperationException("当前数据库不支持判断表是否存在!");
    }

    @Override
    public boolean hasColumn(String tableName, String columnName) {
        throw new UnsupportedOperationException("当前数据库不支持判断表的指定列是否存在!");
    }

    @Override
    public List<String> getTableNameList(){
        throw new UnsupportedOperationException("当前数据库不支持获取所有表名信息!");
    }

    @Override
    public List<Entity> getDatabaseEntityList() {
        throw new UnsupportedOperationException("当前数据库不支持获取表信息!");
    }

    @Override
    public Entity getDatabaseEntity(String tableName) {
        throw new UnsupportedOperationException("当前数据库不支持获取指定表信息!");
    }

    @Override
    public List<Property> getPropertyList(Class clazz) {
        Entity entity = quickDAOConfig.getEntityByClassName(clazz.getName());
        return getPropertyList(entity.tableName);
    }

    @Override
    public List<Property> getPropertyList(String tableName) {
        throw new UnsupportedOperationException("当前数据库不支持获取表字段列表信息!");
    }

    @Override
    public Property getProperty(Class clazz, String columnName) {
        Entity entity = quickDAOConfig.getEntityByClassName(clazz.getName());
        return getProperty(entity.tableName, columnName);
    }

    @Override
    public Property getProperty(String tableName, String columnName) {
        throw new UnsupportedOperationException("当前数据库不支持获取表指定字段信息!");
    }

    @Override
    public void create(Class clazz) {
        Entity entity = quickDAOConfig.getEntityByClassName(clazz.getName());
        create(entity);
    }

    @Override
    public void create(Entity entity) {
        throw new UnsupportedOperationException("当前数据库不支持创建表操作!");
    }

    @Override
    public void dropTable(Class clazz) {
        Entity entity = quickDAOConfig.getEntityByClassName(clazz.getName());
        dropTable(entity.tableName);
    }

    @Override
    public void dropTable(String tableName) {
        String dropTableSQL = "drop table " + quickDAOConfig.databaseProvider.escape(tableName) + ";";
        connectionExecutor.name("删除表").sql(dropTableSQL).executeUpdate();
        quickDAOConfig.deleteDatabaseEntityCache(tableName);
    }

    @Override
    public void rebuild(Class clazz) {
        if (hasTable(clazz)) {
            dropTable(clazz);
        }
        create(clazz);
    }

    @Override
    public void rebuild(String tableName) {
        if(hasTable(tableName)){
            dropTable(tableName);
        }
        Entity entity = quickDAOConfig.getDatabaseEntityByTableName(tableName);
        create(entity);
    }

    @Override
    public Property createColumn(String tableName, Property property) {
        Entity entity = quickDAOConfig.getDatabaseEntityByTableName(tableName);
        entity.tableName = tableName;
        property.entity = entity;
        if (null != property.check) {
            if (!property.check.isEmpty() && !property.check.contains("(")) {
                property.check = "(" + property.check + ")";
            }
            property.check = property.check.replace("#{" + property.name + "}", property.column);
            property.escapeCheck = property.check.replace(property.column, quickDAOConfig.databaseProvider.escape(property.column));
        }

        StringBuilder createColumnBuilder = new StringBuilder("alter table " + quickDAOConfig.databaseProvider.escape(tableName) + " add ");
        if (property.id && property.strategy == IdStrategy.AutoIncrement) {
            createColumnBuilder.append(getAutoIncrementSQL(property));
        } else {
            createColumnBuilder.append(quickDAOConfig.databaseProvider.escape(property.column) + " " + property.columnType + (null == property.length ? "" : "(" + property.length + ")"));
            if (null != property.defaultValue && !property.defaultValue.isEmpty()) {
                createColumnBuilder.append(" default " + property.defaultValue);
            }
            if (property.notNull) {
                createColumnBuilder.append(" not null");
            }
            if (null != property.escapeCheck && !property.escapeCheck.isEmpty()) {
                createColumnBuilder.append(" check " + property.escapeCheck);
            }
            if (null != property.comment) {
                //sqlite不添加注释信息
                if(!"sqlite".equalsIgnoreCase(quickDAOConfig.databaseProvider.name())){
                    createColumnBuilder.append(" " + quickDAOConfig.databaseProvider.comment(property.comment));
                }
            }
        }
        createColumnBuilder.append(";");
        String createPropertySQL = createColumnBuilder.toString();
        connectionExecutor.name("新增列").sql(createPropertySQL).executeUpdate();
        quickDAOConfig.deleteDatabaseEntityCache(tableName);
        return property;
    }

    @Override
    public void alterColumn(Property property) {
        StringBuilder alterColumnBuilder = new StringBuilder("alter table " + quickDAOConfig.databaseProvider.escape(property.entity.tableName));
        if("sqlserver".equalsIgnoreCase(quickDAOConfig.databaseProvider.name())){
            alterColumnBuilder.append(" alter");
        }else{
            alterColumnBuilder.append(" modify");
        }
        alterColumnBuilder.append(" column " + quickDAOConfig.databaseProvider.escape(property.column) + " " + property.columnType + (null == property.length ? "" : "(" + property.length + ")"));
        if (property.notNull) {
            alterColumnBuilder.append(" not null");
        }
        if (null != property.defaultValue && !property.defaultValue.isEmpty()) {
            alterColumnBuilder.append(" default " + property.defaultValue);
        }
        if (null != property.escapeCheck && !property.escapeCheck.isEmpty()) {
            alterColumnBuilder.append(" check " + property.escapeCheck);
        }
        if (null != property.comment) {
            alterColumnBuilder.append(" " + quickDAOConfig.databaseProvider.comment(property.comment));
        }
        alterColumnBuilder.append(";");
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
        String dropColumnSQL = "alter table " + quickDAOConfig.databaseProvider.escape(tableName) + " drop column " + quickDAOConfig.databaseProvider.escape(columnName) + ";";
        connectionExecutor.name("删除列").sql(dropColumnSQL).executeUpdate();
        quickDAOConfig.deleteDatabaseEntityCache(tableName);
        return deletedProperty;
    }

    @Override
    public boolean hasIndex(String tableName, String indexName) {
        throw new UnsupportedOperationException("当前数据库不支持判断表索引是否存在!");
    }

    @Override
    public boolean hasConstraint(String tableName, String constraintName) {
        String hasConstraintExistsSQL = "select constraint_name from information_schema.KEY_COLUMN_USAGE where constraint_name='" + constraintName + "';";
        return connectionExecutor.name("判断约束是否存在").sql(hasConstraintExistsSQL).executeAndCheckExists();
    }

    @Override
    public List<IndexField> getIndexField(String tableName) {
        throw new UnsupportedOperationException("当前数据库不支持获取表索引信息!");
    }

    @Override
    public void createIndex(IndexField indexField) {
        String createIndexSQL = getCreateIndexStatement(indexField);
        connectionExecutor.name("创建索引").sql(createIndexSQL).executeUpdate();
    }

    @Override
    public void dropIndex(String tableName, String indexName) {
        String dropIndexSQL = "drop index " + quickDAOConfig.databaseProvider.escape(indexName) + ";";
        connectionExecutor.name("删除索引").sql(dropIndexSQL).executeUpdate();
        quickDAOConfig.deleteDatabaseEntityCache(tableName);
    }

    @Override
    public void createForeignKey(Property property) {
        String operation = property.foreignKey.foreignKeyOption().getOperation();
        String reference = quickDAOConfig.databaseProvider.escape(quickDAOConfig.getEntityByClassName(property.foreignKey.table().getName()).tableName) + "(" + quickDAOConfig.databaseProvider.escape(property.foreignKey.field()) + ") ON DELETE " + operation + " ON UPDATE " + operation;
        String foreignKeyName = "FK_" + property.entity.tableName + "_" + property.foreignKey.field() + "_" + quickDAOConfig.getEntityByClassName(property.foreignKey.table().getName()).tableName + "_" + property.name;
        String createForeignKeySQL = "alter table " + quickDAOConfig.databaseProvider.escape(property.entity.tableName) + " add constraint " + quickDAOConfig.databaseProvider.escape(foreignKeyName) + " foreign key(" + quickDAOConfig.databaseProvider.escape(property.column) + ") references " + reference + ";";
        if (hasConstraint(property.entity.tableName, foreignKeyName)) {
            logger.warn("外键约束已存在,表名:{},外键约束名:{}", property.entity.tableName, foreignKeyName);
            return;
        }
        connectionExecutor.name("创建外键").sql(createForeignKeySQL).executeUpdate();
        quickDAOConfig.deleteDatabaseEntityCache(property.entity.tableName);
    }

    @Override
    public void enableForeignConstraintCheck(boolean enable) {
        logger.warn("当前数据库不支持开启/关闭外键约束!");
    }

    /**
     * 获取自增语句
     *
     * @param property 自增字段信息
     */
    protected abstract String getAutoIncrementSQL(Property property);

    /**
     * 提取索引信息
     */
    protected abstract void getIndex(List<Entity> entityList);

    /**
     * 获取创建索引语句
     */
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
        createIndexBuilder.append(" index " + quickDAOConfig.databaseProvider.escape(indexField.indexName) + " on " + quickDAOConfig.databaseProvider.escape(indexField.tableName) + "(");
        for (String column : indexField.columns) {
            createIndexBuilder.append(quickDAOConfig.databaseProvider.escape(column) + ",");
        }
        createIndexBuilder.deleteCharAt(createIndexBuilder.length() - 1);
        createIndexBuilder.append(")");
        if (null != indexField.using && !indexField.using.isEmpty()) {
            createIndexBuilder.append(" using " + indexField.using);
        }
        if (null != indexField.comment && !indexField.comment.isEmpty()) {
            createIndexBuilder.append(" " + quickDAOConfig.databaseProvider.comment(indexField.comment));
        }
        createIndexBuilder.append(";");
        return createIndexBuilder.toString();
    }

}