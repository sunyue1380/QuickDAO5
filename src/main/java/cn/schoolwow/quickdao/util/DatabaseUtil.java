package cn.schoolwow.quickdao.util;

import cn.schoolwow.quickdao.dao.DAO;
import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.IndexField;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * 数据库工具类
 */
public class DatabaseUtil {
    private static Logger logger = LoggerFactory.getLogger(DatabaseUtil.class);

    /**
     * 自动建表
     */
    public static void automaticCreateTable(QuickDAOConfig quickDAOConfig) {
        Collection<Entity> entityList = quickDAOConfig.entityMap.values();
        for (Entity entity : entityList) {
            Entity databaseEntity = quickDAOConfig.getDatabaseEntityByTableName(entity.tableName);
            logger.trace("自动创建表,数据库表名:{}, 实体类名:{}", entity.tableName, entity.clazz.getName());
            if (null == databaseEntity) {
                quickDAOConfig.dao.create(entity);
            }
        }
    }

    /**
     * 自动新增字段
     */
    public static void automaticCreateProperty(QuickDAOConfig quickDAOConfig) {
        Collection<Entity> entityList = quickDAOConfig.entityMap.values();
        for (Entity entity : entityList) {
            Entity databaseEntity = quickDAOConfig.getDatabaseEntityByTableName(entity.tableName);
            List<Property> sourcePropertyList = entity.properties;
            for (Property sourceProperty : sourcePropertyList) {
                Property targetProperty = databaseEntity.getPropertyByFieldName(sourceProperty.column);
                if (null == targetProperty) {
                    logger.trace("自动创建表字段,表名:{},字段名:{}", databaseEntity.tableName, sourceProperty.column);
                    quickDAOConfig.dao.createColumn(databaseEntity.tableName, sourceProperty);
                }
                if (null != sourceProperty.foreignKey && quickDAOConfig.databaseOption.openForeignKey) {
                    logger.trace("自动创建表外键,表名:{},字段名:{}", sourceProperty.entity.tableName, sourceProperty.column);
                    quickDAOConfig.dao.createForeignKey(sourceProperty);
                }
            }
            //判断索引是否有新增
            for (IndexField indexField : entity.indexFieldList) {
                if (null == databaseEntity.getIndexFieldByIndexName(indexField.indexName)) {
                    quickDAOConfig.dao.createIndex(indexField);
                }
            }
        }
    }

    /**
     * 删除多余数据库表和字段
     */
    public static void automaticDeleteTableAndProperty(DAO dao) {
        Map<String, Entity> entityMap = dao.getEntityMap();
        if (null == entityMap || entityMap.isEmpty()) {
            logger.warn("未扫描到任何实体类!请设置包路径或者实体类!");
            return;
        }
        Collection<Entity> entityList = entityMap.values();
        List<Entity> databaseEntityList = dao.getDatabaseEntityList();

        //待删除的表
        List<String> dropTableNameList = new ArrayList<>();
        //待删除的字段
        List<Property> dropPropertyList = new ArrayList<>();
        for (Entity databaseEntity : databaseEntityList) {
            Entity sourceEntity = null;
            for (Entity entityItem : entityList) {
                if (databaseEntity.tableName.equalsIgnoreCase(entityItem.tableName)) {
                    sourceEntity = entityItem;
                    break;
                }
            }
            if (null == sourceEntity) {
                dropTableNameList.add(databaseEntity.tableName);
                continue;
            }
            for (Property dbProperty : databaseEntity.properties) {
                Property sourceEntityProperty = null;
                for (Property propertyItem : sourceEntity.properties) {
                    if (dbProperty.column.equalsIgnoreCase(propertyItem.column)) {
                        sourceEntityProperty = propertyItem;
                        break;
                    }
                }
                if (null == sourceEntityProperty) {
                    dropPropertyList.add(dbProperty);
                }
            }
        }
        for (String dropTableName : dropTableNameList) {
            logger.info("同步实体类,删除表:{}", dropTableName);
            dao.dropTable(dropTableName);
        }
        for (Property dropProperty : dropPropertyList) {
            logger.info("同步实体类,删除表字段.表名:{},字段名:{}", dropProperty.entity.tableName, dropProperty.column);
            dao.dropColumn(dropProperty.entity.tableName, dropProperty.column);
        }
    }

}
