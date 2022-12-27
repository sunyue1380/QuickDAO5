package cn.schoolwow.quickdao;

import cn.schoolwow.quickdao.dao.DAO;
import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.external.util.GenerateEntityFileOption;
import cn.schoolwow.quickdao.domain.external.util.MigrateOption;
import cn.schoolwow.quickdao.domain.external.util.SynchronizeTableStructureOption;
import cn.schoolwow.quickdao.util.DatabaseUtil;
import cn.schoolwow.quickdao.util.StringUtil;
import com.alibaba.fastjson.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Collectors;

/**
 * DAO工具类
 * */
public class DAOUtils {
    private static Logger logger = LoggerFactory.getLogger(DAOUtils.class);

    /**生成实体类Java代码文件*/
    public static void generateEntityFile(GenerateEntityFileOption generateEntityFileOption){
        DAO dao = generateEntityFileOption.dao;
        QuickDAOConfig quickDAOConfig = dao.getQuickDAOConfig();
        if(quickDAOConfig.entityOption.packageNameMap.isEmpty()){
            throw new IllegalArgumentException("请先调用packageName方法指定包名");
        }
        List<Entity> databaseEntityList = new ArrayList<>();
        if(null!=generateEntityFileOption.tableNames){
            for(String tableName:generateEntityFileOption.tableNames){
                databaseEntityList.add(dao.getDatabaseEntity(tableName));
            }
        }else{
            databaseEntityList.addAll(dao.getDatabaseEntityList());
        }
        StringBuilder builder = new StringBuilder();
        String packageName = quickDAOConfig.entityOption.packageNameMap.keySet().iterator().next();
        final Set<Map.Entry<String,String>> typeFieldMappingEntrySet = quickDAOConfig.databaseProvider.getTypeFieldMapping().entrySet();
        
        for(Entity databaseEntity:databaseEntityList){
            String entityClassName = StringUtil.underline2Camel(databaseEntity.tableName);
            entityClassName = entityClassName.toUpperCase().charAt(0)+entityClassName.substring(1);
            if(null!=generateEntityFileOption.entityClassNameMapping){
                String newEntityClassName = generateEntityFileOption.entityClassNameMapping.apply(databaseEntity,entityClassName);
                if(null!=newEntityClassName&&!newEntityClassName.isEmpty()){
                    entityClassName = newEntityClassName;
                }
            }
            File targetFile = new File(generateEntityFileOption.sourceClassPath+"/"+ packageName.replace(".","/") + "/" + entityClassName.replace(".","/")+".java");
            if(!targetFile.getParentFile().mkdirs()){
                logger.warn("创建文件夹失败,文件夹路径:{}", targetFile.getParent());
                continue;
            }
            builder.setLength(0);
            //新建Java类
            builder.append("package " + packageName + (entityClassName.contains(".")?"."+entityClassName.substring(0,entityClassName.lastIndexOf(".")):"") +";\n");
            builder.append("import cn.schoolwow.quickdao.annotation.*;\n\n");
            if(null!=databaseEntity.comment){
                builder.append("@Comment(\""+databaseEntity.comment+"\")\n");
            }
            if(null!=databaseEntity.tableName){
                builder.append("@TableName(\""+databaseEntity.tableName+"\")\n");
            }
            builder.append("public class "+(entityClassName.contains(".")?entityClassName.substring(entityClassName.lastIndexOf(".")+1):entityClassName)+"{\n\n");
            for(Property property:databaseEntity.properties){
                if(null!=property.comment&&!property.comment.isEmpty()){
                    builder.append("\t@Comment(\""+property.comment.replaceAll("\r\n","")+"\")\n");
                }
                if(property.id){
                    builder.append("\t@Id\n");
                }
                builder.append("\t@ColumnName(\""+property.column+"\")\n");
                builder.append("\t@ColumnType(\""+property.columnType+"\")\n");
                if(property.columnType.contains("(")){
                    property.columnType = property.columnType.substring(0,property.columnType.indexOf("("));
                }
                if(null!=generateEntityFileOption.columnFieldTypeMapping){
                    property.className = generateEntityFileOption.columnFieldTypeMapping.apply(property.columnType);
                }
                if(null==property.className){
                    for(Map.Entry<String,String> entry:typeFieldMappingEntrySet){
                        if(entry.getValue().contains(property.columnType.toUpperCase())){
                            property.className = entry.getKey().replace("java.lang.","");
                            break;
                        }
                    }
                }
                if(null==property.className){
                    logger.warn("[字段类型匹配失败]表名:{}字段名称:{},类型:{}",databaseEntity.tableName,property.column,property.columnType);
                    property.className = "{{"+property.columnType+"}}";
                }
                if(null==property.name||property.name.isEmpty()){
                    property.name = StringUtil.underline2Camel(property.column);
                }
                builder.append("\tprivate "+property.className+" "+property.name+";\n\n");
            }

            for(Property property:databaseEntity.properties){
                builder.append("\tpublic "+ property.className +" get" +StringUtil.firstLetterUpper(property.name)+"(){\n\t\treturn this."+property.name+";\n\t}\n\n");
                builder.append("\tpublic void set" +StringUtil.firstLetterUpper(property.name)+"("+property.className+" "+property.name+"){\n\t\tthis."+property.name+" = "+property.name+";\n\t}\n\n");
            }

            builder.append("\t@Override\n\tpublic String toString() {\n\t\treturn \"\\n{\\n\" +\n");
            for(Property property:databaseEntity.properties){
                builder.append("\t\t\t\""+(null==property.comment?property.column:property.comment)+":\" + "+property.name + " + \"\\n\" +\n");
            }
            builder.replace(builder.length()-3,builder.length(),";");
            builder.append("\t}\n");

            builder.append("};");
            if(targetFile.exists()){
                if(!targetFile.delete()){
                    logger.warn("删除文件失败,文件路径:{}", targetFile.getAbsolutePath());
                    continue;
                }
            }
            try {
                Files.write(targetFile.toPath(), builder.toString().getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE,StandardOpenOption.WRITE);
            } catch (IOException e) {
                logger.error("实体类文件写入失败", e);
            }
        }
    }

    /**
     * 同步数据库结构
     * @param synchronizeTableStructureOption 同步表结构选项
     * */
    public static void synchronizeTableStructure(SynchronizeTableStructureOption synchronizeTableStructureOption){
        if(null==synchronizeTableStructureOption.source){
            throw new IllegalArgumentException("请指定迁移源数据库!");
        }
        if(null==synchronizeTableStructureOption.target){
            throw new IllegalArgumentException("请指定迁移目标数据库!");
        }
        List<Entity> sourceEntityList = new ArrayList<>();
        if(null==synchronizeTableStructureOption.tableNames||synchronizeTableStructureOption.tableNames.length==0){
            sourceEntityList = synchronizeTableStructureOption.source.getDatabaseEntityList();
        }else{
            for(String tableName:synchronizeTableStructureOption.tableNames){
                Entity sourceEntity = synchronizeTableStructureOption.source.getDatabaseEntity(tableName);
                if(null==sourceEntity){
                    logger.warn("数据库表不存在!表名:"+tableName);
                    continue;
                }
                sourceEntityList.add(sourceEntity);
            }
        }

        //记录需要新增的表,字段以及需要更新的字段
        List<Entity> addEntityList = new ArrayList<>();
        List<Property> addPropertyList = new ArrayList<>();
        Map<Property,Property> updatePropertyMap = new HashMap<>();
        for(Entity sourceEntity:sourceEntityList){
            Entity targetEntity = synchronizeTableStructureOption.target.getDatabaseEntity(sourceEntity.tableName);
            if(null==targetEntity){
                addEntityList.add(sourceEntity);
                continue;
            }
            List<Property> sourcePropertyList = sourceEntity.properties;
            for(Property sourceProperty:sourcePropertyList){
                Property targetProperty = targetEntity.properties.stream().filter(property -> property.column.equalsIgnoreCase(sourceProperty.column)).findFirst().orElse(null);
                if(null==targetProperty){
                    addPropertyList.add(sourceProperty);
                }else if(synchronizeTableStructureOption.diffPropertyPredicate.test(sourceProperty,targetProperty)){
                    updatePropertyMap.put(sourceProperty,targetProperty);
                }
            }
        }

        for(Entity entity:addEntityList){
            if(null!=synchronizeTableStructureOption.createTablePredicate&&!synchronizeTableStructureOption.createTablePredicate.test(entity)){
                continue;
            }
            synchronizeTableStructureOption.target.create(entity);
            if(synchronizeTableStructureOption.executeSQL){
                synchronizeTableStructureOption.target.create(entity);
            }
        }
        for(Property property:addPropertyList){
            if(null!=synchronizeTableStructureOption.createPropertyPredicate&&!synchronizeTableStructureOption.createPropertyPredicate.test(property)){
                continue;
            }
            synchronizeTableStructureOption.target.createColumn(property.entity.tableName, property);
            if(synchronizeTableStructureOption.executeSQL){
                synchronizeTableStructureOption.target.createColumn(property.entity.tableName,property);
            }
        }
        Set<Map.Entry<Property,Property>> propertyEntrySet = updatePropertyMap.entrySet();
        for(Map.Entry<Property,Property> entry:propertyEntrySet){
            if(null!=synchronizeTableStructureOption.updatePropertyPredicate&&!synchronizeTableStructureOption.updatePropertyPredicate.test(entry.getKey(),entry.getValue())){
                continue;
            }
            synchronizeTableStructureOption.target.alterColumn(entry.getKey());
            if(synchronizeTableStructureOption.executeSQL){
                synchronizeTableStructureOption.target.alterColumn(entry.getKey());
            }
        }
    }

    /**
     * 数据库迁移
     * @param migrateOption 迁移选项
     * */
    public static void migrate(MigrateOption migrateOption){
        if(null==migrateOption.source){
            throw new IllegalArgumentException("请指定迁移源数据库!");
        }
        if(null==migrateOption.target){
            throw new IllegalArgumentException("请指定迁移目标数据库!");
        }
        migrateOption.target.enableForeignConstraintCheck(false);
        try{
            if(migrateOption.source.getEntityMap().isEmpty()){
                dbEntityMigrate(migrateOption);
            }else{
                entityMigrate(migrateOption);
            }
        } finally {
            migrateOption.target.enableForeignConstraintCheck(true);
        }
    }

    /**数据库表数据迁移*/
    private static void dbEntityMigrate(MigrateOption migrateOption){
        List<Entity> sourceEntityList = migrateOption.source.getDatabaseEntityList();
        if(null!=migrateOption.tableFilter){
            sourceEntityList = sourceEntityList.stream().filter(migrateOption.tableFilter).collect(Collectors.toList());
        }
        if(null==sourceEntityList||sourceEntityList.isEmpty()){
            logger.warn("[数据迁移]当前迁移源数据库表列表为空!");
            return;
        }

        for(Entity sourceEntity:sourceEntityList){
            Entity targetEntity = sourceEntity.clone();
            if(null!=migrateOption.tableConsumer){
                migrateOption.tableConsumer.accept(sourceEntity,targetEntity);
            }
            long count = migrateOption.source.query(sourceEntity.tableName).execute().count();
            int effect = 0;
            if(count>0){
                //传输数据
                long totalPage = count/migrateOption.batchCount+1;
                logger.info("[数据迁移]准备迁移数据库表,源表:{},总记录数:{},迁移目标表:{}",sourceEntity.tableName,count,targetEntity.tableName);
                for(int i=1;i<=totalPage;i++){
                    logger.debug("[数据迁移]准备传输第{}/{}页数据,源数据库表:{},目标数据库表:{}",i,totalPage,sourceEntity.tableName,targetEntity.tableName);
                    JSONArray array = migrateOption.source.query(sourceEntity.tableName)
                            .page(i,migrateOption.batchCount)
                            .execute()
                            .getArray();
                    effect += migrateOption.target
                            .insert(targetEntity.tableName, array);
                    logger.debug("[数据迁移]第{}/{}页数据传输完毕,迁移完成记录数:{}/{},源数据库表:{},目标数据库表:{}",i,totalPage,effect,count,sourceEntity.tableName,targetEntity.tableName);
                }
            }
            logger.info("[数据迁移]表数据迁移完毕,迁移完成记录数:{}/{},源数据库表:{},目标数据库表:{}",effect,count,sourceEntity.tableName,targetEntity.tableName);
        }
    }

    /**实体类数据迁移*/
    private static void entityMigrate(MigrateOption migrateOption){
        Map<String, Entity> sourceEntityMap = migrateOption.source.getEntityMap();
        if(null!=migrateOption.tableFilter){
            Iterator<Map.Entry<String,Entity>> iterator = sourceEntityMap.entrySet().iterator();
            while(iterator.hasNext()){
                Entity entity = iterator.next().getValue();
                if(!migrateOption.tableFilter.test(entity)){
                    iterator.remove();
                }
            }
        }
        if(null==sourceEntityMap||sourceEntityMap.isEmpty()){
            logger.warn("[增量数据迁移]当前迁移源数据库表列表为空!");
            return;
        }
        DatabaseUtil.automaticCreateTable(migrateOption.target.getQuickDAOConfig());
        DatabaseUtil.automaticCreateProperty(migrateOption.target.getQuickDAOConfig());
        DatabaseUtil.automaticDeleteTableAndProperty(migrateOption.target);
        for(Entity sourceEntity:sourceEntityMap.values()){
            Entity targetEntity = sourceEntity.clone();
            if(null!=migrateOption.tableConsumer){
                migrateOption.tableConsumer.accept(sourceEntity,targetEntity);
            }
            long count = migrateOption.source.query(sourceEntity.tableName).execute().count();
            int effect = 0;
            if(count>0){
                //传输数据
                long totalPage = count/migrateOption.batchCount+1;
                logger.info("[增量数据迁移]准备迁移数据库表,源表:{},总记录数:{},迁移目标表:{}",sourceEntity.tableName,count,targetEntity.tableName);
                for(int i=1;i<=totalPage;i++){
                    logger.debug("[增量数据迁移]准备传输第{}/{}页数据,源数据库表:{},目标数据库表:{}",i,totalPage,sourceEntity.tableName,targetEntity.tableName);
                    List list = migrateOption.source.query(sourceEntity.clazz)
                            .page(i,migrateOption.batchCount)
                            .execute()
                            .getList();
                    effect += migrateOption.target.insertIgnore(list);
                    logger.debug("[增量数据迁移]第{}/{}页数据传输完毕,迁移完成记录数:{}/{},源数据库表:{},目标数据库表:{}",i,totalPage,effect,count,sourceEntity.tableName,targetEntity.tableName);
                }
            }
            logger.info("[增量数据迁移]表数据迁移完毕,迁移完成记录数:{}/{},源数据库表:{},目标数据库表:{}",effect,count,sourceEntity.tableName,targetEntity.tableName);
        }
    }
}
