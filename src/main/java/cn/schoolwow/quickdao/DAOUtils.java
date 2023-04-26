package cn.schoolwow.quickdao;

import cn.schoolwow.quickdao.annotation.IdStrategy;
import cn.schoolwow.quickdao.dao.DAO;
import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.external.util.GenerateEntityFileOption;
import cn.schoolwow.quickdao.domain.external.util.SynchronizeTableStructureOption;
import cn.schoolwow.quickdao.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.*;

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
            if(!targetFile.getParentFile().exists()){
                if(!targetFile.getParentFile().mkdirs()){
                    logger.warn("创建文件夹失败,文件夹路径:{}", targetFile.getParent());
                    continue;
                }
            }
            logger.debug("准备生成文件!表名:{}, 文件名:{}", databaseEntity.tableName, targetFile);
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
                    property.comment = property.comment
                            .replaceAll("\r","\\n")
                            .replaceAll("\n","\\n")
                            .replaceAll("\r\n","\\n");
                    builder.append("\t@Comment(\""+property.comment +"\")\n");
                }
                if(property.id){
                    builder.append("\t@Id");
                    if(!IdStrategy.AutoIncrement.equals(property.strategy)){
                        builder.append("(strategy = IdStrategy.None)");
                    }
                    builder.append("\n");
                }
                builder.append("\t@ColumnName(\""+property.column+"\")\n");
                builder.append("\t@ColumnType(\""+property.columnType+(null==property.length?"":"("+property.length+")")+"\")\n");
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
                    logger.warn("字段类型匹配失败,表名:{}字段名称:{},类型:{}",databaseEntity.tableName,property.column,property.columnType);
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
        logger.info("实体类文件生成完毕!文件夹:{}", generateEntityFileOption.sourceClassPath + "/" + packageName.replace(".","/"));
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
}
