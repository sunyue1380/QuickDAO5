package cn.schoolwow.quickdao.util;

import cn.schoolwow.quickdao.annotation.*;
import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.IndexField;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.internal.EntityOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.JarURLConnection;
import java.net.URL;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * 实体类处理工具类
 */
public class EntityUtil {
    private static Logger logger = LoggerFactory.getLogger(EntityUtil.class);

    /**
     * 获取实体类映射
     */
    public static Map<String, Entity> getEntityMap(EntityOption entityOption) {
        Map<String, Entity> entityMap = new HashMap<>();
        getPackageClass(entityMap, entityOption);
        getEntityClassMap(entityMap, entityOption.entityClassMap);
        Iterator<Entity> entityIterator = entityMap.values().iterator();
        Map<String, String> typeFieldMapping = entityOption.databaseProvider.getTypeFieldMapping();
        while (entityIterator.hasNext()) {
            Entity entity = entityIterator.next();
            //属性列表
            List<Property> propertyList = new ArrayList<>();
            //实体包类列表
            Field[] fields = getAllFields(entity.clazz, entityOption);
            for (Field field : fields) {
                //跳过实体包类
                if (isCompositProperty(field.getType(), entityOption)) {
                    if (!entity.compositFieldMap.containsKey(field.getType().getName())) {
                        entity.compositFieldMap.put(field.getType().getName(), new ArrayList<>());
                    }
                    entity.compositFieldMap.get(field.getType().getName()).add(field.getName());
                    continue;
                }
                Property property = getProperty(field, entity, typeFieldMapping, entityOption);
                propertyList.add(property);
            }
            entity.properties = propertyList;
            getEntity(entity);
            if ("H2".equalsIgnoreCase(entityOption.databaseProvider.name())) {
                entity.tableName = entity.tableName.toUpperCase();
                for (Property property : entity.properties) {
                    property.column = property.column.toUpperCase();
                }
            }
        }
        return entityMap;
    }

    /**
     * 扫描实体类包
     *
     * @param entityMap    实体类映射
     * @param entityOption 实体配置项
     */
    private static void getPackageClass(Map<String, Entity> entityMap, EntityOption entityOption) {
        //扫描实体类包
        for (String packageName : entityOption.packageNameMap.keySet()) {
            List<Class> packageClassList = scanPackageClassList(packageName, entityOption);
            for (Class packageClass : packageClassList) {
                Entity entity = new Entity();
                if (packageClass.getDeclaredAnnotation(TableName.class) != null) {
                    entity.tableName = ((TableName) packageClass.getDeclaredAnnotation(TableName.class)).value();
                } else if ((packageName.length() + packageClass.getSimpleName().length() + 1) == packageClass.getName().length()) {
                    entity.tableName = entityOption.packageNameMap.get(packageName) + StringUtil.camel2Underline(packageClass.getSimpleName());
                } else {
                    String prefix = packageClass.getName().substring(packageName.length() + 1, packageClass.getName().lastIndexOf(".")).replace(".", "_");
                    entity.tableName = entityOption.packageNameMap.get(packageName) + prefix + "@" + StringUtil.camel2Underline(packageClass.getSimpleName());
                }
                entity.clazz = packageClass;
                entityMap.put(packageClass.getName(), entity);
            }
        }
    }

    /**
     * 扫描指定实体类
     *
     * @param entityMap      实体类
     * @param entityClassMap 用户
     */
    private static void getEntityClassMap(Map<String, Entity> entityMap, Map<Class, String> entityClassMap) {
        for (Class entityClass : entityClassMap.keySet()) {
            Entity entity = new Entity();
            if (entityClass.getDeclaredAnnotation(TableName.class) != null) {
                entity.tableName = ((TableName) entityClass.getDeclaredAnnotation(TableName.class)).value();
            } else if (entityClassMap.get(entityClass).isEmpty()) {
                entity.tableName = StringUtil.camel2Underline(entityClass.getSimpleName());
            } else {
                entity.tableName = entityClassMap.get(entityClass) + "@" + StringUtil.camel2Underline(entityClass.getSimpleName());
            }
            entity.clazz = entityClass;
            entityMap.put(entityClass.getName(), entity);
        }
    }

    /**
     * 提取属性信息
     */
    private static Property getProperty(Field field, Entity entity, Map<String, String> typeFieldMapping, EntityOption entityOption) {
        Property property = new Property();
        if (null != field.getAnnotation(ColumnName.class)) {
            property.column = field.getAnnotation(ColumnName.class).value();
        } else {
            property.column = StringUtil.camel2Underline(field.getName());
        }
        property.name = field.getName();
        property.columnLabel = property.name;
        property.clazz = field.getType();
        property.className = field.getType().getName();
        if (null != field.getAnnotation(ColumnType.class)) {
            property.columnType = field.getAnnotation(ColumnType.class).value();
        } else if (typeFieldMapping.containsKey(property.className) && !typeFieldMapping.get(property.className).isEmpty()) {
            property.columnType = typeFieldMapping.get(property.className);
        } else {
            throw new IllegalArgumentException("指定字段无法自动匹配数据库类型!请使用@ColumnType注解手动指定!类名:" + field.getDeclaringClass().getName() + ",字段名:" + field.getName());
        }
        if (property.columnType.contains("(") && property.columnType.contains(")")) {
            String lengthString = property.columnType.substring(property.columnType.indexOf("(") + 1, property.columnType.indexOf(")"));
            if (lengthString.matches("\\d+")) {
                property.length = Integer.parseInt(lengthString);
                property.columnType = property.columnType.substring(0, property.columnType.indexOf("("));
            }
        }
        Constraint constraint = field.getDeclaredAnnotation(Constraint.class);
        if (null != constraint) {
            property.notNull = constraint.notNull();
            if (null != property.check) {
                if (!property.check.isEmpty() && !property.check.contains("(")) {
                    property.check = "(" + property.check + ")";
                }
                property.check = property.check.replace("#{" + property.name + "}", property.column);
                //TODO 后续看看能不能去掉check属性
                property.escapeCheck = property.check.replace(property.column, entityOption.databaseProvider.escape(property.column));
            }
            property.defaultValue = constraint.defaultValue();
        }
        Id id = field.getDeclaredAnnotation(Id.class);
        if (null != id) {
            property.id = true;
            property.strategy = id.strategy();
        }
        TableField tableField = field.getDeclaredAnnotation(TableField.class);
        if (null != tableField) {
            if (!tableField.function().isEmpty()) {
                String databaseName = entityOption.databaseProvider.name();
                if("sqlite".equalsIgnoreCase(databaseName)
                        ||"h2".equalsIgnoreCase(databaseName)
                        ||"sqlserver".equalsIgnoreCase(databaseName)
                ){
                    logger.warn("sqlite,h2,sqlserver数据库不支持指定function属性!");
                }else{
                    property.function = tableField.function().replace("#{" + property.name + "}", "?");
                }
            }
            property.createdAt = tableField.createdAt();
            property.updateAt = tableField.updatedAt();
        }
        List<Index> indexList = new ArrayList<>();
        if (null != field.getDeclaredAnnotation(Index.class)) {
            indexList.add(field.getDeclaredAnnotation(Index.class));
        }
        Indexes indexes = field.getDeclaredAnnotation(Indexes.class);
        if (null != indexes && indexes.value().length > 0) {
            indexList.addAll(Arrays.asList(indexes.value()));
        }
        for (Index index : indexList) {
            IndexField indexField = new IndexField();
            indexField.tableName = entity.tableName;
            indexField.indexType = index.indexType();
            if (!index.indexName().isEmpty()) {
                indexField.indexName = index.indexName();
            } else {
                indexField.indexName = entity.tableName + "_" + indexField.indexType.name().toLowerCase() + "_" + property.column;
            }
            indexField.using = index.using();
            indexField.comment = index.comment();
            indexField.columns.add(property.column);
            entity.indexFieldList.add(indexField);
        }
        if (null != field.getDeclaredAnnotation(Comment.class)) {
            property.comment = field.getDeclaredAnnotation(Comment.class).value();
        }
        property.foreignKey = field.getDeclaredAnnotation(ForeignKey.class);
        if (property.id) {
            entity.id = property;
            property.notNull = true;
            property.comment = "自增id";
            //@Id注解生成策略为默认值又在全局指定里Id生成策略则使用全局策略
            if (property.strategy == IdStrategy.AutoIncrement && null != entityOption.idStrategy) {
                property.strategy = entityOption.idStrategy;
            }
        }
        if (null != property.foreignKey) {
            entity.foreignKeyProperties.add(property);
        }
        property.entity = entity;
        return property;
    }

    private static void getEntity(Entity entity) {
        Comment comment = getFirstAnnotation(entity.clazz, Comment.class);
        if (null != comment) {
            entity.comment = comment.value();
        }
        List<CompositeIndex> compositeIndexList = new ArrayList<>();
        CompositeIndex compositeIndexAnno = getFirstAnnotation(entity.clazz, CompositeIndex.class);
        if (null != compositeIndexAnno) {
            compositeIndexList.add(compositeIndexAnno);
        }
        CompositeIndexes compositeIndexs = getFirstAnnotation(entity.clazz, CompositeIndexes.class);
        if (null != compositeIndexs) {
            compositeIndexList.addAll(Arrays.asList(compositeIndexs.value()));
        }
        if (compositeIndexList.size() > 0) {
            StringBuilder builder = new StringBuilder();
            for (CompositeIndex compositeIndex : compositeIndexList) {
                if (compositeIndex.columns().length == 0) {
                    continue;
                }
                IndexField indexField = new IndexField();
                indexField.tableName = entity.tableName;
                indexField.indexType = compositeIndex.indexType();
                indexField.using = compositeIndex.using();
                for (String column : compositeIndex.columns()) {
                    indexField.columns.add(entity.getColumnNameByFieldName(column));
                }
                indexField.comment = compositeIndex.comment();
                if (!compositeIndex.indexName().isEmpty()) {
                    indexField.indexName = compositeIndex.indexName();
                } else {
                    builder.setLength(0);
                    for (String column : indexField.columns) {
                        builder.append(column + ",");
                    }
                    builder.deleteCharAt(builder.length() - 1);
                    indexField.indexName = entity.tableName + "_" + indexField.indexType.name().toLowerCase() + "_" + builder.toString();
                }
                entity.indexFieldList.add(indexField);
            }
        }
        UniqueField uniqueField = getFirstAnnotation(entity.clazz, UniqueField.class);
        if (null != uniqueField) {
            for (String column : uniqueField.columns()) {
                Property property = entity.getPropertyByFieldName(column);
                if (null == property) {
                    throw new IllegalArgumentException("UniqueField注解参数无法匹配字段!类:" + entity.clazz.getName() + ",字段:" + column);
                }
                entity.uniqueProperties.add(property);
            }
        }
    }

    /**
     * 扫描指定包下的类
     *
     * @param packageName  包名
     * @param entityOption 实体配置项
     */
    private static List<Class> scanPackageClassList(String packageName, EntityOption entityOption) {
        String packageNamePath = packageName.replace(".", "/");
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        URL url = classLoader.getResource(packageNamePath);
        if (url == null) {
            logger.warn("包路径不存在,包名:{}", packageName);
            return new ArrayList<>();
        }
        logger.debug("准备扫描实体类包,包名:{}", packageName);
        List<String> classNameList = new ArrayList<>();
        try {
            switch (url.getProtocol()) {
                case "file": {
                    File file = new File(url.getFile());
                    //TODO 对于有空格或者中文路径会无法识别
                    if (!file.isDirectory()) {
                        throw new IllegalArgumentException("包名不是合法的文件夹!" + url.getFile());
                    }
                    String indexOfString = packageName.replace(".", "/");
                    Files.walkFileTree(file.toPath(), new SimpleFileVisitor<Path>() {
                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                            File f = file.toFile();
                            if (f.getName().endsWith(".class")) {
                                String path = f.getAbsolutePath().replace("\\", "/");
                                int startIndex = path.indexOf(indexOfString);
                                String className = path.substring(startIndex, path.length() - 6).replace("/", ".");
                                classNameList.add(className);
                            }
                            return FileVisitResult.CONTINUE;
                        }
                    });
                }
                break;
                case "jar": {
                    JarURLConnection jarURLConnection = (JarURLConnection) url.openConnection();
                    if (null != jarURLConnection) {
                        JarFile jarFile = jarURLConnection.getJarFile();
                        if (null != jarFile) {
                            Enumeration<JarEntry> jarEntries = jarFile.entries();
                            while (jarEntries.hasMoreElements()) {
                                JarEntry jarEntry = jarEntries.nextElement();
                                String jarEntryName = jarEntry.getName();
                                if (jarEntryName.contains(packageNamePath) && jarEntryName.endsWith(".class")) { //是否是类,是类进行加载
                                    String className = jarEntryName.substring(0, jarEntryName.lastIndexOf(".")).replaceAll("/", ".");
                                    classNameList.add(className);
                                }
                            }
                        }
                    }
                }
                break;
            }
            List<Class> classList = new ArrayList<>(classNameList.size());
            for (String className : classNameList) {
                Class clazz = classLoader.loadClass(className);
                if (shouldIgnoreClass(clazz, entityOption)) {
                    continue;
                }
                classList.add(clazz);
            }
            return classList;
        } catch (Exception e) {
            throw new RuntimeException("读取实体类信息时发生异常!", e);
        }
    }

    /**
     * 是否需要忽略该类
     */
    private static boolean shouldIgnoreClass(Class clazz, EntityOption entityOption) {
        if (clazz.isEnum()) {
            return true;
        }
        if (clazz.getAnnotation(Ignore.class) != null) {
            return true;
        }
        //根据类过滤
        if (null != entityOption.ignoreClassList) {
            for (Class _clazz : entityOption.ignoreClassList) {
                if (_clazz.getName().equals(clazz.getName())) {
                    return true;
                }
            }
        }
        //根据包名过滤
        if (null != entityOption.ignorePackageNameList) {
            for (String ignorePackageName : entityOption.ignorePackageNameList) {
                if (clazz.getName().contains(ignorePackageName)) {
                    return true;
                }
            }
        }
        //如果用户已经指定了该实体类,则跳过
        for (Class _clazz : entityOption.entityClassMap.keySet()) {
            if (_clazz.getName().equals(clazz.getName())) {
                return true;
            }
        }
        //执行用户判断逻辑
        if (null != entityOption.ignorePredicate) {
            if (entityOption.ignorePredicate.test(clazz)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 获得该类所有字段(包括父类字段)
     *
     * @param clazz 类
     */
    private static Field[] getAllFields(Class clazz, EntityOption entityOption) {
        logger.trace("获取指定类的所有字段!类名:{}", clazz.getName());
        List<Field> fieldList = new ArrayList<>();
        Class tempClass = clazz;
        while (null != tempClass) {
            Field[] fields = tempClass.getDeclaredFields();
            Field.setAccessible(fields, true);
            for (Field field : fields) {
                if (Modifier.isStatic(field.getModifiers()) || Modifier.isFinal(field.getModifiers()) || Modifier.isTransient(field.getModifiers())) {
                    logger.trace("跳过常量或静态变量,该字段被static或者final修饰!字段名:{}", field.getName());
                    continue;
                }
                if (field.getDeclaredAnnotation(Ignore.class) != null) {
                    logger.trace("跳过实体属性,该属性被Ignore注解修饰!字段名:{}", field.getName());
                    continue;
                }
                //跳过List类型和数组类型
                if (field.getType().isArray() || (!field.getType().isPrimitive() && isCollection(field.getType()))) {
                    logger.trace("跳过集合类型!字段名:{}", field.getName());
                    continue;
                }
                if (shouldIgnoreClass(field.getType(), entityOption)) {
                    logger.trace("跳过用户指定过滤条件!字段名:{}", field.getName());
                    continue;
                }
                field.setAccessible(true);
                fieldList.add(field);
            }
            tempClass = tempClass.getSuperclass();
            if (null != tempClass && "java.lang.Object".equals(tempClass.getName())) {
                break;
            }
        }
        return fieldList.toArray(new Field[0]);
    }

    /**
     * 判断是否为集合类型
     *
     * @param clazz 类
     */
    private static boolean isCollection(Class clazz) {
        Stack<Class[]> stack = new Stack<>();
        stack.push(clazz.getInterfaces());
        while (!stack.isEmpty()) {
            Class[] classes = stack.pop();
            for (Class _clazz : classes) {
                if (_clazz.getName().equals(Collection.class.getName())) {
                    return true;
                }
                Class[] subClasses = _clazz.getInterfaces();
                if (null != subClasses && subClasses.length > 0) {
                    stack.push(subClasses);
                }
            }
        }
        return false;
    }

    /**
     * 判断是否是实体包类
     **/
    private static boolean isCompositProperty(Class clazz, EntityOption entityOption) {
        Set<String> packageNameSet = entityOption.packageNameMap.keySet();
        for (String packageName : packageNameSet) {
            if (clazz.getName().contains(packageName)) {
                return true;
            }
        }
        Set<Class> classSet = entityOption.entityClassMap.keySet();
        for (Class c : classSet) {
            if (c.getName().equals(clazz.getName())) {
                return true;
            }
        }
        return false;
    }

    /**
     * 自下而上查找注解
     *
     * @param clazz 类
     */
    private static <T> T getFirstAnnotation(Class clazz, Class<T> annotation) {
        T annotation1 = null;
        while (null != clazz && null == annotation1) {
            annotation1 = (T) clazz.getDeclaredAnnotation(annotation);
            clazz = clazz.getSuperclass();
        }
        return annotation1;
    }

}
