package cn.schoolwow.quickdao.dao.ddl;

import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.IndexField;
import cn.schoolwow.quickdao.domain.external.Property;

import java.util.List;

/**
 * 数据定义语言
 */
public interface DatabaseDefinition {
    /**
     * 表是否存在
     *
     * @param clazz 实体类
     */
    boolean hasTable(Class clazz);

    /**
     * 表是否存在
     *
     * @param tableName 表名
     */
    boolean hasTable(String tableName);

    /**
     * 列是否存在
     *
     * @param tableName  表名
     * @param columnName 列名
     */
    boolean hasColumn(String tableName, String columnName);

    /**
     * 获取数据库所有表名
     */
    List<String> getTableNameList();

    /**
     * 获取数据库表列表
     */
    List<Entity> getDatabaseEntityList();

    /**
     * 获取数据库表列表
     *
     * @param tableName 表名
     */
    Entity getDatabaseEntity(String tableName);

    /**
     * 获取表字段列表
     *
     * @param clazz 实体类
     */
    List<Property> getPropertyList(Class clazz);

    /**
     * 获取表字段列表
     *
     * @param tableName 表名
     */
    List<Property> getPropertyList(String tableName);

    /**
     * 获取表字段
     *
     * @param clazz      实体类
     * @param columnName 字段名称
     */
    Property getProperty(Class clazz, String columnName);

    /**
     * 获取表指定字段
     *
     * @param tableName  表名
     * @param columnName 字段名称
     */
    Property getProperty(String tableName, String columnName);

    /**
     * 建表
     */
    void create(Class clazz);

    /**
     * 建表
     */
    void create(Entity entity);

    /**
     * 删表
     */
    void dropTable(Class clazz);

    /**
     * 删表
     */
    void dropTable(String tableName);

    /**
     * 重建表
     */
    void rebuild(Class clazz);

    /**
     * 重建表
     */
    void rebuild(String tableName);

    /**
     * 新增列
     *
     * @param tableName 表名
     * @param property  字段属性
     * @return 修改列
     */
    Property createColumn(String tableName, Property property);

    /**
     * 修改列
     *
     * @param property 列信息
     */
    void alterColumn(Property property);

    /**
     * 删除列
     *
     * @param tableName  表名
     * @param columnName 列名
     */
    Property dropColumn(String tableName, String columnName);

    /**
     * 索引是否存在
     *
     * @param tableName 表名
     * @param indexName 索引名称
     */
    boolean hasIndex(String tableName, String indexName);

    /**
     * 约束否存在
     *
     * @param tableName      表名
     * @param constraintName 约束名称
     */
    boolean hasConstraint(String tableName, String constraintName);

    /**
     * 获取索引列表
     *
     * @param tableName 表名
     */
    List<IndexField> getIndexField(String tableName);

    /**
     * 新增索引
     *
     * @param indexField 索引信息
     */
    void createIndex(IndexField indexField);

    /**
     * 删除索引
     *
     * @param tableName 表名
     * @param indexName 索引名称
     */
    void dropIndex(String tableName, String indexName);

    /**
     * 创建外键约束
     *
     * @param property 数据库列
     */
    void createForeignKey(Property property);

    /**
     * 是否开启外键约束检查
     */
    void enableForeignConstraintCheck(boolean enable);

}
