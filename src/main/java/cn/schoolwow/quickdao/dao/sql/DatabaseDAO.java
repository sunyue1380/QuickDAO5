package cn.schoolwow.quickdao.dao.sql;

import cn.schoolwow.quickdao.domain.internal.SFunction;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.Collection;
import java.util.List;

public interface DatabaseDAO {
    /**
     * 实例对象是否存在
     *
     * @param instance 实例对象
     */
    boolean exist(Object instance);

    /**
     * 是否数据库中存在任意一个示例对象数组内的对象
     *
     * @param instances 实例对象数组
     */
    boolean existAny(Object... instances);

    /**
     * 是否数据库中存在示例对象数组内的所有对象
     *
     * @param instances 实例对象数组
     */
    boolean existAll(Object... instances);

    /**
     * 是否数据库中存在任意一个示例对象数组内的对象
     *
     * @param instances 实例对象数组
     */
    boolean existAny(Collection instances);

    /**
     * 是否数据库中存在示例对象数组内的所有对象
     *
     * @param instances 实例对象数组
     */
    boolean existAll(Collection instances);

    /**
     * 根据id查询实例
     *
     * @param clazz 实例类对象
     * @param id    待查询id值
     */
    <T> T fetch(Class<T> clazz, long id);

    /**
     * 根据属性查询单个记录
     *
     * @param clazz 实例类对象
     * @param field 指定字段名
     * @param value 指字段值
     */
    <T> T fetch(Class<T> clazz, String field, Object value);

    /**
     * 根据属性查询多个记录
     *
     * @param clazz     实例类对象
     * @param fieldName 指定字段名
     * @param value     指字段值
     */
    <T> List<T> fetchList(Class<T> clazz, String fieldName, Object value);

    /**
     * 根据属性查询单个记录
     *
     * @param clazz 实例类对象
     * @param field 指定字段名
     * @param value 指字段值
     */
    <T> T fetch(Class<T> clazz, SFunction<T, ?> field, Object value);

    /**
     * 根据属性查询多个记录
     *
     * @param clazz 实例类对象
     * @param field 指定字段名
     * @param value 指字段值
     */
    <T> List<T> fetchList(Class<T> clazz, SFunction<T, ?> field, Object value);

    /**
     * 根据属性查询单个记录
     *
     * @param tableName 表名
     * @param field     指定字段名
     * @param value     指字段值
     */
    JSONObject fetch(String tableName, String field, Object value);

    /**
     * 根据属性查询多个记录
     *
     * @param tableName  表名
     * @param columnName 指定字段名
     * @param value      指字段值
     */
    JSONArray fetchList(String tableName, String columnName, Object value);

    /**
     * 执行原生查询语句
     *
     * @param selectSQL  SQL查询语句
     * @param parameters 参数
     */
    JSONArray rawSelect(String selectSQL, Object... parameters);

    /**
     * 根据id删除记录
     *
     * @param id 待删除记录id
     */
    int delete(Class clazz, long id);

    /**
     * 根据id删除记录
     *
     * @param id 待删除记录id
     */
    int delete(Class clazz, String id);

    /**
     * 根据指定字段值删除对象
     *
     * @param clazz 实体类对象,对应数据库中的一张表
     * @param field 指定字段名
     * @param value 指定字段值
     */
    int delete(Class clazz, String field, Object value);

    /**
     * 根据指定字段值删除对象
     *
     * @param tableName  数据库表名
     * @param columnName 列名
     * @param value      字段值
     */
    int delete(String tableName, String columnName, Object value);

    /**
     * 根据指定字段值删除对象
     *
     * @param clazz 实体类对象,对应数据库中的一张表
     * @param field 指定字段名
     * @param value 指定字段值
     */
    <T> int delete(Class<T> clazz, SFunction<T, ?> field, Object value);

    /**
     * 清空表
     *
     * @param clazz 实体类
     */
    int clear(Class clazz);

    /**
     * 执行原生更新语句
     *
     * @param updateSQL  SQL更新语句
     * @param parameters 参数
     */
    int rawUpdate(String updateSQL, Object... parameters);
}
