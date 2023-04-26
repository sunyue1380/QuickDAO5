package cn.schoolwow.quickdao.dao.dml;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.Collection;

/**
 * 负责数据增删改查操作
 */
public interface DatabaseManipulation {
    /**
     * 是否返回自增id
     */
    DatabaseManipulation returnGeneratedKeys(boolean returnGeneratedKeys);

    /**
     * 是否使用批处理
     */
    DatabaseManipulation batch(boolean batch);

    /**
     * 每次最大更新个数
     */
    DatabaseManipulation perBatchCount(int perBatchCount);

    /**
     * 是否只插入/更新部分字段
     */
    DatabaseManipulation partColumn(String... fieldNames);

    /**
     * 指定唯一字段
     */
    DatabaseManipulation uniqueFieldNames(String... uniqueFieldNames);

    /**
     * 插入JSON对象
     *
     * @param tableName 表名
     * @param instance  记录
     */
    int insert(String tableName, JSONObject instance);

    /**
     * 插入JSON列表
     *
     * @param tableName 表名
     * @param instances     数据列表
     */
    int insert(String tableName, JSONArray instances);

    /**
     * 忽略插入JSONObject
     * 不存在则插入,存在则忽略
     *
     * @param tableName 表名
     * @param instance  记录
     */
    int insertIgnore(String tableName, JSONObject instance);

    /**
     * 忽略插入JSONArray
     * 不存在则插入,存在则忽略
     *
     * @param tableName 表名
     * @param instances 记录
     */
    int insertIgnore(String tableName, JSONArray instances);

    /**
     * 更新实例
     *
     * @param tableName 表名
     * @param instance 记录
     */
    int update(String tableName, JSONObject instance);

    /**
     * 更新实例
     *
     * @param tableName 表名
     * @param instances 记录
     */
    int update(String tableName, JSONArray instances);

    /**
     * 删除实例
     *
     * @param tableName 表名
     * @param instance 记录
     */
    int delete(String tableName, JSONObject instance);

    /**
     * 删除实例
     *
     * @param tableName 表名
     * @param instances 记录
     */
    int delete(String tableName, JSONArray instances);

    /**
     * 插入对象
     *
     * @param instance 待保存对象
     */
    int insert(Object instance);

    /**
     * 插入对象数组
     *
     * @param instances 待保存对象数组
     */
    int insert(Object[] instances);

    /**
     * 插入对象
     *
     * @param instanceCollection 插入对象集合
     */
    int insert(Collection instanceCollection);

    /**
     * 不存在则插入,存在则忽略
     *
     * @param instance 插入对象
     */
    int insertIgnore(Object instance);

    /**
     * 不存在则插入,存在则忽略
     *
     * @param instances 插入对象数组
     */
    int insertIgnore(Object[] instances);

    /**
     * 不存在则插入,存在则忽略
     *
     * @param instanceCollection 待保存对象集合
     */
    int insertIgnore(Collection instanceCollection);

    /**
     * 更新对象
     * 若对象有唯一性约束,则根据唯一性约束更新,否则根据id更新
     *
     * @param instance 待更新对象
     */
    int update(Object instance);

    /**
     * 更新对象
     * 若对象有唯一性约束,则根据唯一性约束更新,否则根据id更新
     *
     * @param instances 待更新对象数组
     */
    int update(Object[] instances);

    /**
     * 更新对象
     * 若对象有唯一性约束,则根据唯一性约束更新,否则根据id更新
     *
     * @param instanceCollection 待更新对象集合
     */
    int update(Collection instanceCollection);

    /**
     * <p>保存对象</p>
     * <ul>
     *     <li>若对象id不存在,则直接插入该对象</li>
     *     <li>若对象id存在,则判断该对象是否有唯一性约束,若有则根据唯一性约束更新</li>
     *     <li>若该对象无唯一性约束,则根据id更新</li>
     * </ul>
     *
     * @param instance 待保存对象
     */
    int save(Object instance);

    /**
     * <p>保存对象数组</p>
     * <ul>
     *     <li>若对象id不存在,则直接插入该对象</li>
     *     <li>若对象id存在,则判断该对象是否有唯一性约束,若有则根据唯一性约束更新</li>
     *     <li>若该对象无唯一性约束,则根据id更新</li>
     * </ul>
     *
     * @param instances 待保存对象数组
     */
    int save(Object[] instances);

    /**
     * <p>保存对象数组</p>
     * <ul>
     *     <li>若对象id不存在,则直接插入该对象</li>
     *     <li>若对象id存在,则判断该对象是否有唯一性约束,若有则根据唯一性约束更新</li>
     *     <li>若该对象无唯一性约束,则根据id更新</li>
     * </ul>
     *
     * @param instanceCollection 待保存对象集合
     */
    int save(Collection instanceCollection);

    /**
     * 删除对象
     * <p>首先根据实体类唯一性约束删除,若无唯一性约束则根据id删除</p>
     *
     * @param instance 待删除对象
     */
    int delete(Object instance);

    /**
     * 删除对象数组
     * <p>首先根据实体类唯一性约束删除,若无唯一性约束则根据id删除</p>
     *
     * @param instances 待删除对象数组
     */
    int delete(Object[] instances);

    /**
     * 删除对象
     * <p>首先根据实体类唯一性约束删除,若无唯一性约束则根据id删除</p>
     *
     * @param instanceCollection 删除对象集合
     */
    int delete(Collection instanceCollection);

    /**
     * 删除表中所有记录
     *
     * @param clazz 实体类
     */
    int truncate(Class clazz);

    /**
     * 删除表中所有记录
     *
     * @param tableName 表名
     */
    int truncate(String tableName);
}