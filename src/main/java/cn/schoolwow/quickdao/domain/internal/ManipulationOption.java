package cn.schoolwow.quickdao.domain.internal;

import cn.schoolwow.quickdao.dao.ConnectionExecutor;

import java.util.HashSet;
import java.util.Set;


/**
 * 操作语句选项
 */
public class ManipulationOption {
    /**
     * 是否返回自增id
     */
    public boolean returnGeneratedKeys = true;

    /**
     * 是否启用批处理(默认启用)
     */
    public boolean batch = true;

    /**
     * 每次最大更新个数
     */
    public int perBatchCount = 10000;

    /**
     * 部分更新字段
     */
    public Set<String> partColumnSet = new HashSet<>();

    /**
     * 唯一字段
     */
    public Set<String> uniqueFieldNames = new HashSet<>();

    /**
     * 数据库连接执行器
     * */
    public ConnectionExecutor connectionExecutor;
}
