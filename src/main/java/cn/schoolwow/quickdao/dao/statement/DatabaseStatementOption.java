package cn.schoolwow.quickdao.dao.statement;

import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.internal.Query;

import java.util.HashSet;
import java.util.Set;


/**
 * 数据库语句选项
 */
public class DatabaseStatementOption {
    /**
     * 是否返回自增id
     */
    public Boolean returnGeneratedKeys;

    /**
     * 是否启用批处理
     */
    public boolean batch;

    /**
     * 每次最大更新个数
     */
    public int perBatchCount = 10000;

    /**
     * 部分更新字段
     */
    public Set<String> partColumnSet = new HashSet<>();

    /**
     * 实体类
     */
    public Entity entity;

    /**
     * 指定字段名称
     */
    public String columnName;

    /**
     * 复杂查询
     */
    public Query query;

}
