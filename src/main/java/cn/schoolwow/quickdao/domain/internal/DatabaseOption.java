package cn.schoolwow.quickdao.domain.internal;

import cn.schoolwow.quickdao.annotation.IdStrategy;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.external.StatementListener;
import cn.schoolwow.quickdao.domain.external.generator.IDGenerator;
import cn.schoolwow.quickdao.domain.external.generator.SnowflakeIdGenerator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public class DatabaseOption {
    /**
     * 是否启动时自动建表
     */
    public boolean automaticCreateTable = true;

    /**
     * 是否自动新增属性
     */
    public boolean automaticCreateProperty = true;

    /**
     * 是否自动删除多余表和属性(和实体类对比)
     */
    public boolean automaticDeleteTableAndProperty;

    /**
     * 是否开启外键约束
     */
    public boolean openForeignKey;

    /**
     * 全局Id生成策略
     */
    public IdStrategy idStrategy;

    /**
     * Id生成器实例
     * 默认生成器为雪花算法生成器
     */
    public IDGenerator idGenerator = new SnowflakeIdGenerator();

    /**
     * 单次批量插入个数
     */
    public int perBatchCommit = 1000;

    /**
     * 插入时设置数据
     */
    public Function<Property, Object> insertColumnValueFunction;

    /**
     * 更新时设置数据
     */
    public Function<Property, Object> updateColumnValueFunction;

    /**
     * 虚表列表
     */
    public List<String> virtualTableNameList = new ArrayList<>(Arrays.asList("dual"));

    /**
     * SQL执行监听器
     */
    public List<StatementListener> statementListener = new ArrayList<>();

}
