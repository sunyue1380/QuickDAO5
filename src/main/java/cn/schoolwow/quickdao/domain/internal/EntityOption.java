package cn.schoolwow.quickdao.domain.internal;

import cn.schoolwow.quickdao.annotation.IdStrategy;
import cn.schoolwow.quickdao.provider.DatabaseProvider;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public class EntityOption {
    /**
     * 待扫描包名
     */
    public Map<String, String> packageNameMap = new HashMap<>();

    /**
     * 待扫描类
     */
    public Map<Class, String> entityClassMap = new HashMap<>();

    /**
     * 要忽略的类
     */
    public List<Class> ignoreClassList;

    /**
     * 要忽略的包名
     */
    public List<String> ignorePackageNameList;

    /**
     * 函数式接口过滤类,返回true表示过滤,false保留
     */
    public Predicate<Class> ignorePredicate;

    /**
     * 全局Id生成策略
     */
    public IdStrategy idStrategy;

    /**
     * 数据库提供者
     */
    public DatabaseProvider databaseProvider;
}
