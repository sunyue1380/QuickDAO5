package cn.schoolwow.quickdao.provider;

import cn.schoolwow.quickdao.dao.dcl.AbstractDabaseControl;
import cn.schoolwow.quickdao.dao.ddl.AbstractDatabaseDefinition;
import cn.schoolwow.quickdao.dao.dml.AbstractDatabaseManipulation;
import cn.schoolwow.quickdao.dao.dql.condition.Condition;
import cn.schoolwow.quickdao.dao.dql.subCondition.SubCondition;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.internal.Query;
import cn.schoolwow.quickdao.domain.internal.SubQuery;

import java.util.Map;

/**
 * 数据库提供者
 */
public interface DatabaseProvider {
    /**
     * 获取数据库控制语言实例
     */
    AbstractDabaseControl getDatabaseControlInstance(QuickDAOConfig quickDAOConfig);

    /**
     * 获取数据库定义语言实例
     */
    AbstractDatabaseDefinition getDatabaseDefinitionInstance(QuickDAOConfig quickDAOConfig);

    /**
     * 获取数据库操纵语言实例
     */
    AbstractDatabaseManipulation getDatabaseManipulationInstance(QuickDAOConfig quickDAOConfig);

    /**
     * 获取Condition实例
     */
    Condition getConditionInstance(Query query);

    /**
     * 获取SubCondition实例
     */
    SubCondition getSubConditionInstance(SubQuery subQuery);

    /**
     * 返回注释语句
     */
    String comment(String comment);

    /**
     * 转义表,列等
     */
    String escape(String value);

    /**
     * 是否返回自增id
     */
    boolean returnGeneratedKeys();

    /**
     * 获取默认Java类型与数据库类型映射关系表
     */
    Map<String, String> getTypeFieldMapping();

    /**
     * 数据库类型名称
     */
    String name();
}
