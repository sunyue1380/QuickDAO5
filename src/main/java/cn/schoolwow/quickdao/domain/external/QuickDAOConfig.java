package cn.schoolwow.quickdao.domain.external;

import cn.schoolwow.quickdao.dao.DAO;
import cn.schoolwow.quickdao.domain.internal.DatabaseOption;
import cn.schoolwow.quickdao.domain.internal.EntityOption;
import cn.schoolwow.quickdao.provider.DatabaseProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;


/**
 * 数据源访问配置选项
 */
public class QuickDAOConfig {
    private Logger logger = LoggerFactory.getLogger(QuickDAOConfig.class);

    /**
     * 数据源
     */
    public DataSource dataSource;

    /**
     * 实体类选项
     */
    public EntityOption entityOption = new EntityOption();

    /**
     * 查询返回结果类型转换
     */
    public Function<Property, Class> queryColumnTypeMapping;

    /**
     * 数据库选项
     */
    public DatabaseOption databaseOption = new DatabaseOption();

    /**
     * 扫描后的实体类信息
     */
    public Map<String, Entity> entityMap;

    /**
     * 数据库表信息缓存
     */
    public final Map<String, Entity> databaseTableCache = new ConcurrentHashMap<>();

    /**
     * SQL语句缓存
     */
    public final ConcurrentHashMap<String, String> statementCache = new ConcurrentHashMap();

    /**
     * 数据库提供者
     */
    public DatabaseProvider databaseProvider;

    /**
     * dao对象,用于返回
     */
    public DAO dao;

    /**
     * 根据类名获取实体类信息
     */
    public Entity getEntityByClassName(String className) {
        if (this.entityMap.containsKey(className)) {
            return this.entityMap.get(className);
        }
        throw new IllegalArgumentException("扫描实体类列表中不包含该实体类!类名:" + className);
    }

    /**
     * 根据表名获取数据库信息
     */
    public Entity getDatabaseEntityByTableName(String tableName) {
        if (!databaseTableCache.containsKey(tableName)) {
            Entity entity = dao.getDatabaseEntity(tableName);
            if (null == entity) {
                return null;
            }
            databaseTableCache.put(tableName, entity);
        }
        return databaseTableCache.get(tableName);
    }

    /**
     * 删除缓存数据表信息
     */
    public void deleteDatabaseEntityCache(String tableName) {
        Iterator<Map.Entry<String, String>> entryIterator = statementCache.entrySet().iterator();
        while (entryIterator.hasNext()) {
            Map.Entry<String, String> entry = entryIterator.next();
            if (entry.getKey().contains("_" + tableName + "_")) {
                logger.trace("删除SQL语句缓存:{}", entry.getKey());
                entryIterator.remove();
            }
        }
        logger.trace("删除数据库表缓存,表名:{}", tableName);
        databaseTableCache.remove(tableName);
    }

    @Override
    public String toString() {
        return "\n{\n" +
                "数据源:" + dataSource + "\n"
                + "扫描后的实体类信息个数:" + entityMap.size() + "\n"
                + "SQL语句缓存个数:" + statementCache.size() + "\n"
                + "数据库类型:" + databaseProvider.name() + "\n"
                + "}\n";
    }
}
