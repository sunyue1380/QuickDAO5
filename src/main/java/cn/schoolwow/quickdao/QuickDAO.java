package cn.schoolwow.quickdao;

import cn.schoolwow.quickdao.annotation.IdStrategy;
import cn.schoolwow.quickdao.dao.DAO;
import cn.schoolwow.quickdao.dao.DAOInvocationHandler;
import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.external.StatementListener;
import cn.schoolwow.quickdao.domain.external.generator.IDGenerator;
import cn.schoolwow.quickdao.entity.TableDefiner;
import cn.schoolwow.quickdao.entity.TableDefinerImpl;
import cn.schoolwow.quickdao.exception.SQLRuntimeException;
import cn.schoolwow.quickdao.provider.*;
import cn.schoolwow.quickdao.util.DatabaseUtil;
import cn.schoolwow.quickdao.util.EntityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

public class QuickDAO {
    private Logger logger = LoggerFactory.getLogger(QuickDAO.class);
    //数据库提供者列表
    private static List<DatabaseProvider> databaseProviders = new ArrayList<>(Arrays.asList(
            new H2DatabaseProvider(),
            new MariaDBDatabaseProvider(),
            new MySQLDatabaseProvider(),
            new OracleDatabaseProvider(),
            new PostgreDatabaseProvider(),
            new SQLiteDatabaseProvider(),
            new SQLServerDatabaseProvider()
    ));

    private QuickDAOConfig quickDAOConfig = new QuickDAOConfig();

    /**
     * 添加新的数据库提供者
     */
    public static void addDatabaseProvider(DatabaseProvider databaseProvider) {
        databaseProviders.add(databaseProvider);
    }

    /**
     * 新建实例
     */
    public static QuickDAO newInstance() {
        return new QuickDAO();
    }

    private QuickDAO() {
    }

    /**
     * 设置数据库连接池
     *
     * @param dataSource 数据库连接池
     */
    public QuickDAO dataSource(DataSource dataSource) {
        quickDAOConfig.dataSource = dataSource;
        try {
            Connection connection = quickDAOConfig.dataSource.getConnection();
            connection.setAutoCommit(false);
            String jdbcUrl = connection.getMetaData().getURL();
            for (DatabaseProvider databaseProvider : databaseProviders) {
                if (jdbcUrl.contains("jdbc:" + databaseProvider.name())) {
                    quickDAOConfig.databaseProvider = databaseProvider;
                    break;
                }
            }
            if (null == quickDAOConfig.databaseProvider) {
                throw new IllegalArgumentException("不支持的数据库类型!jdbcurl:" + jdbcUrl);
            }
            logger.info("[数据源]类型:{},地址:{}", quickDAOConfig.databaseProvider.name(), jdbcUrl);
            connection.close();
        } catch (Exception e) {
            throw new SQLRuntimeException(e);
        }
        return this;
    }

    /**
     * 待扫描实体类包名,支持嵌套扫描
     *
     * @param packageName 实体类所在包名
     */
    public QuickDAO packageName(String packageName) {
        quickDAOConfig.entityOption.packageNameMap.put(packageName, "");
        return this;
    }

    /**
     * 待扫描实体类包名,支持嵌套扫描
     *
     * @param packageName 实体类所在包名
     * @param prefix      表前缀
     */
    public QuickDAO packageName(String packageName, String prefix) {
        quickDAOConfig.entityOption.packageNameMap.put(packageName, prefix + "_");
        return this;
    }

    /**
     * 待扫描实体类包名,支持嵌套扫描
     *
     * @param entityClasses 实体类
     */
    public QuickDAO entity(Class... entityClasses) {
        for (Class entityClass : entityClasses) {
            quickDAOConfig.entityOption.entityClassMap.put(entityClass, "");
        }
        return this;
    }

    /**
     * 待扫描实体类包名,支持嵌套扫描
     *
     * @param entityClass 实体类
     * @param prefix      表前缀
     */
    public QuickDAO entity(Class entityClass, String prefix) {
        quickDAOConfig.entityOption.entityClassMap.put(entityClass, prefix);
        return this;
    }

    /**
     * 忽略包名
     *
     * @param ignorePackageName 扫描实体类时需要忽略的包
     */
    public QuickDAO ignorePackageName(String ignorePackageName) {
        if (quickDAOConfig.entityOption.ignorePackageNameList == null) {
            quickDAOConfig.entityOption.ignorePackageNameList = new ArrayList<>();
        }
        quickDAOConfig.entityOption.ignorePackageNameList.add(ignorePackageName);
        return this;
    }

    /**
     * 忽略该实体类
     *
     * @param ignoreClass 需要忽略的实体类
     */
    public QuickDAO ignoreClass(Class ignoreClass) {
        if (quickDAOConfig.entityOption.ignoreClassList == null) {
            quickDAOConfig.entityOption.ignoreClassList = new ArrayList<>();
        }
        quickDAOConfig.entityOption.ignoreClassList.add(ignoreClass);
        return this;
    }

    /**
     * 过滤实体类
     *
     * @param ignorePredicate 过滤实体类函数
     */
    public QuickDAO filter(Predicate<Class> ignorePredicate) {
        quickDAOConfig.entityOption.ignorePredicate = ignorePredicate;
        return this;
    }

    /**
     * 是否建立外键约束
     *
     * @param openForeignKey 指定管是否建立外键约束
     */
    public QuickDAO foreignKey(boolean openForeignKey) {
        quickDAOConfig.databaseOption.openForeignKey = openForeignKey;
        return this;
    }

    /**
     * 是否自动建表
     *
     * @param autoCreateTable 指定是否自动建表,默认为true
     */
    public QuickDAO automaticCreateTable(boolean autoCreateTable) {
        quickDAOConfig.databaseOption.automaticCreateTable = autoCreateTable;
        return this;
    }

    /**
     * 是否自动新增属性
     *
     * @param autoCreateProperty 指定是否自动新增字段,默认为true
     */
    public QuickDAO automaticCreateProperty(boolean autoCreateProperty) {
        quickDAOConfig.databaseOption.automaticCreateProperty = autoCreateProperty;
        return this;
    }

    /**
     * 是否自动删除多余表和属性(和实体类对比)
     *
     * @param autoDeleteTableAndProperty 指定是否自动删除多余表和属性(和实体类对比),默认为false
     */
    public QuickDAO automaticDeleteTableAndProperty(boolean autoDeleteTableAndProperty) {
        quickDAOConfig.databaseOption.automaticDeleteTableAndProperty = autoDeleteTableAndProperty;
        return this;
    }

    /**
     * 指定全局Id生成策略
     *
     * @param idStrategy 全局id生成策略
     */
    public QuickDAO idStrategy(IdStrategy idStrategy) {
        quickDAOConfig.databaseOption.idStrategy = idStrategy;
        return this;
    }

    /**
     * 指定id生成器接口实例
     * <p><b>当id字段策略为IdGenerator起作用</b></p>
     *
     * @param idGenerator id生成器实例
     */
    public QuickDAO idGenerator(IDGenerator idGenerator) {
        quickDAOConfig.databaseOption.idGenerator = idGenerator;
        return this;
    }

    /**
     * 指定全局类型转换
     *
     * @param columnTypeMapping 全局类型转换函数
     */
    public QuickDAO columnTypeMapping(Function<Property, Class> columnTypeMapping) {
        quickDAOConfig.queryColumnTypeMapping = columnTypeMapping;
        return this;
    }

    /**
     * 指定单次批量插入个数
     *
     * @param perBatchCommit 单次批量插入个数
     */
    public QuickDAO perBatchCommit(int perBatchCommit) {
        quickDAOConfig.databaseOption.perBatchCommit = perBatchCommit;
        return this;
    }

    /**
     * 插入时设置字段值
     *
     * @param insertColumnValueFunction 插入时设置字段值函数,参数为字段信息,返回值为该字段对应值,若为null则忽略该值
     */
    public QuickDAO insertColumnValueFunction(Function<Property, Object> insertColumnValueFunction) {
        quickDAOConfig.databaseOption.insertColumnValueFunction = insertColumnValueFunction;
        return this;
    }

    /**
     * 更新时设置字段值
     *
     * @param updateColumnValueFunction 更新时设置字段值函数,参数为字段信息,返回值为该字段对应值,若为null则忽略该值
     */
    public QuickDAO updateColumnValueFunction(Function<Property, Object> updateColumnValueFunction) {
        quickDAOConfig.databaseOption.updateColumnValueFunction = updateColumnValueFunction;
        return this;
    }

    /**
     * 指定虚拟表
     *
     * @param virtualTableNames 虚拟表名称
     */
    public QuickDAO virtualTableName(String... virtualTableNames) {
        quickDAOConfig.databaseOption.virtualTableNameList.addAll(Arrays.asList(virtualTableNames));
        return this;
    }

    /**
     * 添加语句监听器
     *
     * @param statementListener 语句监听器
     */
    public QuickDAO statementListener(StatementListener statementListener) {
        quickDAOConfig.databaseOption.statementListener.add(statementListener);
        return this;
    }

    /**
     * 自定义表和列
     */
    public TableDefiner define(Class clazz) {
        if (null == quickDAOConfig.dataSource) {
            throw new IllegalArgumentException("请先调用dataSource方法配置数据源!");
        }
        getEntityMap();
        Entity entity = quickDAOConfig.getEntityByClassName(clazz.getName());
        return new TableDefinerImpl(entity, this);
    }

    public DAO build() {
        if (null == quickDAOConfig.databaseProvider) {
            throw new IllegalArgumentException("请先调用dataSource方法配置数据源!");
        }
        getEntityMap();
        DAOInvocationHandler daoInvocationHandler = new DAOInvocationHandler(quickDAOConfig);
        DAO daoProxy = (DAO) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class<?>[]{DAO.class}, daoInvocationHandler);
        quickDAOConfig.dao = daoProxy;
        if (quickDAOConfig.databaseOption.automaticCreateTable) {
            logger.trace("准备执行自动创建表");
            DatabaseUtil.automaticCreateTable(quickDAOConfig);
        }
        if (quickDAOConfig.databaseOption.automaticCreateProperty) {
            logger.trace("准备执行自动创建表字段");
            DatabaseUtil.automaticCreateProperty(quickDAOConfig);
        }
        if (quickDAOConfig.databaseOption.automaticDeleteTableAndProperty) {
            logger.trace("准备执行自动删除表和字段");
            DatabaseUtil.automaticDeleteTableAndProperty(daoProxy);
        }
        return daoProxy;
    }

    private void getEntityMap() {
        if (null == quickDAOConfig.entityMap || quickDAOConfig.entityMap.isEmpty()) {
            //TODO 后续看看怎么修改
            quickDAOConfig.entityOption.databaseProvider = quickDAOConfig.databaseProvider;
            quickDAOConfig.entityMap = EntityUtil.getEntityMap(quickDAOConfig.entityOption);
        }
    }
}
