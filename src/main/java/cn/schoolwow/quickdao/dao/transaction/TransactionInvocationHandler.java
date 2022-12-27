package cn.schoolwow.quickdao.dao.transaction;

import cn.schoolwow.quickdao.dao.dml.AbstractDatabaseManipulation;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

/**
 * 事务操作代理模式对象
 */
public class TransactionInvocationHandler implements InvocationHandler {
    private AbstractDatabaseManipulation databaseManipulation;

    public TransactionInvocationHandler(QuickDAOConfig quickDAOConfig) {
        databaseManipulation = quickDAOConfig.databaseProvider.getDatabaseManipulationInstance(quickDAOConfig);
        databaseManipulation.connectionExecutor.startTransaction();
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String interfaceName = method.getDeclaringClass().getSimpleName();
        switch (interfaceName) {
            case "Closeable":
            case "TransactionOperation": {
                return method.invoke(databaseManipulation.connectionExecutor, args);
            }
            case "DatabaseManipulation": {
                return method.invoke(databaseManipulation, args);
            }
            default: {
                throw new IllegalArgumentException("不支持的方法调用!方法:" + method);
            }
        }
    }
}
