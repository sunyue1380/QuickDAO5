package cn.schoolwow.quickdao.dao;

import cn.schoolwow.quickdao.dao.sql.AbstractDatabaseDAO;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

/**
 * DAO接口调用代理模式对象
 */
public class DAOInvocationHandler implements InvocationHandler {
    private QuickDAOConfig quickDAOConfig;
    private DAOOperation daoOperation;

    public DAOInvocationHandler(QuickDAOConfig quickDAOConfig) {
        this.quickDAOConfig = quickDAOConfig;
        this.daoOperation = new DAOOperationImpl(quickDAOConfig);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String interfaceName = method.getDeclaringClass().getSimpleName();
        Object instance = null;
        switch (interfaceName) {
            case "DatabaseDAO": {
                instance = new AbstractDatabaseDAO(quickDAOConfig);
            }
            break;
            case "DatabaseControl": {
                instance = quickDAOConfig.databaseProvider.getDatabaseControlInstance(quickDAOConfig);
            }
            break;
            case "DatabaseDefinition": {
                instance = quickDAOConfig.databaseProvider.getDatabaseDefinitionInstance(quickDAOConfig);
            }
            break;
            case "DatabaseManipulation": {
                instance = quickDAOConfig.databaseProvider.getDatabaseManipulationInstance(quickDAOConfig);
            }
            break;
            default: {
                return method.invoke(daoOperation, args);
            }
        }
        Object result = method.invoke(instance, args);
        return result;
    }
}
