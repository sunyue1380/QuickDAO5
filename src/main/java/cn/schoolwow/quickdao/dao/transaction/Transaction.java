package cn.schoolwow.quickdao.dao.transaction;

import cn.schoolwow.quickdao.dao.dml.DatabaseManipulation;

/**
 * 事务接口
 */
public interface Transaction extends TransactionOperation, DatabaseManipulation {
}
