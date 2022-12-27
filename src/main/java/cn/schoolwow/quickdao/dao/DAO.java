package cn.schoolwow.quickdao.dao;

import cn.schoolwow.quickdao.dao.dcl.DatabaseControl;
import cn.schoolwow.quickdao.dao.ddl.DatabaseDefinition;
import cn.schoolwow.quickdao.dao.dml.DatabaseManipulation;
import cn.schoolwow.quickdao.dao.sql.DatabaseDAO;

public interface DAO extends DAOOperation, DatabaseDAO, DatabaseDefinition, DatabaseControl, DatabaseManipulation {
}
