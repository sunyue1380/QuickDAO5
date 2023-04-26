package cn.schoolwow.quickdao.dao.dcl;

import cn.schoolwow.quickdao.dao.sql.AbstractDatabaseDAO;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.external.dcl.DataBaseUser;
import cn.schoolwow.quickdao.domain.external.dcl.GrantOption;

import java.util.List;

public class AbstractDabaseControl extends AbstractDatabaseDAO implements DatabaseControl {

    public AbstractDabaseControl(QuickDAOConfig quickDAOConfig) {
        super(quickDAOConfig);
    }

    @Override
    public List<String> getUserNameList() {
        throw new UnsupportedOperationException("当前数据库不支持获取用户列表!");
    }

    @Override
    public boolean hasUserName(String username) {
        List<String> userNameList = getUserNameList();
        for(String username1:userNameList){
            if(username1.equalsIgnoreCase(username)){
                return true;
            }
        }
        return false;
    }

    @Override
    public void createUser(DataBaseUser dataBaseUser) {
        throw new UnsupportedOperationException("当前数据库不支持创建用户!");
    }

    @Override
    public void modifyPassword(String username, String newPassword) {
        throw new UnsupportedOperationException("当前数据库不支持更改密码!");
    }

    @Override
    public void deleteUser(DataBaseUser dataBaseUser) {
        throw new UnsupportedOperationException("当前数据库不支持删除用户!");
    }

    @Override
    public void grant(GrantOption grantOption) {
        throw new UnsupportedOperationException("当前数据库不支持数据库授权!");
    }

    @Override
    public void createUserAndGrant(GrantOption grantOption) {
        throw new UnsupportedOperationException("当前数据库不支持创建用户并授权!");
    }

    @Override
    public void revoke(GrantOption grantOption) {
        throw new UnsupportedOperationException("当前数据库不支持收回权限!");
    }

    @Override
    public void flushPrivileges() {
        throw new UnsupportedOperationException("当前数据库不支持刷新权限!");
    }
}