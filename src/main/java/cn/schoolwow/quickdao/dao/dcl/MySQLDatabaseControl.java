package cn.schoolwow.quickdao.dao.dcl;

import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.external.dcl.DataBaseUser;
import cn.schoolwow.quickdao.domain.external.dcl.GrantOption;

import java.util.ArrayList;
import java.util.List;

public class MySQLDatabaseControl extends AbstractDabaseControl {

    public MySQLDatabaseControl(QuickDAOConfig quickDAOConfig) {
        super(quickDAOConfig);
    }

    @Override
    public List<String> getUserNameList() {
        String getUserNameListSQL = "select distinct user from mysql.user;";
        List<String> userNameList = new ArrayList<>();
        connectionExecutor.name("获取用户列表").sql(getUserNameListSQL).executeQuery((resultSet) -> {
            while (resultSet.next()) {
                userNameList.add(resultSet.getString(1));
            }
        });
        return userNameList;
    }

    @Override
    public void createUser(DataBaseUser dataBaseUser) {
        String createUserSQL = "create user '" + dataBaseUser.username + "'@'" + dataBaseUser.host + "' identified by '" + dataBaseUser.password + "';";
        connectionExecutor.name("创建用户").sql(createUserSQL).executeUpdate();
    }

    @Override
    public void modifyPassword(String username, String newPassword) {
        String modifyPasswordSQL = "set password for " + username + " = password('" + newPassword + "');";
        connectionExecutor.name("更改密码").sql(modifyPasswordSQL).executeUpdate();
    }

    @Override
    public void deleteUser(DataBaseUser dataBaseUser) {
        String deleteUserSQL = "drop user " + dataBaseUser.username + "@'" + dataBaseUser.host + "';";
        connectionExecutor.name("删除用户").sql(deleteUserSQL).executeUpdate();
    }

    @Override
    public void grant(GrantOption grantOption) {
        String grantSQL = "grant " + grantOption.privileges + " on " + grantOption.databaseName + ".* to '" + grantOption.dataBaseUser.username + "'@'" + grantOption.dataBaseUser.host + "';";
        connectionExecutor.name("数据库授权").sql(grantSQL).executeUpdate();
        flushPrivileges();
    }

    @Override
    public void createUserAndGrant(GrantOption grantOption) {
        String createUserAndGrantSQL = "grant " + grantOption.privileges + " on " + grantOption.databaseName + ".* to '" + grantOption.dataBaseUser.username + "'@'" + grantOption.dataBaseUser.host + "' identified by '" + grantOption.dataBaseUser.password + "';";
        connectionExecutor.name("创建用户并授权").sql(createUserAndGrantSQL).executeUpdate();
        flushPrivileges();
    }

    @Override
    public void revoke(GrantOption grantOption) {
        String revokeSQL = "revoke " + grantOption.privileges + " on " + grantOption.databaseName + ".* from '" + grantOption.dataBaseUser.username + "'@'" + grantOption.dataBaseUser.host + "';";
        connectionExecutor.name("收回权限").sql(revokeSQL).executeUpdate();
        flushPrivileges();
    }

    @Override
    public void flushPrivileges() {
        String flushPrivilegesSQL = "flush privileges;";
        connectionExecutor.name("刷新权限").sql(flushPrivilegesSQL).executeUpdate();
    }
}
