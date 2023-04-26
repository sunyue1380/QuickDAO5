package cn.schoolwow.quickdao.dao.dcl;

import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.external.dcl.DataBaseUser;
import cn.schoolwow.quickdao.domain.external.dcl.GrantOption;

import java.util.ArrayList;
import java.util.List;

public class PostgreDatabaseControl extends AbstractDabaseControl {

    public PostgreDatabaseControl(QuickDAOConfig quickDAOConfig) {
        super(quickDAOConfig);
    }

    @Override
    public List<String> getUserNameList() {
        String getUserNameListSQL = "select usename from pg_user;";
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
        String createUserSQL = "create user " + dataBaseUser.username + " with password '" + dataBaseUser.password + "';";
        connectionExecutor.name("创建用户").sql(createUserSQL).executeUpdate();
    }

    @Override
    public void modifyPassword(String username, String newPassword) {
        String modifyPasswordSQL = "alter user " + username + " with password '" + newPassword + "';";
        connectionExecutor.name("更改密码").sql(modifyPasswordSQL).executeUpdate();
    }

    @Override
    public void deleteUser(DataBaseUser dataBaseUser) {
        String deleteUserSQL = "drop role " + dataBaseUser.username + ";";
        connectionExecutor.name("删除用户").sql(deleteUserSQL).executeUpdate();
    }

    @Override
    public void grant(GrantOption grantOption) {
        String grantSQL = "grant " + grantOption.privileges + " on database " + grantOption.databaseName + " to " + grantOption.dataBaseUser.username + ";";
        connectionExecutor.name("数据库授权").sql(grantSQL).executeUpdate();
    }

    @Override
    public void createUserAndGrant(GrantOption grantOption) {
        createUser(grantOption.dataBaseUser);
        grant(grantOption);
    }

    @Override
    public void revoke(GrantOption grantOption) {
        String revokeSQL = "revoke " + grantOption.privileges + " on database " + grantOption.databaseName + " from " + grantOption.dataBaseUser.username + ";";
        connectionExecutor.name("收回权限").sql(revokeSQL).executeUpdate();
    }

}
