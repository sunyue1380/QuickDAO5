package cn.schoolwow.quickdao.dao.dcl;

import cn.schoolwow.quickdao.domain.external.dcl.DataBaseUser;
import cn.schoolwow.quickdao.domain.external.dcl.GrantOption;

import java.util.List;

/**
 * 数据控制语言
 */
public interface DatabaseControl {
    /**
     * 获取用户列表
     */
    List<String> getUserNameList();

    /**
     * 数据库用户是否存在
     * @param username 数据库用户名
     */
    boolean hasUserName(String username);

    /**
     * 创建用户
     *
     * @param dataBaseUser 数据库用户
     */
    void createUser(DataBaseUser dataBaseUser);

    /**
     * 修改用户密码
     *
     * @param username    用户名
     * @param newPassword 新密码
     */
    void modifyPassword(String username, String newPassword);

    /**
     * 删除用户
     *
     * @param dataBaseUser 数据库用户
     */
    void deleteUser(DataBaseUser dataBaseUser);

    /**
     * 授予权限
     *
     * @param grantOption 授权信息
     */
    void grant(GrantOption grantOption);

    /**
     * 创建用户并授予权限
     *
     * @param grantOption 授权信息
     */
    void createUserAndGrant(GrantOption grantOption);

    /**
     * 收回权限
     *
     * @param grantOption 授权信息
     */
    void revoke(GrantOption grantOption);

    /**
     * 刷新权限
     */
    void flushPrivileges();
}
