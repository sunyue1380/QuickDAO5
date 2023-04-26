package cn.schoolwow.quickdao.database;

import cn.schoolwow.quickdao.DataSourceParameterized;
import cn.schoolwow.quickdao.dao.DAO;
import cn.schoolwow.quickdao.domain.external.dcl.DataBaseUser;
import cn.schoolwow.quickdao.domain.external.dcl.GrantOption;
import org.junit.Test;

public class DCLTest extends DataSourceParameterized {

    public DCLTest(DAO dao) {
        super(dao);
    }

    @Test
    public void testDatabaseControl() {
        //跳过sqlite和h2
        String databaseName = dao.getDatabaseProvider().name();
        if("sqlite".equalsIgnoreCase(databaseName)
                ||"h2".equalsIgnoreCase(databaseName)
                ||"sqlserver".equalsIgnoreCase(databaseName)
        ){
            return;
        }
        //创建用户
        DataBaseUser dataBaseUser = new DataBaseUser();
        dataBaseUser.username = "dao_control_name";
        dataBaseUser.password = "123456";
        if(dao.hasUserName(dataBaseUser.username)){
            dao.deleteUser(dataBaseUser);
        }

        //数据库授权
        GrantOption grantOption = new GrantOption();
        grantOption.databaseName = "quickdao";
        grantOption.dataBaseUser = dataBaseUser;
        dao.createUserAndGrant(grantOption);

        if(dao.hasUserName("dao_control_name")){
            dao.revoke(grantOption);
            dao.deleteUser(dataBaseUser);
        }
        //创建用户
        dao.createUser(dataBaseUser);
        //授予权限
        dao.grant(grantOption);
        //收回权限
        dao.revoke(grantOption);
        //修改密码
        dao.modifyPassword(dataBaseUser.username, "654321");
        //删除用户
        dao.deleteUser(dataBaseUser);
    }
}
