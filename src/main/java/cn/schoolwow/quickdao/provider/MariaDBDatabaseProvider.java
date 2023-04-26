package cn.schoolwow.quickdao.provider;

public class MariaDBDatabaseProvider extends MySQLDatabaseProvider {

    @Override
    public String name() {
        return "mariadb";
    }
}
