package cn.schoolwow.quickdao;

import cn.schoolwow.quickdao.dao.DAO;
import cn.schoolwow.quickdao.dao.transaction.Transaction;
import cn.schoolwow.quickdao.entity.*;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.UUID;

@RunWith(Parameterized.class)
public class DataSourceParameterized {
    private static Collection<Object[]> data;
    static{
        try {
            HikariDataSource[] hikariDataSources = new HikariDataSource[]{
                    getSQLiteDataSource(),
                    getH2DataSource(),
                    getMySQLDataSource(),
                    getMariaDataSource(),
                    getPostgresDataSource(),
                    getSQLServerDataSource(),
            };
            Object[][] dataArray = new Object[hikariDataSources.length][1];
            for(int i=0;i<dataArray.length;i++){
                dataArray[i][0] = initialDAO(hikariDataSources[i]);
            }
            data = Arrays.asList(dataArray);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return data;
    }

    protected DAO dao;

    public DataSourceParameterized(DAO dao) {
        this.dao = dao;
    }

    /**初始化数据*/
    protected void initializePersonAndOrder() {
        dao.rebuild(Person.class);
        dao.rebuild(Order.class);
        Person[] persons = new Person[3];
        //初始化数据
        {
            Person person = new Person();
            person.setPassword("123456");
            person.setFirstName("Bill");
            person.setLastName("Gates");
            person.setAddress("Xuanwumen 10");
            person.setCity("Beijing");
            persons[0] = person;
        }
        {
            Person person = new Person();
            person.setPassword("123456");
            person.setFirstName("Thomas");
            person.setLastName("Carter");
            person.setAddress("Changan Street");
            person.setCity("Beijing");
            persons[1] = person;
        }
        {
            Person person = new Person();
            person.setPassword("123456");
            person.setLastName("Wilson");
            person.setAddress("Champs-Elysees");
            persons[2] = person;
        }
        {
            int effect = dao.insert(persons);
            Assert.assertEquals(3, effect);
        }
        {
            Order order = new Order();
            order.setId(UUID.randomUUID().toString());
            order.setPersonId(1);
            order.setLastName("Gates");
            order.setOrderNo(1);
            int effect = dao.insert(order);
            Assert.assertEquals(1, effect);
        }
    }

    /**
     * 初始化数据
     */
    protected void initializeProduct() {
        dao.rebuild(Product.class);
        Transaction transaction = dao.startTransaction();
        String[] productNames = new String[]{"笔记本电脑", "冰箱", "电视机", "智能音箱"};
        String[] types = new String[]{"电器", "电器", "电器", "数码"};
        int[] prices = new int[]{4000, 600, 3000, 1000};
        for (int i = 0; i < productNames.length; i++) {
            Product product = new Product();
            product.setName(productNames[i]);
            product.setType(types[i]);
            product.setPrice(prices[i]);
            product.setPublishTime(new Date());
            product.setPersonId(1);
            transaction.insert(product);
        }
        transaction.commit();
        transaction.close();
    }

    /**获取sqlite数据源*/
    private static HikariDataSource getSQLiteDataSource(){
        HikariDataSource hikariDataSource = new HikariDataSource();
        hikariDataSource.setDriverClassName("org.sqlite.JDBC");
        hikariDataSource.setJdbcUrl("jdbc:sqlite:" + new File("quickdao_sqlite.db").getAbsolutePath());
        return hikariDataSource;
    }

    /**获取h2数据源*/
    private static HikariDataSource getH2DataSource(){
        HikariDataSource hikariDataSource = new HikariDataSource();
        hikariDataSource.setDriverClassName("org.h2.Driver");
        hikariDataSource.setJdbcUrl("jdbc:h2:" + new File("quickdao_h2.db").getAbsolutePath() + ";mode=MYSQL");
        return hikariDataSource;
    }

    /**获取mysql数据源*/
    private static HikariDataSource getMySQLDataSource(){
        HikariDataSource hikariDataSource = new HikariDataSource();
        hikariDataSource.setDriverClassName("com.mysql.jdbc.Driver");
        hikariDataSource.setJdbcUrl("jdbc:mysql://192.168.3.106:3306/quickdao");
        hikariDataSource.setUsername("root");
        hikariDataSource.setPassword("123456");
        hikariDataSource.setConnectionTimeout(60000);
        hikariDataSource.setLeakDetectionThreshold(5000);
        return hikariDataSource;
    }

    /**获取maria数据源*/
    private static HikariDataSource getMariaDataSource(){
        HikariDataSource hikariDataSource = new HikariDataSource();
        hikariDataSource.setDriverClassName("org.mariadb.jdbc.Driver");
        hikariDataSource.setJdbcUrl("jdbc:mariadb://192.168.3.106:3307/quickdao");
        hikariDataSource.setUsername("root");
        hikariDataSource.setPassword("123456");
        hikariDataSource.setConnectionTimeout(60000);
        hikariDataSource.setLeakDetectionThreshold(5000);
        return hikariDataSource;
    }

    /**获取postgres数据源*/
    private static HikariDataSource getPostgresDataSource(){
        HikariDataSource hikariDataSource = new HikariDataSource();
        hikariDataSource.setDriverClassName("org.postgresql.Driver");
        hikariDataSource.setJdbcUrl("jdbc:postgresql://192.168.3.106:5432/quickdao");
        hikariDataSource.setUsername("postgres");
        hikariDataSource.setPassword("123456");
        return hikariDataSource;
    }

    /**获取SQLServer数据源*/
    private static HikariDataSource getSQLServerDataSource(){
        HikariDataSource hikariDataSource = new HikariDataSource();
        hikariDataSource.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        hikariDataSource.setJdbcUrl("jdbc:sqlserver://192.168.3.106:1433");
        hikariDataSource.setUsername("sa");
        hikariDataSource.setPassword("HK7pgstdl8mLebh");
        return hikariDataSource;
    }

    /**初始化DAO*/
    private static DAO initialDAO(HikariDataSource hikariDataSource){
        DAO dao = QuickDAO.newInstance()
                .dataSource(hikariDataSource)
                .packageName("cn.schoolwow.quickdao.entity")
                .build();
        dao.rebuild(Person.class);
        dao.rebuild(Order.class);
        dao.rebuild(Product.class);
        dao.rebuild(TypeEntity.class);
        dao.rebuild(DownloadTask.class);
        return dao;
    }
}