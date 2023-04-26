package cn.schoolwow.quickdao.database;

import cn.schoolwow.quickdao.DataSourceParameterized;
import cn.schoolwow.quickdao.dao.DAO;
import cn.schoolwow.quickdao.entity.Order;
import cn.schoolwow.quickdao.entity.Person;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * DML操作测试
 */
public class DMLTest extends DataSourceParameterized {

    public DMLTest(DAO dao) {
        super(dao);
        String databaseName = dao.getDatabaseProvider().name();
        if("sqlite".equalsIgnoreCase(databaseName)){
            return;
        }
        dao.truncate(Person.class);
        dao.truncate(Order.class);
    }

    @Test
    public void testDatabaseManipulation(){
        testInsert();
        testJson();
        testUpdate();
        testSave();
        testDelete();
    }

    @Test
    public void testRawUpdate() {
        int effect = dao.rawUpdate("delete from download_task;");
        Assert.assertEquals(0, effect);
        effect = dao.rawUpdate("insert into download_task(file_path,file_size,remark) values('filePath',0,'remark');");
        Assert.assertEquals(1, effect);
        effect = dao.rawUpdate("update download_task set file_size = 1024;");
        Assert.assertEquals(1, effect);
        effect = dao.rawUpdate("delete from download_task;");
        Assert.assertEquals(1, effect);
    }

    private void testInsert(){
        //单独插入
        {
            Person person = new Person();
            person.setPassword("123456");
            person.setFirstName("Bill");
            person.setLastName("Gates");
            person.setAddress("Xuanwumen 10");
            person.setCity("Beijing");
            int effect = dao.insert(person);
            Assert.assertEquals(1, effect);
            effect = dao.insertIgnore(person);
            Assert.assertEquals(0, effect);
        }
        //批量插入
        {
            Person[] persons = new Person[5];
            for (int i = 0; i < persons.length; i++) {
                Person person = new Person();
                person.setPassword("123456");
                person.setFirstName("Jack");
                person.setLastName("Ma " + i);
                person.setAddress("Xuanwumen 10");
                person.setCity("Beijing");
                persons[i] = person;
            }
            int effect = dao.batch(false).insert(persons);
            Assert.assertEquals(5, effect);
            for (Person person : persons) {
                Assert.assertTrue(person.getId() > 0);
            }
            effect = dao.insertIgnore(persons);
            Assert.assertEquals(0, effect);
        }
        //插入Order类
        {
            Order order = new Order();
            order.setId(UUID.randomUUID().toString());
            order.setPersonId(1);
            order.setOrderNo(1);
            int effect = dao.insert(order);
            Assert.assertEquals(1, effect);
        }
        {
            Order[] orders = new Order[5];
            for (int i = 0; i < orders.length; i++) {
                Order order = new Order();
                order.setId(UUID.randomUUID().toString());
                order.setPersonId(i + 2);
                order.setOrderNo(i + 2);
                orders[i] = order;
            }
            int effect = dao.insert(orders);
            Assert.assertEquals(5, effect);
        }
    }

    private void testUpdate(){
        //根据唯一性约束更新
        {
            Person person = new Person();
            person.setPassword("123456");
            person.setLastName("Gates");
            person.setAddress("Xuanwumen 11");
            int effect = dao.update(person);
            Assert.assertEquals(1, effect);
            person = dao.fetch(Person.class, "lastName", "Gates");
            Assert.assertEquals("Xuanwumen 11", person.getAddress());
        }
        {
            Person[] persons = new Person[5];
            for (int i = 0; i < persons.length; i++) {
                Person person = new Person();
                person.setPassword("123456");
                person.setFirstName("Jack");
                person.setLastName("Ma " + i);
                person.setAddress("Xuanwumen " + i);
                person.setCity("Beijing");
                persons[i] = person;
            }
            int effect = dao.update(persons);
            Assert.assertEquals(5, effect);
            List<Person> personList = dao.fetchList(Person.class, "firstName", "Jack");
            Assert.assertEquals(5, personList.size());
            for (int i = 0; i < personList.size(); i++) {
                Person person = personList.get(i);
                Assert.assertEquals("Jack", person.getFirstName());
                Assert.assertEquals("Xuanwumen " + i, person.getAddress());
            }
        }
        //根据id更新
        {
            Order order = dao.fetch(Order.class, "orderNo", 1);
            order.setPersonId(2);
            int effect = dao.update(order);
            Assert.assertEquals(1, effect);
            order = dao.fetch(Order.class, "orderNo", 1);
            Assert.assertEquals(2, order.getPersonId());
        }
        {
            List<Order> orderList = dao.query(Order.class)
                    .addBetweenQuery("orderNo", 2, 7)
                    .execute()
                    .getList();
            for (Order order : orderList) {
                order.setPersonId(order.getOrderNo() + 10);
            }
            int effect = dao.update(orderList);
            Assert.assertEquals(5, effect);
            orderList = dao.query(Order.class)
                    .addBetweenQuery("orderNo", 2, 7)
                    .execute()
                    .getList();
            for (Order order : orderList) {
                Assert.assertEquals(order.getOrderNo() + 10, order.getPersonId());
            }
        }
        //部分字段更新
        {
            Person person = new Person();
            person.setPassword("654321");
            person.setLastName("Gates");
            person.setAddress("Xuanwumen 12");
            int effect = dao.partColumn("password").update(person);
            Assert.assertEquals(1, effect);
            person = dao.fetch(Person.class, "lastName", "Gates");
            //sqlite,h2,sqlserver不支持md5等函数
            String databaseName = dao.getDatabaseProvider().name();
            if("sqlite".equalsIgnoreCase(databaseName)
                    ||"h2".equalsIgnoreCase(databaseName)
                    ||"sqlserver".equalsIgnoreCase(databaseName)
            ){
                Assert.assertEquals("654321", person.getPassword());
            }else{
                Assert.assertEquals("2b483f68099717078665cf2a39d835be", person.getPassword());
            }
            Assert.assertEquals("Xuanwumen 11", person.getAddress());
        }
        {
            Order order = dao.fetch(Order.class, "orderNo", 100);
            order.setOrderNo(101);
            order.setPersonId(200);
            int effect = dao.partColumn("orderNo").update(order);
            Assert.assertEquals(1, effect);
            order = dao.fetch(Order.class, "id", order.getId());
            Assert.assertEquals(101, order.getOrderNo());
            Assert.assertEquals(100, order.getPersonId());
        }
        //关联更新
        {
            int effect = dao.query(Order.class)
                    .tableAliasName("o")
                    .addSubQuery("person_id", "=",
                            dao.query(Person.class)
                                    .addColumn("id")
                                    .tableAliasName("p")
                                    .addQuery("id", 1)
                                    .addRawQuery("o.person_id = p.id")
                    )
                    .addUpdate("orderNo", 1)
                    .execute()
                    .update();
            Assert.assertEquals(0, effect);
        }
        {
            int effect = dao.query(Person.class)
                    .addUpdate("password", "654321")
                    .execute()
                    .update();
            Assert.assertEquals(6, effect);
            List<Person> personList = dao.fetchList(Person.class, "password", "654321");
            Assert.assertEquals(6, personList.size());
        }
    }

    private void testSave(){
        //新增记录
        {
            long count = dao.query(Person.class).execute().count();
            Person person = new Person();
            person.setPassword("123456");
            person.setFirstName("John");
            person.setLastName("Adams");
            person.setAddress("Oxford Street");
            person.setCity("London");
            int effect = dao.save(person);
            Assert.assertEquals(1, effect);
            Assert.assertEquals(count + 1, dao.query(Person.class).execute().count());
            effect = dao.save(Arrays.asList(person));
            Assert.assertEquals(1, effect);
        }
        //更新记录(根据唯一性约束更新)
        {
            Person person = new Person();
            person.setPassword("123456");
            person.setFirstName("Bill");
            person.setLastName("Adams");
            person.setAddress("Xuanwumen 100");
            person.setCity("TianJin");
            int effect = dao.save(person);
            Assert.assertEquals(1, effect);
            person = dao.fetch(Person.class, "lastName", "Adams");
            Assert.assertEquals("Xuanwumen 100", person.getAddress());
        }
        //更新记录(根据id更新)
        {
            Order order = dao.fetch(Order.class, "orderNo", 1);
            order.setPersonId(10);
            int effect = dao.save(order);
            Assert.assertEquals(1, effect);
            order = dao.fetch(Order.class, "orderNo", 1);
            Assert.assertEquals(10, order.getPersonId());
        }
        //部分字段更新(根据唯一性约束更新)
        {
            Person person = new Person();
            person.setPassword("654321");
            person.setLastName("Adams");
            person.setAddress("Xuanwumen 101");
            int effect = dao.partColumn("password").save(person);
            Assert.assertEquals(1, effect);
            person = dao.fetch(Person.class, "lastName", "Adams");
            String databaseName = dao.getDatabaseProvider().name();
            if("sqlite".equalsIgnoreCase(databaseName)
                    ||"h2".equalsIgnoreCase(databaseName)
                    ||"sqlserver".equalsIgnoreCase(databaseName)
            ){
                Assert.assertEquals("654321", person.getPassword());
            }else{
                Assert.assertEquals("2b483f68099717078665cf2a39d835be", person.getPassword());
            }
            Assert.assertEquals("Xuanwumen 100", person.getAddress());
        }
        //部分字段更新(根据id更新)
        {
            Order order = dao.fetch(Order.class, "orderNo", 1);
            order.setOrderNo(100);
            order.setPersonId(10);
            int effect = dao.partColumn("personId").save(order);
            Assert.assertEquals(1, effect);
            order = dao.fetch(Order.class, "orderNo", 1);
            Assert.assertEquals(10, order.getPersonId());
        }
    }

    private void testDelete(){
        {
            long count = dao.query(Person.class).execute().count();
            Assert.assertEquals(7, count);
            Person person = dao.query(Person.class).orderByDesc("id").limit(0,1).execute().getOne();
            int effect = dao.delete(Person.class, person.getId());
            Assert.assertEquals(1, effect);
            Assert.assertEquals(count - 1, dao.query(Person.class).execute().count());
        }
        {
            dao.clear(Person.class);
            //单独插入
            {
                Person person = new Person();
                person.setPassword("123456");
                person.setFirstName("Bill");
                person.setLastName("Gates");
                person.setAddress("Xuanwumen 10");
                person.setCity("Beijing");
                int effect = dao.insert(person);
                Assert.assertEquals(1, effect);
                effect = dao.delete(person);
                Assert.assertEquals(1, effect);
            }
            //批量插入
            {
                Person[] persons = new Person[5];
                for (int i = 0; i < persons.length; i++) {
                    Person person = new Person();
                    person.setPassword("123456");
                    person.setFirstName("Jack");
                    person.setLastName("Ma " + i);
                    person.setAddress("Xuanwumen 10");
                    person.setCity("Beijing");
                    persons[i] = person;
                }
                int effect = dao.insert(persons);
                Assert.assertEquals(5, effect);
                effect = dao.delete(persons);
                Assert.assertEquals(5, effect);
            }
        }
        {
            long count = dao.query(Order.class).execute().count();
            int effect = dao.delete(Order.class, "orderNo", 1);
            Assert.assertEquals(1, effect);
            Assert.assertEquals(count - 1, dao.query(Order.class).execute().count());
        }
        {
            long count = dao.query(Order.class).execute().count();
            int effect = dao.query(Order.class)
                    .addBetweenQuery("orderNo", 2, 7)
                    .execute()
                    .delete();
            Assert.assertEquals(5, effect);
            Assert.assertEquals(count - 5, dao.query(Order.class).execute().count());
        }
        {
            dao.clear(Order.class);
            {
                Order order = new Order();
                order.setId(UUID.randomUUID().toString());
                order.setPersonId(1);
                order.setOrderNo(1);
                int effect = dao.insert(order);
                Assert.assertEquals(1, effect);
                effect = dao.delete(order);
                Assert.assertEquals(1, effect);
            }
            {
                Order[] orders = new Order[5];
                for (int i = 0; i < orders.length; i++) {
                    Order order = new Order();
                    order.setId(UUID.randomUUID().toString());
                    order.setPersonId(i + 2);
                    order.setOrderNo(i + 2);
                    orders[i] = order;
                }
                int effect = dao.insert(orders);
                Assert.assertEquals(5, effect);
                effect = dao.delete(orders);
                Assert.assertEquals(5, effect);
            }
        }
    }

    private void testJson(){
        //JSONObject插入
        {
            JSONObject order = new JSONObject();
            order.put("id", UUID.randomUUID().toString());
            order.put("order_no", 100);
            order.put("person_id", 100);
            int effect = dao.insert("order", order);
            Assert.assertEquals(1, effect);
            order.put("person_id", 101);
            effect = dao.uniqueFieldNames("order_no").insertIgnore("order", order);
            Assert.assertEquals(0, effect);
        }
        //JSONObject插入
        {
            JSONObject order = new JSONObject();
            order.put("id", UUID.randomUUID().toString());
            order.put("order_no", 200);
            order.put("person_id", 200);
            int effect = dao.insert("order", order);
            Assert.assertEquals(1, effect);
            effect = dao.uniqueFieldNames("order_no").delete("order", order);
            Assert.assertEquals(1, effect);
        }
        //JSONArray插入
        {
            JSONArray orders = new JSONArray();
            for(int i=0;i<5;i++){
                JSONObject order = new JSONObject();
                order.put("id", UUID.randomUUID().toString());
                order.put("order_no", 200+i);
                order.put("person_id", 200+i);
                orders.add(order);
            }
            int effect = dao.insert("order", orders);
            Assert.assertEquals(5, effect);
            effect = dao.uniqueFieldNames("order_no").insertIgnore("order", orders);
            Assert.assertEquals(0, effect);
        }
        //JSONObject更新
        {
            JSONObject order = new JSONObject();
            order.put("id", UUID.randomUUID().toString());
            order.put("order_no", 200);
            order.put("person_id", 300);
            int effect = dao.uniqueFieldNames("order_no").update("order", order);
            Assert.assertEquals(1, effect);
        }
        //JSONArray更新
        {
            JSONArray orders = new JSONArray();
            for(int i=0;i<5;i++){
                JSONObject order = new JSONObject();
                order.put("id", UUID.randomUUID().toString());
                order.put("order_no", 200+i);
                order.put("person_id", 300);
                orders.add(order);
            }
            int effect = dao.uniqueFieldNames("order_no").update("order", orders);
            Assert.assertEquals(5, effect);
            effect = dao.uniqueFieldNames("order_no").delete("order", orders);
            Assert.assertEquals(5, effect);
        }
    }
}
