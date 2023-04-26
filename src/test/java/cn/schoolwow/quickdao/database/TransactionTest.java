package cn.schoolwow.quickdao.database;

import cn.schoolwow.quickdao.DataSourceParameterized;
import cn.schoolwow.quickdao.dao.DAO;
import cn.schoolwow.quickdao.dao.transaction.Transaction;
import cn.schoolwow.quickdao.entity.Person;
import com.alibaba.fastjson.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * 事务测试
 */
public class TransactionTest extends DataSourceParameterized {

    public TransactionTest(DAO dao) {
        super(dao);
    }

    @Before
    public void before() {
        dao.rebuild(Person.class);
    }

    @Test
    public void commit() {
        {
            Person person = dao.fetch(Person.class, "lastName", "Gates");
            Assert.assertNull(person);
        }

        Transaction transaction = dao.startTransaction();
        {
            //一般事务
            Person person = new Person();
            person.setPassword("123456");
            person.setFirstName("Bill");
            person.setLastName("Gates");
            person.setAddress("Xuanwumen 10");
            person.setCity("Beijing");
            transaction.insert(person);
            transaction.rollback();

            Person expectPerson = dao.fetch(Person.class, "lastName", "Gates");
            Assert.assertNull(expectPerson);

            transaction.insert(person);
            transaction.commit();
            expectPerson = dao.fetch(Person.class, "lastName", "Gates");
            Assert.assertNotNull(expectPerson);
        }
        {
            //Condition事务
            JSONObject personObject = new JSONObject();
            personObject.put("password", "123456");
            personObject.put("first_name", "John");
            personObject.put("last_name", "Adams");
            personObject.put("address", "Oxford Street");
            personObject.put("city", "London");
            transaction.insert("person", personObject);
            transaction.rollback();
            JSONObject person = dao.fetch("person", "last_name", "Adams");
            Assert.assertNull(person);

            transaction.insert("person", personObject);
            transaction.commit();
            person = dao.fetch("person", "last_name", "Adams");
            Assert.assertNotNull(person);
            Assert.assertEquals("Oxford Street", person.containsKey("address")?person.getString("address"):person.getString("ADDRESS"));
        }
        transaction.close();
    }

    @Test
    public void rollback() {
        {
            Person person = dao.fetch(Person.class, "lastName", "Carter");
            Assert.assertNull(person);
        }

        Transaction transaction = dao.startTransaction();
        {
            //一般事务
            Person person = new Person();
            person.setPassword("123456");
            person.setFirstName("Thomas");
            person.setLastName("Carter");
            person.setAddress("Changan Street");
            person.setCity("Beijing");
            int effect = transaction.insert(person);
            Assert.assertEquals(1, effect);

            transaction.rollback();
            person = dao.fetch(Person.class, "lastName", "Carter");
            Assert.assertNull(person);
        }
        {
            //Condition事务
            JSONObject personObject = new JSONObject();
            personObject.put("password", "123456");
            personObject.put("first_name", "Bill");
            personObject.put("last_name", "Gates");
            personObject.put("address", "Xuanwumen 10");
            personObject.put("city", "Beijing");
            int effect = transaction.insert("person", personObject);
            Assert.assertEquals(1, effect);
            transaction.rollback();
            JSONObject person = dao.fetch("person", "last_name", "Gates");
            Assert.assertNull(person);
        }
        transaction.close();
    }
}
