package cn.schoolwow.quickdao.database;

import cn.schoolwow.quickdao.DataSourceParameterized;
import cn.schoolwow.quickdao.annotation.IndexType;
import cn.schoolwow.quickdao.dao.DAO;
import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.IndexField;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.entity.Person;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DDL操作测试
 */
public class DDLTest extends DataSourceParameterized {
    private Logger logger = LoggerFactory.getLogger(DDLTest.class);

    public DDLTest(DAO dao) {
        super(dao);
    }

    @Test
    public void testGetTableAndProperty(){
        //测试获取数据库表和字段信息
        Assert.assertTrue(dao.hasTable(Person.class));
        Assert.assertTrue(dao.hasTable("person"));
        Assert.assertTrue(dao.hasColumn("person", "id"));
        Entity entity = dao.getDatabaseEntity("person");
        Assert.assertNotNull("实体类对应表找不到!实体类:Person", entity);
        Property property = dao.getProperty(Person.class, "id");
        Assert.assertNotNull("实体类字段对应列找不到!属性:Person的id字段", property);
    }

    @Test
    public void testUpdateColumn(){
        if("sqlite".equalsIgnoreCase(dao.getDatabaseProvider().name())){
            logger.info("sqlite数据库不支持修改列!");
            return;
        }
        //测试新增,修改,删除列
        Property newProperty = new Property();
        newProperty.column = "new_column";
        newProperty.comment = "新添加的列";
        newProperty.columnType = "varchar(10)";
        newProperty = dao.createColumn("person", newProperty);
        newProperty.columnType = "varchar(50)";
        newProperty.comment = "这是修改后的列";
        dao.alterColumn(newProperty);
        Property property = dao.getProperty("person", "new_column");
        Assert.assertEquals(newProperty.columnType, (property.columnType+"("+property.length+")").toLowerCase());
        dao.dropColumn("person", newProperty.column);
        Assert.assertFalse(dao.hasColumn("person", "new_column"));
    }

    @Test
    public void testIndexField(){
        if(dao.hasIndex("person", "last_name_index")){
            dao.dropIndex("person", "last_name_index");
        }
        //测试新增,修改和删除索引
        Assert.assertFalse("指定索引已存在", dao.hasIndex("person", "last_name_index"));
        IndexField indexField = new IndexField();
        indexField.tableName = "person";
        indexField.indexType = IndexType.NORMAL;
        indexField.indexName = "last_name_index";
        indexField.comment = "姓氏索引";
        indexField.columns.add("last_name");
        dao.createIndex(indexField);
        Assert.assertTrue("指定索引不存在", dao.hasIndex("person", "last_name_index"));
    }

}
