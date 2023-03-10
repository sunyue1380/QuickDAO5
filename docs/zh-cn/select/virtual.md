# 虚拟查询(无实体类查询)

默认情况下,QuickDAO会扫描用户指定的实体类包然后映射实体类信息到数据表再来进行查询.

但是QuickDAO也实现了无需扫描实体类实现对数据库的增删查改.

假设数据库里存在如下表person表

|id|username|password|type|
|:---:|:---:|:---:|:---:|
|1|tom|123456|1|
|2|jack|123456|2|

## 简单查询
```java
JSONObject person = dao.fetch("person","username","tom");
JSONArray person = dao.fetchList("person","password","123456");
```

## 复杂查询
```java
//select id,username,password,type from person where username = 'tom'
JSONArray array = dao.query("person")
    .addQuery("username","tom")
    .addColumn("id","username","password","type")
    .execute()
    .getArray();
```

> 使用无实体类进行查询操作时必须调用addColumn方法指定要返回的列信息

## 插入
```java
//insert into person(username,password) value('tony','123456')
JSONObject person = new JSONObject();
person.put("username", "tony");
person.put("password", "123456");
dao.insert("person", person);
```

## 更新
```java
int effect = dao.query("person")
    .addUpdate("password","654321")
    .addQuery("username","tony")
    .execute()
    .update();
```

## 删除
```java
//delete from person where username = 'tony'
int effect = dao.query("person")
    .addQuery("username","tony")
    .execute()
    .delete();
```

## dual查询

dual表在许多数据库产品里面作为虚表存在,QuickDAO也支持使用dual表.

```java
//select '1' id from dual
JSONArray array = dao.query("dual")
   .addColumn("'1' id")
   .execute()
   .getArray();
```