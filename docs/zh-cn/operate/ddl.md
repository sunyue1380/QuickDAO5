# DDL操作

QuickDAO支持DDL相关操作.

## 表是否存在

```java
dao.hasTable("person");
```

## 列是否存在

```java
dao.hasColumn("person","name");
```

## 获取表字段列表

```java
List<Property> propertyList = dao.getPropertyList("person");
```

## 获取指定表的指定字段

```java
Property property = dao.getProperty("person","name");
```

## 建表

```java
dao.create(Person.class);
Entity entity = new Entity();
entity.tableName = "person";
entity.comment = "人";
entity.charset = "utf-8";
//设置字段信息
entity.properties = new ArrayList<Property>();
dao.create(entity);
```

## 删表

```java
dao.drop(Person.class);
dao.drop("person");
```

## 重建表

```java
dao.rebuild(Person.class);
dao.rebuild("person");
```

## 新增字段

```java
Property property = new Property();
property.column = "name";
property.columnType = "varchar(16)";
property.comment = "姓名";
dao.createColumn("person",property);
```

## 删除字段

```java
dao.dropColumn("person","name");
```