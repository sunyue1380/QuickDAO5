package cn.schoolwow.quickdao.domain.external;

import cn.schoolwow.quickdao.annotation.ForeignKey;
import cn.schoolwow.quickdao.annotation.IdStrategy;

import java.io.*;

/**
 * 实体类属性信息
 */
public class Property implements Serializable, Cloneable {
    /**
     * 是否是id
     */
    public boolean id;

    /**
     * id生成策略
     */
    public transient IdStrategy strategy;

    /**
     * 返回列标签名称
     */
    public String columnLabel;

    /**
     * 列名
     */
    public String column;

    /**
     * 数据库类型
     */
    public String columnType;

    /**
     * 长度
     */
    public Integer length;

    /**
     * 注释
     */
    public String comment;

    /**
     * 类
     */
    public transient Class clazz;

    /**
     * 类名
     */
    public String className;

    /**
     * 属性名
     */
    public String name;

    /**
     * 是否非空
     */
    public boolean notNull;

    /**
     * check约束
     */
    public String check;

    /**
     * 转义后的check约束
     */
    public String escapeCheck;

    /**
     * 默认值
     */
    public String defaultValue;

    /**
     * 字段函数
     */
    public String function;

    /**
     * 是否填充插入时间
     */
    public boolean createdAt;

    /**
     * 是否填充更新时间
     */
    public boolean updateAt;

    /**
     * 外键关联
     */
    public transient ForeignKey foreignKey;

    /**
     * 所属实体
     */
    public transient Entity entity;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Property property = (Property) o;

        if (!column.equals(property.column)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return column.hashCode();
    }

    /**
     * 复制拷贝transient字段
     */
    public void copyTransientField(Property target) {
        this.strategy = target.strategy;
        this.clazz = target.clazz;
        this.foreignKey = target.foreignKey;
        this.entity = target.entity;
    }

    /**
     * 替换属性
     */
    public void replaceField(Property source) {
        this.id = source.id;
        this.strategy = source.strategy;
        this.columnLabel = source.columnLabel;
        this.column = source.column;
        this.columnType = source.columnType;
        this.length = source.length;
        this.comment = source.comment;
        this.clazz = source.clazz;
        this.className = source.className;
        this.name = source.name;
        this.notNull = source.notNull;
        this.check = source.check;
        this.escapeCheck = source.escapeCheck;
        this.defaultValue = source.defaultValue;
        this.function = source.function;
        this.createdAt = source.createdAt;
        this.updateAt = source.updateAt;
        this.foreignKey = source.foreignKey;
        this.entity = source.entity;
    }

    @Override
    public Property clone() {
        ByteArrayInputStream bais = null;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(this);
            oos.close();

            bais = new ByteArrayInputStream(baos.toByteArray());
            ObjectInputStream ois = new ObjectInputStream(bais);
            Property property = (Property) ois.readObject();
            property.copyTransientField(this);
            bais.close();
            return property;
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            if (null != bais) {
                try {
                    bais.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return "\n{\n" +
                "是否为id:" + id + "\n"
                + "id生成策略:" + strategy + "\n"
                + "返回列标签名称:" + columnLabel + "\n"
                + "列名:" + column + "\n"
                + "数据库类型:" + columnType + "\n"
                + "注释:" + comment + "\n"
                + "类:" + className + "\n"
                + "属性名:" + name + "\n"
                + "是否非空:" + notNull + "\n"
                + "check约束:" + check + "\n"
                + "转义后的check约束:" + escapeCheck + "\n"
                + "默认值:" + defaultValue + "\n"
                + "字段函数:" + function + "\n"
                + "是否填充插入时间:" + createdAt + "\n"
                + "是否填充更新时间:" + updateAt + "\n"
                + "外键关联:" + foreignKey + "\n"
                + "}\n";
    }
}
