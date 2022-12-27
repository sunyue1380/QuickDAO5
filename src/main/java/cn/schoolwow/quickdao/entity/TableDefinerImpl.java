package cn.schoolwow.quickdao.entity;

import cn.schoolwow.quickdao.QuickDAO;
import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.IndexField;
import cn.schoolwow.quickdao.domain.external.Property;

/**
 * 实体类信息定义
 */
public class TableDefinerImpl implements TableDefiner {
    /**
     * 当前实体类
     */
    private Entity entity;

    /**
     * 用于返回QuickDAO对象
     */
    private QuickDAO quickDAO;

    public TableDefinerImpl(Entity entity, QuickDAO quickDAO) {
        this.entity = entity;
        this.quickDAO = quickDAO;
    }

    @Override
    public TableDefiner tableName(String tableName) {
        entity.tableName = tableName;
        return this;
    }

    @Override
    public TableDefiner comment(String comment) {
        entity.comment = comment;
        return this;
    }

    @Override
    public TableDefiner index(IndexField indexField) {
        entity.indexFieldList.add(indexField);
        return this;
    }

    @Override
    public TablePropertyDefiner property(String fieldName) {
        for (Property property : entity.properties) {
            if (property.name.equals(fieldName)) {
                return new TablePropertyDefinerImpl(property, this);
            }
        }
        throw new IllegalArgumentException("不存在的字段名称!字段名:" + fieldName);
    }

    @Override
    public QuickDAO done() {
        return this.quickDAO;
    }
}
