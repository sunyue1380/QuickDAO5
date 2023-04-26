package cn.schoolwow.quickdao.annotation;

import java.lang.annotation.*;

/**
 * 在字段上建立索引
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(Indexes.class)
public @interface Index {
    /**
     * 索引类型
     */
    IndexType indexType() default IndexType.NORMAL;

    /**
     * 索引名称
     */
    String indexName() default "";

    /**
     * 索引方法
     */
    String using() default "";

    /**
     * 索引注释
     */
    String comment() default "";
}
