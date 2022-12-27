package cn.schoolwow.quickdao.dao.statement;

import java.util.List;

/**
 * 操作语言语句
 */
public interface DatabaseStatement {
    /**
     * 获取语句
     */
    String getStatement();

    /**
     * 获取语句参数
     *
     * @param instance 参数
     */
    List getParameters(Object instance);

    /**
     * 名称
     */
    String name();
}
