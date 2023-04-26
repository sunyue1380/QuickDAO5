package cn.schoolwow.quickdao.statement.dql;

import com.alibaba.fastjson.JSONArray;

import java.util.List;

public interface DQLDatabaseStatement {
    /**
     * 获取查询记录个数
     * */
    int getCount();

    /**
     * 获取单列结果
     * */
    List getSingleColumnList();

    /**
     * 获取返回列表
     * */
    JSONArray getArray();

    /**
     * 执行更新语句(Response接口使用)
     * */
    int executeUpdate();
}
