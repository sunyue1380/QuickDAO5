package cn.schoolwow.quickdao.domain.external;

import java.util.List;

/**
 * SQL语句执行监听器
 */
public interface StatementListener {
    /**
     * 在语句执行之前
     *
     * @return 实际执行SQL语句
     */
    String beforeExecute(String name, String sql, List parameters);
}
