package cn.schoolwow.quickdao.dao.statement;

import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;

public abstract class AbstractDatabaseStatement implements DatabaseStatement {
    /**
     * 数据库操纵语言选项
     */
    protected DatabaseStatementOption option;

    /**
     * 配置项
     */
    protected QuickDAOConfig quickDAOConfig;

    public AbstractDatabaseStatement(DatabaseStatementOption databaseStatementOption, QuickDAOConfig quickDAOConfig) {
        this.option = databaseStatementOption;
        this.quickDAOConfig = quickDAOConfig;
    }

    /**
     * 是否需要过滤该字段
     */
    protected boolean skipPartColumn(Property property) {
        if (null == option.partColumnSet || option.partColumnSet.isEmpty()) {
            return false;
        }
        for (String column : option.partColumnSet) {
            if (property.column.equalsIgnoreCase(column) || property.name.equalsIgnoreCase(column)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 返回列名的SQL语句
     *
     * @param entity     实体类
     * @param tableAlias 表别名
     */
    protected String columns(Entity entity, String tableAlias) {
        StringBuilder builder = new StringBuilder();
        for (Property property : entity.properties) {
            builder.append(tableAlias + "." + quickDAOConfig.databaseProvider.escape(property.column) + " as " + tableAlias + "_" + property.column + ",");
        }
        builder.deleteCharAt(builder.length() - 1);
        return builder.toString();
    }
}
