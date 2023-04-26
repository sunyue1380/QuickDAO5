package cn.schoolwow.quickdao.statement.dql;

import cn.schoolwow.quickdao.dao.ConnectionExecutor;
import cn.schoolwow.quickdao.dao.ConnectionExecutorImpl;
import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.statement.AbstractDatabaseStatement;
import cn.schoolwow.quickdao.util.ResponseUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractDQLDatabaseStatement extends AbstractDatabaseStatement implements DQLDatabaseStatement{
    protected ConnectionExecutor connectionExecutor;

    public AbstractDQLDatabaseStatement(QuickDAOConfig quickDAOConfig) {
        super(quickDAOConfig);
        this.connectionExecutor = new ConnectionExecutorImpl(quickDAOConfig);
    }

    @Override
    public int getCount(){
        int[] count = new int[1];
        connectionExecutor.returnGeneratedKeys(false)
                .name(name())
                .sql(getStatement())
                .parameters(getParameters())
                .executeQuery(resultSet->{
                    if(resultSet.next()){
                        count[0] = resultSet.getInt(1);
                    }
                });
        return count[0];
    }

    @Override
    public List getSingleColumnList(){
        List parameters = new ArrayList();
        connectionExecutor.returnGeneratedKeys(false)
                .name(name())
                .sql(getStatement())
                .parameters(getParameters())
                .executeQuery(resultSet->{
                    while(resultSet.next()){
                        parameters.add(resultSet.getObject(1));
                    }
                });
        return parameters;
    }

    @Override
    public JSONArray getArray(){
        throw new UnsupportedOperationException("当前不支持执行getArray方法");
    }

    @Override
    public int executeUpdate(){
        int effect = connectionExecutor.returnGeneratedKeys(false)
                .name(name())
                .sql(getStatement())
                .parameters(getParameters())
                .executeUpdate();
        return effect;
    }

    /**获取返回列表*/
    protected JSONArray getArray(Entity entity, String tableAliasName){
        JSONArray array = new JSONArray();
        connectionExecutor.returnGeneratedKeys(false)
                .name(name())
                .sql(getStatement())
                .parameters(getParameters())
                .executeQuery(resultSet->{
                    while (resultSet.next()) {
                        JSONObject object = ResponseUtil.getObject(entity, tableAliasName, resultSet, quickDAOConfig.databaseProvider);
                        array.add(object);
                    }
                });
        return array;
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
