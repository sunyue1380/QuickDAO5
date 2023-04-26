package cn.schoolwow.quickdao.util;

import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.domain.internal.Query;
import cn.schoolwow.quickdao.domain.internal.SubQuery;
import cn.schoolwow.quickdao.provider.DatabaseProvider;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 返回结果工具类
 */
public class ResponseUtil {
    /**
     * 将数据库结果集转化为JSONObject对象
     *
     * @param entity         实体类信息
     * @param tableAliasName 表别名
     * @param resultSet      结果集
     */
    public static JSONObject getObject(Entity entity, String tableAliasName, ResultSet resultSet, DatabaseProvider databaseProvider) throws SQLException {
        return getObject(entity, null, tableAliasName, resultSet, databaseProvider);
    }

    /**
     * 将数据库结果集转化为JSONObject对象
     *
     * @param entity           实体类信息
     * @param excludeFieldList 排除字段列表
     * @param tableAliasName   表别名
     * @param resultSet        结果集
     * @param databaseProvider 数据库提供者
     */
    public static JSONObject getObject(Entity entity, List<String> excludeFieldList, String tableAliasName, ResultSet resultSet, DatabaseProvider databaseProvider) throws SQLException {
        JSONObject subObject = new JSONObject(true);
        for (Property property : entity.properties) {
            if (null != excludeFieldList && (excludeFieldList.contains(property.name) || excludeFieldList.contains(property.column))) {
                continue;
            }
            String columnName = tableAliasName + "_" + property.column;
            String columnLabel = property.name == null ? property.column : property.name;
            if (null == property.className) {
                subObject.put(columnLabel, resultSet.getObject(columnName));
                continue;
            }
            Object value = null;
            switch (property.className) {
                case "byte": {
                    value = resultSet.getByte(columnName);
                }
                break;
                case "[B": {
                    value = resultSet.getBytes(columnName);
                }
                break;
                case "boolean": {
                    value = resultSet.getBoolean(columnName);
                }
                break;
                case "short": {
                    value = resultSet.getShort(columnName);
                }
                break;
                case "int": {
                    value = resultSet.getInt(columnName);
                }
                break;
                case "float": {
                    value = resultSet.getFloat(columnName);
                }
                break;
                case "long": {
                    value = resultSet.getLong(columnName);
                }
                break;
                case "double": {
                    value = resultSet.getDouble(columnName);
                }
                break;
                case "java.util.Date": {
                    switch (databaseProvider.name()) {
                        case "sqlite": {
                            value = resultSet.getString(columnName);
                        }
                        break;
                        default: {
                            java.sql.Date date = resultSet.getDate(columnName);
                            if (null != date) {
                                value = new Date(date.getTime());
                            }
                        }
                        break;
                    }
                }
                break;
                case "java.sql.Date": {
                    switch (databaseProvider.name()) {
                        case "sqlite": {
                            value = resultSet.getString(columnName);
                        }
                        break;
                        default: {
                            value = resultSet.getDate(columnName);
                        }
                        break;
                    }
                }
                break;
                case "java.sql.Time": {
                    switch (databaseProvider.name()) {
                        case "sqlite": {
                            value = resultSet.getString(columnName);
                        }
                        break;
                        default: {
                            value = resultSet.getTime(columnName);
                        }
                        break;
                    }
                }
                break;
                case "java.sql.Timestamp": {
                    switch (databaseProvider.name()) {
                        case "sqlite": {
                            value = resultSet.getString(columnName);
                        }
                        break;
                        default: {
                            value = resultSet.getTimestamp(columnName);
                        }
                        break;
                    }
                }
                break;
                case "java.time.LocalDate": {
                    switch (databaseProvider.name()) {
                        case "sqlite": {
                            String date = resultSet.getString(columnName);
                            if (null != date) {
                                value = LocalDate.parse(date, DateTimeFormatter.ISO_DATE);
                            }
                        }
                        break;
                        default: {
                            Date date = resultSet.getTimestamp(columnName);
                            if (null != date) {
                                value = Instant.ofEpochMilli(date.getTime()).atZone(ZoneId.systemDefault()).toLocalDate();
                            }
                        }
                        break;
                    }
                }
                break;
                case "java.time.LocalDateTime": {
                    switch (databaseProvider.name()) {
                        case "sqlite": {
                            String datetime = resultSet.getString(columnName);
                            if (null != datetime) {
                                value = LocalDateTime.parse(datetime, DateTimeFormatter.ISO_DATE_TIME);
                            }
                        }
                        break;
                        default: {
                            Date date = resultSet.getTimestamp(columnName);
                            if (null != date) {
                                value = Instant.ofEpochMilli(date.getTime()).atZone(ZoneId.systemDefault()).toLocalDateTime();
                            }
                        }
                        break;
                    }
                }
                break;
                case "java.sql.Array": {
                    value = resultSet.getArray(columnName);
                }
                break;
                case "java.math.BigDecimal": {
                    value = resultSet.getBigDecimal(columnName);
                }
                break;
                case "java.sql.Blob": {
                    value = resultSet.getBlob(columnName);
                }
                break;
                case "java.sql.Clob": {
                    value = resultSet.getClob(columnName);
                }
                break;
                case "java.sql.NClob": {
                    value = resultSet.getNClob(columnName);
                }
                break;
                case "java.sql.Ref": {
                    value = resultSet.getRef(columnName);
                }
                break;
                case "java.net.URL": {
                    value = resultSet.getURL(columnName);
                }
                break;
                case "java.sql.RowId": {
                    value = resultSet.getRowId(columnName);
                }
                break;
                case "java.sql.SQLXML": {
                    value = resultSet.getSQLXML(columnName);
                }
                break;
                case "java.io.InputStream": {
                    value = resultSet.getBinaryStream(columnName);
                }
                break;
                case "java.io.Reader": {
                    value = resultSet.getCharacterStream(columnName);
                }
                break;
                default: {
                    value = resultSet.getObject(columnName);
                }
            }
            subObject.put(columnLabel, value);
        }
        return subObject;
    }

    /**从ResultSet中获取记录*/
    public static void getRawSelectArray(ResultSet resultSet, JSONArray array) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        String[] columnLables = new String[metaData.getColumnCount()];
        for (int i = 1; i <= columnLables.length; i++) {
            columnLables[i - 1] = metaData.getColumnLabel(i);
        }
        while (resultSet.next()) {
            JSONObject o = new JSONObject();
            for (int i = 1; i <= columnLables.length; i++) {
                o.put(columnLables[i - 1], resultSet.getObject(i));
            }
            array.add(o);
        }
    }

    /**获取响应结果列表*/
    public static void getResponseArray(ResultSet resultSet, Query query, JSONArray array) throws SQLException {
        if(query.column.isEmpty()){
            //如果用户未手动指定列名
            while (resultSet.next()) {
                JSONObject o = getObject(query.entity, query.excludeColumnList, query.tableAliasName, resultSet, query.quickDAOConfig.databaseProvider);
                if (query.compositField) {
                    getCompositObject(resultSet, query, o);
                }
                array.add(o);
            }
        }else{
            //如果用户手动指定列名
            if (null == query.columnTypeMapping) {
                query.columnTypeMapping = query.quickDAOConfig.queryColumnTypeMapping;
            }

            ResultSetMetaData metaData = resultSet.getMetaData();
            Property[] properties = new Property[metaData.getColumnCount()];
            for (int i = 1; i <= properties.length; i++) {
                properties[i - 1] = new Property();
                properties[i - 1].columnLabel = metaData.getColumnLabel(i);
                properties[i - 1].column = metaData.getColumnName(i);
                properties[i - 1].columnType = metaData.getColumnTypeName(i);
                properties[i - 1].className = metaData.getColumnClassName(i);
                if (null != query.columnTypeMapping) {
                    Class type = query.columnTypeMapping.apply(properties[i - 1]);
                    if (null != type) {
                        properties[i - 1].clazz = type;
                    }
                }
            }

            while (resultSet.next()) {
                JSONObject o = new JSONObject(true);
                for (int i = 1; i <= properties.length; i++) {
                    if (null == properties[i - 1].clazz) {
                        o.put(properties[i - 1].columnLabel, resultSet.getObject(i));
                    } else {
                        o.put(properties[i - 1].columnLabel, resultSet.getObject(i, properties[i - 1].clazz));
                    }
                }
                array.add(o);
            }
        }
    }

    /**
     * 获取复杂对象
     *
     * @param resultSet 结果集
     * @param query 查询信息
     * @param o         复杂对象
     */
    private static void getCompositObject(ResultSet resultSet, Query query, JSONObject o) throws SQLException {
        for (SubQuery subQuery : query.subQueryList) {
            if (null == subQuery.compositField || subQuery.compositField.isEmpty()) {
                continue;
            }
            JSONObject subObject = ResponseUtil.getObject(subQuery.entity, subQuery.excludeColumnList, subQuery.tableAliasName, resultSet, query.quickDAOConfig.databaseProvider);
            SubQuery parentSubQuery = subQuery.parentSubQuery;
            if (parentSubQuery == null) {
                o.put(subQuery.compositField, subObject);
            } else {
                List<String> fieldNames = new ArrayList<>();
                while (parentSubQuery != null) {
                    fieldNames.add(parentSubQuery.compositField);
                    parentSubQuery = parentSubQuery.parentSubQuery;
                }
                JSONObject oo = o;
                for (int i = fieldNames.size() - 1; i >= 0; i--) {
                    oo = oo.getJSONObject(fieldNames.get(i));
                }
                oo.put(subQuery.compositField, subObject);
            }
        }
    }
}
