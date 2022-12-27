package cn.schoolwow.quickdao.util;

import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.Property;
import cn.schoolwow.quickdao.provider.DatabaseProvider;
import com.alibaba.fastjson.JSONObject;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
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
}
