package cn.schoolwow.quickdao.util;

import cn.schoolwow.quickdao.domain.external.Entity;
import cn.schoolwow.quickdao.domain.external.Property;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Date;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class ParametersUtil {
    private static Logger logger = LoggerFactory.getLogger(ParametersUtil.class);
    /**
     * SQL参数占位符
     */
    private static final String PLACEHOLDER = "** NOT SPECIFIED **";
    /**
     * 格式化旧版本的java.sql.Date类型
     */
    private final static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    /**
     * 格式化旧版本的java.sql.Time类型
     */
    private final static SimpleDateFormat simpleTimeFormat = new SimpleDateFormat("HH:mm:ss");
    /**
     * 格式化旧版本的Timestampt类型
     */
    private final static SimpleDateFormat simpleDateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
    /**
     * 格式化日期参数
     */
    private final static DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    /**
     * 格式化日期参数
     */
    private final static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * 设置主键自增id值
     *
     * @param instance      实例
     * @param entity        实体类
     * @param generatedKeys 自增值
     */
    public static void setGeneratedKeysValue(Object instance, Entity entity, String generatedKeys) {
        Field idField = getFieldFromInstance(instance, entity.id.name);
        try {
            switch (idField.getType().getName()) {
                case "int": {
                    idField.setInt(instance, Integer.parseInt(generatedKeys));
                }
                break;
                case "java.lang.Integer": {
                    idField.set(instance, Integer.valueOf(generatedKeys));
                }
                break;
                case "long": {
                    idField.setLong(instance, Long.parseLong(generatedKeys));
                }
                break;
                case "java.lang.Long": {
                    idField.set(instance, Long.valueOf(generatedKeys));
                }
                break;
                case "java.lang.String": {
                    idField.set(instance, generatedKeys);
                }
                break;
                default: {
                    throw new IllegalArgumentException("当前仅支持int,long,String类型的自增主键!自增字段名称:" + idField.getName() + ",类型:" + idField.getType().getName() + "!");
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("设置主键自增发生异常", e);
        }
    }

    /**
     * 设置字段值为当前日期
     *
     * @param property 字段属性
     * @param instance 实例
     */
    public static void setCurrentDateTime(Property property, Object instance) {
        Field field = getFieldFromInstance(instance, property.name);
        try {
            Object value = field.get(instance);
            //当用户手动设置该字段时,则程序不再注入时间
            if (null != value) {
                return;
            }
            switch (property.className) {
                case "java.util.Date": {
                    field.set(instance, new java.util.Date(System.currentTimeMillis()));
                }
                break;
                case "java.sql.Date": {
                    field.set(instance, new java.sql.Date(System.currentTimeMillis()));
                }
                break;
                case "java.sql.Timestamp": {
                    field.set(instance, new Timestamp(System.currentTimeMillis()));
                }
                break;
                case "java.util.Calendar": {
                    field.set(instance, Calendar.getInstance());
                }
                break;
                case "java.time.LocalDate": {
                    field.set(instance, LocalDate.now());
                }
                break;
                case "java.time.LocalDateTime": {
                    field.set(instance, LocalDateTime.now());
                }
                break;
                default: {
                    throw new IllegalArgumentException("不支持该日期类型,目前支持的类型为Date,Calendar,LocalDate,LocalDateTime!当前类型:" + property.className);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("设置当前时间异常", e);
        }
    }

    /**
     * 替换SQL语句
     *
     * @param sql        语句
     * @param parameters 参数
     */
    public static String replaceStatementPlaceholder(String sql, Collection parameters) {
        StringBuilder sqlBuilder = new StringBuilder(sql.replace("?", PLACEHOLDER));
        if(null!=parameters&&!parameters.isEmpty()){
            for (Object parameter : parameters) {
                String parameterPlaceHolder = getParameterPlaceHolder(parameter);
                int indexOf = sqlBuilder.indexOf(PLACEHOLDER);
                sqlBuilder.replace(indexOf, indexOf + PLACEHOLDER.length(), parameterPlaceHolder);
            }
        }
        String formatSQL = sqlBuilder.toString();
        return formatSQL;
    }

    /**
     * 设置预处理语句参数
     *
     * @param ps           预处理语句
     * @param parameters   参数
     * @param databaseName 数据库名
     */
    public static void setPrepareStatementParameter(PreparedStatement ps, Collection parameters, String databaseName) throws SQLException {
        if (null == parameters) {
            return;
        }
        int parameterIndex = 1;
        Iterator iterator = parameters.iterator();
        while (iterator.hasNext()) {
            Object parameter = iterator.next();
            if (null == parameter) {
                ps.setObject(parameterIndex, null);
            } else {
                switch (parameter.getClass().getName()) {
                    case "byte": {
                        ps.setByte(parameterIndex, (byte) parameter);
                    }
                    break;
                    case "[B": {
                        ps.setBytes(parameterIndex, (byte[]) parameter);
                    }
                    break;
                    case "boolean": {
                        boolean value = (boolean) parameter;
                        ps.setBoolean(parameterIndex, value);
                    }
                    break;
                    case "short": {
                        ps.setShort(parameterIndex, (short) parameter);
                    }
                    break;
                    case "int": {
                        ps.setInt(parameterIndex, (int) parameter);
                    }
                    break;
                    case "float": {
                        ps.setFloat(parameterIndex, (float) parameter);
                    }
                    break;
                    case "long": {
                        ps.setLong(parameterIndex, (long) parameter);
                    }
                    break;
                    case "double": {
                        ps.setDouble(parameterIndex, (double) parameter);
                    }
                    break;
                    case "java.lang.String": {
                        ps.setString(parameterIndex, (String) parameter);
                    }
                    break;
                    case "java.util.Date": {
                        java.util.Date date = (java.util.Date) parameter;
                        ps.setDate(parameterIndex, new Date(date.getTime()));
                    }
                    break;
                    case "java.sql.Date": {
                        Date date = (Date) parameter;
                        ps.setDate(parameterIndex, date);
                    }
                    break;
                    case "java.sql.Time": {
                        Time time = (Time) parameter;
                        ps.setTime(parameterIndex, time);
                    }
                    break;
                    case "java.sql.Timestamp": {
                        Timestamp timestamp = (Timestamp) parameter;
                        ps.setTimestamp(parameterIndex, timestamp);
                    }
                    break;
                    case "java.time.LocalDate": {
                        LocalDate localDate = (LocalDate) parameter;
                        switch (databaseName.toLowerCase()) {
                            case "oracle": {
                                //oracle不支持直接设置LocalDate类型
                                ZonedDateTime zonedDateTime = localDate.atStartOfDay(ZoneId.systemDefault());
                                Date date = new Date(Date.from(zonedDateTime.toInstant()).getTime());
                                ps.setObject(parameterIndex, date);
                            }
                            break;
                            default: {
                                ps.setObject(parameterIndex, localDate);
                            }
                            break;
                        }
                    }
                    break;
                    case "java.time.LocalDateTime": {
                        LocalDateTime localDateTime = (LocalDateTime) parameter;
                        switch (databaseName.toLowerCase()) {
                            case "oracle": {
                                //oracle不支持直接设置LocalDateTime类型
                                ZoneId zoneId = ZoneId.systemDefault();
                                Instant instant = localDateTime.atZone(zoneId).toInstant();
                                Timestamp timestamp = new Timestamp(instant.toEpochMilli());
                                ps.setObject(parameterIndex, timestamp);
                            }
                            break;
                            default: {
                                ps.setObject(parameterIndex, localDateTime);
                            }
                            break;
                        }
                    }
                    break;
                    case "java.sql.Array": {
                        ps.setArray(parameterIndex, (Array) parameter);
                    }
                    break;
                    case "java.math.BigDecimal": {
                        ps.setBigDecimal(parameterIndex, (BigDecimal) parameter);
                    }
                    break;
                    case "java.sql.Blob": {
                        ps.setBlob(parameterIndex, (Blob) parameter);
                    }
                    break;
                    case "java.sql.Clob": {
                        ps.setClob(parameterIndex, (Clob) parameter);
                    }
                    break;
                    case "java.sql.NClob": {
                        ps.setNClob(parameterIndex, (NClob) parameter);
                    }
                    break;
                    case "java.sql.Ref": {
                        ps.setRef(parameterIndex, (Ref) parameter);
                    }
                    break;
                    case "java.net.URL": {
                        ps.setURL(parameterIndex, (URL) parameter);
                    }
                    break;
                    case "java.sql.RowId": {
                        ps.setRowId(parameterIndex, (RowId) parameter);
                    }
                    break;
                    case "java.sql.SQLXML": {
                        ps.setSQLXML(parameterIndex, (SQLXML) parameter);
                    }
                    break;
                    case "java.io.InputStream": {
                        ps.setBinaryStream(parameterIndex, (InputStream) parameter);
                    }
                    break;
                    case "java.io.Reader": {
                        ps.setCharacterStream(parameterIndex, (Reader) parameter);
                    }
                    break;
                    default: {
                        try {
                            ps.setObject(parameterIndex, parameter);
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
            logger.trace("设置参数,索引:{},值:{}", parameterIndex, parameter);
            parameterIndex++;
        }
    }

    /**
     * 获取参数占位符
     *
     * @param parameter 参数值
     */
    private static String getParameterPlaceHolder(Object parameter) {
        if (null == parameter) {
            return "null";
        }
        String parameterSQL = parameter.toString();
        switch (parameter.getClass().getName()) {
            case "boolean": {
                boolean value = (boolean) parameter;
                parameterSQL = value ? "1" : "0";
            }
            break;
            case "java.lang.String": {
                parameterSQL = "'" + parameter.toString() + "'";
            }
            break;
            case "java.util.Date": {
                java.util.Date date = (java.util.Date) parameter;
                parameterSQL = "'" + simpleDateFormat.format(date) + "'";
            }
            break;
            case "java.sql.Date": {
                Date date = (Date) parameter;
                parameterSQL = "'" + simpleDateFormat.format(date) + "'";
            }
            break;
            case "java.sql.Time": {
                Time time = (Time) parameter;
                parameterSQL = "'" + simpleTimeFormat.format(time) + "'";
            }
            break;
            case "java.sql.Timestamp": {
                Timestamp timestamp = (Timestamp) parameter;
                parameterSQL = "'" + simpleDateTimeFormat.format(timestamp) + "'";
            }
            break;
            case "java.time.LocalDate": {
                LocalDate localDate = (LocalDate) parameter;
                parameterSQL = "'" + localDate.format(dateFormatter) + "'";
            }
            break;
            case "java.time.LocalDateTime": {
                LocalDateTime localDateTime = (LocalDateTime) parameter;
                parameterSQL = "'" + localDateTime.format(dateTimeFormatter) + "'";
            }
            break;
        }
        return parameterSQL;
    }

    /**
     * 从实例数组中获取字段值
     *
     * @param instances 实例列表
     * @param name 字段名
     */
    public static List getFieldValueListFromInstance(Object[] instances, String name) {
        Field field = getFieldFromInstance(instances[0], name);
        List valueList = new ArrayList(instances.length);
        for(int i=0;i<instances.length;i++){
            Object value = null;
            try {
                value = field.get(instances[i]);
            } catch (IllegalAccessException e) {
                throw new RuntimeException("获取实例字段值失败", e);
            }
            valueList.add(value);
        }
        return valueList;
    }

    /**
     * 从实例从获取字段值
     *
     * @param instance 实例
     * @param name 字段名
     */
    public static Object getFieldValueFromInstance(Object instance, String name) {
        Field field = getFieldFromInstance(instance, name);
        try {
            Object value = field.get(instance);
            return value;
        } catch (IllegalAccessException e) {
            throw new RuntimeException("获取实例字段值失败", e);
        }
    }

    /**
     * 从实例从获取字段信息
     *
     * @param instance 实例
     * @param name 字段名
     */
    public static Field getFieldFromInstance(Object instance, String name) {
        Class tempClass = instance.getClass();
        Field field = null;
        while (null == field && null != tempClass) {
            Field[] fields = tempClass.getDeclaredFields();
            for (Field field1 : fields) {
                if (field1.getName().equals(name)) {
                    field = field1;
                    break;
                }
            }
            tempClass = tempClass.getSuperclass();
        }
        if (null == field) {
            throw new IllegalArgumentException("字段不存在!字段名:" + name + ",类名:" + instance.getClass().getName());
        }
        field.setAccessible(true);
        return field;
    }
}
