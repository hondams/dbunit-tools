package com.github.hondams.dbunit.tool.util;

import java.util.Map;
import lombok.experimental.UtilityClass;

@UtilityClass
public class DatabaseUtils {

    private Map<Integer, String> SQL_TYPE_NAME_MAP = Map.ofEntries(//
        Map.entry(java.sql.Types.ARRAY, "ARRAY"),//
        Map.entry(java.sql.Types.BIGINT, "BIGINT"),//
        Map.entry(java.sql.Types.BINARY, "BINARY"),//
        Map.entry(java.sql.Types.BIT, "BIT"),//
        Map.entry(java.sql.Types.BLOB, "BLOB"),//
        Map.entry(java.sql.Types.BOOLEAN, "BOOLEAN"),//
        Map.entry(java.sql.Types.CHAR, "CHAR"),//
        Map.entry(java.sql.Types.CLOB, "CLOB"),//
        Map.entry(java.sql.Types.DATALINK, "DATALINK"),//
        Map.entry(java.sql.Types.DATE, "DATE"),//
        Map.entry(java.sql.Types.DECIMAL, "DECIMAL"),//
        Map.entry(java.sql.Types.DISTINCT, "DISTINCT"),//
        Map.entry(java.sql.Types.DOUBLE, "DOUBLE"),//
        Map.entry(java.sql.Types.FLOAT, "FLOAT"),//
        Map.entry(java.sql.Types.INTEGER, "INTEGER"),//
        Map.entry(java.sql.Types.JAVA_OBJECT, "JAVA_OBJECT"),//
        Map.entry(java.sql.Types.LONGNVARCHAR, "LONGNVARCHAR"),//
        Map.entry(java.sql.Types.LONGVARBINARY, "LONGVARBINARY"),//
        Map.entry(java.sql.Types.LONGVARCHAR, "LONGVARCHAR"),//
        Map.entry(java.sql.Types.NCHAR, "NCHAR"),//
        Map.entry(java.sql.Types.NCLOB, "NCLOB"),//
        Map.entry(java.sql.Types.NULL, "NULL"),//
        Map.entry(java.sql.Types.NUMERIC, "NUMERIC"),//
        Map.entry(java.sql.Types.NVARCHAR, "NVARCHAR"),//
        Map.entry(java.sql.Types.OTHER, "OTHER"),//
        Map.entry(java.sql.Types.REAL, "REAL"),//
        Map.entry(java.sql.Types.REF, "REF"),//
        Map.entry(java.sql.Types.ROWID, "ROWID"),//
        Map.entry(java.sql.Types.SMALLINT, "SMALLINT"),//
        Map.entry(java.sql.Types.SQLXML, "SQLXML"),//
        Map.entry(java.sql.Types.STRUCT, "STRUCT"),//
        Map.entry(java.sql.Types.TIME, "TIME"),//
        Map.entry(java.sql.Types.TIME_WITH_TIMEZONE, "TIME_WITH_TIMEZONE"),//
        Map.entry(java.sql.Types.TIMESTAMP, "TIMESTAMP"),//
        Map.entry(java.sql.Types.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP_WITH_TIMEZONE"),//
        Map.entry(java.sql.Types.TINYINT, "TINYINT"),//
        Map.entry(java.sql.Types.VARBINARY, "VARBINARY"),//
        Map.entry(java.sql.Types.VARCHAR, "VARCHAR")//

    );

    public String getSqlTypeName(int sqlType) {
        return SQL_TYPE_NAME_MAP.get(sqlType);
    }
}
