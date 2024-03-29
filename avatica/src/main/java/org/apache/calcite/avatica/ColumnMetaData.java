/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.avatica;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.DatabaseMetaData;
import java.sql.ResultSetMetaData;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Metadata for a column.
 * (Compare with {@link ResultSetMetaData}.)
 * 代表一个列对象的元数据
 */
public class ColumnMetaData {
  public final int ordinal; // 0-based 第几个列 从0开始计数
  public final boolean autoIncrement;//是否自增长
  public final boolean caseSensitive;//大小写忽略
  public final boolean searchable;//指示是否可以在 where 子句中使用指定的列。
  public final boolean currency;//指示指定的列是否是一个哈希代码值 基本无用
  public final int nullable;//该列是否允许是null
  public final boolean signed;//指示指定列中的值是否带正负号。
  public final int displaySize;//显示的字符大小  基本无用
  public final String label;//作为as的别名
  public final String columnName;//列名
  public final String schemaName;
  public final int precision;//数据精准度
  public final int scale;//数据精准度
  public final String tableName;//来自哪张表
  public final String catalogName;//来自哪个数据库
  public final boolean readOnly;//该字段是否只读
  public final boolean writable;//指示在指定的列上进行写操作是否可以获得成功。

  public final boolean definitelyWritable;//基本无用
  public final String columnClassName;//数据库类型对应的java全路径,比如是string类型还是integer类型
  public final AvaticaType type;//数据库类型

  @JsonCreator
  public ColumnMetaData(
      @JsonProperty("ordinal") int ordinal,
      @JsonProperty("autoIncrement") boolean autoIncrement,
      @JsonProperty("caseSensitive") boolean caseSensitive,
      @JsonProperty("searchable") boolean searchable,
      @JsonProperty("currency") boolean currency,
      @JsonProperty("nullable") int nullable,
      @JsonProperty("signed") boolean signed,
      @JsonProperty("displaySize") int displaySize,
      @JsonProperty("label") String label,
      @JsonProperty("columnName") String columnName,
      @JsonProperty("schemaName") String schemaName,
      @JsonProperty("precision") int precision,
      @JsonProperty("scale") int scale,
      @JsonProperty("tableName") String tableName,
      @JsonProperty("catalogName") String catalogName,
      @JsonProperty("type") AvaticaType type,
      @JsonProperty("readOnly") boolean readOnly,
      @JsonProperty("writable") boolean writable,
      @JsonProperty("definitelyWritable") boolean definitelyWritable,
      @JsonProperty("columnClassName") String columnClassName) {
    this.ordinal = ordinal;
    this.autoIncrement = autoIncrement;
    this.caseSensitive = caseSensitive;
    this.searchable = searchable;
    this.currency = currency;
    this.nullable = nullable;
    this.signed = signed;
    this.displaySize = displaySize;
    this.label = label;
    // Per the JDBC spec this should be just columnName.
    // For example, the query
    //     select 1 as x, c as y from t
    // should give columns
    //     (label=x, column=null, table=null)
    //     (label=y, column=c table=t)
    // But DbUnit requires every column to have a name. Duh.
    this.columnName = first(columnName, label);
    this.schemaName = schemaName;
    this.precision = precision;
    this.scale = scale;
    this.tableName = tableName;
    this.catalogName = catalogName;
    this.type = type;
    this.readOnly = readOnly;
    this.writable = writable;
    this.definitelyWritable = definitelyWritable;
    this.columnClassName = columnClassName;
  }

  private static <T> T first(T t0, T t1) {
    return t0 != null ? t0 : t1;
  }

  /** Creates a {@link ScalarType}. */
  public static ScalarType scalar(int type, String typeName, Rep rep) {
    return new ScalarType(type, typeName, rep);
  }

  /** Creates a {@link StructType}. */
  public static StructType struct(List<ColumnMetaData> columns) {
    return new StructType(columns);
  }

  /** Creates an {@link ArrayType}. */
  public static ArrayType array(AvaticaType componentType, String typeName,
      Rep rep) {
    return new ArrayType(Types.ARRAY, typeName, rep, componentType);
  }

  /** Creates a ColumnMetaData for result sets that are not based on a struct
   * but need to have a single 'field' for purposes of
   * {@link ResultSetMetaData}. */
  public static ColumnMetaData dummy(AvaticaType type, boolean nullable) {
    return new ColumnMetaData(
        0,
        false,
        true,
        false,
        false,
        nullable
            ? DatabaseMetaData.columnNullable
            : DatabaseMetaData.columnNoNulls,
        true,
        -1,
        null,
        null,
        null,
        -1,
        -1,
        null,
        null,
        type,
        true,
        false,
        false,
        type.columnClassName());
  }

  /** Description of the type used to internally represent a value. For example,
   * a {@link java.sql.Date} might be represented as a {@link #PRIMITIVE_INT}
   * if not nullable, or a {@link #JAVA_SQL_DATE}. */
  public enum Rep {
    PRIMITIVE_BOOLEAN(boolean.class),
    PRIMITIVE_BYTE(byte.class),
    PRIMITIVE_CHAR(char.class),
    PRIMITIVE_SHORT(short.class),
    PRIMITIVE_INT(int.class),
    PRIMITIVE_LONG(long.class),
    PRIMITIVE_FLOAT(float.class),
    PRIMITIVE_DOUBLE(double.class),
    BOOLEAN(Boolean.class),
    BYTE(Byte.class),
    CHARACTER(Character.class),
    SHORT(Short.class),
    INTEGER(Integer.class),
    LONG(Long.class),
    FLOAT(Float.class),
    DOUBLE(Double.class),
    JAVA_SQL_TIME(Time.class),
    JAVA_SQL_TIMESTAMP(Timestamp.class),
    JAVA_SQL_DATE(java.sql.Date.class),
    JAVA_UTIL_DATE(java.util.Date.class),
    STRING(String.class),
    OBJECT(Object.class);

    public final Class clazz;

    public static final Map<Class, Rep> VALUE_MAP;

    static {
      Map<Class, Rep> builder = new HashMap<Class, Rep>();
      for (Rep rep : values()) {
        builder.put(rep.clazz, rep);
      }
      VALUE_MAP = Collections.unmodifiableMap(builder);
    }

    Rep(Class clazz) {
      this.clazz = clazz;
    }

    public static Rep of(Type clazz) {
      //noinspection SuspiciousMethodCalls
      final Rep rep = VALUE_MAP.get(clazz);
      return rep != null ? rep : OBJECT;
    }
  }

  /** Base class for a column type. */
  @JsonTypeInfo(
      use = JsonTypeInfo.Id.NAME,
      property = "type",
      defaultImpl = ScalarType.class)
  @JsonSubTypes({
      @JsonSubTypes.Type(value = ScalarType.class, name = "scalar"),
      @JsonSubTypes.Type(value = StructType.class, name = "struct"),
      @JsonSubTypes.Type(value = ArrayType.class, name = "array") })
  public static class AvaticaType {
    public final int type;
    public final String typeName;

    /** The type of the field that holds the value. Not a JDBC property. */
    public final Rep representation;

    protected AvaticaType(int type, String typeName, Rep representation) {
      this.type = type;
      this.typeName = typeName;
      this.representation = representation;
      assert representation != null;
    }

    public String columnClassName() {
      return SqlType.valueOf(type).clazz.getName();
    }
  }

  /** Scalar type. */
  public static class ScalarType extends AvaticaType {
    @JsonCreator
    public ScalarType(@JsonProperty("type") int type,
        @JsonProperty("typeName") String typeName,
        @JsonProperty("representation") Rep representation) {
      super(type, typeName, representation);
    }
  }

  /** Record type. */
  public static class StructType extends AvaticaType {
    public final List<ColumnMetaData> columns;//列集合

    @JsonCreator
    public StructType(List<ColumnMetaData> columns) {
      super(Types.STRUCT, "STRUCT", ColumnMetaData.Rep.OBJECT);
      this.columns = columns;
    }
  }

  /** Array type. */
  public static class ArrayType extends AvaticaType {
    public final AvaticaType component;

    private ArrayType(int type, String typeName, Rep representation,
        AvaticaType component) {
      super(type, typeName, representation);
      this.component = component;
    }
  }

  /** Extends the information in {@link java.sql.Types}. */
  private enum SqlType {
    BOOLEAN(Types.BOOLEAN, Boolean.class),
    TINYINT(Types.TINYINT, Byte.class),
    SMALLINT(Types.SMALLINT, Short.class),
    INTEGER(Types.INTEGER, Integer.class),
    BIGINT(Types.BIGINT, Long.class),
    DECIMAL(Types.DECIMAL, BigDecimal.class),
    FLOAT(Types.FLOAT, Float.class),
    REAL(Types.REAL, Float.class),
    DOUBLE(Types.DOUBLE, Double.class),
    DATE(Types.DATE, java.sql.Date.class),
    TIME(Types.TIME, Time.class),
    TIMESTAMP(Types.TIMESTAMP, Timestamp.class),
    INTERVAL_YEAR_MONTH(Types.OTHER, Boolean.class),
    INTERVAL_DAY_TIME(Types.OTHER, Boolean.class),
    CHAR(Types.CHAR, String.class),
    VARCHAR(Types.VARCHAR, String.class),
    BINARY(Types.BINARY, byte[].class),
    VARBINARY(Types.VARBINARY, byte[].class),
    NULL(Types.NULL, Void.class),
    ANY(Types.JAVA_OBJECT, Object.class),
    SYMBOL(Types.OTHER, Object.class),
    MULTISET(Types.ARRAY, List.class),
    ARRAY(Types.ARRAY, Array.class),
    MAP(Types.OTHER, Map.class),
    DISTINCT(Types.DISTINCT, Object.class),
    STRUCTURED(Types.STRUCT, Object.class),
    ROW(Types.STRUCT, Object.class),
    OTHER(Types.OTHER, Object.class),
    CURSOR(2012, Object.class),
    COLUMN_LIST(Types.OTHER + 2, Object.class);

    private final int type;
    private final Class clazz;

    private static final Map<Integer, SqlType> BY_ID =
        new HashMap<Integer, SqlType>();
    static {
      for (SqlType sqlType : values()) {
        BY_ID.put(sqlType.type, sqlType);
      }
    }

    SqlType(int type, Class clazz) {
      this.type = type;
      this.clazz = clazz;
    }

    public static SqlType valueOf(int type) {
      return BY_ID.get(type);
    }
  }
}

// End ColumnMetaData.java
