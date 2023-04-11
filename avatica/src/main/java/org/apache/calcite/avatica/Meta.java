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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Command handler for getting various metadata. Should be implemented by each
 * driver.
 *
 * <p>Also holds other abstract methods that are not related to metadata
 * that each provider must implement. This is not ideal.</p>
 * 获取数据库级别的元数据信息---每一个connect都可以关联到该对象
 */
public interface Meta {

    /**
     * Returns comma-separated list of JDBC keywords.
     * 返回逗号分隔的关键词集合
     */
  String getSqlKeywords();//所有系统支持的关键词

    /**
     * SqlJdbcFunctionCall.NUMERIC_FUNCTIONS
     * constructFuncList(
     *       "ABS", "ACOS", "ASIN", "ATAN", "ATAN2", "CEILING", "COS", "COT",
     *       "DEGREES", "EXP", "FLOOR", "LOG", "LOG10", "MOD", "PI",
     *       "POWER", "RADIANS", "RAND", "ROUND", "SIGN", "SIN", "SQRT",
     *       "TAN", "TRUNCATE")
     * @return 逗号分隔的数学函数
     */
  String getNumericFunctions();//所有数字处理的函数

    /**
     * constructFuncList(
     *       "ASCII", "CHAR", "CONCAT", "DIFFERENCE", "INSERT", "LCASE",
     *       "LEFT", "LENGTH", "LOCATE", "LTRIM", "REPEAT", "REPLACE",
     *       "RIGHT", "RTRIM", "SOUNDEX", "SPACE", "SUBSTRING", "UCASE")
     * @return 逗号分隔的字符串处理函数
     */
  String getStringFunctions();//所有字符串处理的函数

    /**
     *
     * constructFuncList(
     *       "DATABASE", "IFNULL", "USER"
     * @return 逗号分隔的系统函数
     */
  String getSystemFunctions();//所有系统函数

    /**
     * constructFuncList(
     *       "CURDATE", "CURTIME", "DAYNAME", "DAYOFMONTH", "DAYOFWEEK",
     *       "DAYOFYEAR", "HOUR", "MINUTE", "MONTH", "MONTHNAME", "NOW",
     *       "QUARTER", "SECOND", "TIMESTAMPADD", "TIMESTAMPDIFF",
     *       "WEEK", "YEAR");
     * @return 逗号分隔的时间日期函数
     */
  String getTimeDateFunctions();//所有日期函数

  //所有的表信息
  MetaResultSet getTables(String catalog,
      Pat schemaPattern,
      Pat tableNamePattern,
      List<String> typeList);

  //所有的列信息
  MetaResultSet getColumns(String catalog,
      Pat schemaPattern,
      Pat tableNamePattern,
      Pat columnNamePattern);

  //所有的schema信息,一般情况是空
  MetaResultSet getSchemas(String catalog, Pat schemaPattern);

  //所有的catalog信息
  MetaResultSet getCatalogs();

  //所有的table类型--目前只支持table和view
  MetaResultSet getTableTypes();

  MetaResultSet getProcedures(String catalog,
      Pat schemaPattern,
      Pat procedureNamePattern);

  MetaResultSet getProcedureColumns(String catalog,
      Pat schemaPattern,
      Pat procedureNamePattern,
      Pat columnNamePattern);

  MetaResultSet getColumnPrivileges(String catalog,
      String schema,
      String table,
      Pat columnNamePattern);

  MetaResultSet getTablePrivileges(String catalog,
      Pat schemaPattern,
      Pat tableNamePattern);

  MetaResultSet getBestRowIdentifier(String catalog,
      String schema,
      String table,
      int scope,
      boolean nullable);

  MetaResultSet getVersionColumns(String catalog, String schema, String table);

  MetaResultSet getPrimaryKeys(String catalog, String schema, String table);

  MetaResultSet getImportedKeys(String catalog, String schema, String table);

  MetaResultSet getExportedKeys(String catalog, String schema, String table);

  MetaResultSet getCrossReference(String parentCatalog,
      String parentSchema,
      String parentTable,
      String foreignCatalog,
      String foreignSchema,
      String foreignTable);

  MetaResultSet getTypeInfo();

  MetaResultSet getIndexInfo(String catalog,
      String schema,
      String table,
      boolean unique,
      boolean approximate);

  MetaResultSet getUDTs(String catalog,
      Pat schemaPattern,
      Pat typeNamePattern,
      int[] types);

  MetaResultSet getSuperTypes(String catalog,
      Pat schemaPattern,
      Pat typeNamePattern);

  MetaResultSet getSuperTables(String catalog,
      Pat schemaPattern,
      Pat tableNamePattern);

  MetaResultSet getAttributes(String catalog,
      Pat schemaPattern,
      Pat typeNamePattern,
      Pat attributeNamePattern);

  MetaResultSet getClientInfoProperties();

  MetaResultSet getFunctions(String catalog,
      Pat schemaPattern,
      Pat functionNamePattern);

  MetaResultSet getFunctionColumns(String catalog,
      Pat schemaPattern,
      Pat functionNamePattern,
      Pat columnNamePattern);

  MetaResultSet getPseudoColumns(String catalog,
      Pat schemaPattern,
      Pat tableNamePattern,
      Pat columnNamePattern);

  /** Creates an iterable for a result set.
   *
   * <p>The default implementation just returns {@code iterable}, which it
   * requires to be not null; derived classes may instead choose to execute the
   * relational expression in {@code signature}.
   * 针对一个已经解析好的sql元数据对象Signature,产生迭代器集合
   **/
  Iterable<Object> createIterable(StatementHandle handle, Signature signature,
      Iterable<Object> iterable);

  /** Prepares a statement.
   *  解析一个sql
   * @param h Statement handle
   * @param sql SQL query
   * @param maxRowCount Negative for no limit (different meaning than JDBC)
   * @return Signature of prepared statement
   */
  Signature prepare(StatementHandle h, String sql, int maxRowCount);

  /** Prepares and executes a statement.
   *
   * @param h Statement handle
   * @param sql SQL query
   * @param maxRowCount Negative for no limit (different meaning than JDBC)
   * @param callback Callback to lock, clear and assign cursor
   * @return Signature of prepared statement
   * 解析一个sql,并且执行该sql
   */
  MetaResultSet prepareAndExecute(StatementHandle h, String sql,
      int maxRowCount, PrepareCallback callback);

  /** Called during the creation of a statement to allocate a new handle.
   *
   * @param ch Connection handle
   */
  StatementHandle createStatement(ConnectionHandle ch);

  /** Factory to create instances of {@link Meta}. */
  interface Factory {
    Meta create(List<String> args); //给定参数,如何创建一个Meta工厂
  }

  /** Wrapper to remind API calls that a parameter is a pattern (allows '%' and
   * '_' wildcards, per the JDBC spec) rather than a string to be matched
   * exactly.
   * 就是一个字符串,但字符串有更特殊的意义，是一个包装器,支持%与_这样的通配符
   **/
  class Pat {
    public final String s;

    private Pat(String s) {
      this.s = s;
    }

    @JsonCreator
    public static Pat of(@JsonProperty("s") String name) {
      return new Pat(name);
    }
  }

  /** Meta data from which a result set can be constructed.
   * 结果集包含以下信息:
   * 1.属于哪个statement,从而知道属于哪个connect。
   * 2.结果集的迭代器集合
   * 3.结果集的元数据信息，包含查询sql、列集合元数据信息、如何将一行迭代器信息转换成正确的格式提取数据
   **/
  class MetaResultSet {
    public final int statementId;//Statement的唯一id,即connect中创建的第几个Statement对象
    public final boolean ownStatement;
    public final Iterable<Object> iterable;//集合,每一个元素存储的只有每一个元素具体的值
    public final Signature signature;//描述每一个元素的列schema等信息

    public MetaResultSet(int statementId, boolean ownStatement,
        Signature signature, Iterable<Object> iterable) {
      this.signature = signature;
      this.statementId = statementId;
      this.ownStatement = ownStatement;
      this.iterable = iterable;
    }
  }

  /** Information necessary to convert an {@link Iterable} into a
   * {@link org.apache.calcite.avatica.util.Cursor}.
   * 游标循环结果集的工厂对象
   **/
  final class CursorFactory {
    public final Style style;
    public final Class clazz;
    @JsonIgnore
    public final List<Field> fields;
    public final List<String> fieldNames;

    private CursorFactory(Style style, Class clazz, List<Field> fields,
        List<String> fieldNames) {
      assert (fieldNames != null)
          == (style == Style.RECORD_PROJECTION || style == Style.MAP);
      assert (fields != null) == (style == Style.RECORD_PROJECTION);
      this.style = style;
      this.clazz = clazz;
      this.fields = fields;
      this.fieldNames = fieldNames;
    }

    @JsonCreator
    public static CursorFactory create(@JsonProperty("style") Style style,
        @JsonProperty("clazz") Class clazz,
        @JsonProperty("fieldNames") List<String> fieldNames) {
      switch (style) {
      case OBJECT:
        return OBJECT;
      case ARRAY:
        return ARRAY;
      case LIST:
        return LIST;
      case RECORD:
        return record(clazz);
      case RECORD_PROJECTION:
        return record(clazz, null, fieldNames);//fieldNames是class的属性name
      case MAP:
        return map(fieldNames);
      default:
        throw new AssertionError("unknown style: " + style);
      }
    }

    public static final CursorFactory OBJECT =
        new CursorFactory(Style.OBJECT, null, null, null);

    public static final CursorFactory ARRAY =
        new CursorFactory(Style.ARRAY, null, null, null);

    public static final CursorFactory LIST =
        new CursorFactory(Style.LIST, null, null, null);

    //一行数据是一个对象,对象就是参数的class,即把一行数据转换成该对象
    public static CursorFactory record(Class resultClazz) {
      return new CursorFactory(Style.RECORD, resultClazz, null, null);
    }

    //project,获取resultClass中的字段子集，创建的

      /**
       * 一行数据对应的字段集合
       * @param resultClass  一行数据对应的字段存储在该对象的field中
       * @param fields 字段的顺序对应的field对象
       * @param fieldNames 按照字段顺序存储字段名称集合
       * @return
       */
    public static CursorFactory record(Class resultClass, List<Field> fields,
        List<String> fieldNames) {
      if (fields == null) {
        fields = new ArrayList<Field>();
        for (String fieldName : fieldNames) {
          try {
            fields.add(resultClass.getField(fieldName));
          } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
          }
        }
      }
      return new CursorFactory(Style.RECORD_PROJECTION, resultClass, fields,
          fieldNames);
    }

    public static CursorFactory map(List<String> fieldNames) {
      return new CursorFactory(Style.MAP, null, null, fieldNames);
    }

    public static CursorFactory deduce(List<ColumnMetaData> columns,
        Class resultClazz) {
      if (columns.size() == 1) {
        return OBJECT;
      } else if (resultClazz != null && !resultClazz.isArray()) {
        return record(resultClazz);
      } else {
        return ARRAY;
      }
    }
  }

  /** How logical fields are represented in the objects returned by the
   * iterator. */
  enum Style {
    OBJECT,//一行数据只有一个值
    RECORD,//一行数据是一个对象
    RECORD_PROJECTION,//对象中某几个属性组成的一行数据,即一行数据不是整个对象,而是部分字段
    ARRAY,//一行数据是数组
    LIST,//一行数据是List
    MAP //一行数据是Map
  }

  /** Result of preparing a statement. 一个已经解析好的sql元数据对象Signature,描述一个结果的元数据信息*/
  class Signature {
    public final List<ColumnMetaData> columns;//描述结果集中字段集合的信息
    public final String sql;
    public final List<AvaticaParameter> parameters;
    public final Map<String, Object> internalParameters;
    public final CursorFactory cursorFactory;//如何一行一行解析数据值

    @JsonCreator
    public Signature(@JsonProperty("columns") List<ColumnMetaData> columns,
        @JsonProperty("sql") String sql,
        @JsonProperty("parameters") List<AvaticaParameter> parameters,
        @JsonProperty("internalParameters") Map<String, Object>
            internalParameters,
        @JsonProperty("cursorFactory") CursorFactory cursorFactory) {
      this.columns = columns;
      this.sql = sql;
      this.parameters = parameters;
      this.internalParameters = internalParameters;
      this.cursorFactory = cursorFactory;
    }

    /** Returns a copy of this Signature, substituting given CursorFactory. */
    public Signature setCursorFactory(CursorFactory cursorFactory) {
      return new Signature(columns, sql, parameters, internalParameters,
          cursorFactory);
    }
  }

  /** Connection handle.
   * Connection的唯一id
   **/
  class ConnectionHandle {
    public final int id;

    @Override public String toString() {
      return Integer.toString(id);
    }

    @JsonCreator
    public ConnectionHandle(@JsonProperty("id") int id) {
      this.id = id;
    }
  }

  /** Statement handle.
   * connect中创建的statement的唯一id,即第几个创建的statement对象
   **/
  class StatementHandle {
    public final int id;

    @Override public String toString() {
      return Integer.toString(id);
    }

    @JsonCreator
    public StatementHandle(@JsonProperty("id") int id) {
      this.id = id;
    }
  }

  /** API to put a result set into a statement, being careful to enforce
   * thread-safety and not to overwrite existing open result sets.
   * 讲一个查询结果放入到 statement 中
   **/
  interface PrepareCallback {
    //定义了一套模板方法,按照顺序依次执行,可以在statement中产生一个结果集
    Object getMonitor();//如何上锁
    void clear() throws SQLException;//如果resultSet存在,则说明是历史的resultSet,将其销毁
    void assign(Signature signature, Iterable<Object> iterable) //设置数据集合的元数据信息,比如sql、列信息集合、如何一行一行读取数据方式,以及 数据集合本身的迭代器
        throws SQLException;
    void execute() throws SQLException;//真正执行结果集生产
  }
}

// End Meta.java
