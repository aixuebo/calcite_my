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

import org.apache.calcite.avatica.util.ArrayIteratorCursor;
import org.apache.calcite.avatica.util.Cursor;
import org.apache.calcite.avatica.util.IteratorCursor;
import org.apache.calcite.avatica.util.ListIteratorCursor;
import org.apache.calcite.avatica.util.MapIteratorCursor;
import org.apache.calcite.avatica.util.RecordIteratorCursor;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Basic implementation of {@link Meta}.
 *
 * <p>Each sub-class must implement the two remaining abstract methods,
 * {@link #prepare} and
 * {@link #prepareAndExecute}.
 * It should also override metadata methods such as {@link #getCatalogs()} and
 * {@link #getTables} for the element types for which it has instances; the
 * default metadata methods return empty collections.
 */
public abstract class MetaImpl implements Meta {
  protected final AvaticaConnection connection;

  public MetaImpl(AvaticaConnection connection) {
    this.connection = connection;
  }

  /** Uses a {@link org.apache.calcite.avatica.Meta.CursorFactory} to convert
   * an {@link Iterable} into a
   * {@link org.apache.calcite.avatica.util.Cursor}.
   * 如何处理一行数据内容---注意 此时已经查询到结果集了
   **/
  public static Cursor createCursor(CursorFactory cursorFactory,
      Iterable<Object> iterable) {
    switch (cursorFactory.style) {
    case OBJECT:
      return new IteratorCursor<Object>(iterable.iterator()) {//返回一行数据
        protected Getter createGetter(int ordinal) {
          return new ObjectGetter(ordinal);
        }
      };
    case ARRAY:
      @SuppressWarnings("unchecked") final Iterable<Object[]> iterable1 =
          (Iterable<Object[]>) (Iterable) iterable;
      return new ArrayIteratorCursor(iterable1.iterator());//返回一行数据,参数是如何读取一行数据中的某一列，使用createGetter方法
    case RECORD:
      @SuppressWarnings("unchecked") final Class<Object> clazz =
          cursorFactory.clazz;
      return new RecordIteratorCursor<Object>(iterable.iterator(), clazz);
    case RECORD_PROJECTION:
      @SuppressWarnings("unchecked") final Class<Object> clazz2 =
          cursorFactory.clazz;
      return new RecordIteratorCursor<Object>(iterable.iterator(), clazz2,
          cursorFactory.fields);
    case LIST:
      @SuppressWarnings("unchecked") final Iterable<List<Object>> iterable2 =
          (Iterable<List<Object>>) (Iterable) iterable;
      return new ListIteratorCursor(iterable2.iterator());
    case MAP:
      @SuppressWarnings("unchecked") final Iterable<Map<String, Object>>
          iterable3 =
          (Iterable<Map<String, Object>>) (Iterable) iterable;
      return new MapIteratorCursor(iterable3.iterator(),
          cursorFactory.fieldNames);
    default:
      throw new AssertionError("unknown style: " + cursorFactory.style);
    }
  }

  public static List<List<Object>> collect(CursorFactory cursorFactory,
      Iterable<Object> iterable,
      List<List<Object>> list) {
    switch (cursorFactory.style) {
    case OBJECT:
      for (Object o : iterable) {
        list.add(Collections.singletonList(o));
      }
      return list;
    case ARRAY:
      @SuppressWarnings("unchecked") final Iterable<Object[]> iterable1 =
          (Iterable<Object[]>) (Iterable) iterable;
      for (Object[] objects : iterable1) {
        list.add(Arrays.asList(objects));
      }
      return list;
    case RECORD:
    case RECORD_PROJECTION:
      final Field[] fields;
      switch (cursorFactory.style) {
      case RECORD:
        fields = cursorFactory.clazz.getFields();
        break;
      default:
        fields = cursorFactory.fields.toArray(
            new Field[cursorFactory.fields.size()]);
      }
      for (Object o : iterable) {
        final Object[] objects = new Object[fields.length];
        for (int i = 0; i < fields.length; i++) {
          Field field = fields[i];
          try {
            objects[i] = field.get(o);
          } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
          }
        }
        list.add(Arrays.asList(objects));
      }
      return list;
    case LIST:
      @SuppressWarnings("unchecked") final Iterable<List<Object>> iterable2 =
          (Iterable<List<Object>>) (Iterable) iterable;
      for (List<Object> objects : iterable2) {
        list.add(objects);
      }
      return list;
    case MAP:
      @SuppressWarnings("unchecked") final Iterable<Map<String, Object>>
          iterable3 =
          (Iterable<Map<String, Object>>) (Iterable) iterable;
      for (Map<String, Object> map : iterable3) {
        final List<Object> objects = new ArrayList<Object>();
        for (String fieldName : cursorFactory.fieldNames) {
          objects.add(map.get(fieldName));
        }
        list.add(objects);
      }
      return list;
    default:
      throw new AssertionError("unknown style: " + cursorFactory.style);
    }
  }

  public StatementHandle createStatement(ConnectionHandle ch) {
    return new StatementHandle(connection.statementCount++);
  }

  /** Creates an empty result set. Useful for JDBC metadata methods that are
   * not implemented or which query entities that are not supported (e.g.
   * triggers in Lingual). */
  protected <E> MetaResultSet createEmptyResultSet(final Class<E> clazz) {
    return createResultSet(Collections.<String, Object>emptyMap(),
        fieldMetaData(clazz).columns,
        CursorFactory.deduce(fieldMetaData(clazz).columns, null),
        Collections.emptyList());
  }

  protected static ColumnMetaData columnMetaData(String name, int index,
      Class<?> type) {
    TypeInfo pair = TypeInfo.m.get(type);
    ColumnMetaData.Rep rep =
        ColumnMetaData.Rep.VALUE_MAP.get(type);
    ColumnMetaData.AvaticaType scalarType =
        ColumnMetaData.scalar(pair.sqlType, pair.sqlTypeName, rep);
    return new ColumnMetaData(
        index, false, true, false, false,
        pair.primitive
            ? DatabaseMetaData.columnNullable
            : DatabaseMetaData.columnNoNulls,
        true, -1, name, name, null,
        0, 0, null, null, scalarType, true, false, false,
        scalarType.columnClassName());
  }

  //class所有的属性组成元数据---作为列集合输出
  protected static ColumnMetaData.StructType fieldMetaData(Class clazz) {
    final List<ColumnMetaData> list = new ArrayList<ColumnMetaData>();
    for (Field field : clazz.getFields()) {
      if (Modifier.isPublic(field.getModifiers())
          && !Modifier.isStatic(field.getModifiers())) {
        list.add(
            columnMetaData(
                AvaticaUtils.camelToUpper(field.getName()),
                list.size() + 1, field.getType()));
      }
    }
    return ColumnMetaData.struct(list);
  }

  protected MetaResultSet createResultSet(
      Map<String, Object> internalParameters, List<ColumnMetaData> columns,
      CursorFactory cursorFactory, Iterable<Object> iterable) {
    try {
      final AvaticaStatement statement = connection.createStatement();
      final SignatureWithIterable signature =
          new SignatureWithIterable(internalParameters, columns, "",
              Collections.<AvaticaParameter>emptyList(), cursorFactory,
              iterable);
      return new MetaResultSet(statement.getId(), true, signature, iterable);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /** An object that has a name. */
  public interface Named {
    @JsonIgnore String getName();
  }

  /** Metadata describing a column.列的元数据 */
  public static class MetaColumn implements Named {
    public final String tableCat;//catalog
    public final String tableSchem;//schema
    public final String tableName;//table
    public final String columnName;//列名
    public final int dataType;//类型
    public final String typeName;
    public final int columnSize;
    public final String bufferLength = null;
    public final Integer decimalDigits;
    public final int numPrecRadix;
    public final int nullable;
    public final String remarks = null;
    public final String columnDef = null;
    public final String sqlDataType = null;
    public final String sqlDatetimeSub = null;
    public final int charOctetLength;
    public final int ordinalPosition;
    public final String isNullable;
    public final String scopeCatalog = null;
    public final String scopeTable = null;
    public final String sourceDataType = null;
    public final String isAutoincrement = null;
    public final String isGeneratedcolumn = null;

    public MetaColumn(
        String tableCat,
        String tableSchem,
        String tableName,
        String columnName,
        int dataType,
        String typeName,
        int columnSize,
        Integer decimalDigits,
        int numPrecRadix,
        int nullable,
        int charOctetLength,
        int ordinalPosition,
        String isNullable) {
      this.tableCat = tableCat;
      this.tableSchem = tableSchem;
      this.tableName = tableName;
      this.columnName = columnName;
      this.dataType = dataType;
      this.typeName = typeName;
      this.columnSize = columnSize;
      this.decimalDigits = decimalDigits;
      this.numPrecRadix = numPrecRadix;
      this.nullable = nullable;
      this.charOctetLength = charOctetLength;
      this.ordinalPosition = ordinalPosition;
      this.isNullable = isNullable;
    }

    public String getName() {
      return columnName;
    }
  }

  /** Metadata describing a table.
   * 表级别的元数据 catalog+schema+table
   **/
  public static class MetaTable implements Named {
    public final String tableCat;//catalog
    public final String tableSchem;//schema
    public final String tableName;//tableName
    public final String tableType;//表类型
    public final String remarks = null;
    public final String typeCat = null;
    public final String typeSchem = null;
    public final String typeName = null;
    public final String selfReferencingColName = null;
    public final String refGeneration = null;

    public MetaTable(String tableCat,
        String tableSchem,
        String tableName,
        String tableType) {
      this.tableCat = tableCat;
      this.tableSchem = tableSchem;
      this.tableName = tableName;
      this.tableType = tableType;
    }

    public String getName() {
      return tableName;
    }
  }

  /** Metadata describing a schema.
   * 描述 Catalog+schema信息
   **/
  public static class MetaSchema implements Named {
    public final String tableCatalog;
    public final String tableSchem;

    public MetaSchema(
        String tableCatalog,
        String tableSchem) {
      this.tableCatalog = tableCatalog;
      this.tableSchem = tableSchem;
    }

    public String getName() {
      return tableSchem;
    }
  }

  /** Metadata describing a catalog.
   * 描述catalog信息
   * catalog是schema的父层名称，一般都是空,即一个catalog包含多个schema,每一个schema包含多个table
   **/
  public static class MetaCatalog implements Named {
    public final String tableCatalog;

    public MetaCatalog(
        String tableCatalog) {
      this.tableCatalog = tableCatalog;
    }

    public String getName() {
      return tableCatalog;
    }
  }

  /** Metadata describing a table type. */
  public static class MetaTableType {
    public final String tableType;

    public MetaTableType(String tableType) {
      this.tableType = tableType;
    }
  }

  /** Metadata describing a procedure. */
  public static class MetaProcedure {
  }

  /** Metadata describing a procedure column. */
  public static class MetaProcedureColumn {
  }

  /** Metadata describing a column privilege. */
  public static class MetaColumnPrivilege {
  }

  /** Metadata describing a table privilege. */
  public static class MetaTablePrivilege {
  }

  /** Metadata describing the best identifier for a row. */
  public static class MetaBestRowIdentifier {
  }

  /** Metadata describing a version column. */
  public static class MetaVersionColumn {
    public final short scope;
    public final String columnName;
    public final int dataType;
    public final String typeName;
    public final int columnSize;
    public final int bufferLength;
    public final short decimalDigits;
    public final short pseudoColumn;

    MetaVersionColumn(short scope, String columnName, int dataType,
        String typeName, int columnSize, int bufferLength, short decimalDigits,
        short pseudoColumn) {
      this.scope = scope;
      this.columnName = columnName;
      this.dataType = dataType;
      this.typeName = typeName;
      this.columnSize = columnSize;
      this.bufferLength = bufferLength;
      this.decimalDigits = decimalDigits;
      this.pseudoColumn = pseudoColumn;
    }
  }

  /** Metadata describing a primary key. */
  public static class MetaPrimaryKey {
    public final String tableCat;
    public final String tableSchem;
    public final String tableName;
    public final String columnName;
    public final short keySeq;
    public final String pkName;

    MetaPrimaryKey(String tableCat, String tableSchem, String tableName,
        String columnName, short keySeq, String pkName) {
      this.tableCat = tableCat;
      this.tableSchem = tableSchem;
      this.tableName = tableName;
      this.columnName = columnName;
      this.keySeq = keySeq;
      this.pkName = pkName;
    }
  }

  /** Metadata describing an imported key. */
  public static class MetaImportedKey {
  }

  /** Metadata describing an exported key. */
  public static class MetaExportedKey {
  }

  /** Metadata describing a cross reference. */
  public static class MetaCrossReference {
  }

  /** Metadata describing type info. */
  public static class MetaTypeInfo {
  }

  /** Metadata describing index info. */
  public static class MetaIndexInfo {
  }

  /** Metadata describing a user-defined type. */
  public static class MetaUdt {
  }

  /** Metadata describing a super-type. */
  public static class MetaSuperType {
  }

  /** Metadata describing an attribute. */
  public static class MetaAttribute {
  }

  /** Metadata describing a client info property. */
  public static class MetaClientInfoProperty {
  }

  /** Metadata describing a function. */
  public static class MetaFunction {
  }

  /** Metadata describing a function column. */
  public static class MetaFunctionColumn {
  }

  /** Metadata describing a pseudo column. */
  public static class MetaPseudoColumn {
  }

  /** Metadata describing a super-table. */
  public static class MetaSuperTable {
  }

  public String getSqlKeywords() {
    return "";
  }

  public String getNumericFunctions() {
    return "";
  }

  public String getStringFunctions() {
    return "";
  }

  public String getSystemFunctions() {
    return "";
  }

  public String getTimeDateFunctions() {
    return "";
  }

  public MetaResultSet getTables(String catalog,
      Pat schemaPattern,
      Pat tableNamePattern,
      List<String> typeList) {
    return createEmptyResultSet(MetaTable.class);
  }

  public MetaResultSet getColumns(String catalog,
      Pat schemaPattern,
      Pat tableNamePattern,
      Pat columnNamePattern) {
    return createEmptyResultSet(MetaColumn.class);
  }

  public MetaResultSet getSchemas(String catalog, Pat schemaPattern) {
    return createEmptyResultSet(MetaSchema.class);
  }

  public MetaResultSet getCatalogs() {
    return createEmptyResultSet(MetaCatalog.class);
  }

  public MetaResultSet getTableTypes() {
    return createEmptyResultSet(MetaTableType.class);
  }

  public MetaResultSet getProcedures(String catalog,
      Pat schemaPattern,
      Pat procedureNamePattern) {
    return createEmptyResultSet(MetaProcedure.class);
  }

  public MetaResultSet getProcedureColumns(String catalog,
      Pat schemaPattern,
      Pat procedureNamePattern,
      Pat columnNamePattern) {
    return createEmptyResultSet(MetaProcedureColumn.class);
  }

  public MetaResultSet getColumnPrivileges(String catalog,
      String schema,
      String table,
      Pat columnNamePattern) {
    return createEmptyResultSet(MetaColumnPrivilege.class);
  }

  public MetaResultSet getTablePrivileges(String catalog,
      Pat schemaPattern,
      Pat tableNamePattern) {
    return createEmptyResultSet(MetaTablePrivilege.class);
  }

  public MetaResultSet getBestRowIdentifier(String catalog,
      String schema,
      String table,
      int scope,
      boolean nullable) {
    return createEmptyResultSet(MetaBestRowIdentifier.class);
  }

  public MetaResultSet getVersionColumns(String catalog,
      String schema,
      String table) {
    return createEmptyResultSet(MetaVersionColumn.class);
  }

  public MetaResultSet getPrimaryKeys(String catalog,
      String schema,
      String table) {
    return createEmptyResultSet(MetaPrimaryKey.class);
  }

  public MetaResultSet getImportedKeys(String catalog,
      String schema,
      String table) {
    return createEmptyResultSet(MetaImportedKey.class);
  }

  public MetaResultSet getExportedKeys(String catalog,
      String schema,
      String table) {
    return createEmptyResultSet(MetaExportedKey.class);
  }

  public MetaResultSet getCrossReference(String parentCatalog,
      String parentSchema,
      String parentTable,
      String foreignCatalog,
      String foreignSchema,
      String foreignTable) {
    return createEmptyResultSet(MetaCrossReference.class);
  }

  public MetaResultSet getTypeInfo() {
    return createEmptyResultSet(MetaTypeInfo.class);
  }

  public MetaResultSet getIndexInfo(String catalog,
      String schema,
      String table,
      boolean unique,
      boolean approximate) {
    return createEmptyResultSet(MetaIndexInfo.class);
  }

  public MetaResultSet getUDTs(String catalog,
      Pat schemaPattern,
      Pat typeNamePattern,
      int[] types) {
    return createEmptyResultSet(MetaUdt.class);
  }

  public MetaResultSet getSuperTypes(String catalog,
      Pat schemaPattern,
      Pat typeNamePattern) {
    return createEmptyResultSet(MetaSuperType.class);
  }

  public MetaResultSet getSuperTables(String catalog,
      Pat schemaPattern,
      Pat tableNamePattern) {
    return createEmptyResultSet(MetaSuperTable.class);
  }

  public MetaResultSet getAttributes(String catalog,
      Pat schemaPattern,
      Pat typeNamePattern,
      Pat attributeNamePattern) {
    return createEmptyResultSet(MetaAttribute.class);
  }

  public MetaResultSet getClientInfoProperties() {
    return createEmptyResultSet(MetaClientInfoProperty.class);
  }

  public MetaResultSet getFunctions(String catalog,
      Pat schemaPattern,
      Pat functionNamePattern) {
    return createEmptyResultSet(MetaFunction.class);
  }

  public MetaResultSet getFunctionColumns(String catalog,
      Pat schemaPattern,
      Pat functionNamePattern,
      Pat columnNamePattern) {
    return createEmptyResultSet(MetaFunctionColumn.class);
  }

  public MetaResultSet getPseudoColumns(String catalog,
      Pat schemaPattern,
      Pat tableNamePattern,
      Pat columnNamePattern) {
    return createEmptyResultSet(MetaPseudoColumn.class);
  }

  public Iterable<Object> createIterable(StatementHandle handle,
      Signature signature, Iterable<Object> iterable) {
    return iterable;
  }

  /** Information about a type. java类型与sql类型映射*/
  private static class TypeInfo {
    private static Map<Class<?>, TypeInfo> m =
        new HashMap<Class<?>, TypeInfo>();
    static {
      put(boolean.class, true, Types.BOOLEAN, "BOOLEAN");
      put(Boolean.class, false, Types.BOOLEAN, "BOOLEAN");
      put(byte.class, true, Types.TINYINT, "TINYINT");
      put(Byte.class, false, Types.TINYINT, "TINYINT");
      put(short.class, true, Types.SMALLINT, "SMALLINT");
      put(Short.class, false, Types.SMALLINT, "SMALLINT");
      put(int.class, true, Types.INTEGER, "INTEGER");
      put(Integer.class, false, Types.INTEGER, "INTEGER");
      put(long.class, true, Types.BIGINT, "BIGINT");
      put(Long.class, false, Types.BIGINT, "BIGINT");
      put(float.class, true, Types.FLOAT, "FLOAT");
      put(Float.class, false, Types.FLOAT, "FLOAT");
      put(double.class, true, Types.DOUBLE, "DOUBLE");
      put(Double.class, false, Types.DOUBLE, "DOUBLE");
      put(String.class, false, Types.VARCHAR, "VARCHAR");
      put(java.sql.Date.class, false, Types.DATE, "DATE");
      put(Time.class, false, Types.TIME, "TIME");
      put(Timestamp.class, false, Types.TIMESTAMP, "TIMESTAMP");
    }

    private final boolean primitive;//是否原生的java基础类型---8种
    private final int sqlType;//sql类型 VARCHAR
    private final String sqlTypeName;//sql类型 VARCHAR

    public TypeInfo(boolean primitive, int sqlType, String sqlTypeName) {
      this.primitive = primitive;
      this.sqlType = sqlType;
      this.sqlTypeName = sqlTypeName;
    }

    static void put(Class clazz, boolean primitive, int sqlType,
        String sqlTypeName) {
      m.put(clazz, new TypeInfo(primitive, sqlType, sqlTypeName));
    }
  }

  /** Prepare result with a iterable. Use this for simple statements
   * that have a canned response and don't need to be executed. */
  protected interface WithIterable {
    Iterable getIterable();
  }

  /** Prepare result that contains an iterable. */
  protected static class SignatureWithIterable extends Signature
      implements WithIterable {
    private final Iterable iterable;

    public SignatureWithIterable(Map<String, Object> internalParameters,
        List<ColumnMetaData> columns, String sql,
        List<AvaticaParameter> parameters, CursorFactory cursorFactory,
        Iterable<Object> iterable) {
      super(columns, sql, parameters, internalParameters, cursorFactory);
      this.iterable = iterable;
    }

    public Iterable getIterable() {
      return iterable;
    }
  }
}

// End MetaImpl.java
