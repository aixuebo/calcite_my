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
package org.apache.calcite.jdbc;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Functions;
import org.apache.calcite.linq4j.function.Predicate1;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.FlatLists;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.server.CalciteServerStatement;
import org.apache.calcite.sql.SqlJdbcFunctionCall;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.util.Util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.lang.reflect.Field;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Helper for implementing the {@code getXxx} methods such as
 * {@link org.apache.calcite.avatica.AvaticaDatabaseMetaData#getTables}.
 * 提供整个数据库维度的元数据信息
 * 比如支持哪些数值类型的函数、系统函数、字符串函数等
 */
public class CalciteMetaImpl extends MetaImpl {
  static final Driver DRIVER = new Driver();

  public CalciteMetaImpl(CalciteConnectionImpl connection) {
    super(connection);
  }

  //生产决策器对象,要求决策期的name符合正则表达式
  static <T extends Named> Predicate1<T> namedMatcher(final Pat pattern) {
    if (pattern.s == null || pattern.s.equals("%")) {
      return Functions.truePredicate1();//永远返回true
    }
    final Pattern regex = likeToRegex(pattern);
    return new Predicate1<T>() {
      public boolean apply(T v1) {//T是Named对象,因此有name方法
        return regex.matcher(v1.getName()).matches();
      }
    };
  }

  static Predicate1<String> matcher(final Pat pattern) {
    if (pattern.s == null || pattern.s.equals("%")) {//%在sql中相当于.*
      return Functions.truePredicate1();
    }
    final Pattern regex = likeToRegex(pattern);
    return new Predicate1<String>() {
      public boolean apply(String v1) {
        return regex.matcher(v1).matches();
      }
    };
  }

  /** Converts a LIKE-style pattern (where '%' represents a wild-card, escaped
   * using '\') to a Java regex. */
  public static Pattern likeToRegex(Pat pattern) {
    StringBuilder buf = new StringBuilder("^");
    char[] charArray = pattern.s.toCharArray();
    int slash = -2;
    for (int i = 0; i < charArray.length; i++) {
      char c = charArray[i];
      if (slash == i - 1) {
        buf.append('[').append(c).append(']');
      } else {
        switch (c) {
        case '\\':
          slash = i;
          break;
        case '%':
          buf.append(".*");
          break;
        case '[':
          buf.append("\\[");
          break;
        case ']':
          buf.append("\\]");
          break;
        default:
          buf.append('[').append(c).append(']');
        }
      }
    }
    buf.append("$");
    return Pattern.compile(buf.toString());
  }

  @Override public StatementHandle createStatement(ConnectionHandle ch) {
    final StatementHandle h = super.createStatement(ch);
    final CalciteConnectionImpl calciteConnection = getConnection();
    calciteConnection.server.addStatement(calciteConnection, h);
    return h;
  }

  /**
   *
   * @param enumerable 结果集迭代器
   * @param clazz 每一个迭代器元素是什么对象
   * @param names name数属性名,属性值从class中获取---即迭代器内容是值,这些值对外叫什么名字,相当于列
   * @param <E>
   * @return
   */
  private <E> MetaResultSet createResultSet(Enumerable<E> enumerable,
      Class clazz, String... names) {
    final List<ColumnMetaData> columns = new ArrayList<ColumnMetaData>();//对应的sql属性对象
    final List<Field> fields = new ArrayList<Field>();//按照顺序添加name对应的field属性对象
    final List<String> fieldNames = new ArrayList<String>();//驼峰方式的属性name
    for (String name : names) {//添加列信息
      final int index = fields.size();
      final String fieldName = AvaticaUtils.toCamelCase(name);
      final Field field;
      try {
        field = clazz.getField(fieldName);
      } catch (NoSuchFieldException e) {
        throw new RuntimeException(e);
      }
      columns.add(columnMetaData(name, index, field.getType()));
      fields.add(field);
      fieldNames.add(fieldName);
    }
    //noinspection unchecked
    final Iterable<Object> iterable = (Iterable<Object>) (Iterable) enumerable;
    return createResultSet(Collections.<String, Object>emptyMap(),
        columns, CursorFactory.record(clazz, fields, fieldNames),//以什么方式读取迭代器的内容
        iterable);
  }

  //空结果迭代器
  @Override protected <E> MetaResultSet
  createEmptyResultSet(final Class<E> clazz) {
    final List<ColumnMetaData> columns = fieldMetaData(clazz).columns;
    final CursorFactory cursorFactory = CursorFactory.deduce(columns, clazz);
    return createResultSet(Collections.<String, Object>emptyMap(), columns,
        cursorFactory, Collections.emptyList());
  }


  protected MetaResultSet createResultSet(
      Map<String, Object> internalParameters,//内部参数信息,一般情况是空
      List<ColumnMetaData> columns,//每一个列的元数据内容
      CursorFactory cursorFactory,//以什么方式读取迭代器的内容
      final Iterable<Object> iterable) {//迭代器具体的值对象
    try {
      final CalciteConnectionImpl connection = getConnection();
      final AvaticaStatement statement = connection.createStatement();
      final CalcitePrepare.CalciteSignature<Object> signature =
          new CalcitePrepare.CalciteSignature<Object>("",
              ImmutableList.<AvaticaParameter>of(), internalParameters, null,
              columns, cursorFactory, -1, null) {
            @Override public Enumerable<Object> enumerable(
                DataContext dataContext) {
              return Linq4j.asEnumerable(iterable);
            }
          };
      return new MetaResultSet(statement.getId(), true, signature, iterable);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  CalciteConnectionImpl getConnection() {
    return (CalciteConnectionImpl) connection;
  }

  //返回sql中支持的keyword关键词集合,使用逗号拆分
  public String getSqlKeywords() {
    return SqlParser.create("").getMetadata().getJdbcKeywords();
  }

  //返回所有数值型函数
  public String getNumericFunctions() {
    return SqlJdbcFunctionCall.getNumericFunctions();
  }

  public String getStringFunctions() {
    return SqlJdbcFunctionCall.getStringFunctions();
  }

  public String getSystemFunctions() {
    return SqlJdbcFunctionCall.getSystemFunctions();
  }

  public String getTimeDateFunctions() {
    return SqlJdbcFunctionCall.getTimeDateFunctions();
  }

  //找到匹配的table迭代器
  public MetaResultSet getTables(String catalog,
      final Pat schemaPattern,//如何匹配schema
      final Pat tableNamePattern,//如何匹配表名
      final List<String> typeList) {//如何匹配表类型,比如view
    final Predicate1<MetaTable> typeFilter;
    if (typeList == null) {
      typeFilter = Functions.truePredicate1();//表示所有表类型都可以匹配
    } else {
      typeFilter = new Predicate1<MetaTable>() {
        public boolean apply(MetaTable v1) {
          return typeList.contains(v1.tableType);//返回必须命中typeList类型中任意类型
        }
      };
    }
    final Predicate1<MetaSchema> schemaMatcher = namedMatcher(schemaPattern);
    return createResultSet(schemas(catalog)//所有schema
            .where(schemaMatcher)//找到匹配的schema
            .selectMany(
                new Function1<MetaSchema, Enumerable<MetaTable>>() {//循环所有的table
                  public Enumerable<MetaTable> apply(MetaSchema schema) {
                    return tables(schema, matcher(tableNamePattern));//匹配表name符合条件的
                  }
                })
            .where(typeFilter),//找到符合类型条件的table
        MetaTable.class,//迭代器每一个元素是MetaTable类型
        "TABLE_CAT",
        "TABLE_SCHEM",
        "TABLE_NAME",
        "TABLE_TYPE",
        "REMARKS",
        "TYPE_CAT",
        "TYPE_SCHEM",
        "TYPE_NAME",
        "SELF_REFERENCING_COL_NAME",
        "REF_GENERATION");
  }

  //返回满足的列元数据集合
  public MetaResultSet getColumns(String catalog,
      Pat schemaPattern,//如何匹配schema
      Pat tableNamePattern,//如何匹配table
      Pat columnNamePattern) {//如何匹配column
    final Predicate1<String> tableNameMatcher = matcher(tableNamePattern);
    final Predicate1<MetaSchema> schemaMatcher = namedMatcher(schemaPattern);
    final Predicate1<MetaColumn> columnMatcher = namedMatcher(columnNamePattern);

    return createResultSet(schemas(catalog) //所有子schema集合
            .where(schemaMatcher) //过滤schema
            .selectMany(//每一个元素转换成多个元素,相当于flatMap
                new Function1<MetaSchema, Enumerable<MetaTable>>() {//每一个schema 转换成table迭代器
                  public Enumerable<MetaTable> apply(MetaSchema schema) {
                    return tables(schema, tableNameMatcher);//过滤table
                  }
                })
            .selectMany(
                new Function1<MetaTable, Enumerable<MetaColumn>>() {//每一个table 转换成多个列
                  public Enumerable<MetaColumn> apply(MetaTable schema) {
                    return columns(schema);
                  }
                })
            .where(columnMatcher),//过滤column
        MetaColumn.class,//迭代器的元素每一个是MetaColumn对象
        "TABLE_CAT",
        "TABLE_SCHEM",
        "TABLE_NAME",
        "COLUMN_NAME",
        "DATA_TYPE",
        "TYPE_NAME",
        "COLUMN_SIZE",
        "BUFFER_LENGTH",
        "DECIMAL_DIGITS",
        "NUM_PREC_RADIX",
        "NULLABLE",
        "REMARKS",
        "COLUMN_DEF",
        "SQL_DATA_TYPE",
        "SQL_DATETIME_SUB",
        "CHAR_OCTET_LENGTH",
        "ORDINAL_POSITION",
        "IS_NULLABLE",
        "SCOPE_CATALOG",
        "SCOPE_TABLE",
        "SOURCE_DATA_TYPE",
        "IS_AUTOINCREMENT",
        "IS_GENERATEDCOLUMN");
  }

  Enumerable<MetaCatalog> catalogs() {
    return Linq4j.asEnumerable(
        ImmutableList.of(new MetaCatalog(connection.getCatalog())));
  }

  //只有table和view两种类型---写死
  Enumerable<MetaTableType> tableTypes() {
    return Linq4j.asEnumerable(
        ImmutableList.of(
            new MetaTableType("TABLE"), new MetaTableType("VIEW")));
  }

  //所有子schema集合
  Enumerable<MetaSchema> schemas(String catalog) {
    return Linq4j.asEnumerable(
        getConnection().rootSchema.getSubSchemaMap().values())//所有子schema对象
        .select(
            new Function1<CalciteSchema, MetaSchema>() {//将子schema转换成MetaSchema
              public MetaSchema apply(CalciteSchema calciteSchema) {
                return new CalciteMetaSchema(
                    calciteSchema,
                    connection.getCatalog(),
                    calciteSchema.getName());
              }
            })
        .orderBy(//将MetaSchema转换成可以排序的Comparable对象,参与排序
            new Function1<MetaSchema, Comparable>() {
              public Comparable apply(MetaSchema metaSchema) {//先比较catalog,再比较schema
                return (Comparable) FlatLists.of(
                    Util.first(metaSchema.tableCatalog, ""),
                    metaSchema.tableSchem);
              }
            });
  }

  //所有table表集合
  Enumerable<MetaTable> tables(String catalog) {
    return schemas(catalog)//所有子schema集合
        .selectMany(
            new Function1<MetaSchema, Enumerable<MetaTable>>() {//所有table
              public Enumerable<MetaTable> apply(MetaSchema schema) {
                return tables(schema, Functions.<String>truePredicate1());
              }
            });
  }

  //某一个schema下的所有table
  Enumerable<MetaTable> tables(final MetaSchema schema_) {
    final CalciteMetaSchema schema = (CalciteMetaSchema) schema_;
    return Linq4j.asEnumerable(schema.calciteSchema.getTableNames())//schema下所有table
        .select(
            new Function1<String, MetaTable>() {
              public MetaTable apply(String name) {
                final Table table =
                    schema.calciteSchema.getTable(name, true).getValue();
                return new CalciteMetaTable(table,
                    schema.tableCatalog,
                    schema.tableSchem,
                    name);
              }
            })
        .concat(
            Linq4j.asEnumerable(
                schema.calciteSchema.getTablesBasedOnNullaryFunctions()
                    .entrySet())
                .select(
                    new Function1<Map.Entry<String, Table>, MetaTable>() {
                      public MetaTable apply(Map.Entry<String, Table> pair) {
                        final Table table = pair.getValue();
                        return new CalciteMetaTable(table,
                            schema.tableCatalog,
                            schema.tableSchem,
                            pair.getKey());
                      }
                    }));
  }

  //schema下table+过滤条件
  Enumerable<MetaTable> tables(
      final MetaSchema schema,
      final Predicate1<String> matcher) {
    return tables(schema)
        .where(
            new Predicate1<MetaTable>() {
              public boolean apply(MetaTable v1) {
                return matcher.apply(v1.getName());//满足过滤条件的table
              }
            });
  }

  //某一个table下所有列
  public Enumerable<MetaColumn> columns(final MetaTable table_) {
    final CalciteMetaTable table = (CalciteMetaTable) table_;
    final RelDataType rowType =
        table.calciteTable.getRowType(getConnection().typeFactory);//列信息集合
    return Linq4j.asEnumerable(rowType.getFieldList())
        .select(
            new Function1<RelDataTypeField, MetaColumn>() {//列对象 转换成列元数据
              public MetaColumn apply(RelDataTypeField field) {
                final int precision =
                    field.getType().getSqlTypeName().allowsPrec()
                        && !(field.getType()
                        instanceof RelDataTypeFactoryImpl.JavaType)
                        ? field.getType().getPrecision()
                        : -1;
                return new MetaColumn(
                    table.tableCat,
                    table.tableSchem,
                    table.tableName,
                    field.getName(),
                    field.getType().getSqlTypeName().getJdbcOrdinal(),
                    field.getType().getFullTypeString(),
                    precision,
                    field.getType().getSqlTypeName().allowsScale()
                        ? field.getType().getScale()
                        : null,
                    10,
                    field.getType().isNullable()
                        ? DatabaseMetaData.columnNullable
                        : DatabaseMetaData.columnNoNulls,
                    precision,
                    field.getIndex() + 1,
                    field.getType().isNullable() ? "YES" : "NO");
              }
            });
  }

  //返回schema
  public MetaResultSet getSchemas(String catalog, Pat schemaPattern) {
    final Predicate1<MetaSchema> schemaMatcher = namedMatcher(schemaPattern);//schema条件
    return createResultSet(schemas(catalog).where(schemaMatcher),//查找所有的schema,过滤满足条件的schema
        MetaSchema.class,
        "TABLE_SCHEM",
        "TABLE_CATALOG");
  }

  //返回Catalog
  public MetaResultSet getCatalogs() {
    return createResultSet(catalogs(),
        MetaCatalog.class,
        "TABLE_CATALOG");
  }

  //返回table所有类型---目前只支持table、view两种
  public MetaResultSet getTableTypes() {
    return createResultSet(tableTypes(),
        MetaTableType.class,
        "TABLE_TYPE");
  }

  @Override public Iterable<Object> createIterable(StatementHandle handle,
      Signature signature, Iterable<Object> iterable) {
    try {
      //noinspection unchecked
      final CalcitePrepare.CalciteSignature<Object> calciteSignature =
          (CalcitePrepare.CalciteSignature<Object>) signature;
      return getConnection().enumerable(handle, calciteSignature);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public Signature prepare(StatementHandle h, String sql, int maxRowCount) {
    final CalciteConnectionImpl calciteConnection = getConnection();
    CalciteServerStatement statement = calciteConnection.server.getStatement(h);
    return calciteConnection.parseQuery(sql, statement.createPrepareContext(),
        maxRowCount);
  }

  public MetaResultSet prepareAndExecute(StatementHandle h, String sql,
      int maxRowCount, PrepareCallback callback) {
    final CalcitePrepare.CalciteSignature<Object> signature;
    try {
      synchronized (callback.getMonitor()) {
        callback.clear();
        final CalciteConnectionImpl calciteConnection = getConnection();
        CalciteServerStatement statement =
            calciteConnection.server.getStatement(h);
        signature = calciteConnection.parseQuery(sql,
            statement.createPrepareContext(), maxRowCount);
        callback.assign(signature, null);
      }
      callback.execute();
      return new MetaResultSet(h.id, false, signature, null);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    // TODO: share code with prepare and createIterable
  }

  /** A trojan-horse method, subject to change without notice. */
  @VisibleForTesting
  public static DataContext createDataContext(CalciteConnection connection) {
    return ((CalciteConnectionImpl) connection)
        .createDataContext(ImmutableMap.<String, Object>of());
  }

  /** A trojan-horse method, subject to change without notice. */
  @VisibleForTesting
  public static CalciteConnection connect(CalciteRootSchema schema,
      JavaTypeFactory typeFactory) {
    return DRIVER.connect(schema, typeFactory);
  }

  /** Metadata describing a Calcite table.
   * 描述一个表的元数据
   **/
  private static class CalciteMetaTable extends MetaTable {
    private final Table calciteTable;

    public CalciteMetaTable(Table calciteTable, String tableCat,
        String tableSchem, String tableName) {
      super(tableCat, tableSchem, tableName,
          calciteTable.getJdbcTableType().name());
      this.calciteTable = Preconditions.checkNotNull(calciteTable);
    }
  }

  /** Metadata describing a Calcite schema. 描述一个子schema的元数据*/
  private static class CalciteMetaSchema extends MetaSchema {
    private final CalciteSchema calciteSchema;//子schema

    public CalciteMetaSchema(CalciteSchema calciteSchema,
        String tableCatalog, String tableSchem) {
      super(tableCatalog, tableSchem);
      this.calciteSchema = calciteSchema;
    }
  }

  /** Table whose contents are metadata. */
  abstract static class MetadataTable<E> extends AbstractQueryableTable {
    public MetadataTable(Class<E> clazz) {
      super(clazz);
    }

    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return ((JavaTypeFactory) typeFactory).createType(elementType);
    }

    @Override public Schema.TableType getJdbcTableType() {
      return Schema.TableType.SYSTEM_TABLE;
    }

    @SuppressWarnings("unchecked")
    @Override public Class<E> getElementType() {
      return (Class<E>) elementType;
    }

    protected abstract Enumerator<E> enumerator(CalciteMetaImpl connection);

    public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
        SchemaPlus schema, String tableName) {
      return new AbstractTableQueryable<T>(queryProvider, schema, this,
          tableName) {
        @SuppressWarnings("unchecked")
        public Enumerator<T> enumerator() {
          return (Enumerator<T>) MetadataTable.this.enumerator(
              ((CalciteConnectionImpl) queryProvider).meta());
        }
      };
    }
  }
}

// End CalciteMetaImpl.java
