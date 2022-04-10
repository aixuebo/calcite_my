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
package org.apache.calcite.prepare;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableInterpreter;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ExtensibleTable;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.SqlAccessType;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;

/**
 * Implementation of {@link org.apache.calcite.plan.RelOptTable}.
 */
public class RelOptTableImpl implements Prepare.PreparingTable {
  private final RelOptSchema schema;//表所在schema
  private final RelDataType rowType;//表的schema数据结构
  private final Table table;//具体表对象
  private final Function<Class, Expression> expressionFunction;
  private final ImmutableList<String> names;//表的schema全路径

  /** Estimate for the row count, or null.
   *
   * <p>If not null, overrides the estimate from the actual table.
   *
   * <p>Useful when a table that contains a materialized query result is being
   * used to replace a query expression that wildly underestimates the row
   * count. Now the materialized table can tell the same lie. */
  private final Double rowCount;//数据量行数

  private RelOptTableImpl(
      RelOptSchema schema,
      RelDataType rowType,
      List<String> names,
      Table table,
      Function<Class, Expression> expressionFunction,
      Double rowCount) {
    this.schema = schema;
    this.rowType = rowType;
    this.names = ImmutableList.copyOf(names);
    this.table = table; // may be null
    this.expressionFunction = expressionFunction;
    this.rowCount = rowCount;
    assert expressionFunction != null;
    assert rowType != null;
  }

  public static RelOptTableImpl create(
      RelOptSchema schema,
      RelDataType rowType,
      List<String> names,
      Expression expression) {
    //noinspection unchecked
    final Function<Class, Expression> expressionFunction =
        (Function) Functions.constant(expression);
    return new RelOptTableImpl(schema, rowType, names, null,
        expressionFunction, null);
  }

  //转换成读取表的关系表达式
  public static RelOptTableImpl create(RelOptSchema schema, RelDataType rowType,
      final CalciteSchema.TableEntry tableEntry, Double rowCount) {
    Function<Class, Expression> expressionFunction;
    final Table table = tableEntry.getTable();
    if (table instanceof QueryableTable) {
      final QueryableTable queryableTable = (QueryableTable) table;
      expressionFunction = new Function<Class, Expression>() {
        public Expression apply(Class clazz) {
          return queryableTable.getExpression(tableEntry.schema.plus(),
              tableEntry.name, clazz);
        }
      };
    } else if (table instanceof ScannableTable
        || table instanceof FilterableTable
        || table instanceof ProjectableFilterableTable) {
      expressionFunction = new Function<Class, Expression>() {
        public Expression apply(Class clazz) {
          return Schemas.tableExpression(tableEntry.schema.plus(),
              Object[].class, tableEntry.name,
              table.getClass());
        }
      };
    } else {
      expressionFunction = new Function<Class, Expression>() {
        public Expression apply(Class input) {
          throw new UnsupportedOperationException();
        }
      };
    }
    return new RelOptTableImpl(schema, rowType, tableEntry.path(),
        table, expressionFunction, rowCount);
  }

  public static RelOptTableImpl create(
      RelOptSchema schema,
      RelDataType rowType,
      TranslatableTable table) {
    final Function<Class, Expression> expressionFunction =
        new Function<Class, Expression>() {
          public Expression apply(Class input) {
            throw new UnsupportedOperationException();
          }
        };
    return new RelOptTableImpl(schema, rowType, ImmutableList.<String>of(),
        table, expressionFunction, null);
  }

  public <T> T unwrap(Class<T> clazz) {
    if (clazz.isInstance(this)) {
      return clazz.cast(this);
    }
    if (clazz.isInstance(table)) {
      return clazz.cast(table);
    }
    if (clazz == CalciteSchema.class) {
      return clazz.cast(
          Schemas.subSchema(((CalciteCatalogReader) schema).rootSchema,
              Util.skipLast(getQualifiedName())));
    }
    return null;
  }

  public Expression getExpression(Class clazz) {
    return expressionFunction.apply(clazz);
  }

  public RelOptTable extend(List<RelDataTypeField> extendedFields) {
    if (table instanceof ExtensibleTable) {
      final Table extendedTable =
          ((ExtensibleTable) table).extend(extendedFields);
      final RelDataType extendedRowType =
          extendedTable.getRowType(schema.getTypeFactory());
      return new RelOptTableImpl(schema, extendedRowType, names, extendedTable,
          expressionFunction, rowCount);
    }
    throw new RuntimeException("Cannot extend " + table); // TODO: user error
  }

  public double getRowCount() {
    if (rowCount != null) {
      return rowCount;
    }
    if (table != null) {
      final Double rowCount = table.getStatistic().getRowCount();
      if (rowCount != null) {
        return rowCount;
      }
    }
    return 100d;
  }

  public RelOptSchema getRelOptSchema() {
    return schema;
  }

  //如何转换成表达式
  public RelNode toRel(ToRelContext context) {
    if (table instanceof TranslatableTable) {
      return ((TranslatableTable) table).toRel(context, this);
    }
    if (CalcitePrepareImpl.ENABLE_BINDABLE) {//直接扫描table
      return new LogicalTableScan(context.getCluster(), this);
    }
    if (CalcitePrepareImpl.ENABLE_ENUMERABLE) {
      RelOptCluster cluster = context.getCluster();
      Class elementType = deduceElementType();//读取一行元素如何存储，比如数组存储
      final RelNode scan = new EnumerableTableScan(cluster,
          cluster.traitSetOf(EnumerableConvention.INSTANCE), this, elementType);
      if (table instanceof FilterableTable
          || table instanceof ProjectableFilterableTable) {
        return new EnumerableInterpreter(cluster, scan.getTraitSet(),
            scan, 1d);
      }
      return scan;
    }
    throw new AssertionError();
  }

  private Class deduceElementType() {
    if (table instanceof QueryableTable) {
      final QueryableTable queryableTable = (QueryableTable) table;
      final Type type = queryableTable.getElementType();
      if (type instanceof Class) {
        return (Class) type;
      } else {
        return Object[].class;
      }
    } else if (table instanceof ScannableTable
        || table instanceof FilterableTable
        || table instanceof ProjectableFilterableTable) {
      return Object[].class;
    } else {
      return Object.class;
    }
  }

  public List<RelCollation> getCollationList() {
    return Collections.emptyList();
  }

  public boolean isKey(ImmutableBitSet columns) {
    return table.getStatistic().isKey(columns);
  }

  public RelDataType getRowType() {
    return rowType;
  }

  public List<String> getQualifiedName() {
    return names;
  }

  //返回字段的单调性 ---默认显示每一个列是非单调的 NOT_MONOTONIC
  public SqlMonotonicity getMonotonicity(String columnName) {
    return SqlMonotonicity.NOT_MONOTONIC;
  }

  //访问该表的权限---增删改查都支持访问
  public SqlAccessType getAllowedAccess() {
    return SqlAccessType.ALL;
  }
}

// End RelOptTableImpl.java
