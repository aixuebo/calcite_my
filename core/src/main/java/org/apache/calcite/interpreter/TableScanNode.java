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
package org.apache.calcite.interpreter;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.Enumerables;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.List;

/**
 * Interpreter node that implements a
 * {@link org.apache.calcite.rel.core.TableScan}.
 * 如何扫描数据
 */
public class TableScanNode implements Node {
  private final Sink sink;//输出临时存储
  private final TableScan rel;//扫描哪个表
  private final ImmutableList<RexNode> filters;//where条件
  private final DataContext root;
  private final int[] projects;//select的投影字段

  TableScanNode(Interpreter interpreter, TableScan rel,
      ImmutableList<RexNode> filters, ImmutableIntList projects) {
    this.rel = rel;
    this.filters = Preconditions.checkNotNull(filters);
    this.projects = projects == null ? null : projects.toIntArray();
    this.sink = interpreter.sink(rel);
    this.root = interpreter.getDataContext();
  }

  public void run() throws InterruptedException {
    final Enumerable<Row> iterable = iterable();
    final Enumerator<Row> enumerator = iterable.enumerator();
    while (enumerator.moveNext()) {
      sink.send(enumerator.current());
    }
    enumerator.close();
    sink.end();
  }

  private Enumerable<Row> iterable() {
    final RelOptTable table = rel.getTable();
    final ProjectableFilterableTable pfTable = table.unwrap(ProjectableFilterableTable.class);
    if (pfTable != null) {
      final List<RexNode> filters1 = Lists.newArrayList(filters);
      //isIdentity true 表示select *,查询全部字段
      final int[] projects1 =  projects == null || isIdentity(projects, rel.getRowType().getFieldCount()) ? null : projects;
      final Enumerable<Object[]> enumerator = pfTable.scan(root, filters1, projects1);
      assert filters1.isEmpty()
          : "table could not handle a filter it earlier said it could";
      return Enumerables.toRow(enumerator);
    }
    if (projects != null) {
      throw new AssertionError("have projects, but table cannot handle them");
    }

    final FilterableTable filterableTable = table.unwrap(FilterableTable.class);
    if (filterableTable != null) {
      final List<RexNode> filters1 = Lists.newArrayList(filters);
      final Enumerable<Object[]> enumerator = filterableTable.scan(root, filters1);
      assert filters1.isEmpty()
          : "table could not handle a filter it earlier said it could";
      return Enumerables.toRow(enumerator);
    }
    if (!filters.isEmpty()) {
      throw new AssertionError("have filters, but table cannot handle them");
    }

    final ScannableTable scannableTable =  table.unwrap(ScannableTable.class);
    if (scannableTable != null) {
      return Enumerables.toRow(scannableTable.scan(root));
    }
    //noinspection unchecked
    Enumerable<Row> iterable = table.unwrap(Enumerable.class);
    if (iterable != null) {
      return iterable;
    }
    final QueryableTable queryableTable = table.unwrap(QueryableTable.class);
    if (queryableTable != null) {
      final Type elementType = queryableTable.getElementType();
      SchemaPlus schema = root.getRootSchema();
      for (String name : Util.skipLast(table.getQualifiedName())) {
        schema = schema.getSubSchema(name);
      }
      if (elementType instanceof Class) {
        //noinspection unchecked
        final Queryable<Object> queryable = Schemas.queryable(root,
            (Class) elementType, table.getQualifiedName());
        ImmutableList.Builder<Field> fieldBuilder = ImmutableList.builder();
        Class type = (Class) elementType;
        for (Field field : type.getFields()) {
          if (Modifier.isPublic(field.getModifiers())
              && !Modifier.isStatic(field.getModifiers())) {
            fieldBuilder.add(field);
          }
        }
        final List<Field> fields = fieldBuilder.build();
        return queryable.select(
            new Function1<Object, Row>() {
              public Row apply(Object o) {
                final Object[] values = new Object[fields.size()];
                for (int i = 0; i < fields.size(); i++) {
                  Field field = fields.get(i);
                  try {
                    values[i] = field.get(o);
                  } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                  }
                }
                return new Row(values);
              }
            });
      } else {
        return Schemas.queryable(root, Row.class,
            table.getQualifiedName());
      }
    }
    throw new AssertionError("cannot convert table " + table + " to iterable");
  }

  /**
   * true 表示select *,查询全部字段
   */
  private static boolean isIdentity(int[] is, int count) {
    if (is.length != count) {
      return false;
    }
    for (int i = 0; i < is.length; i++) {//全部字段都是名字
      if (is[i] != i) {
        return false;
      }
    }
    return true;
  }
}

// End TableScanNode.java
