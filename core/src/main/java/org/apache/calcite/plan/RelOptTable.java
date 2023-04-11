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
package org.apache.calcite.plan;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

/**
 * Represents a relational dataset in a {@link RelOptSchema}. It has methods to
 * describe and implement itself.
 * 表达式rel操作的一个table表
 */
public interface RelOptTable {
  //~ Methods ----------------------------------------------------------------

  /**
   * Obtains an identifier for this table. The identifier must be unique with
   * respect to the Connection producing this table.
   *
   * @return qualified name 表的全路径名称
   */
  List<String> getQualifiedName();

  /**
   * Returns an estimate of the number of rows in the table.
   * 预估表的数据量
   */
  double getRowCount();

  /**
   * Describes the type of rows returned by this table.
   * 表的表结构
   */
  RelDataType getRowType();

  /**
   * Returns the {@link RelOptSchema} this table belongs to.
   * 表归属哪个schema
   */
  RelOptSchema getRelOptSchema();

  /**
   * Converts this table into a {@link RelNode relational expression}.
   *
   * <p>The {@link org.apache.calcite.plan.RelOptPlanner planner} calls this
   * method to convert a table into an initial relational expression,
   * generally something abstract, such as a
   * {@link org.apache.calcite.rel.logical.LogicalTableScan},
   * then optimizes this expression by
   * applying {@link org.apache.calcite.plan.RelOptRule rules} to transform it
   * into more efficient access methods for this table.</p>
   * 转换成关系表达式
   */
  RelNode toRel(ToRelContext context);

  /**
   * Returns a description of the physical ordering (or orderings) of the rows
   * returned from this table.
   *
   * @see RelNode#getCollationList()
   * 如何排序
   */
  List<RelCollation> getCollationList();

  /**
   * Returns whether the given columns are a key or a superset of a unique key
   * of this table.
   *
   * @param columns Ordinals of key columns
   * @return Whether the given columns are a key or a superset of a key
   * 是否是表的主键
   */
  boolean isKey(ImmutableBitSet columns);

  /**
   * Finds an interface implemented by this table.
   * 找到该类实现的接口
   */
  <T> T unwrap(Class<T> clazz);

  /**
   * Generates code for this table.
   *
   * @param clazz The desired collection class; for example {@code Queryable}.
   */
  Expression getExpression(Class clazz);

  /** Returns a table with the given extra fields.
   * 追加额外的列
   **/
  RelOptTable extend(List<RelDataTypeField> extendedFields);

  /** Can expand a view into relational expressions. */
  interface ViewExpander {
    RelNode expandView(
        RelDataType rowType,
        String queryString,
        List<String> schemaPath);
  }

  /** Contains the context needed to convert a a table into a relational
   * expression.
   * 实现类是LixToRelTranslator
   **/
  interface ToRelContext extends ViewExpander {
    RelOptCluster getCluster();
  }
}

// End RelOptTable.java
