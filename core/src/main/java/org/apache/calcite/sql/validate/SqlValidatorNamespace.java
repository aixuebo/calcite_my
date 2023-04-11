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
package org.apache.calcite.sql.validate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Pair;

import java.util.List;

/**
 * 命名空间，描述sql的一部分表示的关系
 * A namespace describes the relation returned by a section of a SQL query.
 *
 * 比如下面的例子，from由2个表组成一个新的空间
 * <p>For example, in the query <code>SELECT emp.deptno, age FROM emp,
 * dept</code>, the FROM clause forms a namespace consisting of two tables EMP
 * and DEPT, and a row type consisting of the combined columns of those tables.
 *
 *
 * <p>Other examples of namespaces include a table in the from list (the
 * namespace contains the constituent columns) and a subquery (the namespace
 * contains the columns in the SELECT clause of the subquery).
 *
 * <p>These various kinds of namespace are implemented by classes
 * {@link IdentifierNamespace} for table names, {@link SelectNamespace} for
 * SELECT queries, {@link SetopNamespace} for UNION, EXCEPT and INTERSECT, and
 * so forth. But if you are looking at a SELECT query and call
 * {@link SqlValidator#getNamespace(org.apache.calcite.sql.SqlNode)}, you may
 * not get a SelectNamespace. Why? Because the validator is allowed to wrap
 * namespaces in other objects which implement
 * {@link SqlValidatorNamespace}. Your SelectNamespace will be there somewhere,
 * but might be one or two levels deep.  Don't try to cast the namespace or use
 * <code>instanceof</code>; use {@link SqlValidatorNamespace#unwrap(Class)} and
 * {@link SqlValidatorNamespace#isWrapperFor(Class)} instead.</p>
 *
 * @see SqlValidator
 * @see SqlValidatorScope
 * 标识一个节点的输出类型与name、持有的root节点(比如selectNode)
 *
 * 一个命名空间多半指代一直表 或者 一个子查询
 * 一个scope可以包含多个命名空间，比如一顿join,他们都在一个scope范围内，我是这么理解scope和Namespace区别的
 */
public interface SqlValidatorNamespace {
  //~ Methods ----------------------------------------------------------------

  /**
   * Returns the validator.
   *
   * @return validator 返回SqlValidatorImpl,即返回校验器对象,这个对象是全局一起用，核心类,但在该空间内可忽略
   */
  SqlValidator getValidator();

  /**
   * Returns the underlying table, or null if there is none.
   * 如果是一个table表,则设置表空间
   */
  SqlValidatorTable getTable();

  /**
   * Returns the row type of this namespace, which comprises a list of names
   * and types of the output columns. If the scope's type has not yet been
   * derived, derives it.
   *
   * @return Row type of this namespace, never null, always a struct
   * 输出row的类型,包含输出列的名字以及类型
   */
  RelDataType getRowType();

  /**
   * Returns the type of this namespace.
   * 输出row的类型,包含输出列的名字以及类型
   * @return Row type converted to struct
   */
  RelDataType getType();

  /**
   * Sets the type of this namespace.
   *
   * <p>Allows the type for the namespace to be explicitly set, but usually is
   * called during {@link #validate()}.</p>
   *
   * <p>Implicitly also sets the row type. If the type is not a struct, then
   * the row type is the type wrapped as a struct with a single column,
   * otherwise the type and row type are the same.</p>
   * 设置输出类型
   */
  void setType(RelDataType type);

  /**
   * Returns the row type of this namespace, sans any system columns.
   *
   * sans 表示无，即不包含系统列的所有列信息
   * @return Row type sans system columns
   * 返回getRowType 不包含所有系统列集合
   */
  RelDataType getRowTypeSansSystemColumns();

  /**
   * Validates this namespace.
   * 校验该命名空间 ---命名空间作为一个实体,肯定也需要被校验的? 主要校验状态、校验输出row的类型,包含输出列的名字以及类型
   * <p>If the scope has already been validated, does nothing.</p>
   * 如果已经被校验过了,则什么也不用做了,不需要再次校验
   *
   * <p>Please call {@link SqlValidatorImpl#validateNamespace} rather than
   * calling this method directly.</p>
   */
  void validate();

  /**
   * Returns the parse tree node at the root of this namespace.
   * 该命名空间持有的根节点,比如SelectNode、或者SqlJoin extends SqlCall等节点
   * 即属于哪个sqlNode节点的空间
   *
   * @return parse tree node; null for {@link TableNamespace}
   */
  SqlNode getNode();

  /**
   * Returns the parse tree node that at is at the root of this namespace and
   * includes all decorations. If there are no decorations, returns the same
   * as {@link #getNode()}.
   * 返回root根节点的Node,包含所有的装饰对象,如果没有装饰对象,则与返回getNode结果相同
   * 封闭节点
   */
  SqlNode getEnclosingNode();

  /**
   * Looks up a child namespace of a given name.
   *
   * <p>For example, in the query <code>select e.name from emps as e</code>,
   * <code>e</code> is an {@link IdentifierNamespace} which has a child <code>
   * name</code> which is a {@link FieldNamespace}.
   *
   * @param name Name of namespace
   * @return Namespace
   * 找到子空间,可能是一个字段,因此拿字段的类型组成字段子空间FieldNamespace
   */
  SqlValidatorNamespace lookupChild(String name);

  /**
   * Returns whether this namespace has a field of a given name.
   *
   * @param name Field name
   * @return Whether field exists
   * 是否存在某一个列
   */
  boolean fieldExists(String name);

  /**
   * Returns a list of expressions which are monotonic in this namespace. For
   * example, if the namespace represents a relation ordered by a column
   * called "TIMESTAMP", then the list would contain a
   * {@link org.apache.calcite.sql.SqlIdentifier} called "TIMESTAMP".
   * 返回空间内有单调性的表达式集合 --- 表达式Node与单调性
   */
  List<Pair<SqlNode, SqlMonotonicity>> getMonotonicExprs();

  /**
   * Returns whether and how a given column is sorted.
   * 字段是否是排序的,以及如何排序的
   */
  SqlMonotonicity getMonotonicity(String columnName);

  /**
   * Makes all fields in this namespace nullable (typically because it is on
   * the outer side of an outer join.
   * 强制所有的属性字段都支持 nullable
   */
  void makeNullable();

  /**
   * Translates a field name to the name in the underlying namespace.
   * 将一个名字转换成命名空间下的名字。
   */
  String translate(String name);

  /**
   * Returns this namespace, or a wrapped namespace, cast to a particular
   * class.
   * 相当于强转成具体实现的命名空间,比如SelectNamespace
   * @param clazz Desired type
   * @return This namespace cast to desired type
   * @throws ClassCastException if no such interface is available
   */
  <T> T unwrap(Class<T> clazz);

  /**
   * Returns whether this namespace implements a given interface, or wraps a
   * class which does.
   *
   * @param clazz Interface
   * @return Whether namespace implements given interface
   * 返回class是否是该空间的实现类,或者子类
   */
  boolean isWrapperFor(Class<?> clazz);

  /** If this namespace resolves to another namespace, returns that namespace,
   * following links to the end of the chain.
   *
   * <p>A {@code WITH}) clause defines table names that resolve to queries
   * (the body of the with-item). An {@link IdentifierNamespace} typically
   * resolves to a {@link TableNamespace}.</p>
   *
   * <p>You must not call this method before {@link #validate()} has
   * completed.</p> */
  SqlValidatorNamespace resolve();
}

// End SqlValidatorNamespace.java
