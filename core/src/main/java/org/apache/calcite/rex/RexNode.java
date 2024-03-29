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
package org.apache.calcite.rex;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;

import java.util.Collection;

/**
 * Row expression.
 *
 * <p>Every row-expression has a type.
 * (Compare with {@link org.apache.calcite.sql.SqlNode}, which is created before
 * validation, and therefore types may not be available.)
 * 每一行表达式都是RexNode类型,即RexNode是行表达式的根节点
 * 与SqlNode比较差异的话,SqlNode是在校验前创建的,因此他可能是不可用的。而RexNode一定是校验后产生的,一定可用
 *
 * <p>Some common row-expressions are: {@link RexLiteral} (constant value),
 * {@link RexVariable} (variable), {@link RexCall} (call to operator with
 * operands). Expressions are generally created using a {@link RexBuilder}
 * factory.</p>
 * 比如RexLiteral表示常量,RexVariable表示变量,RexCall表示一个带参数的方法。
 * 他们通常都通过RexBuilder创建
 *
 * <p>All sub-classes of RexNode are immutable.</p>
 * 所有的行表达式都是不可变对象
 *
 * 表示一个表达式
 */
public abstract class RexNode {
  //~ Instance fields --------------------------------------------------------

  // Effectively final. Set in each sub-class constructor, and never re-set.
  protected String digest;

  //~ Methods ----------------------------------------------------------------

  //表达式返回类型
  public abstract RelDataType getType();

  /**
   * Returns whether this expression always returns true. (Such as if this
   * expression is equal to the literal <code>TRUE</code>.)
   */
  public boolean isAlwaysTrue() {
    return false;
  }

  /**
   * Returns whether this expression always returns false. (Such as if this
   * expression is equal to the literal <code>FALSE</code>.)
   */
  public boolean isAlwaysFalse() {
    return false;
  }

  public boolean isA(SqlKind kind) {
    return getKind() == kind;
  }

  public boolean isA(Collection<SqlKind> kinds) {
    return getKind().belongsTo(kinds);
  }

  /**
   * Returns the kind of node this is.
   *
   * @return Node kind, never null
   */
  public SqlKind getKind() {
    return SqlKind.OTHER;
  }

  public String toString() {
    return digest;
  }

  /**
   * Accepts a visitor, dispatching to the right overloaded
   * {@link RexVisitor#visitInputRef visitXxx} method.
   *
   * <p>Also see {@link RexProgram#apply(RexVisitor, java.util.List, RexNode)},
   * which applies a visitor to several expressions simultaneously.
   *
   * 参数是访问器,该访问器是明确要拿到什么内容的。
   * 依次递归访问即可,属于访问者模式。
   */
  public abstract <R> R accept(RexVisitor<R> visitor);
}

// End RexNode.java
