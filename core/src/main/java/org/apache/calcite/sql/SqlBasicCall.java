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
package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.UnmodifiableArrayList;

import java.util.List;

/**
 * Implementation of {@link SqlCall} that keeps its operands in an array.
 * SqlBasicCall 基础实现类
 * 1.只需要构造函数传递 操作对象、参数对象即可。
 * 2.SqlKind取operator.getKind();
 * 3.额外提供参数
 * boolean expanded
 * SqlLiteral functionQuantifier;//DISTINCT等关键词常量
 *
 * 最简单的SqlCall，相比较select、delete等复杂的SqlCall来说
 * 说简单的原因是，他是基础SqlNode的基础上，套一层SqlOperator，比如from (query) as xx
 */
public class SqlBasicCall extends SqlCall {
  private SqlOperator operator;//具体的操作对象,比如具体的函数、具体的insert操作对象---真正执行函数的实现体--主要createCall和acceptCall方法
  public final SqlNode[] operands;//实际参数数组
  private final SqlLiteral functionQuantifier;//DISTINCT等关键词 可能表示函数名
  private final boolean expanded;

  public SqlBasicCall(
      SqlOperator operator,
      SqlNode[] operands,
      SqlParserPos pos) {
    this(operator, operands, pos, false, null);
  }

  //比如 from () as xx
  protected SqlBasicCall(
      SqlOperator operator,//SqlAsOperator
      SqlNode[] operands,//query表达式
      SqlParserPos pos,
      boolean expanded,
      SqlLiteral functionQualifier) {
    super(pos);
    this.operator = operator;
    this.operands = operands;
    this.expanded = expanded;
    this.functionQuantifier = functionQualifier;
  }

  public SqlKind getKind() {
    return operator.getKind();
  }

  @Override public boolean isExpanded() {
    return expanded;
  }

  @Override public void setOperand(int i, SqlNode operand) {
    operands[i] = operand;
  }

  public void setOperator(SqlOperator operator) {
    this.operator = operator;
  }

  public SqlOperator getOperator() {
    return operator;
  }

  public SqlNode[] getOperands() {
    return operands;
  }

  public List<SqlNode> getOperandList() {
    return UnmodifiableArrayList.of(operands); // not immutable, but quick
  }

  //获取第几个参数
  @SuppressWarnings("unchecked")
  @Override public <S extends SqlNode> S operand(int i) {
    return (S) operands[i];
  }

  //参数数量
  @Override public int operandCount() {
    return operands.length;
  }

  @Override public SqlLiteral getFunctionQuantifier() {
    return functionQuantifier;
  }
}

// End SqlBasicCall.java
