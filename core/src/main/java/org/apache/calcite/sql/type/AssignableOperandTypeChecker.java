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
package org.apache.calcite.sql.type;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * AssignableOperandTypeChecker implements {@link SqlOperandTypeChecker} by
 * verifying that the type of each argument is assignable to a predefined set of
 * parameter types (under the SQL definition of "assignable").
 * 分配参数类型,要求参数一定是这些类型的。顺序要一致
 */
public class AssignableOperandTypeChecker implements SqlOperandTypeChecker {
  //~ Instance fields --------------------------------------------------------

  private final List<RelDataType> paramTypes;//校验参数依次是这些类型的,即他一定是具体参数值的父类类型

  //~ Constructors -----------------------------------------------------------

  /**
   * Instantiates this strategy with a specific set of parameter types.
   *
   * @param paramTypes parameter types for operands; index in this array
   *                   corresponds to operand number
   */
  public AssignableOperandTypeChecker(List<RelDataType> paramTypes) {
    this.paramTypes = ImmutableList.copyOf(paramTypes);
  }

  //~ Methods ----------------------------------------------------------------

  // implement SqlOperandTypeChecker
  //参数数量
  public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.of(paramTypes.size());
  }

  // implement SqlOperandTypeChecker
  public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    final List<SqlNode> operands = callBinding.getCall().getOperandList();//获取参数
    for (Pair<RelDataType, SqlNode> pair : Pair.zip(paramTypes, operands)) {
      RelDataType argType = callBinding.getValidator().deriveType(
              callBinding.getScope(),
              pair.right);//推测参数类型
      if (!SqlTypeUtil.canAssignFrom(pair.left, argType)) {//推测的参数类型,必须可以转换成给定的参数类型,即他一定是具体参数值的父类类型
        if (throwOnFailure) {//如果不是,说明校验失败,抛异常
          throw callBinding.newValidationSignatureError();
        } else {
          return false;
        }
      }
    }
    return true;
  }

  // implement SqlOperandTypeChecker
  //打印每一个参数的给定父类类型
  public String getAllowedSignatures(SqlOperator op, String opName) {
    StringBuilder sb = new StringBuilder();
    sb.append(opName);
    sb.append("(");
    for (Ord<RelDataType> paramType : Ord.zip(paramTypes)) {
      if (paramType.i > 0) {
        sb.append(", ");
      }
      sb.append("<");
      sb.append(paramType.e.getFamily());
      sb.append(">");
    }
    sb.append(")");
    return sb.toString();
  }
}

// End AssignableOperandTypeChecker.java
