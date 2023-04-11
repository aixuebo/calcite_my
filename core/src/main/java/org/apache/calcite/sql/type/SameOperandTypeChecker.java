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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Parameter type-checking strategy where all operand types must be the same.
 * 参数校验策略 -- 所有的参数必须相同类型,并且不允许为null,一旦出现null,也说明参数不同,返回false,校验失败
 */
public class SameOperandTypeChecker implements SqlSingleOperandTypeChecker {
  //~ Instance fields --------------------------------------------------------

  protected final int nOperands;//参数数量,-1表示无参数或者无限参数数量

  //~ Constructors -----------------------------------------------------------

  public SameOperandTypeChecker(
      int nOperands) {
    this.nOperands = nOperands;
  }

  //~ Methods ----------------------------------------------------------------

  // implement SqlOperandTypeChecker
  //校验所有参数
  public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    return checkOperandTypesImpl(
        callBinding,
        throwOnFailure,
        callBinding);
  }

  //返回参数序号集合
  protected List<Integer> getOperandList(int operandCount) {
    return nOperands == -1
        ? Util.range(0, operandCount)
        : Util.range(0, nOperands);
  }

  //校验所有参数都是相同的类型
  private boolean checkOperandTypesImpl(
      SqlOperatorBinding operatorBinding,
      boolean throwOnFailure,
      SqlCallBinding callBinding) {
    int nOperandsActual = nOperands;
    if (nOperandsActual == -1) {
      nOperandsActual = operatorBinding.getOperandCount();//真实传递的参数数量
    }
    assert !(throwOnFailure && (callBinding == null));
    RelDataType[] types = new RelDataType[nOperandsActual];//存储每一个参数的返回值
    final List<Integer> operandList = getOperandList(operatorBinding.getOperandCount());//参数序号
    for (int i : operandList) {
      if (operatorBinding.isOperandNull(i, false)) {//参数是否是null
        if (throwOnFailure) {
          throw callBinding.getValidator().newValidationError(
              callBinding.getCall().operand(i), RESOURCE.nullIllegal());
        } else {
          return false;
        }
      }
      types[i] = operatorBinding.getOperandType(i);//返回参数类型
    }
    int prev = -1;//前一个参数序号
    for (int i : operandList) {
      if (prev >= 0) {
        if (!SqlTypeUtil.isComparable(types[i], types[prev])) {//确保当前参数类型与前一个参数类型相同,如果不同,则抛异常
          if (!throwOnFailure) {
            return false;
          }

          // REVIEW jvs 5-June-2005: Why don't we use
          // newValidationSignatureError() here?  It gives more
          // specific diagnostics.
          throw callBinding.newValidationError(
              RESOURCE.needSameTypeParameter());
        }
      }
      prev = i;
    }
    return true;
  }

  /**
   * Similar functionality to
   * {@link #checkOperandTypes(SqlCallBinding, boolean)}, but not part of the
   * interface, and cannot throw an error.
   * 校验全部参数
   */
  public boolean checkOperandTypes(
      SqlOperatorBinding operatorBinding) {
    return checkOperandTypesImpl(operatorBinding, false, null);
  }

  // implement SqlOperandTypeChecker 无限个参数
  public SqlOperandCountRange getOperandCountRange() {
    if (nOperands == -1) {
      return SqlOperandCountRanges.any();//无参数 或者 无穷个参数
    } else {
      return SqlOperandCountRanges.of(nOperands);
    }
  }

  // implement SqlOperandTypeChecker
  //设置(EQUIVALENT_TYPE),告诉使用者,每一个参数都是相同的类型,如果是-1则用..表示无穷个参数,所有参数都是相同类型
  public String getAllowedSignatures(SqlOperator op, String opName) {
    return SqlUtil.getAliasedSignature(op, opName,
        nOperands == -1
            ? ImmutableList.of("EQUIVALENT_TYPE", "EQUIVALENT_TYPE", "...")
            : Collections.nCopies(nOperands, "EQUIVALENT_TYPE"));
  }

  //不支持,因为一个参数是没有办法比较相等的
  public boolean checkSingleOperandType(
      SqlCallBinding callBinding,
      SqlNode operand,
      int iFormalOperand,
      boolean throwOnFailure) {
    throw new UnsupportedOperationException(); // TODO:
  }
}

// End SameOperandTypeChecker.java
