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
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlUtil;

import java.util.Collections;

/**
 * Type checking strategy which verifies that types have the required attributes
 * to be used as arguments to comparison operators.
 * 类型相同,并且是可比较的类型
 */
public class ComparableOperandTypeChecker extends SameOperandTypeChecker {
  //~ Instance fields --------------------------------------------------------

  private final RelDataTypeComparability requiredComparability;//比较形式

  //~ Constructors -----------------------------------------------------------

  public ComparableOperandTypeChecker(
      int nOperands,//参数数量
      RelDataTypeComparability requiredComparability) {
    super(nOperands);
    this.requiredComparability = requiredComparability;
  }

  //~ Methods ----------------------------------------------------------------

  public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    boolean b = true;
    for (int i = 0; i < nOperands; ++i) {//循环每一个参数
      RelDataType type = callBinding.getOperandType(i);
      if (!checkType(callBinding, throwOnFailure, type)) {//校验参数是否支持排序
        b = false;
      }
    }
    if (b) {//调用父类,判断参数类型是否都相同
      b = super.checkOperandTypes(callBinding, false);
      if (!b && throwOnFailure) {
        throw callBinding.newValidationSignatureError();
      }
    }
    return b;
  }

  //校验类型是否支持排序
  private boolean checkType(
      SqlCallBinding callBinding,
      boolean throwOnFailure,
      RelDataType type) {
    if (type.getComparability().ordinal()
        < requiredComparability.ordinal()) {
      if (throwOnFailure) {
        throw callBinding.newValidationSignatureError();
      } else {
        return false;
      }
    } else {
      return true;
    }
  }

  /**
   * Similar functionality to
   * {@link #checkOperandTypes(SqlCallBinding, boolean)}, but not part of the
   * interface, and cannot throw an error.
   */
  public boolean checkOperandTypes(
      SqlOperatorBinding callBinding) {
    boolean b = true;
    for (int i = 0; i < nOperands; ++i) {//校验每一个参数是否支持比较。必须要求都支持比较。
      RelDataType type = callBinding.getOperandType(i);
      boolean result;
      if (type.getComparability().ordinal()
          < requiredComparability.ordinal()) {
        result = false;
      } else {
        result = true;
      }
      if (!result) {
        b = false;
      }
    }
    if (b) {//调用父类,判断参数类型是否都相同
      b = super.checkOperandTypes(callBinding);
    }
    return b;
  }

  // implement SqlOperandTypeChecker
  public String getAllowedSignatures(SqlOperator op, String opName) {
    return SqlUtil.getAliasedSignature(op, opName,
        Collections.nCopies(nOperands, "COMPARABLE_TYPE"));
  }
}

// End ComparableOperandTypeChecker.java
