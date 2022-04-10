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

import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.util.Util;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Parameter type-checking strategy type must be a literal (whether null is
 * allowed is determined by the constructor). <code>CAST(NULL as ...)</code> is
 * considered to be a NULL literal but not <code>CAST(CAST(NULL as ...) AS
 * ...)</code>
 * 参数校验策略:参数必须是一个SqlLiteral类型的值
 * 是否允许null通过,取决于构造函数。
 */
public class LiteralOperandTypeChecker implements SqlSingleOperandTypeChecker {
  //~ Instance fields --------------------------------------------------------

  private boolean allowNull;//是否允许null

  //~ Constructors -----------------------------------------------------------

  public LiteralOperandTypeChecker(boolean allowNull) {
    this.allowNull = allowNull;
  }

  //~ Methods ----------------------------------------------------------------

  public boolean checkSingleOperandType(
      SqlCallBinding callBinding,
      SqlNode node,
      int iFormalOperand,
      boolean throwOnFailure) {
    Util.discard(iFormalOperand);

    if (SqlUtil.isNullLiteral(node, true)) {//是null
      if (allowNull) {//允许
        return true;//返回true,校验成功
      }
      if (throwOnFailure) {//抛异常
        throw callBinding.newError(
            RESOURCE.argumentMustNotBeNull(
                callBinding.getOperator().getName()));
      }
      return false;//值是null,不允许null,则返回false,校验失败
    }

    //说明值是非null
    if (!SqlUtil.isLiteral(node) && !SqlUtil.isLiteralChain(node)) {//不是LITERAL,也不是LITERAL_CHAIN,说明校验失败,直接返回false,或者抛异常
      if (throwOnFailure) {
        throw callBinding.newError(
            RESOURCE.argumentMustBeLiteral(
                callBinding.getOperator().getName()));
      }
      return false;
    }

    return true;
  }

  public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    return checkSingleOperandType(
        callBinding,
        callBinding.getCall().operand(0),//仅有一个参数需要校验,因此调用上面的方法
        0,
        throwOnFailure);
  }

  //返回函数允许的参数数量,即只允许1个参数,并且该参数是LITERAL形式的
  public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.of(1);
  }

  //使用LITERAL,文字形式
  public String getAllowedSignatures(SqlOperator op, String opName) {
    return "<LITERAL>";
  }
}

// End LiteralOperandTypeChecker.java
