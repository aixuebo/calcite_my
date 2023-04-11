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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Util;

/**
 * A postfix unary operator.
 is [not] null --> SqlStdOperatorTable.IS_NOT_NULL/IS_NULL
 is [not] true ---> SqlStdOperatorTable.IS_NOT_TRUE/IS_TRUE
 is [not] false ---> SqlStdOperatorTable.IS_NOT_FALSE/IS_FALSE
 is [not] UNKNOWN ---> SqlStdOperatorTable.IS_NOT_UNKNOWN/IS_UNKNOWN
 is a set ---> SqlStdOperatorTable.IS_A_SET 用于检测嵌套表是否包含重复的元素值
 DESC
 NULLS FIRST、NULLS LAST
 窗口函数中FOLLOWING、PRECEDING
 */
public class SqlPostfixOperator extends SqlOperator {
  //~ Constructors -----------------------------------------------------------

  public SqlPostfixOperator(
      String name,
      SqlKind kind,
      int prec,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      SqlOperandTypeChecker operandTypeChecker) {
    super(
        name,
        kind,
        leftPrec(prec, true),
        rightPrec(prec, true),
        returnTypeInference,
        operandTypeInference,
        operandTypeChecker);
  }

  //~ Methods ----------------------------------------------------------------

  public SqlSyntax getSyntax() {
    return SqlSyntax.POSTFIX;
  }

  public String getSignatureTemplate(final int operandsCount) {
    Util.discard(operandsCount);
    return "{1} {0}";
  }

  protected RelDataType adjustType(
      SqlValidator validator,
      SqlCall call,
      RelDataType type) {
    if (SqlTypeUtil.inCharFamily(type)) {
      // Determine coercibility and resulting collation name of
      // unary operator if needed.
      RelDataType operandType =
          validator.getValidatedNodeType(call.operand(0));
      if (null == operandType) {
        throw Util.newInternal(
            "operand's type should have been derived");
      }
      if (SqlTypeUtil.inCharFamily(operandType)) {
        SqlCollation collation = operandType.getCollation();
        assert null != collation
            : "An implicit or explicit collation should have been set";
        type =
            validator.getTypeFactory()
                .createTypeWithCharsetAndCollation(
                    type,
                    type.getCharset(),
                    new SqlCollation(
                        collation.getCollationName(),
                        collation.getCoercibility()));
      }
    }
    return type;
  }

  @Override public boolean validRexOperands(int count, boolean fail) {
    if (count != 1) {
      assert !fail : "wrong operand count " + count + " for " + this;
      return false;
    }
    return true;
  }
}

// End SqlPostfixOperator.java
