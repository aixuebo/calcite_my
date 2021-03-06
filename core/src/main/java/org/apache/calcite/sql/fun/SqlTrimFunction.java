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
package org.apache.calcite.sql.fun;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SameOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.sql.type.SqlTypeUtil;

import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.List;

/**
 * Definition of the "TRIM" builtin SQL function.
 * trim(both ' ' from e.name)
 */
public class SqlTrimFunction extends SqlFunction {
  //~ Enums ------------------------------------------------------------------

  /**
   * Defines the enumerated values "LEADING", "TRAILING", "BOTH".
   */
  public enum Flag implements SqlLiteral.SqlSymbol {
    BOTH(1, 1), LEADING(1, 0), TRAILING(0, 1);

    private final int left;
    private final int right;

    Flag(int left, int right) {
      this.left = left;
      this.right = right;
    }

    public int getLeft() {
      return left;
    }

    public int getRight() {
      return right;
    }

    /**
     * Creates a parse-tree node representing an occurrence of this flag
     * at a particular position in the parsed text.
     */
    public SqlLiteral symbol(SqlParserPos pos) {
      return SqlLiteral.createSymbol(this, pos);
    }
  }

  //~ Constructors -----------------------------------------------------------

  public SqlTrimFunction() {
    super(
        "TRIM",
        SqlKind.TRIM,
        ReturnTypes.cascade(//???????????????
            ReturnTypes.ARG2,//?????????????????????????????????????????????,?????????????????????string,??????????????????string
            SqlTypeTransforms.TO_NULLABLE,//????????????null
            SqlTypeTransforms.TO_VARYING),//?????????string??????
        null,
        OperandTypes.and(//?????????????????????
            OperandTypes.family(
                SqlTypeFamily.ANY, SqlTypeFamily.STRING, SqlTypeFamily.STRING),//??????????????????,????????????????????????string???string
            // Arguments 1 and 2 must have same type
            new SameOperandTypeChecker(3) {//??????3????????????????????????
              @Override protected List<Integer>
              getOperandList(int operandCount) {//???????????????1???2,????????????1???2??????????????????
                return ImmutableList.of(1, 2);
              }
            }),
        SqlFunctionCategory.STRING);
  }

  //~ Methods ----------------------------------------------------------------

  public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    final SqlWriter.Frame frame = writer.startFunCall(getName());
    assert call.operand(0) instanceof SqlLiteral : call.operand(0);
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    call.operand(1).unparse(writer, leftPrec, rightPrec);
    writer.sep("FROM");
    call.operand(2).unparse(writer, leftPrec, rightPrec);
    writer.endFunCall(frame);
  }

  //??????????????????
  public String getSignatureTemplate(final int operandsCount) {
    switch (operandsCount) {
    case 3:
      return "{0}([BOTH|LEADING|TRAILING} {1} FROM {2})";
    default:
      throw new AssertionError();
    }
  }

  public SqlCall createCall(
      SqlLiteral functionQualifier,
      SqlParserPos pos,
      SqlNode... operands) {//?????????3?????????,?????????flag????????????" "??????????????????trim??????????????????
    assert functionQualifier == null;
    switch (operands.length) {
    case 1:
      // This variant occurs when someone writes TRIM(string)
      // as opposed to the sugared syntax TRIM(string FROM string).
      operands = new SqlNode[]{
        Flag.BOTH.symbol(SqlParserPos.ZERO),//?????????2??????trim
        SqlLiteral.createCharString(" ", pos),//???????????????????????????
        operands[0] //??????????????????
      };
      break;
    case 3:
      assert operands[0] instanceof SqlLiteral
          && ((SqlLiteral) operands[0]).getValue() instanceof Flag;//????????????????????????flag
      if (operands[1] == null) {
        operands[1] = SqlLiteral.createCharString(" ", pos);
      }
      break;
    default:
      throw new IllegalArgumentException(
          "invalid operand count " + Arrays.toString(operands));
    }
    return super.createCall(functionQualifier, pos, operands);
  }

  //??????????????????????????????
  public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    if (!super.checkOperandTypes(callBinding, throwOnFailure)) {//????????????1???2?????????????????????
      return false;
    }
    final List<SqlNode> operands = callBinding.getCall().getOperandList();
    //?????????????????????????????????????????????
    return SqlTypeUtil.isCharTypeComparable(
        callBinding,
        ImmutableList.of(operands.get(1), operands.get(2)), throwOnFailure);
  }
}

// End SqlTrimFunction.java
