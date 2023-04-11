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

    private final int left;//是否trim左边
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
        ReturnTypes.cascade(//设置返回值
            ReturnTypes.ARG2,//基于第三个参数的类型作为返回值,即第三个参数是string,因此返回值是string
            SqlTypeTransforms.TO_NULLABLE,//允许值为null
            SqlTypeTransforms.TO_VARYING),//转换成string类型
        null,
        OperandTypes.and(//设置参数校验器
            OperandTypes.family(
                SqlTypeFamily.ANY, SqlTypeFamily.STRING, SqlTypeFamily.STRING),//接受参数三个,分别是任意类型、string、string
            // Arguments 1 and 2 must have same type
            new SameOperandTypeChecker(3) {//要求3个参数都相同类型
              @Override protected List<Integer>
              getOperandList(int operandCount) {//参数需要是1和2,即只校验1和2类型相同即可
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

  //方法签名描述
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
      SqlNode... operands) {//最终是3个参数,第一个flag、第二个" "、第三个具体trim要处理的字段
    assert functionQualifier == null;
    switch (operands.length) {
    case 1:
      // This variant occurs when someone writes TRIM(string)
      // as opposed to the sugared syntax TRIM(string FROM string).
      operands = new SqlNode[]{
        Flag.BOTH.symbol(SqlParserPos.ZERO),//默认是2边都trim
        SqlLiteral.createCharString(" ", pos),//创建一个字符串常量
        operands[0] //待替换的字段
      };
      break;
    case 3:
      assert operands[0] instanceof SqlLiteral
          && ((SqlLiteral) operands[0]).getValue() instanceof Flag;//第一个参数一定是flag
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

  //校验参数类型是否合法
  public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    if (!super.checkOperandTypes(callBinding, throwOnFailure)) {//校验参数1和2都是字符串类型
      return false;
    }
    final List<SqlNode> operands = callBinding.getCall().getOperandList();
    //参数必须是可以比较的字符串类型
    return SqlTypeUtil.isCharTypeComparable(
        callBinding,
        ImmutableList.of(operands.get(1), operands.get(2)), throwOnFailure);
  }
}

// End SqlTrimFunction.java
