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
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlMoniker;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A <code>SqlCall</code> is a call to an {@link SqlOperator operator}.
 * (Operators can be used to describe any syntactic construct, so in practice,
 * every non-leaf node in a SQL parse tree is a <code>SqlCall</code> of some
 * kind.)
 *
 * SqlCall 用于调用一个操作 SqlOperator
 * 1.主要子类实现两个方法:
 * SqlOperator getOperator() 返回对应的操作是什么操作。
 * List<SqlNode> getOperandList() 操作的参数集合
 *
 *
 * sqlCall本身只表示解析parse.jj后的对象，比如sqlSelect对象，包含select from where等对象，分别代表解析后的结果。
 * 只是sqlCall除了包含解析后的对象外，还有一个能力，就是他可以根据参数信息做一些处理，
 * 因此每一个sqlCall对象都实现了自己的SqlOperator对象。
 */
public abstract class SqlCall extends SqlNode {
  //~ Constructors -----------------------------------------------------------

  public SqlCall(SqlParserPos pos) {
    super(pos);
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Whether this call was created by expanding a parentheses-free call to
   * what was syntactically an identifier.
   */
  public boolean isExpanded() {
    return false;
  }

  /**
   * Changes the value of an operand. Allows some rewrite by
   * {@link SqlValidator}; use sparingly.
   *
   * @param i Operand index
   * @param operand Operand value
   */
  public void setOperand(int i, SqlNode operand) {
    throw new UnsupportedOperationException();
  }

  //返回具体的操作是什么
  public abstract SqlOperator getOperator();

  //返回操作的参数集合
  public abstract List<SqlNode> getOperandList();

  //返回某一个参数
  @SuppressWarnings("unchecked")
  public <S extends SqlNode> S operand(int i) {
    return (S) getOperandList().get(i);
  }

  public int operandCount() {
    return getOperandList().size();
  }

  public SqlNode clone(SqlParserPos pos) {
    return getOperator().createCall(pos, getOperandList());
  }

  public void unparse(
      SqlWriter writer,
      int leftPrec,
      int rightPrec) {
    final SqlOperator operator = getOperator();
    if (leftPrec > operator.getLeftPrec()
        || (operator.getRightPrec() <= rightPrec && (rightPrec != 0))
        || writer.isAlwaysUseParentheses() && isA(SqlKind.EXPRESSION)) {
      final SqlWriter.Frame frame = writer.startList("(", ")");
      operator.unparse(writer, this, 0, 0);
      writer.endList(frame);
    } else {
      operator.unparse(writer, this, leftPrec, rightPrec);
    }
  }

  /**
   * Validates this call.
   *
   * <p>The default implementation delegates the validation to the operator's
   * {@link SqlOperator#validateCall}. Derived classes may override (as do,
   * for example {@link SqlSelect} and {@link SqlUpdate}).
   */
  public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validateCall(this, scope);
  }

  public void findValidOptions(
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlParserPos pos,
      Collection<SqlMoniker> hintList) {
    for (SqlNode operand : getOperandList()) {
      if (operand instanceof SqlIdentifier) {
        SqlIdentifier id = (SqlIdentifier) operand;
        SqlParserPos idPos = id.getParserPosition();
        if (idPos.toString().equals(pos.toString())) {
          ((SqlValidatorImpl) validator).lookupNameCompletionHints(
              scope, id.names, pos, hintList);
          return;
        }
      }
    }
    // no valid options
  }

  public <R> R accept(SqlVisitor<R> visitor) {
    return visitor.visit(this);
  }

  public boolean equalsDeep(SqlNode node, boolean fail) {
    if (node == this) {
      return true;
    }
    if (!(node instanceof SqlCall)) {
      assert !fail : this + "!=" + node;
      return false;
    }
    SqlCall that = (SqlCall) node;

    // Compare operators by name, not identity, because they may not
    // have been resolved yet.
    if (!this.getOperator().getName().equals(that.getOperator().getName())) {
      assert !fail : this + "!=" + node;
      return false;
    }
    return equalDeep(this.getOperandList(), that.getOperandList(), fail);
  }

  /**
   * Returns a string describing the actual argument types of a call, e.g.
   * "SUBSTR(VARCHAR(12), NUMBER(3,2), INTEGER)".
   */
  protected String getCallSignature(
      SqlValidator validator,
      SqlValidatorScope scope) {
    List<String> signatureList = new ArrayList<String>();
    for (final SqlNode operand : getOperandList()) {
      final RelDataType argType = validator.deriveType(scope, operand);
      if (null == argType) {
        continue;
      }
      signatureList.add(argType.toString());
    }
    return SqlUtil.getOperatorSignature(getOperator(), signatureList);
  }

  public SqlMonotonicity getMonotonicity(SqlValidatorScope scope) {
    // Delegate to operator.
    return getOperator().getMonotonicity(this, scope);
  }

  /**
   * Test to see if it is the function COUNT(*)
   *
   * @return boolean true if function call to COUNT(*)
   * 是否参数是count(*)
   */
  public boolean isCountStar() {
    if (getOperator().isName("COUNT") && operandCount() == 1) {//操作是count,并且只有一个参数
      final SqlNode parm = operand(0);//获取参数节点
      if (parm instanceof SqlIdentifier) {//该节点只有一个字段,并且是*
        SqlIdentifier id = (SqlIdentifier) parm;
        if (id.isStar() && id.names.size() == 1) {
          return true;
        }
      }
    }

    return false;
  }

  public SqlLiteral getFunctionQuantifier() {
    return null;
  }
}

// End SqlCall.java
