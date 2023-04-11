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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Util;

import java.util.Arrays;
import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * class SqlCall extends SqlNode
 *   public abstract SqlOperator getOperator();
 * 可见 SqlOperator 是SqlCall这个SqlNode的一个操作类型。
 *
 * 操作是有优先级的
 * A <code>SqlOperator</code> is a type of node in a SQL parse tree (it is NOT a
 * node in a SQL parse tree). It includes functions, operators such as '=', and
 * syntactic constructs such as 'case' statements. Operators may represent
 * query-level expressions (e.g. {@link SqlSelectOperator} or row-level
 * expressions (e.g. {@link org.apache.calcite.sql.fun.SqlBetweenOperator}.
 *
 * 操作可以有参数,比如除法,第一个参数是分组、第二个参数是分母。
 * <p>Operators have <em>formal operands</em>, meaning ordered (and optionally
 * named) placeholders for the values they operate on. For example, the division
 * operator takes two operands; the first is the numerator and the second is the
 * denominator. In the context of subclass {@link SqlFunction}, formal operands
 * are referred to as <em>parameters</em>.
 *
 *
 * <p>When an operator is instantiated via a {@link SqlCall}, it is supplied
 * with <em>actual operands</em>. For example, in the expression <code>3 /
 * 5</code>, the literal expression <code>3</code> is the actual operand
 * corresponding to the numerator, and <code>5</code> is the actual operand
 * corresponding to the denominator. In the context of SqlFunction, actual
 * operands are referred to as <em>arguments</em>
 *
 * <p>In many cases, the formal/actual distinction is clear from context, in
 * which case we drop these qualifiers.
 * sql操作
 * 比如delete操作,name=delete,sqlKind=SqlKind.DELETE
 *
 *
 *
 * 1.RelDataType validateOperands 校验操作的参数合法性，根据参数类型,推测出返回值类型，并且设置到SqlValidator中。
 * a.校验参数数量是否符合规范。
 * b.校验参数类型是否符合规范。
 * c.根据参数类型推测返回值类型策略,推测返回值类型。
 * d.将代码块sqlNode与返回值类型映射,设置到全局的SqlValidator中。
 *
 * 2.RelDataType deriveType 获得操作调用的最终类型
 * a.校验每一个参数必须有返回值，
 * b.调用validateOperands，校验以及返回最终类型。
 */
public abstract class SqlOperator {
  //~ Static fields/initializers ---------------------------------------------

  public static final String NL = System.getProperty("line.separator");

  /**
   * Maximum precedence.
   */
  public static final int MDX_PRECEDENCE = 200;

  //~ Instance fields --------------------------------------------------------

  /**
   * The name of the operator/function. Ex. "OVERLAY" or "TRIM"
   * 函数或者操作名字，比如select、trim
   */
  private final String name;

  /**
   * See {@link SqlKind}. It's possible to have a name that doesn't match the
   * kind
   * sql的关键字
   */
  public final SqlKind kind;

  /**
   * The precedence with which this operator binds to the expression to the
   * left. This is less than the right precedence if the operator is
   * left-associative.
   * 优先级
   */
  private final int leftPrec;

  /**
   * The precedence with which this operator binds to the expression to the
   * right. This is more than the left precedence if the operator is
   * left-associative.
   */
  private final int rightPrec;

  /**
   * used to infer the return type of a call to this operator
   * 操作返回类型,与参数类型之间的关系，即返回类型以哪个参数类型为base去创建
   */
  private final SqlReturnTypeInference returnTypeInference;

  /**
   * used to infer types of unknown operands
   */
  private final SqlOperandTypeInference operandTypeInference;

  /**
   * used to validate operand types 如何校验参数
   */
  private final SqlOperandTypeChecker operandTypeChecker;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an operator.
   */
  protected SqlOperator(
      String name,
      SqlKind kind,
      int leftPrecedence,
      int rightPrecedence,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      SqlOperandTypeChecker operandTypeChecker) {
    assert kind != null;
    this.name = name;
    this.kind = kind;
    this.leftPrec = leftPrecedence;
    this.rightPrec = rightPrecedence;
    this.returnTypeInference = returnTypeInference;
    this.operandTypeInference = operandTypeInference;
    this.operandTypeChecker = operandTypeChecker;
  }

  /**
   * Creates an operator specifying left/right associativity.
   */
  protected SqlOperator(
      String name,
      SqlKind kind,
      int prec,
      boolean leftAssoc,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      SqlOperandTypeChecker operandTypeChecker) {
    this(
        name,
        kind,
        leftPrec(prec, leftAssoc),
        rightPrec(prec, leftAssoc),
        returnTypeInference,
        operandTypeInference,
        operandTypeChecker);
  }

  //~ Methods ----------------------------------------------------------------

  protected static int leftPrec(int prec, boolean leftAssoc) {
    assert (prec % 2) == 0;
    if (!leftAssoc) {
      ++prec;
    }
    return prec;
  }

  protected static int rightPrec(int prec, boolean leftAssoc) {
    assert (prec % 2) == 0;
    if (leftAssoc) {
      ++prec;
    }
    return prec;
  }

  public SqlOperandTypeChecker getOperandTypeChecker() {
    return operandTypeChecker;
  }

  /**
   * Returns a constraint on the number of operands expected by this operator.
   * Subclasses may override this method; when they don't, the range is
   * derived from the {@link SqlOperandTypeChecker} associated with this
   * operator.
   *
   * @return acceptable range
   * 返回参数的允许数量，用于校验参数数量是否符合标准
   */
  public SqlOperandCountRange getOperandCountRange() {
    if (operandTypeChecker != null) {
      return operandTypeChecker.getOperandCountRange();
    }

    // If you see this error you need to override this method
    // or give operandTypeChecker a value.
    throw Util.needToImplement(this);
  }

  public String getName() {
    return name;
  }

  public SqlKind getKind() {
    return kind;
  }

  public String toString() {
    return name;
  }

  public int getLeftPrec() {
    return leftPrec;
  }

  public int getRightPrec() {
    return rightPrec;
  }

  /**
   * Returns the syntactic type of this operator, never null.
   */
  public abstract SqlSyntax getSyntax();

  /**
   * Creates a call to this operand with an array of operands.
   *
   * <p>The position of the resulting call is the union of the <code>
   * pos</code> and the positions of all of the operands.
   *
   * @param functionQualifier function qualifier (e.g. "DISTINCT"), may be
   * @param pos               parser position of the identifier of the call
   * @param operands          array of operands
   * 创建一个函数,提供参数数组SqlNode,functionQualifier为函数名
   *
   * 创建sqlNode
   * 操作是明确的，只需要操作需要的参数operands 以及为操作起一个名字functionQualifier
   */
  public SqlCall createCall(
      SqlLiteral functionQualifier,
      SqlParserPos pos,
      SqlNode... operands) {
    pos = pos.plusAll(Arrays.asList(operands));
    return new SqlBasicCall(this, operands, pos, false, functionQualifier);
  }

  /**
   * Creates a call to this operand with an array of operands.
   *
   * <p>The position of the resulting call is the union of the <code>
   * pos</code> and the positions of all of the operands.
   *
   * @param pos      Parser position
   * @param operands List of arguments
   * @return call to this operator
   * 匿名函数
   *
   * 创建sqlNode --- 最常用的，因为操作是明确的，只需要操作需要的参数operands
   */
  public final SqlCall createCall(
      SqlParserPos pos,
      SqlNode... operands) {
    return createCall(null, pos, operands);
  }

  /**
   * Creates a call to this operand with a list of operands contained in a
   * {@link SqlNodeList}.
   *
   * <p>The position of the resulting call inferred from the SqlNodeList.
   *
   * @param nodeList List of arguments
   * @return call to this operator
   * 匿名函数
   *
   * 创建sqlNode
   */
  public final SqlCall createCall(
      SqlNodeList nodeList) {
    return createCall(
        null,
        nodeList.getParserPosition(),
        nodeList.toArray());
  }

  /**
   * Creates a call to this operand with a list of operands.
   *
   * <p>The position of the resulting call is the union of the <code>
   * pos</code> and the positions of all of the operands.
   * 匿名函数
   *
   * 创建sqlNode
   */
  public final SqlCall createCall(
      SqlParserPos pos,
      List<? extends SqlNode> operandList) {
    return createCall(
        null,
        pos,
        operandList.toArray(new SqlNode[operandList.size()]));
  }

  /**
   * Rewrites a call to this operator. Some operators are implemented as
   * trivial rewrites (e.g. NULLIF becomes CASE). However, we don't do this at
   * createCall time because we want to preserve the original SQL syntax as
   * much as possible; instead, we do this before the call is validated (so
   * the trivial operator doesn't need its own implementation of type
   * derivation methods). The default implementation is to just return the
   * original call without any rewrite.
   * 有时候需要重写，但是这种情况不多，比如NULLIF变成CASE形式
   * @param validator Validator
   * @param call      Call to be rewritten
   * @return rewritten call
   */
  public SqlNode rewriteCall(SqlValidator validator, SqlCall call) {
    return call;
  }

  /**
   * Writes a SQL representation of a call to this operator to a writer,
   * including parentheses if the operators on either side are of greater
   * precedence.
   *
   * <p>The default implementation of this method delegates to
   * {@link SqlSyntax#unparse}.
   */
  public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    getSyntax().unparse(writer, this, call, leftPrec, rightPrec);
  }

  // REVIEW jvs 9-June-2006: See http://issues.eigenbase.org/browse/FRG-149
  // for why this method exists.
  protected void unparseListClause(SqlWriter writer, SqlNode clause) {
    unparseListClause(writer, clause, null);
  }

  protected void unparseListClause(
      SqlWriter writer,
      SqlNode clause,
      SqlKind sepKind) {
    if (clause instanceof SqlNodeList) {
      if (sepKind != null) {
        ((SqlNodeList) clause).andOrList(writer, sepKind);
      } else {
        ((SqlNodeList) clause).commaList(writer);
      }
    } else {
      clause.unparse(writer, 0, 0);
    }
  }

  // override Object
  public boolean equals(Object obj) {
    if (!(obj instanceof SqlOperator)) {
      return false;
    }
    if (!obj.getClass().equals(this.getClass())) {
      return false;
    }
    SqlOperator other = (SqlOperator) obj;
    return name.equals(other.name) && kind == other.kind;
  }

  public boolean isName(String testName) {
    return name.equals(testName);
  }

  // override Object
  public int hashCode() {
    return kind.hashCode() + name.hashCode();
  }

  /**
   * Validates a call to this operator.
   *
   * <p>This method should not perform type-derivation or perform validation
   * related related to types. That is done later, by
   * {@link #deriveType(SqlValidator, SqlValidatorScope, SqlCall)}. This method
   * should focus on structural validation.
   *
   * <p>A typical implementation of this method first validates the operands,
   * then performs some operator-specific logic. The default implementation
   * just validates the operands.
   *
   * <p>This method is the default implementation of {@link SqlCall#validate};
   * but note that some sub-classes of {@link SqlCall} never call this method.
   *
   * @param call         the call to this operator
   * @param validator    the active validator
   * @param scope        validator scope
   * @param operandScope validator scope in which to validate operands to this
   *                     call; usually equal to scope, but not always because
   *                     some operators introduce new scopes
   * @see SqlNode#validateExpr(SqlValidator, SqlValidatorScope)
   * @see #deriveType(SqlValidator, SqlValidatorScope, SqlCall)
   *
   * 递归操作，对每一个sqlNode进行校验
   */
  public void validateCall(
      SqlCall call,
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlValidatorScope operandScope) {
    assert call.getOperator() == this;
    for (SqlNode operand : call.getOperandList()) {
      operand.validateExpr(validator, operandScope);
    }
  }

  /**
   * Validates the operands of a call, inferring the return type in the
   * process.
   *
   * @param validator active validator
   * @param scope     validation scope
   * @param call      call to be validated
   * @return inferred type
   * 校验所有的参数,推导出一个返回值类型
   *
  校验操作的参数合法性，根据参数类型,推测出返回值类型，并且设置到SqlValidator中。
  a.校验参数数量是否符合规范。
  b.校验参数类型是否符合规范。
  c.根据参数类型推测返回值类型策略,推测返回值类型。
  d.将代码块sqlNode与返回值类型映射,设置到全局的SqlValidator中。
   */
  public final RelDataType validateOperands(
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlCall call) {
    // Let subclasses know what's up.
    preValidateCall(validator, scope, call);

    // Check the number of operands 用于校验参数数量是否符合标准
    checkOperandCount(validator, operandTypeChecker, call);

    SqlCallBinding opBinding = new SqlCallBinding(validator, scope, call);

    //校验参数类型是否符合规范
    checkOperandTypes(
        opBinding,
        true);

    // Now infer the result type.
    RelDataType ret = inferReturnType(opBinding);//根据参数类型,推测出返回值类型
    //说明node已经推测计算出返回值类型了,因此做一个设置，即设置node的返回值类型为type
    validator.setValidatedNodeType(call, ret);
    return ret;
  }

  /**
   * Receives notification that validation of a call to this operator is
   * beginning. Subclasses can supply custom behavior; default implementation
   * does nothing.
   *
   * @param validator invoking validator
   * @param scope     validation scope
   * @param call      the call being validated
   *
   * 默认空实现,主要用于接收校验调研通知前,做一些前置操作
   */
  protected void preValidateCall(
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlCall call) {
  }

  /**
   * Infers the return type of an invocation of this operator; only called
   * after the number and types of operands have already been validated.
   * Subclasses must either override this method or supply an instance of
   * {@link SqlReturnTypeInference} to the constructor.
   *
   * @param opBinding description of invocation (not necessarily a
   * {@link SqlCall})
   * @return inferred return type
   * 根据操作的上下文信息,猜测操作的返回值类型
   */
  public RelDataType inferReturnType(
      SqlOperatorBinding opBinding) {
    if (returnTypeInference != null) { //直接根据参数类型策略,推导出返回类型
      return returnTypeInference.inferReturnType(opBinding);
    }

    // Derived type should have overridden this method, since it didn't
    // supply a type inference rule.
    throw Util.needToImplement(this); //必须returnTypeInference不是null,否则抛异常
  }

  /**
   * Derives the type of a call to this operator.
   * 获得操作调用的最终类型
   *
   * <p>This method is an intrinsic part of the validation process so, unlike
   * {@link #inferReturnType}, specific operators would not typically override
   * this method.
   *
   * @param validator Validator
   * @param scope     Scope of validation
   * @param call      Call to this operator
   * @return Type of call
   * 获得操作调用的最终类型
   *  a.校验每一个参数必须有返回值，
   *  b.调用1，校验以及返回最终类型。
   */
  public RelDataType deriveType(
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlCall call) {//操作的sql语法块

    //确保每一个参数都有合理的返回值
    for (SqlNode operand : call.getOperandList()) {
      RelDataType nodeType = validator.deriveType(scope, operand); //获取每一个参数的返回值,该过程是一个递归的过程
      assert nodeType != null;
    }

    //校验操作的参数合法性，根据参数类型,推测出返回值类型，并且设置到SqlValidator中。
    RelDataType type = validateOperands(validator, scope, call);

    // Validate and determine coercibility and resulting collation
    // name of binary operator if needed.
    type = adjustType(validator, call, type);

    //对字符串类型的参数进一步校验编码正确性
    SqlValidatorUtil.checkCharsetAndCollateConsistentIfCharType(type);
    return type;
  }

  /**
   * Validates and determines coercibility and resulting collation name of
   * binary operator if needed.
   * 基本上忽略，主要用于对结果进一步转换成可压缩的字节。默认啥也没有做
   */
  protected RelDataType adjustType(
      SqlValidator validator,
      final SqlCall call,
      RelDataType type) {
    return type;
  }

  /**
   * Infers the type of a call to this operator with a given set of operand
   * types. Shorthand for {@link #inferReturnType(SqlOperatorBinding)}.
   * 明确设置操作的每一个参数的类型
   */
  public final RelDataType inferReturnType(
      RelDataTypeFactory typeFactory,
      List<RelDataType> operandTypes) {
    return inferReturnType(
        new ExplicitOperatorBinding(
            typeFactory,
            this,
            operandTypes));
  }

  /**
   * Checks that the operand values in a {@link SqlCall} to this operator are
   * valid. Subclasses must either override this method or supply an instance
   * of {@link SqlOperandTypeChecker} to the constructor.
   *
   * @param callBinding    description of call
   * @param throwOnFailure whether to throw an exception if check fails
   *                       (otherwise returns false in that case)
   * @return whether check succeeded
   * 校验参数类型是否符合规范
   */
  public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    // Check that all of the operands are of the right type.
    if (null == operandTypeChecker) { //不允许为null,null则抛异常
      // If you see this you must either give operandTypeChecker a value
      // or override this method.
      throw Util.needToImplement(this);
    }

    return operandTypeChecker.checkOperandTypes(
        callBinding,
        throwOnFailure);
  }

  //用于校验参数数量是否符合标准
  protected void checkOperandCount(
      SqlValidator validator,
      SqlOperandTypeChecker argType,
      SqlCall call) {
    SqlOperandCountRange od = call.getOperator().getOperandCountRange();
    if (od.isValidCount(call.operandCount())) { //是否参数数量是符合的
      return;
    }
    if (od.getMin() == od.getMax()) {
      throw validator.newValidationError(call,
          RESOURCE.invalidArgCount(call.getOperator().getName(), od.getMin()));
    } else {
      throw validator.newValidationError(call, RESOURCE.wrongNumOfArguments());
    }
  }

  /**
   * Returns whether the given operands are valid. If not valid and
   * {@code fail}, throws an assertion error.
   *
   * <p>Similar to {@link #checkOperandCount}, but some operators may have
   * different valid operands in {@link SqlNode} and {@code RexNode} formats
   * (some examples are CAST and AND), and this method throws internal errors,
   * not user errors.</p>
   */
  public boolean validRexOperands(int count, boolean fail) {
    return true;
  }

  /**
   * Returns a template describing how the operator signature is to be built.
   * E.g for the binary + operator the template looks like "{1} {0} {2}" {0}
   * is the operator, subsequent numbers are operands.
   *
   * @param operandsCount is used with functions that can take a variable
   *                      number of operands
   * @return signature template, or null to indicate that a default template
   * will suffice
   */
  public String getSignatureTemplate(final int operandsCount) {
    return null;
  }

  /**
   * Returns a string describing the expected operand types of a call, e.g.
   * "SUBSTR(VARCHAR, INTEGER, INTEGER)".
   */
  public final String getAllowedSignatures() {
    return getAllowedSignatures(name);
  }

  /**
   * Returns a string describing the expected operand types of a call, e.g.
   * "SUBSTRING(VARCHAR, INTEGER, INTEGER)" where the name (SUBSTRING in this
   * example) can be replaced by a specified name.
   */
  public String getAllowedSignatures(String opNameToUse) {
    assert operandTypeChecker != null
        : "If you see this, assign operandTypeChecker a value "
        + "or override this function";
    return operandTypeChecker.getAllowedSignatures(this, opNameToUse)
        .trim();
  }

  public SqlOperandTypeInference getOperandTypeInference() {
    return operandTypeInference;
  }

  /**
   * Returns whether this operator is an aggregate function. By default,
   * subclass type is used (an instance of SqlAggFunction is assumed to be an
   * aggregator; anything else is not).
   *
   * <p>Per SQL:2011, there are <dfn>aggregate functions</dfn> and
   * <dfn>window functions</dfn>.
   * Every aggregate function (e.g. SUM) is also a window function.
   * There are window functions that are not aggregate functions, e.g. RANK,
   * NTILE, LEAD, FIRST_VALUE.</p>
   *
   * <p>Collectively, aggregate and window functions are called <dfn>analytic
   * functions</dfn>. Despite its name, this method returns true for every
   * analytic function.</p>
   *
   * @see #requiresOrder()
   *
   * @return whether this operator is an analytic function (aggregate function
   * or window function)
   */
  public boolean isAggregator() {
    return false;
  }

  /**
   * Returns whether this is a window function that requires ordering.
   *
   * <p>Per SQL:2011, 2, 6.10: "If &lt;ntile function&gt;, &lt;lead or lag
   * function&gt;, RANK or DENSE_RANK is specified, then the window ordering
   * clause shall be present."</p>
   *
   * @see #isAggregator()
   */
  public boolean requiresOrder() {
    return false;
  }

  /**
   * Returns whether this is a window function that allows framing (i.e. a
   * ROWS or RANGE clause in the window specification).
   */
  public boolean allowsFraming() {
    return true;
  }

  /**
   * Accepts a {@link SqlVisitor}, visiting each operand of a call. Returns
   * null.
   *
   * @param visitor Visitor
   * @param call    Call to visit
   */
  public <R> R acceptCall(SqlVisitor<R> visitor, SqlCall call) {
    for (SqlNode operand : call.getOperandList()) {
      if (operand == null) {
        continue;
      }
      operand.accept(visitor);
    }
    return null;
  }

  /**
   * Accepts a {@link SqlVisitor}, directing an
   * {@link org.apache.calcite.sql.util.SqlBasicVisitor.ArgHandler}
   * to visit an operand of a call.
   *
   * <p>The argument handler allows fine control about how the operands are
   * visited, and how the results are combined.
   *
   * @param visitor         Visitor
   * @param call            Call to visit
   * @param onlyExpressions If true, ignores operands which are not
   *                        expressions. For example, in the call to the
   *                        <code>AS</code> operator
   * @param argHandler      Called for each operand
   */
  public <R> void acceptCall(
      SqlVisitor<R> visitor,
      SqlCall call,
      boolean onlyExpressions,
      SqlBasicVisitor.ArgHandler<R> argHandler) {
    List<SqlNode> operands = call.getOperandList();//参数
    for (int i = 0; i < operands.size(); i++) {
      argHandler.visitChild(visitor, call, i, operands.get(i));
    }
  }

  /**
   * @return the return type inference strategy for this operator, or null if
   * return type inference is implemented by a subclass override
   */
  public SqlReturnTypeInference getReturnTypeInference() {
    return returnTypeInference;
  }

  /**
   * Returns whether this operator is monotonic.
   *
   * <p>Default implementation returns {@link SqlMonotonicity#NOT_MONOTONIC}.
   *
   * @param call  Call to this operator
   * @param scope Scope in which the call occurs
   * 返回该函数是否具有单调性
   */
  public SqlMonotonicity getMonotonicity(
      SqlCall call,
      SqlValidatorScope scope) {
    return SqlMonotonicity.NOT_MONOTONIC;
  }

  /**
   * @return true iff a call to this operator is guaranteed to always return
   * the same result given the same operands; true is assumed by default
   */
  public boolean isDeterministic() {
    return true;
  }

  /**
   * @return true iff it is unsafe to cache query plans referencing this
   * operator; false is assumed by default
   */
  public boolean isDynamicFunction() {
    return false;
  }

  /**
   * Method to check if call requires expansion when it has decimal operands.
   * The default implementation is to return true.
   */
  public boolean requiresDecimalExpansion() {
    return true;
  }

  /**
   * Returns whether the <code>ordinal</code>th argument to this operator must
   * be scalar (as opposed to a query).
   *
   * <p>If true (the default), the validator will attempt to convert the
   * argument into a scalar subquery, which must have one column and return at
   * most one row.
   *
   * <p>Operators such as <code>SELECT</code> and <code>EXISTS</code> override
   * this method.
   */
  public boolean argumentMustBeScalar(int ordinal) {
    return true;
  }
}

// End SqlOperator.java
