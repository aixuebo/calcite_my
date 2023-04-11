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
package org.apache.calcite.sql.validate;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.util.Stacks;

import com.google.common.collect.Lists;

import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Visitor which throws an exception if any component of the expression is not a
 * group expression.
 *
 * 用于校验聚合函数的合法性
 */
class AggChecker extends SqlBasicVisitor<Void> {
  //~ Instance fields --------------------------------------------------------

  private final List<SqlValidatorScope> scopes = Lists.newArrayList();
  private final List<SqlNode> groupExprs;//group by的表达式集合
  private boolean distinct;
  private SqlValidatorImpl validator;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an AggChecker.
   *
   * @param validator  Validator
   * @param scope      Scope
   * @param groupExprs Expressions in GROUP BY (or SELECT DISTINCT) clause,
   *                   that are therefore available
   * @param distinct   Whether aggregation checking is because of a SELECT
   *                   DISTINCT clause
   */
  AggChecker(
      SqlValidatorImpl validator,
      AggregatingScope scope,
      List<SqlNode> groupExprs,
      boolean distinct) {
    this.validator = validator;
    this.groupExprs = groupExprs;
    this.distinct = distinct;
    Stacks.push(this.scopes, scope);
  }

  //~ Methods ----------------------------------------------------------------

  //确保参数是group by表达式中的某一个字段
  boolean isGroupExpr(SqlNode expr) {
    for (SqlNode groupExpr : groupExprs) {
      if (groupExpr.equalsDeep(expr, false)) { //字段相同,则返回true
        return true;
      }
    }
    return false;
  }

  /**
   * 1.id是group by中用到的字段
   * 2.id是一个无参数的内置函数
   */
  public Void visit(SqlIdentifier id) {

    //确保参数是group by中的某一个,则返回null,不需要再进一步访问
    if (isGroupExpr(id)) {
      return null;
    }

    // If it '*' or 'foo.*'? 不应该出现*
    if (id.isStar()) {
      assert false : "star should have been expanded";
    }

    // Is it a call to a parentheses-free function?
    //id是无参数函数,则调用该函数
    SqlCall call =
        SqlUtil.makeCall(
            validator.getOperatorTable(),
            id);
    if (call != null) {
      return call.accept(this);
    }

    // Didn't find the identifier in the group-by list as is, now find
    // it fully-qualified.
    // TODO: It would be better if we always compared fully-qualified
    // to fully-qualified.
    //打印错误信息，即此时id既不是group by的字段，也不是无参数函数
    final SqlQualified fqId = Stacks.peek(scopes).fullyQualify(id);
    if (isGroupExpr(fqId.identifier)) {
      return null;
    }
    SqlNode originalExpr = validator.getOriginal(id);
    final String exprString = originalExpr.toString();
    throw validator.newValidationError(originalExpr,
        distinct
            ? RESOURCE.notSelectDistinctExpr(exprString)
            : RESOURCE.notGroupExpr(exprString));
  }

  public Void visit(SqlCall call) {
    if (call.getOperator().isAggregator()) {
      if (distinct) { //聚合函数,不能用于distinct关键词一起用
        // Cannot use agg fun in ORDER BY clause if have SELECT DISTINCT.
        SqlNode originalExpr = validator.getOriginal(call);
        final String exprString = originalExpr.toString();
        throw validator.newValidationError(call,
            RESOURCE.notSelectDistinctExpr(exprString));
      }

      // For example, 'sum(sal)' in 'SELECT sum(sal) FROM emp GROUP
      // BY deptno'
      return null;
    }
    if (isGroupExpr(call)) {
      // This call matches an expression in the GROUP BY clause.
      return null;
    }
    if (call.isA(SqlKind.QUERY)) {
      // Allow queries for now, even though they may contain
      // references to forbidden columns.
      return null;
    }

    // Switch to new scope.
    SqlValidatorScope oldScope = Stacks.peek(scopes);
    SqlValidatorScope newScope = oldScope.getOperandScope(call);
    Stacks.push(scopes, newScope);

    // Visit the operands (only expressions).
    call.getOperator()
        .acceptCall(this, call, true, ArgHandlerImpl.<Void>instance());

    // Restore scope.
    Stacks.pop(scopes, newScope);
    return null;
  }
}

// End AggChecker.java
