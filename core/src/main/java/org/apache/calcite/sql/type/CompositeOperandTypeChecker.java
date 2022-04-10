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
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import java.util.AbstractList;
import java.util.List;

/**
 * This class allows multiple existing {@link SqlOperandTypeChecker} rules to be
 * combined into one rule. For example, allowing an operand to be either string
 * or numeric could be done by:
 * 多个存在的规则,如何组合成一个规则.
 *
 * <blockquote>
 * <pre><code>
 *
 * CompositeOperandsTypeChecking newCompositeRule =
 *  new CompositeOperandsTypeChecking(
 *    Composition.OR,
 *    new SqlOperandTypeChecker[]{stringRule, numericRule});
 *
 * </code></pre>
 * </blockquote>
 *
 * Similary a rule that would only allow a numeric literal can be done by:
 *
 * <blockquote>
 * <pre><code>
 *
 * CompositeOperandsTypeChecking newCompositeRule =
 *  new CompositeOperandsTypeChecking(
 *    Composition.AND,
 *    new SqlOperandTypeChecker[]{numericRule, literalRule});
 *
 * </code></pre>
 * </blockquote>
 *
 * <p>Finally, creating a signature expecting a string for the first operand and
 * a numeric for the second operand can be done by:
 *
 * <blockquote>
 * <pre><code>
 *
 * CompositeOperandsTypeChecking newCompositeRule =
 *  new CompositeOperandsTypeChecking(
 *    Composition.SEQUENCE,
 *    new SqlOperandTypeChecker[]{stringRule, numericRule});
 *
 * </code></pre>
 * </blockquote>
 *
 * <p>For SEQUENCE composition, the rules must be instances of
 * SqlSingleOperandTypeChecker, and signature generation is not supported. For
 * AND composition, only the first rule is used for signature generation.
 * 多个规则作用于参数，比如2个规则,并且是and关系,则要求参数都要通过这2个规则
 */
public class CompositeOperandTypeChecker
    implements SqlSingleOperandTypeChecker {
  //~ Enums ------------------------------------------------------------------

  /** How operands are composed.组合方式 并且关系、或者关系、序列化关系 */
  public enum Composition {
    AND, OR, SEQUENCE
  }

  //~ Instance fields --------------------------------------------------------
  //组合集合、什么关系
  private final ImmutableList<SqlSingleOperandTypeChecker> allowedRules;
  private final Composition composition;

  //~ Constructors -----------------------------------------------------------

  /**
   * Package private. Use {@link OperandTypes#and},
   * {@link OperandTypes#or}.
   */
  CompositeOperandTypeChecker(
      Composition composition,
      ImmutableList<SqlSingleOperandTypeChecker> allowedRules) {
    assert null != allowedRules;
    assert allowedRules.size() > 1;
    this.allowedRules = allowedRules;
    this.composition = composition;
  }

  //~ Methods ----------------------------------------------------------------

  public ImmutableList<SqlSingleOperandTypeChecker> getRules() {
    return allowedRules;
  }

  public String getAllowedSignatures(SqlOperator op, String opName) {
    if (composition == Composition.SEQUENCE) {
      throw Util.needToImplement("must override getAllowedSignatures");
    }
    StringBuilder ret = new StringBuilder();
    for (Ord<SqlSingleOperandTypeChecker> ord : Ord.zip(allowedRules)) {
      if (ord.i > 0) {
        ret.append(SqlOperator.NL);
      }
      ret.append(ord.e.getAllowedSignatures(op, opName));
      if (composition == Composition.AND) {
        break;
      }
    }
    return ret.toString();
  }

  public SqlOperandCountRange getOperandCountRange() {
    switch (composition) {
    case SEQUENCE://固定参数大小
      return SqlOperandCountRanges.of(allowedRules.size());
    case AND:
    case OR:
    default:
      final List<SqlOperandCountRange> ranges =
          new AbstractList<SqlOperandCountRange>() {
            public SqlOperandCountRange get(int index) {
              return allowedRules.get(index).getOperandCountRange();
            }

            public int size() {
              return allowedRules.size();
            }
          };
      final int min = minMin(ranges);
      final int max = maxMax(ranges);
      SqlOperandCountRange composite =
          new SqlOperandCountRange() {
            public boolean isValidCount(int count) {
              switch (composition) {
              case AND:
                for (SqlOperandCountRange range : ranges) {
                  if (!range.isValidCount(count)) {
                    return false;
                  }
                }
                return true;
              case OR:
              default:
                for (SqlOperandCountRange range : ranges) {
                  if (range.isValidCount(count)) {
                    return true;
                  }
                }
                return false;
              }
            }

            public int getMin() {
              return min;
            }

            public int getMax() {
              return max;
            }
          };
      if (max >= 0) {
        for (int i = min; i <= max; i++) {
          if (!composite.isValidCount(i)) {
            // Composite is not a simple range. Can't simplify,
            // so return the composite.
            return composite;
          }
        }
      }
      return min == max
          ? SqlOperandCountRanges.of(min)
          : SqlOperandCountRanges.between(min, max);
    }
  }

  //所有max中最小的max数量 即最多允许5个，最多允许10个，结果最多允许就是5个。
  //貌似没有考虑到-1的情况,有bug
  private int minMin(List<SqlOperandCountRange> ranges) {
    int min = Integer.MAX_VALUE;
    for (SqlOperandCountRange range : ranges) {
      min = Math.min(min, range.getMax());
    }
    return min;
  }

  //获取最大的max,即最多允许多少个参数
  private int maxMax(List<SqlOperandCountRange> ranges) {
    int max = Integer.MIN_VALUE;
    for (SqlOperandCountRange range : ranges) {
      if (range.getMax() < 0) {//无边界
        if (composition == Composition.OR) {//or 所以最终结果就是无边界,即-1
          return -1;
        }
      } else {
        max = Math.max(max, range.getMax());
      }
    }
    return max;
  }

  public boolean checkSingleOperandType(
      SqlCallBinding callBinding,
      SqlNode node,
      int iFormalOperand,
      boolean throwOnFailure) {//true表示如果失败的话,需要抛异常出去
    assert allowedRules.size() >= 1;

    if (composition == Composition.SEQUENCE) {
      return allowedRules.get(iFormalOperand).checkSingleOperandType(
          callBinding, node, 0, throwOnFailure);
    }

    int typeErrorCount = 0;

    boolean throwOnAndFailure =
        (composition == Composition.AND)
            && throwOnFailure;

    for (SqlSingleOperandTypeChecker rule : allowedRules) {//每一个规则都进行校验一次
      if (!rule.checkSingleOperandType(
          callBinding,
          node,
          iFormalOperand,
          throwOnAndFailure)) {
        typeErrorCount++;//记录校验失败次数
      }
    }

    boolean ret;
    switch (composition) {
    case AND:
      ret = typeErrorCount == 0;//必须全对
      break;
    case OR:
      ret = typeErrorCount < allowedRules.size();//有一个对即可
      break;
    default:
      // should never come here
      throw Util.unexpected(composition);
    }

    if (!ret && throwOnFailure) {//确实失败了,并且如果失败需要抛异常
      // In the case of a composite OR, we want to throw an error
      // describing in more detail what the problem was, hence doing the
      // loop again.
      for (SqlSingleOperandTypeChecker rule : allowedRules) {
        rule.checkSingleOperandType(
            callBinding,
            node,
            iFormalOperand,
            true);
      }

      // If no exception thrown, just throw a generic validation signature
      // error.
      throw callBinding.newValidationSignatureError();
    }

    return ret;
  }

  public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    int typeErrorCount = 0;

  label:
    for (Ord<SqlSingleOperandTypeChecker> ord : Ord.zip(allowedRules)) {
      SqlSingleOperandTypeChecker rule = ord.e;

      switch (composition) {
      case SEQUENCE:
        if (ord.i >= callBinding.getOperandCount()) {//全部校验完成
          break label;
        }
        if (!rule.checkSingleOperandType(
            callBinding,
            callBinding.getCall().operand(ord.i),
            0,
            false)) {
          typeErrorCount++;//校验失败
        }
        break;
      default:
        if (!rule.checkOperandTypes(callBinding, false)) {//校验
          typeErrorCount++;//校验失败
          if (composition == Composition.AND) {
            // Avoid trying other rules in AND if the first one fails.
            break label;//直接退出
          }
        } else if (composition == Composition.OR) {//有任意一个失败,则都是失败,直接退出
          break label; // true OR any == true, just break
        }
        break;
      }
    }

    boolean failed;//是否失败
    switch (composition) {
    case AND:
    case SEQUENCE:
      failed = typeErrorCount > 0;//有任意一个错误的,则都是失败
      break;
    case OR:
      failed = typeErrorCount == allowedRules.size();//全错误,则失败
      break;
    default:
      throw new AssertionError();
    }

    if (failed) {//失败了
      if (throwOnFailure) {//失败还需要抛异常
        // In the case of a composite OR, we want to throw an error
        // describing in more detail what the problem was, hence doing
        // the loop again.
        if (composition == Composition.OR) {
          for (SqlOperandTypeChecker allowedRule : allowedRules) {
            allowedRule.checkOperandTypes(callBinding, true);
          }
        }

        // If no exception thrown, just throw a generic validation
        // signature error.
        throw callBinding.newValidationSignatureError();
      }
      return false;
    }
    return true;
  }
}

// End CompositeOperandTypeChecker.java
