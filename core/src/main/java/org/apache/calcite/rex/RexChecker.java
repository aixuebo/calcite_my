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
package org.apache.calcite.rex;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.List;

/**
 * Visitor which checks the validity of a {@link RexNode} expression.
 * 校验表达式的正确性
 *
 * <p>There are two modes of operation:
 *
 * <ul>
 * <li>Use<code>fail=true</code> to throw an {@link AssertionError} as soon as
 * an invalid node is detected:
 * fail=true,表示当无效的rexnode被检测到,则抛异常 AssertionError
 *
 * <blockquote><code>RexNode node;<br>
 * RelDataType rowType;<br>
 * assert new RexChecker(rowType, true).isValid(node);</code></blockquote>
 *
 * This mode requires that assertions are enabled.</li>
 *
 * <li>Use <code>fail=false</code> to test for validity without throwing an
 * error.
 *
 * <blockquote><code>RexNode node;<br>
 * RelDataType rowType;<br>
 * RexChecker checker = new RexChecker(rowType, false);<br>
 * node.accept(checker);<br>
 * if (!checker.valid) {<br>
 * &nbsp;&nbsp;&nbsp;...<br>
 * }</code></blockquote>
 * </li>
 * </ul>
 *
 * @see RexNode
 */
public class RexChecker extends RexVisitorImpl<Boolean> {
  //~ Instance fields --------------------------------------------------------

  protected final boolean fail;//true 表示如何失效被发现,则抛异常。false不需要抛异常
  protected final List<RelDataType> inputTypeList;//表的字段集合
  protected int failCount;//失败次数

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a RexChecker with a given input row type.
   *
   * <p>If <code>fail</code> is true, the checker will throw an
   * {@link AssertionError} if an invalid node is found and assertions are
   * enabled.
   *
   * <p>Otherwise, each method returns whether its part of the tree is valid.
   *
   * @param inputRowType Input row type
   * @param fail Whether to throw an {@link AssertionError} if an
   *                     invalid node is detected
   */
  public RexChecker(final RelDataType inputRowType, boolean fail) {
    this(RelOptUtil.getFieldTypeList(inputRowType), fail);
  }

  /**
   * Creates a RexChecker with a given set of input fields.
   *
   * <p>If <code>fail</code> is true, the checker will throw an
   * {@link AssertionError} if an invalid node is found and assertions are
   * enabled.
   *
   * <p>Otherwise, each method returns whether its part of the tree is valid.
   *
   * @param inputTypeList Input row type
   * @param fail Whether to throw an {@link AssertionError} if an
   *                      invalid node is detected
   */
  public RexChecker(List<RelDataType> inputTypeList, boolean fail) {
    super(true);
    this.inputTypeList = inputTypeList;
    this.fail = fail;
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Returns the number of failures encountered.
   *
   * @return Number of failures
   */
  public int getFailureCount() {
    return failCount;
  }

  /**
   * 校验某一个字段变量
   * 1.校验字段序号是否在范围内
   * 2.判断类型要相同
   * @param ref
   * @return
   */
  public Boolean visitInputRef(RexInputRef ref) {
    final int index = ref.getIndex();//第几个字段

    //校验字段序号是否在范围内
    if ((index < 0) || (index >= inputTypeList.size())) {
      assert !fail
          : "RexInputRef index " + index
          + " out of range 0.." + (inputTypeList.size() - 1);//如果是true,则抛异常
      ++failCount;
      return false;
    }

    //判断类型要相同
    if (!ref.getType().isStruct()
        && !RelOptUtil.eq("ref", ref.getType(), "input",
            inputTypeList.get(index), fail)) {
      assert !fail;
      ++failCount;
      return false;
    }
    return true;
  }

  public Boolean visitLocalRef(RexLocalRef ref) {
    assert !fail : "RexLocalRef illegal outside program";
    ++failCount;
    return false;
  }

  //针对参数校验
  public Boolean visitCall(RexCall call) {
    for (RexNode operand : call.getOperands()) {//针对每一个参数,校验每一个参数是否都有效
      Boolean valid = operand.accept(this);
      if (valid != null && !valid) {
        return false;
      }
    }
    return true;
  }

  public Boolean visitFieldAccess(RexFieldAccess fieldAccess) {
    super.visitFieldAccess(fieldAccess);
    final RelDataType refType = fieldAccess.getReferenceExpr().getType();
    assert refType.isStruct();
    final RelDataTypeField field = fieldAccess.getField();
    final int index = field.getIndex();
    if ((index < 0) || (index > refType.getFieldList().size())) {
      assert !fail;
      ++failCount;
      return false;
    }
    final RelDataTypeField typeField = refType.getFieldList().get(index);
    if (!RelOptUtil.eq(
        "type1",
        typeField.getType(),
        "type2",
        fieldAccess.getType(),
        fail)) {
      assert !fail;
      ++failCount;
      return false;
    }
    return true;
  }

  /**
   * Returns whether an expression is valid.
   * 单独校验一个表达式是否是有效的 -- 传入this即该check对象
   */
  public final boolean isValid(RexNode expr) {
    return expr.accept(this);
  }
}

// End RexChecker.java
