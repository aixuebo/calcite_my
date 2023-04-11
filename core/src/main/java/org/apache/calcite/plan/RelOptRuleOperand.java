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
package org.apache.calcite.plan;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Util;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Operand that determines whether a {@link RelOptRule}
 * can be applied to a particular expression.
 * 操作规则
 *
 * <p>For example, the rule to pull a filter up from the left side of a join
 * takes operands: <code>Join(Filter, Any)</code>.</p>
 *
 * <p>Note that <code>children</code> means different things if it is empty or
 * it is <code>null</code>: <code>Join(Filter <b>()</b>, Any)</code> means
 * that, to match the rule, <code>Filter</code> must have no operands.</p>
 */
public class RelOptRuleOperand {
  //~ Instance fields --------------------------------------------------------

  private RelOptRuleOperand parent;//设置父类
  private RelOptRule rule;//如果匹配成功,则返回规则对象


  // REVIEW jvs 29-Aug-2004: some of these are Volcano-specific and should be
  // factored out
  public int[] solveOrder;
  public int ordinalInParent;
  public int ordinalInRule;


  //构造函数需要的内容,匹配class、匹配trait、校验规则
  private final Class<? extends RelNode> clazz;
  private final RelTrait trait;
  private final Predicate<RelNode> predicate;
  /**
   * Whether child operands can be matched in any order.
   * 子操作匹配策略
   */
  public final RelOptRuleOperandChildPolicy childPolicy;
  private final ImmutableList<RelOptRuleOperand> children;//子操作集合

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an operand.
   * 创建一个匹配操作
   *
   * <p>The {@code childOperands} argument is often populated by calling one
   * of the following methods:
   * {@link RelOptRule#some},
   * {@link RelOptRule#none()},
   * {@link RelOptRule#any},
   * {@link RelOptRule#unordered},
   * See {@link org.apache.calcite.plan.RelOptRuleOperandChildren} for more
   * details.</p>
   *
   * @param clazz    Class of relational expression to match (must not be null)
   * @param trait    Trait to match, or null to match any trait
   * @param predicate Predicate to apply to relational expression
   * @param children Child operands
   */
  protected <R extends RelNode> RelOptRuleOperand(
      Class<R> clazz,
      RelTrait trait,
      Predicate<? super R> predicate,
      RelOptRuleOperandChildren children) {
    assert clazz != null;
    //校验
    switch (children.policy) {
    case ANY://随意匹配
      break;
    case LEAF://必须叶子操作
      assert children.operands.size() == 0;
      break;
    case UNORDERED://无序--操作一定是1?不知道为什么
      assert children.operands.size() == 1;
      break;
    default:
      assert children.operands.size() > 0;
    }
    this.childPolicy = children.policy;
    this.clazz = Preconditions.checkNotNull(clazz);
    this.trait = trait;
    //noinspection unchecked
    this.predicate = Preconditions.checkNotNull((Predicate) predicate);
    this.children = children.operands;
    for (RelOptRuleOperand child : this.children) {
      assert child.parent == null : "cannot re-use operands";
      child.parent = this;
    }
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Returns the parent operand.
   *
   * @return parent operand
   */
  public RelOptRuleOperand getParent() {
    return parent;
  }

  /**
   * Sets the parent operand.
   *
   * @param parent Parent operand
   */
  public void setParent(RelOptRuleOperand parent) {
    this.parent = parent;
  }

  /**
   * Returns the rule this operand belongs to.
   *
   * @return containing rule
   */
  public RelOptRule getRule() {
    return rule;
  }

  /**
   * Sets the rule this operand belongs to
   *
   * @param rule containing rule
   */
  public void setRule(RelOptRule rule) {
    this.rule = rule;
  }

  public int hashCode() {
    int h = clazz.hashCode();
    h = Util.hash(h, trait);
    h = Util.hash(h, children);
    return h;
  }

  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof RelOptRuleOperand)) {
      return false;
    }
    RelOptRuleOperand that = (RelOptRuleOperand) obj;

    return (this.clazz == that.clazz)
        && com.google.common.base.Objects.equal(this.trait, that.trait)
        && this.children.equals(that.children);
  }

  /**
   * @return relational expression class matched by this operand
   */
  public Class<? extends RelNode> getMatchedClass() {
    return clazz;
  }

  /**
   * Returns the child operands.
   *
   * @return child operands
   */
  public List<RelOptRuleOperand> getChildOperands() {
    return children;
  }

  /**
   * Returns whether a relational expression matches this operand. It must be
   * of the right class and trait.
   * 去校验是否匹配
   */
  public boolean matches(RelNode rel) {
    if (!clazz.isInstance(rel)) { //匹配class
      return false;
    }
    if ((trait != null) && !rel.getTraitSet().contains(trait)) { //匹配trait,即rel必须包含待规定的trait
      return false;
    }
    return predicate.apply(rel); //校验规则必须通过
  }
}

// End RelOptRuleOperand.java
