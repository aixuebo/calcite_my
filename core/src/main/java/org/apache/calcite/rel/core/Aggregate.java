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
package org.apache.calcite.rel.core;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.IntList;
import org.apache.calcite.util.Util;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.math.IntMath;

import java.util.List;

/**
 * Relational operator that eliminates duplicates and computes totals.
 *关系运算符:消除重复项(group by控制)并计算总数的(聚合函数控制)
 *
 * <p>It corresponds to the {@code GROUP BY} operator in a SQL query
 * statement, together with the aggregate functions in the {@code SELECT}
 * clause.
 * 关联group by语法,以及聚合函数
 * <p>Rules:
 *
 * <ul>
 * <li>{@link org.apache.calcite.rel.rules.AggregateProjectPullUpConstantsRule}
 * <li>{@link org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule}
 * <li>{@link org.apache.calcite.rel.rules.AggregateReduceFunctionsRule}.
 * </ul>
 */
public abstract class Aggregate extends SingleRel {
  /**
   * @see org.apache.calcite.util.Bug#CALCITE_461_FIXED
   * 是否是简单的group by操作
   */
  public static final Predicate<Aggregate> IS_SIMPLE =
      new Predicate<Aggregate>() {
        public boolean apply(Aggregate input) {
          return input.getGroupType() == Group.SIMPLE;
        }
      };

  //~ Instance fields --------------------------------------------------------

  public final boolean indicator;//true表示是一个复杂的窗口函数,用于表示某一些字段是否参与到cube计算了
  protected final List<AggregateCall> aggCalls;//聚合函数集合
  protected final ImmutableBitSet groupSet;//常规group by语法
  public final ImmutableList<ImmutableBitSet> groupSets;//窗口函数groupSet语法,如果该值null,说明只有group by常规分组操作

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an Aggregate.
   *
   * <p>All members of {@code groupSets} must be sub-sets of {@code groupSet}.
   * For a simple {@code GROUP BY}, {@code groupSets} is a singleton list
   * containing {@code groupSet}.
   *
   * <p>If {@code GROUP BY} is not specified,or equivalently if {@code GROUP BY ()} is specified,
   * {@code groupSet} will be the empty set,
   * and {@code groupSets} will have one element, that empty set.
   *
   * <p>If {@code CUBE}, {@code ROLLUP} or {@code GROUPING SETS} are
   * specified, {@code groupSets} will have additional elements,
   * but they must each be a subset of {@code groupSet},
   * and they must be sorted by inclusion:
   * {@code (0, 1, 2), (1), (0, 2), (0), ()}.
   *
   * @param cluster  Cluster
   * @param traits   Traits
   * @param child    Child 数据源
   * @param indicator Whether row type should include indicator fields to
   *                 indicate which grouping set is active; must be true if
   *                 aggregate is not simple ,true表示是一个复杂的窗口函数
   * @param groupSet Bit set of grouping fields,常规group by语法
   * @param groupSets List of all grouping sets; null for just {@code groupSet} 窗口函数groupSet语法,如果该值null,说明只有group by常规分组操作
   * @param aggCalls Collection of calls to aggregate functions 聚合函数集合
   */
  protected Aggregate(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      boolean indicator,
      ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    super(cluster, traits, child);
    this.indicator = indicator;
    this.aggCalls = ImmutableList.copyOf(aggCalls);
    this.groupSet = Preconditions.checkNotNull(groupSet);
    if (groupSets == null) {
      this.groupSets = ImmutableList.of(groupSet);
    } else {
      this.groupSets = ImmutableList.copyOf(groupSets);
      assert ImmutableBitSet.ORDERING.isStrictlyOrdered(groupSets) : groupSets;
      for (ImmutableBitSet set : groupSets) {
        assert groupSet.contains(set);
      }
    }
    assert groupSet.length() <= child.getRowType().getFieldCount();//group by的字段一定小于select字段数量
    for (AggregateCall aggCall : aggCalls) {
      assert typeMatchesInferred(aggCall, true);
    }
  }

  /**
   * Creates an Aggregate by parsing serialized output.
   */
  protected Aggregate(RelInput input) {
    this(input.getCluster(), input.getTraitSet(), input.getInput(),
        input.getBoolean("indicator", false),
        input.getBitSet("group"), input.getBitSetList("groups"),
        input.getAggregateCalls("aggs"));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public final RelNode copy(RelTraitSet traitSet,
      List<RelNode> inputs) {
    return copy(traitSet, sole(inputs), indicator, groupSet, groupSets,
        aggCalls);
  }

  /** Creates a copy of this aggregate.
   *
   * @param traitSet Traits
   * @param input Input
   * @param indicator Whether row type should include indicator fields to
   *                 indicate which grouping set is active; must be true if
   *                 aggregate is not simple
   * @param groupSet Bit set of grouping fields
   * @param groupSets List of all grouping sets; null for just {@code groupSet}
   * @param aggCalls Collection of calls to aggregate functions
   * @return New {@code Aggregate} if any parameter differs from the value of
   *   this {@code Aggregate}, or just {@code this} if all the parameters are
   *   the same
   *
   * @see #copy(org.apache.calcite.plan.RelTraitSet, java.util.List)
   */
  public abstract Aggregate copy(RelTraitSet traitSet, RelNode input,
      boolean indicator, ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls);

  // implement RelNode
  public boolean isDistinct() {
    // we never return duplicate rows
    return true;
  }

  /**
   * Returns a list of calls to aggregate functions.
   *
   * @return list of calls to aggregate functions
   */
  public List<AggregateCall> getAggCallList() {
    return aggCalls;
  }

  /**
   * Returns the number of grouping fields.
   * These grouping fields are the leading fields in both the input and output
   * records.
   *
   * <p>NOTE: The {@link #getGroupSet()} data structure allows for the
   * grouping fields to not be on the leading edge. New code should, if
   * possible, assume that grouping fields are in arbitrary positions in the
   * input relational expression.
   *
   * @return number of grouping fields
   * 有多少个字段参与group by,即有多少个bit
   */
  public int getGroupCount() {
    return groupSet.cardinality();
  }

  /**
   * Returns the number of indicator fields.
   *
   * <p>This is the same as {@link #getGroupCount()} if {@link #indicator} is
   * true, zero if {@code indicator} is false.
   *
   * <p>The offset of the first aggregate call in the output record is always
   * <i>groupCount + indicatorCount</i>.
   *
   * @return number of indicator fields
   */
  public int getIndicatorCount() {
    return indicator ? getGroupCount() : 0;
  }

  /**
   * Returns a bit set of the grouping fields.
   *
   * @return bit set of ordinals of grouping fields
   */
  public ImmutableBitSet getGroupSet() {
    return groupSet;
  }

  /**
   * Returns the list of grouping sets computed by this Aggregate.
   *
   * @return List of all grouping sets; null for just {@code groupSet}
   */
  public ImmutableList<ImmutableBitSet> getGroupSets() {
    return groupSets;
  }

  public RelWriter explainTerms(RelWriter pw) {
    // We skip the "groups" element if it is a singleton of "group".
    super.explainTerms(pw)
        .item("group", groupSet)
        .itemIf("groups", groupSets, getGroupType() != Group.SIMPLE)
        .itemIf("indicator", indicator, indicator)
        .itemIf("aggs", aggCalls, pw.nest());
    if (!pw.nest()) {
      for (Ord<AggregateCall> ord : Ord.zip(aggCalls)) {
        pw.item(Util.first(ord.e.name, "agg#" + ord.i), ord.e);
      }
    }
    return pw;
  }

  @Override public double getRows() {
    // Assume that each sort column has 50% of the value count.
    // Therefore one sort column has .5 * rowCount,
    // 2 sort columns give .75 * rowCount.
    // Zero sort columns yields 1 row (or 0 if the input is empty).
    final int groupCount = groupSet.cardinality();
    if (groupCount == 0) {
      return 1;
    } else {
      double rowCount = super.getRows();
      rowCount *= 1.0 - Math.pow(.5, groupCount);
      return rowCount;
    }
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner) {
    // REVIEW jvs 24-Aug-2008:  This is bogus, but no more bogus
    // than what's currently in Join.
    double rowCount = RelMetadataQuery.getRowCount(this);
    return planner.getCostFactory().makeCost(rowCount, 0, 0);
  }

  protected RelDataType deriveRowType() {
    return deriveRowType(getCluster().getTypeFactory(), getInput().getRowType(),
        indicator, groupSet, groupSets, aggCalls);
  }

  /**
   * Computes the row type of an {@code Aggregate} before it exists.
   *
   * @param typeFactory Type factory
   * @param inputRowType Input row type 数据源的字段类型
   * @param indicator Whether row type should include indicator fields to
   *                 indicate which grouping set is active; must be true if
   *                 aggregate is not simple
   * @param groupSet Bit set of grouping fields
   * @param groupSets List of all grouping sets; null for just {@code groupSet}
   * @param aggCalls Collection of calls to aggregate functions 聚合函数
   * @return Row type of the aggregate
   * 返回group by + select中聚合函数需要的字段 的集合
   */
  public static RelDataType deriveRowType(RelDataTypeFactory typeFactory,
      final RelDataType inputRowType, boolean indicator,
      ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets,
      final List<AggregateCall> aggCalls) {
    final IntList groupList = groupSet.toList();
    assert groupList.size() == groupSet.cardinality();
    final RelDataTypeFactory.FieldInfoBuilder builder = typeFactory.builder();
    final List<RelDataTypeField> fieldList = inputRowType.getFieldList();//原始输入的字段
    for (int groupKey : groupList) {//循环需要group by的字段
      builder.add(fieldList.get(groupKey));//生成需要的schema类型
    }
    if (indicator) {//为每一个group by的字段创建一个boolean类型的字段,用于确定是否该字段参与到cube的分组计算中
      for (int groupKey : groupList) {
        final RelDataType booleanType =
            typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.BOOLEAN), false);
        builder.add("i$" + fieldList.get(groupKey).getName(), booleanType);
      }
    }
    for (Ord<AggregateCall> aggCall : Ord.zip(aggCalls)) {//为每一个聚合函数字段创建字段
      String name;
      if (aggCall.e.name != null) {
        name = aggCall.e.name;
      } else {
        name = "$f" + (groupList.size() + aggCall.i);
      }
      builder.add(name, aggCall.e.type);
    }
    return builder.build();
  }

  /**
   * Returns whether the inferred type of an {@link AggregateCall} matches the
   * type it was given when it was created.
   *
   * @param aggCall Aggregate call
   * @param fail    Whether to fail if the types do not match
   * @return Whether the inferred and declared types match
   */
  private boolean typeMatchesInferred(
      final AggregateCall aggCall,
      final boolean fail) {
    SqlAggFunction aggFunction = aggCall.getAggregation();
    AggCallBinding callBinding = aggCall.createBinding(this);
    RelDataType type = aggFunction.inferReturnType(callBinding);
    RelDataType expectedType = aggCall.type;
    return RelOptUtil.eq("aggCall type",
        expectedType,
        "inferred type",
        type,
        fail);
  }

  /**
   * Returns whether any of the aggregates are DISTINCT.
   *
   * @return Whether any of the aggregates are DISTINCT
   * 是否有某一个聚合字段包含了distinct关键词
   */
  public boolean containsDistinctCall() {
    for (AggregateCall call : aggCalls) {
      if (call.isDistinct()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns the type of roll-up.
   *
   * @return Type of roll-up
   * 返回group by类型
   */
  public Group getGroupType() {
    return Group.induce(groupSet, groupSets);
  }

  /** What kind of roll-up is it?
   * 是否简单的group操作
   **/
  public enum Group {
    SIMPLE,
    ROLLUP,
    CUBE,
    OTHER;

    public static Group induce(ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets) {
      if (!ImmutableBitSet.ORDERING.isStrictlyOrdered(groupSets)) {
        throw new IllegalArgumentException("must be sorted: " + groupSets);
      }
      if (groupSets.size() == 1 && groupSets.get(0).equals(groupSet)) {//只有group by一种语法
        return SIMPLE;
      }
      if (groupSets.size() == IntMath.pow(2, groupSet.cardinality())) {//全排列,所以是cube
        return CUBE;
      }
    checkRollup:
      if (groupSets.size() == groupSet.cardinality() + 1) {
        ImmutableBitSet g = groupSet;
        for (ImmutableBitSet bitSet : groupSets) {
          if (!bitSet.equals(g)) {
            break checkRollup;
          }
          g = g.clear(g.length() - 1);
        }
        assert g.isEmpty();
        return ROLLUP;
      }
      return OTHER;
    }
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Implementation of the {@link SqlOperatorBinding} interface for an
   * {@link AggregateCall aggregate call} applied to a set of operands in the
   * context of a {@link org.apache.calcite.rel.logical.LogicalAggregate}.
   */
  public static class AggCallBinding extends SqlOperatorBinding {
    private final List<RelDataType> operands;
    private final int groupCount;

    /**
     * Creates an AggCallBinding
     *
     * @param typeFactory  Type factory
     * @param aggFunction  Aggregate function
     * @param operands     Data types of operands
     * @param groupCount   Number of columns in the GROUP BY clause
     */
    public AggCallBinding(
        RelDataTypeFactory typeFactory,
        SqlAggFunction aggFunction,
        List<RelDataType> operands,
        int groupCount) {
      super(typeFactory, aggFunction);
      this.operands = operands;
      this.groupCount = groupCount;
      assert operands != null
          : "operands of aggregate call should not be null";
      assert groupCount >= 0
          : "number of group by columns should be greater than zero in "
          + "aggregate call. Got " + groupCount;
    }

    @Override public int getGroupCount() {
      return groupCount;
    }

    public int getOperandCount() {
      return operands.size();
    }

    public RelDataType getOperandType(int ordinal) {
      return operands.get(ordinal);
    }

    public CalciteException newError(
        Resources.ExInst<SqlValidatorException> e) {
      return SqlUtil.newContextException(SqlParserPos.ZERO, e);
    }
  }
}

// End Aggregate.java
