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
package org.apache.calcite.rel.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableList;

import java.math.BigDecimal;
import java.util.List;

/**
 * Sub-class of {@link org.apache.calcite.rel.core.Values}
 * not targeted at any particular engine or calling convention.
 * 属于sql中values,用于生产静态的数据源
 */
public class LogicalValues extends Values {
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a new LogicalValues.
   *
   * <p>Note that tuples passed in become owned by this
   * rel (without a deep copy), so caller must not modify them after this
   * call, otherwise bad things will happen.
   *
   * @param cluster Cluster that this relational expression belongs to
   * @param rowType Row type for tuples produced by this rel
   * @param tuples  2-dimensional array of tuple values to be produced; outer
   *                list contains tuples; each inner list is one tuple; all
   *                tuples must be of same length, conforming to rowType
   */
  public LogicalValues(
      RelOptCluster cluster,
      RelDataType rowType,//每一行的字段schema
      ImmutableList<ImmutableList<RexLiteral>> tuples) {//values一组row,所以是二维数组,外面表示一行数据,里面表示每一行的列
    super(cluster, rowType, tuples, cluster.traitSetOf(Convention.NONE));
  }

  /**
   * Creates a LogicalValues by parsing serialized output.
   */
  public LogicalValues(RelInput input) {
    super(input);
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    assert inputs.isEmpty();
    return new LogicalValues(
        getCluster(),
        rowType,
        tuples);
  }

  /** Creates a LogicalValues that outputs no rows of a given row type.
   * 创建一个空数据源集合
   **/
  public static LogicalValues createEmpty(RelOptCluster cluster,
      RelDataType rowType) {
    return new LogicalValues(cluster, rowType,
        ImmutableList.<ImmutableList<RexLiteral>>of());//不需要有具体的row值,因此是空数组
  }

  /** Creates a LogicalValues that outputs one row and one column.
   * 创建一行一列的数据源
   **/
  public static LogicalValues createOneRow(RelOptCluster cluster) {
    final RelDataType rowType =
        cluster.getTypeFactory().builder()
            .add("ZERO", SqlTypeName.INTEGER).nullable(false)
            .build();//列类型是int

    final ImmutableList<ImmutableList<RexLiteral>> tuples =
        ImmutableList.of(
            ImmutableList.of(cluster.getRexBuilder().makeExactLiteral(BigDecimal.ZERO,rowType.getFieldList().get(0).getType())));//产生一个0的值

    return new LogicalValues(cluster, rowType, tuples);
  }

  @Override public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }
}

// End LogicalValues.java
