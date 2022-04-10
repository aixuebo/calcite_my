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
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;

/**
 * Sub-class of {@link org.apache.calcite.rel.core.Filter}
 * not targeted at any particular engine or calling convention.
 * where过滤
 */
public final class LogicalFilter extends Filter {
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a LogicalFilter.
   *
   * @param cluster   Cluster that this relational expression belongs to
   * @param child     Input relational expression,root节点
   * @param condition Boolean expression which determines whether a row is
   *                  allowed to pass ,where表达式
   */
  public LogicalFilter(
      RelOptCluster cluster,
      RelNode child,//父节点--数据源
      RexNode condition) {//过滤表达式
    super(
        cluster,
        cluster.traitSetOf(Convention.NONE),
        child,
        condition);
  }

  /**
   * Creates a LogicalFilter by parsing serialized output.
   */
  public LogicalFilter(RelInput input) {
    super(input);
  }

  //~ Methods ----------------------------------------------------------------

  public LogicalFilter copy(RelTraitSet traitSet, RelNode input,
      RexNode condition) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new LogicalFilter(getCluster(), input, condition);
  }

  @Override public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }
}

// End LogicalFilter.java
