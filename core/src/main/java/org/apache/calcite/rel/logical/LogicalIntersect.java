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
import org.apache.calcite.rel.core.Intersect;

import java.util.List;

/**
 * Sub-class of {@link org.apache.calcite.rel.core.Intersect}
 * not targeted at any particular engine or calling convention.
 * 交集
 */
public final class LogicalIntersect extends Intersect {
  //~ Constructors -----------------------------------------------------------

  public LogicalIntersect(
      RelOptCluster cluster,
      List<RelNode> inputs,//比如持有一组LogicalProject
      boolean all) {
    super(
        cluster,
        cluster.traitSetOf(Convention.NONE),
        inputs,
        all);
  }

  /** Creates a LogicalIntersect by parsing serialized output. */
  public LogicalIntersect(RelInput input) {
    super(input);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public LogicalIntersect copy(RelTraitSet traitSet,
      List<RelNode> inputs, boolean all) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new LogicalIntersect(getCluster(), inputs, all);
  }

  @Override public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }
}

// End LogicalIntersect.java
