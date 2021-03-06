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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.SqlKind;

import java.util.List;

/**
 * Relational expression that returns the union of the rows of its inputs,
 * optionally eliminating duplicates.
 *
 * <p>Corresponds to SQL {@code UNION} and {@code UNION ALL}.
 * 并集
 */
public abstract class Union extends SetOp {
  //~ Constructors -----------------------------------------------------------

  protected Union(
      RelOptCluster cluster,
      RelTraitSet traits,
      List<RelNode> inputs,
      boolean all) {
    super(cluster, traits, inputs, SqlKind.UNION, all);
  }

  /**
   * Creates a Union by parsing serialized output.
   */
  protected Union(RelInput input) {
    super(input);
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelNode
  public double getRows() {
    double dRows = estimateRowCount(this);
    if (!all) {
      dRows *= 0.5;
    }
    return dRows;
  }

  /**
   * Helper method for computing row count for UNION ALL.
   *
   * @param rel node representing UNION ALL
   * @return estimated row count for rel
   */
  public static double estimateRowCount(RelNode rel) {
    double dRows = 0;
    for (RelNode input : rel.getInputs()) {
      dRows += RelMetadataQuery.getRowCount(input);
    }
    return dRows;
  }
}

// End Union.java
