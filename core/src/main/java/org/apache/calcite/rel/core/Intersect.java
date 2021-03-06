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
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

/**
 * Relational expression that returns the intersection of the rows of its
 * inputs.
 *
 * <p>If "all" is true, performs then multiset intersection; otherwise,
 * performs set set intersection (implying no duplicates in the results).
 * 交集
 */
public abstract class Intersect extends SetOp {
  /**
   * Creates an Intersect.
   */
  public Intersect(
      RelOptCluster cluster,
      RelTraitSet traits,
      List<RelNode> inputs,
      boolean all) {
    super(cluster, traits, inputs, SqlKind.INTERSECT, all);
  }

  /**
   * Creates an Intersect by parsing serialized output.
   */
  protected Intersect(RelInput input) {
    super(input);
  }

  @Override public double getRows() {
    // REVIEW jvs 30-May-2005:  I just pulled this out of a hat.
    double dRows = Double.MAX_VALUE;
    for (RelNode input : inputs) {
      dRows = Math.min(
          dRows, RelMetadataQuery.getRowCount(input));
    }
    dRows *= 0.25;
    return dRows;
  }

  @Override public boolean isKey(ImmutableBitSet columns) {
    for (RelNode input : inputs) {
      if (input.isKey(columns)) {
        return true;
      }
    }
    return super.isKey(columns);
  }
}

// End Intersect.java
