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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;

/**
 * Reference to the current row of a correlating relational expression.
 * 引用当前行的相关表达式
 *
 * <p>Correlating variables are introduced when performing nested loop joins.
 * Each row is received from one side of the join, a correlating variable is
 * assigned a value, and the other side of the join is restarted.</p>
 * 用于select 嵌套join中
 */
public class RexCorrelVariable extends RexVariable {
  //~ Constructors -----------------------------------------------------------

  RexCorrelVariable(
      String varName,
      RelDataType type) {
    super(varName, type);
  }

  //~ Methods ----------------------------------------------------------------

  public <R> R accept(RexVisitor<R> visitor) {
    return visitor.visitCorrelVariable(this);
  }

  @Override public SqlKind getKind() {
    return SqlKind.CORREL_VARIABLE;
  }
}

// End RexCorrelVariable.java
