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

/**
 * A row-expression which references a field.
 * 行表达式,变量,代表一个字段的引用
 */
public abstract class RexVariable extends RexNode {
  //~ Instance fields --------------------------------------------------------

  protected final String name;
  protected final RelDataType type;

  //~ Constructors -----------------------------------------------------------

  protected RexVariable(
      String name,
      RelDataType type) {
    assert type != null;
    assert name != null;
    this.name = name;
    this.digest = name;
    this.type = type;
  }

  //~ Methods ----------------------------------------------------------------

  public RelDataType getType() {
    return type;
  }

  /**
   * Returns the name of this variable.
   */
  public String getName() {
    return name;
  }
}

// End RexVariable.java
