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

/**
 * Describes the necessary parameters for an implementation in order to
 * identify and set dynamic variables
 *
 * 设置变量id 与 name映射关系
 */
public class CorrelationId implements Cloneable, Comparable<CorrelationId> {//按照id排序
  private static final String CORREL_PREFIX = "$cor"; //id前缀

  private final int id;
  private final String name;

  /**
   * Creates a correlation identifier.
   * This is a type-safe wrapper over int.
   *
   * @param id     Identifier
   */
  public CorrelationId(int id) {
    this.id = id;
    this.name = CORREL_PREFIX + id;
  }

  /**
   * Creates a correlation identifier.
   * This is a type-safe wrapper over int.
   *
   * @param name     variable name
   */
  public CorrelationId(String name) {
    assert name != null && name.startsWith(CORREL_PREFIX)
        : "Correlation name should start with " + CORREL_PREFIX
        + " actual name is " + name;
    this.id = Integer.parseInt(name.substring(CORREL_PREFIX.length()));
    this.name = name;
  }

  /**
   * Returns the identifier.
   *
   * @return identifier
   */
  public int getId() {
    return id;
  }

  /**
   * Returns the preffered name of the variable.
   *
   * @return name
   */
  public String getName() {
    return name;
  }

  public String toString() {
    return name;
  }

  public int compareTo(CorrelationId other) {
    return id - other.id;
  }

  @Override public int hashCode() {
    return id;
  }

  @Override public boolean equals(Object obj) {
    return this == obj
        || obj instanceof CorrelationId
        && this.id == ((CorrelationId) obj).id;
  }
}

// End CorrelationId.java
