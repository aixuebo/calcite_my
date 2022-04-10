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
package org.apache.calcite.sql.validate;

/**
 * Enumeration of types of monotonicity.
 * 字段类型值是否是单调递增护着递减的
 * 字段是否是排序的,以及如何排序的
 */
public enum SqlMonotonicity {
  STRICTLY_INCREASING,//严格 增量
  INCREASING,//增量
  STRICTLY_DECREASING,//严格 减少
  DECREASING,
  CONSTANT,//常量
  /**
   * Catch-all value for expressions that have some monotonic properties.
   * Maybe it isn't known whether the expression is increasing or decreasing;
   * or maybe the value is neither increasing nor decreasing but the value
   * never repeats.
   */
  MONOTONIC,//单调的
  NOT_MONOTONIC;//非单调的

  /**
   * If this is a strict monotonicity (StrictlyIncreasing, StrictlyDecreasing)
   * returns the non-strict equivalent (Increasing, Decreasing).
   *
   * @return non-strict equivalent monotonicity
   * 将严格的转换成不严格类型
   */
  public SqlMonotonicity unstrict() {
    switch (this) {
    case STRICTLY_INCREASING:
      return INCREASING;
    case STRICTLY_DECREASING:
      return DECREASING;
    default:
      return this;
    }
  }

  /**
   * Returns the reverse monotonicity.
   *
   * @return reverse monotonicity
   * 取反义词
   */
  public SqlMonotonicity reverse() {
    switch (this) {
    case STRICTLY_INCREASING:
      return STRICTLY_DECREASING;
    case INCREASING:
      return DECREASING;
    case STRICTLY_DECREASING:
      return STRICTLY_INCREASING;
    case DECREASING:
      return INCREASING;
    default:
      return this;
    }
  }

  /**
   * Whether values of this monotonicity are decreasing. That is, if a value
   * at a given point in a sequence is X, no point later in the sequence will
   * have a value greater than X.
   *
   * @return whether values are decreasing
   * 是否是降低的
   */
  public boolean isDecreasing() {
    switch (this) {
    case STRICTLY_DECREASING:
    case DECREASING:
      return true;
    default:
      return false;
    }
  }

  /**
   * Returns whether values of this monotonicity may ever repeat after moving
   * to another value: true for {@link #NOT_MONOTONIC} and {@link #CONSTANT},
   * false otherwise.
   *
   * <p>If a column is known not to repeat, a sort on that column can make
   * progress before all of the input has been seen.
   *
   * @return whether values repeat
   * 只有常量、非单调的，才允许重复，其他都不允许重复
   */
  public boolean mayRepeat() {
    switch (this) {
    case NOT_MONOTONIC:
    case CONSTANT:
      return true;
    default:
      return false;
    }
  }
}

// End SqlMonotonicity.java
