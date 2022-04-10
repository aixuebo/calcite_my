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
package org.apache.calcite.rel;

/**
 * Definition of the ordering of one field of a {@link RelNode} whose
 * output is to be sorted.
 *
 * @see RelCollation
 * 主要描述排序的列是谁、如何排序、如果是null时候该如何排序
 */
public class RelFieldCollation {
  //~ Enums ------------------------------------------------------------------

  /**
   * Direction that a field is ordered in.
   * 排序规则
   */
  public enum Direction {
    /**
     * Ascending direction: A value is always followed by a greater or equal
     * value.
     */
    ASCENDING("ASC"),

    /**
     * Strictly ascending direction: A value is always followed by a greater
     * value.
     * 不能有相等,严格意义的升序
     */
    STRICTLY_ASCENDING("SASC"),

    /**
     * Descending direction: A value is always followed by a lesser or equal
     * value.
     */
    DESCENDING("DESC"),

    /**
     * Strictly descending direction: A value is always followed by a lesser
     * value.
     */
    STRICTLY_DESCENDING("SDESC"),

    /**
     * Clustered direction: Values occur in no particular order, and the
     * same value may occur in contiguous groups, but never occurs after
     * that. This sort order tends to occur when values are ordered
     * according to a hash-key.
     */
    CLUSTERED("CLU");

    public final String shortString;

    Direction(String shortString) {
      this.shortString = shortString;
    }
  }

  /**
   * Ordering of nulls.
   * null的时候如何排序
   */
  public enum NullDirection {
    FIRST,
    LAST,
    UNSPECIFIED //默认 null放在最后面,与last效果相同
  }

  //~ Instance fields --------------------------------------------------------

  /**
   * 0-based index of field being sorted.
   * 被排序的属性序号
   */
  private final int fieldIndex;

  /**
   * Direction of sorting.
   */
  public final Direction direction;

  /**
   * Direction of sorting of nulls.
   */
  public final NullDirection nullDirection;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an ascending field collation.
   */
  public RelFieldCollation(int fieldIndex) {
    this(fieldIndex, Direction.ASCENDING, NullDirection.UNSPECIFIED);
  }

  /**
   * Creates a field collation with unspecified null direction.
   */
  public RelFieldCollation(int fieldIndex, Direction direction) {
    this(fieldIndex, direction, NullDirection.UNSPECIFIED);
  }

  /**
   * Creates a field collation.
   */
  public RelFieldCollation(
      int fieldIndex,
      Direction direction,
      NullDirection nullDirection) {
    this.fieldIndex = fieldIndex;
    this.direction = direction;
    this.nullDirection = nullDirection;
    assert direction != null;
    assert nullDirection != null;
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Creates a copy of this RelFieldCollation against a different field.
   */
  public RelFieldCollation copy(int target) {
    if (target == fieldIndex) {
      return this;
    }
    return new RelFieldCollation(target, direction, nullDirection);
  }

  // implement Object
  public boolean equals(Object obj) {
    if (!(obj instanceof RelFieldCollation)) {
      return false;
    }
    RelFieldCollation other = (RelFieldCollation) obj;
    return (fieldIndex == other.fieldIndex)
        && (direction == other.direction)
        && (nullDirection == other.nullDirection);
  }

  // implement Object
  public int hashCode() {
    return this.fieldIndex
        | (this.direction.ordinal() << 4)
        | (this.nullDirection.ordinal() << 8);
  }

  public int getFieldIndex() {
    return fieldIndex;
  }

  public RelFieldCollation.Direction getDirection() {
    return direction;
  }

  public String toString() {
    return fieldIndex
        + " " + direction.shortString
        + (nullDirection == NullDirection.UNSPECIFIED
        ? ""
        : " " + nullDirection);
  }

  public String shortString() {
    switch (nullDirection) {
    case FIRST:
      return direction.shortString + "-nulls-first";
    case LAST:
      return direction.shortString + "-nulls-last";
    default:
      return direction.shortString;
    }
  }
}

// End RelFieldCollation.java
