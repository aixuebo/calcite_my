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

import java.util.AbstractList;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Abstract base class for {@link RexInputRef} and {@link RexLocalRef}.
 * 为数据源做一个基类,记录该变量是数据源的第几个字段
 */
public abstract class RexSlot extends RexVariable {
  //~ Instance fields --------------------------------------------------------

  protected final int index;//属性序号,即第几个字段

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a slot.
   *
   * @param index Index of the field in the underlying rowtype 属性序号
   * @param type  Type of the column
   */
  protected RexSlot(
      String name,
      int index,
      RelDataType type) {
    super(name, type);
    assert index >= 0;
    this.index = index;
  }

  //~ Methods ----------------------------------------------------------------

  public int getIndex() {
    return index;
  }

  /**
   * Thread-safe list that populates itself if you make a reference beyond
   * the end of the list. Useful if you are using the same entries repeatedly.
   * Once populated, accesses are very efficient.
   * 返回前缀1、前缀2这样的字符串
   * 用于生产字段名称,比如$1、$2等
   */
  protected static class SelfPopulatingList
      extends CopyOnWriteArrayList<String> {
    private final String prefix;

    SelfPopulatingList(final String prefix, final int initialSize) {
      super(fromTo(prefix, 0, initialSize));
      this.prefix = prefix;
    }

    private static AbstractList<String> fromTo(
        final String prefix,
        final int start,
        final int end) {
      return new AbstractList<String>() {
        public String get(int index) {
          return prefix + (index + start);
        }

        public int size() {
          return end - start;
        }
      };
    }

    //返回"前缀1"这样的字符串
    @Override public String get(int index) {
      for (;;) {
        try {
          return super.get(index);
        } catch (IndexOutOfBoundsException e) {
          if (index < 0) {
            throw new IllegalArgumentException();
          }
          addAll(
              fromTo(
                  prefix, size(), Math.max(index + 1, size() * 2)));
        }
      }
    }
  }
}

// End RexSlot.java
