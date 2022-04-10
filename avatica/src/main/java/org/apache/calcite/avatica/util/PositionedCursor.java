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
package org.apache.calcite.avatica.util;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

/**
 * Abstract implementation of {@link org.apache.calcite.avatica.util.Cursor}
 * that caches its current row.
 *
 * @param <T> Element type
 */
public abstract class PositionedCursor<T> extends AbstractCursor {
  /**
   * Returns the current row.
   * 返回一行数据
   * @return current row
   *
   * @throws java.util.NoSuchElementException if the iteration has no more
   * elements
   */
  protected abstract T current();

  /** Implementation of
   * {@link org.apache.calcite.avatica.util.AbstractCursor.Getter}
   * that reads from records that are arrays.
   * 一行元素是一个数组
   *
   *
   * 注意:该对象也是继承AbstractGetter,因此也会有wasNull,此时的wasNull只是指代该字段是否为null
   **/
  protected class ArrayGetter extends AbstractGetter {
    protected final int field;//获取数组的第几个属性,即具体到列

    public ArrayGetter(int field) {
      this.field = field;
    }

    public Object getObject() {
      Object o = ((Object[]) current())[field]; //具体该列的值
      wasNull[0] = o == null; //该列值是否是null
      return o;//返回列值
    }
  }

  /** Implementation of
   * {@link org.apache.calcite.avatica.util.AbstractCursor.Getter}
   * that reads items from a list.
   * 处理一行数据的返回值是list,列是list的第几个元素
   **/
  protected class ListGetter extends AbstractGetter {
    protected final int index;

    public ListGetter(int index) {
      this.index = index;
    }

    public Object getObject() {
      Object o = ((List) current()).get(index);
      wasNull[0] = o == null;
      return o;
    }
  }

  /** Implementation of
   * {@link org.apache.calcite.avatica.util.AbstractCursor.Getter}
   * for records that consist of a single field.
   *
   * <p>Each record is represented as an object, and the value of the sole
   * field is that object.
   * 处理一行数据就一个值
   **/
  protected class ObjectGetter extends AbstractGetter {
    public ObjectGetter(int field) {
      assert field == 0;
    }

    public Object getObject() {
      Object o = current();
      wasNull[0] = o == null;
      return o;
    }
  }

  /** Implementation of
   * {@link org.apache.calcite.avatica.util.AbstractCursor.Getter}
   * that reads fields via reflection.
   * 处理一行数据是一个对象  --- 一行数据是一个java的class
   **/
  protected class FieldGetter extends AbstractGetter {
    protected final Field field;//该字段是class的哪个属性

    public FieldGetter(Field field) {
      this.field = field;
    }

    public Object getObject() {
      Object o;
      try {
        o = field.get(current());
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
      wasNull[0] = o == null;
      return o;
    }
  }

  /** Implementation of
   * {@link org.apache.calcite.avatica.util.AbstractCursor.Getter}
   * that reads entries from a {@link java.util.Map}.
   * 处理一行数据是一个map
   **/
  protected class MapGetter<K> extends AbstractGetter {
    protected final K key;

    public MapGetter(K key) {
      this.key = key;
    }

    public Object getObject() {
      @SuppressWarnings("unchecked") final Map<K, Object> map =
          (Map<K, Object>) current();
      Object o = map.get(key);
      wasNull[0] = o == null;
      return o;
    }
  }
}

// End PositionedCursor.java
