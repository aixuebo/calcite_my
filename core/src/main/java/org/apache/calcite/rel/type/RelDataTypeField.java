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
package org.apache.calcite.rel.type;

import java.util.Map;

/**
 * RelDataTypeField represents the definition of a field in a structured
 * {@link RelDataType}.
 *
 * <p>Extends the {@link java.util.Map.Entry} interface to allow convenient
 * inter-operation with Java collections classes. In any implementation of this
 * interface, {@link #getKey()} must be equivalent to {@link #getName()}
 * and {@link #getValue()} must be equivalent to {@link #getType()}.
 * 表示某一个字段
 */
public interface RelDataTypeField extends Map.Entry<String, RelDataType> {
  //~ Methods ----------------------------------------------------------------

  /**
   * Gets the name of this field, which is unique within its containing type.
   *
   * @return field name 字段名字
   */
  String getName();

  /**
   * Gets the ordinal of this field within its containing type.
   *
   * @return 0-based ordinal 字段在表的位置,属于第几个字段
   */
  int getIndex();

  /**
   * Gets the type of this field.
   * 获取该字段的类型
   * @return field type
   */
  RelDataType getType();
}

// End RelDataTypeField.java
