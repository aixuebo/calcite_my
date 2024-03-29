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
package org.apache.calcite.linq4j.tree;

import java.lang.reflect.Type;

/**
 * Contains the parts of the {@link java.lang.reflect.Field} class needed
 * for code generation, but might be implemented differently.
 *
 * 代表一个属性或者字段
 *
 * 包含属性类型、名称、属性值、以及访问方式public等信息
 */
public interface PseudoField {
  String getName();

  Type getType();

  int getModifiers();

  Object get(Object o) throws IllegalAccessException;

  Type getDeclaringClass();
}

// End PseudoField.java
