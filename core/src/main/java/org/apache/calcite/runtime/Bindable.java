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
package org.apache.calcite.runtime;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;

/**
 * Statement that can be bound to a {@link DataContext} and then executed.
 *
 * @param <T> Element type of the resulting enumerable
 * 意义是Statement绑定一个上下文对象
 */
public interface Bindable<T> {
  /**
   * Executes this statement and returns an enumerable which will yield rows.
   * The {@code environment} parameter provides the values in the root of the
   * environment (usually schemas).
   *
   * @param dataContext Environment that provides tables
   * @return Enumerable over rows
   * 执行statement,返回每一行数据。
   */
  Enumerable<T> bind(DataContext dataContext);
}

// End Bindable.java
