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
package org.apache.calcite.sql.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;

/**
 * Strategy interface to infer the type of an operator call from the type of the
 * operands.
 * 策略:返回值类型来自于参数的某一个类型
 * <p>This interface is an example of the
 * {@link org.apache.calcite.util.Glossary#STRATEGY_PATTERN strategy pattern}.
 * This makes
 * sense because many operators have similar, straightforward strategies, such
 * as to take the type of the first operand.</p>
 */
public interface SqlReturnTypeInference {
  //~ Methods ----------------------------------------------------------------

  /**
   * Infers the return type of a call to an {@link SqlOperator}.
   *
   * @param opBinding description of operator binding
   * @return inferred type; may be null
   * 根据策略实现，通过参数类型,推导函数返回值类型
   */
  RelDataType inferReturnType(SqlOperatorBinding opBinding);
}

// End SqlReturnTypeInference.java
