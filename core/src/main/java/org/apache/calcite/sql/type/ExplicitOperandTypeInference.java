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
import org.apache.calcite.sql.SqlCallBinding;

import com.google.common.collect.ImmutableList;

/**
 * ExplicitOperandTypeInferences implements {@link SqlOperandTypeInference} by
 * explicitly supplying a type for each parameter.
 * 准确的还原参数类型
 */
public class ExplicitOperandTypeInference implements SqlOperandTypeInference {
  //~ Instance fields --------------------------------------------------------

  private final ImmutableList<RelDataType> paramTypes;//准确的参数值

  //~ Constructors -----------------------------------------------------------

  /** Use
   * {@link org.apache.calcite.sql.type.InferTypes#explicit(java.util.List)}. */
  ExplicitOperandTypeInference(ImmutableList<RelDataType> paramTypes) {
    this.paramTypes = paramTypes;
  }

  //~ Methods ----------------------------------------------------------------

  public void inferOperandTypes(
      SqlCallBinding callBinding,
      RelDataType returnType,
      RelDataType[] operandTypes) {//预测参数值
    assert operandTypes.length == paramTypes.size();
    paramTypes.toArray(operandTypes);//将准确的参数值复制给预测参数值类型
  }
}

// End ExplicitOperandTypeInference.java
