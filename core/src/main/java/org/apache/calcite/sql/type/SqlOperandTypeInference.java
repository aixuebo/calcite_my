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

/**
 * Strategy to infer unknown types of the operands of an operator call.
 * 猜测参数类型
 */
public interface SqlOperandTypeInference {
  //~ Methods ----------------------------------------------------------------

  /**
   * Infers any unknown operand types.
   * 推测未知的参数类型
   * @param callBinding  description of the call being analyzed
   * @param returnType   the type known or inferred for the result of the call 结果类型
   * @param operandTypes receives the inferred types for all operands 将所有的参数类型都设置为已推测的类型
   */
  void inferOperandTypes(
      SqlCallBinding callBinding,
      RelDataType returnType,
      RelDataType[] operandTypes);
}

// End SqlOperandTypeInference.java
