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

import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;

/**
 * Strategy interface to check for allowed operand types of an operator call.
 * 校验策略,校验参数是否正确、允许参数数量、对外如何描述需要什么样的参数
 * <p>This interface is an example of the
 * {@link org.apache.calcite.util.Glossary#STRATEGY_PATTERN strategy pattern}.
 */
public interface SqlOperandTypeChecker {
  //~ Methods ----------------------------------------------------------------

  /**
   * Checks the types of all operands to an operator call.
   * 校验所有参数的类型是否正确
   * @param callBinding    description of the call to be checked
   * @param throwOnFailure whether to throw an exception if check fails
   *                       (otherwise returns false in that case) 如果是true,则表示校验失败要抛异常
   * @return whether check succeeded
   *
   * 校验参数类型
   */
  boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure);

  /**
   * @return range of operand counts allowed in a call
   * 返回函数允许的参数数量
   * 校验参数的数量是否符合预期
   */
  SqlOperandCountRange getOperandCountRange();

  /**
   * Returns a string describing the allowed formal signatures of a call, e.g.
   * "SUBSTR(VARCHAR, INTEGER, INTEGER)".
   *
   * @param op     the operator being checked
   * @param opName name to use for the operator in case of aliasing
   * @return generated string
   * 返回一个字符串形式的描述,表示允许什么样的call被调用。
   * 比如SUBSTR(VARCHAR, INTEGER, INTEGER)，即函数的描述内容,让下游知道如何使用。
   */
  String getAllowedSignatures(SqlOperator op, String opName);
}

// End SqlOperandTypeChecker.java
