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
package org.apache.calcite.sql2rel;

import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;

/**
 * Converts expressions from {@link SqlNode} to {@link RexNode}.
 * 负责具体的转换表达式的工作
 */
public interface SqlNodeToRexConverter {
  //~ Methods ----------------------------------------------------------------

  /**
   * Converts a {@link SqlCall} to a {@link RexNode} expression.
   * 将SqlCall对象转换成表达式
   */
  RexNode convertCall(
      SqlRexContext cx,
      SqlCall call);

  /**
   * Converts a {@link SqlLiteral SQL literal} to a
   * {@link RexLiteral REX literal}.
   *
   * <p>The result is {@link RexNode}, not {@link RexLiteral} because if the
   * literal is NULL (or the boolean Unknown value), we make a <code>CAST(NULL
   * AS type)</code> expression.
   * 将常数对象转换成表达式
   */
  RexNode convertLiteral(
      SqlRexContext cx,
      SqlLiteral literal);

  /**
   * Converts a {@link SqlIntervalQualifier SQL Interval Qualifier} to a
   * {@link RexLiteral REX literal}.
   * 将时间日期对象转换成表达式
   */
  RexLiteral convertInterval(
      SqlRexContext cx,
      SqlIntervalQualifier intervalQualifier);
}

// End SqlNodeToRexConverter.java
