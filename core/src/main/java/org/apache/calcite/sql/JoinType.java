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
package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Enumerates the types of join.
 * 表示枚举值
 * 一种字面量对象  即特殊字符串含义的对象
 */
public enum JoinType implements SqlLiteral.SqlSymbol {
  /**
   * Inner join.
   */
  INNER,

  /**
   * Full outer join.
   */
  FULL,

  /**
   * Cross join (also known as Cartesian product).
   */
  CROSS,

  /**
   * Left outer join.
   */
  LEFT,

  /**
   * Right outer join.
   */
  RIGHT,

  /**
   * Comma join: the good old-fashioned SQL <code>FROM</code> clause,
   * where table expressions are specified with commas between them, and
   * join conditions are specified in the <code>WHERE</code> clause.
   * 原始的使用逗号去join
   */
  COMMA;

  /**
   * Creates a parse-tree node representing an occurrence of this
   * condition type keyword at a particular position in the parsed
   * text.
   * 枚举值可以转换成SqlLiteral。
   */
  public SqlLiteral symbol(SqlParserPos pos) {
    return SqlLiteral.createSymbol(this, pos);
  }
}

// End JoinType.java
