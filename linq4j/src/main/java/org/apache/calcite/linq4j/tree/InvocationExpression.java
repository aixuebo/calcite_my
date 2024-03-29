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

/**
 * Represents an expression that applies a delegate or lambda expression to a
 * list of argument expressions.
 *
 * 代表一个表达式，可以利用动态代理或者lambda的方式,执行结果
 */
public class InvocationExpression extends Expression {
  public InvocationExpression(ExpressionType nodeType, Class type) {
    super(nodeType, type);
  }

  @Override public Expression accept(Visitor visitor) {
    return visitor.visit(this);
  }
}

// End InvocationExpression.java
