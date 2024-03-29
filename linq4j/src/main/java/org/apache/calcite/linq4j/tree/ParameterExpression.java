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

import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents a named parameter expression.
 * 代表一个变量,定义变量类型+名称即可
 *
 * 即demo:  private String name
 */
public class ParameterExpression extends Expression {
  private static final AtomicInteger SEQ = new AtomicInteger();

  public final int modifier;//代表public 等定义方式
  public final String name;

  //匿名属性,即不需要具体name,统一赋予p0
  public ParameterExpression(Type type) {
    this(0, type, "p" + SEQ.getAndIncrement());
  }

  public ParameterExpression(int modifier, Type type, String name) {
    super(ExpressionType.Parameter, type);//表达式类型 以及 值类型
    assert name != null : "name should not be null";
    assert Character.isJavaIdentifierStart(name.charAt(0))
      : "parameter name should be valid java identifier: "
        + name + ". The first character is invalid.";
    this.modifier = modifier;
    this.name = name;
  }

  @Override public Expression accept(Visitor visitor) {
    return visitor.visit(this);
  }

  public Object evaluate(Evaluator evaluator) {
    return evaluator.peek(this);
  }

  @Override void accept(ExpressionWriter writer, int lprec, int rprec) {
    writer.append(name);
  }

  String declString() {
    return declString(type);
  }

  //比如 string name  用于生产java的方法的参数信息
  String declString(Type type) {
    final String modifiers = Modifier.toString(modifier);
    return modifiers + (modifiers.isEmpty() ? "" : " ") + Types.className(type)
        + " " + name;
  }

  @Override public boolean equals(Object o) {
    return this == o;
  }

  @Override public int hashCode() {
    return System.identityHashCode(this);
  }
}

// End ParameterExpression.java
