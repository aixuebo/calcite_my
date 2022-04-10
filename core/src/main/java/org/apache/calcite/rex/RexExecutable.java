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
package org.apache.calcite.rex;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.runtime.Utilities;
import org.apache.calcite.util.Pair;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ClassBodyEvaluator;
import org.codehaus.janino.Scanner;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;

/**
 * Result of compiling code generated from a {@link RexNode} expression.
 * 编译代码,产生结果
 */
public class RexExecutable {
  public static final String GENERATED_CLASS_NAME = "Reducer";

  private final Function1<DataContext, Object[]> compiledFunction;
  private final String code;//代码
  private DataContext dataContext;

  public RexExecutable(String code, Object reason) {
    try {
      //noinspection unchecked
      //因为code实现了该接口,因此可以赋予值
      compiledFunction =
          (Function1) ClassBodyEvaluator.createFastClassBodyEvaluator(//动态编译代码
              new Scanner(null, new StringReader(code)),//代码生成一个文件
              GENERATED_CLASS_NAME,//className名称
              Utilities.class,//继承该类,提供工具方法
              new Class[] {Function1.class, Serializable.class},//实现这2个接口
              getClass().getClassLoader());//loader
    } catch (CompileException e) {
      throw new RuntimeException("While compiling " + reason, e);
    } catch (IOException e) {
      throw new RuntimeException("While compiling " + reason, e);
    }
    this.code = code;
  }

  public void setDataContext(DataContext dataContext) {
    this.dataContext = dataContext;
  }

  /**
   * 产生表达式
   * @param rexBuilder 表达式工厂
   * @param constExps 参数
   * @param reducedValues 产生具体的表达式
   */
  public void reduce(RexBuilder rexBuilder, List<RexNode> constExps,
      List<RexNode> reducedValues) {
    Object[] values = compiledFunction.apply(dataContext);//返回结果值
    assert values.length == constExps.size();
    final List<Object> valueList = Arrays.asList(values);
    for (Pair<RexNode, Object> value : Pair.zip(constExps, valueList)) {//表达式节点与参数值映射
      reducedValues.add(
          rexBuilder.makeLiteral(value.right, value.left.getType(), true));//获取具体的表达式值
    }
    Hook.EXPRESSION_REDUCER.run(Pair.of(code, values));//传入代码、表达式集合
  }

  public Function1<DataContext, Object[]> getFunction() {
    return compiledFunction;
  }

  //执行,得到返回值
  public Object[] execute() {
    return compiledFunction.apply(dataContext);
  }

  //源码
  public String getSource() {
    return code;
  }
}

// End RexExecutable.java
