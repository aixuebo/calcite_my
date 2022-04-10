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

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of {@link SqlRexConvertletTable} which uses reflection to call
 * any method of the form <code>public RexNode convertXxx(ConvertletContext,
 * SqlNode)</code> or <code>public RexNode convertXxx(ConvertletContext,
 * SqlOperator, SqlCall)</code>.
 * 参见 StandardConvertletTable,注册每一个sqlNode如何转换成表达是rexNode的过程
 */
public class ReflectiveConvertletTable implements SqlRexConvertletTable {
  //~ Instance fields --------------------------------------------------------

  private final Map<Object, Object> map = new HashMap<Object, Object>();

  //~ Constructors -----------------------------------------------------------

  public ReflectiveConvertletTable() {
    for (final Method method : getClass().getMethods()) {
      registerNodeTypeMethod(method); //匹配public RexNode convertXxx(SqlRexContext,SqlNode)方法
      registerOpTypeMethod(method); //匹配public RexNode convertXxx(SqlRexContext,SqlOperator,SqlCall)方法
    }
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Registers method if it: a. is public, and b. is named "convertXxx", and
   * c. has a return type of "RexNode" or a subtype d. has a 2 parameters with
   * types ConvertletContext and SqlNode (or a subtype) respectively.
   *
   * 方法匹配的条件: public RexNode convertXxx(SqlRexContext,SqlNode)
   * 匹配的结果 SqlNode --> ReflectiveConvertletTable.convertXxx()
   */
  private void registerNodeTypeMethod(final Method method) {
    if (!Modifier.isPublic(method.getModifiers())) {
      return;
    }
    if (!method.getName().startsWith("convert")) {
      return;
    }
    if (!RexNode.class.isAssignableFrom(method.getReturnType())) {
      return;
    }
    final Class[] parameterTypes = method.getParameterTypes();
    if (parameterTypes.length != 2) {
      return;
    }
    if (parameterTypes[0] != SqlRexContext.class) {
      return;
    }
    final Class parameterType = parameterTypes[1];
    if (!SqlNode.class.isAssignableFrom(parameterType)) {
      return;
    }
    map.put(
        parameterType,
        new SqlRexConvertlet() {
          public RexNode convertCall(
              SqlRexContext cx,
              SqlCall call) {
            try {
              return (RexNode) method.invoke(
                  ReflectiveConvertletTable.this,
                  cx,
                  call);
            } catch (IllegalAccessException e) {
              throw Util.newInternal(
                  e,
                  "while converting " + call);
            } catch (InvocationTargetException e) {
              throw Util.newInternal(
                  e,
                  "while converting " + call);
            }
          }
        });
  }

  /**
   * Registers method if it: a. is public, and b. is named "convertXxx", and
   * c. has a return type of "RexNode" or a subtype d. has a 3 parameters with
   * types: ConvertletContext; SqlOperator (or a subtype), SqlCall (or a
   * subtype).
   *
   * 方法匹配的条件: public RexNode convertXxx(SqlRexContext,SqlOperator,SqlCall)
   * 匹配的结果: SqlOperator --> ReflectiveConvertletTable.convertXxx()
   */
  private void registerOpTypeMethod(final Method method) {
    if (!Modifier.isPublic(method.getModifiers())) {
      return;
    }
    if (!method.getName().startsWith("convert")) {
      return;
    }
    if (!RexNode.class.isAssignableFrom(method.getReturnType())) {
      return;
    }
    final Class[] parameterTypes = method.getParameterTypes();
    if (parameterTypes.length != 3) {
      return;
    }
    if (parameterTypes[0] != SqlRexContext.class) {
      return;
    }
    final Class opClass = parameterTypes[1];
    if (!SqlOperator.class.isAssignableFrom(opClass)) {
      return;
    }
    final Class parameterType = parameterTypes[2];
    if (!SqlCall.class.isAssignableFrom(parameterType)) {
      return;
    }
    map.put(
        opClass,
        new SqlRexConvertlet() {
          public RexNode convertCall(
              SqlRexContext cx,
              SqlCall call) {
            try {
              return (RexNode) method.invoke(
                  ReflectiveConvertletTable.this,
                  cx,
                  call.getOperator(),
                  call);
            } catch (IllegalAccessException e) {
              throw Util.newInternal(
                  e,
                  "while converting " + call);
            } catch (InvocationTargetException e) {
              throw Util.newInternal(
                  e,
                  "while converting " + call);
            }
          }
        });
  }

  //找到如何变更表达式的实现类

  /**
   * 先查找SqlOperator、然后查找SqlOperator的父类、最后查找SqlCall的父类
   */
  public SqlRexConvertlet get(SqlCall call) {
    SqlRexConvertlet convertlet;
    final SqlOperator op = call.getOperator();

    // Is there a convertlet for this operator
    // (e.g. SqlStdOperatorTable.plusOperator)?
    convertlet = (SqlRexConvertlet) map.get(op);
    if (convertlet != null) {
      return convertlet;
    }

    // Is there a convertlet for this class of operator
    // (e.g. SqlBinaryOperator)?
    Class<? extends Object> clazz = op.getClass();
    while (clazz != null) {
      convertlet = (SqlRexConvertlet) map.get(clazz);
      if (convertlet != null) {
        return convertlet;
      }
      clazz = clazz.getSuperclass();
    }

    // Is there a convertlet for this class of expression
    // (e.g. SqlCall)?
    clazz = call.getClass();
    while (clazz != null) {
      convertlet = (SqlRexConvertlet) map.get(clazz);
      if (convertlet != null) {
        return convertlet;
      }
      clazz = clazz.getSuperclass();
    }
    return null;
  }

  /**
   * Registers a convertlet for a given operator instance
   *
   * @param op         Operator instance, say
   * {@link org.apache.calcite.sql.fun.SqlStdOperatorTable#MINUS}
   * @param convertlet Convertlet
   * 为一个SqlOperator注册如何转换成表达式
   */
  protected void registerOp(SqlOperator op, SqlRexConvertlet convertlet) {
    map.put(op, convertlet);
  }

  /**
   * Registers that one operator is an alias for another.
   *
   * @param alias  Operator which is alias
   * @param target Operator to translate calls to
   * 添加别名
   */
  protected void addAlias(final SqlOperator alias, final SqlOperator target) {
    map.put(
        alias,//别名
        new SqlRexConvertlet() {
          public RexNode convertCall(
              SqlRexContext cx,
              SqlCall call) {
            Util.permAssert(
                call.getOperator() == alias,//说明用到的是别名
                "call to wrong operator");
            final SqlCall newCall =
                target.createCall(//将别名转换成target,即替换成可用的转换对象
                    SqlParserPos.ZERO,
                    call.getOperandList());
            return cx.convertExpression(newCall);
          }
        });
  }
}

// End ReflectiveConvertletTable.java
