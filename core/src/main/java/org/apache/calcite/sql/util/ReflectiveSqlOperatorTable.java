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
package org.apache.calcite.sql.util;

import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.SqlPrefixOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ReflectiveSqlOperatorTable implements the {@link SqlOperatorTable } interface
 * by reflecting the public fields of a subclass.
 */
public abstract class ReflectiveSqlOperatorTable implements SqlOperatorTable {
  public static final String IS_NAME = "INFORMATION_SCHEMA";

  //~ Instance fields --------------------------------------------------------

  //SqlOperator.name 与 SqlOperator的映射
  private final Multimap<String, SqlOperator> operators = HashMultimap.create();

  //SqlOperator.name+动作类型 与 SqlOperator的映射
  private final Map<Pair<String, SqlSyntax>, SqlOperator> mapNameToOp =
      new HashMap<Pair<String, SqlSyntax>, SqlOperator>();

  //~ Constructors -----------------------------------------------------------

  protected ReflectiveSqlOperatorTable() {
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Performs post-constructor initialization of an operator table. It can't
   * be part of the constructor, because the subclass constructor needs to
   * complete first.
   * 每一个SqlFunction作为一个属性
   * 每一个SqlOperator作为一个属性
   */
  public final void init() {
    // Use reflection to register the expressions stored in public fields.
    for (Field field : getClass().getFields()) {
      try {
        if (SqlFunction.class.isAssignableFrom(field.getType())) {
          SqlFunction op = (SqlFunction) field.get(this);
          if (op != null) {
            register(op);
          }
        } else if (
            SqlOperator.class.isAssignableFrom(field.getType())) {
          SqlOperator op = (SqlOperator) field.get(this);
          register(op);
        }
      } catch (IllegalArgumentException e) {
        throw Util.newInternal(
            e,
            "Error while initializing operator table");
      } catch (IllegalAccessException e) {
        throw Util.newInternal(
            e,
            "Error while initializing operator table");
      }
    }
  }

  // implement SqlOperatorTable
  //通过操作name去查找操作集合,找到的操作集合存储到operatorList参数中
  public void lookupOperatorOverloads(SqlIdentifier opName,//操作名字
      SqlFunctionCategory category,
      SqlSyntax syntax,//操作语法
      List<SqlOperator> operatorList) {//返回的操作方法集合
    // NOTE jvs 3-Mar-2005:  ignore category until someone cares

    String simpleName;
    if (opName.names.size() > 1) {
      if (opName.names.get(opName.names.size() - 2).equals(IS_NAME)) {//倒数第2位是INFORMATION_SCHEMA库
        // per SQL99 Part 2 Section 10.4 Syntax Rule 7.b.ii.1
        simpleName = Util.last(opName.names);
      } else {
        return;
      }
    } else {
      simpleName = opName.getSimple();
    }

    // Always look up built-in operators case-insensitively. Even in sessions
    // with unquotedCasing=UNCHANGED and caseSensitive=true.
    final Collection<SqlOperator> list =
        operators.get(simpleName.toUpperCase());//通过操作的名字,找到操作对象
    if (list.isEmpty()) {
      return;
    }

    //同一个操作name可能有多个SqlSyntax
    for (SqlOperator op : list) {
      if (op.getSyntax() == syntax) {
        operatorList.add(op);
      } else if (syntax == SqlSyntax.FUNCTION
          && op instanceof SqlFunction) {
        // this special case is needed for operators like CAST,
        // which are treated as functions but have special syntax
        operatorList.add(op);
      }
    }

    //理论上不会走这个逻辑
    // REVIEW jvs 1-Jan-2005:  why is this extra lookup required?
    // Shouldn't it be covered by search above?
    switch (syntax) {
    case BINARY:
    case PREFIX:
    case POSTFIX:
      SqlOperator extra = mapNameToOp.get(Pair.of(simpleName, syntax));
      // REVIEW: should only search operators added during this method?
      if (extra != null && !operatorList.contains(extra)) {
        operatorList.add(extra);
      }
      break;
    }
  }

  public void register(SqlOperator op) {
    operators.put(op.getName(), op);
    if (op instanceof SqlBinaryOperator) {
      mapNameToOp.put(Pair.of(op.getName(), SqlSyntax.BINARY), op);
    } else if (op instanceof SqlPrefixOperator) {
      mapNameToOp.put(Pair.of(op.getName(), SqlSyntax.PREFIX), op);
    } else if (op instanceof SqlPostfixOperator) {
      mapNameToOp.put(Pair.of(op.getName(), SqlSyntax.POSTFIX), op);
    }
  }

  /**
   * Registers a function in the table.
   *
   * @param function Function to register
   */
  public void register(SqlFunction function) {
    operators.put(function.getName(), function);
    SqlFunctionCategory funcType = function.getFunctionType();
    assert funcType != null
        : "Function type for " + function.getName() + " not set";
  }

  //返回所有的注册的操作集合
  public List<SqlOperator> getOperatorList() {
    return ImmutableList.copyOf(operators.values());
  }
}

// End ReflectiveSqlOperatorTable.java
