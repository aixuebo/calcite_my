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
package org.apache.calcite.sql.validate;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.util.Util;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Visitor which looks for an aggregate function inside a tree of
 * {@link SqlNode} objects.
 * 寻找聚合函数
 * 参见 class SqlAggFunction extends SqlFunction
 */
class AggFinder extends SqlBasicVisitor<Void> {
  //~ Instance fields --------------------------------------------------------

  private final SqlOperatorTable opTab;
  private final boolean over;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an AggFinder.
   *
   * @param opTab Operator table
   * @param over Whether to find windowed function calls {@code Agg(x) OVER
   *             windowSpec}
   */
  AggFinder(SqlOperatorTable opTab, boolean over) {
    this.opTab = opTab;
    this.over = over;
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Finds an aggregate.
   *
   * @param node Parse tree to search
   * @return First aggregate function in parse tree, or null if not found
   *
   * 参见 visit(SqlCall call) 中 if (operator.isAggregator())
   */
  public SqlNode findAgg(SqlNode node) {
    try {
      node.accept(this);
      return null;
    } catch (Util.FoundOne e) {
      Util.swallow(e, null);
      return (SqlNode) e.getNode();
    }
  }

  //参见 visit(SqlCall call) 中 if (operator.isAggregator())
  public SqlNode findAgg(List<SqlNode> nodes) {
    try {
      for (SqlNode node : nodes) {
        node.accept(this);
      }
      return null;
    } catch (Util.FoundOne e) {//找到聚合函数,则抛异常
      Util.swallow(e, null);
      return (SqlNode) e.getNode();//返回找到的节点
    }
  }

  //查找聚合函数
  public Void visit(SqlCall call) {
    final SqlOperator operator = call.getOperator();
    if (operator.isAggregator()) {
      throw new Util.FoundOne(call);//找到聚合函数,则抛异常
    }
    // User-defined function may not be resolved yet.
    if (operator instanceof SqlFunction
        && ((SqlFunction) operator).getFunctionType()
        == SqlFunctionCategory.USER_DEFINED_FUNCTION) {
      final List<SqlOperator> list = Lists.newArrayList();
      opTab.lookupOperatorOverloads(((SqlFunction) operator).getSqlIdentifier(),
          SqlFunctionCategory.USER_DEFINED_FUNCTION, SqlSyntax.FUNCTION, list);
      for (SqlOperator sqlOperator : list) {
        if (sqlOperator.isAggregator()) {
          throw new Util.FoundOne(call);//找到聚合函数,则抛异常
        }
      }
    }
    if (call.isA(SqlKind.QUERY)) {
      // don't traverse into queries
      return null;
    }
    if (call.getKind() == SqlKind.OVER) {
      if (over) {
        throw new Util.FoundOne(call);
      } else {
        // an aggregate function over a window is not an aggregate!
        return null;
      }
    }
    return super.visit(call);
  }
}

// End AggFinder.java
