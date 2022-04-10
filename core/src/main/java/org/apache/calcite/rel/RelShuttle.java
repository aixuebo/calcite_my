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
package org.apache.calcite.rel;

import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;

/**
 * Visitor that has methods for the common logical relational expressions.
 * 处理逻辑表达式
 */
public interface RelShuttle {
  RelNode visit(TableScan scan);//扫描全表

  RelNode visit(TableFunctionScan scan);//使用函数创建rows数据集合

  RelNode visit(LogicalValues values);//属于sql中values,用于生产静态的数据源

  RelNode visit(LogicalFilter filter);//追加where

  RelNode visit(LogicalProject project);//select 投影

  RelNode visit(LogicalJoin join);//join表

  RelNode visit(LogicalCorrelate correlate);//关联查询 select (select id from biao1) from biao2

  RelNode visit(LogicalUnion union);//并集,比如持有一组LogicalProject

  RelNode visit(LogicalIntersect intersect);//交集,比如持有一组LogicalProject

  RelNode visit(LogicalMinus minus);//差值,table1存在 && table2不存在的数据--输出,即table1 - table1与table2的交集,比如持有一组LogicalProject

  RelNode visit(LogicalAggregate aggregate);//相当于group by操作

  RelNode visit(Sort sort);//如何排序,支持order by offset limit语法

  RelNode visit(RelNode other);//其他表达式---基本上没有了
}

// End RelShuttle.java
