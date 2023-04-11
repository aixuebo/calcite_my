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
package org.apache.calcite.adapter.csv;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.sql.SqlKind;

import java.io.File;
import java.util.Iterator;
import java.util.List;

/**
 * Table based on a CSV file that can implement simple filtering.
 *
 * <p>It implements the {@link FilterableTable} interface, so Calcite gets
 * data by calling the {@link #scan(DataContext, List)} method.
 * 相当于where条件的扫描
 */
public class CsvFilterableTable extends CsvTable
    implements FilterableTable {
  /** Creates a CsvFilterableTable. */
  CsvFilterableTable(File file, RelProtoDataType protoRowType) {
    super(file, protoRowType);
  }

  public String toString() {
    return "CsvFilterableTable";
  }

  /**
   *
   * @param root
   * @param filters where条件对应的节点信息
   * @return
   */
  public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
    //用于存储每一个字段要如何过滤信息
    final String[] filterValues = new String[fieldTypes.size()];//如果filterValues某一列有值,则要求行的数据中,该列必须有相同的值

    //解析每一个待过滤的字段对应的信息
    for (final Iterator<RexNode> i = filters.iterator(); i.hasNext();) {
      final RexNode filter = i.next();//返回节点信息
      if (addFilter(filter, filterValues)) {//提取节点信息,即where条件
        i.remove();
      }
    }
    final int[] fields = CsvEnumerator.identityList(fieldTypes.size());
    return new AbstractEnumerable<Object[]>() {
      public Enumerator<Object[]> enumerator() {
        return new CsvEnumerator<Object[]>(file, filterValues,
            new CsvEnumerator.ArrayRowConverter(fieldTypes, fields));
      }
    };
  }

  //将filter节点信息提取出来后,存储到filterValues指定字段位置中
  private boolean addFilter(RexNode filter, Object[] filterValues) {
    if (filter.isA(SqlKind.EQUALS)) {//只解析=逻辑
      final RexCall call = (RexCall) filter;
      RexNode left = call.getOperands().get(0); //获取key
      if (left.isA(SqlKind.CAST)) {
        left = ((RexCall) left).operands.get(0);
      }
      final RexNode right = call.getOperands().get(1); //获取value
      if (left instanceof RexInputRef //列的引用
          && right instanceof RexLiteral) {//值
        final int index = ((RexInputRef) left).getIndex(); //字段key对应的序号
        if (filterValues[index] == null) {//为该列赋值
          filterValues[index] = ((RexLiteral) right).getValue2().toString();
          return true;
        }
      }
    }
    return false;
  }
}

// End CsvFilterableTable.java
