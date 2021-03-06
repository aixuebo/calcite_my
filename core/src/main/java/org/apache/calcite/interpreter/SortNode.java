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
package org.apache.calcite.interpreter;

import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexLiteral;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Interpreter node that implements a
 * {@link org.apache.calcite.rel.core.Sort}.
 * 从数据源读取数据,然后排序操作,再输出
 */
public class SortNode extends AbstractSingleNode<Sort> {
  public SortNode(Interpreter interpreter, Sort rel) {
    super(interpreter, rel);
  }

  public void run() throws InterruptedException {
    final int offset =
        rel.offset == null
            ? 0
            : ((BigDecimal) ((RexLiteral) rel.offset).getValue()).intValue();
    final int fetch =
        rel.fetch == null
            ? -1
            : ((BigDecimal) ((RexLiteral) rel.fetch).getValue()).intValue();
    // In pure limit mode. No sort required.
    Row row;
  loop:
    if (rel.getCollation().getFieldCollations().isEmpty()) {//说明已经排序了
      for (int i = 0; i < offset; i++) {//先读取offset行数据,抛弃掉
        row = source.receive();
        if (row == null) {//数据无数据了
          break loop;
        }
      }

      //不断的读取数据,sink出去
      if (fetch >= 0) {
        for (int i = 0; i < fetch && (row = source.receive()) != null; i++) {
          sink.send(row);
        }
      } else {
        while ((row = source.receive()) != null) {
          sink.send(row);
        }
      }
    } else {
      //先读取数据到内存,然后排序,然后在输出limit数据
      // Build a sorted collection.
      final List<Row> list = Lists.newArrayList();
      while ((row = source.receive()) != null) {
        list.add(row);
      }
      Collections.sort(list, comparator());
      final int end = fetch < 0 || offset + fetch > list.size()
          ? list.size()
          : offset + fetch;
      for (int i = offset; i < end; i++) {
        sink.send(list.get(i));
      }
    }
    sink.end();
  }

  private Comparator<Row> comparator() {
    if (rel.getCollation().getFieldCollations().size() == 1) {//只有一个排序规则
      return comparator(rel.getCollation().getFieldCollations().get(0));
    }
    return Ordering.compound(//生产一组排序规则,按照一组排序规则进行排序数据
        Iterables.transform(rel.getCollation().getFieldCollations(),
            new Function<RelFieldCollation, Comparator<? super Row>>() {
              public Comparator<? super Row> apply(RelFieldCollation input) {
                return comparator(input);
              }
            }));
  }

  private static int compare(Comparable c1, Comparable c2,int nullComparison) {
    if (c1 == c2) {
      return 0;
    } else if (c1 == null) {
      return nullComparison;
    } else if (c2 == null) {
      return -nullComparison;
    } else {
      //noinspection unchecked
      return c1.compareTo(c2);
    }
  }

  private Comparator<Row> comparator(final RelFieldCollation fieldCollation) {
    final int nullComparison = getNullComparison(fieldCollation.nullDirection);//null的排序位置
    switch (fieldCollation.direction) {
    case ASCENDING:
      return new Comparator<Row>() {
        public int compare(Row o1, Row o2) {
          final int x = fieldCollation.getFieldIndex();
          final Comparable c1 = (Comparable) o1.getValues()[x];
          final Comparable c2 = (Comparable) o2.getValues()[x];
          return SortNode.compare(c1, c2, nullComparison);
        }
      };
    default:
      return new Comparator<Row>() {
        public int compare(Row o1, Row o2) {
          final int x = fieldCollation.getFieldIndex();
          final Comparable c1 = (Comparable) o1.getValues()[x];
          final Comparable c2 = (Comparable) o2.getValues()[x];
          return SortNode.compare(c2, c1, -nullComparison);
        }
      };
    }
  }

  private int getNullComparison(RelFieldCollation.NullDirection nullDirection) {
    switch (nullDirection) {
    case FIRST:
      return -1;
    case UNSPECIFIED:
    case LAST:
      return 1;
    default:
      throw new AssertionError(nullDirection);
    }
  }
}

// End SortNode.java
