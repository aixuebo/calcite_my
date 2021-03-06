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

import org.apache.calcite.rel.core.Union;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import java.util.Set;

/**
 * Interpreter node that implements a
 * {@link org.apache.calcite.rel.core.Union}.
 */
public class UnionNode implements Node {
  private final ImmutableList<Source> sources;//数据源是一组source集合
  private final Sink sink;
  private final Union rel;//union 节点

  public UnionNode(Interpreter interpreter, Union rel) {
    ImmutableList.Builder<Source> builder = ImmutableList.builder();
    for (int i = 0; i < rel.getInputs().size(); i++) {
      builder.add(interpreter.source(rel, i));
    }
    this.sources = builder.build();
    this.sink = interpreter.sink(rel);
    this.rel = rel;
  }

  //如果有union all操作,则全部数据都sink出去,如果无all关键词,则生产set,过滤重复
  public void run() throws InterruptedException {
    final Set<Row> rows = rel.all ? null : Sets.<Row>newHashSet();
    for (Source source : sources) {
      Row row;
      while ((row = source.receive()) != null) {
        if (rows == null || rows.add(row)) {
          sink.send(row);
        }
      }
    }
  }
}

// End UnionNode.java
