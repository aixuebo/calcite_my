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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.io.File;

/**
 * Table based on a JSON file.
 * 读取一个json文件
 */
public class JsonTable extends AbstractTable implements ScannableTable {
  private final File file;

  /** Creates a JsonTable. */
  JsonTable(File file) {
    this.file = file;
  }

  public String toString() {
    return "JsonTable";
  }

  //表的字段类型  json的形式文件，每一行只包含一列数据
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    //创建一个map类型,因为csv一行是有多列的
    return typeFactory.builder().add("_MAP",
        typeFactory.createMapType(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            typeFactory.createSqlType(SqlTypeName.ANY))).build();
  }

  //读取文件内容，解析成List<Object[]>，即解析成一个表的所有数据。每一行数据是Object
  public Enumerable<Object[]> scan(DataContext root) {
    return new AbstractEnumerable<Object[]>() {
      public Enumerator<Object[]> enumerator() {
        return new JsonEnumerator(file);
      }
    };
  }
}

// End JsonTable.java
