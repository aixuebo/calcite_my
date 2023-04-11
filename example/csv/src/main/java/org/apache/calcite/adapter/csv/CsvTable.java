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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.impl.AbstractTable;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Base class for table that reads CSV files.
 */
public abstract class CsvTable extends AbstractTable {
  protected final File file;
  private final RelProtoDataType protoRowType;//类型工厂对象,可以转换成csv文件对应的类型
  protected List<CsvFieldType> fieldTypes;

  /** Creates a CsvAbstractTable. */
  CsvTable(File file, RelProtoDataType protoRowType) {
    this.file = file;
    this.protoRowType = protoRowType;
  }

  /**
   * 有两种方式可以获取csv文件的字段格式。
   * 1.一个是第一行有描述,通过deduceRowType方法解析
   * 2.明确字段的类型,通过构造函数传过来
   */
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (protoRowType != null) {
      return protoRowType.apply(typeFactory);
    }
    if (fieldTypes == null) {
      fieldTypes = new ArrayList<CsvFieldType>();
      return CsvEnumerator.deduceRowType((JavaTypeFactory) typeFactory, file,
          fieldTypes);
    } else {
      return CsvEnumerator.deduceRowType((JavaTypeFactory) typeFactory,
          file,
          null);
    }
  }

  /** Various degrees of table "intelligence". */
  public enum Flavor {
    SCANNABLE, //纯扫描
    FILTERABLE, //带有where条件的过滤扫描
    TRANSLATABLE //数据转换
  }
}

// End CsvTable.java
