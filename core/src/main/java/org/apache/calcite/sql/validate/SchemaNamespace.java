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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/** Namespace based on a schema.
 * 固定schema的基础上的命名空间
 * <p>The visible names are tables and sub-schemas.
 */
class SchemaNamespace extends AbstractNamespace {
  /** The path of this schema. */
  private final ImmutableList<String> names;//schema全路径

  /** Creates a SchemaNamespace. */
  SchemaNamespace(SqlValidatorImpl validator, ImmutableList<String> names) {
    super(validator, null);
    this.names = Preconditions.checkNotNull(names);
  }

  /**
   * 查找该schema下所以的table
   * @return tableName 与 table表结构映射关系
   *
   * TODO 为什么getAllSchemaObjectNames返回的内容有schema和function,而程序在哪里处理的保证只有table呢?不处理schema和function,因为一旦处理会发生空指针异常。
   */
  protected RelDataType validateImpl() {
    final RelDataTypeFactory.FieldInfoBuilder builder =
        validator.getTypeFactory().builder();
    for (SqlMoniker moniker
        : validator.catalogReader.getAllSchemaObjectNames(names)) {//给一个schema的全路径,读取参数schema下的所有子schema、子table、子function
      final List<String> names1 = moniker.getFullyQualifiedNames();
      final SqlValidatorTable table = validator.catalogReader.getTable(names1);
      builder.add(Util.last(names1), table.getRowType());//添加tableName与table表结构
    }
    return builder.build();
  }

  public SqlNode getNode() {
    return null;
  }
}

// End SchemaNamespace.java
