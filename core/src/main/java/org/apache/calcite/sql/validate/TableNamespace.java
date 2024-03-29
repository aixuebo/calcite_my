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
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlNode;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.List;

/** Namespace based on a table from the catalog. 一个表的空间
 * from user u join student s
 * 此时分别代表user或者student表，即已经解析了user，并且查询了catalog
 *
 *
 * 表空间，描述了读取的是哪个表，以及该表返回了哪些字段
 **/
class TableNamespace extends AbstractNamespace {
  private final SqlValidatorTable table;//对应的user表
  public final ImmutableList<RelDataTypeField> extendedFields;//大多数场景都是null,即不需要有扩展属性，都是从元数据获取

  /** Creates a TableNamespace. */
  TableNamespace(SqlValidatorImpl validator, SqlValidatorTable table,
      ImmutableList<RelDataTypeField> fields) {
    super(validator, null);
    this.table = Preconditions.checkNotNull(table);
    this.extendedFields = fields;
  }

  public TableNamespace(SqlValidatorImpl validator, SqlValidatorTable table) {
    this(validator, table, ImmutableList.<RelDataTypeField>of());
  }

  //直接返回表的所有列集对象集合
  protected RelDataType validateImpl() {
    if (extendedFields.isEmpty()) {
      return table.getRowType();
    }
    final RelDataTypeFactory.FieldInfoBuilder builder =
        validator.getTypeFactory().builder();
    builder.addAll(table.getRowType().getFieldList());
    builder.addAll(extendedFields);
    return builder.build();
  }

  public SqlNode getNode() {
    // This is the only kind of namespace not based on a node in the parse tree.
    return null;
  }

  @Override public SqlValidatorTable getTable() {
    return table;
  }

  /** Creates a TableNamespace based on the same table as this one, but with
   * extended fields.
   *
   * <p>Extended fields are "hidden" or undeclared fields that may nevertheless
   * be present if you ask for them. Phoenix uses them, for instance, to access
   * rarely used fields in the underlying HBase table.
   * 继续追加列
   **/
  public TableNamespace extend(List<RelDataTypeField> extendedFields) {
    return new TableNamespace(validator, table,
        ImmutableList.copyOf(
            Iterables.concat(this.extendedFields, extendedFields)));
  }
}

// End TableNamespace.java
