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
import org.apache.calcite.sql.SqlNode;

/**
 * Implementation of {@link SqlValidatorNamespace} for a field of a record.
 *
 * <p>A field is not a very interesting namespace - except if the field has a
 * record or multiset type - but this class exists to make fields behave
 * similarly to other records for purposes of name resolution.
 * 代表一行数据中的一个字段，为该字段设置一个空间
 */
class FieldNamespace extends AbstractNamespace {
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a FieldNamespace.
   *
   * @param validator Validator
   * @param dataType  Data type of field
   */
  FieldNamespace(
      SqlValidatorImpl validator,
      RelDataType dataType) {
    super(validator, null);
    assert dataType != null;
    this.rowType = dataType;//具体字段类型
  }

  //~ Methods ----------------------------------------------------------------

  //不需要设置类型,因为具体字段类型赢存在了(他是字段,比较特别,从构造函数传递过来)
  public void setType(RelDataType type) {
    throw new UnsupportedOperationException();
  }

  //也不需要实现了,直接返回
  protected RelDataType validateImpl() {
    return rowType;
  }

  //不是sqlNode
  public SqlNode getNode() {
    return null;
  }

  //如果是struct类型,才有子节点
  public SqlValidatorNamespace lookupChild(String name) {
    if (rowType.isStruct()) {
      return validator.lookupFieldNamespace(
          rowType,
          name);
    }
    return null;
  }

  //不存在
  public boolean fieldExists(String name) {
    return false;
  }
}

// End FieldNamespace.java
