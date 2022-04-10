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
import org.apache.calcite.sql.SqlAccessType;

import java.util.List;

/**
 * Supplies a {@link SqlValidator} with the metadata for a table.
 *
 * @see SqlValidatorCatalogReader
 * 用于校验时，获取一个table的元数据信息--比如表可以访问哪些权限(增删改查)
 */
public interface SqlValidatorTable {
  //~ Methods ----------------------------------------------------------------

  RelDataType getRowType();//表的schema数据结构

  List<String> getQualifiedName();//表的全路径

  /**
   * Returns whether a given column is monotonic.返回字段的单调性 ---默认显示每一个列是非单调的 NOT_MONOTONIC
   */
  SqlMonotonicity getMonotonicity(String columnName);

  /**
   * Returns the access type of the table
   * 访问该表的权限---增删改查
   */
  SqlAccessType getAllowedAccess();
}

// End SqlValidatorTable.java
