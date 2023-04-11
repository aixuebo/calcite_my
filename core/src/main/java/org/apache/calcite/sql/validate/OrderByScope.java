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
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;

import java.util.List;

/**
 * Represents the name-resolution context for expressions in an ORDER BY clause.
 *
 * <p>In some dialects of SQL, the ORDER BY clause can reference column aliases
 * in the SELECT clause. For example, the query</p>
 *
 * <blockquote><code>SELECT empno AS x<br>
 * FROM emp<br>
 * ORDER BY x</code></blockquote>
 *
 * <p>is valid.</p>
 *
 * 因为order by 的字段是别名,所以需要从命名空间查找
 */
public class OrderByScope extends DelegatingScope {
  //~ Instance fields --------------------------------------------------------

  //order by 语法包含select和order by
  private final SqlNodeList orderList;
  private final SqlSelect select;

  //~ Constructors -----------------------------------------------------------

  OrderByScope(
      SqlValidatorScope parent,
      SqlNodeList orderList,
      SqlSelect select) {
    super(parent);
    this.orderList = orderList;
    this.select = select;
  }

  //~ Methods ----------------------------------------------------------------

  public SqlNode getNode() {
    return orderList;
  }

  //输出select的输出字段到result中
  public void findAllColumnNames(List<SqlMoniker> result) {
    final SqlValidatorNamespace ns = validator.getNamespace(select);
    addColumnNames(ns, result);
  }

  public SqlQualified fullyQualify(SqlIdentifier identifier) {
    // If it's a simple identifier, look for an alias.
    if (identifier.isSimple()
        && validator.getConformance().isSortByAlias()) {
      String name = identifier.names.get(0);
      final SqlValidatorNamespace selectNs =
          validator.getNamespace(select);
      final RelDataType rowType = selectNs.getRowType();
      if (validator.catalogReader.field(rowType, name) != null) {
        return SqlQualified.create(this, 1, selectNs, identifier);
      }
    }
    return super.fullyQualify(identifier);
  }

  //获取某一个列的类型 --- 该name一定是select中的某一个字段
  public RelDataType resolveColumn(String name, SqlNode ctx) {
    final SqlValidatorNamespace selectNs = validator.getNamespace(select);//select的输出表空间
    final RelDataType rowType = selectNs.getRowType();//表字段类型
    final RelDataTypeField field = validator.catalogReader.field(rowType, name);//找到列对象 -- 该name一定是select中的某一个字段
    if (field != null) {
      return field.getType();
    }
    final SqlValidatorScope selectScope = validator.getSelectScope(select);
    return selectScope.resolveColumn(name, ctx);
  }

  public void validateExpr(SqlNode expr) {
    SqlNode expanded = validator.expandOrderExpr(select, expr);

    // expression needs to be valid in parent scope too
    parent.validateExpr(expanded);
  }
}

// End OrderByScope.java
