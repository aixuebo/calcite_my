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
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Abstract base for a scope which is defined by a list of child namespaces and
 * which inherits from a parent scope.
 * 持有多个子表、或者 子查询的sql,比如一顿join多个表 或者 select 多个字段
 */
public abstract class ListScope extends DelegatingScope {
  //~ Instance fields --------------------------------------------------------

  /**
   * List of child {@link SqlValidatorNamespace} objects and their names.
   * 表示子查询的别名与子查询表之间的映射关系,即每一个元素都是一个子查询表。。。所有的子查询表都共存在一个scope中
   */
  protected final List<Pair<String, SqlValidatorNamespace>> children = new ArrayList<Pair<String, SqlValidatorNamespace>>();

  //~ Constructors -----------------------------------------------------------

  public ListScope(SqlValidatorScope parent) {
    super(parent);
  }

  //~ Methods ----------------------------------------------------------------

  public void addChild(SqlValidatorNamespace ns, String alias) {
    assert alias != null;
    children.add(Pair.of(alias, ns));
  }

  /**
   * Returns an immutable list of child namespaces.
   *
   * @return list of child namespaces
   */
  public List<SqlValidatorNamespace> getChildren() {
    return Pair.right(children);
  }

  //通过别名找空间
  protected SqlValidatorNamespace getChild(String alias) {
    if (alias == null) {
      if (children.size() != 1) {
        throw Util.newInternal(
            "no alias specified, but more than one table in from list");
      }
      return children.get(0).right;
    } else {
      final int i = validator.catalogReader.match(Pair.left(children), alias);//找到别名是所有子节点第几个
      if (i >= 0) {
        return children.get(i).right;//直接返回空间
      }
      return null;
    }
  }

  /** Returns a child namespace that matches a fully-qualified list of names.
   * This will be a schema-qualified table, for example
   *
   * <blockquote><pre>SELECT sales.emp.empno FROM sales.emp</pre></blockquote>
   * 通过别名全称找空间
   */
  protected SqlValidatorNamespace getChild(List<String> names) {
    int i = findChild(names);
    return i < 0 ? null : children.get(i).right;
  }

  //返回第几个子节点是参数name对应的表
  protected int findChild(List<String> names) {
    for (int i = 0; i < children.size(); i++) {
      Pair<String, SqlValidatorNamespace> child = children.get(i);
      if (child.left != null) {
        if (validator.catalogReader.matches(child.left, Util.last(names))) {//说明找到left与name相同的,继续向上找,即找schema
          if (names.size() == 1) {//直接返回
            return i;
          }
        } else {
          // Alias does not match last segment. Don't consider the
          // fully-qualified name. E.g.
          //    SELECT sales.emp.name FROM sales.emp AS otherAlias
          continue;
        }
      }

      // Look up the 2 tables independently, in case one is qualified with
      // catalog & schema and the other is not.
      final SqlValidatorTable table = child.right.getTable();
      if (table != null) {
        final SqlValidatorTable table2 =
            validator.catalogReader.getTable(names);
        if (table2 != null
            && table.getQualifiedName().equals(table2.getQualifiedName())) {
          return i;
        }
      }
    }
    return -1;
  }

  //将所有命名空间内所有的输出字段--追加到result中,存储每一个列信息
  public void findAllColumnNames(List<SqlMoniker> result) {
    for (Pair<String, SqlValidatorNamespace> pair : children) {//循环每一个表
      addColumnNames(pair.right, result);//将命名空间内所有的输出字段--追加到colNames中
    }
    parent.findAllColumnNames(result);//继续追加父类的字段
  }

  //将每一个表与别名关系，插入到参数中
  public void findAliases(Collection<SqlMoniker> result) {
    for (Pair<String, SqlValidatorNamespace> pair : children) {
      result.add(new SqlMonikerImpl(pair.left, SqlMonikerType.TABLE));
    }
    parent.findAliases(result);
  }

  /**
   * 通过列名,找到包含列的 别名以及子查询对象或者表
   * @return <别名,子查询表空间>
   */
  public Pair<String, SqlValidatorNamespace> findQualifyingTableName(final String columnName, SqlNode ctx) {
    int count = 0;
    Pair<String, SqlValidatorNamespace> tableName = null;
    for (Pair<String, SqlValidatorNamespace> child : children) {//循环子表
      final RelDataType rowType = child.right.getRowType();//子表字段集合
      if (validator.catalogReader.field(rowType, columnName) != null) {//子表包含该字段
        tableName = child;
        count++;
      }
    }
    switch (count) {//没有找到
    case 0:
      return parent.findQualifyingTableName(columnName, ctx);//父类继续查找
    case 1:
      return tableName;
    default:
      throw validator.newValidationError(ctx,
          RESOURCE.columnAmbiguous(columnName));//命名冲突
    }
  }

  //找到别名对应的空间--空间可以代表具体的表
  public SqlValidatorNamespace resolve(
      List<String> names,//table的别名全路径
      SqlValidatorScope[] ancestorOut,
      int[] offsetOut) {
    // First resolve by looking through the child namespaces.
    final int i = findChild(names);//找到某一个子节点
    if (i >= 0) {
      if (ancestorOut != null) {
        ancestorOut[0] = this;
      }
      if (offsetOut != null) {
        offsetOut[0] = i;
      }
      return children.get(i).right;
    }

    // Then call the base class method, which will delegate to the
    // parent scope.
    return parent.resolve(names, ancestorOut, offsetOut);
  }

  //通过列名找列的类型---列名需要在各个子查询的命名空间中查找
  public RelDataType resolveColumn(String columnName, SqlNode ctx) {
    int found = 0;
    RelDataType type = null;
    for (Pair<String, SqlValidatorNamespace> pair : children) {
      SqlValidatorNamespace childNs = pair.right;//空间---真实数据结构
      final RelDataType childRowType = childNs.getRowType();
      final RelDataTypeField field =
          validator.catalogReader.field(childRowType, columnName);//是否有该字段
      if (field != null) {
        found++;
        type = field.getType();
      }
    }
    switch (found) {
    case 0:
      return null;//没找到该name
    case 1:
      return type;
    default:
      throw validator.newValidationError(ctx,
          RESOURCE.columnAmbiguous(columnName));//名字冲突了
    }
  }
}

// End ListScope.java
