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
package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

/**
 * An item in a WITH clause of a query.
 * It has a name, an optional column list, and a query.
 * 代表with中的一个item
 *
 * 比如with语法:
 临时表后面跟字段，不过感觉意义不大，暂时可忽略
 with table1 (lable,b) as (
   select label,1 b
   from biao
   where label = 1
 ),
 table2 (lable,b) as (
   select label,2 b
   from biao
   where label = 0
 )
 select label,count(*)
 from
 (
 select * from table1
 union all
 select * from table2
 ) a
 group by label

 一个with后面跟了多个子查询，每一个子查询都是一个SqlWithItem
 SqlWithItem由子查询别名、可选择的列字段集合、查询sql语法组成

 * SimpleIdentifier (xx,xx,xx) 具体表达式
 *
 *
 1.语法—参见with
 2.操作:new SqlWithItemOperator(“WITH_ITEM”, SqlKind.WITH_ITEM)
 3.参数:
 name(SqlIdentifier临时表别名)、columnList(SqlNodeList可选择的列名集合)、query(SqlNode临时表sql)
 4.SqlKind:SqlKind.WITH_ITEM

 */
public class SqlWithItem extends SqlCall {
  public SqlIdentifier name;
  public SqlNodeList columnList; // may be null
  public SqlNode query;

  public SqlWithItem(SqlParserPos pos, SqlIdentifier name,
      SqlNodeList columnList, SqlNode query) {
    super(pos);
    this.name = name;
    this.columnList = columnList;
    this.query = query;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlKind getKind() {
    return SqlKind.WITH_ITEM;
  }

  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name, columnList, query);
  }

  @Override public void setOperand(int i, SqlNode operand) {
    switch (i) {
    case 0:
      name = (SqlIdentifier) operand;
      break;
    case 1:
      columnList = (SqlNodeList) operand;
      break;
    case 2:
      query = operand;
      break;
    default:
      throw new AssertionError(i);
    }
  }

  public SqlOperator getOperator() {
    return SqlWithItemOperator.INSTANCE;
  }

  /**
   * SqlWithItemOperator is used to represent an item in a WITH clause of a
   * query. It has a name, an optional column list, and a query.
   */
  private static class SqlWithItemOperator extends SqlSpecialOperator {
    private static final SqlWithItemOperator INSTANCE =
        new SqlWithItemOperator();

    public SqlWithItemOperator() {
      super("WITH_ITEM", SqlKind.WITH_ITEM, 0);
    }

    //~ Methods ----------------------------------------------------------------

    public void unparse(
        SqlWriter writer,
        SqlCall call,
        int leftPrec,
        int rightPrec) {
      final SqlWithItem withItem = (SqlWithItem) call;
      withItem.name.unparse(writer, getLeftPrec(), getRightPrec());
      if (withItem.columnList != null) {
        withItem.columnList.unparse(writer, getLeftPrec(), getRightPrec());
      }
      writer.keyword("AS");
      withItem.query.unparse(writer, getLeftPrec(), getRightPrec());
    }

    @Override public SqlCall createCall(SqlLiteral functionQualifier,
        SqlParserPos pos, SqlNode... operands) {
      assert functionQualifier == null;
      assert operands.length == 3;
      return new SqlWithItem(pos, (SqlIdentifier) operands[0],
          (SqlNodeList) operands[1], operands[2]);
    }
  }
}

// End SqlWithItem.java
