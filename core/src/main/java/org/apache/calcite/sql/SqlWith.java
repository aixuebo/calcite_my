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
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * The WITH clause of a query. It wraps a SELECT, UNION, or INTERSECT.
 * with语句
 1.语法:
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

 2.操作:new SqlWithOperator(“with”, SqlKind.WITH)
 3.参数:
 withList(SqlNodeList),sqlBody(SqlNode)
 由with语法以及具体查询sql组成。
 4.SqlKind:SqlKind.WITH
 */
public class SqlWith extends SqlCall {
  public final SqlNodeList withList;//持有SqlWithItem集合,即执行每一个子查询
  public final SqlNode body;//查询语法,该语法会用到withList中的子查询结果

  //~ Constructors -----------------------------------------------------------

  public SqlWith(SqlParserPos pos, SqlNodeList withList, SqlNode body) {
    super(pos);
    this.withList = withList;
    this.body = body;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlKind getKind() {
    return SqlKind.WITH;
  }

  @Override public SqlOperator getOperator() {
    return SqlWithOperator.INSTANCE;
  }

  public List<SqlNode> getOperandList() {
    return ImmutableList.of(withList, body);
  }

  @Override public void validate(SqlValidator validator,
      SqlValidatorScope scope) {
    validator.validateWith(this, scope);
  }

  /**
   * SqlWithOperator is used to represent a WITH clause of a query. It wraps
   * a SELECT, UNION, or INTERSECT.
   */
  private static class SqlWithOperator extends SqlSpecialOperator {
    private static final SqlWithOperator INSTANCE = new SqlWithOperator();

    private SqlWithOperator() {
      // NOTE:  make precedence lower then SELECT to avoid extra parens
      super("WITH", SqlKind.WITH, 2);
    }

    //~ Methods ----------------------------------------------------------------

    public void unparse(
        SqlWriter writer,
        SqlCall call,
        int leftPrec,
        int rightPrec) {
      final SqlWith with = (SqlWith) call;
      final SqlWriter.Frame frame =
          writer.startList(SqlWriter.FrameTypeEnum.WITH, "WITH", "");
      final SqlWriter.Frame frame1 = writer.startList("", "");
      for (SqlNode node : with.withList) {
        writer.sep(",");
        node.unparse(writer, 0, 0);
      }
      writer.endList(frame1);
      with.body.unparse(writer, 0, 0);
      writer.endList(frame);
    }


    @Override public SqlCall createCall(SqlLiteral functionQualifier,
        SqlParserPos pos, SqlNode... operands) {
      return new SqlWith(pos, (SqlNodeList) operands[0], operands[1]);
    }

    @Override public void validateCall(SqlCall call,
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlValidatorScope operandScope) {
      validator.validateWith((SqlWith) call, scope);
    }
  }
}

// End SqlWith.java
