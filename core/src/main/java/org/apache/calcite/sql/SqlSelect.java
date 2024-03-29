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
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

/**
 * A <code>SqlSelect</code> is a node of a parse tree which represents a select
 * statement. It warrants its own node type just because we have a lot of
 * methods to put somewhere.
 * 1.语法:
 * 2.操作:SqlSelectOperator
 * 3.参数:
 * keywordList(SqlNodeList关键词参数,比如distinct)、selectList(SqlNodeList)、from(SqlNode)、where(SqlNode)、
 * groupBy(SqlNodeList)、having(SqlNode)、windowDecls(SqlNodeList)、orderBy(SqlNodeList)、offset(SqlNode)、fetch(SqlNode)
 * 4.SqlKind:SqlKind.SELECT
 */
public class SqlSelect extends SqlCall {
  //~ Static fields/initializers ---------------------------------------------

  // constants representing operand positions
  public static final int FROM_OPERAND = 2;//参数的位置,比如from是参数第2个位置,从0开始计数
  public static final int WHERE_OPERAND = 3;
  public static final int HAVING_OPERAND = 5;

  SqlNodeList keywordList;//存储distinct关键词
  SqlNodeList selectList;
  SqlNode from;
  SqlNode where;
  SqlNodeList groupBy;
  SqlNode having;
  SqlNodeList windowDecls;
  SqlNodeList orderBy;
  SqlNode offset;
  SqlNode fetch;

  //~ Constructors -----------------------------------------------------------

  public SqlSelect(SqlParserPos pos,
      SqlNodeList keywordList,
      SqlNodeList selectList,
      SqlNode from,
      SqlNode where,
      SqlNodeList groupBy,
      SqlNode having,
      SqlNodeList windowDecls,
      SqlNodeList orderBy,
      SqlNode offset,
      SqlNode fetch) {
    super(pos);
    this.keywordList = keywordList != null ? keywordList : new SqlNodeList(pos);
    this.selectList = selectList;
    this.from = from;
    this.where = where;
    this.groupBy = groupBy;
    this.having = having;
    this.windowDecls = windowDecls != null ? windowDecls : new SqlNodeList(pos);
    this.orderBy = orderBy;
    this.offset = offset;
    this.fetch = fetch;
  }

  //~ Methods ----------------------------------------------------------------

  public SqlOperator getOperator() {
    return SqlSelectOperator.INSTANCE;
  }

  @Override public SqlKind getKind() {
    return SqlKind.SELECT;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(keywordList, selectList, from, where,
        groupBy, having, windowDecls, orderBy, offset, fetch);
  }

  @Override public void setOperand(int i, SqlNode operand) {
    switch (i) {
    case 0:
      keywordList = (SqlNodeList) operand;
      break;
    case 1:
      selectList = (SqlNodeList) operand;
      break;
    case 2:
      from = operand;
      break;
    case 3:
      where = operand;
      break;
    case 4:
      groupBy = (SqlNodeList) operand;
      break;
    case 5:
      having = operand;
      break;
    case 6:
      windowDecls = (SqlNodeList) operand;
      break;
    case 7:
      orderBy = (SqlNodeList) operand;
      break;
    case 8:
      offset = operand;
      break;
    case 9:
      fetch = operand;
      break;
    default:
      throw new AssertionError(i);
    }
  }

  //是否包含distinct关键词
  public final boolean isDistinct() {
    return getModifierNode(SqlSelectKeyword.DISTINCT) != null;
  }

  public final SqlNode getModifierNode(SqlSelectKeyword modifier) {
    for (SqlNode keyword : keywordList) {
      SqlSelectKeyword keyword2 = ((SqlLiteral) keyword).symbolValue();
      if (keyword2 == modifier) {
        return keyword;
      }
    }
    return null;
  }

  public final SqlNode getFrom() {
    return from;
  }

  public void setFrom(SqlNode from) {
    this.from = from;
  }

  public final SqlNodeList getGroup() {
    return groupBy;
  }

  public void setGroupBy(SqlNodeList groupBy) {
    this.groupBy = groupBy;
  }

  public final SqlNode getHaving() {
    return having;
  }

  public final SqlNodeList getSelectList() {
    return selectList;
  }

  public void setSelectList(SqlNodeList selectList) {
    this.selectList = selectList;
  }

  public final SqlNode getWhere() {
    return where;
  }

  public void setWhere(SqlNode whereClause) {
    this.where = whereClause;
  }

  public final SqlNodeList getWindowList() {
    return windowDecls;
  }

  public final SqlNodeList getOrderList() {
    return orderBy;
  }

  public void setOrderBy(SqlNodeList orderBy) {
    this.orderBy = orderBy;
  }

  public final SqlNode getOffset() {
    return offset;
  }

  public void setOffset(SqlNode offset) {
    this.offset = offset;
  }

  public final SqlNode getFetch() {
    return fetch;
  }

  public void setFetch(SqlNode fetch) {
    this.fetch = fetch;
  }

  public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validateQuery(this, scope);
  }

  // Override SqlCall, to introduce a subquery frame.
  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    if (!writer.inQuery()) {
      // If this SELECT is the topmost item in a subquery, introduce a new
      // frame. (The topmost item in the subquery might be a UNION or
      // ORDER. In this case, we don't need a wrapper frame.)
      final SqlWriter.Frame frame =
          writer.startList(SqlWriter.FrameTypeEnum.SUB_QUERY, "(", ")");
      getOperator().unparse(writer, this, 0, 0);
      writer.endList(frame);
    } else {
      getOperator().unparse(writer, this, leftPrec, rightPrec);
    }
  }

  public boolean hasOrderBy() {
    return orderBy != null && orderBy.size() != 0;
  }

  public boolean hasWhere() {
    return where != null;
  }

  public boolean isKeywordPresent(SqlSelectKeyword targetKeyWord) {
    return getModifierNode(targetKeyWord) != null;
  }
}

// End SqlSelect.java
