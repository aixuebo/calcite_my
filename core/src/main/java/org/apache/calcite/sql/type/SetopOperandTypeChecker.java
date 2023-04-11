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
package org.apache.calcite.sql.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.validate.SqlValidator;

import java.util.AbstractList;
import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Parameter type-checking strategy for a set operator (UNION, INTERSECT,
 * EXCEPT).
 * set集合相关的参数校验策略,用于UNION, INTERSECT,EXCEPT
 *
 * <p>Both arguments must be records with the same number of fields, and the
 * fields must be union-compatible.
 * 所有的参数必须有相同的列数量。每一个相同的列必须拥有相同的父类型
 *
 * 比如sql1 uinon all sql2的语法校验
 */
public class SetopOperandTypeChecker implements SqlOperandTypeChecker {
  //~ Methods ----------------------------------------------------------------

  public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    //目前集合操作，只支持2个参数做union等操作。
    assert callBinding.getOperandCount() == 2 : "setops are binary (for now)";

    final RelDataType[] argTypes = new RelDataType[callBinding.getOperandCount()];//每一个union子查询的返回值
    int colCount = -1;//列数,用于校验列数必须相同

    final SqlValidator validator = callBinding.getValidator();
    for (int i = 0; i < argTypes.length; i++) {
      final RelDataType argType = argTypes[i] = callBinding.getOperandType(i);//获取参数类型

      //因为是union all操作,所以每一个参数都是一个子查询，因此类型都是一个Struct对象
      if (!argType.isStruct()) {//参数必须是isStruct类型,即包含多个字段
        if (throwOnFailure) {
          throw new AssertionError("setop arg must be a struct");
        } else {
          return false;
        }
      }

      // Each operand must have the same number of columns.
      final List<RelDataTypeField> fields = argType.getFieldList();
      if (i == 0) {
        colCount = fields.size();//获取列数
        continue;
      }

      if (fields.size() != colCount) {//每一个union all的子查询,列数必须相同
        if (throwOnFailure) {
          SqlNode node = callBinding.getCall().operand(i);
          if (node instanceof SqlSelect) {
            node = ((SqlSelect) node).getSelectList();
          }
          throw validator.newValidationError(node,
              RESOURCE.columnCountMismatchInSetop(
                  callBinding.getOperator().getName()));
        } else {
          return false;
        }
      }
    }

    // The columns must be pairwise union compatible. For each column
    // ordinal, form a 'slice' containing the types of the ordinal'th
    // column j.
    for (int i = 0; i < colCount; i++) {//循环每一个列
      final int i2 = i;//第几列
      final RelDataType type = callBinding.getTypeFactory().leastRestrictive(//相同的列必须可以有相同的父类类型,获取该父类类型
              //leastRestrictive参数传入List,返回List公共的类型
              new AbstractList<RelDataType>() {
                public RelDataType get(int index) { //第index个子查询
                  return argTypes[index].getFieldList().get(i2)//获取第index个子查询的第i2列类型
                      .getType();
                }

                public int size() { //一共多少个子查询
                  return argTypes.length;
                }
              });
      if (type == null) {//说明没有相同的父类,所以抛异常,校验失败
        if (throwOnFailure) {
          SqlNode field =
              SqlUtil.getSelectListItem(
                  callBinding.getCall().operand(0),
                  i);
          throw validator.newValidationError(field,
              RESOURCE.columnTypeMismatchInSetop(i + 1, // 1-based
                  callBinding.getOperator().getName()));
        } else {
          return false;
        }
      }
    }

    return true;
  }

  //2个参数
  public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.of(2);
  }

  //sql1 union all sql2
  public String getAllowedSignatures(SqlOperator op, String opName) {
    return "{0} " + opName + " {1}"; // todo: Wael, please review.
  }
}

// End SetopOperandTypeChecker.java
