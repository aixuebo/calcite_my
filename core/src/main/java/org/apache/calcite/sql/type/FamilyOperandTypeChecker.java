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

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUtil;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Operand type-checking strategy which checks operands for inclusion in type
 * families.
 * 校验参数策略:必须是给定类型中 依次对应的关系
 */
public class FamilyOperandTypeChecker implements SqlSingleOperandTypeChecker {
  //~ Instance fields --------------------------------------------------------

  protected final ImmutableList<SqlTypeFamily> families;//需要支持哪些字段类型,一一对应的关系

  //~ Constructors -----------------------------------------------------------

  /**
   * Package private. Create using {@link OperandTypes#family}.
   */
  FamilyOperandTypeChecker(List<SqlTypeFamily> families) {
    this.families = ImmutableList.copyOf(families);
  }

  //~ Methods ----------------------------------------------------------------

  // implement SqlSingleOperandTypeChecker
  //校验某一个参数
  public boolean checkSingleOperandType(
      SqlCallBinding callBinding,
      SqlNode node,
      int iFormalOperand,
      boolean throwOnFailure) {
    SqlTypeFamily family = families.get(iFormalOperand);//获取要求的参数类型
    if (family == SqlTypeFamily.ANY) {
      // no need to check
      return true;
    }
    if (SqlUtil.isNullLiteral(node, false)) {//如果是null,则不允许
      if (throwOnFailure) {
        throw callBinding.getValidator().newValidationError(node,
            RESOURCE.nullIllegal());
      } else {
        return false;
      }
    }
    RelDataType type = callBinding.getValidator().deriveType(callBinding.getScope(), node);//返回真实该节点的类型

    SqlTypeName typeName = type.getSqlTypeName();

    // Pass type checking for operators if it's of type 'ANY'.
    if (typeName.getFamily() == SqlTypeFamily.ANY) {
      return true;
    }

    if (!family.getTypeNames().contains(typeName)) {
      if (throwOnFailure) {
        throw callBinding.newValidationSignatureError();
      }
      return false;
    }
    return true;
  }

  // implement SqlOperandTypeChecker
  public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    if (families.size() != callBinding.getOperandCount()) {//参数数量不对
      // assume this is an inapplicable sub-rule of a composite rule;
      // don't throw
      return false;
    }

    for (Ord<SqlNode> op : Ord.zip(callBinding.getCall().getOperandList())) {//一个一个参数校验
      if (!checkSingleOperandType(
          callBinding,
          op.e,
          op.i,
          throwOnFailure)) {
        return false;
      }
    }
    return true;
  }

  // implement SqlOperandTypeChecker 需要的参数数量取决于families的数量
  public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.of(families.size());
  }

  // implement SqlOperandTypeChecker
  //families 确定了每一个参数的类型
  public String getAllowedSignatures(SqlOperator op, String opName) {
    return SqlUtil.getAliasedSignature(op, opName, families);
  }
}

// End FamilyOperandTypeChecker.java
