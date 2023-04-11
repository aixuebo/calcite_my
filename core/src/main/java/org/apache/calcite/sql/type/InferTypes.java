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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlNode;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Strategies for inferring operand types.
 * 推测参数类型工厂类
 * @see org.apache.calcite.sql.type.SqlOperandTypeInference
 * @see org.apache.calcite.sql.type.ReturnTypes
 */
public abstract class InferTypes {
  private InferTypes() {}

  /**
   * Operand type-inference strategy where an unknown operand type is derived
   * from the first operand with a known type.
   * 返回参数类型为第一个识别的类型，作为推测的类型
   */
  public static final SqlOperandTypeInference FIRST_KNOWN =
      new SqlOperandTypeInference() {
        public void inferOperandTypes(
            SqlCallBinding callBinding,
            RelDataType returnType,
            RelDataType[] operandTypes) {
          final RelDataType unknownType = callBinding.getValidator().getUnknownType();//初始化未知类型,比如string
          RelDataType knownType = unknownType;
          for (SqlNode operand : callBinding.getCall().getOperandList()) {//循环每一个参数,推测他的值类型
            knownType = callBinding.getValidator().deriveType(
                callBinding.getScope(), operand);
            if (!knownType.equals(unknownType)) {//找到第一个非初始化类型,则将其设置为推测类型
              break;
            }
          }

          // REVIEW jvs 11-Nov-2008:  We can't assert this
          // because SqlAdvisorValidator produces
          // unknown types for incomplete expressions.
          // Maybe we need to distinguish the two kinds of unknown.
          //assert !knownType.equals(unknownType);
          for (int i = 0; i < operandTypes.length; ++i) {
            operandTypes[i] = knownType;
          }
        }
      };

  /**
   * Operand type-inference strategy where an unknown operand type is derived
   * from the call's return type. If the return type is a record, it must have
   * the same number of fields as the number of operands.
   * 将类型设置成return类型
   */
  public static final SqlOperandTypeInference RETURN_TYPE =
      new SqlOperandTypeInference() {
        public void inferOperandTypes(
            SqlCallBinding callBinding,
            RelDataType returnType,
            RelDataType[] operandTypes) {
          for (int i = 0; i < operandTypes.length; ++i) {
            operandTypes[i] =
                returnType.isStruct()
                    ? returnType.getFieldList().get(i).getType() //如果return类型是明确的对象，他表示为每一个参数都设置了一个类型
                    : returnType;//参数类型统一设置为returnType
          }
        }
      };

  /**
   * Operand type-inference strategy where an unknown operand type is assumed
   * to be boolean.
   * 将参数类型设置成boolean类型
   */
  public static final SqlOperandTypeInference BOOLEAN =
      new SqlOperandTypeInference() {
        public void inferOperandTypes(
            SqlCallBinding callBinding,
            RelDataType returnType,
            RelDataType[] operandTypes) {
          RelDataTypeFactory typeFactory = callBinding.getTypeFactory();
          for (int i = 0; i < operandTypes.length; ++i) {
            operandTypes[i] =
                typeFactory.createSqlType(SqlTypeName.BOOLEAN);
          }
        }
      };

  /**
   * Operand type-inference strategy where an unknown operand type is assumed
   * to be VARCHAR(1024).  This is not something which should be used in most
   * cases (especially since the precision is arbitrary), but for IS [NOT]
   * NULL, we don't really care about the type at all, so it's reasonable to
   * use something that every other type can be cast to.
   * 将参数类型设置成字符串类型
   */
  public static final SqlOperandTypeInference VARCHAR_1024 =
      new SqlOperandTypeInference() {
        public void inferOperandTypes(
            SqlCallBinding callBinding,
            RelDataType returnType,
            RelDataType[] operandTypes) {
          RelDataTypeFactory typeFactory = callBinding.getTypeFactory();
          for (int i = 0; i < operandTypes.length; ++i) {
            operandTypes[i] =
                typeFactory.createSqlType(SqlTypeName.VARCHAR, 1024);
          }
        }
      };

  /** Returns an {@link SqlOperandTypeInference} that returns a given list of
   * types. 精准投放类型*/
  public static SqlOperandTypeInference explicit(List<RelDataType> types) {
    return new ExplicitOperandTypeInference(ImmutableList.copyOf(types));
  }
}

// End InferTypes.java
