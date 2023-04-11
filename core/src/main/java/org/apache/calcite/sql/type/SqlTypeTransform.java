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
import org.apache.calcite.sql.SqlOperatorBinding;

/**
 * Strategy to transform one type to another. The transformation is dependent on
 * the implemented strategy object and in the general case is a function of the
 * type and the other operands. Can not be used by itself. Must be used in an
 * object of type {@link SqlTypeTransformCascade}.
 *
 * <p>This class is an example of the
 * {@link org.apache.calcite.util.Glossary#STRATEGY_PATTERN strategy pattern}.
 * 类型转换,从一个类型 转换成另外一个类型。
 */
public interface SqlTypeTransform {
  //~ Methods ----------------------------------------------------------------

  /**
   * Transforms a type.
   * 类型转换，最终转换成typeToTransform类型
   *
   * @param opBinding       call context in which transformation is being
   *                        performed 可以获取每一个参数的真实类型
   * @param typeToTransform type to be transformed, never null 目标结果类型
   * @return transformed type, never null 返回转换后的类型,一定不允许是null
   */
  RelDataType transformType(SqlOperatorBinding opBinding,RelDataType typeToTransform);

}

// End SqlTypeTransform.java
