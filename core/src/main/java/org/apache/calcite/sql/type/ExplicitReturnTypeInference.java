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
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.SqlOperatorBinding;

/**
 * A {@link SqlReturnTypeInference} which always returns the same SQL type.
 * 明确的返回一个类型--用于自定义返回类型
 */
public class ExplicitReturnTypeInference implements SqlReturnTypeInference {
  //~ Instance fields --------------------------------------------------------

  protected final RelProtoDataType protoType;//Function1<RelDataTypeFactory, RelDataType> 给定一个工厂,根据工厂返回需要的类型

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an inference rule which always returns the same type object.
   *
   * <p>If the requesting type factory is different, returns a copy of the
   * type object made using {@link RelDataTypeFactory#copyType(RelDataType)}
   * within the requesting type factory.
   *
   * <p>A copy of the type is required because each statement is prepared using
   * a different type factory; each type factory maintains its own cache of
   * canonical instances of each type.
   *
   * @param protoType Type object
   * 参数是传入的是已经明确的类型了,比如你给我一个工厂,我就能造出来我想要的返回类型
   */
  protected ExplicitReturnTypeInference(RelProtoDataType protoType) {
    assert protoType != null;
    this.protoType = protoType;
  }

  //~ Methods ----------------------------------------------------------------

  //根据工厂自己造返回类型
  public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    return protoType.apply(opBinding.getTypeFactory());
  }
}

// End ExplicitReturnTypeInference.java
