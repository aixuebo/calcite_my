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
package org.apache.calcite.rel.type;

import org.apache.calcite.util.Util;

/**
 * RelDataTypeComparability is an enumeration of the categories of comparison
 * operators which types may support.
 *
 * <p>NOTE jvs 17-Mar-2005: the order of values of this enumeration is
 * significant (from least inclusive to most inclusive) and should not be
 * changed.
 * 比较方式
 */
public enum RelDataTypeComparability {
  NONE("No comparisons allowed"),//不允许比较,作为默认值,表示不能参与比较的列
  UNORDERED("Only equals/not-equals allowed"),//仅允许=或者!=,即不支持排序
  ALL("All comparisons allowed");//允许所有的比较形式

  RelDataTypeComparability(String description) {
    Util.discard(description);
  }
}

// End RelDataTypeComparability.java
