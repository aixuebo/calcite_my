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

/**
 * An enumeration of moniker types.
 * sql绰号类型--标识一个绰号是为什么类型起的
 *
 * <p>Used in {@link SqlMoniker}.
 * 是列、表、视图、schema、还是函数、关键词
 */
public enum SqlMonikerType {
  COLUMN, TABLE, VIEW, SCHEMA, CATALOG, REPOSITORY, FUNCTION, KEYWORD
}

// End SqlMonikerType.java
