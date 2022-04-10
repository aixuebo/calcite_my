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
package org.apache.calcite.sql.parser;

import java.io.Reader;

/**
 * Factory for
 * {@link org.apache.calcite.sql.parser.SqlAbstractParserImpl} objects.
 *
 * <p>A parser factory allows you to include a custom parser in
 * {@link org.apache.calcite.tools.Planner} created through
 * {@link org.apache.calcite.tools.Frameworks}.</p>
 */
public interface SqlParserImplFactory {

  /**
   * Get the underlying parser implementation.
   * 传入读取sql reader流,如何生成Node树
   * @return {@link SqlAbstractParserImpl} object.
   */
  SqlAbstractParserImpl getParser(Reader stream);
}

// End SqlParserImplFactory.java
