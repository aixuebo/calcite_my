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
package org.apache.calcite.jdbc;

import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;

import java.util.List;

/**
 * Record type based on a Java class. The fields of the type are the fields
 * of the class.
 *
 * <p><strong>NOTE: This class is experimental and subject to
 * change/removal without notice</strong>.</p>
 * 一个java对象表示一行数据，相当于struck对象
 */
public class JavaRecordType extends RelRecordType {
  final Class clazz;//java对象

  public JavaRecordType(List<RelDataTypeField> fields, Class clazz) {
    super(fields);//每一个字段
    this.clazz = clazz;
    assert clazz != null;
  }

  @Override public boolean equals(Object obj) {
    return this == obj
        || obj instanceof JavaRecordType
        && fieldList.equals(((JavaRecordType) obj).fieldList)
        && clazz == ((JavaRecordType) obj).clazz;
  }

  @Override public int hashCode() {
    return fieldList.hashCode() ^ clazz.hashCode();
  }
}

// End JavaRecordType.java
