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

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.sql.type.SqlTypeName;

import java.io.Serializable;
import java.util.List;

/**
 * RelRecordType represents a structured type having named fields.
 * 表示一个java对象组成的一行数据
 */
public class RelRecordType extends RelDataTypeImpl implements Serializable {
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a <code>RecordType</code>. This should only be called from a
   * factory method.
   */
  public RelRecordType(List<RelDataTypeField> fields) {
    super(fields);
    computeDigest();
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelDataType
  public SqlTypeName getSqlTypeName() {
    return SqlTypeName.ROW;
  }

  // implement RelDataType
  public boolean isNullable() {
    return false;
  }

  // implement RelDataType
  public int getPrecision() {
    // REVIEW: angel 18-Aug-2005 Put in fake implementation for precision
    return 0;
  }

  protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    sb.append("RecordType(");
    for (Ord<RelDataTypeField> ord : Ord.zip(fieldList)) {
      if (ord.i > 0) {
        sb.append(", ");
      }
      RelDataTypeField field = ord.e;
      if (withDetail) {
        sb.append(field.getType().getFullTypeString());
      } else {
        sb.append(field.getType().toString());
      }
      sb.append(" ");
      sb.append(field.getName());
    }
    sb.append(")");
  }

  /**
   * Per {@link Serializable} API, provides a replacement object to be written
   * during serialization.
   *
   * <p>This implementation converts this RelRecordType into a
   * SerializableRelRecordType, whose <code>readResolve</code> method converts
   * it back to a RelRecordType during deserialization.
   */
  private Object writeReplace() {
    return new SerializableRelRecordType(fieldList);
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Skinny object which has the same information content as a
   * {@link RelRecordType} but skips redundant stuff like digest and the
   * immutable list.
   */
  private static class SerializableRelRecordType implements Serializable {
    private List<RelDataTypeField> fields;

    private SerializableRelRecordType(List<RelDataTypeField> fields) {
      this.fields = fields;
    }

    /**
     * Per {@link Serializable} API. See
     * {@link RelRecordType#writeReplace()}.
     */
    private Object readResolve() {
      return new RelRecordType(fields);
    }
  }
}

// End RelRecordType.java
