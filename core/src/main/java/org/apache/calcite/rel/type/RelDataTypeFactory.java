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

import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.type.SqlTypeName;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * RelDataTypeFactory is a factory for datatype descriptors. It defines methods
 * for instantiating and combining SQL, Java, and collection types. The factory
 * also provides methods for return type inference for arithmetic in cases where
 * SQL 2003 is implementation defined or impractical.
 *
 * <p>This interface is an example of the
 * {@link org.apache.calcite.util.Glossary#ABSTRACT_FACTORY_PATTERN abstract factory pattern}.
 * Any implementation of <code>RelDataTypeFactory</code> must ensure that type
 * objects are canonical: two types are equal if and only if they are
 * represented by the same Java object. This reduces memory consumption and
 * comparison cost.
 *
 * 转换成java对象，在内存中使用该对象存储数据
 * 数据库类型转换成java类型
 */
public interface RelDataTypeFactory {
  //~ Methods ----------------------------------------------------------------

  /**
   * Returns the type system.
   *
   * @return Type system
   */
  RelDataTypeSystem getTypeSystem();

  /**
   * Creates a type which corresponds to a Java class.
   *
   * @param clazz the Java class used to define the type
   * @return canonical Java type descriptor
   */
  RelDataType createJavaType(Class clazz);

  /**
   * Creates a cartesian product type.
   *
   * @return canonical join type descriptor
   * @param types array of types to be joined
   * 用于A left join B时,传入的参数是A类型和B类型的对象，两个对象都是object类型,因为都是子查询,包含了很多个列
   */
  RelDataType createJoinType(RelDataType... types);

  /**
   * Creates a type which represents a structured collection of fields, given
   * lists of the names and types of the fields.
   *
   * @param typeList      types of the fields
   * @param fieldNameList names of the fields
   * @return canonical struct type descriptor
   */
  RelDataType createStructType(
      List<RelDataType> typeList,
      List<String> fieldNameList);

  /**
   * Creates a type which represents a structured collection of fields,
   * obtaining the field information via a callback.
   *
   * @param fieldInfo callback for field information
   * @return canonical struct type descriptor
   */
  RelDataType createStructType(FieldInfo fieldInfo);

  /**
   * Creates a type which represents a structured collection of fieldList,
   * obtaining the field information from a list of (name, type) pairs.
   *
   * @param fieldList List of (name, type) pairs
   * @return canonical struct type descriptor
   */
  RelDataType createStructType(
      List<? extends Map.Entry<String, RelDataType>> fieldList);

  /**
   * Creates an array type. Arrays are ordered collections of elements.
   *
   * @param elementType    type of the elements of the array
   * @param maxCardinality maximum array size, or -1 for unlimited
   * @return canonical array type descriptor
   */
  RelDataType createArrayType(
      RelDataType elementType,
      long maxCardinality);

  /**
   * Creates a map type. Maps are unordered collections of key/value pairs.
   *
   * @param keyType   type of the keys of the map
   * @param valueType type of the values of the map
   * @return canonical map type descriptor
   */
  RelDataType createMapType(
      RelDataType keyType,
      RelDataType valueType);

  /**
   * Creates a multiset type. Multisets are unordered collections of elements.
   *
   * @param elementType    type of the elements of the multiset
   * @param maxCardinality maximum collection size, or -1 for unlimited
   * @return canonical multiset type descriptor
   */
  RelDataType createMultisetType(
      RelDataType elementType,
      long maxCardinality);

  /**
   * Duplicates a type, making a deep copy. Normally, this is a no-op, since
   * canonical type objects are returned. However, it is useful when copying a
   * type from one factory to another.
   *
   * @param type input type
   * @return output type, a new object equivalent to input type
   */
  RelDataType copyType(RelDataType type);

  /**
   * Creates a type which is the same as another type but with possibly
   * different nullability. The output type may be identical to the input
   * type. For type systems without a concept of nullability, the return value
   * is always the same as the input.
   *
   * @param type     input type
   * @param nullable true to request a nullable type; false to request a NOT
   *                 NULL type ,true表示允许为null
   * @return output type, same as input type except with specified nullability
   * @throws NullPointerException if type is null
   */
  RelDataType createTypeWithNullability(
      RelDataType type,
      boolean nullable);

  /**
   * Creates a Type which is the same as another type but with possibly
   * different charset or collation. For types without a concept of charset or
   * collation this function must throw an error.
   *
   * @param type      input type
   * @param charset   charset to assign
   * @param collation collation to assign
   * @return output type, same as input type except with specified charset and
   * collation
   */
  RelDataType createTypeWithCharsetAndCollation(
      RelDataType type,
      Charset charset,
      SqlCollation collation);

  /**
   * @return the default {@link Charset} for string types
   */
  Charset getDefaultCharset();

  /**
   * Returns the most general of a set of types (that is, one type to which
   * they can all be cast), or null if conversion is not possible. The result
   * may be a new type which is less restrictive than any of the input types,
   * e.g. <code>leastRestrictive(INT, NUMERIC(3, 2))</code> could be
   * {@code NUMERIC(12, 2)}.
   *
   * @param types input types to be combined using union (not null, not empty)
   * @return canonical union type descriptor
   * 这个规则用于union、except、intersect等场景,返回类型中公共的类型,即共同父类。
   * 如果没有公共父类,即任意两个类型不能互相转换,则结果返回null
   */
  RelDataType leastRestrictive(List<RelDataType> types);

  /**
   * Creates a SQL type with no precision or scale.
   *
   * @param typeName Name of the type, for example {@link SqlTypeName#BOOLEAN},
   *   never null
   * @return canonical type descriptor
   */
  RelDataType createSqlType(SqlTypeName typeName);

  /**
   * Creates a SQL type with length (precision) but no scale.
   *
   * @param typeName  Name of the type, for example {@link SqlTypeName#VARCHAR}.
   *                  Never null.
   * @param precision Maximum length of the value (non-numeric types) or the
   *                  precision of the value (numeric/datetime types).
   *                  Must be non-negative or
   *                  {@link RelDataType#PRECISION_NOT_SPECIFIED}.
   * @return canonical type descriptor
   */
  RelDataType createSqlType(
      SqlTypeName typeName,
      int precision);

  /**
   * Creates a SQL type with precision and scale.
   *
   * @param typeName  Name of the type, for example {@link SqlTypeName#DECIMAL}.
   *                  Never null.
   * @param precision Precision of the value.
   *                  Must be non-negative or
   *                  {@link RelDataType#PRECISION_NOT_SPECIFIED}.
   * @param scale     scale of the values, i.e. the number of decimal places to
   *                  shift the value. For example, a NUMBER(10,3) value of
   *                  "123.45" is represented "123450" (that is, multiplied by
   *                  10^3). A negative scale <em>is</em> valid.
   * @return canonical type descriptor
   */
  RelDataType createSqlType(
      SqlTypeName typeName,
      int precision,
      int scale);

  /**
   * Creates a SQL interval type.
   *
   * @param intervalQualifier contains information if it is a year-month or a
   *                          day-time interval along with precision information
   * @return canonical type descriptor
   */
  RelDataType createSqlIntervalType(
      SqlIntervalQualifier intervalQualifier);

  /**
   * Infers the return type of a decimal multiplication. Decimal
   * multiplication involves at least one decimal operand and requires both
   * operands to have exact numeric types.
   *
   * @param type1 type of the first operand
   * @param type2 type of the second operand
   * @return the result type for a decimal multiplication, or null if decimal
   * multiplication should not be applied to the operands.
   */
  RelDataType createDecimalProduct(
      RelDataType type1,
      RelDataType type2);

  /**
   * Returns whether a decimal multiplication should be implemented by casting
   * arguments to double values.
   *
   * <p>Pre-condition: <code>createDecimalProduct(type1, type2) != null</code>
   */
  boolean useDoubleMultiplication(
      RelDataType type1,
      RelDataType type2);

  /**
   * Infers the return type of a decimal division. Decimal division involves
   * at least one decimal operand and requires both operands to have exact
   * numeric types.
   *
   * @param type1 type of the first operand
   * @param type2 type of the second operand
   * @return the result type for a decimal division, or null if decimal
   * division should not be applied to the operands.
   */
  RelDataType createDecimalQuotient(
      RelDataType type1,
      RelDataType type2);

  /**
   * Creates a
   * {@link org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder}.
   */
  FieldInfoBuilder builder();

  //~ Inner Interfaces -------------------------------------------------------

  /**
   * Callback which provides enough information to create fields.
   * 定义schema
   */
  public interface FieldInfo {
    /**
     * Returns the number of fields.
     *
     * @return number of fields
     * 多少个字段
     */
    int getFieldCount();

    /**
     * Returns the name of a given field.
     *
     * @param index Ordinal of field
     * @return Name of given field
     * 每一个字段的名称
     */
    String getFieldName(int index);

    /**
     * Returns the type of a given field.
     *
     * @param index Ordinal of field
     * @return Type of given field
     * 每一个字段的类型
     */
    RelDataType getFieldType(int index);
  }

  /**
   * Implementation of {@link FieldInfo} that provides a fluid API to build
   * a list of fields.
   * 构造一组属性集合
   */
  public static class FieldInfoBuilder implements FieldInfo {
    private final List<String> names = new ArrayList<String>();
    private final List<RelDataType> types = new ArrayList<RelDataType>();

    private final RelDataTypeFactory typeFactory;

    /**
     * Creates a FieldInfoBuilder with the given type factory.
     */
    public FieldInfoBuilder(RelDataTypeFactory typeFactory) {
      this.typeFactory = typeFactory;
      assert typeFactory != null;
    }

    public int getFieldCount() {
      return names.size();
    }

    public String getFieldName(int index) {
      return names.get(index);
    }

    public RelDataType getFieldType(int index) {
      return types.get(index);
    }

    /**
     * Adds a field with given name and type.
     * 添加一个属性与对应的类型映射
     */
    public FieldInfoBuilder add(String name, RelDataType type) {
      names.add(name);
      types.add(type);
      return this;
    }

    /**
     * Adds a field with a type created using
     * {@link org.apache.calcite.rel.type.RelDataTypeFactory#createSqlType(org.apache.calcite.sql.type.SqlTypeName)}.
     */
    public FieldInfoBuilder add(String name, SqlTypeName typeName) {
      add(name, typeFactory.createSqlType(typeName));
      return this;
    }

    /**
     * Adds a field with a type created using
     * {@link org.apache.calcite.rel.type.RelDataTypeFactory#createSqlType(org.apache.calcite.sql.type.SqlTypeName, int)}.
     */
    public FieldInfoBuilder add(
        String name, SqlTypeName typeName, int precision) {
      add(name, typeFactory.createSqlType(typeName, precision));
      return this;
    }

    /**
     * Adds a field with a type created using
     * {@link org.apache.calcite.rel.type.RelDataTypeFactory#createSqlType(org.apache.calcite.sql.type.SqlTypeName, int, int)}.
     */
    public FieldInfoBuilder add(
        String name, SqlTypeName typeName, int precision, int scale) {
      add(name, typeFactory.createSqlType(typeName, precision, scale));
      return this;
    }

    /**
     * Changes the nullability of the last field added.
     *
     * @throws java.lang.IndexOutOfBoundsException if no fields have been
     *                                             added
     */
    public FieldInfoBuilder nullable(boolean nullable) {
      RelDataType lastType = types.get(types.size() - 1);
      if (lastType.isNullable() != nullable) {
        final RelDataType type =
            typeFactory.createTypeWithNullability(lastType, nullable);
        types.set(types.size() - 1, type);
      }
      return this;
    }

    /**
     * Adds a field. Field's ordinal is ignored.
     */
    public FieldInfoBuilder add(RelDataTypeField field) {
      add(field.getName(), field.getType());
      return this;
    }

    /**
     * Adds all fields in a collection.
     */
    public FieldInfoBuilder addAll(
        Iterable<? extends Map.Entry<String, RelDataType>> fields) {
      for (Map.Entry<String, RelDataType> field : fields) {
        add(field.getKey(), field.getValue());
      }
      return this;
    }

    /**
     * Creates a struct type with the current contents of this builder.
     * 创建一个包含多个列的对象
     */
    public RelDataType build() {
      return typeFactory.createStructType(types, names);
    }
  }
}

// End RelDataTypeFactory.java
