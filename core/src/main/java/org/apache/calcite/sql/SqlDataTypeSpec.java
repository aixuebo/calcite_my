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
package org.apache.calcite.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Util;

import com.google.common.base.Objects;

import java.nio.charset.Charset;
import java.util.TimeZone;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Represents a SQL data type specification in a parse tree.
 *
 * <p>A <code>SqlDataTypeSpec</code> is immutable; once created, you cannot
 * change any of the fields.</p>
 *
 * <p>todo: This should really be a subtype of {@link SqlCall}.</p>
 *
 * <p>In its full glory, we will have to support complex type expressions
 * like:</p>
 *
 * <blockquote><code>ROW(<br>
 *   NUMBER(5, 2) NOT NULL AS foo,<br>
 *   ROW(BOOLEAN AS b, MyUDT NOT NULL AS i) AS rec)</code></blockquote>
 *
 * <p>Currently it only supports simple datatypes like CHAR, VARCHAR and DOUBLE,
 * with optional precision and scale.</p>
 * 用于描述sql类型的节点。
 * 包含字段描述符、类型、精准度、字符串编码、时间分区、是否允许null，将这些信息都解析后生成该SqlDataTypeSpec节点
 *
 * 用于定义字段的DDL部分，columnName varchar(20) key  comment  ''
 * 注意:
 * 1.SqlDataTypeSpec只是表示一个字段的定义。 并且不包含字段的name
 * 2.最终表的DDL组成形式是Map<SqlIdentifier,SqlDataTypeSpec>
 *
 1.SqlIdentifier typeName = TypeName() 解析并且设置字段类型
 2. (precision,scale) 解析并且设置精准度
 3.CHARACTER SET Identifier(),解析并且设置charSetName
 4.解析并且设置collectionTypeName = CollectionsTypeName() --- 不太重要，核心关键词是MULTISET
 */
public class SqlDataTypeSpec extends SqlNode {
  //~ Instance fields --------------------------------------------------------

  private final SqlIdentifier collectionsTypeName;//不太重要，也不常用
  private final SqlIdentifier typeName;
  private final int scale;
  private final int precision;
  private final String charSetName;
  private final TimeZone timeZone;

  /** Whether data type is allows nulls.
   *
   * <p>Nullable is nullable! Null means "not specified". E.g.
   * {@code CAST(x AS INTEGER)} preserves has the same nullability as {@code x}.
   */
  private Boolean nullable;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a type specification representing a regular, non-collection type.
   */
  public SqlDataTypeSpec(
      final SqlIdentifier typeName,
      int precision,
      int scale,
      String charSetName,
      TimeZone timeZone,
      SqlParserPos pos) {
    this(null, typeName, precision, scale, charSetName, timeZone, null, pos);
  }

  /**
   * Creates a type specification representing a collection type.
   */
  public SqlDataTypeSpec(
      SqlIdentifier collectionsTypeName,
      SqlIdentifier typeName,
      int precision,
      int scale,
      String charSetName,
      SqlParserPos pos) {
    this(collectionsTypeName, typeName, precision, scale, charSetName, null,
        null, pos);
  }

  /**
   * Creates a type specification.
   */
  public SqlDataTypeSpec(
      SqlIdentifier collectionsTypeName,
      SqlIdentifier typeName,
      int precision,
      int scale,
      String charSetName,
      TimeZone timeZone,
      Boolean nullable,
      SqlParserPos pos) {
    super(pos);
    this.collectionsTypeName = collectionsTypeName;
    this.typeName = typeName;
    this.precision = precision;
    this.scale = scale;
    this.charSetName = charSetName;
    this.timeZone = timeZone;
    this.nullable = nullable;
  }

  //~ Methods ----------------------------------------------------------------

  public SqlNode clone(SqlParserPos pos) {
    return (collectionsTypeName != null)
        ? new SqlDataTypeSpec(collectionsTypeName, typeName, precision, scale,
            charSetName, pos)
        : new SqlDataTypeSpec(typeName, precision, scale, charSetName, timeZone,
            pos);
  }

  public SqlMonotonicity getMonotonicity(SqlValidatorScope scope) {
    return SqlMonotonicity.CONSTANT;
  }

  public SqlIdentifier getCollectionsTypeName() {
    return collectionsTypeName;
  }

  public SqlIdentifier getTypeName() {
    return typeName;
  }

  public int getScale() {
    return scale;
  }

  public int getPrecision() {
    return precision;
  }

  public String getCharSetName() {
    return charSetName;
  }

  public TimeZone getTimeZone() {
    return timeZone;
  }

  /** Returns a copy of this data type specification with a given
   * nullability. */
  public SqlDataTypeSpec withNullable(Boolean nullable) {
    if (SqlFunctions.eq(nullable, this.nullable)) {
      return this;
    }
    return new SqlDataTypeSpec(collectionsTypeName, typeName, precision, scale,
        charSetName, timeZone, nullable, getParserPosition());
  }

  /**
   * Returns a new SqlDataTypeSpec corresponding to the component type if the
   * type spec is a collections type spec.<br>
   * Collection types are <code>ARRAY</code> and <code>MULTISET</code>.
   */
  public SqlDataTypeSpec getComponentTypeSpec() {
    assert getCollectionsTypeName() != null;
    return new SqlDataTypeSpec(
        typeName,
        precision,
        scale,
        charSetName,
        timeZone,
        getParserPosition());
  }

  public void unparse(
      SqlWriter writer,
      int leftPrec,
      int rightPrec) {
    String name = typeName.getSimple();
    if (SqlTypeName.get(name) != null) {
      SqlTypeName sqlTypeName = SqlTypeName.get(name);

      // we have a built-in data type
      writer.keyword(name);

      if (sqlTypeName.allowsPrec() && (precision >= 0)) {
        final SqlWriter.Frame frame =
            writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
        writer.print(precision);
        if (sqlTypeName.allowsScale() && (scale >= 0)) {
          writer.sep(",", true);
          writer.print(scale);
        }
        writer.endList(frame);
      }

      if (charSetName != null) {
        writer.keyword("CHARACTER SET");
        writer.identifier(charSetName);
      }

      if (collectionsTypeName != null) {
        writer.keyword(collectionsTypeName.getSimple());
      }
    } else if (name.startsWith("_")) {
      // We're generating a type for an alien system. For example,
      // UNSIGNED is a built-in type in MySQL.
      // (Need a more elegant way than '_' of flagging this.)
      writer.keyword(name.substring(1));
    } else {
      // else we have a user defined type
      typeName.unparse(writer, leftPrec, rightPrec);
    }
  }

  public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validateDataType(this);
  }

  public <R> R accept(SqlVisitor<R> visitor) {
    return visitor.visit(this);
  }

  public boolean equalsDeep(SqlNode node, boolean fail) {
    if (!(node instanceof SqlDataTypeSpec)) {
      assert !fail : this + "!=" + node;
      return false;
    }
    SqlDataTypeSpec that = (SqlDataTypeSpec) node;
    if (!SqlNode.equalDeep(
        this.collectionsTypeName,
        that.collectionsTypeName,
        fail)) {
      return false;
    }
    if (!this.typeName.equalsDeep(that.typeName, fail)) {
      return false;
    }
    if (this.precision != that.precision) {
      assert !fail : this + "!=" + node;
      return false;
    }
    if (this.scale != that.scale) {
      assert !fail : this + "!=" + node;
      return false;
    }
    if (!Objects.equal(this.timeZone, that.timeZone)) {
      assert !fail : this + "!=" + node;
      return false;
    }
    if (!com.google.common.base.Objects.equal(this.charSetName,
        that.charSetName)) {
      assert !fail : this + "!=" + node;
      return false;
    }
    return true;
  }

  /**
   * Throws an error if the type is not built-in.
   */
  public RelDataType deriveType(SqlValidator validator) {
    String name = typeName.getSimple();

    // for now we only support builtin datatypes
    if (SqlTypeName.get(name) == null) {
      throw validator.newValidationError(this,
          RESOURCE.unknownDatatypeName(name));
    }

    if (null != collectionsTypeName) {
      final String collectionName = collectionsTypeName.getSimple();
      if (SqlTypeName.get(collectionName) == null) {
        throw validator.newValidationError(this,
            RESOURCE.unknownDatatypeName(collectionName));
      }
    }

    RelDataTypeFactory typeFactory = validator.getTypeFactory();
    return deriveType(typeFactory);
  }

  /**
   * Does not throw an error if the type is not built-in.
   */
  public RelDataType deriveType(RelDataTypeFactory typeFactory) {
    String name = typeName.getSimple();

    SqlTypeName sqlTypeName = SqlTypeName.get(name);

    // NOTE jvs 15-Jan-2009:  earlier validation is supposed to
    // have caught these, which is why it's OK for them
    // to be assertions rather than user-level exceptions.
    RelDataType type;
    if ((precision >= 0) && (scale >= 0)) {
      assert sqlTypeName.allowsPrecScale(true, true);
      type = typeFactory.createSqlType(sqlTypeName, precision, scale);
    } else if (precision >= 0) {
      assert sqlTypeName.allowsPrecNoScale();
      type = typeFactory.createSqlType(sqlTypeName, precision);
    } else {
      assert sqlTypeName.allowsNoPrecNoScale();
      type = typeFactory.createSqlType(sqlTypeName);
    }

    if (SqlTypeUtil.inCharFamily(type)) {
      // Applying Syntax rule 10 from SQL:99 spec section 6.22 "If TD is a
      // fixed-length, variable-length or large object character string,
      // then the collating sequence of the result of the <cast
      // specification> is the default collating sequence for the
      // character repertoire of TD and the result of the <cast
      // specification> has the Coercible coercibility characteristic."
      SqlCollation collation = SqlCollation.COERCIBLE;

      Charset charset;
      if (null == charSetName) {
        charset = typeFactory.getDefaultCharset();
      } else {
        String javaCharSetName =
            SqlUtil.translateCharacterSetName(charSetName);
        charset = Charset.forName(javaCharSetName);
      }
      type =
          typeFactory.createTypeWithCharsetAndCollation(
              type,
              charset,
              collation);
    }

    if (null != collectionsTypeName) {
      final String collectionName = collectionsTypeName.getSimple();

      SqlTypeName collectionsSqlTypeName =
          SqlTypeName.get(collectionName);

      switch (collectionsSqlTypeName) {
      case MULTISET:
        type = typeFactory.createMultisetType(type, -1);
        break;

      default:
        throw Util.unexpected(collectionsSqlTypeName);
      }
    }

    if (nullable != null) {
      type = typeFactory.createTypeWithNullability(type, nullable);
    }

    return type;
  }
}

// End SqlDataTypeSpec.java
