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

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlQualified;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * A <code>SqlIdentifier</code> is an identifier, possibly compound.
 * 标识符节点
 * 相当于字符串
 */
public class SqlIdentifier extends SqlNode {
  //~ Instance fields --------------------------------------------------------

  /**
   * Array of the components of this compound identifier.
   *
   * <p>It's convenient to have this member public, and it's convenient to
   * have this member not-final, but it's a shame it's public and not-final.
   * If you assign to this member, please use
   * {@link #setNames(java.util.List, java.util.List)}.
   * And yes, we'd like to make identifiers immutable one day.
   * xx 或者 xx.xx.xx 或者 xx.xx.xx.*(无限个xx)
   *
   */
  public ImmutableList<String> names;

  /**
   * This identifier's collation (if any).
   */
  final SqlCollation collation;

  /**
   * A list of the positions of the components of compound identifiers.
   * 每一个xx对应的token信息
   */
  private ImmutableList<SqlParserPos> componentPositions;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a compound identifier, for example <code>foo.bar</code>.
   *
   * @param names Parts of the identifier, length &ge; 1
   */
  public SqlIdentifier(
      List<String> names,
      SqlCollation collation,
      SqlParserPos pos,//整个xx.xx.xx的全部token信息
      List<SqlParserPos> componentPositions) {
    super(pos);
    this.names = ImmutableList.copyOf(names);
    this.collation = collation;
    this.componentPositions = componentPositions == null ? null
        : ImmutableList.copyOf(componentPositions);
    for (String name : names) {
      assert name != null;
    }
  }

  public SqlIdentifier(List<String> names, SqlParserPos pos) {
    this(names, null, pos, null);
  }

  /**
   * Creates a simple identifier, for example <code>foo</code>, with a
   * collation.
   */
  public SqlIdentifier(
      String name,
      SqlCollation collation,
      SqlParserPos pos) {
    this(ImmutableList.of(name), collation, pos, null);
  }

  /**
   * Creates a simple identifier, for example <code>foo</code>.
   */
  public SqlIdentifier(
      String name,
      SqlParserPos pos) {
    this(ImmutableList.of(name), null, pos, null);
  }

  //~ Methods ----------------------------------------------------------------

  public SqlKind getKind() {
    return SqlKind.IDENTIFIER;
  }

  public SqlNode clone(SqlParserPos pos) {
    return new SqlIdentifier(names, collation, pos, componentPositions);
  }

  public String toString() {
    return Util.sepList(names, ".");
  }

  /**
   * Modifies the components of this identifier and their positions.
   *
   * @param names Names of components
   * @param poses Positions of components
   */
  public void setNames(List<String> names, List<SqlParserPos> poses) {
    this.names = ImmutableList.copyOf(names);
    this.componentPositions = poses == null ? null
        : ImmutableList.copyOf(poses);
  }

  /** Returns an identifier that is the same as this except one modified name.
   * Does not modify this identifier. */
  public SqlIdentifier setName(int i, String name) {
    if (!names.get(i).equals(name)) {
      String[] nameArray = names.toArray(new String[names.size()]);
      nameArray[i] = name;
      return new SqlIdentifier(ImmutableList.copyOf(nameArray), collation, pos,
          componentPositions);
    } else {
      return this;
    }
  }

  /**
   * Returns the position of the <code>i</code>th component of a compound
   * identifier, or the position of the whole identifier if that information
   * is not present.
   *
   * @param i Ordinal of component.
   * @return Position of i'th component
   */
  public SqlParserPos getComponentParserPosition(int i) {
    assert (i >= 0) && (i < names.size());
    return (componentPositions == null) ? getParserPosition()
        : componentPositions.get(i);
  }

  /**
   * Copies names and components from another identifier. Does not modify the
   * cross-component parser position.
   *
   * @param other identifier from which to copy
   */
  public void assignNamesFrom(SqlIdentifier other) {
    setNames(other.names, other.componentPositions);
  }

  /**
   * Creates an identifier which contains only the <code>ordinal</code>th
   * component of this compound identifier. It will have the correct
   * {@link SqlParserPos}, provided that detailed position information is
   * available.
   */
  public SqlIdentifier getComponent(int ordinal) {
    return getComponent(ordinal, ordinal + 1);
  }

  public SqlIdentifier getComponent(int from, int to) {
    final SqlParserPos pos;
    final ImmutableList<SqlParserPos> pos2;
    if (componentPositions == null) {
      pos2 = null;
      pos = this.pos;
    } else {
      pos2 = componentPositions.subList(from, to);
      pos = SqlParserPos.sum(pos2);
    }
    return new SqlIdentifier(names.subList(from, to), collation, pos, pos2);
  }

  /**
   * Creates an identifier that consists of this identifier plus a name segment.
   * Does not modify this identifier.
   */
  public SqlIdentifier plus(String name, SqlParserPos pos) {
    final ImmutableList<String> names =
        ImmutableList.<String>builder().addAll(this.names).add(name).build();
    final ImmutableList.Builder<SqlParserPos> builder = ImmutableList.builder();
    final ImmutableList<SqlParserPos> componentPositions =
        builder.addAll(this.componentPositions).add(pos).build();
    final SqlParserPos pos2 =
        SqlParserPos.sum(builder.add(this.pos).build());
    return new SqlIdentifier(names, collation, pos2, componentPositions);
  }

  /** Creates an identifier that consists of all but the last {@code n}
   * name segments of this one. */
  public SqlIdentifier skipLast(int n) {
    return getComponent(0, names.size() - n);
  }

  public void unparse(
      SqlWriter writer,
      int leftPrec,
      int rightPrec) {
    final SqlWriter.Frame frame =
        writer.startList(SqlWriter.FrameTypeEnum.IDENTIFIER);
    for (String name : names) {
      writer.sep(".");
      if (name.equals("*")) {
        writer.print(name);
      } else {
        writer.identifier(name);
      }
    }

    if (null != collation) {
      collation.unparse(writer, leftPrec, rightPrec);
    }
    writer.endList(frame);
  }

  public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validateIdentifier(this, scope);
  }

  //通常情况下，该方法与validate方法是一样的，只有在SqlIdentifier这个子类的时候，对validateExpr进行了覆写,有自己的实现方式
  public void validateExpr(SqlValidator validator, SqlValidatorScope scope) {
    // First check for builtin functions which don't have parentheses,
    // like "LOCALTIME".
    SqlCall call =
        SqlUtil.makeCall(
            validator.getOperatorTable(),//返回操作的是哪个table表对象
            this); //判断是否是无参数内置函数
    if (call != null) {
      validator.validateCall(call, scope);//校验无参数构造函数
      return;
    }

    validator.validateIdentifier(this, scope);
  }

  public boolean equalsDeep(SqlNode node, boolean fail) {
    if (!(node instanceof SqlIdentifier)) {
      assert !fail : this + "!=" + node;
      return false;
    }
    SqlIdentifier that = (SqlIdentifier) node;
    if (this.names.size() != that.names.size()) {
      assert !fail : this + "!=" + node;
      return false;
    }
    for (int i = 0; i < names.size(); i++) {
      if (!this.names.get(i).equals(that.names.get(i))) {
        assert !fail : this + "!=" + node;
        return false;
      }
    }
    return true;
  }

  public <R> R accept(SqlVisitor<R> visitor) {
    return visitor.visit(this);
  }

  public SqlCollation getCollation() {
    return collation;
  }

  public String getSimple() {
    assert names.size() == 1;
    return names.get(0);
  }

  /**
   * Returns whether this identifier is a star, such as "*" or "foo.bar.*".
   */
  public boolean isStar() {
    return Util.last(names).equals("*");
  }

  /**
   * Returns whether this is a simple identifier. "FOO" is simple; "*",
   * "FOO.*" and "FOO.BAR" are not.
   * 只有一个name,并且不是*
   */
  public boolean isSimple() {
    return (names.size() == 1) && !names.get(0).equals("*");
  }

  public SqlMonotonicity getMonotonicity(SqlValidatorScope scope) {
    // First check for builtin functions which don't have parentheses,
    // like "LOCALTIME".
    final SqlValidator validator = scope.getValidator();
    SqlCall call = SqlUtil.makeCall(validator.getOperatorTable(),this);
    if (call != null) {
      return call.getMonotonicity(scope);
    }
    final SqlQualified qualified = scope.fullyQualify(this);
    final SqlIdentifier fqId = qualified.identifier;
    return qualified.namespace.resolve().getMonotonicity(Util.last(fqId.names));
  }
}

// End SqlIdentifier.java
