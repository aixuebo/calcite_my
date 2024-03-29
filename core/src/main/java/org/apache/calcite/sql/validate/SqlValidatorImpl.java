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

import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.Feature;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlAccessEnum;
import org.apache.calcite.sql.SqlAccessType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlMerge;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSampleSpec;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.AssignableOperandTypeChecker;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.util.BitString;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.calcite.sql.SqlUtil.stripAs;
import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Default implementation of {@link SqlValidator}.
 */
public class SqlValidatorImpl implements SqlValidatorWithHints {
  //~ Static fields/initializers ---------------------------------------------

  public static final Logger TRACER = CalciteTrace.PARSER_LOGGER;

  /**
   * Alias generated for the source table when rewriting UPDATE to MERGE.
   */
  public static final String UPDATE_SRC_ALIAS = "SYS$SRC";

  /**
   * Alias generated for the target table when rewriting UPDATE to MERGE if no
   * alias was specified by the user.
   */
  public static final String UPDATE_TGT_ALIAS = "SYS$TGT";

  /**
   * Alias prefix generated for source columns when rewriting UPDATE to MERGE.
   */
  public static final String UPDATE_ANON_PREFIX = "SYS$ANON";

  @VisibleForTesting
  public SqlValidatorScope getEmptyScope() {
    return new EmptyScope(this);
  }

  //~ Enums ------------------------------------------------------------------

  /**
   * Validation status.
   * 校验状态
   */
  public enum Status {
    /**
     * Validation has not started for this scope.
     * 尚未开始
     */
    UNVALIDATED,

    /**
     * Validation is in progress for this scope.
     * 在校验过程中
     */
    IN_PROGRESS,

    /**
     * Validation has completed (perhaps unsuccessfully).
     * 已校验完成,但不管是否成功
     */
    VALID
  }

  //~ Instance fields --------------------------------------------------------

  private final SqlOperatorTable opTab;
  final SqlValidatorCatalogReader catalogReader;

  /**
   * Maps ParsePosition strings to the {@link SqlIdentifier} identifier
   * objects at these positions
   */
  protected final Map<String, IdInfo> idPositions =
      new HashMap<String, IdInfo>();

  /**
   * Maps {@link SqlNode query node} objects to the {@link SqlValidatorScope}
   * scope created from them}.
   */
  protected final Map<SqlNode, SqlValidatorScope> scopes =
      new IdentityHashMap<SqlNode, SqlValidatorScope>();

  /**
   * Maps a {@link SqlSelect} node to the scope used by its WHERE and HAVING
   * clauses.
   */
  private final Map<SqlSelect, SqlValidatorScope> whereScopes =
      new IdentityHashMap<SqlSelect, SqlValidatorScope>();

  /**
   * Maps a {@link SqlSelect} node to the scope used by its SELECT and HAVING
   * clauses.
   */
  private final Map<SqlSelect, SqlValidatorScope> selectScopes =
      new IdentityHashMap<SqlSelect, SqlValidatorScope>();

  /**
   * Maps a {@link SqlSelect} node to the scope used by its ORDER BY clause.
   */
  private final Map<SqlSelect, SqlValidatorScope> orderScopes =
      new IdentityHashMap<SqlSelect, SqlValidatorScope>();

  /**
   * Maps a {@link SqlSelect} node that is the argument to a CURSOR
   * constructor to the scope of the result of that select node
   */
  private final Map<SqlSelect, SqlValidatorScope> cursorScopes =
      new IdentityHashMap<SqlSelect, SqlValidatorScope>();

  /**
   * Maps a {@link SqlNode node} to the
   * {@link SqlValidatorNamespace namespace} which describes what columns they
   * contain.
   */
  protected final Map<SqlNode, SqlValidatorNamespace> namespaces =
      new IdentityHashMap<SqlNode, SqlValidatorNamespace>();

  /**
   * Set of select expressions used as cursor definitions. In standard SQL,
   * only the top-level SELECT is a cursor; Calcite extends this with
   * cursors as inputs to table functions.
   */
  private final Set<SqlNode> cursorSet = Sets.newIdentityHashSet();

  /**
   * Stack of objects that maintain information about function calls. A stack
   * is needed to handle nested function calls. The function call currently
   * being validated is at the top of the stack.
   */
  protected final Stack<FunctionParamInfo> functionCallStack =
      new Stack<FunctionParamInfo>();

  private int nextGeneratedId;
  protected final RelDataTypeFactory typeFactory;
  protected final RelDataType unknownType;
  private final RelDataType booleanType;

  /**
   * Map of derived RelDataType for each node. This is an IdentityHashMap
   * since in some cases (such as null literals) we need to discriminate by
   * instance.
   * 存储每一个SqlNode对应的类型
   */
  private final Map<SqlNode, RelDataType> nodeToTypeMap =
      new IdentityHashMap<SqlNode, RelDataType>();
  private final AggFinder aggFinder;
  private final AggFinder aggOrOverFinder;
  private final SqlConformance conformance;
  private final Map<SqlNode, SqlNode> originalExprs =
      new HashMap<SqlNode, SqlNode>();

  // REVIEW jvs 30-June-2006: subclasses may override shouldExpandIdentifiers
  // in a way that ignores this; we should probably get rid of the protected
  // method and always use this variable (or better, move preferences like
  // this to a separate "parameter" class)
  protected boolean expandIdentifiers;

  protected boolean expandColumnReferences;

  private boolean rewriteCalls;

  // TODO jvs 11-Dec-2008:  make this local to performUnconditionalRewrites
  // if it's OK to expand the signature of that method.
  private boolean validatingSqlMerge;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a validator.
   *
   * @param opTab         Operator table
   * @param catalogReader Catalog reader
   * @param typeFactory   Type factory
   * @param conformance   Compatibility mode
   */
  protected SqlValidatorImpl(
      SqlOperatorTable opTab,
      SqlValidatorCatalogReader catalogReader,
      RelDataTypeFactory typeFactory,
      SqlConformance conformance) {
    Linq4j.requireNonNull(opTab);
    Linq4j.requireNonNull(catalogReader);
    Linq4j.requireNonNull(typeFactory);
    Linq4j.requireNonNull(conformance);
    this.opTab = opTab;
    this.catalogReader = catalogReader;
    this.typeFactory = typeFactory;
    this.conformance = conformance;

    // NOTE jvs 23-Dec-2003:  This is used as the type for dynamic
    // parameters and null literals until a real type is imposed for them.
    unknownType = typeFactory.createSqlType(SqlTypeName.NULL);
    booleanType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);

    rewriteCalls = true;
    expandColumnReferences = true;
    aggFinder = new AggFinder(opTab, false);
    aggOrOverFinder = new AggFinder(opTab, true);
  }

  //~ Methods ----------------------------------------------------------------

  public SqlConformance getConformance() {
    return conformance;
  }

  public SqlValidatorCatalogReader getCatalogReader() {
    return catalogReader;
  }

  public SqlOperatorTable getOperatorTable() {
    return opTab;
  }

  public RelDataTypeFactory getTypeFactory() {
    return typeFactory;
  }

  public RelDataType getUnknownType() {
    return unknownType;
  }

    /**
     * 对select *部分进行扩展,扩展成N个字段
     * @param selectList        Select clause to be expanded,selectItem内容,其中包含.*的内容
     * @param select  select语法块
     * @param includeSystemVars Whether to include system variables
     * @return 因为要对*进行扩展,所以会将SqlIdentifier(*)转换成new SqlNodeList(new ArrayList<SqlNode>();)
     */
  public SqlNodeList expandStar(
      SqlNodeList selectList,
      SqlSelect select,
      boolean includeSystemVars) {

    List<SqlNode> list = new ArrayList<SqlNode>(); //存储扩展的节点
    //存储每一个扩展的字段的别名 以及 对应的类型
    List<Map.Entry<String, RelDataType>> types = new ArrayList<Map.Entry<String, RelDataType>>();//类型在设置过程中,会被解析元数据类型,缓存到全局变量nodeToTypeMap中

    for (int i = 0; i < selectList.size(); i++) { //循环每一个selectItem
      final SqlNode selectItem = selectList.get(i);
      expandSelectItem(
          selectItem,
          select,
          list,
          new LinkedHashSet<String>(),//用于存储每一个字段对应的别名
          types,
          includeSystemVars); //扩展每一个selectItem,如果出现*,则对齐扩展;
    }
    getRawSelectScope(select).setExpandedSelectList(list);//将select *部分中的内容,转换成具体的字段节点,赋值
    return new SqlNodeList(list, SqlParserPos.ZERO);
  }

  // implement SqlValidator
  public void declareCursor(SqlSelect select, SqlValidatorScope parentScope) {
    cursorSet.add(select);

    // add the cursor to a map that maps the cursor to its select based on
    // the position of the cursor relative to other cursors in that call
    FunctionParamInfo funcParamInfo = functionCallStack.peek();
    Map<Integer, SqlSelect> cursorMap = funcParamInfo.cursorPosToSelectMap;
    int numCursors = cursorMap.size();
    cursorMap.put(numCursors, select);

    // create a namespace associated with the result of the select
    // that is the argument to the cursor constructor; register it
    // with a scope corresponding to the cursor
    SelectScope cursorScope = new SelectScope(parentScope, null, select);
    cursorScopes.put(select, cursorScope);
    final SelectNamespace selectNs = createSelectNamespace(select, select);
    String alias = deriveAlias(select, nextGeneratedId++);
    registerNamespace(cursorScope, alias, selectNs, false);
  }

  // implement SqlValidator
  public void pushFunctionCall() {
    FunctionParamInfo funcInfo = new FunctionParamInfo();
    functionCallStack.push(funcInfo);
  }

  // implement SqlValidator
  public void popFunctionCall() {
    functionCallStack.pop();
  }

  // implement SqlValidator
  public String getParentCursor(String columnListParamName) {
    FunctionParamInfo funcParamInfo = functionCallStack.peek();
    Map<String, String> parentCursorMap =
        funcParamInfo.columnListParamToParentCursorMap;
    return parentCursorMap.get(columnListParamName);
  }

  /**
   * If <code>selectItem</code> is "*" or "TABLE.*", expands it and returns
   * true; otherwise writes the unexpanded item.
   *
   * @param selectItem        Select-list item
   * @param select            Containing select clause
   * @param selectItems       List that expanded items are written to
   * @param aliases           Set of aliases
   * @param types             List of data types in alias order
   * @param includeSystemVars If true include system vars in lists
   * @return Whether the node was expanded
   * 扩展每一个selectItem,如果出现*,则对齐扩展;
   */
  private boolean expandSelectItem(
      final SqlNode selectItem,
      SqlSelect select,
      List<SqlNode> selectItems,
      Set<String> aliases,
      List<Map.Entry<String, RelDataType>> types,
      final boolean includeSystemVars) {

    final SelectScope scope = (SelectScope) getWhereScope(select);
    if (expandStar(selectItems, aliases, types, includeSystemVars, scope,
        selectItem)) {
      return true;
    }

    //以下代码基本上不会被执行到,可忽略
    // Expand the select item: fully-qualify columns, and convert
    // parentheses-free functions such as LOCALTIME into explicit function
    // calls.
    SqlNode expanded = expand(selectItem, scope);
    final String alias =
        deriveAlias(
            selectItem,
            aliases.size());

    // If expansion has altered the natural alias, supply an explicit 'AS'.
    final SqlValidatorScope selectScope = getSelectScope(select);
    if (expanded != selectItem) {
      String newAlias =
          deriveAlias(
              expanded,
              aliases.size());
      if (!newAlias.equals(alias)) {
        expanded =
            SqlStdOperatorTable.AS.createCall(
                selectItem.getParserPosition(),
                expanded,
                new SqlIdentifier(alias, SqlParserPos.ZERO));
        deriveTypeImpl(selectScope, expanded);
      }
    }

    selectItems.add(expanded);
    aliases.add(alias);

    final RelDataType type = deriveType(selectScope, expanded);
    setValidatedNodeTypeImpl(expanded, type);
    types.add(Pair.of(alias, type));
    return false;
  }

  //扩展每一个selectItem,如果出现*,则对齐扩展;
  private boolean expandStar(List<SqlNode> selectItems, Set<String> aliases,
      List<Map.Entry<String, RelDataType>> types, boolean includeSystemVars,
      SelectScope scope, SqlNode node) {

    //*,一定是  SqlIdentifier节点
    if (!(node instanceof SqlIdentifier)) {
      return false;
    }

    //* 一定是以*结尾
    final SqlIdentifier identifier = (SqlIdentifier) node;
    if (!identifier.isStar()) {
      return false;
    }

    final SqlParserPos starPosition = identifier.getParserPosition();
    switch (identifier.names.size()) {
    case 1:
      for (Pair<String, SqlValidatorNamespace> p : scope.children) {
        final SqlNode from = p.right.getNode();
        final SqlValidatorNamespace fromNs = getNamespace(from, scope);
        assert fromNs != null;
        final RelDataType rowType = fromNs.getRowType();//所属表对应的字段集合
        for (RelDataTypeField field : rowType.getFieldList()) { //循环每一个字段
          String columnName = field.getName();

          // TODO: do real implicit collation here
          final SqlNode exp =
              new SqlIdentifier(
                  ImmutableList.of(p.left, columnName),
                  starPosition);
          addToSelectList(
              selectItems,
              aliases,
              types,
              exp,
              scope,
              includeSystemVars);
        }
      }
      return true;
    default:
      final SqlIdentifier prefixId = identifier.skipLast(1);
      final SqlValidatorNamespace fromNs;
      if (prefixId.names.size() == 1) {
        String tableName = prefixId.names.get(0);
        final SqlValidatorNamespace childNs = scope.getChild(tableName);
        if (childNs == null) {
          // e.g. "select r.* from e"
          throw newValidationError(identifier.getComponent(0),
              RESOURCE.unknownIdentifier(tableName));
        }
        final SqlNode from = childNs.getNode();
        fromNs = getNamespace(from);
      } else {
        fromNs = scope.getChild(prefixId.names);
        if (fromNs == null) {
          // e.g. "select s.t.* from e"
          throw newValidationError(prefixId,
              RESOURCE.unknownIdentifier(prefixId.toString()));
        }
      }
      assert fromNs != null;
      final RelDataType rowType = fromNs.getRowType();
      for (RelDataTypeField field : rowType.getFieldList()) {
        String columnName = field.getName();

        // TODO: do real implicit collation here
        addToSelectList(
            selectItems,
            aliases,
            types,
            prefixId.plus(columnName, starPosition),
            scope,
            includeSystemVars);
      }
      return true;
    }
  }

  //核心入口
  public SqlNode validate(SqlNode topNode) {
    SqlValidatorScope scope = new EmptyScope(this);
    scope = new CatalogScope(scope, ImmutableList.of("CATALOG"));
    final SqlNode topNode2 = validateScopedExpression(topNode, scope);//核心校验方法,返回校验成功的SqlNode
    final RelDataType type = getValidatedNodeType(topNode2);//确定sql的返回值是正确的
    Util.discard(type);
    return topNode2;
  }

  public List<SqlMoniker> lookupHints(SqlNode topNode, SqlParserPos pos) {
    SqlValidatorScope scope = new EmptyScope(this);
    SqlNode outermostNode = performUnconditionalRewrites(topNode, false);
    cursorSet.add(outermostNode);
    if (outermostNode.isA(SqlKind.TOP_LEVEL)) {
      registerQuery(
          scope,
          null,
          outermostNode,
          outermostNode,
          null,
          false);
    }
    final SqlValidatorNamespace ns = getNamespace(outermostNode);
    if (ns == null) {
      throw Util.newInternal("Not a query: " + outermostNode);
    }
    Collection<SqlMoniker> hintList = Sets.newTreeSet(SqlMoniker.COMPARATOR);
    lookupSelectHints(ns, pos, hintList);
    return ImmutableList.copyOf(hintList);
  }

  public SqlMoniker lookupQualifiedName(SqlNode topNode, SqlParserPos pos) {
    final String posString = pos.toString();
    IdInfo info = idPositions.get(posString);
    if (info != null) {
      final SqlQualified qualified = info.scope.fullyQualify(info.id);
      return new SqlIdentifierMoniker(qualified.identifier);
    } else {
      return null;
    }
  }

  /**
   * Looks up completion hints for a syntactically correct select SQL that has
   * been parsed into an expression tree.
   *
   * @param select   the Select node of the parsed expression tree
   * @param pos      indicates the position in the sql statement we want to get
   *                 completion hints for
   * @param hintList list of {@link SqlMoniker} (sql identifiers) that can
   *                 fill in at the indicated position
   */
  void lookupSelectHints(
      SqlSelect select,
      SqlParserPos pos,
      Collection<SqlMoniker> hintList) {
    IdInfo info = idPositions.get(pos.toString());
    if ((info == null) || (info.scope == null)) {
      SqlNode fromNode = select.getFrom();
      final SqlValidatorScope fromScope = getFromScope(select);
      lookupFromHints(fromNode, fromScope, pos, hintList);
    } else {
      lookupNameCompletionHints(info.scope, info.id.names,
          info.id.getParserPosition(), hintList);
    }
  }

  private void lookupSelectHints(
      SqlValidatorNamespace ns,
      SqlParserPos pos,
      Collection<SqlMoniker> hintList) {
    final SqlNode node = ns.getNode();
    if (node instanceof SqlSelect) {
      lookupSelectHints((SqlSelect) node, pos, hintList);
    }
  }

  private void lookupFromHints(
      SqlNode node,
      SqlValidatorScope scope,
      SqlParserPos pos,
      Collection<SqlMoniker> hintList) {
    final SqlValidatorNamespace ns = getNamespace(node);
    if (ns.isWrapperFor(IdentifierNamespace.class)) {
      IdentifierNamespace idNs = ns.unwrap(IdentifierNamespace.class);
      final SqlIdentifier id = idNs.getId();
      for (int i = 0; i < id.names.size(); i++) {
        if (pos.toString().equals(
            id.getComponent(i).getParserPosition().toString())) {
          List<SqlMoniker> objNames = new ArrayList<SqlMoniker>();
          SqlValidatorUtil.getSchemaObjectMonikers(
              getCatalogReader(),
              id.names.subList(0, i + 1),
              objNames);
          for (SqlMoniker objName : objNames) {
            if (objName.getType() != SqlMonikerType.FUNCTION) {
              hintList.add(objName);
            }
          }
          return;
        }
      }
    }
    switch (node.getKind()) {
    case JOIN:
      lookupJoinHints((SqlJoin) node, scope, pos, hintList);
      break;
    default:
      lookupSelectHints(ns, pos, hintList);
      break;
    }
  }

  private void lookupJoinHints(
      SqlJoin join,
      SqlValidatorScope scope,
      SqlParserPos pos,
      Collection<SqlMoniker> hintList) {
    SqlNode left = join.getLeft();
    SqlNode right = join.getRight();
    SqlNode condition = join.getCondition();
    lookupFromHints(left, scope, pos, hintList);
    if (hintList.size() > 0) {
      return;
    }
    lookupFromHints(right, scope, pos, hintList);
    if (hintList.size() > 0) {
      return;
    }
    final JoinConditionType conditionType = join.getConditionType();
    final SqlValidatorScope joinScope = scopes.get(join);
    switch (conditionType) {
    case ON:
      condition.findValidOptions(this, joinScope, pos, hintList);
      return;
    default:

      // No suggestions.
      // Not supporting hints for other types such as 'Using' yet.
      return;
    }
  }

  /**
   * Populates a list of all the valid alternatives for an identifier.
   *
   * @param scope    Validation scope
   * @param names    Components of the identifier
   * @param pos      position
   * @param hintList a list of valid options
   */
  public final void lookupNameCompletionHints(
      SqlValidatorScope scope,
      List<String> names,
      SqlParserPos pos,
      Collection<SqlMoniker> hintList) {
    // Remove the last part of name - it is a dummy
    List<String> subNames = Util.skipLast(names);

    if (subNames.size() > 0) {
      // If there's a prefix, resolve it to a namespace.
      SqlValidatorNamespace ns = null;
      for (String name : subNames) {
        if (ns == null) {
          ns = scope.resolve(ImmutableList.of(name), null, null);
        } else {
          ns = ns.lookupChild(name);
        }
        if (ns == null) {
          break;
        }
      }
      if (ns != null) {
        RelDataType rowType = ns.getRowType();
        for (RelDataTypeField field : rowType.getFieldList()) {
          hintList.add(
              new SqlMonikerImpl(
                  field.getName(),
                  SqlMonikerType.COLUMN));
        }
      }

      // builtin function names are valid completion hints when the
      // identifier has only 1 name part
      findAllValidFunctionNames(names, this, hintList, pos);
    } else {
      // No prefix; use the children of the current scope (that is,
      // the aliases in the FROM clause)
      scope.findAliases(hintList);

      // If there's only one alias, add all child columns
      SelectScope selectScope =
          SqlValidatorUtil.getEnclosingSelectScope(scope);
      if ((selectScope != null)
          && (selectScope.getChildren().size() == 1)) {
        RelDataType rowType =
            selectScope.getChildren().get(0).getRowType();
        for (RelDataTypeField field : rowType.getFieldList()) {
          hintList.add(
              new SqlMonikerImpl(
                  field.getName(),
                  SqlMonikerType.COLUMN));
        }
      }
    }

    findAllValidUdfNames(names, this, hintList);
  }

  private static void findAllValidUdfNames(
      List<String> names,
      SqlValidator validator,
      Collection<SqlMoniker> result) {
    List<SqlMoniker> objNames = new ArrayList<SqlMoniker>();
    SqlValidatorUtil.getSchemaObjectMonikers(
        validator.getCatalogReader(),
        names,
        objNames);
    for (SqlMoniker objName : objNames) {
      if (objName.getType() == SqlMonikerType.FUNCTION) {
        result.add(objName);
      }
    }
  }

  private static void findAllValidFunctionNames(
      List<String> names,
      SqlValidator validator,
      Collection<SqlMoniker> result,
      SqlParserPos pos) {
    // a function name can only be 1 part
    if (names.size() > 1) {
      return;
    }
    for (SqlOperator op : validator.getOperatorTable().getOperatorList()) {
      SqlIdentifier curOpId =
          new SqlIdentifier(
              op.getName(),
              pos);

      final SqlCall call =
          SqlUtil.makeCall(
              validator.getOperatorTable(),
              curOpId);
      if (call != null) {
        result.add(
            new SqlMonikerImpl(
                op.getName(),
                SqlMonikerType.FUNCTION));
      } else {
        if ((op.getSyntax() == SqlSyntax.FUNCTION)
            || (op.getSyntax() == SqlSyntax.PREFIX)) {
          if (op.getOperandTypeChecker() != null) {
            String sig = op.getAllowedSignatures();
            sig = sig.replaceAll("'", "");
            result.add(
                new SqlMonikerImpl(
                    sig,
                    SqlMonikerType.FUNCTION));
            continue;
          }
          result.add(
              new SqlMonikerImpl(
                  op.getName(),
                  SqlMonikerType.FUNCTION));
        }
      }
    }
  }

  public SqlNode validateParameterizedExpression(
      SqlNode topNode,
      final Map<String, RelDataType> nameToTypeMap) {//sqlNode与类型的映射关系
    SqlValidatorScope scope = new ParameterScope(this, nameToTypeMap);
    return validateScopedExpression(topNode, scope);
  }

  //核心校验入口
  private SqlNode validateScopedExpression(
      SqlNode topNode,
      SqlValidatorScope scope) {
    SqlNode outermostNode = performUnconditionalRewrites(topNode, false);//sql重写
    cursorSet.add(outermostNode);
    if (TRACER.isLoggable(Level.FINER)) {
      TRACER.finer("After unconditional rewrite: " + outermostNode.toString());//打印无条件重写后的节点内容
    }
    if (outermostNode.isA(SqlKind.TOP_LEVEL)) {
      registerQuery(scope, null, outermostNode, outermostNode, null, false);
    }
    outermostNode.validate(this, scope);
    if (!outermostNode.isA(SqlKind.TOP_LEVEL)) {
      // force type derivation so that we can provide it to the
      // caller later without needing the scope
      deriveType(scope, outermostNode);
    }
    if (TRACER.isLoggable(Level.FINER)) {
      TRACER.finer("After validation: " + outermostNode.toString());
    }
    return outermostNode;
  }

  public void validateQuery(SqlNode node, SqlValidatorScope scope) {
    final SqlValidatorNamespace ns = getNamespace(node, scope);
    if (node.getKind() == SqlKind.TABLESAMPLE) {
      List<SqlNode> operands = ((SqlCall) node).getOperandList();
      SqlSampleSpec sampleSpec = SqlLiteral.sampleValue(operands.get(1));
      if (sampleSpec instanceof SqlSampleSpec.SqlTableSampleSpec) {
        validateFeature(RESOURCE.sQLFeature_T613(), node.getParserPosition());
      } else if (sampleSpec
          instanceof SqlSampleSpec.SqlSubstitutionSampleSpec) {
        validateFeature(RESOURCE.sQLFeatureExt_T613_Substitution(),
            node.getParserPosition());
      }
    }

    validateNamespace(ns);
    validateAccess(
        node,
        ns.getTable(),
        SqlAccessEnum.SELECT); //校验table的访问权限是否满足需求
  }

  /**
   * Validates a namespace.
   * 校验一个命名空间,
   * 参数是具体的命名空间实现类,比如SelectNamespace
   */
  protected void validateNamespace(final SqlValidatorNamespace namespace) {
    namespace.validate();
    if (namespace.getNode() != null) {
      setValidatedNodeType(namespace.getNode(), namespace.getType());
    }
  }

  public SqlValidatorScope getCursorScope(SqlSelect select) {
    return cursorScopes.get(select);
  }

  public SqlValidatorScope getWhereScope(SqlSelect select) {
    return whereScopes.get(select);
  }

  public SqlValidatorScope getSelectScope(SqlSelect select) {
    return selectScopes.get(select);
  }

  public SelectScope getRawSelectScope(SqlSelect select) {
    SqlValidatorScope scope = getSelectScope(select);
    if (scope instanceof AggregatingSelectScope) {
      scope = ((AggregatingSelectScope) scope).getParent();
    }
    return (SelectScope) scope;
  }

  public SqlValidatorScope getHavingScope(SqlSelect select) {
    // Yes, it's the same as getSelectScope
    return selectScopes.get(select);
  }

  public SqlValidatorScope getGroupScope(SqlSelect select) {
    // Yes, it's the same as getWhereScope
    return whereScopes.get(select);
  }

  public SqlValidatorScope getFromScope(SqlSelect select) {
    return scopes.get(select);
  }

  public SqlValidatorScope getOrderScope(SqlSelect select) {
    return orderScopes.get(select);
  }

  public SqlValidatorScope getJoinScope(SqlNode node) {
    return scopes.get(stripAs(node));
  }

  public SqlValidatorScope getOverScope(SqlNode node) {
    return scopes.get(node);
  }

  private SqlValidatorNamespace getNamespace(SqlNode node,
      SqlValidatorScope scope) {
    if (node instanceof SqlIdentifier && scope instanceof DelegatingScope) {
      final SqlIdentifier id = (SqlIdentifier) node;
      final SqlValidatorScope parentScope =
          ((DelegatingScope) scope).getParent();
      if (id.isSimple()) {
        SqlValidatorNamespace ns =
            parentScope.resolve(id.names, null, null);
        if (ns != null) {
          return ns;
        }
      }
    }
    return getNamespace(node);
  }

  //该节点对应的空间表结果
  public SqlValidatorNamespace getNamespace(SqlNode node) {
    switch (node.getKind()) {
    case AS:

      // AS has a namespace if it has a column list 'AS t (c1, c2, ...)'
      final SqlValidatorNamespace ns = namespaces.get(node);
      if (ns != null) {
        return ns;
      }
      // fall through
    case OVER:
    case COLLECTION_TABLE:
    case ORDER_BY:
    case TABLESAMPLE:
      return getNamespace(((SqlCall) node).operand(0));
    default:
      return namespaces.get(node);
    }
  }

  /**
   * Performs expression rewrites which are always used unconditionally. These
   * rewrites massage the expression tree into a standard form so that the
   * rest of the validation logic can be simpler.
   * 总是无条件的去执行表达式重写。
   * 重写表达式 进入标准形式,目的是余下来的校验更简单
   * @param node      expression to be rewritten 要重写的表达式
   * @param underFrom whether node appears directly under a FROM clause
   * @return rewritten expression
   */
  protected SqlNode performUnconditionalRewrites(
      SqlNode node,
      boolean underFrom) {
    if (node == null) {
      return node;
    }

    SqlNode newOperand;

    // first transform operands and invoke generic call rewrite
    if (node instanceof SqlCall) {
      if (node instanceof SqlMerge) {
        validatingSqlMerge = true;
      }
      SqlCall call = (SqlCall) node;
      final SqlKind kind = call.getKind();
      final List<SqlNode> operands = call.getOperandList();
      for (int i = 0; i < operands.size(); i++) {
        SqlNode operand = operands.get(i);
        boolean childUnderFrom;//true表示循环到from参数了
        if (kind == SqlKind.SELECT) {
          childUnderFrom = i == SqlSelect.FROM_OPERAND;
        } else if (kind == SqlKind.AS && (i == 0)) {
          // for an aliased expression, it is under FROM if
          // the AS expression is under FROM
          childUnderFrom = underFrom;
        } else {
          childUnderFrom = false;
        }
        newOperand =
            performUnconditionalRewrites(operand, childUnderFrom);
        if (newOperand != null && newOperand != operand) {
          call.setOperand(i, newOperand);//替换成标准形式
        }
      }

      if (call.getOperator() instanceof SqlUnresolvedFunction) {
        assert call instanceof SqlBasicCall;
        final SqlUnresolvedFunction function =
            (SqlUnresolvedFunction) call.getOperator();
        // This function hasn't been resolved yet.  Perform
        // a half-hearted resolution now in case it's a
        // builtin function requiring special casing.  If it's
        // not, we'll handle it later during overload resolution.
        final List<SqlOperator> overloads = Lists.newArrayList();
        opTab.lookupOperatorOverloads(function.getNameAsId(),
            function.getFunctionType(), SqlSyntax.FUNCTION, overloads);
        if (overloads.size() == 1) {
          ((SqlBasicCall) call).setOperator(overloads.get(0));
        }
      }
      if (rewriteCalls) {
        node = call.getOperator().rewriteCall(this, call);
      }
    } else if (node instanceof SqlNodeList) {
      SqlNodeList list = (SqlNodeList) node;
      for (int i = 0, count = list.size(); i < count; i++) {
        SqlNode operand = list.get(i);
        newOperand =
            performUnconditionalRewrites(
                operand,
                false);
        if (newOperand != null) {
          list.getList().set(i, newOperand);
        }
      }
    }

    // now transform node itself
    final SqlKind kind = node.getKind();
    switch (kind) {
    case VALUES:
      // CHECKSTYLE: IGNORE 1
      if (underFrom || true) {
        // leave FROM (VALUES(...)) [ AS alias ] clauses alone,
        // otherwise they grow cancerously if this rewrite is invoked
        // over and over
        return node;
      } else {
        final SqlNodeList selectList =
            new SqlNodeList(SqlParserPos.ZERO);
        selectList.add(new SqlIdentifier("*", SqlParserPos.ZERO));
        return new SqlSelect(node.getParserPosition(), null, selectList, node,
            null, null, null, null, null, null, null);
      }

    case ORDER_BY: {
      SqlOrderBy orderBy = (SqlOrderBy) node;
      if (orderBy.query instanceof SqlSelect) {
        SqlSelect select = (SqlSelect) orderBy.query;

        // Don't clobber existing ORDER BY.  It may be needed for
        // an order-sensitive function like RANK.
        //不能破坏已存在的order by,如果子查询没有order by,可以考虑将外部的order by 赋予select对象
        if (select.getOrderList() == null) {
          // push ORDER BY into existing select
          select.setOrderBy(orderBy.orderList);
          select.setOffset(orderBy.offset);
          select.setFetch(orderBy.fetch);
          return select;
        }
      }
      final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
      selectList.add(new SqlIdentifier("*", SqlParserPos.ZERO));
      final SqlNodeList orderList;
      //包含select语法 && select语法内有聚合函数
      if (getInnerSelect(node) != null && isAggregate(getInnerSelect(node))) {
        orderList =
            orderBy.orderList.clone(orderBy.orderList.getParserPosition());
        // We assume that ORDER BY item does not have ASC etc.
        // We assume that ORDER BY item is present in SELECT list.
        for (int i = 0; i < orderList.size(); i++) {
          SqlNode sqlNode = orderList.get(i);//每一个order by的内容
          SqlNodeList selectList2 = getInnerSelect(node).getSelectList();//order by中select的内容
          for (Ord<SqlNode> sel : Ord.zip(selectList2)) {
            if (stripAs(sel.e).equalsDeep(sqlNode, false)) {//如果order by的内容在select中
              orderList.set(i,
                  SqlLiteral.createExactNumeric(Integer.toString(sel.i + 1),
                      SqlParserPos.ZERO));//设置order by 字段位置与selet字段位置的映射关系
            }
          }
        }
      } else {
        orderList = orderBy.orderList;
      }
      return new SqlSelect(SqlParserPos.ZERO, null, selectList, orderBy.query,
          null, null, null, null, orderList, orderBy.offset,
          orderBy.fetch);
    }

    case EXPLICIT_TABLE: {
      // (TABLE t) is equivalent to (SELECT * FROM t)
      SqlCall call = (SqlCall) node;
      final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
      selectList.add(new SqlIdentifier("*", SqlParserPos.ZERO));
      return new SqlSelect(SqlParserPos.ZERO, null, selectList, call.operand(0),
          null, null, null, null, null, null, null);
    }

    case DELETE: {
      SqlDelete call = (SqlDelete) node;
      SqlSelect select = createSourceSelectForDelete(call);
      call.setSourceSelect(select);
      break;
    }

    case UPDATE: {
      SqlUpdate call = (SqlUpdate) node;
      SqlSelect select = createSourceSelectForUpdate(call);
      call.setSourceSelect(select);

      // See if we're supposed to rewrite UPDATE to MERGE
      // (unless this is the UPDATE clause of a MERGE,
      // in which case leave it alone).
      if (!validatingSqlMerge) {
        SqlNode selfJoinSrcExpr =
            getSelfJoinExprForUpdate(
                call.getTargetTable(),
                UPDATE_SRC_ALIAS);
        if (selfJoinSrcExpr != null) {
          node = rewriteUpdateToMerge(call, selfJoinSrcExpr);
        }
      }
      break;
    }

    case MERGE: {
      SqlMerge call = (SqlMerge) node;
      rewriteMerge(call);
      break;
    }
    }
    return node;
  }

  //返回select语法
  private SqlSelect getInnerSelect(SqlNode node) {
    for (;;) {
      if (node instanceof SqlSelect) {
        return (SqlSelect) node;
      } else if (node instanceof SqlOrderBy) {
        node = ((SqlOrderBy) node).query;
      } else if (node instanceof SqlWith) {
        node = ((SqlWith) node).body;
      } else {
        return null;
      }
    }
  }

  private void rewriteMerge(SqlMerge call) {
    SqlNodeList selectList;
    SqlUpdate updateStmt = call.getUpdateCall();
    if (updateStmt != null) {
      // if we have an update statement, just clone the select list
      // from the update statement's source since it's the same as
      // what we want for the select list of the merge source -- '*'
      // followed by the update set expressions
      selectList =
          (SqlNodeList) updateStmt.getSourceSelect().getSelectList()
              .clone();
    } else {
      // otherwise, just use select *
      selectList = new SqlNodeList(SqlParserPos.ZERO);
      selectList.add(new SqlIdentifier("*", SqlParserPos.ZERO));
    }
    SqlNode targetTable = call.getTargetTable();
    if (call.getAlias() != null) {
      targetTable =
          SqlValidatorUtil.addAlias(
              targetTable,
              call.getAlias().getSimple());
    }

    // Provided there is an insert substatement, the source select for
    // the merge is a left outer join between the source in the USING
    // clause and the target table; otherwise, the join is just an
    // inner join.  Need to clone the source table reference in order
    // for validation to work
    SqlNode sourceTableRef = call.getSourceTableRef();
    SqlInsert insertCall = call.getInsertCall();
    JoinType joinType = (insertCall == null) ? JoinType.INNER : JoinType.LEFT;
    SqlNode leftJoinTerm = (SqlNode) sourceTableRef.clone();
    SqlNode outerJoin =
        new SqlJoin(SqlParserPos.ZERO,
            leftJoinTerm,
            SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
            joinType.symbol(SqlParserPos.ZERO),
            targetTable,
            JoinConditionType.ON.symbol(SqlParserPos.ZERO),
            call.getCondition());
    SqlSelect select =
        new SqlSelect(SqlParserPos.ZERO, null, selectList, outerJoin, null,
            null, null, null, null, null, null);
    call.setSourceSelect(select);

    // Source for the insert call is a select of the source table
    // reference with the select list being the value expressions;
    // note that the values clause has already been converted to a
    // select on the values row constructor; so we need to extract
    // that via the from clause on the select
    if (insertCall != null) {
      SqlSelect valuesSelect = (SqlSelect) insertCall.getSource();
      SqlCall valuesCall = (SqlCall) valuesSelect.getFrom();
      SqlCall rowCall = valuesCall.operand(0);
      selectList =
          new SqlNodeList(
              rowCall.getOperandList(),
              SqlParserPos.ZERO);
      SqlNode insertSource = (SqlNode) sourceTableRef.clone();
      select =
          new SqlSelect(SqlParserPos.ZERO, null, selectList, insertSource, null,
              null, null, null, null, null, null);
      insertCall.setSource(select);
    }
  }

  private SqlNode rewriteUpdateToMerge(
      SqlUpdate updateCall,
      SqlNode selfJoinSrcExpr) {
    // Make sure target has an alias.
    if (updateCall.getAlias() == null) {
      updateCall.setAlias(
          new SqlIdentifier(UPDATE_TGT_ALIAS, SqlParserPos.ZERO));
    }
    SqlNode selfJoinTgtExpr =
        getSelfJoinExprForUpdate(
            updateCall.getTargetTable(),
            updateCall.getAlias().getSimple());
    assert selfJoinTgtExpr != null;

    // Create join condition between source and target exprs,
    // creating a conjunction with the user-level WHERE
    // clause if one was supplied
    SqlNode condition = updateCall.getCondition();
    SqlNode selfJoinCond =
        SqlStdOperatorTable.EQUALS.createCall(
            SqlParserPos.ZERO,
            selfJoinSrcExpr,
            selfJoinTgtExpr);
    if (condition == null) {
      condition = selfJoinCond;
    } else {
      condition =
          SqlStdOperatorTable.AND.createCall(
              SqlParserPos.ZERO,
              selfJoinCond,
              condition);
    }
    SqlNode target =
        updateCall.getTargetTable().clone(SqlParserPos.ZERO);

    // For the source, we need to anonymize the fields, so
    // that for a statement like UPDATE T SET I = I + 1,
    // there's no ambiguity for the "I" in "I + 1";
    // this is OK because the source and target have
    // identical values due to the self-join.
    // Note that we anonymize the source rather than the
    // target because downstream, the optimizer rules
    // don't want to see any projection on top of the target.
    IdentifierNamespace ns =
        new IdentifierNamespace(this, target, null, null);
    RelDataType rowType = ns.getRowType();
    SqlNode source = updateCall.getTargetTable().clone(SqlParserPos.ZERO);
    final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
    int i = 1;
    for (RelDataTypeField field : rowType.getFieldList()) {
      SqlIdentifier col =
          new SqlIdentifier(
              field.getName(),
              SqlParserPos.ZERO);
      selectList.add(
          SqlValidatorUtil.addAlias(col, UPDATE_ANON_PREFIX + i));
      ++i;
    }
    source =
        new SqlSelect(SqlParserPos.ZERO, null, selectList, source, null, null,
            null, null, null, null, null);
    source = SqlValidatorUtil.addAlias(source, UPDATE_SRC_ALIAS);
    SqlMerge mergeCall =
        new SqlMerge(updateCall.getParserPosition(), target, condition, source,
            updateCall, null, null, updateCall.getAlias());
    rewriteMerge(mergeCall);
    return mergeCall;
  }

  /**
   * Allows a subclass to provide information about how to convert an UPDATE
   * into a MERGE via self-join. If this method returns null, then no such
   * conversion takes place. Otherwise, this method should return a suitable
   * unique identifier expression for the given table.
   *
   * @param table identifier for table being updated
   * @param alias alias to use for qualifying columns in expression, or null
   *              for unqualified references; if this is equal to
   *              {@value #UPDATE_SRC_ALIAS}, then column references have been
   *              anonymized to "SYS$ANONx", where x is the 1-based column
   *              number.
   * @return expression for unique identifier, or null to prevent conversion
   */
  protected SqlNode getSelfJoinExprForUpdate(
      SqlNode table,
      String alias) {
    return null;
  }

  /**
   * Creates the SELECT statement that putatively feeds rows into an UPDATE
   * statement to be updated.
   *
   * @param call Call to the UPDATE operator
   * @return select statement
   */
  protected SqlSelect createSourceSelectForUpdate(SqlUpdate call) {
    final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
    selectList.add(new SqlIdentifier("*", SqlParserPos.ZERO));
    int ordinal = 0;
    for (SqlNode exp : call.getSourceExpressionList()) {
      // Force unique aliases to avoid a duplicate for Y with
      // SET X=Y
      String alias = SqlUtil.deriveAliasFromOrdinal(ordinal);
      selectList.add(SqlValidatorUtil.addAlias(exp, alias));
      ++ordinal;
    }
    SqlNode sourceTable = call.getTargetTable();
    if (call.getAlias() != null) {
      sourceTable =
          SqlValidatorUtil.addAlias(
              sourceTable,
              call.getAlias().getSimple());
    }
    return new SqlSelect(SqlParserPos.ZERO, null, selectList, sourceTable,
        call.getCondition(), null, null, null, null, null, null);
  }

  /**
   * Creates the SELECT statement that putatively feeds rows into a DELETE
   * statement to be deleted.
   *
   * @param call Call to the DELETE operator
   * @return select statement
   */
  protected SqlSelect createSourceSelectForDelete(SqlDelete call) {
    final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
    selectList.add(new SqlIdentifier("*", SqlParserPos.ZERO));
    SqlNode sourceTable = call.getTargetTable();
    if (call.getAlias() != null) {
      sourceTable =
          SqlValidatorUtil.addAlias(
              sourceTable,
              call.getAlias().getSimple());
    }
    return new SqlSelect(SqlParserPos.ZERO, null, selectList, sourceTable,
        call.getCondition(), null, null, null, null, null, null);
  }

  /**
   * Returns null if there is no common type. E.g. if the rows have a
   * different number of columns.
   */
  RelDataType getTableConstructorRowType(
      SqlCall values,
      SqlValidatorScope scope) {
    final List<SqlNode> rows = values.getOperandList();
    assert rows.size() >= 1;
    List<RelDataType> rowTypes = new ArrayList<RelDataType>();
    for (final SqlNode row : rows) {
      assert row.getKind() == SqlKind.ROW;
      SqlCall rowConstructor = (SqlCall) row;

      // REVIEW jvs 10-Sept-2003: Once we support single-row queries as
      // rows, need to infer aliases from there.
      final List<String> aliasList = new ArrayList<String>();
      final List<RelDataType> typeList = new ArrayList<RelDataType>();
      for (Ord<SqlNode> column : Ord.zip(rowConstructor.getOperandList())) {
        final String alias = deriveAlias(column.e, column.i);
        aliasList.add(alias);
        final RelDataType type = deriveType(scope, column.e);
        typeList.add(type);
      }
      rowTypes.add(typeFactory.createStructType(typeList, aliasList));
    }
    if (rows.size() == 1) {
      // TODO jvs 10-Oct-2005:  get rid of this workaround once
      // leastRestrictive can handle all cases
      return rowTypes.get(0);
    }
    return typeFactory.leastRestrictive(rowTypes);
  }

  public RelDataType getValidatedNodeType(SqlNode node) {
    RelDataType type = getValidatedNodeTypeIfKnown(node);
    if (type == null) {
      throw Util.needToImplement(node);
    } else {
      return type;
    }
  }

  public RelDataType getValidatedNodeTypeIfKnown(SqlNode node) {
    final RelDataType type = nodeToTypeMap.get(node);
    if (type != null) {
      return type;
    }
    final SqlValidatorNamespace ns = getNamespace(node);
    if (ns != null) {
      return ns.getType();
    }
    final SqlNode original = originalExprs.get(node);
    if (original != null && original != node) {
      return getValidatedNodeType(original);
    }
    return null;
  }

  public void setValidatedNodeType(
      SqlNode node,
      RelDataType type) {
    setValidatedNodeTypeImpl(node, type);
  }

  //移除sqlNode与类型映射关系
  public void removeValidatedNodeType(SqlNode node) {
    nodeToTypeMap.remove(node);
  }

  //设置sqlNode类型
  void setValidatedNodeTypeImpl(SqlNode node, RelDataType type) {
    Util.pre(type != null, "type != null");
    Util.pre(node != null, "node != null");
    if (type.equals(unknownType)) {
      // don't set anything until we know what it is, and don't overwrite
      // a known type with the unknown type
      return;
    }
    nodeToTypeMap.put(node, type);
  }

  //推测表达式类型
  public RelDataType deriveType(
      SqlValidatorScope scope,
      SqlNode expr) {
    Util.pre(scope != null, "scope != null");
    Util.pre(expr != null, "expr != null");

    // if we already know the type, no need to re-derive
    RelDataType type = nodeToTypeMap.get(expr); //可能该node已经识别过了,先查询缓存
    if (type != null) {
      return type;
    }
    final SqlValidatorNamespace ns = getNamespace(expr);
    if (ns != null) {
      return ns.getType();
    }
    type = deriveTypeImpl(scope, expr);//推测类型
    Util.permAssert(
        type != null,
        "SqlValidator.deriveTypeInternal returned null");
    setValidatedNodeTypeImpl(expr, type);//设置类型
    return type;
  }

  /**
   * Derives the type of a node.
   *
   * @post return != null
   */
  RelDataType deriveTypeImpl(
      SqlValidatorScope scope,
      SqlNode operand) {
    DeriveTypeVisitor v = new DeriveTypeVisitor(scope);
    final RelDataType type = operand.accept(v);
    return scope.nullifyType(operand, type);
  }

  public RelDataType deriveConstructorType(
      SqlValidatorScope scope,
      SqlCall call,
      SqlFunction unresolvedConstructor,
      SqlFunction resolvedConstructor,
      List<RelDataType> argTypes) {
    SqlIdentifier sqlIdentifier = unresolvedConstructor.getSqlIdentifier();
    assert sqlIdentifier != null;
    RelDataType type = catalogReader.getNamedType(sqlIdentifier);
    if (type == null) {
      // TODO jvs 12-Feb-2005:  proper type name formatting
      throw newValidationError(sqlIdentifier,
          RESOURCE.unknownDatatypeName(sqlIdentifier.toString()));
    }

    if (resolvedConstructor == null) {
      if (call.operandCount() > 0) {
        // This is not a default constructor invocation, and
        // no user-defined constructor could be found
        throw handleUnresolvedFunction(call, unresolvedConstructor, argTypes);
      }
    } else {
      SqlCall testCall =
          resolvedConstructor.createCall(
              call.getParserPosition(),
              call.getOperandList());
      RelDataType returnType =
          resolvedConstructor.validateOperands(
              this,
              scope,
              testCall);
      assert type == returnType;
    }

    if (shouldExpandIdentifiers()) {
      if (resolvedConstructor != null) {
        ((SqlBasicCall) call).setOperator(resolvedConstructor);
      } else {
        // fake a fully-qualified call to the default constructor
        ((SqlBasicCall) call).setOperator(
            new SqlFunction(
                type.getSqlIdentifier(),
                ReturnTypes.explicit(type),
                null,
                null,
                null,
                SqlFunctionCategory.USER_DEFINED_CONSTRUCTOR));
      }
    }
    return type;
  }

  public CalciteException handleUnresolvedFunction(
      SqlCall call,
      SqlFunction unresolvedFunction,
      List<RelDataType> argTypes) {
    // For builtins, we can give a better error message
    final List<SqlOperator> overloads = Lists.newArrayList();
    opTab.lookupOperatorOverloads(unresolvedFunction.getNameAsId(), null,
        SqlSyntax.FUNCTION, overloads);
    if (overloads.size() == 1) {
      SqlFunction fun = (SqlFunction) overloads.get(0);
      if ((fun.getSqlIdentifier() == null)
          && (fun.getSyntax() != SqlSyntax.FUNCTION_ID)) {
        final int expectedArgCount =
            fun.getOperandCountRange().getMin();
        throw newValidationError(call,
            RESOURCE.invalidArgCount(call.getOperator().getName(),
                expectedArgCount));
      }
    }

    AssignableOperandTypeChecker typeChecking =
        new AssignableOperandTypeChecker(argTypes);
    String signature =
        typeChecking.getAllowedSignatures(
            unresolvedFunction,
            unresolvedFunction.getName());
    throw newValidationError(call,
        RESOURCE.validatorUnknownFunction(signature));
  }

  protected void inferUnknownTypes(
      RelDataType inferredType,
      SqlValidatorScope scope,
      SqlNode node) {
    final SqlValidatorScope newScope = scopes.get(node);
    if (newScope != null) {
      scope = newScope;
    }
    boolean isNullLiteral = SqlUtil.isNullLiteral(node, false);
    if ((node instanceof SqlDynamicParam) || isNullLiteral) {
      if (inferredType.equals(unknownType)) {
        if (isNullLiteral) {
          throw newValidationError(node, RESOURCE.nullIllegal());
        } else {
          throw newValidationError(node, RESOURCE.dynamicParamIllegal());
        }
      }

      // REVIEW:  should dynamic parameter types always be nullable?
      RelDataType newInferredType =
          typeFactory.createTypeWithNullability(inferredType, true);
      if (SqlTypeUtil.inCharFamily(inferredType)) { //如果类型是string类型,继续处理
        newInferredType =
            typeFactory.createTypeWithCharsetAndCollation(
                newInferredType,
                inferredType.getCharset(),
                inferredType.getCollation());
      }
      setValidatedNodeTypeImpl(node, newInferredType);
    } else if (node instanceof SqlNodeList) {
      SqlNodeList nodeList = (SqlNodeList) node;
      if (inferredType.isStruct()) {
        if (inferredType.getFieldCount() != nodeList.size()) {
          // this can happen when we're validating an INSERT
          // where the source and target degrees are different;
          // bust out, and the error will be detected higher up
          return;
        }
      }
      int i = 0;
      for (SqlNode child : nodeList) {
        RelDataType type;
        if (inferredType.isStruct()) {
          type = inferredType.getFieldList().get(i).getType();
          ++i;
        } else {
          type = inferredType;
        }
        inferUnknownTypes(type, scope, child); //设置当前child的类型
      }
    } else if (node instanceof SqlCase) {
      // REVIEW wael: can this be done in a paramtypeinference strategy
      // object?
      SqlCase caseCall = (SqlCase) node;
      RelDataType returnType = deriveType(scope, node);

      SqlNodeList whenList = caseCall.getWhenOperands();
      for (int i = 0; i < whenList.size(); i++) {
        SqlNode sqlNode = whenList.get(i);
        inferUnknownTypes(unknownType, scope, sqlNode);
      }
      SqlNodeList thenList = caseCall.getThenOperands();
      for (int i = 0; i < thenList.size(); i++) {
        SqlNode sqlNode = thenList.get(i);
        inferUnknownTypes(returnType, scope, sqlNode);
      }

      if (!SqlUtil.isNullLiteral(
          caseCall.getElseOperand(),
          false)) {
        inferUnknownTypes(
            returnType,
            scope,
            caseCall.getElseOperand());
      } else {
        setValidatedNodeTypeImpl(
            caseCall.getElseOperand(),
            returnType);
      }
    } else if (node instanceof SqlCall) {
      SqlCall call = (SqlCall) node;
      SqlOperandTypeInference operandTypeInference =
          call.getOperator().getOperandTypeInference();
      List<SqlNode> operands = call.getOperandList();
      RelDataType[] operandTypes = new RelDataType[operands.size()];
      if (operandTypeInference == null) {
        // TODO:  eventually should assert(operandTypeInference != null)
        // instead; for now just eat it
        Arrays.fill(operandTypes, unknownType);
      } else {
        operandTypeInference.inferOperandTypes(
            new SqlCallBinding(this, scope, call),
            inferredType,
            operandTypes);
      }
      for (int i = 0; i < operands.size(); ++i) {
        inferUnknownTypes(operandTypes[i], scope, operands.get(i));
      }
    }
  }

  /**
   * Adds an expression to a select list, ensuring that its alias does not
   * clash with any existing expressions on the list.
   */
  protected void addToSelectList(
      List<SqlNode> list,
      Set<String> aliases,
      List<Map.Entry<String, RelDataType>> fieldList,
      SqlNode exp,
      SqlValidatorScope scope,
      final boolean includeSystemVars) {
    String alias = SqlValidatorUtil.getAlias(exp, -1); //获取别名
    String uniqueAlias =
        SqlValidatorUtil.uniquify(
            alias, aliases, SqlValidatorUtil.EXPR_SUGGESTER);//返回唯一的名字
    if (!alias.equals(uniqueAlias)) { //说明别名有变化，因此套一层call
      exp = SqlValidatorUtil.addAlias(exp, uniqueAlias);
    }
    fieldList.add(Pair.of(uniqueAlias, deriveType(scope, exp)));//设置唯一别名与类型映射
    list.add(exp);
  }

  //获取select 中的别名，即推测一个别名
  public String deriveAlias(
      SqlNode node,
      int ordinal) {
    return SqlValidatorUtil.getAlias(node, ordinal);
  }

  // implement SqlValidator
  public void setIdentifierExpansion(boolean expandIdentifiers) {
    this.expandIdentifiers = expandIdentifiers;
  }

  // implement SqlValidator
  public void setColumnReferenceExpansion(
      boolean expandColumnReferences) {
    this.expandColumnReferences = expandColumnReferences;
  }

  // implement SqlValidator
  public boolean getColumnReferenceExpansion() {
    return expandColumnReferences;
  }

  // implement SqlValidator
  public void setCallRewrite(boolean rewriteCalls) {
    this.rewriteCalls = rewriteCalls;
  }

  public boolean shouldExpandIdentifiers() {
    return expandIdentifiers;
  }

  //是否允许中间结果 orderby
  protected boolean shouldAllowIntermediateOrderBy() {
    return true;
  }

  /**
   * Registers a new namespace, and adds it as a child of its parent scope.
   * Derived class can override this method to tinker with namespaces as they
   * are created.
   *
   * @param usingScope    Parent scope (which will want to look for things in
   *                      this namespace)
   * @param alias         Alias by which parent will refer to this namespace
   * @param ns            Namespace
   * @param forceNullable Whether to force the type of namespace to be
   */
  protected void registerNamespace(
      SqlValidatorScope usingScope,
      String alias,
      SqlValidatorNamespace ns,
      boolean forceNullable) {
    if (forceNullable) {
      ns.makeNullable();
    }
    namespaces.put(
        ns.getNode(),
        ns);
    if (usingScope != null) {
      usingScope.addChild(ns, alias);
    }
  }

  /**
   * Registers scopes and namespaces implied a relational expression in the
   * FROM clause.
   *
   * <p>{@code parentScope} and {@code usingScope} are often the same. They
   * differ when the namespace are not visible within the parent. (Example
   * needed.)
   *
   * <p>Likewise, {@code enclosingNode} and {@code node} are often the same.
   * {@code enclosingNode} is the topmost node within the FROM clause, from
   * which any decorations like an alias (<code>AS alias</code>) or a table
   * sample clause are stripped away to get {@code node}. Both are recorded in
   * the namespace.
   *
   * @param parentScope   Parent scope which this scope turns to in order to
   *                      resolve objects
   * @param usingScope    Scope whose child list this scope should add itself to
   * @param node          Node which namespace is based on
   * @param enclosingNode Outermost node for namespace, including decorations
   *                      such as alias and sample clause
   * @param alias         Alias
   * @param extendList    Definitions of extended columns
   * @param forceNullable Whether to force the type of namespace to be
   *                      nullable because it is in an outer join
   * @return registered node, usually the same as {@code node}
   */
  private SqlNode registerFrom(
      SqlValidatorScope parentScope,
      SqlValidatorScope usingScope,
      final SqlNode node,
      SqlNode enclosingNode,
      String alias,
      SqlNodeList extendList,
      boolean forceNullable) {
    final SqlKind kind = node.getKind();

    SqlNode expr;
    SqlNode newExpr;

    // Add an alias if necessary.
    SqlNode newNode = node;
    if (alias == null) {
      switch (kind) {
      case IDENTIFIER:
      case OVER:
        alias = deriveAlias(node, -1);
        if (alias == null) {
          alias = deriveAlias(node, nextGeneratedId++);
        }
        if (shouldExpandIdentifiers()) {
          newNode = SqlValidatorUtil.addAlias(node, alias);
        }
        break;

      case SELECT:
      case UNION:
      case INTERSECT:
      case EXCEPT:
      case VALUES:
      case UNNEST:
      case OTHER_FUNCTION:
      case COLLECTION_TABLE:

        // give this anonymous construct a name since later
        // query processing stages rely on it
        alias = deriveAlias(node, nextGeneratedId++);
        if (shouldExpandIdentifiers()) {
          // Since we're expanding identifiers, we should make the
          // aliases explicit too, otherwise the expanded query
          // will not be consistent if we convert back to SQL, e.g.
          // "select EXPR$1.EXPR$2 from values (1)".
          newNode = SqlValidatorUtil.addAlias(node, alias);
        }
        break;
      }
    }

    SqlCall call;
    SqlNode operand;
    SqlNode newOperand;

    switch (kind) {
    case AS:
      call = (SqlCall) node;
      if (alias == null) {
        alias = call.operand(1).toString();
      }
      SqlValidatorScope usingScope2 = usingScope;
      if (call.operandCount() > 2) {
        usingScope2 = null;
      }
      expr = call.operand(0);
      newExpr =
          registerFrom(
              parentScope,
              usingScope2,
              expr,
              enclosingNode,
              alias,
              extendList,
              forceNullable);
      if (newExpr != expr) {
        call.setOperand(0, newExpr);
      }

      // If alias has a column list, introduce a namespace to translate
      // column names.
      if (call.operandCount() > 2) {
        registerNamespace(
            usingScope,
            alias,
            new AliasNamespace(this, call, enclosingNode),
            false);
      }
      return node;

    case TABLESAMPLE:
      call = (SqlCall) node;
      expr = call.operand(0);
      newExpr =
          registerFrom(
              parentScope,
              usingScope,
              expr,
              enclosingNode,
              alias,
              extendList,
              forceNullable);
      if (newExpr != expr) {
        call.setOperand(0, newExpr);
      }
      return node;

    case JOIN:
      final SqlJoin join = (SqlJoin) node;
      final JoinScope joinScope =
          new JoinScope(parentScope, usingScope, join);
      scopes.put(join, joinScope);
      final SqlNode left = join.getLeft();
      final SqlNode right = join.getRight();
      final boolean rightIsLateral = isLateral(right);
      boolean forceLeftNullable = forceNullable;
      boolean forceRightNullable = forceNullable;
      switch (join.getJoinType()) {
      case LEFT:
        forceRightNullable = true;
        break;
      case RIGHT:
        forceLeftNullable = true;
        break;
      case FULL:
        forceLeftNullable = true;
        forceRightNullable = true;
        break;
      }
      final SqlNode newLeft =
          registerFrom(
              parentScope,
              joinScope,
              left,
              left,
              null,
              null,
              forceLeftNullable);
      if (newLeft != left) {
        join.setLeft(newLeft);
      }
      final SqlValidatorScope rightParentScope;
      if (rightIsLateral) {
        rightParentScope = joinScope;
      } else {
        rightParentScope = parentScope;
      }
      final SqlNode newRight =
          registerFrom(
              rightParentScope,
              joinScope,
              right,
              right,
              null,
              null,
              forceRightNullable);
      if (newRight != right) {
        join.setRight(newRight);
      }
      final JoinNamespace joinNamespace = new JoinNamespace(this, join);
      registerNamespace(null, null, joinNamespace, forceNullable);
      return join;

    case IDENTIFIER://xx.xx,应对与from xxx.xx
      final SqlIdentifier id = (SqlIdentifier) node;
      final IdentifierNamespace newNs =
          new IdentifierNamespace(
              this, id, extendList, enclosingNode,
              parentScope);
      registerNamespace(usingScope, alias, newNs, forceNullable);
      return newNode;

    case LATERAL:
      return registerFrom(
          parentScope,
          usingScope,
          ((SqlCall) node).operand(0),
          enclosingNode,
          alias,
          extendList,
          forceNullable);

    case COLLECTION_TABLE:
      call = (SqlCall) node;
      operand = call.operand(0);
      newOperand =
          registerFrom(
              parentScope,
              usingScope,
              operand,
              enclosingNode,
              alias,
              extendList,
              forceNullable);
      if (newOperand != operand) {
        call.setOperand(0, newOperand);
      }
      return newNode;

    case SELECT:
    case UNION:
    case INTERSECT:
    case EXCEPT:
    case VALUES:
    case WITH:
    case UNNEST:
    case OTHER_FUNCTION:
      if (alias == null) {
        alias = deriveAlias(node, nextGeneratedId++);
      }
      registerQuery(
          parentScope,
          usingScope,
          node,
          enclosingNode,
          alias,
          forceNullable);
      return newNode;

    case OVER:
      if (!shouldAllowOverRelation()) {
        throw Util.unexpected(kind);
      }
      call = (SqlCall) node;
      final OverScope overScope = new OverScope(usingScope, call);
      scopes.put(call, overScope);
      operand = call.operand(0);
      newOperand =
          registerFrom(
              parentScope,
              overScope,
              operand,
              enclosingNode,
              alias,
              extendList,
              forceNullable);
      if (newOperand != operand) {
        call.setOperand(0, newOperand);
      }

      for (Pair<String, SqlValidatorNamespace> p : overScope.children) {
        registerNamespace(
            usingScope,
            p.left,
            p.right,
            forceNullable);
      }

      return newNode;

    case EXTEND:
      final SqlCall extend = (SqlCall) node;
      return registerFrom(parentScope,
          usingScope,
          extend.getOperandList().get(0),
          extend,
          alias,
          (SqlNodeList) extend.getOperandList().get(1),
          forceNullable);

    default:
      throw Util.unexpected(kind);
    }
  }

  private static boolean isLateral(SqlNode node) {
    switch (node.getKind()) {
    case LATERAL:
    case UNNEST:
      // Per SQL std, UNNEST is implicitly LATERAL.
      return true;
    case AS:
      return isLateral(((SqlCall) node).operand(0));
    default:
      return false;
    }
  }

  protected boolean shouldAllowOverRelation() {
    return false;
  }

  /**
   * Creates a namespace for a <code>SELECT</code> node. Derived class may
   * override this factory method.
   *
   * @param select        Select node
   * @param enclosingNode Enclosing node
   * @return Select namespace
   */
  protected SelectNamespace createSelectNamespace(
      SqlSelect select,
      SqlNode enclosingNode) {
    return new SelectNamespace(this, select, enclosingNode);
  }

  /**
   * Creates a namespace for a set operation (<code>UNION</code>, <code>
   * INTERSECT</code>, or <code>EXCEPT</code>). Derived class may override
   * this factory method.
   *
   * @param call          Call to set operation
   * @param enclosingNode Enclosing node
   * @return Set operation namespace
   */
  protected SetopNamespace createSetopNamespace(
      SqlCall call,
      SqlNode enclosingNode) {
    return new SetopNamespace(this, call, enclosingNode);
  }

  /**
   * Registers a query in a parent scope.
   *
   * @param parentScope Parent scope which this scope turns to in order to
   *                    resolve objects
   * @param usingScope  Scope whose child list this scope should add itself to
   * @param node        Query node
   * @param alias       Name of this query within its parent. Must be specified
   *                    if usingScope != null
   * @pre usingScope == null || alias != null
   */
  private void registerQuery(
      SqlValidatorScope parentScope,
      SqlValidatorScope usingScope,
      SqlNode node,
      SqlNode enclosingNode,
      String alias,
      boolean forceNullable) {
    registerQuery(
        parentScope,
        usingScope,
        node,
        enclosingNode,
        alias,
        forceNullable,
        true);
  }

  /**
   * Registers a query in a parent scope.
   *
   * @param parentScope Parent scope which this scope turns to in order to
   *                    resolve objects
   * @param usingScope  Scope whose child list this scope should add itself to
   * @param node        Query node
   * @param alias       Name of this query within its parent. Must be specified
   *                    if usingScope != null
   * @param checkUpdate if true, validate that the update feature is supported
   *                    if validating the update statement
   * @pre usingScope == null || alias != null
   */
  private void registerQuery(
      SqlValidatorScope parentScope,
      SqlValidatorScope usingScope,
      SqlNode node,
      SqlNode enclosingNode,//封闭的节点
      String alias,
      boolean forceNullable,
      boolean checkUpdate) {
    assert node != null;
    assert enclosingNode != null;
    assert usingScope == null || alias != null : usingScope;

    SqlCall call;
    List<SqlNode> operands;
    switch (node.getKind()) {
    case SELECT:
      final SqlSelect select = (SqlSelect) node;
      final SelectNamespace selectNs = createSelectNamespace(select, enclosingNode);//创建select表空间,即该表有什么字段
      registerNamespace(usingScope, alias, selectNs, forceNullable);//注册表空间

      final SqlValidatorScope windowParentScope = (usingScope != null) ? usingScope : parentScope;
      SelectScope selectScope = new SelectScope(parentScope, windowParentScope, select);//创建select的scope
      scopes.put(select, selectScope);

      // Start by registering the WHERE clause
      whereScopes.put(select, selectScope);//处理where语句
      registerOperandSubqueries(
          selectScope,
          select,
          SqlSelect.WHERE_OPERAND);//处理where语法节点

      // Register FROM with the inherited scope 'parentScope', not
      // 'selectScope', otherwise tables in the FROM clause would be
      // able to see each other.
      final SqlNode from = select.getFrom();
      final SqlNode newFrom =
          registerFrom(
              parentScope,
              selectScope,
              from,
              from,
              null,
              null,
              false);
      if (newFrom != from) {
        select.setFrom(newFrom);
      }

      // If this is an aggregating query, the SELECT list and HAVING
      // clause use a different scope, where you can only reference
      // columns which are in the GROUP BY clause.
      SqlValidatorScope aggScope = selectScope;
      if (isAggregate(select)) {
        aggScope =
            new AggregatingSelectScope(selectScope, select, false);
        selectScopes.put(select, aggScope);
      } else {
        selectScopes.put(select, selectScope);
      }
      registerSubqueries(selectScope, select.getGroup());
      registerOperandSubqueries(
          aggScope,
          select,
          SqlSelect.HAVING_OPERAND);
      registerSubqueries(aggScope, select.getSelectList());
      final SqlNodeList orderList = select.getOrderList();
      if (orderList != null) {
        // If the query is 'SELECT DISTINCT', restrict the columns
        // available to the ORDER BY clause.
        if (select.isDistinct()) {
          aggScope =
              new AggregatingSelectScope(selectScope, select, true);
        }
        OrderByScope orderScope =
            new OrderByScope(aggScope, orderList, select);
        orderScopes.put(select, orderScope);
        registerSubqueries(orderScope, orderList);

        if (!isAggregate(select)) {
          // Since this is not an aggregating query,
          // there cannot be any aggregates in the ORDER BY clause.
          SqlNode agg = aggFinder.findAgg(orderList);
          if (agg != null) {
            throw newValidationError(agg, RESOURCE.aggregateIllegalInOrderBy());
          }
        }
      }
      break;

    case INTERSECT:
      validateFeature(RESOURCE.sQLFeature_F302(), node.getParserPosition());
      registerSetop(
          parentScope,
          usingScope,
          node,
          node,
          alias,
          forceNullable);
      break;

    case EXCEPT:
      validateFeature(RESOURCE.sQLFeature_E071_03(), node.getParserPosition());
      registerSetop(
          parentScope,
          usingScope,
          node,
          node,
          alias,
          forceNullable);
      break;

    case UNION:
      registerSetop(
          parentScope,
          usingScope,
          node,
          node,
          alias,
          forceNullable);
      break;

    case WITH:
      registerWith(parentScope, usingScope, (SqlWith) node, enclosingNode,
          alias, forceNullable, checkUpdate);
      break;

    case VALUES:
      call = (SqlCall) node;
      scopes.put(call, parentScope);
      final TableConstructorNamespace tableConstructorNamespace =
          new TableConstructorNamespace(
              this,
              call,
              parentScope,
              enclosingNode);
      registerNamespace(
          usingScope,
          alias,
          tableConstructorNamespace,
          forceNullable);
      operands = call.getOperandList();
      for (int i = 0; i < operands.size(); ++i) {
        assert operands.get(i).getKind() == SqlKind.ROW;

        // FIXME jvs 9-Feb-2005:  Correlation should
        // be illegal in these subqueries.  Same goes for
        // any non-lateral SELECT in the FROM list.
        registerOperandSubqueries(parentScope, call, i);
      }
      break;

    case INSERT:
      SqlInsert insertCall = (SqlInsert) node;
      InsertNamespace insertNs =
          new InsertNamespace(
              this,
              insertCall,
              enclosingNode,
              parentScope);
      registerNamespace(usingScope, null, insertNs, forceNullable);
      registerQuery(
          parentScope,
          usingScope,
          insertCall.getSource(),
          enclosingNode,
          null,
          false);
      break;

    case DELETE:
      SqlDelete deleteCall = (SqlDelete) node;
      DeleteNamespace deleteNs =
          new DeleteNamespace(
              this,
              deleteCall,
              enclosingNode,
              parentScope);
      registerNamespace(usingScope, null, deleteNs, forceNullable);
      registerQuery(
          parentScope,
          usingScope,
          deleteCall.getSourceSelect(),
          enclosingNode,
          null,
          false);
      break;

    case UPDATE:
      if (checkUpdate) {
        validateFeature(RESOURCE.sQLFeature_E101_03(),
            node.getParserPosition());
      }
      SqlUpdate updateCall = (SqlUpdate) node;
      UpdateNamespace updateNs =
          new UpdateNamespace(
              this,
              updateCall,
              enclosingNode,
              parentScope);
      registerNamespace(usingScope, null, updateNs, forceNullable);
      registerQuery(
          parentScope,
          usingScope,
          updateCall.getSourceSelect(),
          enclosingNode,
          null,
          false);
      break;

    case MERGE:
      validateFeature(RESOURCE.sQLFeature_F312(), node.getParserPosition());
      SqlMerge mergeCall = (SqlMerge) node;
      MergeNamespace mergeNs =
          new MergeNamespace(
              this,
              mergeCall,
              enclosingNode,
              parentScope);
      registerNamespace(usingScope, null, mergeNs, forceNullable);
      registerQuery(
          parentScope,
          usingScope,
          mergeCall.getSourceSelect(),
          enclosingNode,
          null,
          false);

      // update call can reference either the source table reference
      // or the target table, so set its parent scope to the merge's
      // source select; when validating the update, skip the feature
      // validation check
      if (mergeCall.getUpdateCall() != null) {
        registerQuery(
            whereScopes.get(mergeCall.getSourceSelect()),
            null,
            mergeCall.getUpdateCall(),
            enclosingNode,
            null,
            false,
            false);
      }
      if (mergeCall.getInsertCall() != null) {
        registerQuery(
            parentScope,
            null,
            mergeCall.getInsertCall(),
            enclosingNode,
            null,
            false);
      }
      break;

    case UNNEST:
      call = (SqlCall) node;
      final UnnestNamespace unnestNs =
          new UnnestNamespace(this, call, parentScope, enclosingNode);
      registerNamespace(
          usingScope,
          alias,
          unnestNs,
          forceNullable);
      registerOperandSubqueries(parentScope, call, 0);
      break;

    case OTHER_FUNCTION:
      call = (SqlCall) node;
      ProcedureNamespace procNs =
          new ProcedureNamespace(
              this,
              parentScope,
              call,
              enclosingNode);
      registerNamespace(
          usingScope,
          alias,
          procNs,
          forceNullable);
      registerSubqueries(parentScope, call);
      break;

    case MULTISET_QUERY_CONSTRUCTOR:
    case MULTISET_VALUE_CONSTRUCTOR:
      validateFeature(RESOURCE.sQLFeature_S271(), node.getParserPosition());
      call = (SqlCall) node;
      CollectScope cs = new CollectScope(parentScope, usingScope, call);
      final CollectNamespace tableConstructorNs =
          new CollectNamespace(call, cs, enclosingNode);
      final String alias2 = deriveAlias(node, nextGeneratedId++);
      registerNamespace(
          usingScope,
          alias2,
          tableConstructorNs,
          forceNullable);
      operands = call.getOperandList();
      for (int i = 0; i < operands.size(); i++) {
        registerOperandSubqueries(parentScope, call, i);
      }
      break;

    default:
      throw Util.unexpected(node.getKind());
    }
  }

  private void registerSetop(
      SqlValidatorScope parentScope,
      SqlValidatorScope usingScope,
      SqlNode node,
      SqlNode enclosingNode,
      String alias,
      boolean forceNullable) {
    SqlCall call = (SqlCall) node;
    final SetopNamespace setopNamespace =
        createSetopNamespace(call, enclosingNode);
    registerNamespace(usingScope, alias, setopNamespace, forceNullable);

    // A setop is in the same scope as its parent.
    scopes.put(call, parentScope);
    for (SqlNode operand : call.getOperandList()) {
      registerQuery(
          parentScope,
          null,
          operand,
          operand,
          null,
          false);
    }
  }

  private void registerWith(
      SqlValidatorScope parentScope,
      SqlValidatorScope usingScope,
      SqlWith with,
      SqlNode enclosingNode,
      String alias,
      boolean forceNullable,
      boolean checkUpdate) {
    final WithNamespace withNamespace =
        new WithNamespace(this, with, enclosingNode);
    registerNamespace(usingScope, alias, withNamespace, forceNullable);

    SqlValidatorScope scope = parentScope;
    for (SqlNode withItem_ : with.withList) {
      final SqlWithItem withItem = (SqlWithItem) withItem_;
      final WithScope withScope = new WithScope(scope, withItem);
      scopes.put(withItem, withScope);

      registerQuery(scope, null, withItem.query, with,
          withItem.name.getSimple(), false);
      registerNamespace(null, alias,
          new WithItemNamespace(this, withItem, enclosingNode),
          false);
      scope = withScope;
    }

    registerQuery(scope, null, with.body, enclosingNode, alias, forceNullable,
        checkUpdate);
  }

  public boolean isAggregate(SqlSelect select) {
    return select.getGroup() != null
        || select.getHaving() != null
        || getAgg(select) != null;
  }

  private SqlNode getAgg(SqlSelect select) {
    final SelectScope selectScope = getRawSelectScope(select);
    if (selectScope != null) {
      final List<SqlNode> selectList = selectScope.getExpandedSelectList();
      if (selectList != null) {
        return aggFinder.findAgg(selectList);
      }
    }
    return aggFinder.findAgg(select.getSelectList());
  }

  public boolean isAggregate(SqlNode selectNode) {
    return aggFinder.findAgg(selectNode) != null;
  }

  private void validateNodeFeature(SqlNode node) {
    switch (node.getKind()) {
    case MULTISET_VALUE_CONSTRUCTOR:
      validateFeature(RESOURCE.sQLFeature_S271(), node.getParserPosition());
      break;
    }
  }

  private void registerSubqueries(
      SqlValidatorScope parentScope,//join对应的scope
      SqlNode node) {//aa.id = bb.id
    if (node == null) {
      return;
    }
    if (node.getKind().belongsTo(SqlKind.QUERY)
        || node.getKind() == SqlKind.MULTISET_QUERY_CONSTRUCTOR
        || node.getKind() == SqlKind.MULTISET_VALUE_CONSTRUCTOR) {
      registerQuery(parentScope, null, node, node, null, false);
    } else if (node instanceof SqlCall) {
      validateNodeFeature(node);
      SqlCall call = (SqlCall) node;
      for (int i = 0; i < call.operandCount(); i++) {
        registerOperandSubqueries(parentScope, call, i);
      }
    } else if (node instanceof SqlNodeList) {
      SqlNodeList list = (SqlNodeList) node;
      for (int i = 0, count = list.size(); i < count; i++) {
        SqlNode listNode = list.get(i);
        if (listNode.getKind().belongsTo(SqlKind.QUERY)) {
          listNode =
              SqlStdOperatorTable.SCALAR_QUERY.createCall(
                  listNode.getParserPosition(),
                  listNode);
          list.set(i, listNode);
        }
        registerSubqueries(parentScope, listNode);
      }
    } else {
      // atomic node -- can be ignored
    }
  }

  /**
   * Registers any subqueries inside a given call operand, and converts the
   * operand to a scalar subquery if the operator requires it.
   *
   * @param parentScope    Parent scope
   * @param call           Call 比如select
   * @param operandOrdinal Ordinal of operand within call 获取call的第几个参数,比如获取select中的where节点
   * @see SqlOperator#argumentMustBeScalar(int)
   */
  private void registerOperandSubqueries(
      SqlValidatorScope parentScope,
      SqlCall call,
      int operandOrdinal) {
    SqlNode operand = call.operand(operandOrdinal);//比如获取where节点
    if (operand == null) {
      return;
    }
    if (operand.getKind().belongsTo(SqlKind.QUERY)
        && call.getOperator().argumentMustBeScalar(operandOrdinal)) {
      operand = SqlStdOperatorTable.SCALAR_QUERY.createCall(
              operand.getParserPosition(),
              operand);
      call.setOperand(operandOrdinal, operand);
    }
    registerSubqueries(parentScope, operand);
  }

  public void validateIdentifier(SqlIdentifier id, SqlValidatorScope scope) {
    final SqlQualified fqId = scope.fullyQualify(id);
    if (expandColumnReferences) {
      // NOTE jvs 9-Apr-2007: this doesn't cover ORDER BY, which has its
      // own ideas about qualification.
      id.assignNamesFrom(fqId.identifier);
    } else {
      Util.discard(fqId);
    }
  }

  //校验字面量sqlNode字符内容
  public void validateLiteral(SqlLiteral literal) {
    switch (literal.getTypeName()) {//字面量对应的类型
    case DECIMAL://校验必须能转换成DECIMAL类型
      // Decimal and long have the same precision (as 64-bit integers), so
      // the unscaled value of a decimal must fit into a long.

      // REVIEW jvs 4-Aug-2004:  This should probably be calling over to
      // the available calculator implementations to see what they
      // support.  For now use ESP instead.
      //
      // jhyde 2006/12/21: I think the limits should be baked into the
      // type system, not dependent on the calculator implementation.
      BigDecimal bd = (BigDecimal) literal.getValue();
      BigInteger unscaled = bd.unscaledValue();
      long longValue = unscaled.longValue();
      if (!BigInteger.valueOf(longValue).equals(unscaled)) {
        // overflow
        throw newValidationError(literal,
            RESOURCE.numberLiteralOutOfRange(bd.toString()));
      }
      break;

    case DOUBLE:
      validateLiteralAsDouble(literal);//校验必须能转换成double类型
      break;

    case BINARY:
      final BitString bitString = (BitString) literal.getValue();
      if ((bitString.getBitCount() % 8) != 0) {
        throw newValidationError(literal, RESOURCE.binaryLiteralOdd());
      }
      break;

    case DATE:
    case TIME:
    case TIMESTAMP:
      Calendar calendar = (Calendar) literal.getValue();
      final int year = calendar.get(Calendar.YEAR);
      final int era = calendar.get(Calendar.ERA);
      if (year < 1 || era == GregorianCalendar.BC || year > 9999) {
        throw newValidationError(literal,
            RESOURCE.dateLiteralOutOfRange(literal.toString()));
      }
      break;

    case INTERVAL_YEAR_MONTH:
    case INTERVAL_DAY_TIME:
      if (literal instanceof SqlIntervalLiteral) {
        SqlIntervalLiteral.IntervalValue interval =
            (SqlIntervalLiteral.IntervalValue)
                literal.getValue();
        SqlIntervalQualifier intervalQualifier =
            interval.getIntervalQualifier();

        // ensure qualifier is good before attempting to validate literal
        validateIntervalQualifier(intervalQualifier);
        String intervalStr = interval.getIntervalLiteral();
        // throws CalciteContextException if string is invalid
        int[] values = intervalQualifier.evaluateIntervalLiteral(intervalStr,
            literal.getParserPosition(), typeFactory.getTypeSystem());
        Util.discard(values);
      }
      break;
    default:
      // default is to do nothing
    }
  }

  //必须是一个double值
  private void validateLiteralAsDouble(SqlLiteral literal) {
    BigDecimal bd = (BigDecimal) literal.getValue();
    double d = bd.doubleValue();
    if (Double.isInfinite(d) || Double.isNaN(d)) {
      // overflow
      throw newValidationError(literal,
          RESOURCE.numberLiteralOutOfRange(Util.toScientificNotation(bd)));
    }

    // REVIEW jvs 4-Aug-2004:  what about underflow?
  }

  //处理日期内容
  public void validateIntervalQualifier(SqlIntervalQualifier qualifier) {
    assert qualifier != null;
    boolean startPrecisionOutOfRange = false;
    boolean fractionalSecondPrecisionOutOfRange = false;
    final RelDataTypeSystem typeSystem = typeFactory.getTypeSystem();

    final int startPrecision = qualifier.getStartPrecision(typeSystem);
    final int fracPrecision =
        qualifier.getFractionalSecondPrecision(typeSystem);
    final int maxPrecision = typeSystem.getMaxPrecision(qualifier.typeName());
    final int minPrecision = qualifier.typeName().getMinPrecision();
    final int minScale = qualifier.typeName().getMinScale();
    final int maxScale = typeSystem.getMaxScale(qualifier.typeName());
    if (qualifier.isYearMonth()) {
      if (startPrecision < minPrecision || startPrecision > maxPrecision) {
        startPrecisionOutOfRange = true;
      } else {
        if (fracPrecision < minScale || fracPrecision > maxScale) {
          fractionalSecondPrecisionOutOfRange = true;
        }
      }
    } else {
      if (startPrecision < minPrecision || startPrecision > maxPrecision) {
        startPrecisionOutOfRange = true;
      } else {
        if (fracPrecision < minScale || fracPrecision > maxScale) {
          fractionalSecondPrecisionOutOfRange = true;
        }
      }
    }

    if (startPrecisionOutOfRange) {
      throw newValidationError(qualifier,
          RESOURCE.intervalStartPrecisionOutOfRange(startPrecision,
              "INTERVAL " + qualifier));
    } else if (fractionalSecondPrecisionOutOfRange) {
      throw newValidationError(qualifier,
          RESOURCE.intervalFractionalSecondPrecisionOutOfRange(
              fracPrecision,
              "INTERVAL " + qualifier));
    }
  }

  /**
   * Validates the FROM clause of a query, or (recursively) a child node of
   * the FROM clause: AS, OVER, JOIN, VALUES, or subquery.
   *
   * @param node          Node in FROM clause, typically a table or derived
   *                      table
   * @param targetRowType Desired row type of this expression, or
   *                      {@link #unknownType} if not fussy. Must not be null.
   * @param scope         Scope
   */
  protected void validateFrom(
      SqlNode node,
      RelDataType targetRowType,
      SqlValidatorScope scope) {
    Util.pre(targetRowType != null, "targetRowType != null");
    switch (node.getKind()) {
    case AS:
      validateFrom( // user as u ，此时sqlNode是SqlIdentifier
          ((SqlCall) node).operand(0),
          targetRowType,
          scope);
      break;
    case VALUES:
      validateValues((SqlCall) node, targetRowType, scope);
      break;
    case JOIN:
      validateJoin((SqlJoin) node, scope);
      break;
    case OVER:
      validateOver((SqlCall) node, scope);
      break;
    default:
      validateQuery(node, scope); // from user,此时sqlNode是SqlIdentifier
      break;
    }

    // Validate the namespace representation of the node, just in case the
    // validation did not occur implicitly.
    getNamespace(node, scope).validate();
  }

  protected void validateOver(SqlCall call, SqlValidatorScope scope) {
    throw Util.newInternal("OVER unexpected in this context");
  }

  protected void validateJoin(SqlJoin join, SqlValidatorScope scope) {
    SqlNode left = join.getLeft();
    SqlNode right = join.getRight();
    SqlNode condition = join.getCondition();
    boolean natural = join.isNatural();
    final JoinType joinType = join.getJoinType();//join、left join
    final JoinConditionType conditionType = join.getConditionType();//on
    final SqlValidatorScope joinScope = scopes.get(join);
    validateFrom(left, unknownType, joinScope);
    validateFrom(right, unknownType, joinScope);

    // Validate condition.
    switch (conditionType) {
    case NONE:
      Util.permAssert(condition == null, "condition == null");
      break;
    case ON:
      Util.permAssert(condition != null, "condition != null");
      validateWhereOrOn(joinScope, condition, "ON");
      break;
    case USING:
      SqlNodeList list = (SqlNodeList) condition;

      // Parser ensures that using clause is not empty.
      Util.permAssert(list.size() > 0, "Empty USING clause");
      for (int i = 0; i < list.size(); i++) {
        SqlIdentifier id = (SqlIdentifier) list.get(i);
        final RelDataType leftColType = validateUsingCol(id, left);
        final RelDataType rightColType = validateUsingCol(id, right);
        if (!SqlTypeUtil.isComparable(leftColType, rightColType)) {
          throw newValidationError(id,
              RESOURCE.naturalOrUsingColumnNotCompatible(id.getSimple(),
                  leftColType.toString(), rightColType.toString()));
        }
      }
      break;
    default:
      throw Util.unexpected(conditionType);
    }

    // Validate NATURAL.
    if (natural) {
      if (condition != null) {
        throw newValidationError(condition,
            RESOURCE.naturalDisallowsOnOrUsing());
      }

      // Join on fields that occur exactly once on each side. Ignore
      // fields that occur more than once on either side.
      final RelDataType leftRowType = getNamespace(left).getRowType();
      final RelDataType rightRowType = getNamespace(right).getRowType();
      List<String> naturalColumnNames =
          SqlValidatorUtil.deriveNaturalJoinColumnList(
              leftRowType,
              rightRowType);

      // Check compatibility of the chosen columns.
      for (String name : naturalColumnNames) {
        final RelDataType leftColType =
            catalogReader.field(leftRowType, name).getType();
        final RelDataType rightColType =
            catalogReader.field(rightRowType, name).getType();
        if (!SqlTypeUtil.isComparable(leftColType, rightColType)) {
          throw newValidationError(join,
              RESOURCE.naturalOrUsingColumnNotCompatible(name,
                  leftColType.toString(), rightColType.toString()));
        }
      }
    }

    // Which join types require/allow a ON/USING condition, or allow
    // a NATURAL keyword?
    switch (joinType) {
    case INNER:
    case LEFT:
    case RIGHT:
    case FULL:
      if ((condition == null) && !natural) {
        throw newValidationError(join, RESOURCE.joinRequiresCondition());
      }
      break;
    case COMMA:
    case CROSS:
      if (condition != null) {
        throw newValidationError(join.getConditionTypeNode(),
            RESOURCE.crossJoinDisallowsCondition());
      }
      if (natural) {
        throw newValidationError(join.getConditionTypeNode(),
            RESOURCE.crossJoinDisallowsCondition());
      }
      break;
    default:
      throw Util.unexpected(joinType);
    }
  }

  /**
   * Throws an error if there is an aggregate or windowed aggregate in the
   * given clause.
   *
   * @param condition Parse tree
   * @param clause    Name of clause: "WHERE", "GROUP BY", "ON"
   * 用于where语句条件的校验,因此不允许有聚合函数和窗口函数
   *
   * 校验SqlNode节点不允许有聚合函数
   */
  private void validateNoAggs(SqlNode condition, String clause) {
    final SqlNode agg = aggOrOverFinder.findAgg(condition);
    if (agg != null) {
      if (SqlUtil.isCallTo(agg, SqlStdOperatorTable.OVER)) {
        throw newValidationError(agg,
            RESOURCE.windowedAggregateIllegalInClause(clause));
      } else {
        throw newValidationError(agg,
            RESOURCE.aggregateIllegalInClause(clause));
      }
    }
  }

  private RelDataType validateUsingCol(SqlIdentifier id, SqlNode leftOrRight) {
    if (id.names.size() == 1) {
      String name = id.names.get(0);
      final SqlValidatorNamespace namespace = getNamespace(leftOrRight);
      final RelDataType rowType = namespace.getRowType();
      final RelDataTypeField field = catalogReader.field(rowType, name);
      if (field != null) {
        if (Collections.frequency(rowType.getFieldNames(), name) > 1) {
          throw newValidationError(id,
              RESOURCE.columnInUsingNotUnique(id.toString()));
        }
        return field.getType();
      }
    }
    throw newValidationError(id, RESOURCE.columnNotFound(id.toString()));
  }

  /**
   * Validates a SELECT statement.
   * 校验selectNode
   * @param select        Select statement
   * @param targetRowType Desired row type, must not be null, may be the data
   *                      type 'unknown'.
   */
  protected void validateSelect(
      SqlSelect select,
      RelDataType targetRowType) {
    assert targetRowType != null;

    // Namespace is either a select namespace or a wrapper around one.
    final SelectNamespace ns =
        getNamespace(select).unwrap(SelectNamespace.class);//强转成SelectNamespace

    // Its rowtype is null, meaning it hasn't been validated yet.
    // This is important, because we need to take the targetRowType into
    // account.
    assert ns.rowType == null;

    if (select.isDistinct()) {
      validateFeature(RESOURCE.sQLFeature_E051_01(),//校验关键字的位置?后续看到runtime的时候补一下
          select.getModifierNode(SqlSelectKeyword.DISTINCT)//获取distinct关键字的位置
              .getParserPosition());
    }

    final SqlNodeList selectItems = select.getSelectList();
    RelDataType fromType = unknownType;
    if (selectItems.size() == 1) {
      final SqlNode selectItem = selectItems.get(0);
      if (selectItem instanceof SqlIdentifier) {
        SqlIdentifier id = (SqlIdentifier) selectItem;
        if (id.isStar() && (id.names.size() == 1)) {
          // Special case: for INSERT ... VALUES(?,?), the SQL
          // standard says we're supposed to propagate the target
          // types down.  So iff the select list is an unqualified
          // star (as it will be after an INSERT ... VALUES has been
          // expanded), then propagate.
          fromType = targetRowType;
        }
      }
    }

    // Make sure that items in FROM clause have distinct aliases.
    final SqlValidatorScope fromScope = getFromScope(select);
    final List<Pair<String, SqlValidatorNamespace>> children = ((SelectScope) fromScope).children;//返回from 表集合,比如查询from a,b 则返回a与b
    //返回出现重复数据的位置
    int duplicateAliasOrdinal = Util.firstDuplicate(Pair.left(children));//校验a和b等from的表名集合中,是否有重复的
    if (duplicateAliasOrdinal >= 0) { //说明有重复数据
      final Pair<String, SqlValidatorNamespace> child = children.get(duplicateAliasOrdinal);
      throw newValidationError(child.right.getEnclosingNode(),RESOURCE.fromAliasDuplicate(child.left));
    }

    validateFrom(select.getFrom(), fromType, fromScope);
    validateWhereClause(select);
    validateGroupClause(select);
    validateHavingClause(select);
    validateWindowClause(select);

    // Validate the SELECT clause late, because a select item might
    // depend on the GROUP BY list, or the window function might reference
    // window name in the WINDOW clause etc.
    final RelDataType rowType =
        validateSelectList(selectItems, select, targetRowType);//校验selectItem
    ns.setType(rowType);//设置输出row的类型

    // Validate ORDER BY after we have set ns.rowType because in some
    // dialects you can refer to columns of the select list, e.g.
    // "SELECT empno AS x FROM emp ORDER BY x"
    validateOrderList(select);
  }

  protected void validateWindowClause(SqlSelect select) {
    final SqlNodeList windowList = select.getWindowList();
    if ((windowList == null) || (windowList.size() == 0)) {
      return;
    }

    final SelectScope windowScope = (SelectScope) getFromScope(select);
    assert windowScope != null;

    // 1. ensure window names are simple
    // 2. ensure they are unique within this scope
    for (SqlNode node : windowList) {
      final SqlWindow child = (SqlWindow) node;
      SqlIdentifier declName = child.getDeclName();
      if (!declName.isSimple()) {
        throw newValidationError(declName, RESOURCE.windowNameMustBeSimple());
      }

      if (windowScope.existingWindowName(declName.toString())) {
        throw newValidationError(declName, RESOURCE.duplicateWindowName());
      } else {
        windowScope.addWindowName(declName.toString());
      }
    }

    // 7.10 rule 2
    // Check for pairs of windows which are equivalent.
    for (int i = 0; i < windowList.size(); i++) {
      SqlNode window1 = windowList.get(i);
      for (int j = i + 1; j < windowList.size(); j++) {
        SqlNode window2 = windowList.get(j);
        if (window1.equalsDeep(window2, false)) {
          throw newValidationError(window2, RESOURCE.dupWindowSpec());
        }
      }
    }

    // Hand off to validate window spec components
    windowList.validate(this, windowScope);
  }

  public void validateWith(SqlWith with, SqlValidatorScope scope) {
    final SqlValidatorNamespace namespace = getNamespace(with);
    validateNamespace(namespace);
  }

  public void validateWithItem(SqlWithItem withItem) {
    if (withItem.columnList != null) {
      final RelDataType rowType = getValidatedNodeType(withItem.query);
      final int fieldCount = rowType.getFieldCount();
      if (withItem.columnList.size() != fieldCount) {
        throw newValidationError(withItem.columnList,
            RESOURCE.columnCountMismatch());
      }
      final List<String> names = Lists.transform(withItem.columnList.getList(),
          new Function<SqlNode, String>() {
            public String apply(SqlNode o) {
              return ((SqlIdentifier) o).getSimple();
            }
          });
      final int i = Util.firstDuplicate(names);
      if (i >= 0) {
        throw newValidationError(withItem.columnList.get(i),
            RESOURCE.duplicateNameInColumnList(names.get(i)));
      }
    } else {
      // Luckily, field names have not been make unique yet.
      final List<String> fieldNames =
          getValidatedNodeType(withItem.query).getFieldNames();
      final int i = Util.firstDuplicate(fieldNames);
      if (i >= 0) {
        throw newValidationError(withItem.query,
            RESOURCE.duplicateColumnAndNoColumnList(fieldNames.get(i)));
      }
    }
  }

  public SqlValidatorScope getWithScope(SqlNode withItem) {
    assert withItem.getKind() == SqlKind.WITH_ITEM;
    return scopes.get(withItem);
  }

  /**
   * Validates the ORDER BY clause of a SELECT statement.
   *
   * @param select Select statement
   * 校验order by语法
   *
   */
  protected void validateOrderList(SqlSelect select) {
    // ORDER BY is validated in a scope where aliases in the SELECT clause
    // are visible. For example, "SELECT empno AS x FROM emp ORDER BY x"
    // is valid.
    SqlNodeList orderList = select.getOrderList();
    if (orderList == null) {
      return;
    }
    if (!shouldAllowIntermediateOrderBy()) { //是否允许中间结果 orderby
      if (!cursorSet.contains(select)) {
        throw newValidationError(select, RESOURCE.invalidOrderByPos());
      }
    }
    final SqlValidatorScope orderScope = getOrderScope(select);

    Util.permAssert(orderScope != null, "orderScope != null");
    for (SqlNode orderItem : orderList) {
      validateOrderItem(select, orderItem);//校验每一个order by item
    }
  }

  private void validateOrderItem(SqlSelect select, SqlNode orderItem) {
    if (SqlUtil.isCallTo(
        orderItem,
        SqlStdOperatorTable.DESC)) {//是否是desc排序
      validateFeature(RESOURCE.sQLConformance_OrderByDesc(),
          orderItem.getParserPosition());
      validateOrderItem(select,
          ((SqlCall) orderItem).operand(0));
      return;
    }

    final SqlValidatorScope orderScope = getOrderScope(select);
    validateExpr(orderItem, orderScope);//校验order by的item表达式
  }


  public SqlNode expandOrderExpr(SqlSelect select, SqlNode orderExpr) {
    return new OrderExpressionExpander(select, orderExpr).go();
  }

  /**
   * Validates the GROUP BY clause of a SELECT statement. This method is
   * called even if no GROUP BY clause is present.
   * 校验group by 语法
   * 校验group by没有聚合函数
   * 校验节点本身信息
   * 校验每一个group by item语法
   */
  protected void validateGroupClause(SqlSelect select) {
    SqlNodeList groupList = select.getGroup();
    if (groupList == null) {
      return;
    }
    validateNoAggs(groupList, "GROUP BY");//校验group by没有聚合函数
    final SqlValidatorScope groupScope = getGroupScope(select);
    inferUnknownTypes(unknownType, groupScope, groupList);

    groupList.validate(this, groupScope);//校验节点本身信息

    // Derive the type of each GROUP BY item. We don't need the type, but
    // it resolves functions, and that is necessary for deducing
    // monotonicity.
    //推导每一个group by item的类型,这个类型本身是没用到的,但他在推论函数的单调性上,是必要的
    final SqlValidatorScope selectScope = getSelectScope(select);
    AggregatingSelectScope aggregatingScope = null;
    if (selectScope instanceof AggregatingSelectScope) {
      aggregatingScope = (AggregatingSelectScope) selectScope;
    }
    for (SqlNode groupItem : groupList) {
      if (groupItem instanceof SqlNodeList
          && ((SqlNodeList) groupItem).size() == 0) {
        continue;
      }
      validateGroupItem(groupScope, aggregatingScope, groupItem);
    }

    //group by字段里不能有聚合函数
    SqlNode agg = aggFinder.findAgg(groupList);
    if (agg != null) {
      throw newValidationError(agg, RESOURCE.aggregateIllegalInGroupBy());
    }
  }

  private void validateGroupItem(SqlValidatorScope groupScope,
      AggregatingSelectScope aggregatingScope,
      SqlNode groupItem) {
    switch (groupItem.getKind()) {
    case GROUPING_SETS:
    case ROLLUP:
    case CUBE:
      validateGroupingSets(groupScope, aggregatingScope, (SqlCall) groupItem);
      break;
    default:
      if (groupItem instanceof SqlNodeList) {
        break;
      }
      final RelDataType type = deriveType(groupScope, groupItem);//校验每一个group by item的推测返回值类型
      setValidatedNodeTypeImpl(groupItem, type);//设置每一个类型的返回值
    }
  }

  private void validateGroupingSets(SqlValidatorScope groupScope,
      AggregatingSelectScope aggregatingScope, SqlCall groupItem) {
    for (SqlNode node : groupItem.getOperandList()) {
      validateGroupItem(groupScope, aggregatingScope, node);
    }
  }

  protected void validateWhereClause(SqlSelect select) {
    // validate WHERE clause
    final SqlNode where = select.getWhere();
    if (where == null) {
      return;
    }
    final SqlValidatorScope whereScope = getWhereScope(select);
    validateWhereOrOn(whereScope, where, "WHERE");
  }

  /**
   * 校验where节点是否合法，或者on条件是否合法
   * @param scope
   * @param condition
   * @param keyword
   *
   *     conditionSqlNode.validate(this, scope);//校验sql节点本身
   *     推测返回值，校验返回值是否是boolean
   */
  protected void validateWhereOrOn(
      SqlValidatorScope scope,
      SqlNode condition,
      String keyword) {
    validateNoAggs(condition, keyword);//校验SqlNode节点不允许有聚合函数
    inferUnknownTypes(
        booleanType,
        scope,
        condition);
    condition.validate(this, scope);//校验节点本身
    final RelDataType type = deriveType(scope, condition);//推测返回值
    if (!SqlTypeUtil.inBooleanFamily(type)) { //返回值必须是boolean类型
      throw newValidationError(condition, RESOURCE.condMustBeBoolean(keyword));
    }
  }

  /**
   * 校验having是否都在group by里
   * 校验having节点本身
   * 校验返回值是否是boolean类型
   * @param select
   */
  protected void validateHavingClause(SqlSelect select) {
    // HAVING is validated in the scope after groups have been created.
    // For example, in "SELECT empno FROM emp WHERE empno = 10 GROUP BY
    // deptno HAVING empno = 10", the reference to 'empno' in the HAVING
    // clause is illegal.
    final SqlNode having = select.getHaving();
    if (having == null) {
      return;
    }
    final AggregatingScope havingScope =
        (AggregatingScope) getSelectScope(select);
    havingScope.checkAggregateExpr(having, true);
    inferUnknownTypes(
        booleanType,
        havingScope,
        having);
    having.validate(this, havingScope);
    final RelDataType type = deriveType(havingScope, having);
    if (!SqlTypeUtil.inBooleanFamily(type)) {
      throw newValidationError(having, RESOURCE.havingMustBeBoolean());
    }
  }

  /**
   * 校验selectItem
   * @param selectItems
   * @param select
   * @param targetRowType
   * @return 返回select的结果集类型
   */
  protected RelDataType validateSelectList(
      final SqlNodeList selectItems,
      SqlSelect select,
      RelDataType targetRowType) {
    // First pass, ensure that aliases are unique. "*" and "TABLE.*" items
    // are ignored.

    // Validate SELECT list. Expand terms of the form "*" or "TABLE.*".
    final SqlValidatorScope selectScope = getSelectScope(select);
    final List<SqlNode> expandedSelectItems = Lists.newArrayList();
    final Set<String> aliases = Sets.newHashSet();
    final List<Map.Entry<String, RelDataType>> fieldList = Lists.newArrayList();

    for (int i = 0; i < selectItems.size(); i++) {
      SqlNode selectItem = selectItems.get(i);
      if (selectItem instanceof SqlSelect) {//select item本身又是一个子查询,这个部分并不常见--因为一般同学不会这么写
        handleScalarSubQuery(
            select,
            (SqlSelect) selectItem,
            expandedSelectItems,
            aliases,
            fieldList);
      } else {
        expandSelectItem(
            selectItem,
            select,
            expandedSelectItems,
            aliases,
            fieldList,
            false);
      }
    }

    // Check expanded select list for aggregation.
    if (selectScope instanceof AggregatingScope) { //校验聚合selectItem
      AggregatingScope aggScope = (AggregatingScope) selectScope;
      for (SqlNode selectItem : expandedSelectItems) {
        boolean matches = aggScope.checkAggregateExpr(selectItem, true);
        Util.discard(matches);
      }
    }

    // Create the new select list with expanded items.  Pass through
    // the original parser position so that any overall failures can
    // still reference the original input text.
    SqlNodeList newSelectList =
        new SqlNodeList(
            expandedSelectItems,
            selectItems.getParserPosition());
    if (shouldExpandIdentifiers()) {
      select.setSelectList(newSelectList);
    }
    getRawSelectScope(select).setExpandedSelectList(expandedSelectItems);

    // TODO: when SELECT appears as a value subquery, should be using
    // something other than unknownType for targetRowType
    inferUnknownTypes(targetRowType, selectScope, newSelectList);

    for (SqlNode selectItem : expandedSelectItems) {
      validateExpr(selectItem, selectScope);
    }

    assert fieldList.size() >= aliases.size();
    return typeFactory.createStructType(fieldList);
  }

  /**
   * Validates an expression.
   *
   * @param expr  Expression
   * @param scope Scope in which expression occurs
   * 比如校验order by的item表达式
   */
  private void validateExpr(SqlNode expr, SqlValidatorScope scope) {
    // Call on the expression to validate itself.
    expr.validateExpr(this, scope);//通常情况下，该方法与validate方法是一样的，只有在SqlIdentifier这个子类的时候，对validateExpr进行了覆写,有自己的实现方式

    // Perform any validation specific to the scope. For example, an
    // aggregating scope requires that expressions are valid aggregations.
    scope.validateExpr(expr);
  }

  /**
   * Processes SubQuery found in Select list. Checks that is actually Scalar
   * subquery and makes proper entries in each of the 3 lists used to create
   * the final rowType entry.
   *
   * @param parentSelect        base SqlSelect item
   * @param selectItem          child SqlSelect from select list
   * @param expandedSelectItems Select items after processing
   * @param aliasList           built from user or system values
   * @param fieldList           Built up entries for each select list entry
   *
   * select item本身又是一个子查询,这个部分并不常见
   *
  select id
  (
    select name
    from biao
    where id = a.id
  ) as mo_seq
  from biao a
  limit 100
   */
  private void handleScalarSubQuery(
      SqlSelect parentSelect,
      SqlSelect selectItem,
      List<SqlNode> expandedSelectItems,
      Set<String> aliasList,
      List<Map.Entry<String, RelDataType>> fieldList) {
    // A scalar subquery only has one output column.
    if (1 != selectItem.getSelectList().size()) {//只允许有一个列
      throw newValidationError(selectItem,
          RESOURCE.onlyScalarSubqueryAllowed());
    }

    // No expansion in this routine just append to list.
    expandedSelectItems.add(selectItem);

    // Get or generate alias and add to list.
    //获取子查询的别名
    final String alias =
        deriveAlias(
            selectItem,
            aliasList.size());
    aliasList.add(alias);

    final SelectScope scope = (SelectScope) getWhereScope(parentSelect);
    final RelDataType type = deriveType(scope, selectItem);//计算子查询的返回值类型
    setValidatedNodeTypeImpl(selectItem, type);//设置子查询的返回类型

    // we do not want to pass on the RelRecordType returned
    // by the sub query.  Just the type of the single expression
    // in the subquery select list.
    // 因为返回值是select的结果集,因此是RelRecordType,而这个结果集中只有一个字段,这个是语法的强约束,所以获取第0个类型即可
    assert type instanceof RelRecordType;
    RelRecordType rec = (RelRecordType) type;

    RelDataType nodeType = rec.getFieldList().get(0).getType();
    nodeType = typeFactory.createTypeWithNullability(nodeType, true);
    fieldList.add(Pair.of(alias, nodeType));
  }

  /**
   * Derives a row-type for INSERT and UPDATE operations.
   *
   * @param table            Target table for INSERT/UPDATE
   * @param targetColumnList List of target columns, or null if not specified
   * @param append           Whether to append fields to those in <code>
   *                         baseRowType</code>
   * @return Rowtype
   */
  protected RelDataType createTargetRowType(
      SqlValidatorTable table,
      SqlNodeList targetColumnList,
      boolean append) {
    RelDataType baseRowType = table.getRowType();
    if (targetColumnList == null) {
      return baseRowType;
    }
    List<RelDataTypeField> targetFields = baseRowType.getFieldList();
    final List<Map.Entry<String, RelDataType>> types =
        new ArrayList<Map.Entry<String, RelDataType>>();
    if (append) {
      for (RelDataTypeField targetField : targetFields) {
        types.add(
            Pair.of(SqlUtil.deriveAliasFromOrdinal(types.size()),
                targetField.getType()));
      }
    }
    Set<Integer> assignedFields = new HashSet<Integer>();
    for (SqlNode node : targetColumnList) {
      SqlIdentifier id = (SqlIdentifier) node;
      String name = id.getSimple();
      RelDataTypeField targetField = catalogReader.field(baseRowType, name);
      if (targetField == null) {
        throw newValidationError(id, RESOURCE.unknownTargetColumn(name));
      }
      if (!assignedFields.add(targetField.getIndex())) {
        throw newValidationError(id,
            RESOURCE.duplicateTargetColumn(targetField.getName()));
      }
      types.add(targetField);
    }
    return typeFactory.createStructType(types);
  }

  public void validateInsert(SqlInsert insert) {
    SqlValidatorNamespace targetNamespace = getNamespace(insert);
    validateNamespace(targetNamespace);
    SqlValidatorTable table = targetNamespace.getTable();

    // INSERT has an optional column name list.  If present then
    // reduce the rowtype to the columns specified.  If not present
    // then the entire target rowtype is used.
    RelDataType targetRowType =
        createTargetRowType(
            table,
            insert.getTargetColumnList(),
            false);

    SqlNode source = insert.getSource();
    if (source instanceof SqlSelect) {
      SqlSelect sqlSelect = (SqlSelect) source;
      validateSelect(sqlSelect, targetRowType);
    } else {
      SqlValidatorScope scope = scopes.get(source);
      validateQuery(source, scope);
    }

    // REVIEW jvs 4-Dec-2008: In FRG-365, this namespace row type is
    // discarding the type inferred by inferUnknownTypes (which was invoked
    // from validateSelect above).  It would be better if that information
    // were used here so that we never saw any untyped nulls during
    // checkTypeAssignment.
    RelDataType sourceRowType = getNamespace(source).getRowType();
    RelDataType logicalTargetRowType =
        getLogicalTargetRowType(targetRowType, insert);
    setValidatedNodeType(insert, logicalTargetRowType);
    RelDataType logicalSourceRowType =
        getLogicalSourceRowType(sourceRowType, insert);

    checkFieldCount(insert, logicalSourceRowType, logicalTargetRowType);

    checkTypeAssignment(logicalSourceRowType, logicalTargetRowType, insert);

    validateAccess(insert.getTargetTable(), table, SqlAccessEnum.INSERT);
  }

  private void checkFieldCount(
      SqlNode node,
      RelDataType logicalSourceRowType,
      RelDataType logicalTargetRowType) {
    final int sourceFieldCount = logicalSourceRowType.getFieldCount();
    final int targetFieldCount = logicalTargetRowType.getFieldCount();
    if (sourceFieldCount != targetFieldCount) {
      throw newValidationError(node,
          RESOURCE.unmatchInsertColumn(targetFieldCount, sourceFieldCount));
    }
  }

  protected RelDataType getLogicalTargetRowType(
      RelDataType targetRowType,
      SqlInsert insert) {
    return targetRowType;
  }

  protected RelDataType getLogicalSourceRowType(
      RelDataType sourceRowType,
      SqlInsert insert) {
    return sourceRowType;
  }

  protected void checkTypeAssignment(
      RelDataType sourceRowType,
      RelDataType targetRowType,
      final SqlNode query) {
    // NOTE jvs 23-Feb-2006: subclasses may allow for extra targets
    // representing system-maintained columns, so stop after all sources
    // matched
    List<RelDataTypeField> sourceFields = sourceRowType.getFieldList();
    List<RelDataTypeField> targetFields = targetRowType.getFieldList();
    final int sourceCount = sourceFields.size();
    for (int i = 0; i < sourceCount; ++i) {
      RelDataType sourceType = sourceFields.get(i).getType();
      RelDataType targetType = targetFields.get(i).getType();
      if (!SqlTypeUtil.canAssignFrom(targetType, sourceType)) {
        // FRG-255:  account for UPDATE rewrite; there's
        // probably a better way to do this.
        int iAdjusted = i;
        if (query instanceof SqlUpdate) {
          int nUpdateColumns =
              ((SqlUpdate) query).getTargetColumnList().size();
          assert sourceFields.size() >= nUpdateColumns;
          iAdjusted -= sourceFields.size() - nUpdateColumns;
        }
        SqlNode node = getNthExpr(query, iAdjusted, sourceCount);
        String targetTypeString;
        String sourceTypeString;
        if (SqlTypeUtil.areCharacterSetsMismatched(
            sourceType,
            targetType)) {
          sourceTypeString = sourceType.getFullTypeString();
          targetTypeString = targetType.getFullTypeString();
        } else {
          sourceTypeString = sourceType.toString();
          targetTypeString = targetType.toString();
        }
        throw newValidationError(node,
            RESOURCE.typeNotAssignable(
                targetFields.get(i).getName(), targetTypeString,
                sourceFields.get(i).getName(), sourceTypeString));
      }
    }
  }

  /**
   * Locates the n'th expression in an INSERT or UPDATE query.
   *
   * @param query       Query
   * @param ordinal     Ordinal of expression
   * @param sourceCount Number of expressions
   * @return Ordinal'th expression, never null
   */
  private SqlNode getNthExpr(SqlNode query, int ordinal, int sourceCount) {
    if (query instanceof SqlInsert) {
      SqlInsert insert = (SqlInsert) query;
      if (insert.getTargetColumnList() != null) {
        return insert.getTargetColumnList().get(ordinal);
      } else {
        return getNthExpr(
            insert.getSource(),
            ordinal,
            sourceCount);
      }
    } else if (query instanceof SqlUpdate) {
      SqlUpdate update = (SqlUpdate) query;
      if (update.getTargetColumnList() != null) {
        return update.getTargetColumnList().get(ordinal);
      } else if (update.getSourceExpressionList() != null) {
        return update.getSourceExpressionList().get(ordinal);
      } else {
        return getNthExpr(
            update.getSourceSelect(),
            ordinal,
            sourceCount);
      }
    } else if (query instanceof SqlSelect) {
      SqlSelect select = (SqlSelect) query;
      if (select.getSelectList().size() == sourceCount) {
        return select.getSelectList().get(ordinal);
      } else {
        return query; // give up
      }
    } else {
      return query; // give up
    }
  }

  public void validateDelete(SqlDelete call) {
    SqlSelect sqlSelect = call.getSourceSelect();
    validateSelect(sqlSelect, unknownType);

    IdentifierNamespace targetNamespace =
        getNamespace(call.getTargetTable()).unwrap(
            IdentifierNamespace.class);
    validateNamespace(targetNamespace);
    SqlValidatorTable table = targetNamespace.getTable();

    validateAccess(call.getTargetTable(), table, SqlAccessEnum.DELETE);
  }

  public void validateUpdate(SqlUpdate call) {
    IdentifierNamespace targetNamespace =
        getNamespace(call.getTargetTable()).unwrap(
            IdentifierNamespace.class);
    validateNamespace(targetNamespace);
    SqlValidatorTable table = targetNamespace.getTable();

    RelDataType targetRowType =
        createTargetRowType(
            table,
            call.getTargetColumnList(),
            true);

    SqlSelect select = call.getSourceSelect();
    validateSelect(select, targetRowType);

    RelDataType sourceRowType = getNamespace(select).getRowType();
    checkTypeAssignment(sourceRowType, targetRowType, call);

    validateAccess(call.getTargetTable(), table, SqlAccessEnum.UPDATE);
  }

  public void validateMerge(SqlMerge call) {
    SqlSelect sqlSelect = call.getSourceSelect();
    // REVIEW zfong 5/25/06 - Does an actual type have to be passed into
    // validateSelect()?

    // REVIEW jvs 6-June-2006:  In general, passing unknownType like
    // this means we won't be able to correctly infer the types
    // for dynamic parameter markers (SET x = ?).  But
    // maybe validateUpdate and validateInsert below will do
    // the job?

    // REVIEW ksecretan 15-July-2011: They didn't get a chance to
    // since validateSelect() would bail.
    // Let's use the update/insert targetRowType when available.
    IdentifierNamespace targetNamespace =
        (IdentifierNamespace) getNamespace(call.getTargetTable());
    validateNamespace(targetNamespace);

    SqlValidatorTable table = targetNamespace.getTable();
    validateAccess(call.getTargetTable(), table, SqlAccessEnum.UPDATE);

    RelDataType targetRowType = unknownType;

    if (call.getUpdateCall() != null) {
      targetRowType = createTargetRowType(
          table,
          call.getUpdateCall().getTargetColumnList(),
          true);
    }
    if (call.getInsertCall() != null) {
      targetRowType = createTargetRowType(
          table,
          call.getInsertCall().getTargetColumnList(),
          false);
    }

    validateSelect(sqlSelect, targetRowType);

    if (call.getUpdateCall() != null) {
      validateUpdate(call.getUpdateCall());
    }
    if (call.getInsertCall() != null) {
      validateInsert(call.getInsertCall());
    }
  }

  /**
   * Validates access to a table.
   *
   * @param table          Table
   * @param requiredAccess Access requested on table
   *
   * 校验table的访问权限是否满足需求
   */
  private void validateAccess(
      SqlNode node,
      SqlValidatorTable table,
      SqlAccessEnum requiredAccess) {
    if (table != null) {
      SqlAccessType access = table.getAllowedAccess();
      if (!access.allowsAccess(requiredAccess)) {
        throw newValidationError(node,
            RESOURCE.accessNotAllowed(requiredAccess.name(),
                table.getQualifiedName().toString()));
      }
    }
  }

  /**
   * Validates a VALUES clause.
   *
   * @param node          Values clause
   * @param targetRowType Row type which expression must conform to
   * @param scope         Scope within which clause occurs
   */
  protected void validateValues(
      SqlCall node,
      RelDataType targetRowType,
      final SqlValidatorScope scope) {
    assert node.getKind() == SqlKind.VALUES;

    final List<SqlNode> operands = node.getOperandList();
    for (SqlNode operand : operands) {
      if (!(operand.getKind() == SqlKind.ROW)) {
        throw Util.needToImplement(
            "Values function where operands are scalars");
      }

      SqlCall rowConstructor = (SqlCall) operand;
      if (targetRowType.isStruct()
          && rowConstructor.operandCount() != targetRowType.getFieldCount()) {
        return;
      }

      inferUnknownTypes(
          targetRowType,
          scope,
          rowConstructor);
    }

    for (SqlNode operand : operands) {
      operand.validate(this, scope);
    }

    // validate that all row types have the same number of columns
    //  and that expressions in each column are compatible.
    // A values expression is turned into something that looks like
    // ROW(type00, type01,...), ROW(type11,...),...
    final int rowCount = operands.size();
    if (rowCount >= 2) {
      SqlCall firstRow = (SqlCall) operands.get(0);
      final int columnCount = firstRow.operandCount();

      // 1. check that all rows have the same cols length
      for (SqlNode operand : operands) {
        SqlCall thisRow = (SqlCall) operand;
        if (columnCount != thisRow.operandCount()) {
          throw newValidationError(node,
              RESOURCE.incompatibleValueType(
                  SqlStdOperatorTable.VALUES.getName()));
        }
      }

      // 2. check if types at i:th position in each row are compatible
      for (int col = 0; col < columnCount; col++) {
        final int c = col;
        final RelDataType type =
            typeFactory.leastRestrictive(
                new AbstractList<RelDataType>() {
                  public RelDataType get(int row) {
                    SqlCall thisRow = (SqlCall) operands.get(row);
                    return deriveType(scope, thisRow.operand(c));
                  }

                  public int size() {
                    return rowCount;
                  }
                });

        if (null == type) {
          throw newValidationError(node,
              RESOURCE.incompatibleValueType(
                  SqlStdOperatorTable.VALUES.getName()));
        }
      }
    }
  }

  public void validateDataType(SqlDataTypeSpec dataType) {
  }

  public void validateDynamicParam(SqlDynamicParam dynamicParam) {
  }

  public CalciteContextException newValidationError(SqlNode node,
      Resources.ExInst<SqlValidatorException> e) {
    assert node != null;
    final SqlParserPos pos = node.getParserPosition();
    return SqlUtil.newContextException(pos, e);
  }

  protected SqlWindow getWindowByName(
      SqlIdentifier id,
      SqlValidatorScope scope) {
    SqlWindow window = null;
    if (id.isSimple()) {
      final String name = id.getSimple();
      window = scope.lookupWindow(name);
    }
    if (window == null) {
      throw newValidationError(id, RESOURCE.windowNotFound(id.toString()));
    }
    return window;
  }

  public SqlWindow resolveWindow(
      SqlNode windowOrRef,
      SqlValidatorScope scope,
      boolean populateBounds) {
    SqlWindow window;
    if (windowOrRef instanceof SqlIdentifier) {
      window = getWindowByName((SqlIdentifier) windowOrRef, scope);
    } else {
      window = (SqlWindow) windowOrRef;
    }
    while (true) {
      final SqlIdentifier refId = window.getRefName();
      if (refId == null) {
        break;
      }
      final String refName = refId.getSimple();
      SqlWindow refWindow = scope.lookupWindow(refName);
      if (refWindow == null) {
        throw newValidationError(refId, RESOURCE.windowNotFound(refName));
      }
      window = window.overlay(refWindow, this);
    }

    if (populateBounds) {
      window.populateBounds();
    }
    return window;
  }

  public SqlNode getOriginal(SqlNode expr) {
    SqlNode original = originalExprs.get(expr);
    if (original == null) {
      original = expr;
    }
    return original;
  }

  public void setOriginal(SqlNode expr, SqlNode original) {
    // Don't overwrite the original original.
    if (originalExprs.get(expr) == null) {
      originalExprs.put(expr, original);
    }
  }

  //找到一个字段对应的类型创建一个空间
  SqlValidatorNamespace lookupFieldNamespace(RelDataType rowType, String name) {
    final RelDataTypeField field = catalogReader.field(rowType, name);
    return new FieldNamespace(this, field.getType());
  }

  public void validateWindow(
      SqlNode windowOrId,
      SqlValidatorScope scope,
      SqlCall call) {
    final SqlWindow targetWindow;
    switch (windowOrId.getKind()) {
    case IDENTIFIER:
      // Just verify the window exists in this query.  It will validate
      // when the definition is processed
      targetWindow = getWindowByName((SqlIdentifier) windowOrId, scope);
      break;
    case WINDOW:
      targetWindow = (SqlWindow) windowOrId;
      break;
    default:
      throw Util.unexpected(windowOrId.getKind());
    }

    assert targetWindow.getWindowCall() == null;
    targetWindow.setWindowCall(call);
    targetWindow.validate(this, scope);
    targetWindow.setWindowCall(null);
    call.validate(this, scope);
  }

  public void validateAggregateParams(
      SqlCall aggFunction,
      SqlValidatorScope scope) {
    // For agg(expr), expr cannot itself contain aggregate function
    // invocations.  For example, SUM(2*MAX(x)) is illegal; when
    // we see it, we'll report the error for the SUM (not the MAX).
    // For more than one level of nesting, the error which results
    // depends on the traversal order for validation.
    for (SqlNode param : aggFunction.getOperandList()) {
      final SqlNode agg = aggOrOverFinder.findAgg(param);
      if (aggOrOverFinder.findAgg(param) != null) {
        throw newValidationError(aggFunction, RESOURCE.nestedAggIllegal());
      }
    }
  }

  public void validateCall(
      SqlCall call,
      SqlValidatorScope scope) {
    final SqlOperator operator = call.getOperator();
    if ((call.operandCount() == 0)
        && (operator.getSyntax() == SqlSyntax.FUNCTION_ID)
        && !call.isExpanded()) {
      // For example, "LOCALTIME()" is illegal. (It should be
      // "LOCALTIME", which would have been handled as a
      // SqlIdentifier.)
      throw handleUnresolvedFunction(
          call,
          (SqlFunction) operator,
          ImmutableList.<RelDataType>of());
    }

    SqlValidatorScope operandScope = scope.getOperandScope(call);

    // Delegate validation to the operator.
    operator.validateCall(call, this, scope, operandScope);
  }

  /**
   * Validates that a particular feature is enabled. By default, all features
   * are enabled; subclasses may override this method to be more
   * discriminating.
   *
   * @param feature feature being used, represented as a resource instance
   * @param context parser position context for error reporting, or null if
   */
  protected void validateFeature(
      Feature feature,
      SqlParserPos context) {
    // By default, do nothing except to verify that the resource
    // represents a real feature definition.
    assert feature.getProperties().get("FeatureDefinition") != null;
  }

  public SqlNode expand(SqlNode expr, SqlValidatorScope scope) {
    final Expander expander = new Expander(this, scope);
    SqlNode newExpr = expr.accept(expander);
    if (expr != newExpr) {
      setOriginal(newExpr, expr);
    }
    return newExpr;
  }

  public boolean isSystemField(RelDataTypeField field) {
    return false;
  }

  public List<List<String>> getFieldOrigins(SqlNode sqlQuery) {
    if (sqlQuery instanceof SqlExplain) {
      return Collections.emptyList();
    }
    final RelDataType rowType = getValidatedNodeType(sqlQuery);
    final int fieldCount = rowType.getFieldCount();
    if (!sqlQuery.isA(SqlKind.QUERY)) {
      return Collections.nCopies(fieldCount, null);
    }
    final ArrayList<List<String>> list = new ArrayList<List<String>>();
    for (int i = 0; i < fieldCount; i++) {
      List<String> origin = getFieldOrigin(sqlQuery, i);
//            assert origin == null || origin.size() >= 4 : origin;
      list.add(origin);
    }
    return list;
  }

  private List<String> getFieldOrigin(SqlNode sqlQuery, int i) {
    if (sqlQuery instanceof SqlSelect) {
      SqlSelect sqlSelect = (SqlSelect) sqlQuery;
      final SelectScope scope = getRawSelectScope(sqlSelect);
      final List<SqlNode> selectList = scope.getExpandedSelectList();
      final SqlNode selectItem = stripAs(selectList.get(i));
      if (selectItem instanceof SqlIdentifier) {
        final SqlQualified qualified =
            scope.fullyQualify((SqlIdentifier) selectItem);
        SqlValidatorNamespace namespace = qualified.namespace;
        final SqlValidatorTable table = namespace.getTable();
        if (table == null) {
          return null;
        }
        final List<String> origin =
            Lists.newArrayList(table.getQualifiedName());
        for (String name : qualified.suffix()) {
          namespace = namespace.lookupChild(name);
          if (namespace == null) {
            return null;
          }
          origin.add(name);
        }
        return origin;
      }
    }
    return null;
  }

  public RelDataType getParameterRowType(SqlNode sqlQuery) {
    // NOTE: We assume that bind variables occur in depth-first tree
    // traversal in the same order that they occurred in the SQL text.
    final List<RelDataType> types = new ArrayList<RelDataType>();
    sqlQuery.accept(
        new SqlShuttle() {
          @Override public SqlNode visit(SqlDynamicParam param) {
            RelDataType type = getValidatedNodeType(param);
            types.add(type);
            return param;
          }
        });
    return typeFactory.createStructType(
        types,
        new AbstractList<String>() {
          @Override public String get(int index) {
            return "?" + index;
          }

          @Override public int size() {
            return types.size();
          }
        });
  }

  public void validateColumnListParams(
      SqlFunction function,
      List<RelDataType> argTypes,
      List<SqlNode> operands) {
    throw new UnsupportedOperationException();
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Common base class for DML statement namespaces.
   */
  public static class DmlNamespace extends IdentifierNamespace {
    protected DmlNamespace(SqlValidatorImpl validator, SqlNode id,
        SqlNode enclosingNode, SqlValidatorScope parentScope) {
      super(validator, id, enclosingNode, parentScope);
    }
  }

  /**
   * Namespace for an INSERT statement.
   */
  private static class InsertNamespace extends DmlNamespace {
    private final SqlInsert node;

    public InsertNamespace(SqlValidatorImpl validator, SqlInsert node,
        SqlNode enclosingNode, SqlValidatorScope parentScope) {
      super(validator, node.getTargetTable(), enclosingNode, parentScope);
      this.node = Preconditions.checkNotNull(node);
    }

    public SqlInsert getNode() {
      return node;
    }
  }

  /**
   * Namespace for an UPDATE statement.
   */
  private static class UpdateNamespace extends DmlNamespace {
    private final SqlUpdate node;

    public UpdateNamespace(SqlValidatorImpl validator, SqlUpdate node,
        SqlNode enclosingNode, SqlValidatorScope parentScope) {
      super(validator, node.getTargetTable(), enclosingNode, parentScope);
      this.node = Preconditions.checkNotNull(node);
    }

    public SqlUpdate getNode() {
      return node;
    }
  }

  /**
   * Namespace for a DELETE statement.
   */
  private static class DeleteNamespace extends DmlNamespace {
    private final SqlDelete node;

    public DeleteNamespace(SqlValidatorImpl validator, SqlDelete node,
        SqlNode enclosingNode, SqlValidatorScope parentScope) {
      super(validator, node.getTargetTable(), enclosingNode, parentScope);
      this.node = Preconditions.checkNotNull(node);
    }

    public SqlDelete getNode() {
      return node;
    }
  }

  /**
   * Namespace for a MERGE statement.
   */
  private static class MergeNamespace extends DmlNamespace {
    private final SqlMerge node;

    public MergeNamespace(SqlValidatorImpl validator, SqlMerge node,
        SqlNode enclosingNode, SqlValidatorScope parentScope) {
      super(validator, node.getTargetTable(), enclosingNode, parentScope);
      this.node = Preconditions.checkNotNull(node);
    }

    public SqlMerge getNode() {
      return node;
    }
  }

  /**
   * Visitor which derives the type of a given {@link SqlNode}.
   *
   * <p>Each method must return the derived type. This visitor is basically a
   * single-use dispatcher; the visit is never recursive.
   *
   * 访问sqlNode节点,返回该节点对应的数据类型
   */
  private class DeriveTypeVisitor implements SqlVisitor<RelDataType> {
    private final SqlValidatorScope scope;

    public DeriveTypeVisitor(SqlValidatorScope scope) {
      this.scope = scope;
    }

    //根据字面量的sql类型,返回java类型
    public RelDataType visit(SqlLiteral literal) {
      return literal.createSqlType(typeFactory);
    }

    public RelDataType visit(SqlCall call) {
      final SqlOperator operator = call.getOperator();
      return operator.deriveType(SqlValidatorImpl.this, scope, call);
    }

    //正常情况下,list是无返回结果的
    public RelDataType visit(SqlNodeList nodeList) {
      // Operand is of a type that we can't derive a type for. If the
      // operand is of a peculiar type, such as a SqlNodeList, then you
      // should override the operator's validateCall() method so that it
      // doesn't try to validate that operand as an expression.
      throw Util.needToImplement(nodeList);
    }

    //为字符串返回类型，他可能是无参数的函数，也可能是某一个列名称
    public RelDataType visit(SqlIdentifier id) {
      // First check for builtin functions which don't have parentheses,
      // like "LOCALTIME".
      //校验字符串会不会是内置的无()的函数
      SqlCall call = SqlUtil.makeCall(opTab, id);
      if (call != null) {
        return call.getOperator().validateOperands(
            SqlValidatorImpl.this,
            scope,
            call);//此时call是无参数的函数,因此也就不需要有参数Operand的解析
      }

      //说明此时字符串不是函数
      RelDataType type = null;
      if (!(scope instanceof EmptyScope)) {
        id = scope.fullyQualify(id).identifier;
      }

      // Resolve the longest prefix of id that we can
      SqlValidatorNamespace resolvedNs;
      int i;
      for (i = id.names.size() - 1; i > 0; i--) {
        // REVIEW jvs 9-June-2005: The name resolution rules used
        // here are supposed to match SQL:2003 Part 2 Section 6.6
        // (identifier chain), but we don't currently have enough
        // information to get everything right.  In particular,
        // routine parameters are currently looked up via resolve;
        // we could do a better job if they were looked up via
        // resolveColumn.

        resolvedNs = scope.resolve(id.names.subList(0, i), null, null);
        if (resolvedNs != null) {
          // There's a namespace with the name we seek.
          type = resolvedNs.getRowType();
          break;
        }
      }

      // Give precedence to namespace found, unless there
      // are no more identifier components.
      if (type == null || id.names.size() == 1) {
        // See if there's a column with the name we seek in
        // precisely one of the namespaces in this scope.
        RelDataType colType = scope.resolveColumn(id.names.get(0), id);//查看id是不是一个列名,如果是返回列的类型
        if (colType != null) {
          type = colType;
        }
        ++i;
      }

      if (type == null) {
        final SqlIdentifier last = id.getComponent(i - 1, i);
        throw newValidationError(last,
            RESOURCE.unknownIdentifier(last.toString()));
      }

      // Resolve rest of identifier 全局属性信息,不常用,可忽略
      for (; i < id.names.size(); i++) {
        String name = id.names.get(i);
        final RelDataTypeField field = catalogReader.field(type, name);
        if (field == null) {
          throw newValidationError(id.getComponent(i),
              RESOURCE.unknownField(name));
        }
        type = field.getType();
      }
      type =
          SqlTypeUtil.addCharsetAndCollation(
              type,
              getTypeFactory());
      return type;
    }

    public RelDataType visit(SqlDataTypeSpec dataType) {
      // Q. How can a data type have a type?
      // A. When it appears in an expression. (Say as the 2nd arg to the
      //    CAST operator.)
      validateDataType(dataType);
      return dataType.deriveType(SqlValidatorImpl.this);
    }

    //返回位置类型
    public RelDataType visit(SqlDynamicParam param) {
      return unknownType;
    }

    public RelDataType visit(SqlIntervalQualifier intervalQualifier) {
      return typeFactory.createSqlIntervalType(intervalQualifier);
    }
  }

  /**
   * Converts an expression into canonical form by fully-qualifying any
   * identifiers.
   * 将任意identifiers字符串表达式转换成合法的
   */
  private static class Expander extends SqlScopedShuttle {
    private final SqlValidatorImpl validator;

    public Expander(
        SqlValidatorImpl validator,
        SqlValidatorScope scope) {
      super(scope);
      this.validator = validator;
    }

    @Override public SqlNode visit(SqlIdentifier id) {
      // First check for builtin functions which don't have
      // parentheses, like "LOCALTIME".
      //id字符串可能是无参数的函数,如果是,则将SqlIdentifier转换成SqlCall
      SqlCall call = SqlUtil.makeCall(validator.getOperatorTable(),id);
      if (call != null) {
        return call.accept(this);
      }
      final SqlIdentifier fqId = getScope().fullyQualify(id).identifier;//将列名字补全
      validator.setOriginal(fqId, id);
      return fqId;
    }

    @Override protected SqlNode visitScoped(SqlCall call) {
      switch (call.getKind()) {
      case SCALAR_QUERY:
      case CURRENT_VALUE:
      case NEXT_VALUE:
        return call;
      }
      // Only visits arguments which are expressions. We don't want to
      // qualify non-expressions such as 'x' in 'empno * 5 AS x'.
      ArgHandler<SqlNode> argHandler = new CallCopyingArgHandler(call, false);
      call.getOperator().acceptCall(this, call, true, argHandler);
      final SqlNode result = argHandler.result();
      validator.setOriginal(result, call);
      return result;
    }
  }

  /**
   * Shuttle which walks over an expression in the ORDER BY clause, replacing
   * usages of aliases with the underlying expression.
   */
  class OrderExpressionExpander extends SqlScopedShuttle {
    private final List<String> aliasList;
    private final SqlSelect select;
    private final SqlNode root;

    OrderExpressionExpander(SqlSelect select, SqlNode root) {
      super(getOrderScope(select));
      this.select = select;
      this.root = root;
      this.aliasList = getNamespace(select).getRowType().getFieldNames();
    }

    public SqlNode go() {
      return root.accept(this);
    }

    public SqlNode visit(SqlLiteral literal) {
      // Ordinal markers, e.g. 'select a, b from t order by 2'.
      // Only recognize them if they are the whole expression,
      // and if the dialect permits.
      if (literal == root && getConformance().isSortByOrdinal()) {
        switch (literal.getTypeName()) {
        case DECIMAL:
        case DOUBLE:
          final int intValue = literal.intValue(false);
          if (intValue >= 0) {
            if (intValue < 1 || intValue > aliasList.size()) {
              throw newValidationError(
                  literal, RESOURCE.orderByOrdinalOutOfRange());
            }

            // SQL ordinals are 1-based, but Sort's are 0-based
            int ordinal = intValue - 1;
            return nthSelectItem(ordinal, literal.getParserPosition());
          }
          break;
        }
      }

      return super.visit(literal);
    }

    /**
     * Returns the <code>ordinal</code>th item in the select list.
     */
    private SqlNode nthSelectItem(int ordinal, final SqlParserPos pos) {
      // TODO: Don't expand the list every time. Maybe keep an expanded
      // version of each expression -- select lists and identifiers -- in
      // the validator.

      SqlNodeList expandedSelectList =
          expandStar(
              select.getSelectList(),
              select,
              false);
      SqlNode expr = expandedSelectList.get(ordinal);
      expr = stripAs(expr);//去除as节点
      if (expr instanceof SqlIdentifier) {
        expr = getScope().fullyQualify((SqlIdentifier) expr).identifier;
      }

      // Create a copy of the expression with the position of the order
      // item.
      return expr.clone(pos);
    }

    public SqlNode visit(SqlIdentifier id) {
      // Aliases, e.g. 'select a as x, b from t order by x'.
      if (id.isSimple()
          && getConformance().isSortByAlias()) {
        String alias = id.getSimple();
        final SqlValidatorNamespace selectNs = getNamespace(select);
        final RelDataType rowType =
            selectNs.getRowTypeSansSystemColumns();
        RelDataTypeField field =
            catalogReader.field(rowType, alias);
        if (field != null) {
          return nthSelectItem(
              field.getIndex(),
              id.getParserPosition());
        }
      }

      // No match. Return identifier unchanged.
      return getScope().fullyQualify(id).identifier;
    }

    protected SqlNode visitScoped(SqlCall call) {
      // Don't attempt to expand sub-queries. We haven't implemented
      // these yet.
      if (call instanceof SqlSelect) {
        return call;
      }
      return super.visitScoped(call);
    }
  }

  /** Information about an identifier in a particular scope. */
  protected static class IdInfo {
    public final SqlValidatorScope scope;
    public final SqlIdentifier id;

    public IdInfo(SqlValidatorScope scope, SqlIdentifier id) {
      this.scope = scope;
      this.id = id;
    }
  }

  /**
   * Utility object used to maintain information about the parameters in a
   * function call.
   */
  protected static class FunctionParamInfo {
    /**
     * Maps a cursor (based on its position relative to other cursor
     * parameters within a function call) to the SELECT associated with the
     * cursor.
     */
    public final Map<Integer, SqlSelect> cursorPosToSelectMap;

    /**
     * Maps a column list parameter to the parent cursor parameter it
     * references. The parameters are id'd by their names.
     */
    public final Map<String, String> columnListParamToParentCursorMap;

    public FunctionParamInfo() {
      cursorPosToSelectMap = new HashMap<Integer, SqlSelect>();
      columnListParamToParentCursorMap = new HashMap<String, String>();
    }
  }

}

// End SqlValidatorImpl.java
