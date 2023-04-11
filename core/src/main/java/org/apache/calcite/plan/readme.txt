一、RelOptRuleOperand
学习参见 ProjectTableRule中INSTANCE方法

在plan计划的时候,会有很多规则,到底该规则是否适配relNode呢？怎么决定的呢？


输入：
1.List<RelOptRule>规则集合。
2.relNode 表达式节点。
3.RelOptRule持有RelOptRuleOperand,RelOptRuleOperand起到中间桥梁作用。
通过RelOptRuleOperand对relNode进行校验，检验通过,说明RelOptRuleOperand中定义的RelOptRule是可以应用在relNode节点上的。
RelOptRuleOperand定义了match方法,校验规则包含:class是否匹配、trait是否匹配、校验函数是否匹配

输出:relNode匹配了哪些List<RelOptRule>规则



RelOptRuleOperand是可以有嵌套的,即操作1+操作2+操作3都要满足等。因此就有一定的匹配策略，参见RelOptRuleOperandChildPolicy+RelOptRuleOperandChildren

比如:
  public static final ProjectTableRule INSTANCE =
      new ProjectTableRule( //定义一个规则,包含了若干个operand(RelOptRuleOperand)
          operand(Project.class,
              operand(EnumerableInterpreter.class,
                  operand(TableScan.class, null, PREDICATE, none()))),
          "ProjectTableRule:basic") {
        @Override public void onMatch(RelOptRuleCall call) {
          final Project project = call.rel(0);
          final EnumerableInterpreter interpreter = call.rel(1);
          final TableScan scan = call.rel(2);
          final RelOptTable table = scan.getTable();
          assert table.unwrap(ProjectableFilterableTable.class) != null;
          apply(call, project, null, interpreter);
        }
      };


二、RelOptRule 代表规则,参见ProjectTableRule
1.持有RelOptRuleOperand对象,用于校验是否匹配relNode节点。
2.有自己的match方法,做规则匹配

三、interface RelOptTable 表达式rel操作的一个table表

四、interface RelOptSchema 代表操作schema的对象

五、interface RelOptCost
评估表的代价,属于CBO范畴。
包含表的数据条数、IO使用情况、CPU使用情况