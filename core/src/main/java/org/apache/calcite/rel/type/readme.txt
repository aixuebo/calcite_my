一、背景与核心


二、基础接口
1.interface RelDataType 仅代表某一个字段的类型。
如果对象是struct,则可以嵌套。
2.interface RelDataTypeFactory 类型工厂
创建各种RelDataType。
3.interface RelDataTypeField extends Map.Entry<String, RelDataType> 代表一个字段,包含字段名称、字段类型、字段序号
4.enum RelDataTypeComparability 字段参与的比较方式
NONE 不允许比较,作为默认值,表示不能参与比较的列
UNORDERED 仅允许=或者!=,即不支持排序
ALL 支持比较


三、class RelCrossType extends RelDataTypeImpl 相当于元组对象,即(类型1,类型2,类型3)
  ImmutableList<RelDataType> types;//类型集合
  List<RelDataTypeField> fields) //元组name、序号、类型

四、abstract class RelDataTypeImpl implements RelDataType, RelDataTypeFamily 表示一行数据对应的字段集合,相当于表的schema
即他是struct
List<RelDataTypeField> fieldList;//表示字段集合

实现类  class RelRecordType extends RelDataTypeImpl implements Serializable,即一个class代表schema,他是struct类型