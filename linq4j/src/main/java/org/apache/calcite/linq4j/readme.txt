核心实现该功能的工具类是 DefaultEnumerable<T> extends Enumerable<T> 调用EnumerableDefaults实现功能


一、interface ExtendedEnumerable<TSource> 数据集合工具接口,为类似list这种集合提供更多的能力
0基础功能

Enumerable<TResult> select(Function1<TSource, TResult> selector);
    相当于map操作,转换成新的数据集合

Enumerable<TResult> select(Function2<TSource, Integer, TResult> selector);
    相当于map操作,转换成新的数据集合,只是转换函数是由 <元素+序号(从0开始计数))组成

<TResult> Enumerable<TResult> selectMany(Function1<TSource, Enumerable<TResult>> selector);
    每一个元素转换成多个元素,相当于flatMap

Enumerable<TResult> zip(Enumerable<T1> source1,Function2<TSource, T1, TResult> resultSelector);
    将元素+下标,生产一个数据,相当于map操作

Enumerable<TResult> ofType(Class<TResult> clazz);
    匹配元素是class的子类的元素被保留,for(value --> value instance clazz)

Enumerable<TSource> where(Predicate1<TSource> predicate);
    返回结果是true的元素

Enumerable<TSource> where(Predicate2<TSource, Integer> predicate);
    返回结果是true的元素,参数是元素值+元素序号,序号从0开始计数,即第几条数据

Enumerable<TSource> distinct();将元素集合存放到set中

1.R foreach(Function1<TSource, R> func)
    相当于for循环,用于输出打印结果即可,最终会返回最后一个元素的参与function1的计算结果
2.集合基本功能
Enumerable<TSource> intersect(Enumerable<TSource> enumerable1);
    两个集合去交集。new set().add( set1.contain(enumerable1的元素))
Enumerable<TSource> union(Enumerable<TSource> source1);
    将两个集合做并集 new set().addALL(source).addALL(source1)
Enumerable<TSource> except(Enumerable<TSource> enumerable1);
    返回集合的子集,从集合中删除参数集合内的数据。set<>.remove(enumerable1)
boolean contains(TSource element); 判断集合中是否有参数元素
TSource first();返回第一个元素
TSource first(Predicate1<TSource> predicate);返回满足条件的第一个元素
TSource last();
TSource last(Predicate1<TSource> predicate);
  boolean all(Predicate1<TSource> predicate);
    for(value:List) predicate(value) 所有元素必须全返回true,结果才是true
  boolean any();
    for(value:List),只要有元素存在,则就返回true。
  boolean any(Predicate1<TSource> predicate);
    有任意元素是true,结果就是true。

3.Enumerable<TSource> asEnumerable() 转换
4.Enumerable<TSource> concat(Enumerable<TSource> enumerable1);
把两个集合连接起来,相当于new list().addAll(enumerable0).addAll(enumerable1)
5.Enumerable<T2> cast(Class<T2> clazz);
将每一个元素,强转成class对象,即相当于map+cast操作

6.聚合函数
a.TSource aggregate(Function2<TSource, TSource, TSource> func);
    for(value:List) fun(temp,value) --- 中间结果,迭代下一个元素,计算最终结果。要求中间结果与最终结果类型一致
b.TAccumulate aggregate(TAccumulate seed,Function2<TAccumulate, TSource, TAccumulate> func);
    for(value:List) fun(temp,value) --- 中间结果,迭代下一个元素,计算最终结果。要求中间结果与最终结果类型可以不一致。
    TAccumulate seed是初始化中间结果值
c.TResult aggregate(TAccumulate seed,
      Function2<TAccumulate, TSource, TAccumulate> func,
      Function1<TAccumulate, TResult> selector);

         result = for(value:List) fun(temp,value) --- 中间结果,迭代下一个元素,计算最终结果。要求中间结果与最终结果类型可以不一致
          TAccumulate seed是初始化中间结果值
         selector(result) --- 对结果再进一步转换,包含类型转换

7.简单的聚合函数
  BigDecimal sum(BigDecimalFunction1<TSource> selector);
    for(value --> map(value)).sum() 先将元素转换成double,然后求和
  TSource min/max();返回集合中最大的元素
  BigDecimal min/max(BigDecimalFunction1<TSource> selector);
    将元素先map转换成double类型,然后返回集合中最大的元素
  long longCount();  int count();计算集合的元素数量
  int longCount/count(Predicate1<TSource> predicate);计算满足参数条件的 元素数量
  BigDecimal average(BigDecimalFunction1<TSource> selector);
    扫描两次集合,分别计算sum/count

8.集合高级功能
Enumerable<TSource> skip(int count);
  跳过n个元素,使用skipWhile实现

Enumerable<TSource> skipWhile(Predicate1<TSource> predicate);
    跳过参数是false的数据

Enumerable<TSource> take(int count);
    获取前count条元素。
    调用takeWhile函数,满足条件是元素序号<count

Enumerable<TSource> takeWhile(Predicate1<TSource> predicate);
    实现while语法,直到函数返回值false时停止循环,即保留true的数据
    for(value) {
        boolean b = predicate(value,index)
        if(b == false) return
    }

9.集合之间转换功能
<C extends Collection<? super TSource>> C into(C sink);
    复制集合 for(value --> Collection.add(value))

Enumerable<TSource> reverse();
   将元素反过来,即从后往前输出集合

Map<TKey, TSource> toMap(Function1<TSource, TKey> keySelector);
    将集合转换成map,通过提供函数keySelector将value转换成key；for(value --> map.put(keySelector(value),value) )

Map<TKey, TElement> toMap(Function1<TSource, TKey> keySelector,
      Function1<TSource, TElement> elementSelector);
      value值也可以进一步转换

List<TSource> toList();
    将集合转换成List；for(value --> list.add(value))

Lookup<TKey, TSource> toLookup(Function1<TSource, TKey> keySelector);
    转换成Map<K,List<V>> --- 循环元素,将相同的元素放在List里,转换成map<key,List<value>>形式

Lookup<TKey, TSource> toLookup(Function1<TSource, TKey> keySelector,
      EqualityComparer<TKey> comparer);
      转换成TreeMap<K,List<V>> --- 循环元素,将相同的元素放在List里,转换成map<key,List<value>>形式

Lookup<TKey, TElement> toLookup(
      Function1<TSource, TKey> keySelector,
      Function1<TSource, TElement> elementSelector);
      转换成TreeMap<K,List<V>> --- 循环元素,将相同的元素放在List里,转换成map<key,List<value>>形式

10.group by
Enumerable<Grouping<TKey, TSource>> groupBy(Function1<TSource, TKey> keySelector);
    1.LookupImpl 将集合按照key分组,转换成Map<k,List<v>>
    2.返回每一个map的entry对象即可组成 key,list --> 转换成key,value迭代器 --> 转换成Grouping<TKey, TSource>

Enumerable<Grouping<TKey, TElement>> groupBy(
      Function1<TSource, TKey> keySelector,
      Function1<TSource, TElement> elementSelector);
      增加对value的处理函数,即map存储的list不在是value,而是经过转换的value

Enumerable<Grouping<TKey, TResult>> groupBy(
      Function1<TSource, TKey> keySelector,
      Function2<TKey, Enumerable<TSource>, TResult> elementSelector);
      对map的<k,list<v>> 再进一步转换,elementSelector转换成<TKey, TResult>

11.join
Enumerable<TResult> join(Enumerable<TInner> inner,//右表数据
      Function1<TSource, TKey> outerKeySelector,//提取左表key
      Function1<TInner, TKey> innerKeySelector,//提取右表key
      Function2<TSource, TInner, TResult> resultSelector,//左表一行数据+右表一行数据,生产结果数据
      EqualityComparer<TKey> comparer,
      boolean generateNullsOnLeft, //左表允许null,用于left join等场景
      boolean generateNullsOnRight);//左表允许null,用于left join等场景
    将右表转换成Map，然后循环左表,去右表Map中找到list,与list做循环


Enumerable<TResult> correlateJoin(
      CorrelateJoinType joinType,
      Function1<TSource, Enumerable<TInner>> inner,//右边表
      Function2<TSource, TInner, TResult> resultSelector);//完成join操作
      相互关联的join，不提供右边表,但提供通过key查找右边表匹配的list方法，其实就是简单的join操作

Enumerable<TResult> groupJoin(
      Enumerable<TInner> inner, //右边表
      Function1<TSource, TKey> outerKeySelector,//左边表如何转换成key
      Function1<TInner, TKey> innerKeySelector,//左边表如何转换成key
      Function2<TSource, Enumerable<TInner>, TResult> resultSelector); //左边数据+右边list匹配的集合,返回结果
    相当于join操作,只是匹配的是 左边表+右边匹配的list一起参与运算,计算结果

12.order by
Enumerable<TSource> orderBy(Function1<TSource, TKey> keySelector);
    将k转换成可以排序的value,先进行LookupImpl处理,将数据转换成treeMap --> 输出treeMap中value迭代器,自然就有顺序的

Enumerable<TSource> orderByDescending(Function1<TSource, TKey> keySelector);倒序


13.Queryable<TSource> asQueryable();
