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
package org.apache.calcite.linq4j;

import org.apache.calcite.linq4j.function.BigDecimalFunction1;
import org.apache.calcite.linq4j.function.DoubleFunction1;
import org.apache.calcite.linq4j.function.EqualityComparer;
import org.apache.calcite.linq4j.function.FloatFunction1;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.IntegerFunction1;
import org.apache.calcite.linq4j.function.LongFunction1;
import org.apache.calcite.linq4j.function.NullableBigDecimalFunction1;
import org.apache.calcite.linq4j.function.NullableDoubleFunction1;
import org.apache.calcite.linq4j.function.NullableFloatFunction1;
import org.apache.calcite.linq4j.function.NullableIntegerFunction1;
import org.apache.calcite.linq4j.function.NullableLongFunction1;
import org.apache.calcite.linq4j.function.Predicate1;
import org.apache.calcite.linq4j.function.Predicate2;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Extension methods in {@link Enumerable}.
 *
 * @param <TSource> Element type
 *
 * 参见 EnumerableDefaults
 *


LookupImpl --- 转换成TreeMap<K,List<V>> --- 循环元素,将相同的元素放在List里,转换成map<key,List<value>>形式
参数:
1.集合队列
2.排序对象,key按照什么方式排序
3.key转换函数,将集合元素转换成key
4.value转换器,支持将原始value转换成需要的value元素形式。默认函数可以返回原始值本身,表示不需要转换


map = new treeMap<key,List<Value>>(comparator)
for(value){
key = keySelect(value)
value = map(value)
list = map.get(key).add(value)
map.put(key,list)
}
return new LookupImpl<TKey, TElement>(treeMap)


 */
public interface ExtendedEnumerable<TSource> {

  /**
   * Performs an operation for each member of this enumeration.
   *
   * <p>Returns the value returned by the function for the last element in
   * this enumeration, or null if this enumeration is empty.</p>
   *
   * @param func Operation
   * @param <R> Return type
   * 相当于for循环,用于输出打印结果即可,最终会返回最后一个元素的参与function1的计算结果
   */
  <R> R foreach(Function1<TSource, R> func);

  /**
   * Applies an accumulator function over a sequence.
   *
   * for(value:List) fun(temp,value) --- 中间结果,迭代下一个元素,计算最终结果。要求中间结果与最终结果类型一致
   */
  TSource aggregate(Function2<TSource, TSource, TSource> func);

  /**
   * Applies an accumulator function over a
   * sequence. The specified seed value is used as the initial
   * accumulator value.
   *
   * for(value:List) fun(temp,value) --- 中间结果,迭代下一个元素,计算最终结果。要求中间结果与最终结果类型可以不一致
   * TAccumulate seed是初始化中间结果值
   */
  <TAccumulate> TAccumulate aggregate(TAccumulate seed,
      Function2<TAccumulate, TSource, TAccumulate> func);

  /**
   * Applies an accumulator function over a
   * sequence. The specified seed value is used as the initial
   * accumulator value, and the specified function is used to select
   * the result value.
   *
   * 1.result = for(value:List) fun(temp,value) --- 中间结果,迭代下一个元素,计算最终结果。要求中间结果与最终结果类型可以不一致
   * TAccumulate seed是初始化中间结果值
   * 2.selector(result) --- 对结果再进一步转换,包含类型转换
   */
  <TAccumulate, TResult> TResult aggregate(TAccumulate seed,
      Function2<TAccumulate, TSource, TAccumulate> func,
      Function1<TAccumulate, TResult> selector);

  /**
   * Determines whether all elements of a sequence
   * satisfy a condition.
   * for(value:List) predicate(value) 所有元素必须全返回true,结果才是true
   */
  boolean all(Predicate1<TSource> predicate);

  /**
   * Determines whether a sequence contains any
   * elements. (Defined by Enumerable.)
   *
   * for(value:List),只要有元素存在,则就返回true。
   * 即是否非空集合
   */
  boolean any();

  /**
   * Determines whether any element of a sequence
   * satisfies a condition.
   *
   * for(value:List) predicate(value) 有任意元素是true,结果就是true。
   */
  boolean any(Predicate1<TSource> predicate);

  /**
   * Returns the input typed as {@code Enumerable<TSource>}.
   *
   * <p>This method has no effect
   * other than to change the compile-time type of source from a type that
   * implements {@code Enumerable<TSource>} to {@code Enumerable<TSource>}
   * itself.
   *
   * <p>{@code asEnumerable<TSource>(Enumerable<TSource>)} can be used to choose
   * between query implementations when a sequence implements

   * {@code Enumerable<TSource>} but also has a different set of public query
   * methods available. For example, given a generic class Table that implements
   * {@code Enumerable<TSource>} and has its own methods such as {@code where},
   * {@code select}, and {@code selectMany}, a call to {@code where} would
   * invoke the public {@code where} method of {@code Table}. A {@code Table}
   * type that represents a database table could have a {@code where} method
   * that takes the predicate argument as an expression tree and converts the
   * tree to SQL for remote execution. If remote execution is not desired, for
   * example because the predicate invokes a local method, the
   * {@code asEnumerable<TSource>} method can be used to hide the custom methods
   * and instead make the standard query operators available.
   */
  Enumerable<TSource> asEnumerable();

  /**
   * Converts an Enumerable to a {@link Queryable}.
   *
   * <p>If the type of source implements {@code Queryable}, this method
   * returns it directly. Otherwise, it returns a {@code Queryable} that
   * executes queries by calling the equivalent query operator methods in
   * {@code Enumerable} instead of those in {@code Queryable}.</p>
   *
   * <p>Analogous to the LINQ's Enumerable.AsQueryable extension method.</p>
   *
   * @return A queryable
   */
  Queryable<TSource> asQueryable();

  /**
   * Computes the average of a sequence of Decimal
   * values that are obtained by invoking a transform function on
   * each element of the input sequence.
   * 扫描两次集合,分别计算sum/count
   */
  BigDecimal average(BigDecimalFunction1<TSource> selector);

  /**
   * Computes the average of a sequence of nullable
   * Decimal values that are obtained by invoking a transform
   * function on each element of the input sequence.
   */
  BigDecimal average(NullableBigDecimalFunction1<TSource> selector);

  /**
   * Computes the average of a sequence of Double
   * values that are obtained by invoking a transform function on
   * each element of the input sequence.
   */
  double average(DoubleFunction1<TSource> selector);

  /**
   * Computes the average of a sequence of nullable
   * Double values that are obtained by invoking a transform
   * function on each element of the input sequence.
   */
  Double average(NullableDoubleFunction1<TSource> selector);

  /**
   * Computes the average of a sequence of int values
   * that are obtained by invoking a transform function on each
   * element of the input sequence.
   */
  int average(IntegerFunction1<TSource> selector);

  /**
   * Computes the average of a sequence of nullable
   * int values that are obtained by invoking a transform function
   * on each element of the input sequence.
   */
  Integer average(NullableIntegerFunction1<TSource> selector);

  /**
   * Computes the average of a sequence of long values
   * that are obtained by invoking a transform function on each
   * element of the input sequence.
   */
  long average(LongFunction1<TSource> selector);

  /**
   * Computes the average of a sequence of nullable
   * long values that are obtained by invoking a transform function
   * on each element of the input sequence.
   */
  Long average(NullableLongFunction1<TSource> selector);

  /**
   * Computes the average of a sequence of Float
   * values that are obtained by invoking a transform function on
   * each element of the input sequence.
   */
  float average(FloatFunction1<TSource> selector);

  /**
   * Computes the average of a sequence of nullable
   * Float values that are obtained by invoking a transform
   * function on each element of the input sequence.
   */
  Float average(NullableFloatFunction1<TSource> selector);

  /**
   * Converts the elements of this Enumerable to the specified type.
   *
   * <p>This method is implemented by using deferred execution. The immediate
   * return value is an object that stores all the information that is
   * required to perform the action. The query represented by this method is
   * not executed until the object is enumerated either by calling its
   * {@link Enumerable#enumerator} method directly or by using
   * {@code for (... in ...)}.
   *
   * <p>If an element cannot be cast to type TResult, the
   * {@link Enumerator#current()} method will throw a
   * {@link ClassCastException} a exception when the element it accessed. To
   * obtain only those elements that can be cast to type TResult, use the
   * {@link #ofType(Class)} method instead.
   *
   * @see EnumerableDefaults#cast
   * @see #ofType(Class)
   *
   * 将每一个元素,强转成class对象,即相当于map+cast操作
   */
  <T2> Enumerable<T2> cast(Class<T2> clazz);

  /**
   * Concatenates two sequences.
   * 把两个集合连接起来,相当于new list().addAll(enumerable0).addAll(enumerable1)
   */
  Enumerable<TSource> concat(Enumerable<TSource> enumerable1);

  /**
   * Determines whether a sequence contains a specified
   * element by using the default equality comparer.
   * 判断集合中是否有参数元素
   */
  boolean contains(TSource element);

  /**
   * Determines whether a sequence contains a specified
   * element by using a specified {@code EqualityComparer<TSource>}.
   *
   * 判断集合中是否有参数元素，判断是否存在,依赖comparer对象
   */
  boolean contains(TSource element, EqualityComparer comparer);

  /**
   * Returns the number of elements in a
   * sequence.
   * 计算集合的元素数量
   */
  int count();

  /**
   * Returns a number that represents how many elements
   * in the specified sequence satisfy a condition.
   *
   * 计算满足参数条件的 元素数量
   */
  int count(Predicate1<TSource> predicate);

  /**
   * Returns an long that represents the total number
   * of elements in a sequence.
   * 计算元素数量
   */
  long longCount();

  /**
   * Returns an long that represents how many elements
   * in a sequence satisfy a condition.
   * 计算满足条件的元素数量
   */
  long longCount(Predicate1<TSource> predicate);

  /**
   * Returns the elements of the specified sequence or
   * the type parameter's default value in a singleton collection if
   * the sequence is empty.
   */
  Enumerable<TSource> defaultIfEmpty();

  /**
   * Returns the elements of the specified sequence or
   * the specified value in a singleton collection if the sequence
   * is empty.
   */
  TSource defaultIfEmpty(TSource value);

  /**
   * Returns distinct elements from a sequence by using
   * the default equality comparer to compare values.
   *
   * 将元素集合存放到set中
   */
  Enumerable<TSource> distinct();

  /**
   * Returns distinct elements from a sequence by using
   * a specified {@code EqualityComparer<TSource>} to compare values.
   */
  Enumerable<TSource> distinct(EqualityComparer<TSource> comparer);

  /**
   * Returns the element at a specified index in a
   * sequence.
   */
  TSource elementAt(int index);

  /**
   * Returns the element at a specified index in a
   * sequence or a default value if the index is out of
   * range.
   */
  TSource elementAtOrDefault(int index);

  /**
   * Produces the set difference of two sequences by
   * using the default equality comparer to compare values. (Defined
   * by Enumerable.)
   *
   * 返回集合的子集,从集合中删除参数集合内的数据。
   * set<>.remove(enumerable1)
   */
  Enumerable<TSource> except(Enumerable<TSource> enumerable1);

  /**
   * Produces the set difference of two sequences by
   * using the specified {@code EqualityComparer<TSource>} to compare
   * values.
   */
  Enumerable<TSource> except(Enumerable<TSource> enumerable1,
      EqualityComparer<TSource> comparer);

  /**
   * Returns the first element of a sequence. (Defined
   * by Enumerable.)
   * 返回第一个元素
   */
  TSource first();

  /**
   * Returns the first element in a sequence that
   * satisfies a specified condition.
   * 返回满足条件的第一个元素
   */
  TSource first(Predicate1<TSource> predicate);

  /**
   * Returns the first element of a sequence, or a
   * default value if the sequence contains no elements.
   */
  TSource firstOrDefault();

  /**
   * Returns the first element of the sequence that
   * satisfies a condition or a default value if no such element is
   * found.
   */
  TSource firstOrDefault(Predicate1<TSource> predicate);

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function.
   *
   * 1.LookupImpl 将集合按照key分组,转换成Map<k,List<v>>
   * 2.返回每一个map的entry对象即可组成 key,list --> 转换成key,value迭代器 --> 转换成Grouping<TKey, TSource>
   */
  <TKey> Enumerable<Grouping<TKey, TSource>> groupBy(
      Function1<TSource, TKey> keySelector);

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function and compares the keys by using
   * a specified comparer.
   */
  <TKey> Enumerable<Grouping<TKey, TSource>> groupBy(
      Function1<TSource, TKey> keySelector, EqualityComparer<TKey> comparer);

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function and projects the elements for
   * each group by using a specified function.
   *
   * 增加对value的处理函数,即map存储的list不在是value,而是经过转换的value
   */
  <TKey, TElement> Enumerable<Grouping<TKey, TElement>> groupBy(
      Function1<TSource, TKey> keySelector,
      Function1<TSource, TElement> elementSelector);

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function and creates a result value from
   * each group and its key.
   *
   * 对map的<k,list<v>> 再进一步转换,elementSelector转换成<TKey, TResult>
   */
  <TKey, TResult> Enumerable<Grouping<TKey, TResult>> groupBy(
      Function1<TSource, TKey> keySelector,
      Function2<TKey, Enumerable<TSource>, TResult> elementSelector);

  /**
   * Groups the elements of a sequence according to a
   * key selector function. The keys are compared by using a
   * comparer and each group's elements are projected by using a
   * specified function.
   */
  <TKey, TElement> Enumerable<Grouping<TKey, TElement>> groupBy(
      Function1<TSource, TKey> keySelector,
      Function1<TSource, TElement> elementSelector, EqualityComparer comparer);

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function and creates a result value from
   * each group and its key. The keys are compared by using a
   * specified comparer.
   */
  <TKey, TResult> Enumerable<TResult> groupBy(
      Function1<TSource, TKey> keySelector,
      Function2<TKey, Enumerable<TSource>, TResult> elementSelector,
      EqualityComparer comparer);

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function and creates a result value from
   * each group and its key. The elements of each group are
   * projected by using a specified function.
   */
  <TKey, TElement, TResult> Enumerable<TResult> groupBy(
      Function1<TSource, TKey> keySelector,
      Function1<TSource, TElement> elementSelector,
      Function2<TKey, Enumerable<TElement>, TResult> resultSelector);

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function and creates a result value from
   * each group and its key. Key values are compared by using a
   * specified comparer, and the elements of each group are
   * projected by using a specified function.
   */
  <TKey, TElement, TResult> Enumerable<TResult> groupBy(
      Function1<TSource, TKey> keySelector,
      Function1<TSource, TElement> elementSelector,
      Function2<TKey, Enumerable<TElement>, TResult> resultSelector,
      EqualityComparer<TKey> comparer);

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function, initializing an accumulator for each
   * group and adding to it each time an element with the same key is seen.
   * Creates a result value from each accumulator and its key using a
   * specified function.
   */
  <TKey, TAccumulate, TResult> Enumerable<TResult> groupBy(
      Function1<TSource, TKey> keySelector,
      Function0<TAccumulate> accumulatorInitializer,
      Function2<TAccumulate, TSource, TAccumulate> accumulatorAdder,
      Function2<TKey, TAccumulate, TResult> resultSelector);

  /**
   * Groups the elements of a sequence according to a
   * specified key selector function, initializing an accumulator for each
   * group and adding to it each time an element with the same key is seen.
   * Creates a result value from each accumulator and its key using a
   * specified function. Key values are compared by using a
   * specified comparer.
   */
  <TKey, TAccumulate, TResult> Enumerable<TResult> groupBy(
      Function1<TSource, TKey> keySelector,
      Function0<TAccumulate> accumulatorInitializer,
      Function2<TAccumulate, TSource, TAccumulate> accumulatorAdder,
      Function2<TKey, TAccumulate, TResult> resultSelector,
      EqualityComparer<TKey> comparer);

  /**
   * Correlates the elements of two sequences based on
   * equality of keys and groups the results. The default equality
   * comparer is used to compare keys.
   * 相当于join操作,只是匹配的是 左边表+右边匹配的list一起参与运算,计算结果
   */
  <TInner, TKey, TResult> Enumerable<TResult> groupJoin(
      Enumerable<TInner> inner, //右边表
      Function1<TSource, TKey> outerKeySelector,//左边表如何转换成key
      Function1<TInner, TKey> innerKeySelector,//左边表如何转换成key
      Function2<TSource, Enumerable<TInner>, TResult> resultSelector); //左边数据+右边list匹配的集合,返回结果

  /**
   * Correlates the elements of two sequences based on
   * key equality and groups the results. A specified
   * {@code EqualityComparer<TSource>} is used to compare keys.
   */
  <TInner, TKey, TResult> Enumerable<TResult> groupJoin(
      Enumerable<TInner> inner,
      Function1<TSource, TKey> outerKeySelector,
      Function1<TInner, TKey> innerKeySelector,
      Function2<TSource, Enumerable<TInner>, TResult> resultSelector,
      EqualityComparer<TKey> comparer);

  /**
   * Produces the set intersection of two sequences by
   * using the default equality comparer to compare values. (Defined
   * by Enumerable.)
   *
   * 两个集合去交集。
   * new set().add( set1.contain(enumerable1的元素))
   */
  Enumerable<TSource> intersect(Enumerable<TSource> enumerable1);

  /**
   * Produces the set intersection of two sequences by
   * using the specified {@code EqualityComparer<TSource>} to compare
   * values.
   */
  Enumerable<TSource> intersect(Enumerable<TSource> enumerable1,
      EqualityComparer<TSource> comparer);

  /**
   * Copies the contents of the sequence into a collection.
   *
   * 复制集合 for(value --> Collection.add(value))
   */
  <C extends Collection<? super TSource>> C into(C sink);

  /**
   * Correlates the elements of two sequences based on
   * matching keys. The default equality comparer is used to compare
   * keys.
   */
  <TInner, TKey, TResult> Enumerable<TResult> join(Enumerable<TInner> inner,
      Function1<TSource, TKey> outerKeySelector,
      Function1<TInner, TKey> innerKeySelector,
      Function2<TSource, TInner, TResult> resultSelector);

  /**
   * Correlates the elements of two sequences based on
   * matching keys. A specified {@code EqualityComparer<TSource>} is used to
   * compare keys.
   */
  <TInner, TKey, TResult> Enumerable<TResult> join(Enumerable<TInner> inner,
      Function1<TSource, TKey> outerKeySelector,
      Function1<TInner, TKey> innerKeySelector,
      Function2<TSource, TInner, TResult> resultSelector,
      EqualityComparer<TKey> comparer);

  /**
   * Correlates the elements of two sequences based on matching keys, with
   * optional outer join semantics. A specified
   * {@code EqualityComparer<TSource>} is used to compare keys.
   *
   * <p>A left join generates nulls on right, and vice versa:</p>
   *
   * <table>
   *   <caption>Join types</caption>
   *   <tr>
   *     <td>Join type</td>
   *     <td>generateNullsOnLeft</td>
   *     <td>generateNullsOnRight</td>
   *   </tr>
   *   <tr><td>INNER</td><td>false</td><td>false</td></tr>
   *   <tr><td>LEFT</td><td>false</td><td>true</td></tr>
   *   <tr><td>RIGHT</td><td>true</td><td>false</td></tr>
   *   <tr><td>FULL</td><td>true</td><td>true</td></tr>
   * </table>
   *
   * 讲右表转换成Map，然后循环左表,去右表Map中找到list,与list做循环
   */
  <TInner, TKey, TResult> Enumerable<TResult> join(Enumerable<TInner> inner,//右表数据
      Function1<TSource, TKey> outerKeySelector,//提取左表key
      Function1<TInner, TKey> innerKeySelector,//提取右表key
      Function2<TSource, TInner, TResult> resultSelector,//左表一行数据+右表一行数据,生产结果数据
      EqualityComparer<TKey> comparer,
      boolean generateNullsOnLeft, //左表允许null,用于left join等场景
      boolean generateNullsOnRight);//左表允许null,用于left join等场景

  /**
   * For each row of the current enumerable returns the correlated rows
   * from the {@code inner} enumerable (nested loops join).
   *
   * @param joinType inner, left, semi or anti join type
   * @param inner generator of inner enumerable
   * @param resultSelector selector of the result. For semi/anti join
   *                       inner argument is always null.
   * 相互关联的join
   * 不提供右边表,但提供通过key查找右边表匹配的list方法
   *
   * 其实就是简单的join操作
   */
  <TInner, TResult> Enumerable<TResult> correlateJoin(
      CorrelateJoinType joinType,
      Function1<TSource, Enumerable<TInner>> inner,//右边表
      Function2<TSource, TInner, TResult> resultSelector);//完成join操作

  /**
   * Returns the last element of a sequence. (Defined
   * by Enumerable.)
   */
  TSource last();

  /**
   * Returns the last element of a sequence that
   * satisfies a specified condition.
   */
  TSource last(Predicate1<TSource> predicate);

  /**
   * Returns the last element of a sequence, or a
   * default value if the sequence contains no elements.
   */
  TSource lastOrDefault();

  /**
   * Returns the last element of a sequence that
   * satisfies a condition or a default value if no such element is
   * found.
   */
  TSource lastOrDefault(Predicate1<TSource> predicate);

  /**
   * Returns the maximum value in a generic
   * sequence.
   *
   * 返回集合中最大的元素
   */
  TSource max();

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the maximum Decimal value.
   *
   *
   * 将元素先map转换成double类型,然后返回集合中最大的元素
   */
  BigDecimal max(BigDecimalFunction1<TSource> selector);

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the maximum nullable Decimal
   * value.
   */
  BigDecimal max(NullableBigDecimalFunction1<TSource> selector);

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the maximum Double value.
   */
  double max(DoubleFunction1<TSource> selector);

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the maximum nullable Double
   * value.
   */
  Double max(NullableDoubleFunction1<TSource> selector);

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the maximum int value.
   */
  int max(IntegerFunction1<TSource> selector);

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the maximum nullable int value. (Defined
   * by Enumerable.)
   */
  Integer max(NullableIntegerFunction1<TSource> selector);

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the maximum long value.
   */
  long max(LongFunction1<TSource> selector);

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the maximum nullable long value. (Defined
   * by Enumerable.)
   */
  Long max(NullableLongFunction1<TSource> selector);

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the maximum Float value.
   */
  float max(FloatFunction1<TSource> selector);

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the maximum nullable Float
   * value.
   */
  Float max(NullableFloatFunction1<TSource> selector);

  /**
   * Invokes a transform function on each element of a
   * generic sequence and returns the maximum resulting
   * value.
   */
  <TResult extends Comparable<TResult>> TResult max(
      Function1<TSource, TResult> selector);

  /**
   * Returns the minimum value in a generic
   * sequence.
   */
  TSource min();

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the minimum Decimal value.
   */
  BigDecimal min(BigDecimalFunction1<TSource> selector);

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the minimum nullable Decimal
   * value.
   */
  BigDecimal min(NullableBigDecimalFunction1<TSource> selector);

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the minimum Double value.
   */
  double min(DoubleFunction1<TSource> selector);

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the minimum nullable Double
   * value.
   */
  Double min(NullableDoubleFunction1<TSource> selector);

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the minimum int value.
   */
  int min(IntegerFunction1<TSource> selector);

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the minimum nullable int value. (Defined
   * by Enumerable.)
   */
  Integer min(NullableIntegerFunction1<TSource> selector);

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the minimum long value.
   */
  long min(LongFunction1<TSource> selector);

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the minimum nullable long value. (Defined
   * by Enumerable.)
   */
  Long min(NullableLongFunction1<TSource> selector);

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the minimum Float value.
   */
  float min(FloatFunction1<TSource> selector);

  /**
   * Invokes a transform function on each element of a
   * sequence and returns the minimum nullable Float
   * value.
   */
  Float min(NullableFloatFunction1<TSource> selector);

  /**
   * Invokes a transform function on each element of a
   * generic sequence and returns the minimum resulting
   * value.
   */
  <TResult extends Comparable<TResult>> TResult min(
      Function1<TSource, TResult> selector);

  /**
   * Filters the elements of an Enumerable based on a
   * specified type.
   *
   * <p>Analogous to LINQ's Enumerable.OfType extension method.</p>
   *
   * @param clazz Target type
   * @param <TResult> Target type
   *
   * @return Collection of T2
   * 匹配元素是class的子类的元素被保留
   *
   * for(value --> value instance clazz)
   */
  <TResult> Enumerable<TResult> ofType(Class<TResult> clazz);

  /**
   * Sorts the elements of a sequence in ascending
   * order according to a key.
   * 将k转换成可以排序的value
   *
   * 先进行LookupImpl处理,将数据转换成treeMap --> 输出treeMap中value迭代器,自然就有顺序的
   */
  <TKey extends Comparable> Enumerable<TSource> orderBy(
      Function1<TSource, TKey> keySelector);

  /**
   * Sorts the elements of a sequence in ascending
   * order by using a specified comparer.
   *
   * 先进行LookupImpl处理,将数据转换成treeMap --> 输出treeMap中value迭代器,自然就有顺序的
   */
  <TKey> Enumerable<TSource> orderBy(Function1<TSource, TKey> keySelector,
      Comparator<TKey> comparator);

  /**
   * Sorts the elements of a sequence in descending
   * order according to a key.
   */
  <TKey extends Comparable> Enumerable<TSource> orderByDescending(
      Function1<TSource, TKey> keySelector);

  /**
   * Sorts the elements of a sequence in descending
   * order by using a specified comparer.
   */
  <TKey> Enumerable<TSource> orderByDescending(
      Function1<TSource, TKey> keySelector, Comparator<TKey> comparator);

  /**
   * Inverts the order of the elements in a
   * sequence.
   *
   * 将元素反过来,即从后往前输出集合
   */
  Enumerable<TSource> reverse();

  /**
   * Projects each element of a sequence into a new
   * form.
   *
   * 相当于map操作,转换成新的数据集合
   */
  <TResult> Enumerable<TResult> select(Function1<TSource, TResult> selector);

  /**
   * Projects each element of a sequence into a new
   * form by incorporating the element's index.
   *
   * 相当于map操作,转换成新的数据集合,只是转换函数是由 <元素+序号(从0开始计数))组成
   */
  <TResult> Enumerable<TResult> select(
      Function2<TSource, Integer, TResult> selector);

  /**
   * Projects each element of a sequence to an
   * {@code Enumerable<TSource>} and flattens the resulting sequences into one
   * sequence.
   * 每一个元素转换成多个元素,相当于flatMap
   */
  <TResult> Enumerable<TResult> selectMany(
      Function1<TSource, Enumerable<TResult>> selector);

  /**
   * Projects each element of a sequence to an
   * {@code Enumerable<TSource>}, and flattens the resulting sequences into one
   * sequence. The index of each source element is used in the
   * projected form of that element.
   */
  <TResult> Enumerable<TResult> selectMany(
      Function2<TSource, Integer, Enumerable<TResult>> selector);

  /**
   * Projects each element of a sequence to an
   * {@code Enumerable<TSource>}, flattens the resulting sequences into one
   * sequence, and invokes a result selector function on each
   * element therein. The index of each source element is used in
   * the intermediate projected form of that element.
   */
  <TCollection, TResult> Enumerable<TResult> selectMany(
      Function2<TSource, Integer, Enumerable<TCollection>> collectionSelector,
      Function2<TSource, TCollection, TResult> resultSelector);

  /**
   * Projects each element of a sequence to an
   * {@code Enumerable<TSource>}, flattens the resulting sequences into one
   * sequence, and invokes a result selector function on each
   * element therein.
   */
  <TCollection, TResult> Enumerable<TResult> selectMany(
      Function1<TSource, Enumerable<TCollection>> collectionSelector,
      Function2<TSource, TCollection, TResult> resultSelector);

  /**
   * Determines whether two sequences are equal by
   * comparing the elements by using the default equality comparer
   * for their type.
   */
  boolean sequenceEqual(Enumerable<TSource> enumerable1);

  /**
   * Determines whether two sequences are equal by
   * comparing their elements by using a specified
   * {@code EqualityComparer<TSource>}.
   */
  boolean sequenceEqual(Enumerable<TSource> enumerable1,
      EqualityComparer<TSource> comparer);

  /**
   * Returns the only element of a sequence, and throws
   * an exception if there is not exactly one element in the
   * sequence.
   */
  TSource single();

  /**
   * Returns the only element of a sequence that
   * satisfies a specified condition, and throws an exception if
   * more than one such element exists.
   */
  TSource single(Predicate1<TSource> predicate);

  /**
   * Returns the only element of a sequence, or a
   * default value if the sequence is empty; this method throws an
   * exception if there is more than one element in the
   * sequence.
   */
  TSource singleOrDefault();

  /**
   * Returns the only element of a sequence that
   * satisfies a specified condition or a default value if no such
   * element exists; this method throws an exception if more than
   * one element satisfies the condition.
   */
  TSource singleOrDefault(Predicate1<TSource> predicate);

  /**
   * Bypasses a specified number of elements in a
   * sequence and then returns the remaining elements.
   * 跳过n个元素,使用skipWhile实现
   */
  Enumerable<TSource> skip(int count);

  /**
   * Bypasses elements in a sequence as long as a
   * specified condition is true and then returns the remaining
   * elements.
   * 跳过参数是false的数据
   */
  Enumerable<TSource> skipWhile(Predicate1<TSource> predicate);

  /**
   * Bypasses elements in a sequence as long as a
   * specified condition is true and then returns the remaining
   * elements. The element's index is used in the logic of the
   * predicate function.
   */
  Enumerable<TSource> skipWhile(Predicate2<TSource, Integer> predicate);

  /**
   * Computes the sum of the sequence of Decimal values
   * that are obtained by invoking a transform function on each
   * element of the input sequence.
   *
   * for(value --> map(value)).sum() 先将元素转换成double,然后求和
   */
  BigDecimal sum(BigDecimalFunction1<TSource> selector);

  /**
   * Computes the sum of the sequence of nullable
   * Decimal values that are obtained by invoking a transform
   * function on each element of the input sequence.
   */
  BigDecimal sum(NullableBigDecimalFunction1<TSource> selector);

  /**
   * Computes the sum of the sequence of Double values
   * that are obtained by invoking a transform function on each
   * element of the input sequence.
   */
  double sum(DoubleFunction1<TSource> selector);

  /**
   * Computes the sum of the sequence of nullable
   * Double values that are obtained by invoking a transform
   * function on each element of the input sequence.
   */
  Double sum(NullableDoubleFunction1<TSource> selector);

  /**
   * Computes the sum of the sequence of int values
   * that are obtained by invoking a transform function on each
   * element of the input sequence.
   */
  int sum(IntegerFunction1<TSource> selector);

  /**
   * Computes the sum of the sequence of nullable int
   * values that are obtained by invoking a transform function on
   * each element of the input sequence.
   */
  Integer sum(NullableIntegerFunction1<TSource> selector);

  /**
   * Computes the sum of the sequence of long values
   * that are obtained by invoking a transform function on each
   * element of the input sequence.
   */
  long sum(LongFunction1<TSource> selector);

  /**
   * Computes the sum of the sequence of nullable long
   * values that are obtained by invoking a transform function on
   * each element of the input sequence.
   */
  Long sum(NullableLongFunction1<TSource> selector);

  /**
   * Computes the sum of the sequence of Float values
   * that are obtained by invoking a transform function on each
   * element of the input sequence.
   */
  float sum(FloatFunction1<TSource> selector);

  /**
   * Computes the sum of the sequence of nullable
   * Float values that are obtained by invoking a transform
   * function on each element of the input sequence.
   */
  Float sum(NullableFloatFunction1<TSource> selector);

  /**
   * Returns a specified number of contiguous elements
   * from the start of a sequence.
   *
   * 获取前count条元素。
   * 调用takeWhile函数,满足条件是元素序号<count
   */
  Enumerable<TSource> take(int count);

  /**
   * Returns elements from a sequence as long as a
   * specified condition is true.
   *
   * 实现while语法,直到函数返回值false时停止循环,即保留true的数据
   * for(value) {
   *
   *     boolean b = predicate(value,index)
   *     if(b == false) return
   * }
   */
  Enumerable<TSource> takeWhile(Predicate1<TSource> predicate);

  /**
   * Returns elements from a sequence as long as a
   * specified condition is true. The element's index is used in the
   * logic of the predicate function.
   */
  Enumerable<TSource> takeWhile(Predicate2<TSource, Integer> predicate);

  /**
   * Creates a {@code Map<TKey, TValue>} from an
   * {@code Enumerable<TSource>} according to a specified key selector
   * function.
   *
   * <p>NOTE: Called {@code toDictionary} in LINQ.NET.</p>
   *
   * 将集合转换成map,通过提供函数keySelector将value转换成key
   * for(value --> map.put(keySelector(value),value) )
   */
  <TKey> Map<TKey, TSource> toMap(Function1<TSource, TKey> keySelector);

  /**
   * Creates a {@code Map<TKey, TValue>} from an
   * {@code Enumerable<TSource>} according to a specified key selector function
   * and key comparer.
   */
  <TKey> Map<TKey, TSource> toMap(Function1<TSource, TKey> keySelector,
      EqualityComparer<TKey> comparer);

  /**
   * Creates a {@code Map<TKey, TValue>} from an
   * {@code Enumerable<TSource>} according to specified key selector and element
   * selector functions.
   * 将集合转换成map,通过提供函数keySelector将value转换成key
   * for(value --> map.put(keySelector(value),elementSelector(value)) )
   *
   */
  <TKey, TElement> Map<TKey, TElement> toMap(
      Function1<TSource, TKey> keySelector,
      Function1<TSource, TElement> elementSelector);

  /**
   * Creates a {@code Map<TKey, TValue>} from an
   * {@code Enumerable<TSource>} according to a specified key selector function,
   * a comparer, and an element selector function.
   */
  <TKey, TElement> Map<TKey, TElement> toMap(
      Function1<TSource, TKey> keySelector,
      Function1<TSource, TElement> elementSelector,
      EqualityComparer<TKey> comparer);

  /**
   * Creates a {@code List<TSource>} from an {@code Enumerable<TSource>}.
   * 将集合转换成List
   * for(value --> list.add(value))
   */
  List<TSource> toList();

  /**
   * Creates a {@code Lookup<TKey, TElement>} from an
   * {@code Enumerable<TSource>} according to a specified key selector
   * function.
   *
   *
   * 转换成Map<K,List<V>> --- 循环元素,将相同的元素放在List里,转换成map<key,List<value>>形式
   */
  <TKey> Lookup<TKey, TSource> toLookup(Function1<TSource, TKey> keySelector);

  /**
   * Creates a {@code Lookup<TKey, TElement>} from an
   * {@code Enumerable<TSource>} according to a specified key selector function
   * and key comparer.
   *
   * 转换成TreeMap<K,List<V>> --- 循环元素,将相同的元素放在List里,转换成map<key,List<value>>形式
   */
  <TKey> Lookup<TKey, TSource> toLookup(Function1<TSource, TKey> keySelector,
      EqualityComparer<TKey> comparer);

  /**
   * Creates a {@code Lookup<TKey, TElement>} from an
   * {@code Enumerable<TSource>} according to specified key selector and element
   * selector functions.
   *
   * 转换成TreeMap<K,List<V>> --- 循环元素,将相同的元素放在List里,转换成map<key,List<value>>形式
   */
  <TKey, TElement> Lookup<TKey, TElement> toLookup(
      Function1<TSource, TKey> keySelector,
      Function1<TSource, TElement> elementSelector);

  /**
   * Creates a {@code Lookup<TKey, TElement>} from an
   * {@code Enumerable<TSource>} according to a specified key selector function,
   * a comparer and an element selector function.
   *
   * 转换成TreeMap<K,List<V>> --- 循环元素,将相同的元素放在List里,转换成map<key,List<value>>形式
   */
  <TKey, TElement> Lookup<TKey, TElement> toLookup(
      Function1<TSource, TKey> keySelector,
      Function1<TSource, TElement> elementSelector,
      EqualityComparer<TKey> comparer);

  /**
   * Produces the set union of two sequences by using
   * the default equality comparer.
   *
   * 将两个集合做并集
   * new set().addALL(source).addALL(source1)
   */
  Enumerable<TSource> union(Enumerable<TSource> source1);

  /**
   * Produces the set union of two sequences by using a
   * specified {@code EqualityComparer<TSource>}.
   */
  Enumerable<TSource> union(Enumerable<TSource> source1,
      EqualityComparer<TSource> comparer);

  /**
   * Filters a sequence of values based on a
   * predicate.
   * 返回结果是true的元素
   */
  Enumerable<TSource> where(Predicate1<TSource> predicate);

  /**
   * Filters a sequence of values based on a
   * predicate. Each element's index is used in the logic of the
   * predicate function.
   * 返回结果是true的元素,参数是元素值+元素序号,序号从0开始计数,即第几条数据
   */
  Enumerable<TSource> where(Predicate2<TSource, Integer> predicate);

  /**
   * Applies a specified function to the corresponding
   * elements of two sequences, producing a sequence of the
   * results.
   */
  <T1, TResult> Enumerable<TResult> zip(Enumerable<T1> source1,
      Function2<TSource, T1, TResult> resultSelector);
}

// End ExtendedEnumerable.java
