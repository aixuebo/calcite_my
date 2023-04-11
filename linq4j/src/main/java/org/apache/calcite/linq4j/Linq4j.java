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

import com.google.common.collect.Lists;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.RandomAccess;

/**
 * Utility and factory methods for Linq4j.
 *
 * 核心入口
 1.基础迭代器功能
 Enumerable a = Linq4j.asEnumerable(Iterable) //迭代一个集合,可以是Iterable、List、Collection、T[]
 Enumerable a = Linq4j.emptyEnumerable() //迭代一个空集合
 Enumerable<T> a = Linq4j.singletonEnumerable(T element) //返回只有一个元素的集合
 Enumerator<T> a = Linq4j.singletonNullEnumerator() //返回只有一个元素,并且该元素是null的集合

 2.Enumerator与Iterator转换
 Iterator<T> enumeratorIterator(Enumerator<T> enumerator)
 Enumerator<T> iterableEnumerator(final Iterable<? extends T> iterable)
 Enumerator<List<T>> product(List<Enumerator<T>> enumerators)
 Iterable<List<T>> product(final Iterable<? extends Iterable<T>> iterables)

 3.迭代器高级功能
 Enumerable<TResult> cast(Iterable<TSource> source, Class<TResult> clazz) 将元素强转成class对象
 Enumerable<TResult> ofType(Iterable<TSource> source, Class<TResult> clazz) 将元素强转成class对象
 Enumerable<E> concat(final List<Enumerable<E>> enumerableList) 依次循环Enumerable中每一个元素,即相当于new list().addAll(value).addAll(value)

 QueryProvider query = Linq4j.DEFAULT_PROVIDER

 4.demo:
 try {
 Enumerator<String> result = Linq4j.enumerator(list);
 while(result.moveNext()){
 System.out.println(result.current());
 }

 Enumerable result2 = Linq4j.asEnumerable(list).select(new Function1<String,Integer>(){
 public Integer apply(String v0) {
 return 1;
 }
 });

 Enumerator<Integer> result3 = result2.enumerator();
 while(result3.moveNext()){
 System.out.println(result3.current());
 }

 } catch (Exception e) {
 e.printStackTrace();
 }

 */
public abstract class Linq4j {
  private Linq4j() {}

  private static final Object DUMMY = new Object();

  private static final Method AUTO_CLOSEABLE_CLOSE_METHOD =
      getMethod("java.lang.AutoCloseable", "close");

  //反射返回某一个Method对象
  public static Method getMethod(String className, String methodName,
      Class... parameterTypes) {
    try {
      return Class.forName(className).getMethod(methodName, parameterTypes);
    } catch (NoSuchMethodException e) {
      return null;
    } catch (ClassNotFoundException e) {
      return null;
    }
  }

  /**
   * Query provider that simply executes a {@link Queryable} by calling its
   * enumerator method; does not attempt optimization.
   */
  public static final QueryProvider DEFAULT_PROVIDER = new QueryProviderImpl() {
    public <T> Enumerator<T> executeQuery(Queryable<T> queryable) {
      return queryable.enumerator();
    }
  };

  //空集合迭代器
  private static final Enumerator<Object> EMPTY_ENUMERATOR =
      new Enumerator<Object>() {
        public Object current() {
          throw new NoSuchElementException();
        }

        public boolean moveNext() {
          return false;
        }

        public void reset() {
        }

        public void close() {
        }
      };

  public static final Enumerable<?> EMPTY_ENUMERABLE =
      new AbstractEnumerable<Object>() {
        public Enumerator<Object> enumerator() {
          return EMPTY_ENUMERATOR;
        }
      };

  /**
   * Adapter that converts an enumerator into an iterator.
   *
   * <p><b>WARNING</b>: The iterator returned by this method does not call
   * {@link org.apache.calcite.linq4j.Enumerator#close()}, so it is not safe to
   * use with an enumerator that allocates resources.</p>
   *
   * @param enumerator Enumerator
   * @param <T> Element type
   *
   * @return Iterator
   * 将Enumerator转换成Iterator处理
   */
  public static <T> Iterator<T> enumeratorIterator(Enumerator<T> enumerator) {
    return new EnumeratorIterator<T>(enumerator);
  }

  /**
   * Adapter that converts an iterable into an enumerator.
   *
   * @param iterable Iterable
   * @param <T> Element type
   *
   * @return enumerator
   */
  public static <T> Enumerator<T> iterableEnumerator(
      final Iterable<? extends T> iterable) {
    if (iterable instanceof Enumerable) {
      @SuppressWarnings("unchecked") final Enumerable<T> enumerable =
          (Enumerable) iterable;
      return enumerable.enumerator();
    }
    return new IterableEnumerator<T>(iterable);
  }

  /**
   * Adapter that converts an {@link List} into an {@link Enumerable}.
   *
   * @param list List
   * @param <T> Element type
   *
   * @return enumerable
   */
  public static <T> Enumerable<T> asEnumerable(final List<T> list) {
    return new ListEnumerable<T>(list);
  }

  /**
   * Adapter that converts an {@link Collection} into an {@link Enumerable}.
   *
   * <p>It uses more efficient implementations if the iterable happens to
   * be a {@link List}.</p>
   *
   * @param collection Collection
   * @param <T> Element type
   *
   * @return enumerable
   */
  public static <T> Enumerable<T> asEnumerable(final Collection<T> collection) {
    if (collection instanceof List) {
      //noinspection unchecked
      return asEnumerable((List) collection);
    }
    return new CollectionEnumerable<T>(collection);
  }

  /**
   * Adapter that converts an {@link Iterable} into an {@link Enumerable}.
   *
   * <p>It uses more efficient implementations if the iterable happens to
   * be a {@link Collection} or a {@link List}.</p>
   *
   * @param iterable Iterable
   * @param <T> Element type
   *
   * @return enumerable
   */
  public static <T> Enumerable<T> asEnumerable(final Iterable<T> iterable) {
    if (iterable instanceof Collection) {
      //noinspection unchecked
      return asEnumerable((Collection) iterable);
    }
    return new IterableEnumerable<T>(iterable);
  }

  /**
   * Adapter that converts an array into an enumerable.
   *
   * @param ts Array
   * @param <T> Element type
   *
   * @return enumerable
   */
  public static <T> Enumerable<T> asEnumerable(final T[] ts) {
    return new ListEnumerable<T>(Arrays.asList(ts));
  }

  /**
   * Adapter that converts a collection into an enumerator.
   *
   * @param values Collection
   * @param <V> Element type
   *
   * @return Enumerator over the collection
   */
  public static <V> Enumerator<V> enumerator(Collection<? extends V> values) {
    if (values instanceof List && values instanceof RandomAccess) {
      //noinspection unchecked
      return listEnumerator((List) values);
    }
    return iterableEnumerator(values);
  }

  private static <V> Enumerator<V> listEnumerator(List<? extends V> list) {
    return new ListEnumerator<V>(list);
  }

  /**
   * Converts the elements of a given Iterable to the specified type.
   *
   * <p>This method is implemented by using deferred execution. The immediate
   * return value is an object that stores all the information that is
   * required to perform the action. The query represented by this method is
   * not executed until the object is enumerated either by calling its
   * {@link Enumerable#enumerator} method directly or by using
   * {@code for (... in ...)}.
   *
   * <p>Since standard Java {@link Collection} objects implement the
   * {@link Iterable} interface, the {@code cast} method enables the standard
   * query operators to be invoked on collections
   * (including {@link java.util.List} and {@link java.util.Set}) by supplying
   * the necessary type information. For example, {@link ArrayList} does not
   * implement {@link Enumerable}&lt;T&gt;, but you can invoke
   *
   * <blockquote><code>Linq4j.cast(list, Integer.class)</code></blockquote>
   *
   * to convert the list of an enumerable that can be queried using the
   * standard query operators.
   *
   * <p>If an element cannot be cast to type &lt;TResult&gt;, this method will
   * throw a {@link ClassCastException}. To obtain only those elements that
   * can be cast to type TResult, use the {@link #ofType} method instead.
   *
   * @see Enumerable#cast(Class)
   * @see #ofType
   * @see #asEnumerable(Iterable)
   */
  public static <TSource, TResult> Enumerable<TResult> cast(
      Iterable<TSource> source, Class<TResult> clazz) {
    return asEnumerable(source).cast(clazz);
  }

  /**
   * Returns elements of a given {@link Iterable} that are of the specified
   * type.
   *
   * <p>This method is implemented by using deferred execution. The immediate
   * return value is an object that stores all the information that is
   * required to perform the action. The query represented by this method is
   * not executed until the object is enumerated either by calling its
   * {@link Enumerable#enumerator} method directly or by using
   * {@code for (... in ...)}.
   *
   * <p>The {@code ofType} method returns only those elements in source that
   * can be cast to type TResult. To instead receive an exception if an
   * element cannot be cast to type TResult, use
   * {@link #cast(Iterable, Class)}.</p>
   *
   * <p>Since standard Java {@link Collection} objects implement the
   * {@link Iterable} interface, the {@code cast} method enables the standard
   * query operators to be invoked on collections
   * (including {@link java.util.List} and {@link java.util.Set}) by supplying
   * the necessary type information. For example, {@link ArrayList} does not
   * implement {@link Enumerable}&lt;T&gt;, but you can invoke
   *
   * <blockquote><code>Linq4j.ofType(list, Integer.class)</code></blockquote>
   *
   * to convert the list of an enumerable that can be queried using the
   * standard query operators.
   *
   * @see Enumerable#cast(Class)
   * @see #cast
   */
  public static <TSource, TResult> Enumerable<TResult> ofType(
      Iterable<TSource> source, Class<TResult> clazz) {
    return asEnumerable(source).ofType(clazz);
  }

  /**
   * Returns an {@link Enumerable} that has one element.
   *
   * @param <T> Element type
   *
   * @return Singleton enumerable
   */
  public static <T> Enumerable<T> singletonEnumerable(final T element) {
    return new AbstractEnumerable<T>() {
      public Enumerator<T> enumerator() {
        return singletonEnumerator(element);
      }
    };
  }

  /**
   * Returns an {@link Enumerator} that has one element.
   *
   * @param <T> Element type
   *
   * @return Singleton enumerator
   */
  public static <T> Enumerator<T> singletonEnumerator(T element) {
    return new SingletonEnumerator<T>(element);
  }

  /**
   * Returns an {@link Enumerator} that has one null element.
   *
   * @param <T> Element type
   *
   * @return Singleton enumerator
   */
  public static <T> Enumerator<T> singletonNullEnumerator() {
    return new SingletonNullEnumerator<T>();
  }

  /**
   * Returns an {@link Enumerable} that has no elements.
   *
   * @param <T> Element type
   *
   * @return Empty enumerable
   */
  public static <T> Enumerable<T> emptyEnumerable() {
    //noinspection unchecked
    return (Enumerable<T>) EMPTY_ENUMERABLE;
  }

  /**
   * Returns an {@link Enumerator} that has no elements.
   *
   * @param <T> Element type
   *
   * @return Empty enumerator
   */
  public static <T> Enumerator<T> emptyEnumerator() {
    //noinspection unchecked
    return (Enumerator<T>) EMPTY_ENUMERATOR;
  }

  /**
   * Concatenates two or more {@link Enumerable}s to form a composite
   * enumerable that contains the union of their elements.
   *
   * @param enumerableList List of enumerable objects
   * @param <E> Element type
   *
   * @return Composite enumerator
   */
  public static <E> Enumerable<E> concat(
      final List<Enumerable<E>> enumerableList) {
    return new CompositeEnumerable<E>(enumerableList);
  }

  /**
   * Returns an enumerator that is the cartesian product of the given
   * enumerators.
   *
   * <p>For example, given enumerator A that returns {"a", "b", "c"} and
   * enumerator B that returns {"x", "y"}, product(List(A, B)) will return
   * {List("a", "x"), List("a", "y"),
   * List("b", "x"), List("b", "y"),
   * List("c", "x"), List("c", "y")}.</p>
   *
   * <p>Notice that the cardinality of the result is the product of the
   * cardinality of the inputs. The enumerators A and B have 3 and 2
   * elements respectively, and the result has 3 * 2 = 6 elements.
   * This is always the case. In
   * particular, if any of the enumerators is empty, the result is empty.</p>
   *
   * @param enumerators List of enumerators
   * @param <T> Element type
   *
   * @return Enumerator over the cartesian product
   */
  public static <T> Enumerator<List<T>> product(
      List<Enumerator<T>> enumerators) {
    return new CartesianProductEnumerator<T>(enumerators);
  }

  /** Returns the cartesian product of an iterable of iterables. */
  public static <T> Iterable<List<T>> product(
      final Iterable<? extends Iterable<T>> iterables) {
    return new Iterable<List<T>>() {
      public Iterator<List<T>> iterator() {
        final List<Enumerator<T>> enumerators = Lists.newArrayList();
        for (Iterable<T> iterable : iterables) {
          enumerators.add(iterableEnumerator(iterable));
        }
        return enumeratorIterator(
            new CartesianProductEnumerator<T>(enumerators));
      }
    };
  }

  /**
   * Returns whether the arguments are equal to each other.
   *
   * <p>Equivalent to {@code Objects#equals} in JDK 1.7 and above.</p>
   */
  public static <T> boolean equals(T t0, T t1) {
    return t0 == t1 || t0 != null && t0.equals(t1);
  }

  /**
   * Throws {@link NullPointerException} if argument is null, otherwise
   * returns argument.
   *
   * <p>Equivalent to {@code Objects#equireNonNull} in JDK 1.7 and above.</p>
   */
  public static <T> T requireNonNull(T o) {
    if (o == null) {
      throw new NullPointerException();
    }
    return o;
  }

  /** Closes an iterator, if it can be closed. */
  private static <T> void closeIterator(Iterator<T> iterator) {
    if (AUTO_CLOSEABLE_CLOSE_METHOD != null) {
      // JDK 1.7 or later
      if (AUTO_CLOSEABLE_CLOSE_METHOD.getDeclaringClass()
          .isInstance(iterator)) {
        try {
          AUTO_CLOSEABLE_CLOSE_METHOD.invoke(iterator);
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
          throw new RuntimeException(e.getCause());
        }
      }
    } else {
      // JDK 1.5 or 1.6. No AutoCloseable. Cover the two most common cases
      // with a close().
      if (iterator instanceof Closeable) {
        try {
          ((Closeable) iterator).close();
          return;
        } catch (RuntimeException e) {
          throw e;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      if (iterator instanceof ResultSet) {
        try {
          ((ResultSet) iterator).close();
        } catch (RuntimeException e) {
          throw e;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  /** Iterable enumerator.
   * Iterable 转换成 Enumerator。因为他俩天生就是可以相互转换的
   *
   * 因为iterable 可以重复转换成iterator,因此可以实现reset功能 iterable.iterator();
   *
   * 而iterator不能作为参数的原因是 他不能实现reset功能
   **/
  @SuppressWarnings("unchecked")
  static class IterableEnumerator<T> implements Enumerator<T> {
    private final Iterable<? extends T> iterable;
    Iterator<? extends T> iterator;
    T current;

    public IterableEnumerator(Iterable<? extends T> iterable) {
      this.iterable = iterable;
      iterator = iterable.iterator();
      current = (T) DUMMY;
    }

    public T current() {
      if (current == DUMMY) {
        throw new NoSuchElementException();
      }
      return current;
    }

    public boolean moveNext() {
      if (iterator.hasNext()) {
        current = iterator.next();
        return true;
      }
      current = (T) DUMMY;
      return false;
    }

    public void reset() {
      iterator = iterable.iterator();
      current = (T) DUMMY;
    }

    public void close() {
      final Iterator<? extends T> iterator1 = this.iterator;
      this.iterator = null;
      closeIterator(iterator1);
    }
  }

  /** Composite enumerable. */
  static class CompositeEnumerable<E> extends AbstractEnumerable<E> {
    private final Enumerator<Enumerable<E>> enumerableEnumerator;

    CompositeEnumerable(List<Enumerable<E>> enumerableList) {
      enumerableEnumerator = iterableEnumerator(enumerableList);
    }

    public Enumerator<E> enumerator() {
      return new Enumerator<E>() {
        // Never null.
        Enumerator<E> current = emptyEnumerator();

        public E current() {
          return current.current();
        }

        public boolean moveNext() {
          for (;;) {
            if (current.moveNext()) {
              return true;
            }
            current.close();
            if (!enumerableEnumerator.moveNext()) {
              current = emptyEnumerator();
              return false;
            }
            current = enumerableEnumerator.current().enumerator();
          }
        }

        public void reset() {
          enumerableEnumerator.reset();
          current = emptyEnumerator();
        }

        public void close() {
          current.close();
          current = emptyEnumerator();
        }
      };
    }
  }

  /** Iterable enumerable. */
  static class IterableEnumerable<T> extends AbstractEnumerable2<T> {
    protected final Iterable<T> iterable;

    IterableEnumerable(Iterable<T> iterable) {
      this.iterable = iterable;
    }

    public Iterator<T> iterator() {
      return iterable.iterator();
    }

    @Override public boolean any() {
      return iterable.iterator().hasNext();
    }
  }

  /** Collection enumerable. */
  static class CollectionEnumerable<T> extends IterableEnumerable<T> {
    CollectionEnumerable(Collection<T> iterable) {
      super(iterable);
    }

    protected Collection<T> getCollection() {
      return (Collection<T>) iterable;
    }

    @Override public int count() {
      return getCollection().size();
    }

    @Override public long longCount() {
      return getCollection().size();
    }

    @Override public boolean contains(T element) {
      return getCollection().contains(element);
    }

    @Override public boolean any() {
      return !getCollection().isEmpty();
    }
  }

  /** List enumerable. */
  static class ListEnumerable<T> extends CollectionEnumerable<T> {
    ListEnumerable(List<T> list) {
      super(list);
    }

    @Override public Enumerator<T> enumerator() {
      if (iterable instanceof RandomAccess) {
        //noinspection unchecked
        return new ListEnumerator<T>((List) iterable);
      }
      return super.enumerator();
    }

    @Override public List<T> toList() {
      return (List<T>) iterable;
    }

    @Override public Enumerable<T> skip(int count) {
      final List<T> list = toList();
      if (count >= list.size()) {
        return Linq4j.emptyEnumerable();
      }
      return new ListEnumerable<T>(list.subList(count, list.size()));
    }

    @Override public Enumerable<T> take(int count) {
      final List<T> list = toList();
      if (count >= list.size()) {
        return this;
      }
      return new ListEnumerable<T>(list.subList(0, count));
    }

    @Override public T elementAt(int index) {
      return toList().get(index);
    }
  }

  /** Enumerator that returns one element.
   * 只有一个非null元素的Enumerator
   **/
  private static class SingletonEnumerator<E> implements Enumerator<E> {
    final E e;
    int i = 0;

    SingletonEnumerator(E e) {
      this.e = e;
    }

    public E current() {
      return e;
    }

    public boolean moveNext() {
      return i++ == 0;
    }

    public void reset() {
      i = 0;
    }

    public void close() {
    }
  }

  /** Enumerator that returns one null element.
   * 只有一个null元素的Enumerator
   **/
  private static class SingletonNullEnumerator<E> implements Enumerator<E> {
    int i = 0;

    public E current() {
      return null;
    }

    public boolean moveNext() {
      return i++ == 0;
    }

    public void reset() {
      i = 0;
    }

    public void close() {
    }
  }

  /** Iterator that reads from an underlying {@link Enumerator}.
   * 将Enumerator转换成Iterator处理
   **/
  private static class EnumeratorIterator<T> implements Iterator<T>, Closeable {
    private final Enumerator<T> enumerator;
    boolean hasNext;

    public EnumeratorIterator(Enumerator<T> enumerator) {
      this.enumerator = enumerator;
      hasNext = enumerator.moveNext();
    }

    public boolean hasNext() {
      return hasNext;
    }

    public T next() {
      T t = enumerator.current();
      hasNext = enumerator.moveNext();
      return t;
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }

    public void close() throws IOException {
      enumerator.close();
    }
  }

  /** Enumerator optimized for random-access list.
   * 迭代器持有list,reset方法移动index指针为-1即可完成
   **/
  private static class ListEnumerator<V> implements Enumerator<V> {
    private final List<? extends V> list;
    int i = -1;

    public ListEnumerator(List<? extends V> list) {
      this.list = list;
    }

    public V current() {
      return list.get(i);
    }

    public boolean moveNext() {
      return ++i < list.size();
    }

    public void reset() {
      i = -1;
    }

    public void close() {
    }
  }
}

// End Linq4j.java
