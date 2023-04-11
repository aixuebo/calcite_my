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

import org.apache.calcite.linq4j.function.Function2;

import java.util.Map;

/**
 * Represents a collection of keys each mapped to one or more values.
 * 是一个Map,只是value是一个list
 * @param <K> Key type
 * @param <V> Value type
 *

LookupImpl  转换成TreeMap<K,List<V>> --- 循环元素,将相同的元素放在List里,转换成map<key,List<value>>形式
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
public interface Lookup<K, V>
    extends Map<K, Enumerable<V>>, Enumerable<Grouping<K, V>> {
  /**
   * Applies a transform function to each key and its associated values and
   * returns the results.
   *
   * @param resultSelector Result selector,参数是key和list<V>,转换成另外一个结果
   * @param <TResult> Result type
   *
   * @return Enumerable over results
   * 拿到key和key对应的list，一起创建一个新的对象
   */
  <TResult> Enumerable<TResult> applyResultSelector(
      Function2<K, Enumerable<V>, TResult> resultSelector);
}

// End Lookup.java
