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
package org.apache.calcite.plan;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * RelTraitDef represents a class of {@link RelTrait}s. Implementations of
 * RelTraitDef may be singletons under the following conditions:
 *
 * <ol>
 * <li>if the set of all possible associated RelTraits is finite and fixed (e.g.
 * all RelTraits for this RelTraitDef are known at compile time). For example,
 * the CallingConvention trait meets this requirement, because CallingConvention
 * is effectively an enumeration.</li>
 * <li>Either
 *
 * <ul>
 * <li> {@link #canConvert(RelOptPlanner, RelTrait, RelTrait)} and
 * {@link #convert(RelOptPlanner, RelNode, RelTrait, boolean)} do not require
 * planner-instance-specific information, <b>or</b></li>
 *
 * <li>the RelTraitDef manages separate sets of conversion data internally. See
 * {@link ConventionTraitDef} for an example of this.</li>
 * </ul>
 * </li>
 * </ol>
 *
 * <p>Otherwise, a new instance of RelTraitDef must be constructed and
 * registered with each new planner instantiated.</p>
 *
 * @param <T> Trait that this trait definition is based upon
 *
 * 定义特性
 */
public abstract class RelTraitDef<T extends RelTrait> {
  //~ Instance fields --------------------------------------------------------

  //内存存储对应的T对象
  private final LoadingCache<T, T> canonicalMap =
      CacheBuilder.newBuilder()
          .softValues()
          .build(
              new CacheLoader<T, T>() {
                @Override public T load(T key) throws Exception {
                  return key;
                }
              });

  //~ Constructors -----------------------------------------------------------

  protected RelTraitDef() {
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Whether a relational expression may possess more than one instance of
   * this trait simultaneously.
   *
   * <p>A subset has only one instance of a trait.</p>
   */
  public boolean multiple() {
    return false;
  }

  /**
   * @return the specific RelTrait type associated with this RelTraitDef.
   * 返回具体的特性是什么对象
   */
  public abstract Class<T> getTraitClass();

  /**
   * @return a simple name for this RelTraitDef (for use in
   * {@link org.apache.calcite.rel.RelNode#explain}).
   * 为特性起一个简称名字,比如sort简称排序特性
   */
  public abstract String getSimpleName();

  /**
   * Returns the default member of this trait.
   * 返回特性的实现类
   */
  public abstract T getDefault();

  /**
   * Takes an arbitrary RelTrait and returns the canonical representation of
   * that RelTrait. Canonized RelTrait objects may always be compared using
   * the equality operator (<code>==</code>).
   *
   * <p>If an equal RelTrait has already been canonized and is still in use,
   * it will be returned. Otherwise, the given RelTrait is made canonical and
   * returned.
   *
   * @param trait a possibly non-canonical RelTrait
   * @return a canonical RelTrait.
   *
   * 格式化--标准化
   */
  public final T canonize(T trait) {
    assert getTraitClass().isInstance(trait) //校验trait一定是要求的子类
        : getClass().getName()
        + " cannot canonize a "
        + trait.getClass().getName();

    return canonicalMap.getUnchecked(trait);
  }

  /**
   * Converts the given RelNode to the given RelTrait.
   *
   * @param planner                     the planner requesting the conversion
   * @param rel                         RelNode to convert
   * @param toTrait                     RelTrait to convert to
   * @param allowInfiniteCostConverters flag indicating whether infinite cost
   *                                    converters are allowed
   * @return a converted RelNode or null if conversion is not possible
   */
  public abstract RelNode convert(
      RelOptPlanner planner,
      RelNode rel,
      T toTrait,
      boolean allowInfiniteCostConverters);

  /**
   * Tests whether the given RelTrait can be converted to another RelTrait.
   *
   * @param planner   the planner requesting the conversion test
   * @param fromTrait the RelTrait to convert from
   * @param toTrait   the RelTrait to convert to
   * @return true if fromTrait can be converted to toTrait
   */
  public abstract boolean canConvert(
      RelOptPlanner planner,
      T fromTrait,
      T toTrait);

  /**
   * Provides notification of the registration of a particular
   * {@link ConverterRule} with a {@link RelOptPlanner}. The default
   * implementation does nothing.
   *
   * @param planner       the planner registering the rule
   * @param converterRule the registered converter rule
   */
  public void registerConverterRule(
      RelOptPlanner planner,
      ConverterRule converterRule) {
  }

  /**
   * Provides notification that a particular {@link ConverterRule} has been
   * de-registered from a {@link RelOptPlanner}. The default implementation
   * does nothing.
   *
   * @param planner       the planner registering the rule
   * @param converterRule the registered converter rule
   */
  public void deregisterConverterRule(
      RelOptPlanner planner,
      ConverterRule converterRule) {
  }


}

// End RelTraitDef.java
