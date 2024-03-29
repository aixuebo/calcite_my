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
package org.apache.calcite.interpreter;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.AggAddContext;
import org.apache.calcite.adapter.enumerable.AggImpState;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.adapter.enumerable.impl.AggAddContextImpl;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.interpreter.Row.RowBuilder;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.impl.AggregateFunctionImpl;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Interpreter node that implements an
 * {@link org.apache.calcite.rel.core.Aggregate}.
 */
public class AggregateNode extends AbstractSingleNode<Aggregate> {
  private final List<Grouping> groups = Lists.newArrayList();
  private final ImmutableBitSet unionGroups;
  private final int outputRowLength;
  private final ImmutableList<AccumulatorFactory> accumulatorFactories;
  private final DataContext dataContext;

  public AggregateNode(Interpreter interpreter, Aggregate rel) {
    super(interpreter, rel);
    this.dataContext = interpreter.getDataContext();

    ImmutableBitSet union = ImmutableBitSet.of();

    if (rel.getGroupSets() != null) {
      for (ImmutableBitSet group : rel.getGroupSets()) {
        union = union.union(group);
        groups.add(new Grouping(group));
      }
    }

    this.unionGroups = union;
    this.outputRowLength = unionGroups.cardinality()
        + (rel.indicator ? unionGroups.cardinality() : 0)
        + rel.getAggCallList().size();

    ImmutableList.Builder<AccumulatorFactory> builder = ImmutableList.builder();
    for (AggregateCall aggregateCall : rel.getAggCallList()) {
      builder.add(getAccumulator(aggregateCall));
    }
    accumulatorFactories = builder.build();
  }

  public void run() throws InterruptedException {
    Row r;
    while ((r = source.receive()) != null) {
      for (Grouping group : groups) {
        group.send(r);
      }
    }

    for (Grouping group : groups) {
      group.end(sink);
    }
  }

  private AccumulatorFactory getAccumulator(final AggregateCall call) {
    if (call.getAggregation() == SqlStdOperatorTable.COUNT) {
      return new AccumulatorFactory() {
        public Accumulator get() {
          return new CountAccumulator(call);
        }
      };
    } else if (call.getAggregation() == SqlStdOperatorTable.SUM) {
      return new UdaAccumulatorFactory(
          AggregateFunctionImpl.create(IntSum.class), call);
    } else {
      final JavaTypeFactory typeFactory =
          (JavaTypeFactory) rel.getCluster().getTypeFactory();
      int stateOffset = 0;
      final AggImpState agg = new AggImpState(0, call, false);
      int stateSize = agg.state.size();

      final BlockBuilder builder2 = new BlockBuilder();
      final PhysType inputPhysType =
          PhysTypeImpl.of(typeFactory, rel.getInput().getRowType(),
              JavaRowFormat.ARRAY);
      final RelDataTypeFactory.FieldInfoBuilder builder = typeFactory.builder();
      for (Expression expression : agg.state) {
        builder.add("a",
            typeFactory.createJavaType((Class) expression.getType()));
      }
      final PhysType accPhysType =
          PhysTypeImpl.of(typeFactory, builder.build(), JavaRowFormat.ARRAY);
      final ParameterExpression inParameter =
          Expressions.parameter(inputPhysType.getJavaRowType(), "in");
      final ParameterExpression acc_ =
          Expressions.parameter(accPhysType.getJavaRowType(), "acc");

      List<Expression> accumulator =
          new ArrayList<Expression>(stateSize);
      for (int j = 0; j < stateSize; j++) {
        accumulator.add(accPhysType.fieldReference(acc_, j + stateOffset));
      }
      agg.state = accumulator;

      AggAddContext addContext =
          new AggAddContextImpl(builder2, accumulator) {
            public List<RexNode> rexArguments() {
              List<RelDataTypeField> inputTypes =
                  inputPhysType.getRowType().getFieldList();
              List<RexNode> args = new ArrayList<RexNode>();
              for (Integer index : agg.call.getArgList()) {
                args.add(
                    new RexInputRef(index, inputTypes.get(index).getType()));
              }
              return args;
            }

            public RexToLixTranslator rowTranslator() {
              return RexToLixTranslator.forAggregation(typeFactory,
                  currentBlock(),
                  new RexToLixTranslator.InputGetterImpl(
                      Collections.singletonList(
                          Pair.of((Expression) inParameter, inputPhysType))))
                  .setNullable(currentNullables());
            }
          };

      agg.implementor.implementAdd(agg.context, addContext);

      final ParameterExpression context_ =
          Expressions.parameter(Context.class, "context");
      final ParameterExpression outputValues_ =
          Expressions.parameter(Object[].class, "outputValues");
      Scalar addScalar =
          JaninoRexCompiler.baz(context_, outputValues_, builder2.toBlock());
      return new ScalarAccumulatorDef(null, addScalar, null,
          rel.getInput().getRowType().getFieldCount(), stateSize, dataContext);
    }
  }

  /** Accumulator for calls to the COUNT function. */
  private static class CountAccumulator implements Accumulator {
    private final AggregateCall call;
    long cnt;

    public CountAccumulator(AggregateCall call) {
      this.call = call;
      cnt = 0;
    }

    public void send(Row row) {
      boolean notNull = true;
      for (Integer i : call.getArgList()) {
        if (row.getObject(i) == null) {
          notNull = false;
          break;
        }
      }
      if (notNull) {
        cnt++;
      }
    }

    public Object end() {
      return cnt;
    }
  }

  /** Creates an {@link Accumulator}. */
  private interface AccumulatorFactory extends Supplier<Accumulator> {
  }

  /** Accumulator powered by {@link Scalar} code fragments. */
  private static class ScalarAccumulatorDef implements AccumulatorFactory {
    final Scalar initScalar;
    final Scalar addScalar;
    final Scalar endScalar;
    final Context sendContext;
    final Context endContext;
    final int rowLength;
    final int accumulatorLength;

    private ScalarAccumulatorDef(Scalar initScalar, Scalar addScalar,
        Scalar endScalar, int rowLength, int accumulatorLength,
        DataContext root) {
      this.initScalar = initScalar;
      this.addScalar = addScalar;
      this.endScalar = endScalar;
      this.accumulatorLength = accumulatorLength;
      this.rowLength = rowLength;
      this.sendContext = new Context(root);
      this.sendContext.values = new Object[rowLength + accumulatorLength];
      this.endContext = new Context(root);
      this.endContext.values = new Object[accumulatorLength];
    }

    public Accumulator get() {
      return new ScalarAccumulator(this, new Object[accumulatorLength]);
    }
  }

  /** Accumulator powered by {@link Scalar} code fragments. */
  private static class ScalarAccumulator implements Accumulator {
    final ScalarAccumulatorDef def;
    final Object[] values;

    private ScalarAccumulator(ScalarAccumulatorDef def, Object[] values) {
      this.def = def;
      this.values = values;
    }

    public void send(Row row) {
      System.arraycopy(row.getValues(), 0, def.sendContext.values, 0,
          def.rowLength);
      System.arraycopy(values, 0, def.sendContext.values, def.rowLength,
          values.length);
      def.addScalar.execute(def.sendContext, values);
    }

    public Object end() {
      System.arraycopy(values, 0, def.endContext.values, 0, values.length);
      return def.endScalar.execute(def.endContext);
    }
  }

  /**
   * Internal class to track groupings.
   */
  private class Grouping {
    private final ImmutableBitSet grouping;
    private final Map<Row, AccumulatorList> accumulators = Maps.newHashMap();

    private Grouping(ImmutableBitSet grouping) {
      this.grouping = grouping;
    }

    public void send(Row row) {
      // TODO: fix the size of this row.
      RowBuilder builder = Row.newBuilder(grouping.cardinality());
      for (Integer i : grouping) {
        builder.set(i, row.getObject(i));
      }
      Row key = builder.build();

      if (!accumulators.containsKey(key)) {
        AccumulatorList list = new AccumulatorList();
        for (AccumulatorFactory factory : accumulatorFactories) {
          list.add(factory.get());
        }
        accumulators.put(key, list);
      }

      accumulators.get(key).send(row);
    }

    public void end(Sink sink) throws InterruptedException {
      for (Map.Entry<Row, AccumulatorList> e : accumulators.entrySet()) {
        final Row key = e.getKey();
        final AccumulatorList list = e.getValue();

        RowBuilder rb = Row.newBuilder(outputRowLength);
        int index = 0;
        for (Integer groupPos : unionGroups) {
          if (grouping.get(groupPos)) {
            rb.set(index, key.getObject(groupPos));
            if (rel.indicator) {
              rb.set(unionGroups.cardinality() + index, true);
            }
          }
          // need to set false when not part of grouping set.

          index++;
        }

        list.end(rb);

        sink.send(rb.build());
      }
    }
  }

  /**
   * A list of accumulators used during grouping.
   */
  private class AccumulatorList extends ArrayList<Accumulator> {
    public void send(Row row) {
      for (Accumulator a : this) {
        a.send(row);
      }
    }

    public void end(RowBuilder r) {
      for (int accIndex = 0, rowIndex = r.size() - size();
          rowIndex < r.size(); rowIndex++, accIndex++) {
        r.set(rowIndex, get(accIndex).end());
      }
    }
  }

  /**
   * Defines function implementation for
   * things like {@code count()} and {@code sum()}.
   */
  private interface Accumulator {
    void send(Row row);
    Object end();
  }

  /** Implementation of {@code SUM} over INTEGER values as a user-defined
   * aggregate.
   * 集合方法,返回integer
   **/
  public static class IntSum {
    public IntSum() {
    }
    //初始化的值
    public int init() {
      return 0;
    }
    //每次追加v时该如何操作
    public int add(int accumulator, int v) {
      return accumulator + v;
    }
    //如何处理merge操作
    public int merge(int accumulator0, int accumulator1) {
      return accumulator0 + accumulator1;
    }
    //如何返回结果
    public int result(int accumulator) {
      return accumulator;
    }
  }

  /** Implementation of {@code SUM} over BIGINT values as a user-defined
   * aggregate. */
  public static class LongSum {
    public LongSum() {
    }
    public long init() {
      return 0L;
    }
    public long add(long accumulator, int v) {
      return accumulator + v;
    }
    public long merge(long accumulator0, long accumulator1) {
      return accumulator0 + accumulator1;
    }
    public long result(long accumulator) {
      return accumulator;
    }
  }

  /** Accumulator factory based on a user-defined aggregate function. */
  private static class UdaAccumulatorFactory implements AccumulatorFactory {
    public final AggregateFunctionImpl aggFunction;
    public final int argOrdinal;
    public final Object instance;

    public UdaAccumulatorFactory(AggregateFunctionImpl aggFunction,
        AggregateCall call) {
      this.aggFunction = aggFunction;
      if (call.getArgList().size() != 1) {
        throw new UnsupportedOperationException("in current implementation, "
            + "aggregate must have precisely one argument");
      }
      argOrdinal = call.getArgList().get(0);
      if (aggFunction.isStatic) {
        instance = null;
      } else {
        try {
          instance = aggFunction.declaringClass.newInstance();
        } catch (InstantiationException e) {
          throw Throwables.propagate(e);
        } catch (IllegalAccessException e) {
          throw Throwables.propagate(e);
        }
      }
    }

    public Accumulator get() {
      return new UdaAccumulator(this);
    }
  }

  /** Accumulator based upon a user-defined aggregate. */
  private static class UdaAccumulator implements Accumulator {
    private final UdaAccumulatorFactory factory;
    private Object value;

    public UdaAccumulator(UdaAccumulatorFactory factory) {
      this.factory = factory;
      try {
        this.value = factory.aggFunction.initMethod.invoke(factory.instance);
      } catch (IllegalAccessException e) {
        throw Throwables.propagate(e);
      } catch (InvocationTargetException e) {
        throw Throwables.propagate(e);
      }
    }

    public void send(Row row) {
      final Object[] args = {value, row.getValues()[factory.argOrdinal]};
      for (int i = 1; i < args.length; i++) {
        if (args[i] == null) {
          return; // one of the arguments is null; don't add to the total
        }
      }
      try {
        value = factory.aggFunction.addMethod.invoke(factory.instance, args);
      } catch (IllegalAccessException e) {
        throw Throwables.propagate(e);
      } catch (InvocationTargetException e) {
        throw Throwables.propagate(e);
      }
    }

    public Object end() {
      final Object[] args = {value};
      try {
        return factory.aggFunction.resultMethod.invoke(factory.instance, args);
      } catch (IllegalAccessException e) {
        throw Throwables.propagate(e);
      } catch (InvocationTargetException e) {
        throw Throwables.propagate(e);
      }
    }
  }
}

// End AggregateNode.java
