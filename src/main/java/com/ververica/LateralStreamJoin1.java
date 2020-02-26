package com.ververica;

import static com.ververica.LateralTableJoin.getEnvironment;

import com.ververica.tables.DimensionTable;
import com.ververica.tables.DimensionTable.Dimension;
import com.ververica.tables.FactTable;
import com.ververica.tables.FactTable.Fact;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LateralStreamJoin1 {

  private static final Logger LOG = LoggerFactory.getLogger(LateralStreamJoin1.class);

  public static void main(String[] args) throws Exception {
    ParameterTool tool = ParameterTool.fromArgs(args);
    int factTableRate = tool.getInt("factRate", 500_000);
    int dimTableRate = tool.getInt("dimRate", 1_000);

    // disable optimisations:
    boolean noObjectReuse = tool.has("noObjectReuse");

    StreamExecutionEnvironment env = getEnvironment(tool);

    env.enableCheckpointing(60 * 1000 * 3);
    if (!noObjectReuse) {
      env.getConfig().enableObjectReuse();
    }

    int maxId = 100_000;
    DataStream<Fact> factTableSource =
        env.addSource(new FactTable(factTableRate, maxId)).name("fact_table");
    DataStream<Dimension> dimTable1Source =
        env.addSource(new DimensionTable(dimTableRate, maxId)).name("dim_table1");
    DataStream<Dimension> dimTable2Source =
        env.addSource(new DimensionTable(dimTableRate, maxId)).name("dim_table2");
    DataStream<Dimension> dimTable3Source =
        env.addSource(new DimensionTable(dimTableRate, maxId)).name("dim_table3");
    DataStream<Dimension> dimTable4Source =
        env.addSource(new DimensionTable(dimTableRate, maxId)).name("dim_table4");
    DataStream<Dimension> dimTable5Source =
        env.addSource(new DimensionTable(dimTableRate, maxId)).name("dim_table5");

    DataStream<DenormalizedFact> joinedStream =
        factTableSource
            .keyBy(x -> x.dim1)
            .connect(dimTable1Source.keyBy(x -> x.id))
            .process(
                new AbstractFactDimTableJoin<Fact, Fact1>() {
                  private static final long serialVersionUID = 1L;

                  @Override
                  Fact1 join(Fact value, Dimension dim) {
                    return new Fact1(value, dim);
                  }
                })
            .name("join1")
            .uid("join1")
            .keyBy(x -> x.dim2)
            .connect(dimTable2Source.keyBy(x -> x.id))
            .process(
                new AbstractFactDimTableJoin<Fact1, Fact2>() {
                  private static final long serialVersionUID = 1L;

                  @Override
                  Fact2 join(Fact1 value, Dimension dim) {
                    return new Fact2(value, dim);
                  }
                })
            .name("join2")
            .uid("join2")
            .keyBy(x -> x.dim3)
            .connect(dimTable3Source.keyBy(x -> x.id))
            .process(
                new AbstractFactDimTableJoin<Fact2, Fact3>() {
                  private static final long serialVersionUID = 1L;

                  @Override
                  Fact3 join(Fact2 value, Dimension dim) {
                    return new Fact3(value, dim);
                  }
                })
            .name("join3")
            .uid("join3")
            .keyBy(x -> x.dim4)
            .connect(dimTable4Source.keyBy(x -> x.id))
            .process(
                new AbstractFactDimTableJoin<Fact3, Fact4>() {
                  private static final long serialVersionUID = 1L;

                  @Override
                  Fact4 join(Fact3 value, Dimension dim) {
                    return new Fact4(value, dim);
                  }
                })
            .name("join4")
            .uid("join4")
            .keyBy(x -> x.dim5)
            .connect(dimTable5Source.keyBy(x -> x.id))
            .process(
                new AbstractFactDimTableJoin<Fact4, DenormalizedFact>() {
                  private static final long serialVersionUID = 1L;

                  @Override
                  DenormalizedFact join(Fact4 value, Dimension dim) {
                    return new DenormalizedFact(value, dim);
                  }
                })
            .name("join5")
            .uid("join5");

    joinedStream.addSink(new DiscardingSink<>());
    env.execute();
  }

  public static class Fact1 {
    public String A;
    public String B;
    public String C;
    public String D;
    public String E;
    public long dim2;
    public long dim3;
    public long dim4;
    public long dim5;

    @SuppressWarnings("unused")
    public Fact1() {}

    public Fact1(Fact fact, Dimension dim) {
      A = dim.col1;
      B = dim.col2;
      C = dim.col3;
      D = dim.col4;
      E = dim.col5;
      dim2 = fact.dim2;
      dim3 = fact.dim3;
      dim4 = fact.dim4;
      dim5 = fact.dim5;
    }
  }

  public static class Fact2 {
    public String A;
    public String B;
    public String C;
    public String D;
    public String E;
    public String F;
    public String G;
    public String H;
    public String I;
    public String J;
    public long dim3;
    public long dim4;
    public long dim5;

    @SuppressWarnings("unused")
    public Fact2() {}

    public Fact2(Fact1 fact, Dimension dim) {
      A = fact.A;
      B = fact.B;
      C = fact.C;
      D = fact.D;
      E = fact.E;
      F = dim.col1;
      G = dim.col2;
      H = dim.col3;
      I = dim.col4;
      J = dim.col5;
      dim3 = fact.dim3;
      dim4 = fact.dim4;
      dim5 = fact.dim5;
    }
  }

  public static class Fact3 {
    public String A;
    public String B;
    public String C;
    public String D;
    public String E;
    public String F;
    public String G;
    public String H;
    public String I;
    public String J;
    public String K;
    public String L;
    public String M;
    public String N;
    public String O;
    public long dim4;
    public long dim5;

    @SuppressWarnings("unused")
    public Fact3() {}

    public Fact3(Fact2 fact, Dimension dim) {
      A = fact.A;
      B = fact.B;
      C = fact.C;
      D = fact.D;
      E = fact.E;
      F = fact.F;
      G = fact.G;
      H = fact.H;
      I = fact.I;
      J = fact.J;
      K = dim.col1;
      L = dim.col2;
      M = dim.col3;
      N = dim.col4;
      O = dim.col5;
      dim4 = fact.dim4;
      dim5 = fact.dim5;
    }
  }

  public static class Fact4 {
    public String A;
    public String B;
    public String C;
    public String D;
    public String E;
    public String F;
    public String G;
    public String H;
    public String I;
    public String J;
    public String K;
    public String L;
    public String M;
    public String N;
    public String O;
    public String P;
    public String Q;
    public String R;
    public String S;
    public String T;
    public long dim5;

    @SuppressWarnings("unused")
    public Fact4() {}

    public Fact4(Fact3 fact, Dimension dim) {
      A = fact.A;
      B = fact.B;
      C = fact.C;
      D = fact.D;
      E = fact.E;
      F = fact.F;
      G = fact.G;
      H = fact.H;
      I = fact.I;
      J = fact.J;
      K = fact.K;
      L = fact.L;
      M = fact.M;
      N = fact.N;
      O = fact.O;
      P = dim.col1;
      Q = dim.col2;
      R = dim.col3;
      S = dim.col4;
      T = dim.col5;
      dim5 = fact.dim5;
    }
  }

  public static class DenormalizedFact {
    public String A;
    public String B;
    public String C;
    public String D;
    public String E;
    public String F;
    public String G;
    public String H;
    public String I;
    public String J;
    public String K;
    public String L;
    public String M;
    public String N;
    public String O;
    public String P;
    public String Q;
    public String R;
    public String S;
    public String T;
    public String U;
    public String V;
    public String W;
    public String X;
    public String Y;

    @SuppressWarnings("unused")
    public DenormalizedFact() {}

    public DenormalizedFact(Fact4 fact, Dimension dim) {
      A = fact.A;
      B = fact.B;
      C = fact.C;
      D = fact.D;
      E = fact.E;
      F = fact.F;
      G = fact.G;
      H = fact.H;
      I = fact.I;
      J = fact.J;
      K = fact.K;
      L = fact.L;
      M = fact.M;
      N = fact.N;
      O = fact.O;
      P = fact.P;
      Q = fact.Q;
      R = fact.R;
      S = fact.S;
      T = fact.T;
      U = dim.col1;
      V = dim.col2;
      W = dim.col3;
      X = dim.col4;
      Y = dim.col5;
    }
  }

  private abstract static class AbstractFactDimTableJoin<IN1, OUT>
      extends CoProcessFunction<IN1, Dimension, OUT> {
    private static final long serialVersionUID = 1L;

    protected transient ValueState<Dimension> dimState;

    @Override
    public void processElement1(IN1 value, Context ctx, Collector<OUT> out) throws Exception {
      Dimension dim = dimState.value();
      if (dim == null) {
        return;
      }
      out.collect(join(value, dim));
    }

    abstract OUT join(IN1 value, Dimension dim);

    @Override
    public void processElement2(Dimension value, Context ctx, Collector<OUT> out) throws Exception {
      dimState.update(value);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      ValueStateDescriptor<Dimension> dimStateDesc =
          new ValueStateDescriptor<>("dimstate", Dimension.class);
      this.dimState = getRuntimeContext().getState(dimStateDesc);
    }
  }
}
