package com.ververica;

import static com.ververica.LateralTableJoin.getEnvironment;

import com.ververica.tables.DimensionTable;
import com.ververica.tables.DimensionTable.Dimension;
import com.ververica.tables.FactTable;
import com.ververica.tables.FactTable.Fact;
import java.io.IOException;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LateralStreamJoin2 {

  private static final Logger LOG = LoggerFactory.getLogger(LateralStreamJoin2.class);

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
            .connect(dimTable1Source.map(new MapDimensionToBinary()).keyBy(x -> x.id))
            .process(
                new AbstractFactDimTableJoin<Fact, Tuple2<Fact, byte[]>>() {
                  private static final long serialVersionUID = 1L;

                  @Override
                  Tuple2<Fact, byte[]> join(Fact value, BinaryDimension dim) {
                    return Tuple2.of(value, dim.data);
                  }
                })
            .name("join1")
            .uid("join1")
            .keyBy(x -> x.f0.dim2)
            .connect(dimTable2Source.map(new MapDimensionToBinary()).keyBy(x -> x.id))
            .process(
                new AbstractFactDimTableJoin<Tuple2<Fact, byte[]>, Tuple3<Fact, byte[], byte[]>>() {
                  private static final long serialVersionUID = 1L;

                  @Override
                  Tuple3<Fact, byte[], byte[]> join(
                      Tuple2<Fact, byte[]> value, BinaryDimension dim) {
                    return Tuple3.of(value.f0, value.f1, dim.data);
                  }
                })
            .name("join2")
            .uid("join2")
            .keyBy(x -> x.f0.dim3)
            .connect(dimTable3Source.map(new MapDimensionToBinary()).keyBy(x -> x.id))
            .process(
                new AbstractFactDimTableJoin<
                    Tuple3<Fact, byte[], byte[]>, Tuple4<Fact, byte[], byte[], byte[]>>() {
                  private static final long serialVersionUID = 1L;

                  @Override
                  Tuple4<Fact, byte[], byte[], byte[]> join(
                      Tuple3<Fact, byte[], byte[]> value, BinaryDimension dim) {
                    return Tuple4.of(value.f0, value.f1, value.f2, dim.data);
                  }
                })
            .name("join3")
            .uid("join3")
            .keyBy(x -> x.f0.dim4)
            .connect(dimTable4Source.map(new MapDimensionToBinary()).keyBy(x -> x.id))
            .process(
                new AbstractFactDimTableJoin<
                    Tuple4<Fact, byte[], byte[], byte[]>,
                    Tuple5<Fact, byte[], byte[], byte[], byte[]>>() {
                  private static final long serialVersionUID = 1L;

                  @Override
                  Tuple5<Fact, byte[], byte[], byte[], byte[]> join(
                      Tuple4<Fact, byte[], byte[], byte[]> value, BinaryDimension dim) {
                    return Tuple5.of(value.f0, value.f1, value.f2, value.f3, dim.data);
                  }
                })
            .name("join4")
            .uid("join4")
            .keyBy(x -> x.f0.dim5)
            .connect(dimTable5Source.map(new MapDimensionToBinary()).keyBy(x -> x.id))
            .process(
                new AbstractFactDimTableJoin<
                    Tuple5<Fact, byte[], byte[], byte[], byte[]>, DenormalizedFact>() {
                  private static final long serialVersionUID = 1L;
                  private TypeSerializer<Dimension> dimSerializer;

                  @Override
                  DenormalizedFact join(
                      Tuple5<Fact, byte[], byte[], byte[], byte[]> value, BinaryDimension dim)
                      throws IOException {
                    DataInputDeserializer deserializerBuffer = new DataInputDeserializer();
                    deserializerBuffer.setBuffer(value.f1);
                    Dimension dim1 = dimSerializer.deserialize(deserializerBuffer);
                    deserializerBuffer.setBuffer(value.f2);
                    Dimension dim2 = dimSerializer.deserialize(deserializerBuffer);
                    deserializerBuffer.setBuffer(value.f3);
                    Dimension dim3 = dimSerializer.deserialize(deserializerBuffer);
                    deserializerBuffer.setBuffer(value.f4);
                    Dimension dim4 = dimSerializer.deserialize(deserializerBuffer);
                    deserializerBuffer.setBuffer(dim.data);
                    Dimension dim5 = dimSerializer.deserialize(deserializerBuffer);
                    return new DenormalizedFact(value.f0, dim1, dim2, dim3, dim4, dim5);
                  }

                  @Override
                  public void open(Configuration parameters) throws Exception {
                    super.open(parameters);
                    dimSerializer =
                        TypeInformation.of(Dimension.class)
                            .createSerializer(getRuntimeContext().getExecutionConfig());
                  }
                })
            .name("join5")
            .uid("join5");

    joinedStream.addSink(new DiscardingSink<>());
    env.execute();
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

    public DenormalizedFact(
        Fact fact, Dimension dim1, Dimension dim2, Dimension dim3, Dimension dim4, Dimension dim5) {
      A = dim1.col1;
      B = dim1.col2;
      C = dim1.col3;
      D = dim1.col4;
      E = dim1.col5;
      F = dim2.col1;
      G = dim2.col2;
      H = dim2.col3;
      I = dim2.col4;
      J = dim2.col5;
      K = dim3.col1;
      L = dim3.col2;
      M = dim3.col3;
      N = dim3.col4;
      O = dim3.col5;
      P = dim4.col1;
      Q = dim4.col2;
      R = dim4.col3;
      S = dim4.col4;
      T = dim4.col5;
      U = dim5.col1;
      V = dim5.col2;
      W = dim5.col3;
      X = dim5.col4;
      Y = dim5.col5;
    }
  }

  public static class BinaryDimension {
    public long id;
    public byte[] data;

    @SuppressWarnings("unused")
    public BinaryDimension() {}

    public BinaryDimension(long id, byte[] data) {
      this.id = id;
      this.data = data;
    }

    public static BinaryDimension fromDimension(
        Dimension dim,
        TypeSerializer<Dimension> dimSerializer,
        DataOutputSerializer serializationBuffer)
        throws IOException {
      serializationBuffer.clear();
      dimSerializer.serialize(dim, serializationBuffer);
      BinaryDimension binaryDimension =
          new BinaryDimension(dim.id, serializationBuffer.getCopyOfBuffer());
      serializationBuffer.clear();
      return binaryDimension;
    }
  }

  private abstract static class AbstractFactDimTableJoin<IN1, OUT>
      extends CoProcessFunction<IN1, BinaryDimension, OUT> {
    private static final long serialVersionUID = 1L;

    protected transient ValueState<BinaryDimension> dimState;

    @Override
    public void processElement1(IN1 value, Context ctx, Collector<OUT> out) throws Exception {
      BinaryDimension dim = dimState.value();
      if (dim == null) {
        return;
      }
      out.collect(join(value, dim));
    }

    abstract OUT join(IN1 value, BinaryDimension dim) throws IOException;

    @Override
    public void processElement2(BinaryDimension value, Context ctx, Collector<OUT> out)
        throws Exception {
      dimState.update(value);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      ValueStateDescriptor<BinaryDimension> dimStateDesc =
          new ValueStateDescriptor<>("dimstate", BinaryDimension.class);
      this.dimState = getRuntimeContext().getState(dimStateDesc);
    }
  }

  private static class MapDimensionToBinary extends RichMapFunction<Dimension, BinaryDimension> {
    private static final long serialVersionUID = 1L;

    private transient TypeSerializer<Dimension> dimSerializer;
    private transient DataOutputSerializer serializationBuffer;

    @Override
    public BinaryDimension map(Dimension value) throws Exception {
      return BinaryDimension.fromDimension(value, dimSerializer, serializationBuffer);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      serializationBuffer = new DataOutputSerializer(100);
      dimSerializer =
          TypeInformation.of(Dimension.class)
              .createSerializer(getRuntimeContext().getExecutionConfig());
    }
  }
}
