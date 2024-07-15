package io.github.sullis.flink.playground;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class FlinkTest {

  @Test
  void testStreamExecutionEnvironment() throws Exception {
    SimpleJobListener jobListener = new SimpleJobListener();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.registerJobListener(jobListener);
    env.enableCheckpointing(1000);

    RowTypeInfo typeInfo = new RowTypeInfo(new TypeInformation[]{Types.INT, Types.LONG, Types.STRING}, new String[]{"a", "b", "c"});
    List<Row> data = new ArrayList<>();
    data.add(makeRow());

    DataStream<Row> sourceDataStream = env.fromCollection(data).returns(typeInfo);

    SinkFunction<Row> sinkFunction = new SimpleSinkFunction();

    sourceDataStream.addSink(sinkFunction);

    env.execute();
    env.close();
  }

  private Row makeRow() {
    return Row.of(1, 1L, "Hello");
  }
}
