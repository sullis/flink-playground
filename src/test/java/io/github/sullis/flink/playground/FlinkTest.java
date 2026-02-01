package io.github.sullis.flink.playground;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.assertj.core.api.Assertions.assertThat;


public class FlinkTest {

  private Configuration config = FlinkTestConfig.resolveConfiguration();

  @BeforeEach
  public void beforeEach() {
    config = FlinkTestConfig.resolveConfiguration();
  }

  @Test
  void testStreamExecutionEnvironment() throws Exception {
    assertThat(config.keySet()).contains("metrics.reporters");
    SimpleJobListener jobListener = new SimpleJobListener();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
    assertThat(env).isInstanceOf(LocalStreamEnvironment.class);
    env.setParallelism(1);
    env.registerJobListener(jobListener);
    env.enableCheckpointing(1000);

    RowTypeInfo typeInfo = new RowTypeInfo(new TypeInformation[]{Types.INT, Types.LONG, Types.STRING}, new String[]{"a", "b", "c"});
    List<Row> data = new ArrayList<>();
    data.add(makeRow());

    DataStream<Row> sourceDataStream = env.fromCollection(data).returns(typeInfo);

    SimpleSinkFunction sinkFunction = new SimpleSinkFunction();

    DataStreamSink<Row> dataStreamSink = sourceDataStream.addSink(sinkFunction);

    assertThat(dataStreamSink).isNotNull();

    env.execute();
    env.close();

    assertThat(jobListener.jobSubmittedCount.get()).isEqualTo(1);
    assertThat(jobListener.jobExecutedCount.get()).isEqualTo(1);

    // System.out.println("sinkFunction: " + sinkFunction);
    // assertThat(sinkFunction.invocationCount.intValue()).isEqualTo(1);
  }

  private Row makeRow() {
    return Row.of(1, 1L, "Hello");
  }
}
