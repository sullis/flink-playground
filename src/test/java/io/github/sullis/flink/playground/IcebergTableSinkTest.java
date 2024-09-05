package io.github.sullis.flink.playground;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.types.Row;
import org.apache.iceberg.flink.FlinkDynamicTableFactory;
import org.junit.jupiter.api.Test;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.flink.IcebergTableSink;

public class IcebergTableSinkTest {

  @Test
  void testStreamExecutionEnvironment() throws Exception {
    SimpleJobListener jobListener = new SimpleJobListener();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    assertThat(env).isInstanceOf(LocalStreamEnvironment.class);
    env.setParallelism(1);
    env.registerJobListener(jobListener);
    env.enableCheckpointing(1000);

    RowTypeInfo typeInfo = new RowTypeInfo(new TypeInformation[]{Types.INT, Types.LONG, Types.STRING}, new String[]{"a", "b", "c"});
    List<Row> data = new ArrayList<>();
    data.add(makeRow());

    DataStream<Row> sourceDataStream = env.fromCollection(data).returns(typeInfo);

    FlinkDynamicTableFactory tableFactory = new FlinkDynamicTableFactory();
    assertThat(tableFactory.factoryIdentifier()).isEqualTo("iceberg");

    DataStreamSink<Row> dataStreamSink = null; // sourceDataStream.addSink(sink);

    assertThat(dataStreamSink).isNotNull();

    env.execute();
    env.close();

    assertThat(jobListener.jobSubmittedCount).isEqualTo(1);
    assertThat(jobListener.jobExecutedCount).isEqualTo(1);

    // System.out.println("sinkFunction: " + sinkFunction);
    // assertThat(sinkFunction.invocationCount.intValue()).isEqualTo(1);
  }

  private Row makeRow() {
    return Row.of(1, 1L, "Hello");
  }
}
