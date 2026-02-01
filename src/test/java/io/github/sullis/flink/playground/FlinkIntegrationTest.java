package io.github.sullis.flink.playground;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;


public class FlinkIntegrationTest {

  private Configuration config;

  @BeforeEach
  public void beforeEach() {
    config = FlinkTestConfig.resolveConfiguration();
  }

  @Test
  void testStreamWithMultipleRows() throws Exception {
    SimpleJobListener jobListener = new SimpleJobListener();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
    assertThat(env).isInstanceOf(LocalStreamEnvironment.class);
    env.setParallelism(1);
    env.registerJobListener(jobListener);
    env.enableCheckpointing(1000);

    RowTypeInfo typeInfo = new RowTypeInfo(
        new TypeInformation[]{Types.INT, Types.LONG, Types.STRING}, 
        new String[]{"id", "timestamp", "message"}
    );
    
    List<Row> data = new ArrayList<>();
    data.add(Row.of(1, 1000L, "First message"));
    data.add(Row.of(2, 2000L, "Second message"));
    data.add(Row.of(3, 3000L, "Third message"));
    data.add(Row.of(4, 4000L, "Fourth message"));
    data.add(Row.of(5, 5000L, "Fifth message"));

    DataStream<Row> sourceDataStream = env.fromCollection(data).returns(typeInfo);
    SimpleSinkFunction sinkFunction = new SimpleSinkFunction();
    DataStreamSink<Row> dataStreamSink = sourceDataStream.addSink(sinkFunction);

    assertThat(dataStreamSink).isNotNull();

    env.execute("Multi-Row Test Job");
    env.close();

    assertThat(jobListener.jobSubmittedCount.get()).isEqualTo(1);
    assertThat(jobListener.jobExecutedCount.get()).isEqualTo(1);
  }

  @Test
  void testStreamWithDifferentParallelism() throws Exception {
    SimpleJobListener jobListener = new SimpleJobListener();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
    env.setParallelism(2);
    env.registerJobListener(jobListener);

    RowTypeInfo typeInfo = new RowTypeInfo(
        new TypeInformation[]{Types.INT, Types.STRING}, 
        new String[]{"id", "value"}
    );
    
    List<Row> data = Arrays.asList(
        Row.of(1, "value1"),
        Row.of(2, "value2")
    );

    DataStream<Row> sourceDataStream = env.fromCollection(data).returns(typeInfo);
    SimpleSinkFunction sinkFunction = new SimpleSinkFunction();
    sourceDataStream.addSink(sinkFunction);

    env.execute("Parallelism Test Job");
    env.close();

    assertThat(jobListener.jobSubmittedCount.get()).isEqualTo(1);
    assertThat(jobListener.jobExecutedCount.get()).isEqualTo(1);
  }

  @Test
  void testStreamWithSingleRow() throws Exception {
    SimpleJobListener jobListener = new SimpleJobListener();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
    env.setParallelism(1);
    env.registerJobListener(jobListener);

    RowTypeInfo typeInfo = new RowTypeInfo(
        new TypeInformation[]{Types.INT, Types.STRING}, 
        new String[]{"id", "value"}
    );
    
    List<Row> data = Arrays.asList(Row.of(1, "single"));

    DataStream<Row> sourceDataStream = env.fromCollection(data).returns(typeInfo);
    SimpleSinkFunction sinkFunction = new SimpleSinkFunction();
    sourceDataStream.addSink(sinkFunction);

    env.execute("Single Row Test Job");
    env.close();

    assertThat(jobListener.jobSubmittedCount.get()).isEqualTo(1);
    assertThat(jobListener.jobExecutedCount.get()).isEqualTo(1);
  }

  @Test
  void testStreamWithComplexRowTypes() throws Exception {
    SimpleJobListener jobListener = new SimpleJobListener();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
    env.setParallelism(1);
    env.registerJobListener(jobListener);

    RowTypeInfo typeInfo = new RowTypeInfo(
        new TypeInformation[]{
            Types.INT, 
            Types.LONG, 
            Types.STRING, 
            Types.DOUBLE, 
            Types.BOOLEAN
        }, 
        new String[]{"id", "timestamp", "message", "score", "active"}
    );
    
    List<Row> data = Arrays.asList(
        Row.of(1, 1000L, "Test", 98.5, true),
        Row.of(2, 2000L, "Another", 75.2, false),
        Row.of(3, 3000L, "Data", 88.9, true)
    );

    DataStream<Row> sourceDataStream = env.fromCollection(data).returns(typeInfo);
    SimpleSinkFunction sinkFunction = new SimpleSinkFunction();
    sourceDataStream.addSink(sinkFunction);

    env.execute("Complex Row Types Test Job");
    env.close();

    assertThat(jobListener.jobSubmittedCount.get()).isEqualTo(1);
    assertThat(jobListener.jobExecutedCount.get()).isEqualTo(1);
  }

  @Test
  void testStreamWithMultipleJobListeners() throws Exception {
    SimpleJobListener listener1 = new SimpleJobListener();
    SimpleJobListener listener2 = new SimpleJobListener();
    SimpleJobListener listener3 = new SimpleJobListener();
    
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
    env.setParallelism(1);
    env.registerJobListener(listener1);
    env.registerJobListener(listener2);
    env.registerJobListener(listener3);

    RowTypeInfo typeInfo = new RowTypeInfo(
        new TypeInformation[]{Types.INT}, 
        new String[]{"value"}
    );
    
    List<Row> data = Arrays.asList(Row.of(42));

    DataStream<Row> sourceDataStream = env.fromCollection(data).returns(typeInfo);
    SimpleSinkFunction sinkFunction = new SimpleSinkFunction();
    sourceDataStream.addSink(sinkFunction);

    env.execute("Multiple Listeners Test Job");
    env.close();

    assertThat(listener1.jobSubmittedCount.get()).isEqualTo(1);
    assertThat(listener1.jobExecutedCount.get()).isEqualTo(1);
    assertThat(listener2.jobSubmittedCount.get()).isEqualTo(1);
    assertThat(listener2.jobExecutedCount.get()).isEqualTo(1);
    assertThat(listener3.jobSubmittedCount.get()).isEqualTo(1);
    assertThat(listener3.jobExecutedCount.get()).isEqualTo(1);
  }

  @Test
  void testStreamWithoutCheckpointing() throws Exception {
    SimpleJobListener jobListener = new SimpleJobListener();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
    env.setParallelism(1);
    env.registerJobListener(jobListener);
    // Explicitly run without checkpointing to verify it is optional

    RowTypeInfo typeInfo = new RowTypeInfo(
        new TypeInformation[]{Types.STRING}, 
        new String[]{"message"}
    );
    
    List<Row> data = Arrays.asList(
        Row.of("Hello"),
        Row.of("World")
    );

    DataStream<Row> sourceDataStream = env.fromCollection(data).returns(typeInfo);
    SimpleSinkFunction sinkFunction = new SimpleSinkFunction();
    sourceDataStream.addSink(sinkFunction);

    env.execute("No Checkpointing Test Job");
    env.close();

    assertThat(jobListener.jobSubmittedCount.get()).isEqualTo(1);
    assertThat(jobListener.jobExecutedCount.get()).isEqualTo(1);
  }
}
