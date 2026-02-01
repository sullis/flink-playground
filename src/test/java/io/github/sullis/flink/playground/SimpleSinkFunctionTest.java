package io.github.sullis.flink.playground;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;


public class SimpleSinkFunctionTest {

  private SimpleSinkFunction sinkFunction;

  @BeforeEach
  public void setUp() {
    sinkFunction = new SimpleSinkFunction();
  }

  @Test
  void testInitialCountsAreZero() {
    assertThat(sinkFunction.invocationCount.get()).isEqualTo(0);
    assertThat(sinkFunction.watermarkWriteCount.get()).isEqualTo(0);
    assertThat(sinkFunction.finishCount.get()).isEqualTo(0);
  }

  @Test
  void testInvokeIncrementsCount() throws Exception {
    Row row = Row.of(1, 2L, "test");
    SinkFunction.Context context = mock(SinkFunction.Context.class);
    
    sinkFunction.invoke(row, context);
    
    assertThat(sinkFunction.invocationCount.get()).isEqualTo(1);
    assertThat(sinkFunction.watermarkWriteCount.get()).isEqualTo(0);
    assertThat(sinkFunction.finishCount.get()).isEqualTo(0);
  }

  @Test
  void testMultipleInvocations() throws Exception {
    Row row1 = Row.of(1, 2L, "test1");
    Row row2 = Row.of(2, 3L, "test2");
    Row row3 = Row.of(3, 4L, "test3");
    SinkFunction.Context context = mock(SinkFunction.Context.class);
    
    sinkFunction.invoke(row1, context);
    sinkFunction.invoke(row2, context);
    sinkFunction.invoke(row3, context);
    
    assertThat(sinkFunction.invocationCount.get()).isEqualTo(3);
  }

  @Test
  void testWriteWatermarkIncrementsCount() throws Exception {
    Watermark watermark = new Watermark(1000L);
    
    sinkFunction.writeWatermark(watermark);
    
    assertThat(sinkFunction.invocationCount.get()).isEqualTo(0);
    assertThat(sinkFunction.watermarkWriteCount.get()).isEqualTo(1);
    assertThat(sinkFunction.finishCount.get()).isEqualTo(0);
  }

  @Test
  void testMultipleWatermarks() throws Exception {
    Watermark watermark1 = new Watermark(1000L);
    Watermark watermark2 = new Watermark(2000L);
    Watermark watermark3 = new Watermark(3000L);
    
    sinkFunction.writeWatermark(watermark1);
    sinkFunction.writeWatermark(watermark2);
    sinkFunction.writeWatermark(watermark3);
    
    assertThat(sinkFunction.watermarkWriteCount.get()).isEqualTo(3);
  }

  @Test
  void testFinishIncrementsCount() throws Exception {
    sinkFunction.finish();
    
    assertThat(sinkFunction.invocationCount.get()).isEqualTo(0);
    assertThat(sinkFunction.watermarkWriteCount.get()).isEqualTo(0);
    assertThat(sinkFunction.finishCount.get()).isEqualTo(1);
  }

  @Test
  void testMultipleFinishCalls() throws Exception {
    sinkFunction.finish();
    sinkFunction.finish();
    
    assertThat(sinkFunction.finishCount.get()).isEqualTo(2);
  }

  @Test
  void testCompleteSinkLifecycle() throws Exception {
    Row row1 = Row.of(1, 2L, "test1");
    Row row2 = Row.of(2, 3L, "test2");
    SinkFunction.Context context = mock(SinkFunction.Context.class);
    Watermark watermark = new Watermark(1000L);
    
    // Invoke with rows
    sinkFunction.invoke(row1, context);
    sinkFunction.invoke(row2, context);
    
    // Write watermark
    sinkFunction.writeWatermark(watermark);
    
    // Finish
    sinkFunction.finish();
    
    assertThat(sinkFunction.invocationCount.get()).isEqualTo(2);
    assertThat(sinkFunction.watermarkWriteCount.get()).isEqualTo(1);
    assertThat(sinkFunction.finishCount.get()).isEqualTo(1);
  }

  @Test
  void testInvokeWithNullRow() throws Exception {
    SinkFunction.Context context = mock(SinkFunction.Context.class);
    
    sinkFunction.invoke(null, context);
    
    assertThat(sinkFunction.invocationCount.get()).isEqualTo(1);
  }

  @Test
  void testInvokeWithNullContext() throws Exception {
    Row row = Row.of(1, 2L, "test");
    
    sinkFunction.invoke(row, null);
    
    assertThat(sinkFunction.invocationCount.get()).isEqualTo(1);
  }
}
