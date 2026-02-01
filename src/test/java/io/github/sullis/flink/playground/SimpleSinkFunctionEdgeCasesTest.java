package io.github.sullis.flink.playground;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;


public class SimpleSinkFunctionEdgeCasesTest {

  private SimpleSinkFunction sinkFunction;

  @BeforeEach
  public void setUp() {
    sinkFunction = new SimpleSinkFunction();
  }

  @Test
  void testInvokeWithEmptyRow() throws Exception {
    Row emptyRow = Row.of();
    SinkFunction.Context context = mock(SinkFunction.Context.class);
    
    sinkFunction.invoke(emptyRow, context);
    
    assertThat(sinkFunction.invocationCount.get()).isEqualTo(1);
  }

  @Test
  void testInvokeWithLargeRow() throws Exception {
    Row largeRow = Row.of(1, 2L, "test", 3.14, true, 4, 5L, "another", 6.28, false);
    SinkFunction.Context context = mock(SinkFunction.Context.class);
    
    sinkFunction.invoke(largeRow, context);
    
    assertThat(sinkFunction.invocationCount.get()).isEqualTo(1);
  }

  @ParameterizedTest
  @ValueSource(longs = {0L, 1000L, 5000L, 10000L, Long.MAX_VALUE})
  void testWriteWatermarkWithDifferentTimestamps(long timestamp) throws Exception {
    Watermark watermark = new Watermark(timestamp);
    
    sinkFunction.writeWatermark(watermark);
    
    assertThat(sinkFunction.watermarkWriteCount.get()).isEqualTo(1);
  }

  @Test
  void testConcurrentInvocations() throws InterruptedException {
    int numThreads = 20;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch latch = new CountDownLatch(numThreads);
    SinkFunction.Context context = mock(SinkFunction.Context.class);
    
    for (int i = 0; i < numThreads; i++) {
      final int index = i;
      executor.submit(() -> {
        try {
          Row row = Row.of(index, (long) index, "test-" + index);
          sinkFunction.invoke(row, context);
        } catch (Exception e) {
          throw new RuntimeException(e);
        } finally {
          latch.countDown();
        }
      });
    }
    
    latch.await(5, TimeUnit.SECONDS);
    executor.shutdown();
    
    assertThat(sinkFunction.invocationCount.get()).isEqualTo(numThreads);
  }

  @Test
  void testConcurrentWatermarkWrites() throws InterruptedException {
    int numThreads = 15;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch latch = new CountDownLatch(numThreads);
    
    for (int i = 0; i < numThreads; i++) {
      final long timestamp = i * 1000L;
      executor.submit(() -> {
        try {
          Watermark watermark = new Watermark(timestamp);
          sinkFunction.writeWatermark(watermark);
        } catch (Exception e) {
          throw new RuntimeException(e);
        } finally {
          latch.countDown();
        }
      });
    }
    
    latch.await(5, TimeUnit.SECONDS);
    executor.shutdown();
    
    assertThat(sinkFunction.watermarkWriteCount.get()).isEqualTo(numThreads);
  }

  @Test
  void testConcurrentMixedOperations() throws InterruptedException {
    int numThreads = 30;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch latch = new CountDownLatch(numThreads);
    SinkFunction.Context context = mock(SinkFunction.Context.class);
    
    for (int i = 0; i < numThreads; i++) {
      final int index = i;
      executor.submit(() -> {
        try {
          if (index % 3 == 0) {
            Row row = Row.of(index, (long) index, "test-" + index);
            sinkFunction.invoke(row, context);
          } else if (index % 3 == 1) {
            Watermark watermark = new Watermark(index * 1000L);
            sinkFunction.writeWatermark(watermark);
          } else {
            sinkFunction.finish();
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        } finally {
          latch.countDown();
        }
      });
    }
    
    latch.await(5, TimeUnit.SECONDS);
    executor.shutdown();
    
    assertThat(sinkFunction.invocationCount.get()).isEqualTo(10);
    assertThat(sinkFunction.watermarkWriteCount.get()).isEqualTo(10);
    assertThat(sinkFunction.finishCount.get()).isEqualTo(10);
  }

  @Test
  void testAlternatingOperations() throws Exception {
    SinkFunction.Context context = mock(SinkFunction.Context.class);
    
    for (int i = 0; i < 5; i++) {
      Row row = Row.of(i, (long) i, "test-" + i);
      sinkFunction.invoke(row, context);
      
      Watermark watermark = new Watermark(i * 1000L);
      sinkFunction.writeWatermark(watermark);
    }
    
    sinkFunction.finish();
    
    assertThat(sinkFunction.invocationCount.get()).isEqualTo(5);
    assertThat(sinkFunction.watermarkWriteCount.get()).isEqualTo(5);
    assertThat(sinkFunction.finishCount.get()).isEqualTo(1);
  }

  @Test
  void testRowWithNullValues() throws Exception {
    Row rowWithNulls = Row.of(1, null, "test", null);
    SinkFunction.Context context = mock(SinkFunction.Context.class);
    
    sinkFunction.invoke(rowWithNulls, context);
    
    assertThat(sinkFunction.invocationCount.get()).isEqualTo(1);
  }

  @Test
  void testRowWithAllNullValues() throws Exception {
    Row allNulls = Row.of(null, null, null);
    SinkFunction.Context context = mock(SinkFunction.Context.class);
    
    sinkFunction.invoke(allNulls, context);
    
    assertThat(sinkFunction.invocationCount.get()).isEqualTo(1);
  }

  @Test
  void testFinishCountsAllInvocations() throws Exception {
    sinkFunction.finish();
    sinkFunction.finish();
    sinkFunction.finish();
    sinkFunction.finish();
    sinkFunction.finish();
    
    // finish() increments the counter on each call
    assertThat(sinkFunction.finishCount.get()).isEqualTo(5);
  }
}
