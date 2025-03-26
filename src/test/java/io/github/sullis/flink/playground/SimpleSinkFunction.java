package io.github.sullis.flink.playground;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.types.Row;


public class SimpleSinkFunction implements SinkFunction<Row> {
  AtomicInteger invocationCount = new AtomicInteger(0);
  AtomicInteger watermarkWriteCount = new AtomicInteger(0);
  AtomicInteger finishCount = new AtomicInteger(0);

  public SimpleSinkFunction() {
    // zero arg constructor
  }

  @Override
  public void invoke(Row value, Context context) throws Exception {
    System.out.println("invoke called on this: " + this);
    invocationCount.incrementAndGet();
  }

  @Override
  public void writeWatermark(Watermark watermark) throws Exception {
    watermarkWriteCount.incrementAndGet();
  }

  @Override
  public void finish() throws Exception {
    finishCount.incrementAndGet();
  }
}
