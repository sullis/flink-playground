package io.github.sullis.flink.playground;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;


public class SimpleSinkFunction implements SinkFunction<Row> {
  int invocationCount = 0;

  @Override
  public void invoke(Row value, Context context) throws Exception {
    invocationCount++;
  }
}
