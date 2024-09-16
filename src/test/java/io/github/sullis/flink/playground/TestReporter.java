package io.github.sullis.flink.playground;

import java.util.Map;
import java.util.Properties;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.reporter.AbstractReporter;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.MetricReporterFactory;


/*

 Flink Metric Reporters
 https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/metric_reporters/

 */
public class TestReporter extends AbstractReporter
    implements MetricReporterFactory {

  @Override
  public String filterCharacters(String input) {
    return input;
  }

  @Override
  public void open(MetricConfig config) {
    // no-op
  }

  @Override
  public void close() {
    // no-op
  }

  public Map<Counter, String> counters() {
    return this.counters;
  }

  public Map<Gauge<?>, String> gauges() {
    return this.gauges;
  }

  public Map<Meter, String> meters() {
    return this.meters;
  }

  public Map<Histogram, String> histograms() {
    return this.histograms;
  }

  @Override
  public MetricReporter createMetricReporter(Properties properties) {
    return new TestReporter();
  }
}
