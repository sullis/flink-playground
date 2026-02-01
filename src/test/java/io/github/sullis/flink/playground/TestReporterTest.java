package io.github.sullis.flink.playground;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestReporterTest {

  private TestReporter reporter;
  private MetricGroup mockMetricGroup;

  @BeforeEach
  public void setUp() {
    reporter = new TestReporter();
    mockMetricGroup = mock(MetricGroup.class);
    when(mockMetricGroup.getMetricIdentifier(anyString(), any())).thenReturn("test.identifier");
  }

  @Test
  void testFilterCharactersReturnsInput() {
    String input = "test.metric.name";
    String result = reporter.filterCharacters(input);
    
    assertThat(result).isEqualTo(input);
  }

  @Test
  void testFilterCharactersWithSpecialCharacters() {
    String input = "test@metric#name$123";
    String result = reporter.filterCharacters(input);
    
    assertThat(result).isEqualTo(input);
  }

  @Test
  void testFilterCharactersWithEmptyString() {
    String input = "";
    String result = reporter.filterCharacters(input);
    
    assertThat(result).isEqualTo(input);
  }

  @Test
  void testOpenDoesNotThrow() {
    MetricConfig config = new MetricConfig();
    
    reporter.open(config);
    
    // No exception means success
  }

  @Test
  void testCloseDoesNotThrow() {
    reporter.close();
    
    // No exception means success
  }

  @Test
  void testCountersMapInitiallyEmpty() {
    Map<Counter, String> counters = reporter.counters();
    
    assertThat(counters).isNotNull();
    assertThat(counters).isEmpty();
  }

  @Test
  void testGaugesMapInitiallyEmpty() {
    Map<Gauge<?>, String> gauges = reporter.gauges();
    
    assertThat(gauges).isNotNull();
    assertThat(gauges).isEmpty();
  }

  @Test
  void testMetersMapInitiallyEmpty() {
    Map<Meter, String> meters = reporter.meters();
    
    assertThat(meters).isNotNull();
    assertThat(meters).isEmpty();
  }

  @Test
  void testHistogramsMapInitiallyEmpty() {
    Map<Histogram, String> histograms = reporter.histograms();
    
    assertThat(histograms).isNotNull();
    assertThat(histograms).isEmpty();
  }

  @Test
  void testNotifyOfAddedCounter() {
    Counter counter = new SimpleCounter();
    String metricName = "test.counter";
    
    reporter.notifyOfAddedMetric(counter, metricName, mockMetricGroup);
    
    Map<Counter, String> counters = reporter.counters();
    assertThat(counters).hasSize(1);
    assertThat(counters).containsKey(counter);
    assertThat(counters.get(counter)).isEqualTo("test.identifier");
  }

  @Test
  void testNotifyOfAddedGauge() {
    Gauge<Integer> gauge = () -> 42;
    String metricName = "test.gauge";
    
    reporter.notifyOfAddedMetric(gauge, metricName, mockMetricGroup);
    
    Map<Gauge<?>, String> gauges = reporter.gauges();
    assertThat(gauges).hasSize(1);
    assertThat(gauges).containsKey(gauge);
    assertThat(gauges.get(gauge)).isEqualTo("test.identifier");
  }

  @Test
  void testNotifyOfAddedMeter() {
    Meter meter = mock(Meter.class);
    when(meter.getMetricType()).thenReturn(org.apache.flink.metrics.MetricType.METER);
    String metricName = "test.meter";
    
    reporter.notifyOfAddedMetric(meter, metricName, mockMetricGroup);
    
    Map<Meter, String> meters = reporter.meters();
    assertThat(meters).hasSize(1);
    assertThat(meters).containsKey(meter);
    assertThat(meters.get(meter)).isEqualTo("test.identifier");
  }

  @Test
  void testNotifyOfAddedHistogram() {
    Histogram histogram = mock(Histogram.class);
    when(histogram.getMetricType()).thenReturn(org.apache.flink.metrics.MetricType.HISTOGRAM);
    String metricName = "test.histogram";
    
    reporter.notifyOfAddedMetric(histogram, metricName, mockMetricGroup);
    
    Map<Histogram, String> histograms = reporter.histograms();
    assertThat(histograms).hasSize(1);
    assertThat(histograms).containsKey(histogram);
    assertThat(histograms.get(histogram)).isEqualTo("test.identifier");
  }

  @Test
  void testNotifyOfRemovedCounter() {
    Counter counter = new SimpleCounter();
    String metricName = "test.counter";
    
    reporter.notifyOfAddedMetric(counter, metricName, mockMetricGroup);
    assertThat(reporter.counters()).hasSize(1);
    
    reporter.notifyOfRemovedMetric(counter, metricName, mockMetricGroup);
    assertThat(reporter.counters()).isEmpty();
  }

  @Test
  void testCreateMetricReporter() {
    Properties properties = new Properties();
    
    TestReporter newReporter = (TestReporter) reporter.createMetricReporter(properties);
    
    assertThat(newReporter).isNotNull();
    assertThat(newReporter).isInstanceOf(TestReporter.class);
  }

  @Test
  void testCreateMetricReporterWithProperties() {
    Properties properties = new Properties();
    properties.setProperty("test.key", "test.value");
    
    TestReporter newReporter = (TestReporter) reporter.createMetricReporter(properties);
    
    assertThat(newReporter).isNotNull();
    assertThat(newReporter).isInstanceOf(TestReporter.class);
  }

  @Test
  void testMultipleCounters() {
    Counter counter1 = new SimpleCounter();
    Counter counter2 = new SimpleCounter();
    Counter counter3 = new SimpleCounter();
    
    reporter.notifyOfAddedMetric(counter1, "counter.1", mockMetricGroup);
    reporter.notifyOfAddedMetric(counter2, "counter.2", mockMetricGroup);
    reporter.notifyOfAddedMetric(counter3, "counter.3", mockMetricGroup);
    
    Map<Counter, String> counters = reporter.counters();
    assertThat(counters).hasSize(3);
  }
}
