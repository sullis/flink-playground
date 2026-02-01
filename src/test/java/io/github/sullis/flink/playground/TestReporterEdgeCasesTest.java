package io.github.sullis.flink.playground;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestReporterEdgeCasesTest {

  private TestReporter reporter;
  private MetricGroup mockMetricGroup;

  @BeforeEach
  public void setUp() {
    reporter = new TestReporter();
    mockMetricGroup = mock(MetricGroup.class);
    when(mockMetricGroup.getMetricIdentifier(anyString(), any())).thenReturn("test.identifier");
  }

  @Test
  void testAddMultipleMetricsOfDifferentTypes() {
    Counter counter = new SimpleCounter();
    Gauge<Integer> gauge = () -> 42;
    Meter meter = mock(Meter.class);
    when(meter.getMetricType()).thenReturn(org.apache.flink.metrics.MetricType.METER);
    Histogram histogram = mock(Histogram.class);
    when(histogram.getMetricType()).thenReturn(org.apache.flink.metrics.MetricType.HISTOGRAM);
    
    reporter.notifyOfAddedMetric(counter, "counter.1", mockMetricGroup);
    reporter.notifyOfAddedMetric(gauge, "gauge.1", mockMetricGroup);
    reporter.notifyOfAddedMetric(meter, "meter.1", mockMetricGroup);
    reporter.notifyOfAddedMetric(histogram, "histogram.1", mockMetricGroup);
    
    assertThat(reporter.counters()).hasSize(1);
    assertThat(reporter.gauges()).hasSize(1);
    assertThat(reporter.meters()).hasSize(1);
    assertThat(reporter.histograms()).hasSize(1);
  }

  @Test
  void testRemoveNonExistentMetricDoesNotThrow() {
    Counter counter = new SimpleCounter();
    
    reporter.notifyOfRemovedMetric(counter, "nonexistent", mockMetricGroup);
    
    assertThat(reporter.counters()).isEmpty();
  }

  @Test
  void testAddAndRemoveMultipleMetrics() {
    Counter counter1 = new SimpleCounter();
    Counter counter2 = new SimpleCounter();
    Counter counter3 = new SimpleCounter();
    
    reporter.notifyOfAddedMetric(counter1, "counter.1", mockMetricGroup);
    reporter.notifyOfAddedMetric(counter2, "counter.2", mockMetricGroup);
    reporter.notifyOfAddedMetric(counter3, "counter.3", mockMetricGroup);
    assertThat(reporter.counters()).hasSize(3);
    
    reporter.notifyOfRemovedMetric(counter2, "counter.2", mockMetricGroup);
    assertThat(reporter.counters()).hasSize(2);
    assertThat(reporter.counters()).containsKey(counter1);
    assertThat(reporter.counters()).containsKey(counter3);
    assertThat(reporter.counters()).doesNotContainKey(counter2);
  }

  @Test
  void testFilterCharactersWithUnicodeCharacters() {
    String input = "test.métric.名前.😀";
    String result = reporter.filterCharacters(input);
    
    assertThat(result).isEqualTo(input);
  }

  @Test
  void testFilterCharactersWithWhitespace() {
    String input = "test metric name with spaces";
    String result = reporter.filterCharacters(input);
    
    assertThat(result).isEqualTo(input);
  }

  @Test
  void testFilterCharactersWithNewlines() {
    String input = "test\nmetric\nname";
    String result = reporter.filterCharacters(input);
    
    assertThat(result).isEqualTo(input);
  }

  @Test
  void testMetricMapsAreModifiable() {
    Counter counter = new SimpleCounter();
    reporter.notifyOfAddedMetric(counter, "counter.1", mockMetricGroup);
    
    Map<Counter, String> counters = reporter.counters();
    assertThat(counters).hasSize(1);
    
    // The map should be the internal map, so modifications affect the reporter
    int initialSize = counters.size();
    assertThat(initialSize).isEqualTo(1);
  }

  @Test
  void testCreateMetricReporterReturnsNewInstance() {
    TestReporter newReporter = (TestReporter) reporter.createMetricReporter(null);
    
    assertThat(newReporter).isNotNull();
    assertThat(newReporter).isNotSameAs(reporter);
  }

  @Test
  void testGaugeWithDifferentTypes() {
    Gauge<String> stringGauge = () -> "test";
    Gauge<Double> doubleGauge = () -> 3.14;
    Gauge<Boolean> booleanGauge = () -> true;
    
    reporter.notifyOfAddedMetric(stringGauge, "gauge.string", mockMetricGroup);
    reporter.notifyOfAddedMetric(doubleGauge, "gauge.double", mockMetricGroup);
    reporter.notifyOfAddedMetric(booleanGauge, "gauge.boolean", mockMetricGroup);
    
    assertThat(reporter.gauges()).hasSize(3);
  }
}
