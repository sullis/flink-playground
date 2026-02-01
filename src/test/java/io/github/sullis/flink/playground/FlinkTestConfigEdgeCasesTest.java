package io.github.sullis.flink.playground;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


public class FlinkTestConfigEdgeCasesTest {

  @Test
  void testConfigurationContainsExpectedKeys() {
    Configuration config = FlinkTestConfig.resolveConfiguration();
    
    assertThat(config.keySet()).contains("metrics.reporters");
  }

  @Test
  void testConfigurationIsNotEmpty() {
    Configuration config = FlinkTestConfig.resolveConfiguration();
    
    assertThat(config.keySet()).isNotEmpty();
    assertThat(config.keySet().size()).isGreaterThan(0);
  }

  @Test
  void testMultipleResolveCallsReturnConsistentData() {
    Configuration config1 = FlinkTestConfig.resolveConfiguration();
    Configuration config2 = FlinkTestConfig.resolveConfiguration();
    Configuration config3 = FlinkTestConfig.resolveConfiguration();
    
    assertThat(config1.keySet()).isEqualTo(config2.keySet());
    assertThat(config2.keySet()).isEqualTo(config3.keySet());
  }

  @Test
  void testConfigurationContainsMetricsReportersValue() {
    Configuration config = FlinkTestConfig.resolveConfiguration();
    
    String metricsReporters = config.getString("metrics.reporters", null);
    assertThat(metricsReporters).isNotNull();
  }
}
