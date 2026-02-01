package io.github.sullis.flink.playground;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;


public class FlinkTestConfigTest {

  @Test
  void testResolveConfiguration() {
    Configuration config = FlinkTestConfig.resolveConfiguration();
    
    assertThat(config).isNotNull();
    assertThat(config.keySet()).isNotEmpty();
    assertThat(config.keySet()).contains("metrics.reporters");
  }

  @Test
  void testResolveConfigurationIsRepeatable() {
    Configuration config1 = FlinkTestConfig.resolveConfiguration();
    Configuration config2 = FlinkTestConfig.resolveConfiguration();
    
    assertThat(config1).isNotNull();
    assertThat(config2).isNotNull();
    assertThat(config1.keySet()).isEqualTo(config2.keySet());
  }
}
