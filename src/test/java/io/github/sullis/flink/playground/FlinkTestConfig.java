package io.github.sullis.flink.playground;

import java.io.File;
import java.net.URL;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;

import static org.assertj.core.api.Assertions.assertThat;

public class FlinkTestConfig {
  private static final String YAML_FILENAME = "config.yaml";

  public static Configuration resolveConfiguration() {
    URL resource = FlinkTestConfig.class.getResource("/conf/" + YAML_FILENAME);
    if (resource == null) {
      throw new IllegalStateException("unable to find " + YAML_FILENAME);
    }
    File dir = new File(resource.getFile()).getParentFile();
    assertThat(dir).exists();
    assertThat(dir).isDirectory();
    return GlobalConfiguration.loadConfiguration(dir.getAbsolutePath());
  }
}
