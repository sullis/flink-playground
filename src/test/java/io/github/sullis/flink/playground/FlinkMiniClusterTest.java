package io.github.sullis.flink.playground;

import java.util.stream.Stream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;


@Testcontainers
@ExtendWith(MiniClusterExtension.class)
public class FlinkMiniClusterTest {

  @Test
  public void testHappyPath() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    assertThat(env).isInstanceOf(TestStreamEnvironment.class);
    TestStreamEnvironment testEnv = (TestStreamEnvironment) env;
    assertThat(testEnv.getParallelism()).isEqualTo(1);
    assertThat(testEnv.getMaxParallelism()).isEqualTo(-1);
  }
}