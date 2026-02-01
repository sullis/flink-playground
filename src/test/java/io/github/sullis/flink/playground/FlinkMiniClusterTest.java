package io.github.sullis.flink.playground;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


@Testcontainers
@ExtendWith(MiniClusterExtension.class)
public class FlinkMiniClusterTest {

  @Test
  public void testHappyPath(@InjectClusterClient RestClusterClient<?> restClusterClient)
    throws Exception {

    assertThat(restClusterClient).isNotNull();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    assertThat(env).isInstanceOf(TestStreamEnvironment.class);
    TestStreamEnvironment testEnv = (TestStreamEnvironment) env;
    assertThat(testEnv.getParallelism()).isEqualTo(1);
    assertThat(testEnv.getMaxParallelism()).isEqualTo(-1);

    assertThat(restClusterClient.getClusterId()).isNotNull();

    JobID bogusJobId = new JobID();
    assertThatThrownBy(() -> restClusterClient.getJobStatus(bogusJobId).get())
        .hasMessageContaining("Could not find Flink job (" + bogusJobId.toString() + ")");
  }

  @Test
  public void testClusterIdIsConsistent(@InjectClusterClient RestClusterClient<?> restClusterClient) {
    assertThat(restClusterClient).isNotNull();
    
    Object clusterId1 = restClusterClient.getClusterId();
    Object clusterId2 = restClusterClient.getClusterId();
    
    assertThat(clusterId1).isNotNull();
    assertThat(clusterId2).isNotNull();
    assertThat(clusterId1).isEqualTo(clusterId2);
  }

  @Test
  public void testMultipleJobIdChecks(@InjectClusterClient RestClusterClient<?> restClusterClient) {
    assertThat(restClusterClient).isNotNull();
    
    JobID jobId1 = new JobID();
    JobID jobId2 = new JobID();
    
    assertThat(jobId1).isNotEqualTo(jobId2);
    
    // Both should fail since neither job exists
    assertThatThrownBy(() -> restClusterClient.getJobStatus(jobId1).get())
        .hasMessageContaining("Could not find Flink job");
    
    assertThatThrownBy(() -> restClusterClient.getJobStatus(jobId2).get())
        .hasMessageContaining("Could not find Flink job");
  }

  @Test
  public void testStreamEnvironmentParallelismConfiguration(@InjectClusterClient RestClusterClient<?> restClusterClient) {
    assertThat(restClusterClient).isNotNull();
    
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    assertThat(env).isInstanceOf(TestStreamEnvironment.class);
    
    TestStreamEnvironment testEnv = (TestStreamEnvironment) env;
    
    // Verify default parallelism
    assertThat(testEnv.getParallelism()).isEqualTo(1);
    
    // Verify max parallelism
    assertThat(testEnv.getMaxParallelism()).isEqualTo(-1);
  }
}