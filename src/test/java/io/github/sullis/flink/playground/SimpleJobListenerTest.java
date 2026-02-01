package io.github.sullis.flink.playground;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.execution.JobClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;


public class SimpleJobListenerTest {

  private SimpleJobListener listener;

  @BeforeEach
  public void setUp() {
    listener = new SimpleJobListener();
  }

  @Test
  void testInitialCountsAreZero() {
    assertThat(listener.jobSubmittedCount.get()).isEqualTo(0);
    assertThat(listener.jobExecutedCount.get()).isEqualTo(0);
  }

  @Test
  void testOnJobSubmittedIncrementsCount() {
    JobClient mockJobClient = mock(JobClient.class);
    
    listener.onJobSubmitted(mockJobClient, null);
    
    assertThat(listener.jobSubmittedCount.get()).isEqualTo(1);
    assertThat(listener.jobExecutedCount.get()).isEqualTo(0);
  }

  @Test
  void testOnJobSubmittedWithNullJobClient() {
    listener.onJobSubmitted(null, null);
    
    assertThat(listener.jobSubmittedCount.get()).isEqualTo(1);
    assertThat(listener.jobExecutedCount.get()).isEqualTo(0);
  }

  @Test
  void testOnJobSubmittedWithThrowable() {
    Exception exception = new RuntimeException("Test exception");
    
    listener.onJobSubmitted(null, exception);
    
    assertThat(listener.jobSubmittedCount.get()).isEqualTo(1);
    assertThat(listener.jobExecutedCount.get()).isEqualTo(0);
  }

  @Test
  void testOnJobExecutedIncrementsCount() {
    JobExecutionResult mockResult = mock(JobExecutionResult.class);
    
    listener.onJobExecuted(mockResult, null);
    
    assertThat(listener.jobSubmittedCount.get()).isEqualTo(0);
    assertThat(listener.jobExecutedCount.get()).isEqualTo(1);
  }

  @Test
  void testOnJobExecutedWithNullResult() {
    listener.onJobExecuted(null, null);
    
    assertThat(listener.jobSubmittedCount.get()).isEqualTo(0);
    assertThat(listener.jobExecutedCount.get()).isEqualTo(1);
  }

  @Test
  void testOnJobExecutedWithThrowable() {
    Exception exception = new RuntimeException("Test exception");
    
    listener.onJobExecuted(null, exception);
    
    assertThat(listener.jobSubmittedCount.get()).isEqualTo(0);
    assertThat(listener.jobExecutedCount.get()).isEqualTo(1);
  }

  @Test
  void testMultipleJobSubmissions() {
    listener.onJobSubmitted(null, null);
    listener.onJobSubmitted(null, null);
    listener.onJobSubmitted(null, null);
    
    assertThat(listener.jobSubmittedCount.get()).isEqualTo(3);
    assertThat(listener.jobExecutedCount.get()).isEqualTo(0);
  }

  @Test
  void testMultipleJobExecutions() {
    listener.onJobExecuted(null, null);
    listener.onJobExecuted(null, null);
    
    assertThat(listener.jobSubmittedCount.get()).isEqualTo(0);
    assertThat(listener.jobExecutedCount.get()).isEqualTo(2);
  }

  @Test
  void testCompleteJobLifecycle() {
    // Simulate a complete job lifecycle
    listener.onJobSubmitted(null, null);
    listener.onJobExecuted(null, null);
    
    assertThat(listener.jobSubmittedCount.get()).isEqualTo(1);
    assertThat(listener.jobExecutedCount.get()).isEqualTo(1);
  }
}
