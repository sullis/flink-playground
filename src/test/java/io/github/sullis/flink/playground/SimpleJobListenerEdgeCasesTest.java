package io.github.sullis.flink.playground;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.core.execution.JobClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;


public class SimpleJobListenerEdgeCasesTest {

  private SimpleJobListener listener;

  @BeforeEach
  public void setUp() {
    listener = new SimpleJobListener();
  }

  @Test
  void testMultipleCompleteLifecycles() {
    listener.onJobSubmitted(null, null);
    listener.onJobExecuted(null, null);
    listener.onJobSubmitted(null, null);
    listener.onJobExecuted(null, null);
    listener.onJobSubmitted(null, null);
    listener.onJobExecuted(null, null);
    
    assertThat(listener.jobSubmittedCount).isEqualTo(3);
    assertThat(listener.jobExecutedCount).isEqualTo(3);
  }

  @Test
  void testOnlySubmissionsNoExecutions() {
    for (int i = 0; i < 10; i++) {
      listener.onJobSubmitted(null, null);
    }
    
    assertThat(listener.jobSubmittedCount).isEqualTo(10);
    assertThat(listener.jobExecutedCount).isEqualTo(0);
  }

  @Test
  void testOnlyExecutionsNoSubmissions() {
    for (int i = 0; i < 5; i++) {
      listener.onJobExecuted(null, null);
    }
    
    assertThat(listener.jobSubmittedCount).isEqualTo(0);
    assertThat(listener.jobExecutedCount).isEqualTo(5);
  }

  @Test
  void testMixedErrorAndSuccessScenarios() {
    Exception error1 = new RuntimeException("Submit error");
    Exception error2 = new IllegalStateException("Execution error");
    
    listener.onJobSubmitted(null, error1);
    listener.onJobSubmitted(null, null);
    listener.onJobExecuted(null, error2);
    listener.onJobExecuted(null, null);
    
    assertThat(listener.jobSubmittedCount).isEqualTo(2);
    assertThat(listener.jobExecutedCount).isEqualTo(2);
  }

  @Test
  void testWithActualMockedJobClient() {
    JobClient mockClient = mock(JobClient.class);
    JobExecutionResult mockResult = mock(JobExecutionResult.class);
    
    listener.onJobSubmitted(mockClient, null);
    listener.onJobExecuted(mockResult, null);
    
    assertThat(listener.jobSubmittedCount).isEqualTo(1);
    assertThat(listener.jobExecutedCount).isEqualTo(1);
  }

  @Test
  void testConcurrentJobSubmissions() throws InterruptedException {
    int numThreads = 10;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch latch = new CountDownLatch(numThreads);
    
    for (int i = 0; i < numThreads; i++) {
      executor.submit(() -> {
        try {
          listener.onJobSubmitted(null, null);
        } finally {
          latch.countDown();
        }
      });
    }
    
    latch.await(5, TimeUnit.SECONDS);
    executor.shutdown();
    
    assertThat(listener.jobSubmittedCount).isEqualTo(numThreads);
    assertThat(listener.jobExecutedCount).isEqualTo(0);
  }

  @Test
  void testConcurrentJobExecutions() throws InterruptedException {
    int numThreads = 10;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch latch = new CountDownLatch(numThreads);
    
    for (int i = 0; i < numThreads; i++) {
      executor.submit(() -> {
        try {
          listener.onJobExecuted(null, null);
        } finally {
          latch.countDown();
        }
      });
    }
    
    latch.await(5, TimeUnit.SECONDS);
    executor.shutdown();
    
    assertThat(listener.jobSubmittedCount).isEqualTo(0);
    assertThat(listener.jobExecutedCount).isEqualTo(numThreads);
  }

  @Test
  void testMixedConcurrentOperations() throws InterruptedException {
    int numThreads = 20;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch latch = new CountDownLatch(numThreads);
    
    // Half submit, half execute
    for (int i = 0; i < numThreads; i++) {
      final int index = i;
      executor.submit(() -> {
        try {
          if (index % 2 == 0) {
            listener.onJobSubmitted(null, null);
          } else {
            listener.onJobExecuted(null, null);
          }
        } finally {
          latch.countDown();
        }
      });
    }
    
    latch.await(5, TimeUnit.SECONDS);
    executor.shutdown();
    
    assertThat(listener.jobSubmittedCount).isEqualTo(10);
    assertThat(listener.jobExecutedCount).isEqualTo(10);
  }
}
