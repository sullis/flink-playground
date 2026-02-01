package io.github.sullis.flink.playground;

import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;


public class SimpleJobListener implements JobListener {
  AtomicInteger jobSubmittedCount = new AtomicInteger(0);
  AtomicInteger jobExecutedCount = new AtomicInteger(0);

  @Override
  public void onJobSubmitted(@Nullable JobClient jobClient, @Nullable Throwable throwable) {
    jobSubmittedCount.incrementAndGet();
    if (throwable != null) {
      throwable.printStackTrace();
    }
  }

  @Override
  public void onJobExecuted(@Nullable JobExecutionResult jobExecutionResult, @Nullable Throwable throwable) {
    jobExecutedCount.incrementAndGet();
    if (throwable != null) {
      throwable.printStackTrace();
    }
  }
}
