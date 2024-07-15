package io.github.sullis.flink.playground;

import javax.annotation.Nullable;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;


public class SimpleJobListener implements JobListener {
  int jobSubmittedCount = 0;
  int jobExecutedCount = 0;

  @Override
  public void onJobSubmitted(@Nullable JobClient jobClient, @Nullable Throwable throwable) {
    jobSubmittedCount++;
    if (throwable != null) {
      throwable.printStackTrace();
    }
  }

  @Override
  public void onJobExecuted(@Nullable JobExecutionResult jobExecutionResult, @Nullable Throwable throwable) {
    jobExecutedCount++;
    if (throwable != null) {
      throwable.printStackTrace();
    }
  }
}
