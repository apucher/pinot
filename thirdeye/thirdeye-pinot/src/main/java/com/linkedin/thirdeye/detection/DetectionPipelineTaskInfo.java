package com.linkedin.thirdeye.detection;

import com.linkedin.thirdeye.anomaly.task.TaskInfo;


public class DetectionPipelineTaskInfo implements TaskInfo {
  long configId;
  long start;
  long end;

  public long getConfigId() {
    return configId;
  }

  public void setConfigId(long configId) {
    this.configId = configId;
  }

  public long getStart() {
    return start;
  }

  public void setStart(long start) {
    this.start = start;
  }

  public long getEnd() {
    return end;
  }

  public void setEnd(long end) {
    this.end = end;
  }
}
