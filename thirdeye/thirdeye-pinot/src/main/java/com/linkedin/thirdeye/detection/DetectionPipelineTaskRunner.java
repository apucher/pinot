package com.linkedin.thirdeye.detection;

import com.linkedin.thirdeye.anomaly.task.TaskContext;
import com.linkedin.thirdeye.anomaly.task.TaskInfo;
import com.linkedin.thirdeye.anomaly.task.TaskResult;
import com.linkedin.thirdeye.anomaly.task.TaskRunner;
import com.linkedin.thirdeye.datalayer.bao.DetectionConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import java.util.Collections;
import java.util.List;


public class DetectionPipelineTaskRunner implements TaskRunner {
  final DetectionConfigManager detectionDAO = DAORegistry.getInstance().getDetectionConfigManager();

  @Override
  public List<TaskResult> execute(TaskInfo taskInfo, TaskContext taskContext) throws Exception {
    DetectionPipelineTaskInfo info = (DetectionPipelineTaskInfo) taskInfo;

    DetectionConfigDTO config = this.detectionDAO.findById(info.configId);
    if (config == null) {
      throw new IllegalArgumentException(String.format("Could not resolve config id %d", info.configId));
    }

    DetectionPipeline pipeline = DetectionPipelineLoader.from(config, info.start, info.end);
    pipeline.run();

    return Collections.emptyList();
  }
}
