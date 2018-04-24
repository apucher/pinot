package com.linkedin.thirdeye.detection;

import com.linkedin.thirdeye.anomaly.alert.v2.AlertJobRunnerV2;
import com.linkedin.thirdeye.datalayer.bao.DetectionConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import java.util.Collection;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DetectionPipelineScheduler implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(DetectionPipelineScheduler.class);

  final DetectionConfigManager detectionDAO;
  final Scheduler scheduler;

  public DetectionPipelineScheduler(DetectionConfigManager detectionDAO) throws Exception {
    this.detectionDAO = detectionDAO;
    this.scheduler = StdSchedulerFactory.getDefaultScheduler();
  }

  @Override
  public void run() {
    try {
      Collection<DetectionConfigDTO> configs = this.detectionDAO.findAll();

      // add and update
      for (DetectionConfigDTO config : configs) {
        JobKey key = new JobKey(String.valueOf(config.getId()));
        Trigger trigger = TriggerBuilder.newTrigger().withSchedule(CronScheduleBuilder.cronSchedule(config.getCron())).build();
        JobDetail job = JobBuilder.newJob(AlertJobRunnerV2.class).withIdentity(key).build();
        this.scheduler.scheduleJob(job, trigger);
      }

      // remove
      // TODO

    } catch (SchedulerException e) {
      LOG.error("Error while updating detection schedule", e);
    }
  }
}
