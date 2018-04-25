package com.linkedin.thirdeye.detection;

import com.linkedin.thirdeye.anomaly.utils.AnomalyUtils;
import com.linkedin.thirdeye.datalayer.bao.DetectionConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
  public static final int DEFAULT_DETECTION_DELAY = 5;
  public static final TimeUnit DEFAULT_ALERT_DELAY_UNIT = TimeUnit.MINUTES;

  final DetectionConfigManager detectionDAO;
  final Scheduler scheduler;
  private ScheduledExecutorService scheduledExecutorService;

  public DetectionPipelineScheduler(DetectionConfigManager detectionDAO) throws Exception {
    this.detectionDAO = detectionDAO;
    this.scheduler = StdSchedulerFactory.getDefaultScheduler();
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
  }

  public void start() throws SchedulerException {
    scheduler.start();
    scheduledExecutorService.scheduleWithFixedDelay(this, 0, DEFAULT_DETECTION_DELAY, DEFAULT_ALERT_DELAY_UNIT);
  }

  @Override
  public void run() {
    try {
      Collection<DetectionConfigDTO> configs = this.detectionDAO.findAll();

      // add and update
      for (DetectionConfigDTO config : configs) {
        JobKey key = new JobKey(String.valueOf(config.getId()));
        if (scheduler.checkExists(key)) {
          LOG.warn("Detection config  " + key + " is already scheduled.");
          continue;
        }
        Trigger trigger =
            TriggerBuilder.newTrigger().withSchedule(CronScheduleBuilder.cronSchedule(config.getCron())).build();
        JobDetail job = JobBuilder.newJob(DetectionPipelineJob.class).withIdentity(key).build();
        this.scheduler.scheduleJob(job, trigger);
        LOG.info(String.format("scheduled detection pipeline job %s.", key.getName()));
      }

      // remove
      // TODO

    } catch (SchedulerException e) {
      LOG.error("Error while updating detection schedule", e);
    }
  }

  public void shutdown() throws SchedulerException {
    AnomalyUtils.safelyShutdownExecutionService(scheduledExecutorService, this.getClass());
    scheduler.shutdown();
  }
}
