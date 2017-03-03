package com.linkedin.thirdeye.anomaly;

import com.linkedin.thirdeye.anomaly.alert.AlertJobResource;
import com.linkedin.thirdeye.anomaly.alert.AlertJobScheduler;
import com.linkedin.thirdeye.anomaly.alert.v2.AlertJobSchedulerV2;
import com.linkedin.thirdeye.anomaly.detection.DetectionJobResource;
import com.linkedin.thirdeye.anomaly.detection.DetectionJobScheduler;
import com.linkedin.thirdeye.anomaly.detectionex.DetectionExJobScheduler;
import com.linkedin.thirdeye.anomaly.merge.AnomalyMergeExecutor;
import com.linkedin.thirdeye.anomaly.monitor.MonitorJobScheduler;
import com.linkedin.thirdeye.anomaly.task.TaskDriver;
import com.linkedin.thirdeye.anomalydetection.alertFilterAutotune.AlertFilterAutotuneFactory;
import com.linkedin.thirdeye.autoload.pinot.metrics.AutoLoadPinotMetricsService;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClientConfig;
import com.linkedin.thirdeye.common.BaseThirdEyeApplication;
import com.linkedin.thirdeye.completeness.checker.DataCompletenessScheduler;
import com.linkedin.thirdeye.dashboard.resources.AnomalyFunctionResource;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExContext;
import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExDataSource;
import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExFactory;
import com.linkedin.thirdeye.detector.functionex.dataframe.DataFrame;
import com.linkedin.thirdeye.detector.functionex.impl.ThirdEyePinotConnection;
import com.linkedin.thirdeye.detector.functionex.impl.ThirdEyePinotDataSource;
import com.yammer.metrics.core.Counter;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class ThirdEyeAnomalyApplication
    extends BaseThirdEyeApplication<ThirdEyeAnomalyConfiguration> {

  private DetectionJobScheduler detectionJobScheduler = null;
  private DetectionExJobScheduler detectionExJobScheduler = null;
  private TaskDriver taskDriver = null;
  private MonitorJobScheduler monitorJobScheduler = null;
  private AlertJobScheduler alertJobScheduler = null;
  private AlertJobSchedulerV2 alertJobSchedulerV2;
  private AnomalyFunctionFactory anomalyFunctionFactory = null;
  private AnomalyFunctionExFactory anomalyFunctionExFactory = null;
  private AnomalyMergeExecutor anomalyMergeExecutor = null;
  private AutoLoadPinotMetricsService autoLoadPinotMetricsService = null;
  private DataCompletenessScheduler dataCompletenessScheduler = null;
  private AlertFilterFactory alertFilterFactory = null;
  private AlertFilterAutotuneFactory alertFilterAutotuneFactory = null;

  public static final Counter detectionTaskCounter =
      metricsRegistry.newCounter(ThirdEyeAnomalyApplication.class, "detectionTaskCounter");

  public static final Counter detectionTaskSuccessCounter =
      metricsRegistry.newCounter(ThirdEyeAnomalyApplication.class, "detectionTaskSuccessCounter");

  public static void main(final String[] args) throws Exception {

    List<String> argList = new ArrayList<>(Arrays.asList(args));
    if (argList.size() == 1) {
      argList.add(0, "server");
    }

    int lastIndex = argList.size() - 1;
    String thirdEyeConfigDir = argList.get(lastIndex);
    System.setProperty("dw.rootDir", thirdEyeConfigDir);
    String detectorApplicationConfigFile = thirdEyeConfigDir + "/" + "detector.yml";
    argList.set(lastIndex, detectorApplicationConfigFile); // replace config dir with the
                                                           // actual config file
    new ThirdEyeAnomalyApplication().run(argList.toArray(new String[argList.size()]));
  }

  @Override
  public String getName() {
    return "Thirdeye Controller";
  }

  @Override
  public void initialize(final Bootstrap<ThirdEyeAnomalyConfiguration> bootstrap) {
    bootstrap.addBundle(new AssetsBundle("/assets/", "/", "index.html"));
  }

  @Override
  public void run(final ThirdEyeAnomalyConfiguration config, final Environment environment)
      throws Exception {
    LOG.info("Starting ThirdeyeAnomalyApplication : Scheduler {} Worker {}", config.isScheduler(), config.isWorker());
    super.initDAOs();
    ThirdEyeCacheRegistry.initializeCaches(config);
    environment.lifecycle().manage(new Managed() {
      @Override
      public void start() throws Exception {

        if (config.isWorker()) {
          if(alertFilterFactory == null)
            alertFilterFactory = new AlertFilterFactory(config.getAlertFilterConfigPath());
          anomalyFunctionFactory = makeAnomalyFunctionFactory(config);
          anomalyFunctionExFactory = makeAnomalyfunctionExFactory(config);
          taskDriver = new TaskDriver(config, anomalyFunctionFactory, anomalyFunctionExFactory, alertFilterFactory);
          taskDriver.start();
        }
        if (config.isScheduler()) {
          if(alertFilterFactory == null)
            alertFilterFactory = new AlertFilterFactory(config.getAlertFilterConfigPath());
          detectionJobScheduler = new DetectionJobScheduler();
          alertFilterAutotuneFactory = new AlertFilterAutotuneFactory(config.getFilterAutotuneConfigPath());
          detectionJobScheduler.start();
          environment.jersey().register(new DetectionJobResource(detectionJobScheduler, alertFilterFactory, alertFilterAutotuneFactory));
          environment.jersey().register(new AnomalyFunctionResource(config.getFunctionConfigPath()));

          detectionExJobScheduler = new DetectionExJobScheduler();
          detectionExJobScheduler.start();
        }
        if (config.isMonitor()) {
          monitorJobScheduler = new MonitorJobScheduler(config.getMonitorConfiguration());
          monitorJobScheduler.start();
        }
        if (config.isAlert()) {
          alertJobScheduler = new AlertJobScheduler();
          alertJobScheduler.start();

          // start alert scheduler v2
          alertJobSchedulerV2 = new AlertJobSchedulerV2();
          alertJobSchedulerV2.start();

          environment.jersey()
          .register(new AlertJobResource(alertJobScheduler, emailConfigurationDAO));
        }
        if (config.isMerger()) {
          // anomalyFunctionFactory might have initiated if current machine is also a worker
          if (anomalyFunctionFactory == null) {
            anomalyFunctionFactory = new AnomalyFunctionFactory(config.getFunctionConfigPath());
          }
          ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
          anomalyMergeExecutor =
              new AnomalyMergeExecutor(executorService, anomalyFunctionFactory);
          anomalyMergeExecutor.start();
        }
        if (config.isAutoload()) {
          autoLoadPinotMetricsService = new AutoLoadPinotMetricsService(config);
          autoLoadPinotMetricsService.start();
        }
        if (config.isDataCompleteness()) {
          dataCompletenessScheduler = new DataCompletenessScheduler();
          dataCompletenessScheduler.start();
        }
      }

      @Override
      public void stop() throws Exception {
        if (config.isWorker()) {
          taskDriver.stop();
        }
        if (config.isScheduler()) {
          detectionJobScheduler.shutdown();
          detectionExJobScheduler.shutdown();
        }
        if (config.isMonitor()) {
          monitorJobScheduler.stop();
        }
        if (config.isAlert()) {
          alertJobScheduler.shutdown();
          alertJobSchedulerV2.shutdown();
        }
        if (config.isMerger()) {
          anomalyMergeExecutor.stop();
        }
        if (config.isAutoload()) {
          autoLoadPinotMetricsService.shutdown();
        }
        if (config.isDataCompleteness()) {
          dataCompletenessScheduler.shutdown();
        }
      }
    });
  }

  private AnomalyFunctionFactory makeAnomalyFunctionFactory(ThirdEyeAnomalyConfiguration config) throws Exception {
    return new AnomalyFunctionFactory(config.getFunctionConfigPath());
  }

  private AnomalyFunctionExFactory makeAnomalyfunctionExFactory(ThirdEyeAnomalyConfiguration config) throws Exception {
    PinotThirdEyeClientConfig clientConfig = PinotThirdEyeClientConfig.createThirdEyeClientConfig(config);

    ThirdEyePinotConnection pinotConn = new ThirdEyePinotConnection(clientConfig);

    AnomalyFunctionExFactory factory = new AnomalyFunctionExFactory();
    factory.addDataSource("mock", new MockDataSource());
    factory.addDataSource("pinot", new ThirdEyePinotDataSource(pinotConn));

    return factory;
  }

  static class MockDataSource implements AnomalyFunctionExDataSource<String, DataFrame> {
    @Override
    public DataFrame query(String query, AnomalyFunctionExContext context) {
      DataFrame df = new DataFrame(5);
      df.addSeries("long", 3, 4, 5, 6, 7);
      df.addSeries("double", 1.2, 3.5, 2.8, 6.4, 4.9);
      df.addSeries("stable", 1, 1, 1, 1, 1);
      df.addSeries("string", "aaa", "abb", "bcb", "caa", "ccb");
      return df;
    }
  }
}
