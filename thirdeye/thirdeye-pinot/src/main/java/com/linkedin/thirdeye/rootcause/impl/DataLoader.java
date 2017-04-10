package com.linkedin.thirdeye.rootcause.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.common.ThirdEyeConfiguration;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.EventManager;
import com.linkedin.thirdeye.datalayer.bao.jdbc.DatasetConfigManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.EventManagerImpl;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.RootCauseEntityDTO;
import com.linkedin.thirdeye.datalayer.pojo.RootCauseEntityBean;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;

import java.io.*;
import java.util.*;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * DataLoader for populating root cause analysis database
 */
public class DataLoader {
  private static final Logger LOG = LoggerFactory.getLogger(DataLoader.class);

  private static final String ID_MONITOR_FROM = "monitor-from";
  private static final String ID_MONITOR_TO = "monitor-to";
  private static final String ID_PINOT = "enable-pinot";
  private static final String ID_THIRDEYE = "enable-thirdeye";
  private static final String ID_EVENT = "enable-event";
  private static final String ID_METRIC = "enable-metric";
  private static final String ID_METRIC_CORRELATION = "metric-correlation";
  private static final String ID_OUTPUT_PATH = "output";

  private static final String ID_METRIC_PATH = "metric-path";
  private static final String ID_DIMENSION_PATH = "dimension-path";
  private static final String ID_METRIC_DIMENSION = "metric-dimension";

  public static void main(String[] args) throws Exception {
    Options options = makeParserOptions();
    CommandLineParser parser = new BasicParser();
    HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd;

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      formatter.printHelp(DataLoader.class.getSimpleName(), options);
      System.exit(1);
      return;
    }

//    String classname = cmd.getOptionValue(ID_CLASSNAME);
//    LOG.info("Using anomaly function '{}'", classname);

    long monitoringEnd = Long.parseLong(cmd.getOptionValue(ID_MONITOR_TO, String.valueOf(DateTime.now(DateTimeZone.UTC).getMillis())));
    long monitoringStart = Long.parseLong(cmd.getOptionValue(ID_MONITOR_FROM, String.valueOf(monitoringEnd - 3600000 * 24 * 7)));
    LOG.info("Setting monitoring window from '{}' to '{}'", monitoringStart, monitoringEnd);

//    Map<String, String> config = new HashMap<>();
//    if(cmd.hasOption(ID_CONFIG_FILE))
//      config.putAll(parseConfigFile(new File(cmd.getOptionValue(ID_CONFIG_FILE))));
//    if(cmd.hasOption(ID_CONFIG))
//      config.putAll(parseConfig(cmd.getOptionValue(ID_CONFIG)));
//    LOG.info("Using configuration '{}'", config);

//    if(cmd.hasOption(ID_AS_JSOM)) {
//      LOG.info(config2json(config));
//      System.exit(0);
//    }

    if(cmd.hasOption(ID_THIRDEYE)) {
      LOG.info("Enabling ThirdEye database connector with config '{}'", cmd.getOptionValue(ID_THIRDEYE));
      File configFile = new File(cmd.getOptionValue(ID_THIRDEYE));
      DaoProviderUtil.init(configFile);
    }

    DatasetConfigManager datasetDAO = null;
    if(cmd.hasOption(ID_METRIC_CORRELATION)) {
      if(!cmd.hasOption(ID_THIRDEYE)) {
        LOG.error("--{} requires --{}", ID_METRIC_CORRELATION, ID_THIRDEYE);
        System.exit(1);
      }

      LOG.info("Enabling 'metric' datasource");
      datasetDAO = DaoProviderUtil.getInstance(DatasetConfigManagerImpl.class);
      LOG.info("metric: {}", datasetDAO.findAll());
    }

    ThirdEyeCacheRegistry cacheRegistry = null;
    if(cmd.hasOption(ID_PINOT)) {
      if(!cmd.hasOption(ID_METRIC_CORRELATION)) {
        LOG.error("--{} requires --{}", ID_PINOT, ID_METRIC);
        System.exit(1);
      }

      LOG.info("Enabling pinot connector with config '{}'", cmd.getOptionValue(ID_PINOT));
      File file = new File(cmd.getOptionValue(ID_PINOT));
      String rootDir = file.getParent();

//      System.setProperty("max_pinot_connections", "3");

      ThirdEyeConfiguration thirdEyeConfig = new ThirdEyeAnomalyConfiguration();
      thirdEyeConfig.setRootDir(rootDir);
      thirdEyeConfig.setSkipPeriodicCacheRefresh(true);

      ThirdEyeCacheRegistry.initializeCaches(thirdEyeConfig);
      cacheRegistry = ThirdEyeCacheRegistry.getInstance();
      LOG.info("pinot: {}", cacheRegistry.getCollectionsCache().getCollections());

      String dataset = "login_hourly_additive";

      DatasetConfigDTO config = datasetDAO.findByDataset(dataset);

      // TODO adapt to recent API changes
      throw new IllegalStateException("not implemented yet");

//      List<MetricFunction> functions = new ArrayList<>();
//      functions.add(new MetricFunction(MetricAggFunction.COUNT, "*"));
//
////      TimeGranularity tg = new TimeGranularity(config.getTimeDuration(), config.getTimeUnit());
////      long startTime = tg.convertToUnit(monitoringStart);
////      long endTime = tg.convertToUnit(monitoringEnd);
////      LOG.info("column={} startTime={} endTime={}", config.getTimeColumn(), startTime, endTime);
//
//      ThirdEyeRequest request = ThirdEyeRequest.newBuilder()
//          .setCollection(dataset)
//          .setMetricFunctions(functions)
//          .setStartTimeInclusive(monitoringStart)
//          .setEndTimeExclusive(monitoringEnd)
//          .addGroupBy(config.getTimeColumn())
//          .build("ref");
//
//      ThirdEyeResponse response = cacheRegistry.getQueryCache().getQueryResult(request);
//      LOG.info("pinot: datTimeSpec: {}", response.getDataTimeSpec());
//      LOG.info("pinot: groupKeyColumns: {}", response.getGroupKeyColumns());
//      LOG.info("pinot: metricFunctions: {}", response.getMetricFunctions());
//      LOG.info("pinot: numRows: {}", response.getNumRows());
    }

//    if(cmd.hasOption(ID_MOCK)) {
//      LOG.info("Enabling 'mock' datasource");
//      factory.addDataSource("mock", new ThirdEyeMockDataSource());
//    }

    EventManager eventDAO = null;
    if(cmd.hasOption(ID_EVENT)) {
      if(!cmd.hasOption(ID_THIRDEYE)) {
        LOG.error("--{} requires --{}", ID_EVENT, ID_THIRDEYE);
        System.exit(1);
      }

      LOG.info("Enabling 'event' datasource");
      eventDAO = DaoProviderUtil.getInstance(EventManagerImpl.class);
      LOG.info("event: {}", eventDAO.findAll());
//      factory.addDataSource("event", new ThirdEyeEventDataSource(manager));
    }

    Collection<String> metrics = null;
    if(cmd.hasOption(ID_METRIC_PATH)) {
      LOG.info("Reading metrics from '{}'", cmd.getOptionValue(ID_METRIC_PATH));
      metrics = parseMetricConfig(new FileReader(new File(cmd.getOptionValue(ID_METRIC_PATH))));
      LOG.info("*** Metrics:\n{}", StringUtils.join(metrics, "\n"));
    }

    Map<String, Collection<String>> dimensions = null;
    if(cmd.hasOption(ID_DIMENSION_PATH)) {
      LOG.info("Reading dimensions from '{}'", cmd.getOptionValue(ID_DIMENSION_PATH));
      dimensions = parseDimensionConfig(new FileReader(new File(cmd.getOptionValue(ID_DIMENSION_PATH))));
      LOG.info("*** Dimensions:\n{}", StringUtils.join(dimensions.keySet(), "\n"));
    }

    Collection<RootCauseEntityDTO> rootCauseEntities = null;
    if(cmd.hasOption(ID_METRIC_DIMENSION)) {
      if(!cmd.hasOption(ID_METRIC_PATH)) {
        LOG.error("--{} requires --{}", ID_METRIC_DIMENSION, ID_METRIC_PATH);
        System.exit(1);
      }
      if(!cmd.hasOption(ID_DIMENSION_PATH)) {
        LOG.error("--{} requires --{}", ID_METRIC_DIMENSION, ID_DIMENSION_PATH);
        System.exit(1);
      }

      LOG.info("Generating metric-dimension cross products");
      rootCauseEntities = metricDimensionCrossProducts(metrics, dimensions);

      Collection<String> names = new ArrayList<>();
      for(RootCauseEntityDTO dto : rootCauseEntities) {
        names.add(dto.getName());
      }
      LOG.info("*** MetricDimensions:\n{}", StringUtils.join(names, "\n"));
    }

//    AnomalyFunctionExContext context = new AnomalyFunctionExContext();
//    context.setClassName(classname);
//    context.setMonitoringWindowStart(monitoringStart);
//    context.setMonitoringWindowEnd(monitoringEnd);
//    context.setConfig(config);
//
//    LOG.info("Instantiating ...");
//    AnomalyFunctionEx function = null;
//    try {
//      function = factory.fromContext(context);
//    } catch (Exception e) {
//      LOG.error("Error instantiating anomaly function", e);
//      System.exit(1);
//    }
//
//    LOG.info("Applying ...");
//    AnomalyFunctionExResult result = null;
//    try {
//      result = function.apply();
//    } catch (Exception e) {
//      LOG.error("Error applying anomaly function", e);
//      System.exit(1);
//    }
//
//    LOG.info("Got function result with {} anomalies", result.getAnomalies().size());
//    for(AnomalyFunctionExResult.Anomaly a : result.getAnomalies()) {
//      String data = "";
//      if(!a.getData().isEmpty())
//        data = a.getData().toString();
//      LOG.info("Anomaly at '{}-{}': '{}' {}", a.getStart(), a.getEnd(), a.getMessage(), data);
//    }

    LOG.info("Done.");

    if(cmd.hasOption(ID_PINOT)) {
      LOG.info("Forcing termination (Pinot connection workaround)");
      System.exit(0);
    }
  }

  private static Options makeParserOptions() {
    Options options = new Options();

    options.addOption(new Option("s", ID_MONITOR_FROM, true,
        "Monitoring window start timestamp in seconds. (Default: monitoring end timestamp - 1 hour)"));
    options.addOption(new Option("t", ID_MONITOR_TO, true,
        "Monitoring window end timestamp in seconds. (Default: now)"));
    options.addOption(new Option("P", ID_PINOT, true,
        "Enables 'pinot' data source. Requires path to pinot client config YAML file."));
    options.addOption(new Option("T", ID_THIRDEYE, true,
        "Enables access to the ThirdEye internal database. Requires path to thirdeye persistence config YAML file."));
    options.addOption(new Option("E", ID_EVENT, false,
        "Enables 'event' data source. (Requires: " + ID_THIRDEYE + ")"));
    options.addOption(new Option("M", ID_METRIC_CORRELATION, false,
        "Enables 'metric' data source. (Requires: " + ID_THIRDEYE + ")"));
    options.addOption(new Option("o", ID_OUTPUT_PATH, true,
        "Output path for CSV file generated by the DataLoader."));
    options.addOption(new Option("m", ID_METRIC_PATH, true, "Path to metric file"));
    options.addOption(new Option("d", ID_DIMENSION_PATH, true, "Path to dimension file"));
    options.addOption(new Option("d", ID_METRIC_DIMENSION, false, "Generate metric-dimension cross product"));

    return options;
  }

  private static Map<String, String> parseConfig(String config) throws IOException {
    String[] fragments = config.split(";");
    Map<String, String> map = new HashMap<>();
    for(String s : fragments) {
      String kv[] = s.split("=", 2);
      map.put(kv[0], kv[1]);
    }
    return map;
  }

  private static Map<String, String> parseConfigFile(Reader r) throws IOException {
    Properties p = new Properties();
    p.load(r);

    Map<String, String> map = new HashMap<>();
    for(Map.Entry<Object, Object> e : p.entrySet()) {
      map.put(e.getKey().toString(), e.getValue().toString());
    }
    return map;
  }

  private static String config2json(Map<String, String> config) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.writeValueAsString(config);
  }

  private static Collection<String> parseMetricConfig(Reader r) throws IOException {
    List<String> list = new ArrayList<>();
    BufferedReader br = new BufferedReader(r);

    String l;
    while((l = br.readLine()) != null) {
      for(String m : l.split(",")) {
        list.add(m);
      }
    }
    return list;
  }

  private static Map<String, Collection<String>> parseDimensionConfig(Reader r) throws IOException {
    Properties p = new Properties();
    p.load(r);

    Map<String, Collection<String>> map = new HashMap<>();
    for(Map.Entry<Object, Object> e : p.entrySet()) {
      map.put(e.getKey().toString(), Arrays.asList(e.getValue().toString().split(",")));
    }
    return map;
  }

  private static Collection<RootCauseEntityDTO> metricDimensionCrossProducts(Collection<String> metrics, Map<String, Collection<String>> dimensions) {
    List<RootCauseEntityDTO> dtos = new ArrayList<>();
    for(String m : metrics) {
      dtos.addAll(metricDimensionPow2(m, dimensions));
    }
    return dtos;
  }

  private static Collection<RootCauseEntityDTO> metricDimensionPow2(String metric, Map<String, Collection<String>> dimensions) {
    List<RootCauseEntityDTO> dtos = new ArrayList<>();
    List<Map.Entry<String, Collection<String>>> entries = new ArrayList<>(dimensions.entrySet());
    Collections.sort(entries, new Comparator<Map.Entry<String, Collection<String>>>() {
      @Override
      public int compare(Map.Entry<String, Collection<String>> o1, Map.Entry<String, Collection<String>> o2) {
        return o1.getKey().compareTo(o2.getKey());
      }
    });

    for(int i=0; i<entries.size(); i++) {
      for(int j=i+1; j<entries.size(); j++) {
        Map.Entry<String, Collection<String>> d1 = entries.get(i);
        Map.Entry<String, Collection<String>> d2 = entries.get(j);

        dtos.addAll(metricDimensionPow2Helper(metric, d1.getKey(), d1.getValue(), d2.getKey(), d2.getValue()));
      }
    }
    return dtos;
  }

  private static Collection<RootCauseEntityDTO> metricDimensionPow2Helper(String metric, String dim1, Collection<String> val1, String dim2, Collection<String> val2) {
    if(Objects.equals(dim1, dim2))
      return Collections.emptyList();

    List<RootCauseEntityDTO> dtos = new ArrayList<>();
    for(String v1 : val1) {
      for(String v2 : val2) {
        String s1 = dim1 + "=" + v1;
        String s2 = dim2 + "=" + v2;

        String name;
        if(s1.compareTo(s2) <= 0) {
          name = metric + "|" + s1 + "|" + s2;
        } else {
          name = metric + "|" + s2 + "|" + s1;
        }

        RootCauseEntityDTO dto = new RootCauseEntityDTO();
        dto.setName(name);
        dto.setType(RootCauseEntityBean.EntityType.METRIC_DIMENSION);

        dtos.add(dto);
      }
    }
    return dtos;
  }

}
