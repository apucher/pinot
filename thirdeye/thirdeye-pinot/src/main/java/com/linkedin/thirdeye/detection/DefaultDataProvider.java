package com.linkedin.thirdeye.detection;

import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.dashboard.resources.v2.aggregation.AggregationLoader;
import com.linkedin.thirdeye.dashboard.resources.v2.timeseries.TimeSeriesLoader;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.bao.EventManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import java.util.Collection;
import java.util.Map;


public class DefaultDataProvider implements DataProvider {
  private final MetricConfigManager metricDAO;
  private final EventManager eventDAO;
  private final MergedAnomalyResultManager anomalyDAO;
  private final TimeSeriesLoader timeseriesLoader;
  private final AggregationLoader aggregationLoader;

  public DefaultDataProvider(MetricConfigManager metricDAO, EventManager eventDAO, MergedAnomalyResultManager anomalyDAO,
      TimeSeriesLoader timeseriesLoader, AggregationLoader aggregationLoader) {
    this.metricDAO = metricDAO;
    this.eventDAO = eventDAO;
    this.anomalyDAO = anomalyDAO;
    this.timeseriesLoader = timeseriesLoader;
    this.aggregationLoader = aggregationLoader;
  }

  @Override
  public Map<MetricSlice, DataFrame> fetchTimeseries(Collection<MetricSlice> slices) {
    throw new IllegalStateException("not implemented yet");
  }

  @Override
  public Map<MetricSlice, DataFrame> fetchAggregates(Collection<MetricSlice> slices) {
    throw new IllegalStateException("not implemented yet");
  }

  @Override
  public Map<MetricSlice, DataFrame> fetchBreakdowns(Collection<MetricSlice> slices) {
    throw new IllegalStateException("not implemented yet");
  }

  @Override
  public Multimap<MetricSlice, MergedAnomalyResultDTO> fetchAnomalies(Collection<MetricSlice> slices) {
    throw new IllegalStateException("not implemented yet");
  }

  @Override
  public Multimap<EventSlice, EventDTO> fetchEvents(Collection<EventSlice> slices) {
    throw new IllegalStateException("not implemented yet");
  }

  @Override
  public Map<Long, MetricConfigDTO> fetchMetrics(Collection<Long> ids) {
    throw new IllegalStateException("not implemented yet");
  }
}
