package com.linkedin.thirdeye.detection;

import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.Map;


public class StaticDetectionPipelineData {
  final StaticDetectionPipelineModel model;
  final Map<MetricSlice, DataFrame> timeseries;
  final Map<MetricSlice, DataFrame> aggregates;
  final Map<MetricSlice, DataFrame> breakdowns;
  final Multimap<MetricSlice, MergedAnomalyResultDTO> anomalies;
  final Multimap<MetricSlice, EventDTO> events;

  public StaticDetectionPipelineData(StaticDetectionPipelineModel model, Map<MetricSlice, DataFrame> timeseries,
      Map<MetricSlice, DataFrame> aggregates, Map<MetricSlice, DataFrame> breakdowns,
      Multimap<MetricSlice, MergedAnomalyResultDTO> anomalies, Multimap<MetricSlice, EventDTO> events) {
    this.model = model;
    this.timeseries = timeseries;
    this.aggregates = aggregates;
    this.breakdowns = breakdowns;
    this.anomalies = anomalies;
    this.events = events;
  }

  public StaticDetectionPipelineModel getModel() {
    return model;
  }

  public Map<MetricSlice, DataFrame> getTimeseries() {
    return timeseries;
  }

  public Map<MetricSlice, DataFrame> getAggregates() {
    return aggregates;
  }

  public Map<MetricSlice, DataFrame> getBreakdowns() {
    return breakdowns;
  }

  public Multimap<MetricSlice, MergedAnomalyResultDTO> getAnomalies() {
    return anomalies;
  }

  public Multimap<MetricSlice, EventDTO> getEvents() {
    return events;
  }
}
