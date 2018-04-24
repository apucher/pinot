package com.linkedin.thirdeye.detection;

import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.Collection;
import java.util.List;
import java.util.Map;


public abstract class DetectionPipeline {
  final DetectionConfigDTO config;
  final long startTime;
  final long endTime;

  public DetectionPipeline(DetectionConfigDTO config, long startTime, long endTime) {
    this.config = config;
    this.startTime = startTime;
    this.endTime = endTime;
  }

  public abstract List<MergedAnomalyResultDTO> run();

  final Map<MetricSlice, DataFrame> fetchTimeseries(Collection<MetricSlice> slices) {
    throw new IllegalStateException("not implemented yet");
  }

  final Map<MetricSlice, DataFrame> fetchAggregates(Collection<MetricSlice> slices) {
    throw new IllegalStateException("not implemented yet");
  }

  final Map<MetricSlice, DataFrame> fetchBreakdowns(Collection<MetricSlice> slices) {
    throw new IllegalStateException("not implemented yet");
  }

  final Multimap<MetricSlice, MergedAnomalyResultDTO> fetchAnomalies(Collection<MetricSlice> slices) {
    throw new IllegalStateException("not implemented yet");
  }

  final Multimap<MetricSlice, EventDTO> fetchEvents(Collection<EventSlice> slices) {
    throw new IllegalStateException("not implemented yet");
  }
}
