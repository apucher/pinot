package com.linkedin.thirdeye.detection;

import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import java.util.Collection;
import java.util.Map;


public interface DataProvider {
  Map<MetricSlice, DataFrame> fetchTimeseries(Collection<MetricSlice> slices);

  Map<MetricSlice, DataFrame> fetchAggregates(Collection<MetricSlice> slices);

  Map<MetricSlice, DataFrame> fetchBreakdowns(Collection<MetricSlice> slices);

  Multimap<MetricSlice, MergedAnomalyResultDTO> fetchAnomalies(Collection<MetricSlice> slices);

  Multimap<EventSlice, EventDTO> fetchEvents(Collection<EventSlice> slices);

  Map<Long, MetricConfigDTO> fetchMetrics(Collection<Long> ids);
}
