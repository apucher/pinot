package com.linkedin.thirdeye.detection;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.DataFrameUtils;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.collections.MapUtils;


public class TestDataProvider implements DataProvider{

  Map<MetricSlice, DataFrame> timeSeries;
  Map<Long, MetricConfigDTO> metrics;

  public TestDataProvider(Map<Long, MetricConfigDTO> metrics, Map<MetricSlice, DataFrame> timeSeries) {
    this.metrics = metrics;
    this.timeSeries = timeSeries;
  }

  @Override
  public Map<MetricSlice, DataFrame> fetchTimeseries(Collection<MetricSlice> slices) {
    Map<MetricSlice, DataFrame> result = new HashMap<>();
    for (MetricSlice slice : slices) {
      result.put(slice, timeSeries.get(slice));
    }
    return result;
  }

  @Override
  public Map<MetricSlice, DataFrame> fetchAggregates(Collection<MetricSlice> slices) {
    return Collections.emptyMap();
  }

  @Override
  public Map<MetricSlice, DataFrame> fetchBreakdowns(Collection<MetricSlice> slices) {
    return Collections.emptyMap();
  }

  @Override
  public Multimap<MetricSlice, MergedAnomalyResultDTO> fetchAnomalies(Collection<MetricSlice> slices) {
    return ArrayListMultimap.create();
  }

  @Override
  public Multimap<EventSlice, EventDTO> fetchEvents(Collection<EventSlice> slices) {
    return ArrayListMultimap.create();
  }

  @Override
  public Map<Long, MetricConfigDTO> fetchMetrics(Collection<Long> ids) {
    Map<Long, MetricConfigDTO> result = new HashMap<>();
    for (Long id : ids) {
      result.put(id, metrics.get(id));
    }
    return result;
  }
}
