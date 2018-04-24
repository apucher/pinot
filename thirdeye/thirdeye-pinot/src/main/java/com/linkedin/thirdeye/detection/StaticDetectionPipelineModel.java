package com.linkedin.thirdeye.detection;

import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import java.util.Collection;


public class StaticDetectionPipelineModel {
  final Collection<MetricSlice> timeseriesSlices;
  final Collection<MetricSlice> aggregateSlices;
  final Collection<MetricSlice> breakdownSlices;
  final Collection<MetricSlice> anomalySlices;
  final Collection<EventSlice> eventSlices;

  public StaticDetectionPipelineModel(Collection<MetricSlice> timeseriesSlices, Collection<MetricSlice> aggregateSlices,
      Collection<MetricSlice> breakdownSlices, Collection<MetricSlice> anomalySlices,
      Collection<EventSlice> eventSlices) {
    this.timeseriesSlices = timeseriesSlices;
    this.aggregateSlices = aggregateSlices;
    this.breakdownSlices = breakdownSlices;
    this.anomalySlices = anomalySlices;
    this.eventSlices = eventSlices;
  }

  public Collection<MetricSlice> getTimeseriesSlices() {
    return timeseriesSlices;
  }

  public Collection<MetricSlice> getAggregateSlices() {
    return aggregateSlices;
  }

  public Collection<MetricSlice> getBreakdownSlices() {
    return breakdownSlices;
  }

  public Collection<MetricSlice> getAnomalySlices() {
    return anomalySlices;
  }

  public Collection<EventSlice> getEventSlices() {
    return eventSlices;
  }
}
