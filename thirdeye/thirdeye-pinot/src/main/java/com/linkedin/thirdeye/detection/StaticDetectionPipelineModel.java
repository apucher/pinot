package com.linkedin.thirdeye.detection;

import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import java.util.Collection;
import java.util.Collections;


public class StaticDetectionPipelineModel {
  final Collection<MetricSlice> timeseriesSlices;
  final Collection<MetricSlice> aggregateSlices;
  final Collection<MetricSlice> breakdownSlices;
  final Collection<MetricSlice> anomalySlices;
  final Collection<EventSlice> eventSlices;

  public StaticDetectionPipelineModel() {
    this.timeseriesSlices = Collections.emptyList();
    this.aggregateSlices = Collections.emptyList();
    this.breakdownSlices = Collections.emptyList();
    this.anomalySlices = Collections.emptyList();
    this.eventSlices = Collections.emptyList();
  }

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

  public StaticDetectionPipelineModel withTimeseriesSlices(Collection<MetricSlice> timeseriesSlices) {
    return new StaticDetectionPipelineModel(timeseriesSlices, this.aggregateSlices, this.breakdownSlices, this.anomalySlices, this.eventSlices);
  }

  public StaticDetectionPipelineModel withAggregateSlices(Collection<MetricSlice> aggregateSlices) {
    return new StaticDetectionPipelineModel(this.timeseriesSlices, aggregateSlices, this.breakdownSlices, this.anomalySlices, this.eventSlices);
  }
  public StaticDetectionPipelineModel withBreakdownSlices(Collection<MetricSlice> breakdownSlices) {
    return new StaticDetectionPipelineModel(this.timeseriesSlices, this.aggregateSlices, breakdownSlices, this.anomalySlices, this.eventSlices);
  }

  public StaticDetectionPipelineModel withAnomalySlices(Collection<MetricSlice> anomalySlices) {
    return new StaticDetectionPipelineModel(this.timeseriesSlices, this.aggregateSlices, this.breakdownSlices, anomalySlices, this.eventSlices);
  }

  public StaticDetectionPipelineModel withEventSlices(Collection<EventSlice> eventSlices) {
    return new StaticDetectionPipelineModel(this.timeseriesSlices, this.aggregateSlices, this.breakdownSlices, this.anomalySlices, eventSlices);
  }
}
