package com.linkedin.thirdeye.detection;

import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;


public class StaticDetectionPipelineModel {
  final Collection<MetricSlice> timeseriesSlices;
  final Collection<MetricSlice> aggregateSlices;
  final Collection<MetricSlice> breakdownSlices;
  final Collection<AnomalySlice> anomalySlices;
  final Collection<EventSlice> eventSlices;
  final Set<String> breakdownDimensions;

  public StaticDetectionPipelineModel() {
    this.timeseriesSlices = Collections.emptyList();
    this.aggregateSlices = Collections.emptyList();
    this.breakdownSlices = Collections.emptyList();
    this.anomalySlices = Collections.emptyList();
    this.eventSlices = Collections.emptyList();
    this.breakdownDimensions = Collections.emptySet();
  }

  public StaticDetectionPipelineModel(Collection<MetricSlice> timeseriesSlices, Collection<MetricSlice> aggregateSlices,
      Collection<MetricSlice> breakdownSlices, Collection<AnomalySlice> anomalySlices,
      Collection<EventSlice> eventSlices, Set<String> breakdownDimensions) {
    this.timeseriesSlices = timeseriesSlices;
    this.aggregateSlices = aggregateSlices;
    this.breakdownSlices = breakdownSlices;
    this.anomalySlices = anomalySlices;
    this.eventSlices = eventSlices;
    this.breakdownDimensions = breakdownDimensions;
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

  public Collection<AnomalySlice> getAnomalySlices() {
    return anomalySlices;
  }

  public Collection<EventSlice> getEventSlices() {
    return eventSlices;
  }

  public Set<String> getBreakdownDimensions() {
    return breakdownDimensions;
  }

  public StaticDetectionPipelineModel withTimeseriesSlices(Collection<MetricSlice> timeseriesSlices) {
    return new StaticDetectionPipelineModel(timeseriesSlices, this.aggregateSlices, this.breakdownSlices, this.anomalySlices, this.eventSlices, this.breakdownDimensions);
  }

  public StaticDetectionPipelineModel withAggregateSlices(Collection<MetricSlice> aggregateSlices) {
    return new StaticDetectionPipelineModel(this.timeseriesSlices, aggregateSlices, this.breakdownSlices, this.anomalySlices, this.eventSlices, this.breakdownDimensions);
  }
  public StaticDetectionPipelineModel withBreakdownSlices(Collection<MetricSlice> breakdownSlices) {
    return new StaticDetectionPipelineModel(this.timeseriesSlices, this.aggregateSlices, breakdownSlices, this.anomalySlices, this.eventSlices, this.breakdownDimensions);
  }

  public StaticDetectionPipelineModel withAnomalySlices(Collection<AnomalySlice> anomalySlices) {
    return new StaticDetectionPipelineModel(this.timeseriesSlices, this.aggregateSlices, this.breakdownSlices, anomalySlices, this.eventSlices, this.breakdownDimensions);
  }

  public StaticDetectionPipelineModel withEventSlices(Collection<EventSlice> eventSlices) {
    return new StaticDetectionPipelineModel(this.timeseriesSlices, this.aggregateSlices, this.breakdownSlices, this.anomalySlices, eventSlices, this.breakdownDimensions);
  }

  public StaticDetectionPipelineModel withBreakdownDimensions(Set<String> breakdownDimensions) {
    return new StaticDetectionPipelineModel(this.timeseriesSlices, this.aggregateSlices, this.breakdownSlices, this.anomalySlices, this.eventSlices, breakdownDimensions);
  }

}
