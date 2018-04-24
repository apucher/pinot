package com.linkedin.thirdeye.detection;

import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.List;
import java.util.Map;


public abstract class StaticDetectionPipeline extends DetectionPipeline {
  public StaticDetectionPipeline(DetectionConfigDTO config, long startTime, long endTime) {
    super(config, startTime, endTime);
  }

  public abstract StaticDetectionPipelineModel getModel();

  public abstract List<MergedAnomalyResultDTO> run(StaticDetectionPipelineData data);

  @Override
  public final List<MergedAnomalyResultDTO> run() {
    StaticDetectionPipelineModel model = this.getModel();
    Map<MetricSlice, DataFrame> timeseries = this.fetchTimeseries(model.timeseriesSlices);
    Map<MetricSlice, DataFrame> aggregates = this.fetchAggregates(model.aggregateSlices);
    Map<MetricSlice, DataFrame> breakdowns = this.fetchBreakdowns(model.breakdownSlices);
    Multimap<MetricSlice, MergedAnomalyResultDTO> anomalies = this.fetchAnomalies(model.anomalySlices);
    Multimap<MetricSlice, EventDTO> events = this.fetchEvents(model.eventSlices);

    StaticDetectionPipelineData data = new StaticDetectionPipelineData(
        model, timeseries, aggregates, breakdowns, anomalies, events);

    return this.run(data);
  }
}
