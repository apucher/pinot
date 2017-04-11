package com.linkedin.thirdeye.rootcause.entities;

import com.linkedin.thirdeye.datalayer.dto.RootCauseEntityDTO;
import com.linkedin.thirdeye.datalayer.pojo.RootCauseEntityBean;
import java.util.HashMap;
import java.util.Map;


public final class MetricEntity {
  public static final String METRIC_ID = "METRIC_ID";
  public static final String METRIC_NAME = "METRIC_NAME";
  public static final String METRIC_DATASET = "METRIC_DATASET";

  public static MetricEntity loadFromDTO(RootCauseEntityDTO base) {
    if(!RootCauseEntityBean.EntityType.METRIC.equals(base.getType()))
      throw new IllegalArgumentException(String.format("Found type '%s' but requires '%s'",
          base.getType(), RootCauseEntityBean.EntityType.METRIC));

    long metricId = Long.parseLong(base.getProperties().get(METRIC_ID));
    String name = base.getProperties().get(METRIC_NAME);
    String dataset = base.getProperties().get(METRIC_DATASET);

    return new MetricEntity(base, metricId, name, dataset);
  }

  public RootCauseEntityDTO saveToDTO() {
    return this.saveToDTO(this.base);
  }

  public RootCauseEntityDTO saveToDTO(RootCauseEntityDTO base) {
    Map<String, String> properties = base.getProperties();
    if(properties == null)
      properties = new HashMap<>();
    properties.put(METRIC_ID, String.valueOf(this.metricId));
    properties.put(METRIC_NAME, String.valueOf(this.name));
    properties.put(METRIC_DATASET, String.valueOf(this.dataset));
    return this.base;
  }

  long metricId;
  String name;
  String dataset;
  RootCauseEntityDTO base;

  public MetricEntity() {
    // left blank
  }

  public MetricEntity(RootCauseEntityDTO base, long metricId, String name, String dataset) {
    this.metricId = metricId;
    this.name = name;
    this.dataset = dataset;
    this.base = base;
  }

  public long getMetricId() {
    return metricId;
  }

  public void setMetricId(long metricId) {
    this.metricId = metricId;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDataset() {
    return dataset;
  }

  public void setDataset(String dataset) {
    this.dataset = dataset;
  }

  public RootCauseEntityDTO getBase() {
    return base;
  }

  public void setBase(RootCauseEntityDTO base) {
    this.base = base;
  }
}
