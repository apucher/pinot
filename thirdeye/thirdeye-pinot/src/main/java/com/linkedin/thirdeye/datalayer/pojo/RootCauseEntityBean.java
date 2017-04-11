package com.linkedin.thirdeye.datalayer.pojo;

import java.util.Map;


public class RootCauseEntityBean extends AbstractBean {
  public enum EntityType {
    METRIC,
    DIMENSION,
    METRIC_DIMENSION,
    SYSTEM,
    EVENT,
    UNKNOWN
  }

  String name;
  EntityType type = EntityType.UNKNOWN;
  Map<String, String> properties;

  public EntityType getType() {
    return type;
  }

  public void setType(EntityType type) {
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }
}
