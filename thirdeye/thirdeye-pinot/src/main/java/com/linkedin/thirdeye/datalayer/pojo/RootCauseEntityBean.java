package com.linkedin.thirdeye.datalayer.pojo;

public class RootCauseEntityBean extends AbstractBean {
  public enum EntityType {
    METRIC,
    DIMENSION,
    SYSTEM,
    EVENT,
    UNKNOWN
  }

  String name;
  EntityType type = EntityType.UNKNOWN;

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
}
