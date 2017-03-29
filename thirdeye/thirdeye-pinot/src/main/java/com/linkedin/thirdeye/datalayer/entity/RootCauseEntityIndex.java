package com.linkedin.thirdeye.datalayer.entity;

public class RootCauseEntityIndex extends AbstractIndexEntity {
  String name;
  String type; // RootCauseEntityBean.EntityType

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }
}
