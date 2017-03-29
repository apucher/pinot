package com.linkedin.thirdeye.datalayer.entity;

public class RootCauseRelationIndex extends AbstractIndexEntity {
  String type; // RootCauseRelationBean.RelationType
  long fromId;
  long toId;

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public long getFromId() {
    return fromId;
  }

  public void setFromId(long fromId) {
    this.fromId = fromId;
  }

  public long getToId() {
    return toId;
  }

  public void setToId(long toId) {
    this.toId = toId;
  }
}
